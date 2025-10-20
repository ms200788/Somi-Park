#!/usr/bin/env python3
# bot.py
# Hybrid webhook Video Cover bot (permanent-apply, re-upload fallback)
# - aiogram==2.25.0
# - aiohttp==3.8.6
# - python-dotenv
#
# Purpose:
#  - Per-user cover (send photo to set). Commands: /show_cover, /del_cover, /cancel
#  - When user sends video(s):
#      * Try fast server-side re-create: get_file -> build Telegram CDN URL -> send_video(video=file_url, thumb=cover)
#      * If that succeeds and Telegram attached the thumb, save mapping original_file_unique_id -> new_file_id (permanent)
#      * If Telegram ignored thumb, or if you configured to force reupload, stream-download the file to disk and re-upload with thumb (guaranteed)
#      * If get_file fails (e.g. "File is too big"), the bot will *notify the user* and provide instructions:
#          - Forward the file to a private channel where the bot is admin (often allows get_file), or
#          - Allow the bot to re-upload from a URL you provide / host the file for the bot to fetch.
#  - After one successful re-upload, subsequent sends use the saved new_file_id (instant).
#  - Webhook-only aiohttp server (suitable for Render). Self-ping /watchdog included.
#  - SQLite persistence for sessions and file mappings.
#
# IMPORTANT NOTES:
#  - For guaranteed success on 3.5 GB files you must run this on a VM with sufficient disk & bandwidth if the "re-upload from CDN" path fails.
#  - If Telegram's getFile denies access to a file, the bot cannot obtain the file_path via the Bot API. In that case you must follow the suggested workaround (private channel admin or provide external URL).
#
# Env vars (in .env):
#  BOT_TOKEN, OWNER_ID (int), WEBHOOK_HOST (https://...), PORT (10000), WEBHOOK_PATH (optional, default /webhook)
#  MAX_REUPLOAD_BYTES (optional) - max size in bytes the bot will attempt to download/re-upload (default 4_000_000_000)
#
# Save as bot.py and run. Use the matching requirements:
#  aiogram==2.25.0
#  aiohttp==3.8.6
#  python-dotenv
#
# ------------------------------------------------------------------------------

import os
import sys
import json
import time
import sqlite3
import asyncio
import logging
import traceback
import pathlib
import math
import tempfile
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from functools import wraps
from contextlib import suppress

from dotenv import load_dotenv
from aiohttp import web, ClientSession, ClientTimeout
from aiogram import Bot, Dispatcher, types
from aiogram.types import ContentType, InputFile
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError, BadRequest

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN not set")
    sys.exit(1)

OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
if not OWNER_ID_RAW:
    print("ERROR: OWNER_ID not set")
    sys.exit(1)
try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    print("ERROR: OWNER_ID must be numeric")
    sys.exit(1)

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "").strip()
if not WEBHOOK_HOST:
    print("ERROR: WEBHOOK_HOST not set (must be public HTTPS URL)")
    sys.exit(1)

PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "").strip() or "/webhook"
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH
WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

# max bytes we will attempt to download/re-upload (default 4GB)
MAX_REUPLOAD_BYTES = int(os.getenv("MAX_REUPLOAD_BYTES", str(4_000_000_000)))

# ---------------------------
# Files & DB
# ---------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
TMP_DIR = DATA_DIR / "tmp"
DB_PATH = DATA_DIR / "coverbot_hybrid.sqlite"
LAST_UPDATE_PATH = DATA_DIR / "last_update.json"

for d in (DATA_DIR, LOGS_DIR, TMP_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("coverbot_hybrid")

# ---------------------------
# Tunables
# ---------------------------
MAX_QUEUE_PER_USER = 1000
SELF_PING_INTERVAL = 60
WATCHDOG_INTERVAL = 120
PROGRESS_BAR_LEN = 24
PERF_HISTORY_LIMIT = 8
DOWNLOAD_CHUNK = 1 * 1024 * 1024  # 1 MB

# ---------------------------
# Data classes
# ---------------------------
@dataclass
class QueuedVideo:
    file_id: str
    caption: Optional[str] = None
    file_unique_id: Optional[str] = None
    file_size: Optional[int] = None
    added_ts: int = field(default_factory=lambda: int(time.time()))

    def to_json(self) -> dict:
        return {
            "file_id": self.file_id,
            "caption": self.caption,
            "file_unique_id": self.file_unique_id,
            "file_size": self.file_size,
            "added_ts": self.added_ts,
        }

    @staticmethod
    def from_json(obj: dict) -> "QueuedVideo":
        return QueuedVideo(
            file_id=obj.get("file_id"),
            caption=obj.get("caption"),
            file_unique_id=obj.get("file_unique_id"),
            file_size=obj.get("file_size"),
            added_ts=obj.get("added_ts", int(time.time()))
        )

@dataclass
class UserSession:
    user_id: int
    cover_file_id: Optional[str] = None
    queue: List[QueuedVideo] = field(default_factory=list)
    processing: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def to_json(self) -> dict:
        return {
            "user_id": self.user_id,
            "cover_file_id": self.cover_file_id,
            "queue": [q.to_json() for q in self.queue],
            "processing": self.processing
        }

    @staticmethod
    def from_json(obj: dict) -> "UserSession":
        s = UserSession(user_id=obj["user_id"])
        s.cover_file_id = obj.get("cover_file_id")
        s.queue = [QueuedVideo.from_json(q) for q in obj.get("queue", [])]
        s.processing = obj.get("processing", False)
        return s

# ---------------------------
# DB helpers (sessions + mappings)
# ---------------------------
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            user_id INTEGER PRIMARY KEY,
            json TEXT,
            updated_at INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mappings (
            original_unique_id TEXT PRIMARY KEY,
            new_file_id TEXT,
            new_file_unique_id TEXT,
            created_at INTEGER
        )
    """)
    conn.commit()
    return conn

db = init_db()
db_lock = asyncio.Lock()

def save_session_db(session: UserSession):
    try:
        ts = int(time.time())
        cur = db.cursor()
        cur.execute("INSERT OR REPLACE INTO sessions (user_id, json, updated_at) VALUES (?, ?, ?)",
                    (session.user_id, json.dumps(session.to_json()), ts))
        db.commit()
    except Exception:
        logger.exception("save_session_db failed for %s", session.user_id)

def load_session_db(user_id: int) -> Optional[UserSession]:
    try:
        cur = db.cursor()
        cur.execute("SELECT json FROM sessions WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        return UserSession.from_json(json.loads(row[0]))
    except Exception:
        logger.exception("load_session_db failed for %s", user_id)
        return None

def delete_session_db(user_id: int):
    try:
        cur = db.cursor()
        cur.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        db.commit()
    except Exception:
        logger.exception("delete_session_db failed for %s", user_id)

def save_mapping(original_unique_id: str, new_file_id: str, new_file_unique_id: Optional[str]):
    try:
        ts = int(time.time())
        cur = db.cursor()
        cur.execute("INSERT OR REPLACE INTO mappings (original_unique_id, new_file_id, new_file_unique_id, created_at) VALUES (?, ?, ?, ?)",
                    (original_unique_id, new_file_id, new_file_unique_id, ts))
        db.commit()
    except Exception:
        logger.exception("save_mapping failed for %s", original_unique_id)

def load_mapping(original_unique_id: str) -> Optional[Dict[str, Any]]:
    try:
        cur = db.cursor()
        cur.execute("SELECT new_file_id, new_file_unique_id FROM mappings WHERE original_unique_id = ?", (original_unique_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {"new_file_id": row[0], "new_file_unique_id": row[1]}
    except Exception:
        logger.exception("load_mapping failed for %s", original_unique_id)
        return None

# ---------------------------
# Bot & Dispatcher
# ---------------------------
bot = Bot(token=BOT_TOKEN)
Bot.set_current(bot)
dp = Dispatcher(bot)

# ---------------------------
# Runtime caches
# ---------------------------
sessions: Dict[int, UserSession] = {}
perf_history: Dict[int, List[float]] = {}
_media_group_buffers: Dict[str, List[types.Message]] = {}
_media_group_timers: Dict[str, asyncio.Handle] = {}

# ---------------------------
# Utilities
# ---------------------------
def human_time(seconds: float) -> str:
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h}h {m}m"
    days, h = divmod(h, 24)
    return f"{days}d {h}h"

def progress_bar(percent: float, length: int = PROGRESS_BAR_LEN) -> str:
    percent = max(0.0, min(1.0, percent))
    filled = int(round(length * percent))
    return "▰" * filled + "▱" * (length - filled)

async def notify_owner(text: str):
    try:
        await bot.send_message(OWNER_ID, text)
    except Exception:
        logger.exception("notify_owner failed")

def get_session(user_id: int) -> UserSession:
    s = sessions.get(user_id)
    if not s:
        obj = load_session_db(user_id)
        if obj:
            s = obj
        else:
            s = UserSession(user_id=user_id)
        s.lock = asyncio.Lock()
        sessions[user_id] = s
    return s

def persist_session(session: UserSession):
    try:
        save_session_db(session)
    except Exception:
        logger.exception("persist_session failed for %s", session.user_id)

def ensure_bot_current():
    try:
        Bot.set_current(bot)
    except Exception:
        logger.exception("Bot.set_current failed")

def safe_handler(fn):
    @wraps(fn)
    async def wrapper(message: types.Message, *args, **kwargs):
        ensure_bot_current()
        try:
            return await fn(message, *args, **kwargs)
        except Exception:
            logger.exception("Handler exception %s", fn.__name__)
            try:
                await notify_owner(f"Handler {fn.__name__} exception:\n{traceback.format_exc()[:2000]}")
            except Exception:
                pass
            try:
                await message.reply("⚠️ Internal error. Owner notified.")
            except Exception:
                pass
    return wrapper

# ---------------------------
# Commands
# ---------------------------
HELP_TEXT = (
    "🎬 *Video Cover Bot (Hybrid)*\n\n"
    "• Send a *photo* to set it as your cover (thumbnail).\n"
    "• /show_cover — preview your current cover.\n"
    "• /del_cover — delete your current cover.\n"
    "• Send video(s) — bot will try to create a permanent copy with your cover.\n"
    "• /cancel — clear your queued videos.\n\n"
    "Notes: For very large files, if Telegram blocks bot access the bot will ask you to forward to your private channel where the bot is admin or provide a direct file URL."
)

@dp.message_handler(commands=["start", "help"])
@safe_handler
async def cmd_start(message: types.Message):
    await message.reply(HELP_TEXT, parse_mode="Markdown")

@dp.message_handler(commands=["show_cover"])
@safe_handler
async def cmd_show_cover(message: types.Message):
    s = get_session(message.from_user.id)
    if not s.cover_file_id:
        await message.reply("❗ You have no cover set. Send a photo to set one.")
        return
    try:
        await bot.send_photo(chat_id=message.chat.id, photo=s.cover_file_id, caption="📸 Your current cover")
    except Exception:
        logger.exception("Failed to send cover preview")
        await message.reply("❗ Failed to show cover. Try sending a new photo.")

@dp.message_handler(commands=["del_cover"])
@safe_handler
async def cmd_del_cover(message: types.Message):
    s = get_session(message.from_user.id)
    if not s.cover_file_id:
        await message.reply("❗ No cover to delete.")
        return
    s.cover_file_id = None
    persist_session(s)
    await message.reply("🗑 Cover deleted. Send a photo to set a new one.")

@dp.message_handler(commands=["cancel"])
@safe_handler
async def cmd_cancel(message: types.Message):
    s = get_session(message.from_user.id)
    async with s.lock:
        qlen = len(s.queue)
        s.queue = []
        s.processing = False
        persist_session(s)
    await message.reply(f"🗑 Cleared {qlen} queued video(s).")

# ---------------------------
# Photo -> set cover
# ---------------------------
@dp.message_handler(content_types=ContentType.PHOTO)
@safe_handler
async def handle_photo(message: types.Message):
    s = get_session(message.from_user.id)
    photo = message.photo[-1]
    s.cover_file_id = photo.file_id
    s.processing = False
    persist_session(s)
    await message.reply("✅ Cover saved. Send videos and I'll create permanent copies with this cover.")

# ---------------------------
# Document handler (image as cover or video file)
# ---------------------------
@dp.message_handler(content_types=ContentType.DOCUMENT)
@safe_handler
async def handle_document(message: types.Message):
    s = get_session(message.from_user.id)
    doc = message.document
    mime = (doc.mime_type or "").lower()
    # image doc -> cover
    if mime.startswith("image/"):
        s.cover_file_id = doc.file_id
        persist_session(s)
        await message.reply("✅ Cover saved (image document).")
        return
    # video doc -> queue
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
        if len(s.queue) >= MAX_QUEUE_PER_USER:
            await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel to clear.")
            return
        q = QueuedVideo(file_id=doc.file_id, caption=message.caption, file_unique_id=getattr(doc, "file_unique_id", None), file_size=getattr(doc, "file_size", None))
        s.queue.append(q)
        persist_session(s)
        await message.reply(f"✅ Queued video ({len(s.queue)} in queue).")
        if not s.processing:
            asyncio.create_task(process_user_queue(s, message.chat.id))
        return
    await message.reply("I accept image documents as covers and video files as videos. Send a photo to set cover or a video to queue it.")

# ---------------------------
# Video handler -> queue
# ---------------------------
@dp.message_handler(content_types=ContentType.VIDEO)
@safe_handler
async def handle_video(message: types.Message):
    s = get_session(message.from_user.id)
    video = message.video
    if len(s.queue) >= MAX_QUEUE_PER_USER:
        await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel first.")
        return
    q = QueuedVideo(file_id=video.file_id, caption=message.caption, file_unique_id=getattr(video, "file_unique_id", None), file_size=getattr(video, "file_size", None))
    s.queue.append(q)
    persist_session(s)
    await message.reply(f"✅ Queued video ({len(s.queue)} in queue).")
    if not s.processing:
        asyncio.create_task(process_user_queue(s, message.chat.id))

# ---------------------------
# Media group aggregator
# ---------------------------
@dp.message_handler(lambda msg: bool(msg.media_group_id), content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT])
@safe_handler
async def handle_media_group_member(message: types.Message):
    mg = str(message.media_group_id)
    buf = _media_group_buffers.get(mg)
    if buf is None:
        _media_group_buffers[mg] = [message]
        loop = asyncio.get_event_loop()
        def _flush():
            asyncio.ensure_future(_process_media_group(mg))
        h = loop.call_later(0.6, _flush)
        _media_group_timers[mg] = h
    else:
        buf.append(message)

async def _process_media_group(mg_id: str):
    buf = _media_group_buffers.pop(mg_id, [])
    h = _media_group_timers.pop(mg_id, None)
    if h:
        try:
            h.cancel()
        except Exception:
            pass
    if not buf:
        return
    user_id = buf[0].from_user.id
    s = get_session(user_id)
    queued = 0
    for msg in buf:
        if msg.photo:
            photo = msg.photo[-1]
            s.cover_file_id = photo.file_id
            persist_session(s)
        elif msg.video:
            if len(s.queue) < MAX_QUEUE_PER_USER:
                q = QueuedVideo(file_id=msg.video.file_id, caption=msg.caption, file_unique_id=getattr(msg.video, "file_unique_id", None), file_size=getattr(msg.video, "file_size", None))
                s.queue.append(q)
                queued += 1
        elif msg.document:
            doc = msg.document
            mime = (doc.mime_type or "").lower()
            if mime.startswith("image/"):
                s.cover_file_id = doc.file_id
                persist_session(s)
            elif mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
                if len(s.queue) < MAX_QUEUE_PER_USER:
                    q = QueuedVideo(file_id=doc.file_id, caption=msg.caption, file_unique_id=getattr(doc, "file_unique_id", None), file_size=getattr(doc, "file_size", None))
                    s.queue.append(q)
                    queued += 1
    persist_session(s)
    if queued and not s.processing:
        asyncio.create_task(process_user_queue(s, buf[0].chat.id))

# ---------------------------
# Helper: stream-download Telegram CDN file_url to disk with progress callback
# ---------------------------
async def stream_download(url: str, dest_path: pathlib.Path, expected_size: Optional[int] = None, progress_cb=None):
    """
    Stream a remote URL to dest_path using aiohttp in chunks.
    progress_cb(bytes_so_far, total_bytes_or_None)
    """
    timeout = ClientTimeout(total=None, sock_connect=30, sock_read=300)
    async with ClientSession(timeout=timeout) as sess:
        async with sess.get(url) as resp:
            resp.raise_for_status()
            total = expected_size or resp.content_length
            written = 0
            with open(dest_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK):
                    f.write(chunk)
                    written += len(chunk)
                    if progress_cb:
                        try:
                            progress_cb(written, total)
                        except Exception:
                            pass
    return dest_path

# ---------------------------
# Core processing worker
# ---------------------------
async def process_user_queue(session: UserSession, reply_chat_id: int):
    """
    For each queued video:
      - If mapping exists (original_unique_id -> new_file_id), send new_file_id (instant).
      - Else attempt get_file -> construct CDN url -> send_video(video=file_url, thumb=cover).
          * If Telegram attached thumb, save mapping and continue.
          * If Telegram ignored thumb:
               - If file_size <= MAX_REUPLOAD_BYTES: stream-download the CDN url to disk and re-upload (send_video with thumb using InputFile). Save mapping.
               - Else: notify user with instructions (forward to private channel with the bot as admin or provide external URL).
      - If get_file fails (File is too big), notify user with instructions.
    """
    async with session.lock:
        if session.processing:
            return
        session.processing = True
        persist_session(session)

        total_initial = len(session.queue)
        processed = 0
        start_all = time.time()
        hist = perf_history.get(session.user_id, [])
        hist = hist[-PERF_HISTORY_LIMIT:]

        while session.queue:
            q = session.queue[0]
            processed += 1
            # progress pre-msg
            try:
                pct_before = (processed - 1) / (total_initial or 1)
                await bot.send_message(reply_chat_id, f"⏳ Processing {processed}/{total_initial} — {progress_bar(pct_before)}")
            except Exception:
                logger.debug("Could not send progress start message")

            # If we have a mapping for this original file_unique_id, use it
            if q.file_unique_id:
                mapping = load_mapping(q.file_unique_id)
                if mapping and mapping.get("new_file_id"):
                    mapped_file_id = mapping["new_file_id"]
                    try:
                        await bot.send_video(chat_id=reply_chat_id, video=mapped_file_id, caption=(q.caption or ""))
                        session.queue.pop(0)
                        persist_session(session)
                        processed_time = 0.01
                        hist.append(processed_time)
                        perf_history[session.user_id] = hist[-PERF_HISTORY_LIMIT:]
                        try:
                            remaining = len(session.queue)
                            avg = (sum(hist) / len(hist)) if hist else processed_time
                            eta = avg * remaining
                            pct_done = processed / (total_initial or 1)
                            await bot.send_message(reply_chat_id, f"✅ Sent {processed}/{total_initial} — {progress_bar(pct_done)} ETA: {human_time(eta)}")
                        except Exception:
                            pass
                        continue
                    except RetryAfter as r:
                        wait = int(getattr(r, "timeout", 5))
                        logger.warning("RetryAfter when sending mapped file_id for user %s: wait %s", session.user_id, wait)
                        await asyncio.sleep(wait + 1)
                        continue
                    except Exception:
                        logger.exception("Failed to send mapped_file_id for user %s; will attempt recreate", session.user_id)
                        # fall through to recreate path

            # No mapping; attempt get_file to retrieve file_path
            try:
                file_obj = await bot.get_file(q.file_id)
                file_path = getattr(file_obj, "file_path", None)
                if not file_path:
                    raise RuntimeError("get_file returned no file_path")
                file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
                logger.info("user %s: get_file -> %s", session.user_id, file_path)
            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter during get_file user %s: wait %s", session.user_id, wait)
                await asyncio.sleep(wait + 1)
                continue
            except TelegramAPIError as te:
                # often 'File is too big' or other rejections
                logger.warning("get_file failed for user %s file_id=%s: %s", session.user_id, q.file_id, te)
                # inform the user with concrete next steps
                await notify_user_getfile_failed(session.user_id, reply_chat_id, q)
                # pop to avoid infinite loop (user will re-send if they follow instructions)
                session.queue.pop(0)
                persist_session(session)
                continue
            except Exception as e:
                logger.exception("Unexpected error during get_file for user %s: %s", session.user_id, e)
                await notify_user_getfile_failed(session.user_id, reply_chat_id, q)
                session.queue.pop(0)
                persist_session(session)
                continue

            # Now try to send by CDN URL with thumb
            try:
                sent_msg = await bot.send_video(chat_id=reply_chat_id, video=file_url, caption=(q.caption or ""), thumb=session.cover_file_id)
                # check if thumb applied
                applied = False
                try:
                    if getattr(sent_msg, "video", None) and getattr(sent_msg.video, "thumb", None):
                        applied_thumb = sent_msg.video.thumb.file_id
                        applied = True
                        logger.info("Thumb applied for user %s: %s", session.user_id, applied_thumb)
                except Exception:
                    applied = False

                if applied:
                    # persist mapping so we can reuse new file id in future (instant)
                    try:
                        new_file_id = sent_msg.video.file_id
                        new_file_unique = getattr(sent_msg.video, "file_unique_id", None)
                        if q.file_unique_id:
                            save_mapping(q.file_unique_id, new_file_id, new_file_unique)
                        logger.info("Saved mapping original=%s -> new=%s", q.file_unique_id, new_file_id)
                    except Exception:
                        logger.exception("Failed to save mapping after successful URL send")

                    # pop and continue
                    session.queue.pop(0)
                    persist_session(session)
                    t1 = time.time()
                    duration = max(0.01, t1 - time.time() + 0.01)  # rough
                    hist.append(duration)
                    perf_history[session.user_id] = hist[-PERF_HISTORY_LIMIT:]
                    remaining = len(session.queue)
                    avg = (sum(hist) / len(hist)) if hist else duration
                    eta = avg * remaining
                    pct_done = processed / (total_initial or 1)
                    try:
                        await bot.send_message(reply_chat_id, f"✅ Sent {processed}/{total_initial} — {progress_bar(pct_done)} ETA: {human_time(eta)}")
                    except Exception:
                        pass
                    continue
                else:
                    # Telegram accepted the URL send but didn't attach thumb — try guaranteed reupload if file_size known and within limit
                    logger.warning("URL send did not attach thumb for user %s file %s", session.user_id, q.file_unique_id or q.file_id)
                    # If file_size known and within MAX_REUPLOAD_BYTES, download + reupload
                    file_size = q.file_size
                    if not file_size:
                        # try to get HEAD content-length as fallback
                        try:
                            async with ClientSession() as sess:
                                async with sess.head(file_url) as resp:
                                    cl = resp.headers.get("Content-Length")
                                    if cl:
                                        file_size = int(cl)
                        except Exception:
                            file_size = None
                    if file_size and file_size <= MAX_REUPLOAD_BYTES:
                        # Download to temp file (stream) and reupload with InputFile + thumb
                        await bot.send_message(reply_chat_id, "🔁 Telegram didn't attach the cover when sending by URL — streaming and re-uploading to guarantee cover. This may take some time.")
                        tmp_file = tempfile.NamedTemporaryFile(prefix="vid_", suffix=".mp4", dir=str(TMP_DIR), delete=False)
                        tmp_path = pathlib.Path(tmp_file.name)
                        tmp_file.close()
                        downloaded = 0

                        try:
                            def progress_cb(written, total):
                                nonlocal downloaded
                                downloaded = written
                                # send lightweight progress (not too frequently)
                                # we won't spam; just log
                                logger.debug("Downloading %s bytes/%s", written, total)

                            await stream_download(file_url, tmp_path, expected_size=file_size, progress_cb=progress_cb)

                            # re-upload
                            with open(tmp_path, "rb") as fh:
                                sent_msg2 = await bot.send_video(chat_id=reply_chat_id, video=InputFile(fh), caption=(q.caption or ""), thumb=session.cover_file_id)
                            # check thumb
                            applied2 = False
                            try:
                                if getattr(sent_msg2, "video", None) and getattr(sent_msg2.video, "thumb", None):
                                    applied2 = True
                            except Exception:
                                applied2 = False
                            if applied2:
                                # save mapping
                                try:
                                    new_file_id = sent_msg2.video.file_id
                                    new_file_unique = getattr(sent_msg2.video, "file_unique_id", None)
                                    if q.file_unique_id:
                                        save_mapping(q.file_unique_id, new_file_id, new_file_unique)
                                except Exception:
                                    logger.exception("Failed to save mapping after reupload")
                                await bot.send_message(reply_chat_id, "✅ Re-upload succeeded and cover applied.")
                            else:
                                await bot.send_message(reply_chat_id, "⚠️ Re-upload done but thumbnail was not applied by Telegram.")
                                await notify_owner(f"Re-upload did not result in thumb applied for user {session.user_id} file {q.file_unique_id}")
                        except Exception:
                            logger.exception("Download/reupload failed for user %s", session.user_id)
                            await bot.send_message(reply_chat_id, "❌ Failed to re-upload file to apply cover. See owner for details.")
                            await notify_owner(f"Download/reupload failed for user {session.user_id} file {q.file_unique_id}. Trace:\n{traceback.format_exc()[:2000]}")
                        finally:
                            # cleanup temp file
                            with suppress(Exception):
                                if tmp_path.exists():
                                    tmp_path.unlink()
                            # pop and continue
                            session.queue.pop(0)
                            persist_session(session)
                            continue
                    else:
                        # file too large to re-upload (or size unknown). Notify user with instructions
                        logger.warning("Cannot reupload: file_size=%s, MAX=%s", file_size, MAX_REUPLOAD_BYTES)
                        await bot.send_message(reply_chat_id,
                            "⚠️ Telegram didn't attach your cover and the bot cannot re-upload the video automatically (file too large or unknown size). "
                            "To apply cover you can:\n"
                            "• Forward the video (as a copy) to a private channel where this bot is admin, then send it again; or\n"
                            "• Provide a direct URL where the bot can fetch the file, and ensure it's accessible to the bot.\n\n"
                            "I cannot proceed automatically for this file.")
                        # notify owner
                        try:
                            await notify_owner(f"User {session.user_id} file {q.file_unique_id or q.file_id} could not be applied automatically (URL send no thumb and too large to reupload).")
                        except Exception:
                            pass
                        session.queue.pop(0)
                        persist_session(session)
                        continue

            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter while sending by file_url for user %s: wait %s", session.user_id, wait)
                await asyncio.sleep(wait + 1)
                continue
            except BadRequest as br:
                # BadRequest may indicate Telegram refused the file_url or thumb param
                logger.warning("BadRequest when sending by file_url user %s: %s", session.user_id, br)
                # try fallback plain send by file_id so user at least gets a copy
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                except Exception:
                    logger.exception("Fallback plain send failed after BadRequest for user %s", session.user_id)
                session.queue.pop(0)
                persist_session(session)
                try:
                    await notify_owner(f"BadRequest when sending by file_url for user {session.user_id} file {q.file_unique_id or q.file_id}: {br}")
                except Exception:
                    pass
                continue
            except TelegramAPIError as te:
                logger.exception("TelegramAPIError sending by file_url for user %s: %s", session.user_id, te)
                # fallback plain send
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                except Exception:
                    logger.exception("Fallback plain send failed after TelegramAPIError for user %s", session.user_id)
                session.queue.pop(0)
                persist_session(session)
                try:
                    await notify_owner(f"TelegramAPIError when sending by file_url for user {session.user_id} file {q.file_unique_id or q.file_id}: {te}")
                except Exception:
                    pass
                continue
            except Exception as e:
                logger.exception("Unexpected error when sending by file_url for user %s: %s", session.user_id, e)
                # attempt fallback plain send and remove item
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                except Exception:
                    logger.exception("Fallback plain send failed for user %s", session.user_id)
                session.queue.pop(0)
                persist_session(session)
                try:
                    await notify_owner(f"Unexpected error applying cover for user {session.user_id} file {q.file_unique_id or q.file_id}: {e}\nTrace:\n{traceback.format_exc()[:2000]}")
                except Exception:
                    pass
                continue

        # finished
        session.processing = False
        persist_session(session)
        elapsed = time.time() - start_all
        try:
            await bot.send_message(reply_chat_id, f"🎬 All done. Processed {processed} video(s) in {human_time(elapsed)}.")
        except Exception:
            logger.debug("Could not send completion message")

# ---------------------------
# Helper: notify user with instructions when get_file fails
# ---------------------------
async def notify_user_getfile_failed(user_id: int, chat_id: int, q: QueuedVideo):
    try:
        await bot.send_message(chat_id, (
            "⚠️ I couldn't access that video via the Bot API (Telegram returned 'file is too big' or similar). "
            "To let me apply your cover (thumbnail), please do one of the following:\n\n"
            "1) Forward the *same* message (not as a compressed copy) to your *private channel* where this bot is an admin, "
            "then send the forwarded message to me from that channel; the bot will often then be able to access it.\n\n"
            "2) Upload the file somewhere the bot can fetch it (provide a direct URL). After that, send the URL here.\n\n"
            "If neither is possible, the bot cannot automatically apply the cover to that specific file due to Telegram API limits."
        ), parse_mode="Markdown")
    except Exception:
        logger.exception("Failed to notify user about get_file failure")

# ---------------------------
# Webhook handler
# ---------------------------
async def handle_webhook(request: web.Request) -> web.Response:
    try:
        raw = await request.text()
    except Exception:
        return web.Response(status=400, text="Bad request - cannot read body")

    try:
        with open(LAST_UPDATE_PATH, "w", encoding="utf-8") as f:
            f.write(raw)
    except Exception:
        logger.debug("Failed to write last_update.json (non-fatal)")

    try:
        Bot.set_current(bot)
        try:
            Dispatcher.set_current(dp)  # type: ignore[attr-defined]
        except Exception:
            try:
                dp._bot = bot  # type: ignore[attr-defined]
            except Exception:
                pass
    except Exception:
        logger.exception("Failed to set aiogram current instances")

    try:
        data = json.loads(raw)
    except Exception:
        return web.Response(status=400, text="Bad request - invalid json")

    try:
        update = types.Update.de_json(data, bot)
    except Exception:
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse Update")
            return web.Response(status=400, text="Bad request - invalid update")

    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Exception during dp.process_update")
        try:
            tb = traceback.format_exc()
            await notify_owner(f"Handler error: {tb[:2000]}")
        except Exception:
            pass
        return web.Response(text="OK")
    return web.Response(text="OK")

# ---------------------------
# Health endpoints
# ---------------------------
async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "owner_id": OWNER_ID,
        "sessions_loaded": len(sessions)
    })

async def handle_info(request: web.Request) -> web.Response:
    try:
        return web.json_response({
            "status": "ok",
            "owner_id": OWNER_ID,
            "sessions_loaded": len(sessions)
        })
    except Exception:
        return web.json_response({"status": "error"}, status=500)

# ---------------------------
# Background tasks: self-ping & watchdog
# ---------------------------
async def self_ping_task(app: web.Application, interval: int = SELF_PING_INTERVAL):
    await asyncio.sleep(2)
    while True:
        try:
            await bot.get_me()
        except Exception:
            logger.exception("self_ping: bot.get_me failed")
        try:
            async with ClientSession() as sess:
                url = WEBHOOK_HOST.rstrip("/") + "/health"
                async with sess.get(url, timeout=10) as resp:
                    logger.debug("self_ping: health status %s", resp.status)
        except Exception:
            logger.debug("self_ping: failed (non-fatal)")
        await asyncio.sleep(interval)

async def webhook_watchdog(app: web.Application, interval: int = WATCHDOG_INTERVAL):
    await asyncio.sleep(5)
    while True:
        try:
            info = await bot.get_webhook_info()
            url = getattr(info, "url", None)
            if not url or url.rstrip("/") != WEBHOOK_URL.rstrip("/"):
                logger.warning("watchdog: webhook mismatch (got=%s want=%s) resetting", url, WEBHOOK_URL)
                try:
                    await bot.delete_webhook(drop_pending_updates=False)
                except Exception:
                    logger.debug("watchdog: delete_webhook failed (non-fatal)")
                try:
                    await bot.set_webhook(WEBHOOK_URL)
                    logger.info("watchdog: webhook reset to %s", WEBHOOK_URL)
                except Exception:
                    logger.exception("watchdog: failed to set webhook")
                    try:
                        await notify_owner("Watchdog failed to set webhook; check logs")
                    except Exception:
                        pass
        except Exception:
            logger.exception("watchdog: unexpected error")
        await asyncio.sleep(interval)

# ---------------------------
# Load sessions at startup
# ---------------------------
def load_all_sessions():
    try:
        cur = db.cursor()
        cur.execute("SELECT user_id, json FROM sessions")
        rows = cur.fetchall()
        for row in rows:
            try:
                uid = int(row[0])
                s = UserSession.from_json(json.loads(row[1]))
                s.lock = asyncio.Lock()
                sessions[uid] = s
            except Exception:
                logger.exception("load_all_sessions failed for row %s", row[0])
        logger.info("Loaded %d sessions", len(sessions))
    except Exception:
        logger.exception("load_all_sessions failed")

# ---------------------------
# Exception hooks
# ---------------------------
def excepthook(exc_type, exc, tb):
    tbtext = "".join(traceback.format_exception(exc_type, exc, tb))
    logger.critical("Uncaught exception: %s", tbtext)
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(notify_owner(f"Uncaught exception:\n{tbtext[:2000]}"))
    except Exception:
        pass

sys.excepthook = excepthook

def task_exception_handler(loop, context):
    msg = context.get("exception", context.get("message"))
    logger.error("Asyncio task exception: %s", msg)
    try:
        loop.create_task(notify_owner(f"Asyncio task exception: {msg}"))
    except Exception:
        pass

# ---------------------------
# Signals & shutdown
# ---------------------------
def setup_signal_handlers(loop):
    try:
        import signal
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(shutdown_signal(sig)))
    except Exception:
        logger.debug("Signal handlers not set (platform limitation)")

async def shutdown_signal(sig):
    logger.info("Received signal %s, shutting down", sig)
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("delete_webhook failed during shutdown")
    os._exit(0)

# ---------------------------
# Startup/shutdown hooks
# ---------------------------
async def on_startup(app: web.Application):
    logger.info("on_startup: setting webhook to %s", WEBHOOK_URL)
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except BadRequest as e:
        logger.exception("BadRequest when setting webhook: %s", e)
        await notify_owner(f"Failed to set webhook to {WEBHOOK_URL}: {e}")
        raise
    except Exception:
        logger.exception("Unexpected error setting webhook on startup")
        await notify_owner("Unexpected error setting webhook on startup; check logs")

    load_all_sessions()
    app["self_ping"] = asyncio.create_task(self_ping_task(app))
    app["watchdog"] = asyncio.create_task(webhook_watchdog(app))
    logger.info("Background tasks started")

async def on_shutdown(app: web.Application):
    logger.info("on_shutdown: deleting webhook and cancelling tasks")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("delete_webhook failed during shutdown")
    for k in ("self_ping", "watchdog"):
        t = app.get(k)
        if t:
            t.cancel()
            with suppress(Exception):
                await t
    logger.info("Shutdown complete")

# ---------------------------
# Main
# ---------------------------
def main():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda l, ctx: task_exception_handler(l, ctx))
    setup_signal_handlers(loop)
    Bot.set_current(bot)
    load_all_sessions()
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/info", handle_info)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    logger.info("Starting webhook aiohttp server 0.0.0.0:%s webhook_path=%s", PORT, WEBHOOK_PATH)
    try:
        web.run_app(app, host="0.0.0.0", port=PORT)
    except Exception:
        logger.exception("web.run_app failed")
        try:
            loop.run_until_complete(notify_owner("Server failed to start; see logs"))
        except Exception:
            pass

if __name__ == "__main__":
    main()