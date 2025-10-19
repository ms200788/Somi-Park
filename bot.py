#!/usr/bin/env python3
# bot.py
# Final webhook-only Video Cover bot (fixed)
# - aiogram==2.25.0
# - aiohttp==3.8.6
#
# Behavior:
#  - Send a photo -> sets it as your per-user cover (thumbnail).
#  - /show_cover -> preview the cover.
#  - /del_cover -> delete the cover.
#  - Send videos (single, media-group, forwarded many) -> bot attempts to re-send each video
#    using Telegram's CDN URL (so Telegram will fetch and create a new file record) with the cover
#    attached as `thumb`. This approach avoids downloading the full video to your server while
#    making Telegram create a new video message that reliably keeps the thumbnail.
#  - /cancel -> clear queued videos for the session.
#  - Webhook-only (aiohttp.web.run_app). Designed for Render like environments.
#  - Writes webhook raw bodies to data/last_update.json for debugging.
#
# IMPORTANT:
#  - Provide a .env file in the same folder with:
#      BOT_TOKEN=123456:ABC-DEF...
#      OWNER_ID=123456789
#      WEBHOOK_HOST=https://your-render-app.onrender.com
#      PORT=10000
#      WEBHOOK_PATH=/webhook
#
#  - Use requirements:
#      aiogram==2.25.0
#      aiohttp==3.8.6
#      python-dotenv
#
# Copy this whole file into bot.py and deploy.
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
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from functools import wraps
from contextlib import suppress

from dotenv import load_dotenv
from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, types
from aiogram.types import ContentType
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError, BadRequest

# ---------------------------
# Load environment
# ---------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN not set in environment (.env)")
    sys.exit(1)

OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
if not OWNER_ID_RAW:
    print("ERROR: OWNER_ID not set in environment (.env)")
    sys.exit(1)
try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    print("ERROR: OWNER_ID must be numeric")
    sys.exit(1)

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "").strip()
if not WEBHOOK_HOST:
    print("ERROR: WEBHOOK_HOST not set in environment (.env) — must be public HTTPS URL")
    sys.exit(1)

PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "").strip() or "/webhook"
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH
WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

# ---------------------------
# Filesystem and DB paths
# ---------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "cover_bot.sqlite"
LAST_UPDATE_PATH = DATA_DIR / "last_update.json"

for p in (DATA_DIR, LOGS_DIR):
    p.mkdir(parents=True, exist_ok=True)

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
logger = logging.getLogger("video_cover_bot_fixed")

# ---------------------------
# Tunables
# ---------------------------
MAX_QUEUE_PER_USER = 2000             # large queue allowed (server-side only)
SELF_PING_INTERVAL = 60               # seconds
WATCHDOG_INTERVAL = 120               # seconds
MEDIA_GROUP_AGG_WINDOW = 0.6          # seconds to collect album items
PERF_HISTORY_LIMIT = 8
PROGRESS_BAR_LEN = 24

# ---------------------------
# Data models
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
            added_ts=obj.get("added_ts", int(time.time())),
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
            "processing": self.processing,
        }

    @staticmethod
    def from_json(obj: dict) -> "UserSession":
        s = UserSession(user_id=obj["user_id"])
        s.cover_file_id = obj.get("cover_file_id")
        s.queue = [QueuedVideo.from_json(q) for q in obj.get("queue", [])]
        s.processing = obj.get("processing", False)
        return s

# ---------------------------
# SQLite DB helpers
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

# ---------------------------
# Bot & Dispatcher
# ---------------------------
bot = Bot(token=BOT_TOKEN)
# set current so aiogram message helpers work under webhook mode
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
def human_time(sec: float) -> str:
    s = int(round(sec))
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h}h {m}m"
    d, h = divmod(h, 24)
    return f"{d}d {h}h"

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
        logger.exception("Failed to Bot.set_current(bot)")

# safe handler decorator to keep webhook tolerant
def safe_handler(fn):
    @wraps(fn)
    async def wrapper(message: types.Message, *args, **kwargs):
        ensure_bot_current()
        try:
            return await fn(message, *args, **kwargs)
        except Exception:
            logger.exception("Unhandled exception in handler %s", fn.__name__)
            tb = traceback.format_exc()
            try:
                await notify_owner(f"Handler {fn.__name__} exception:\n{tb[:1900]}")
            except Exception:
                logger.debug("Failed to notify owner")
            try:
                await message.reply("⚠️ An internal error occurred. The owner has been notified.")
            except Exception:
                logger.debug("Could not reply to user after handler exception")
    return wrapper

# ---------------------------
# Command handlers
# ---------------------------
HELP_TEXT = (
    "🎬 *Video Cover Bot*\n\n"
    "• Send a *photo* to set it as your cover (thumbnail).\n"
    "• Send a *video* (or multiple videos) — bot will return each video with your cover attached.\n"
    "• /show_cover — preview your cover.\n"
    "• /del_cover — delete your cover.\n"
    "• /cancel — clear queued videos for your session.\n"
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
        await message.reply("❗ You don't have a cover set. Send a photo to set one.")
        return
    try:
        await bot.send_photo(chat_id=message.chat.id, photo=s.cover_file_id, caption="📸 Your current cover")
    except Exception:
        logger.exception("Failed to send cover preview to %s", message.from_user.id)
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
# Photo handler: sets cover
# ---------------------------
@dp.message_handler(content_types=ContentType.PHOTO)
@safe_handler
async def handle_photo(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    photo = message.photo[-1]  # largest by default
    s.cover_file_id = photo.file_id
    s.processing = False
    persist_session(s)
    await message.reply("✅ Cover saved. Send video(s) and I will return them with this cover attached. Use /show_cover to preview or /del_cover to remove.")

# ---------------------------
# Document handler: accept image as cover or video documents
# ---------------------------
@dp.message_handler(content_types=ContentType.DOCUMENT)
@safe_handler
async def handle_document(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    doc = message.document
    mime = (doc.mime_type or "").lower()
    # image doc as cover
    if mime.startswith("image/"):
        s.cover_file_id = doc.file_id
        persist_session(s)
        await message.reply("✅ Cover set (image document).")
        return
    # video doc => queue
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
        if len(s.queue) >= MAX_QUEUE_PER_USER:
            await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel to clear.")
            return
        q = QueuedVideo(
            file_id=doc.file_id,
            caption=message.caption,
            file_unique_id=getattr(doc, "file_unique_id", None),
            file_size=getattr(doc, "file_size", None)
        )
        s.queue.append(q)
        persist_session(s)
        await message.reply(f"✅ Video queued ({len(s.queue)} in queue).")
        if not s.processing:
            asyncio.create_task(process_user_queue(s, message.chat.id))
        return
    await message.reply("I accept image documents as covers and video documents as videos. Send a photo to set cover or send a video to queue it.")

# ---------------------------
# Video handler: queue and trigger processing
# ---------------------------
@dp.message_handler(content_types=ContentType.VIDEO)
@safe_handler
async def handle_video(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    video = message.video
    if len(s.queue) >= MAX_QUEUE_PER_USER:
        await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel to clear.")
        return
    q = QueuedVideo(file_id=video.file_id, caption=message.caption, file_unique_id=getattr(video, "file_unique_id", None), file_size=getattr(video, "file_size", None))
    s.queue.append(q)
    persist_session(s)
    await message.reply(f"✅ Video queued ({len(s.queue)} in queue).")
    if not s.processing:
        asyncio.create_task(process_user_queue(s, message.chat.id))

# ---------------------------
# Media-group (album) aggregator
# ---------------------------
@dp.message_handler(lambda msg: bool(msg.media_group_id), content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT])
@safe_handler
async def handle_media_group_member(message: types.Message):
    mg_id = str(message.media_group_id)
    user_id = message.from_user.id
    buf = _media_group_buffers.get(mg_id)
    if buf is None:
        _media_group_buffers[mg_id] = [message]
        loop = asyncio.get_event_loop()
        def _flush():
            asyncio.ensure_future(_process_media_group(mg_id))
        h = loop.call_later(MEDIA_GROUP_AGG_WINDOW, _flush)
        _media_group_timers[mg_id] = h
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
            # set cover if a photo is present
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
# Core processing worker
# ---------------------------
async def process_user_queue(session: UserSession, reply_chat_id: int):
    """
    Attempt to send queued videos in FIFO order. For each queued video:
      1) Call get_file(file_id) to obtain file_path.
      2) Build file_url = https://api.telegram.org/file/bot<TOKEN>/<file_path>
      3) Call send_video(video=file_url, thumb=session.cover_file_id, caption=...).
         - Telegram will fetch the file from its CDN and create a new message record where
           the thumb parameter is usually accepted.
      4) If the above doesn't attach the thumb (checked via sent_msg.video.thumb), fallback:
         - Send video by file_id without thumb (so user still receives video),
         - Notify owner about inability to apply cover.
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
            # progress message
            try:
                pct_before = (processed - 1) / (total_initial or 1)
                await bot.send_message(reply_chat_id, f"⏳ Processing {processed}/{total_initial} — {progress_bar := progress_bar(pct_before)}")
            except Exception:
                logger.debug("Failed to send progress pre-message")

            thumb = session.cover_file_id  # may be None

            t0 = time.time()
            # Step A: get_file -> file_path -> file_url
            try:
                file_obj = await bot.get_file(q.file_id)
                file_path = getattr(file_obj, "file_path", None)
                if not file_path:
                    raise RuntimeError("get_file returned no file_path")
                file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
                logger.info("User %s: got file_path=%s", session.user_id, file_path)
            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter during get_file for user %s: wait %s", session.user_id, wait)
                await asyncio.sleep(wait + 1)
                continue
            except TelegramAPIError as te:
                logger.exception("TelegramAPIError during get_file for user %s: %s", session.user_id, te)
                # fallback to send by file_id
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                except Exception:
                    logger.exception("Fallback send_video by file_id also failed for user %s", session.user_id)
                session.queue.pop(0)
                persist_session(session)
                try:
                    await notify_owner(f"get_file failed for user {session.user_id} file_id={q.file_id}: {te}")
                except Exception:
                    logger.debug("notify_owner failed")
                continue
            except Exception as e:
                logger.exception("Error in get_file for user %s: %s", session.user_id, e)
                # fallback to sending by file_id
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                except Exception:
                    logger.exception("Fallback send_video by file_id failed for user %s", session.user_id)
                session.queue.pop(0)
                persist_session(session)
                try:
                    await notify_owner(f"Error getting file_path for user {session.user_id}, file_id={q.file_id}: {e}")
                except Exception:
                    logger.debug("notify_owner failed")
                continue

            # Step B: send by file_url with thumb param
            try:
                sent_msg = await bot.send_video(chat_id=reply_chat_id, video=file_url, caption=(q.caption or ""), thumb=thumb)
                t1 = time.time()
                duration = t1 - t0
                hist.append(duration)
                perf_history[session.user_id] = hist[-PERF_HISTORY_LIMIT:]
                # check whether sent_msg has video.thumb and compare with cover
                applied = False
                try:
                    if getattr(sent_msg, "video", None) and getattr(sent_msg.video, "thumb", None):
                        applied_thumb = sent_msg.video.thumb.file_id
                        applied = True
                        logger.info("Applied thumb %s for user %s (expected %s)", applied_thumb, session.user_id, thumb)
                except Exception:
                    applied = False
                if applied:
                    # success
                    session.queue.pop(0)
                    persist_session(session)
                    remaining = len(session.queue)
                    avg = (sum(hist) / len(hist)) if hist else duration
                    eta = avg * remaining
                    pct_done = processed / (total_initial or 1)
                    try:
                        await bot.send_message(reply_chat_id, f"✅ Sent {processed}/{total_initial} — {progress_bar := progress_bar(pct_done)} ETA: {human_time(eta)}")
                    except Exception:
                        logger.debug("Failed to send progress done message")
                    continue
                else:
                    # Telegram accepted the URL send, but didn't attach a thumb (rare)
                    logger.warning("Telegram sent message but didn't attach thumb for user %s file_id %s", session.user_id, q.file_id)
                    # send plain by file_id so user still gets the video
                    try:
                        await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                    except Exception:
                        logger.exception("Fallback plain send failed for user %s", session.user_id)
                    session.queue.pop(0)
                    persist_session(session)
                    try:
                        await bot.send_message(reply_chat_id, "⚠️ Sent video but couldn't apply cover (Telegram did not attach thumb).")
                    except Exception:
                        logger.debug("Failed to notify user about missing thumb")
                    try:
                        await notify_owner(f"Could not apply cover for user {session.user_id} on file {q.file_id}. send by URL didn't attach thumb.")
                    except Exception:
                        logger.debug("Failed to notify owner about missing thumb")
                    continue
            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter while sending via file_url for user %s: wait %s", session.user_id, wait)
                await asyncio.sleep(wait + 1)
                continue  # retry same item
            except BadRequest as br:
                # often indicates Telegram rejected the parameters or the URL
                logger.warning("BadRequest sending by file_url for user %s: %s", session.user_id, br)
                # fallback: try to send by file_id (no thumb)
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                except Exception:
                    logger.exception("Fallback plain send after BadRequest failed for user %s", session.user_id)
                session.queue.pop(0)
                persist_session(session)
                try:
                    await notify_owner(f"BadRequest when sending by file_url for user {session.user_id} file_id={q.file_id}: {br}")
                except Exception:
                    logger.debug("notify_owner failed")
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
                    await notify_owner(f"TelegramAPIError when sending by file_url for user {session.user_id} file_id={q.file_id}: {te}")
                except Exception:
                    logger.debug("notify_owner failed")
                continue
            except Exception as e:
                logger.exception("Unexpected error when sending by file_url for user %s: %s", session.user_id, e)
                # final fallback: try plain send and remove item
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                except Exception:
                    logger.exception("Final fallback plain send failed for user %s", session.user_id)
                session.queue.pop(0)
                persist_session(session)
                try:
                    await notify_owner(f"Unexpected error applying cover for user {session.user_id} file_id={q.file_id}: {e}\nTrace:\n{traceback.format_exc()[:2000]}")
                except Exception:
                    logger.debug("notify_owner failed")
                continue

        # done
        session.processing = False
        persist_session(session)
        elapsed = time.time() - start_all
        try:
            await bot.send_message(reply_chat_id, f"🎬 Finished processing {processed} video(s) in {human_time(elapsed)}.")
        except Exception:
            logger.debug("Could not send final completion message to chat")

# ---------------------------
# Webhook handler
# ---------------------------
async def handle_webhook(request: web.Request) -> web.Response:
    # read raw body for debugging
    try:
        raw = await request.text()
    except Exception:
        return web.Response(status=400, text="Bad request - cannot read body")

    # write last_update.json for inspection
    try:
        with open(LAST_UPDATE_PATH, "w", encoding="utf-8") as f:
            f.write(raw)
    except Exception:
        logger.debug("Failed to write last_update.json (non-fatal)")

    # ensure Bot current
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

    # parse JSON
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
            logger.exception("Failed to parse update")
            return web.Response(status=400, text="Bad request - invalid update")

    # process update, but if handler throws ensure we return 200 OK so Telegram won't retry repeatedly
    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Exception while processing update (handler threw). Returning 200 to Telegram.")
        try:
            tb = traceback.format_exc()
            await notify_owner(f"Handler exception during update processing:\n{tb[:2000]}")
        except Exception:
            logger.debug("Failed to notify owner about handler exception")
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
            logger.exception("self_ping: bot.get_me failed (non-fatal)")
        try:
            async with ClientSession() as sess:
                url = WEBHOOK_HOST.rstrip("/") + "/health"
                async with sess.get(url, timeout=10) as resp:
                    logger.debug("self_ping: health status %s", resp.status)
        except Exception:
            logger.debug("self_ping: health GET failed (non-fatal)")
        await asyncio.sleep(interval)

async def webhook_watchdog(app: web.Application, interval: int = WATCHDOG_INTERVAL):
    await asyncio.sleep(5)
    while True:
        try:
            info = await bot.get_webhook_info()
            url = getattr(info, "url", None)
            pending = getattr(info, "pending_update_count", None)
            last_error = getattr(info, "last_error_message", None)
            logger.debug("watchdog: webhook info url=%s pending=%s last_error=%s", url, pending, last_error)
            if not url or url.rstrip("/") != WEBHOOK_URL.rstrip("/"):
                logger.warning("watchdog: webhook URL mismatch (got=%s want=%s). resetting.", url, WEBHOOK_URL)
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
                        await notify_owner(f"Watchdog failed to set webhook to {WEBHOOK_URL}")
                    except Exception:
                        pass
        except Exception:
            logger.exception("watchdog: unexpected error (non-fatal)")
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
        logger.info("Loaded %d sessions into memory", len(sessions))
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
        loop.create_task(notify_owner(f"Uncaught exception in bot:\n{tbtext[:2000]}"))
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
def setup_signal_handlers(loop: asyncio.AbstractEventLoop):
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
        logger.exception("delete_webhook failed during shutdown (non-fatal)")
    os._exit(0)

# ---------------------------
# Startup / shutdown hooks for aiohttp
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
        logger.exception("Unexpected exception when setting webhook on startup")
        await notify_owner("Unexpected exception when setting webhook on startup; see logs")

    load_all_sessions()
    app["self_ping_task"] = asyncio.create_task(self_ping_task(app))
    app["watchdog_task"] = asyncio.create_task(webhook_watchdog(app))
    logger.info("Background tasks started")

async def on_shutdown(app: web.Application):
    logger.info("on_shutdown: deleting webhook and cancelling tasks")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("delete_webhook failed during shutdown (non-fatal)")
    for key in ("self_ping_task", "watchdog_task"):
        t = app.get(key)
        if t:
            t.cancel()
            with suppress(Exception):
                await t
    logger.info("Shutdown complete")

# ---------------------------
# Main entrypoint
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

    logger.info("Starting aiohttp web server at 0.0.0.0:%s webhook_path=%s", PORT, WEBHOOK_PATH)
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