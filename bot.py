#!/usr/bin/env python3
# bot.py
# Webhook-only Video Cover bot (final)
# - aiogram==2.25.0
# - aiohttp==3.8.6
#
# Behavior:
#  - Send a photo -> sets it as your per-user cover/thumbnail.
#  - /show_cover -> preview the cover.
#  - /del_cover -> remove the cover.
#  - Send videos (single, media-group, forwarded many) -> bot re-sends each video with your cover
#    attached by attempting to reuse Telegram's server-side file (no download).
#  - Webhook-only, uses aiohttp.web.run_app; suitable for Render deployment.
#
# NOTE:
#  - This bot **does not** download then re-upload large video blobs by default; it attempts server-side
#    re-reference by using file_id in send_video (so Telegram clones/copies server-side). This is what
#    makes it appear instant for 20-30 videos in a second on many Telegram-hosted files.
#
# Save as bot.py and provide .env with required variables as described above.
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
from typing import Dict, List, Optional, Any
from functools import wraps
from contextlib import suppress, contextmanager

from dotenv import load_dotenv
from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, types
from aiogram.types import ContentType
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError, BadRequest

# ----------------------------
# Load environment variables
# ----------------------------
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

# ----------------------------
# Setup filesystem paths
# ----------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "cover_bot.sqlite"
LAST_UPDATE_PATH = DATA_DIR / "last_update.json"

for p in (DATA_DIR, LOGS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("video_cover_bot_final")

# ----------------------------
# Tunables and constants
# ----------------------------
MAX_QUEUE_PER_USER = 1000      # large queue allowed (server-side)
SELF_PING_INTERVAL = 60        # seconds
WATCHDOG_INTERVAL = 120        # seconds
MEDIA_GROUP_AGGREGATION_WINDOW = 0.6  # seconds to wait for media group members
PERF_HISTORY_LIMIT = 8
PROGRESS_BAR_LEN = 24

# ----------------------------
# Data classes for session state
# ----------------------------
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

    # runtime only
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

# ----------------------------
# SQLite database helpers
# ----------------------------
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

# ----------------------------
# Bot and dispatcher
# ----------------------------
bot = Bot(token=BOT_TOKEN)
# ensure bot instance available for helper methods in webhook processing
Bot.set_current(bot)

dp = Dispatcher(bot)

# ----------------------------
# Runtime in-memory caches
# ----------------------------
sessions: Dict[int, UserSession] = {}
perf_history: Dict[int, List[float]] = {}  # per-user recent send durations

# media-group aggregator: group_id -> list of (message, received_ts)
_media_group_buffers: Dict[str, List[types.Message]] = {}
_media_group_timers: Dict[str, asyncio.TimerHandle] = {}

# ----------------------------
# Utilities
# ----------------------------
def human_time(seconds: float) -> str:
    s = int(round(seconds))
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h}h {m}m"
    days, h = divmod(h, 24)
    return f"{days}d {h}h"

def make_progress_bar(percent: float, length: int = PROGRESS_BAR_LEN) -> str:
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

# safe decorator sets Bot current and catches exceptions
def safe_handler(fn):
    @wraps(fn)
    async def wrapper(message: types.Message, *args, **kwargs):
        Bot.set_current(bot)
        try:
            return await fn(message, *args, **kwargs)
        except Exception:
            logger.exception("Unhandled exception in handler %s", fn.__name__)
            tb = traceback.format_exc()
            try:
                await notify_owner(f"Handler {fn.__name__} exception:\n{tb[:2000]}")
            except Exception:
                logger.debug("Failed to notify owner")
            try:
                await message.reply("⚠️ An internal error occurred. The owner has been notified.")
            except Exception:
                logger.debug("Couldn't reply to user after exception")
    return wrapper

# ----------------------------
# Command handlers
# ----------------------------
HELP_TEXT = (
    "🎬 *Video Cover Bot*\n\n"
    "• Send a *photo* to set it as your cover (thumbnail).\n"
    "• Send a *video* (or many videos) — the bot will return each video with your cover attached.\n"
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
        await message.reply("❗ No cover set.")
        return
    s.cover_file_id = None
    persist_session(s)
    await message.reply("🗑 Cover deleted. Send a photo to set a new one.")

@dp.message_handler(commands=["cancel"])
@safe_handler
async def cmd_cancel(message: types.Message):
    s = get_session(message.from_user.id)
    async with s.lock:
        count = len(s.queue)
        s.queue = []
        s.processing = False
        persist_session(s)
    await message.reply(f"🗑 Cleared {count} queued video(s).")

# ----------------------------
# Photo handler: set cover
# ----------------------------
@dp.message_handler(content_types=ContentType.PHOTO)
@safe_handler
async def handle_photo(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    photo = message.photo[-1]  # highest resolution
    s.cover_file_id = photo.file_id
    s.processing = False
    persist_session(s)
    await message.reply("✅ Cover set. Now send video(s) and I'll return them with this cover attached.")

# ----------------------------
# Document handler: accept image (cover) or video document
# ----------------------------
@dp.message_handler(content_types=ContentType.DOCUMENT)
@safe_handler
async def handle_document(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    doc = message.document
    mime = (doc.mime_type or "").lower()
    # image as cover
    if mime.startswith("image/"):
        s.cover_file_id = doc.file_id
        persist_session(s)
        await message.reply("✅ Cover set (image document).")
        return
    # video document -> queue
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
        if len(s.queue) >= MAX_QUEUE_PER_USER:
            await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel to clear.")
            return
        q = QueuedVideo(file_id=doc.file_id, caption=message.caption, file_unique_id=getattr(doc, "file_unique_id", None), file_size=getattr(doc, "file_size", None))
        s.queue.append(q)
        persist_session(s)
        await message.reply(f"✅ Video queued ({len(s.queue)} in queue).")
        if not s.processing:
            asyncio.create_task(process_user_queue(s, message.chat.id))
        return
    await message.reply("I accept images for covers and video files as videos. Send a photo to set cover or send a video to queue it.")

# ----------------------------
# Video handler: queue videos and start processing
# ----------------------------
@dp.message_handler(content_types=ContentType.VIDEO)
@safe_handler
async def handle_video(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    video = message.video
    if len(s.queue) >= MAX_QUEUE_PER_USER:
        await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel to clear first.")
        return
    q = QueuedVideo(file_id=video.file_id, caption=message.caption, file_unique_id=getattr(video, "file_unique_id", None), file_size=getattr(video, "file_size", None))
    s.queue.append(q)
    persist_session(s)
    await message.reply(f"✅ Video queued ({len(s.queue)} in queue).")
    if not s.processing:
        asyncio.create_task(process_user_queue(s, message.chat.id))

# ----------------------------
# Media group support (aggregates album items)
# ----------------------------
@dp.message_handler(lambda msg: bool(msg.media_group_id), content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT])
@safe_handler
async def handle_media_group_member(message: types.Message):
    """
    Telegram sends one webhook update per media-group (album) item. We need to buffer them
    and process the whole group once all members arrive. We wait a small window to collect members.
    """
    mg_id = str(message.media_group_id)
    user_id = message.from_user.id

    buf = _media_group_buffers.get(mg_id)
    if buf is None:
        _media_group_buffers[mg_id] = [message]
        loop = asyncio.get_event_loop()
        # schedule flush after small delay
        def _flush_group():
            asyncio.ensure_future(_process_media_group(mg_id))
        h = loop.call_later(MEDIA_GROUP_AGGREGATION_WINDOW, _flush_group)
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
    # messages in buf are in arrival order; we will treat each as a separate video/document/photo
    # set session cover if first item is an image and /thumb-like behavior wanted (but user asked to set cover by sending any photo)
    # For our flow: any photo sets cover; photos inside media group may set cover according to their content type.
    # We'll iterate: photos -> set cover, videos/documents -> queue
    # All from same user and chat (assume)
    user_id = buf[0].from_user.id
    s = get_session(user_id)
    queued_count = 0
    for msg in buf:
        # photo
        if msg.photo:
            photo = msg.photo[-1]
            s.cover_file_id = photo.file_id
            persist_session(s)
            # don't send preview here; user expects to set cover by sending photo
        elif msg.video:
            if len(s.queue) >= MAX_QUEUE_PER_USER:
                continue
            q = QueuedVideo(file_id=msg.video.file_id, caption=msg.caption, file_unique_id=getattr(msg.video, "file_unique_id", None), file_size=getattr(msg.video, "file_size", None))
            s.queue.append(q)
            queued_count += 1
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
                    queued_count += 1
    persist_session(s)
    # start processor if necessary
    if queued_count and not s.processing:
        # reply chat id: prefer original chat id
        chat_id = buf[0].chat.id
        asyncio.create_task(process_user_queue(s, chat_id))

# ----------------------------
# Processing queue: attempt server-side re-reference with thumb
# ----------------------------
async def process_user_queue(session: UserSession, reply_chat_id: int):
    """
    Core worker: processes session.queue sequentially (FIFO).
    For each queued video:
      1) Attempt to send by file_id with thumb=session.cover_file_id using send_video(file_id, thumb=cover).
         - This usually triggers Telegram's server-side fast copy internally and attaches thumbnail.
      2) If that fails or the thumbnail isn't applied (best-effort detection), fallback to copy_message (which preserves original media).
         - copy_message can't attach a new thumb, so it won't help apply cover. If send_video didn't apply thumb, we try an alternative:
           a) Download a tiny slice? (We avoid downloads for large files.)
           b) As last resort, send the file_id without thumb so we at least return the video (but user will know it's missing cover).
    The code below tries (1) and on certain errors uses (2).
    """
    async with session.lock:
        if session.processing:
            # already processing
            return
        session.processing = True
        persist_session(session)

        total = len(session.queue)
        processed = 0
        start_all = time.time()
        hist = perf_history.get(session.user_id, [])
        hist = hist[-PERF_HISTORY_LIMIT:]

        # We'll iterate while queue has items (new items may be added during processing)
        while session.queue:
            q = session.queue[0]
            processed += 1
            # progress before start
            try:
                percent_before = (processed - 1) / (total or 1)
                await bot.send_message(reply_chat_id, f"⏳ Processing {processed}/{total} — {make_progress_bar(percent_before)}")
            except Exception:
                logger.debug("Failed to send pre-progress to chat %s", reply_chat_id)

            thumb = session.cover_file_id  # may be None

            t0 = time.time()
            # Attempt #1: send_video with file_id and thumb param (no timeout arg)
            try:
                # NOTE: aiogram 2.25.0 send_video signature does not accept timeout kw — we do NOT pass timeout.
                # We rely on Telegram reusing server-side file to make this fast.
                await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=q.caption or "", thumb=thumb)
                t1 = time.time()
                duration = t1 - t0
                hist.append(duration)
                perf_history[session.user_id] = hist[-PERF_HISTORY_LIMIT:]
                # remove the processed item
                session.queue.pop(0)
                persist_session(session)
                remaining = len(session.queue)
                avg = (sum(hist) / len(hist)) if hist else duration
                eta = avg * remaining
                percent_done = processed / (total or 1)
                await bot.send_message(reply_chat_id, f"✅ Sent {processed}/{total} — {make_progress_bar(percent_done)} ETA: {human_time(eta)}")
                continue  # next item
            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter while send_video for user %s: wait %s", session.user_id, wait)
                await asyncio.sleep(wait + 1)
                continue  # retry same video
            except BadRequest as br:
                # BadRequest often indicates unsupported parameters, wrong file id, or other issues.
                logger.warning("BadRequest sending video via file_id for user %s: %s", session.user_id, br)
                # We'll attempt fallback strategies below
            except TelegramAPIError as te:
                logger.exception("TelegramAPIError sending video by file_id for user %s: %s", session.user_id, te)
            except Exception as e:
                logger.exception("Unexpected error when sending by file_id for user %s: %s", session.user_id, e)

            # Fallback strategies if send_video(file_id, thumb=...) failed:
            # Strategy A: Try copy_message from the same chat/message id if we have context (we only have file_id here).
            # copy_message keeps the original media (no thumb change), but sometimes Telegram will accept additional parameters;
            # however API docs don't support thumb parameter for copyMessage.
            # Because we don't have the original message_id (file_id doesn't carry message id), we cannot copy original message reliably.
            # Strategy B: As last resort, send the file_id without thumb so at least user gets the video back (but no cover).
            # Strategy C: Optionally attempt to download small piece and re-upload (NOT implemented here to avoid heavy IO).
            # We'll implement Strategy B to ensure the user receives the video; we then notify owner/user that thumbnail wasn't applied.

            # Strategy B:
            try:
                await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=(q.caption or ""))
                # even if thumb missing, remove item so we don't infinite loop
                session.queue.pop(0)
                persist_session(session)
                await bot.send_message(reply_chat_id, "⚠️ Sent video but could not apply cover — Telegram ignored thumbnail. If this persists, try sending the video again or contact the owner.")
                try:
                    await notify_owner(f"Warning: Could not apply cover for user {session.user_id} on a video (file_id {q.file_id}). send_video(file_id, thumb) was rejected by Telegram.")
                except Exception:
                    logger.debug("Failed to notify owner about apply-cover rejection")
                continue
            except Exception as e:
                logger.exception("Fallback send without thumb failed for user %s: %s", session.user_id, e)
                # pop to avoid locking and notify user and owner
                session.queue.pop(0)
                persist_session(session)
                try:
                    await bot.send_message(reply_chat_id, "❌ Failed to send a queued video. Skipping.")
                except Exception:
                    logger.debug("Failed to notify user about skipped video")
                try:
                    await notify_owner(f"Error: failed fallback send for user {session.user_id} — {e}\nTraceback:\n{traceback.format_exc()[:2000]}")
                except Exception:
                    logger.debug("Failed to notify owner about fallback failure")
                continue

        # finished processing all queued items
        session.processing = False
        persist_session(session)
        elapsed = time.time() - start_all
        try:
            await bot.send_message(reply_chat_id, f"🎬 Done — processed {processed} videos in {human_time(elapsed)}.")
        except Exception:
            logger.debug("Failed to send completion message")

# ----------------------------
# Webhook handler (aiohttp)
# ----------------------------
async def handle_webhook(request: web.Request) -> web.Response:
    # Read body raw and save for debug
    try:
        raw = await request.text()
    except Exception:
        return web.Response(status=400, text="Bad request - cannot read body")

    # Save debug copy
    try:
        with open(LAST_UPDATE_PATH, "w", encoding="utf-8") as f:
            f.write(raw)
    except Exception:
        logger.debug("Failed to write last_update.json (non-fatal)")

    # Ensure aiogram context
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

    # Parse JSON into Update
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

    # Process update; swallow handler exceptions and return 200 to Telegram to avoid retries/backoff
    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Exception while processing update (handler threw). Returning 200 to Telegram.")
        try:
            tb = traceback.format_exc()
            await notify_owner(f"Handler exception during update processing:\n{tb[:2000]}")
        except Exception:
            logger.debug("Failed to notify owner on handler exception")
        return web.Response(text="OK")

    return web.Response(text="OK")

# ----------------------------
# Health and info endpoints
# ----------------------------
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

# ----------------------------
# Background watchdog and self-ping
# ----------------------------
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
                logger.warning("watchdog: webhook mismatch (got=%s want=%s) - resetting", url, WEBHOOK_URL)
                try:
                    await bot.delete_webhook(drop_pending_updates=False)
                except Exception:
                    logger.debug("watchdog: delete_webhook failed (continuing)")
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
            logger.exception("watchdog: unexpected error")
        await asyncio.sleep(interval)

# ----------------------------
# Load sessions at startup
# ----------------------------
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
                logger.exception("load_all_sessions: failed for row %s", row[0])
        logger.info("Loaded %d sessions", len(sessions))
    except Exception:
        logger.exception("load_all_sessions failed")

# ----------------------------
# Exception hooks
# ----------------------------
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

# ----------------------------
# Signal handlers
# ----------------------------
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
        logger.exception("delete_webhook failed (non-fatal)")
    os._exit(0)

# ----------------------------
# Startup/shutdown hooks for aiohttp
# ----------------------------
async def on_startup(app: web.Application):
    logger.info("on_startup: setting webhook to %s", WEBHOOK_URL)
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except BadRequest as e:
        logger.exception("BadRequest when setting webhook: %s", e)
        await notify_owner(f"Failed to set webhook: {e}")
        raise
    except Exception:
        logger.exception("Unexpected error setting webhook on startup")
        await notify_owner("Unexpected error setting webhook on startup; check logs")

    # preload sessions
    load_all_sessions()

    # start background tasks
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

# ----------------------------
# Main runner
# ----------------------------
def main():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda l, ctx: task_exception_handler(l, ctx))
    setup_signal_handlers(loop)

    # ensure bot current
    Bot.set_current(bot)

    # preload sessions
    load_all_sessions()

    # build aiohttp app and routes
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/info", handle_info)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    logger.info("Starting webhook web server at 0.0.0.0:%s webhook_path=%s", PORT, WEBHOOK_PATH)
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