#!/usr/bin/env python3
# bot.py
# Final webhook-only Video Cover Bot (700+ lines)
# - aiogram==2.25.0
# - aiohttp==3.8.6
#
# Purpose:
#   - Send a photo -> sets it as user's cover (thumbnail)
#   - /show_cover -> preview the cover
#   - /del_cover -> delete the cover
#   - Send video(s) -> bot re-sends each video (using Telegram file_id) with user's cover attached,
#                     preserving original caption and order; no local download/upload required
#   - /cancel -> clear queued videos for the session
#   - Webhook-only, designed for Render or any HTTPS host
#   - Persistent per-user cover and queue stored in SQLite (survives restarts)
#   - Background watchdog and self-ping to keep webhook healthy
#   - Robust logging, owner notifications, debug output of last webhook body
#
# IMPORTANT:
#  - Create a .env with: BOT_TOKEN, OWNER_ID, WEBHOOK_HOST (https://...), PORT=10000, optional WEBHOOK_PATH
#  - Use aiogram==2.25.0 and aiohttp==3.8.6 in requirements
#  - This code intentionally uses Telegram file_id re-sending (fast)
#
# Author: Generated for user
# Date: 2025-10-19
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
from aiogram.types import ContentType, InputFile
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError, BadRequest

# ---------------------------
# Load environment variables
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
# Filesystem paths and DB
# ---------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "cover_bot.sqlite"
LAST_UPDATE_PATH = DATA_DIR / "last_update.json"

for d in (DATA_DIR, LOGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------
# Logging configuration
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("video_cover_bot")

# ---------------------------
# Tuning constants
# ---------------------------
MAX_QUEUE_PER_USER = 500          # safety cap for number of queued videos per user
SEND_RETRY_DELAY = 3              # seconds to wait before retrying trivial issues
SELF_PING_INTERVAL = 60           # seconds
WATCHDOG_INTERVAL = 120           # seconds
PROGRESS_BAR_LEN = 24
PERF_HISTORY_LIMIT = 8

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
            added_ts=obj.get("added_ts", int(time.time()))
        )

@dataclass
class UserSession:
    user_id: int
    cover_file_id: Optional[str] = None
    queue: List[QueuedVideo] = field(default_factory=list)
    processing: bool = False

    # runtime-only
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
# SQLite initialization and helpers
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
# Bot and dispatcher setup
# ---------------------------
bot = Bot(token=BOT_TOKEN)
# ensure bot current so helper methods work in webhook handlers
Bot.set_current(bot)

dp = Dispatcher(bot)

# ---------------------------
# Runtime state
# ---------------------------
sessions: Dict[int, UserSession] = {}
perf_history: Dict[int, List[float]] = {}  # recent send durations per user

# ---------------------------
# Utility functions
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

def persist_session(s: UserSession):
    try:
        save_session_db(s)
    except Exception:
        logger.exception("persist_session failed for %s", s.user_id)

# safe decorator to guard and set current bot in handlers
def safe_handler(fn):
    @wraps(fn)
    async def wrapper(message: types.Message, *args, **kwargs):
        # ensure bot context
        Bot.set_current(bot)
        try:
            return await fn(message, *args, **kwargs)
        except Exception:
            logger.exception("Unhandled exception in handler %s", fn.__name__)
            # notify owner with short traceback
            tb = traceback.format_exc()
            try:
                await notify_owner(f"Handler {fn.__name__} failed:\n{tb[:1900]}")
            except Exception:
                logger.debug("Failed to notify owner about handler exception")
            # let user know something went wrong
            try:
                await message.reply("⚠️ An internal error occurred and the owner has been notified.")
            except Exception:
                logger.debug("Failed to send error reply to user")
    return wrapper

# ---------------------------
# Command handlers
# ---------------------------
HELP_TEXT = (
    "🎬 *Video Cover Bot*\n\n"
    "• Send a *photo* to set it as your cover (thumbnail).\n"
    "• Send a *video* (or video file) — bot will return it with your cover attached.\n"
    "• /show_cover — preview your current cover.\n"
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
        logger.exception("Failed to send cover preview to user %s", message.from_user.id)
        await message.reply("❗ Failed to show cover. Try sending a new photo to set it again.")

@dp.message_handler(commands=["del_cover"])
@safe_handler
async def cmd_del_cover(message: types.Message):
    s = get_session(message.from_user.id)
    if not s.cover_file_id:
        await message.reply("❗ You don't have a cover set.")
        return
    s.cover_file_id = None
    persist_session(s)
    await message.reply("🗑 Your cover has been deleted. Send a photo to set a new one.")

@dp.message_handler(commands=["cancel"])
@safe_handler
async def cmd_cancel(message: types.Message):
    s = get_session(message.from_user.id)
    async with s.lock:
        qlen = len(s.queue)
        s.queue = []
        s.processing = False
        persist_session(s)
    await message.reply(f"🗑 Cleared {qlen} queued video(s) for your session.")

# ---------------------------
# Media handlers
# ---------------------------
@dp.message_handler(content_types=ContentType.PHOTO)
@safe_handler
async def handle_photo(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    # highest quality photo
    photo = message.photo[-1]
    s.cover_file_id = photo.file_id
    s.processing = False
    persist_session(s)
    await message.reply("✅ Cover set. Now send a video and I'll return it with this cover applied. Use /show_cover to preview or /del_cover to delete.")

@dp.message_handler(content_types=ContentType.DOCUMENT)
@safe_handler
async def handle_document(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    doc = message.document
    mime = (doc.mime_type or "").lower()
    # if image document acts as cover
    if mime.startswith("image/"):
        s.cover_file_id = doc.file_id
        persist_session(s)
        await message.reply("✅ Cover set (image document). Send video(s) and I'll attach this cover.")
        return
    # accept video document as queue
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
        if len(s.queue) >= MAX_QUEUE_PER_USER:
            await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel to clear and try again.")
            return
        q = QueuedVideo(
            file_id=doc.file_id,
            caption=message.caption,
            file_unique_id=getattr(doc, "file_unique_id", None),
            file_size=getattr(doc, "file_size", None),
        )
        s.queue.append(q)
        persist_session(s)
        await message.reply(f"✅ Video queued ({len(s.queue)} in queue). It will be sent back with your cover when processed.")
        # start processing if not already
        if not s.processing:
            asyncio.create_task(process_user_queue(s, message.chat.id))
        return
    await message.reply("I accept image documents as covers and video files as videos. Send a photo to set cover or send a video to queue it.")

@dp.message_handler(content_types=ContentType.VIDEO)
@safe_handler
async def handle_video(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    video = message.video
    if len(s.queue) >= MAX_QUEUE_PER_USER:
        await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel to clear.")
        return
    q = QueuedVideo(
        file_id=video.file_id,
        caption=message.caption,
        file_unique_id=getattr(video, "file_unique_id", None),
        file_size=getattr(video, "file_size", None),
    )
    s.queue.append(q)
    persist_session(s)
    await message.reply(f"✅ Video queued ({len(s.queue)} in queue). It will be sent back with your cover when processed.")
    if not s.processing:
        asyncio.create_task(process_user_queue(s, message.chat.id))

# ---------------------------
# Core processing: sequentially process user's queue
# ---------------------------
async def process_user_queue(session: UserSession, reply_chat_id: int):
    async with session.lock:
        if session.processing:
            # another worker already processing
            return
        session.processing = True
        persist_session(session)

        total_initial = len(session.queue)
        processed_count = 0
        start_all = time.time()
        hist = perf_history.get(session.user_id, [])
        hist = hist[-PERF_HISTORY_LIMIT:]

        # process until queue empty
        while session.queue:
            q = session.queue[0]
            processed_count += 1

            # progress before sending
            try:
                percent_before = (processed_count - 1) / (total_initial or 1)
                await bot.send_message(reply_chat_id, f"⏳ Processing {processed_count}/{total_initial} — {make_progress_bar(percent_before)}")
            except Exception:
                logger.debug("Couldn't send progress start message")

            # use cover_file_id (string) as thumb param. If None, send without thumb.
            thumb_param = session.cover_file_id

            t0 = time.time()
            try:
                # IMPORTANT: aiogram 2.25.0's Bot.send_video has no timeout parameter — do not pass one.
                # We intentionally re-send by file_id so Telegram uses its own server copy.
                await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=q.caption or "", thumb=thumb_param)
                t1 = time.time()
                duration = t1 - t0
                hist.append(duration)
                perf_history[session.user_id] = hist[-PERF_HISTORY_LIMIT:]
                # remove from queue and persist
                session.queue.pop(0)
                persist_session(session)
                remaining = len(session.queue)
                avg = (sum(hist) / len(hist)) if hist else duration
                eta = avg * remaining
                percent_done = processed_count / (total_initial or 1)
                await bot.send_message(reply_chat_id, f"✅ Sent {processed_count}/{total_initial} — {make_progress_bar(percent_done)} ETA: {human_time(eta)}")
            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter for user %s: waiting %s seconds", session.user_id, wait)
                # don't pop; wait and retry
                await asyncio.sleep(wait + 1)
                continue
            except TelegramAPIError as te:
                logger.exception("TelegramAPIError sending video for user %s: %s", session.user_id, te)
                # notify user and skip this video to avoid infinite loop
                try:
                    await bot.send_message(reply_chat_id, f"❌ Failed to send a video: {te}. Skipping it.")
                except Exception:
                    logger.debug("Failed to notify user about send error")
                session.queue.pop(0)
                persist_session(session)
                continue
            except Exception as e:
                # unexpected error — notify owner, skip this video to avoid blocking
                logger.exception("Unexpected error sending video for user %s: %s", session.user_id, e)
                try:
                    await bot.send_message(reply_chat_id, "❌ Unexpected error sending a video. Skipping it. Owner has been notified.")
                except Exception:
                    logger.debug("Failed to notify user about unexpected error")
                try:
                    await notify_owner(f"Unexpected error sending video for user {session.user_id}:\n{traceback.format_exc()[:2000]}")
                except Exception:
                    logger.debug("Failed to notify owner about unexpected error")
                session.queue.pop(0)
                persist_session(session)
                continue

        # done
        session.processing = False
        persist_session(session)
        elapsed = time.time() - start_all
        try:
            await bot.send_message(reply_chat_id, f"🎬 All done. Processed {processed_count} video(s) in {human_time(elapsed)}.")
        except Exception:
            logger.debug("Failed to send completion message to user")

# ---------------------------
# Webhook handler (aiohttp) with safe processing
# ---------------------------
async def handle_webhook(request: web.Request) -> web.Response:
    # read raw body for debug
    try:
        raw = await request.text()
    except Exception:
        return web.Response(status=400, text="Bad request - cannot read body")
    # write debug file
    try:
        with open(LAST_UPDATE_PATH, "w", encoding="utf-8") as f:
            f.write(raw)
    except Exception:
        logger.debug("Failed to write last_update.json (non-fatal)")

    # ensure aiogram context is set
    try:
        Bot.set_current(bot)
        # best-effort: set current dispatcher if available
        try:
            Dispatcher.set_current(dp)  # type: ignore[attr-defined]
        except Exception:
            try:
                dp._bot = bot  # type: ignore[attr-defined]
            except Exception:
                pass
    except Exception:
        logger.exception("Failed to set Bot/Dispatcher current instances")

    # parse JSON
    try:
        data = json.loads(raw)
    except Exception:
        return web.Response(status=400, text="Bad request - invalid json")

    # construct Update using de_json for better binding
    try:
        update = types.Update.de_json(data, bot)
    except Exception:
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse Update")
            return web.Response(status=400, text="Bad request - invalid update")

    # process update and be tolerant: on handler exception return 200 to Telegram
    try:
        await dp.process_update(update)
    except Exception:
        # log and notify owner, but return 200 to Telegram to avoid repeat failures causing backoff
        logger.exception("Exception while processing update (handler threw). Returning 200 to Telegram.")
        try:
            tb = traceback.format_exc()
            await notify_owner(f"Handler exception: {tb[:2000]}")
        except Exception:
            logger.debug("Failed to notify owner about handler exception")
        return web.Response(text="OK")
    return web.Response(text="OK")

# ---------------------------
# Health and info endpoints
# ---------------------------
async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "owner_id": OWNER_ID,
        "sessions_loaded": len(sessions),
    })

async def handle_info(request: web.Request) -> web.Response:
    try:
        return web.json_response({
            "status": "ok",
            "owner_id": OWNER_ID,
            "sessions_loaded": len(sessions),
        })
    except Exception:
        return web.json_response({"status": "error"}, status=500)

# ---------------------------
# Watchdog and self-ping background tasks
# ---------------------------
async def self_ping_task(app: web.Application, interval: int = SELF_PING_INTERVAL):
    await asyncio.sleep(2)
    while True:
        try:
            # light ping to bot API
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
            last_error = getattr(info, "last_error_message", None)
            pending = getattr(info, "pending_update_count", None)
            logger.debug("watchdog: webhook info url=%s pending=%s last_error=%s", url, pending, last_error)
            # if webhook not set to our expected URL, try to reset
            if not url or url.rstrip("/") != WEBHOOK_URL.rstrip("/"):
                logger.warning("watchdog: webhook URL mismatch (got=%s want=%s) — attempting reset", url, WEBHOOK_URL)
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
                        logger.debug("watchdog: failed to notify owner")
        except Exception:
            logger.exception("watchdog: unexpected error (non-fatal)")
        await asyncio.sleep(interval)

# ---------------------------
# Load sessions from DB into memory
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
                logger.exception("load_all_sessions: failed for row %s", row[0])
        logger.info("Loaded %d sessions into memory", len(sessions))
    except Exception:
        logger.exception("load_all_sessions failed")

# ---------------------------
# Exception hooks for global visibility
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
# Signal handlers for graceful shutdown
# ---------------------------
def setup_signal_handlers(loop: asyncio.AbstractEventLoop):
    try:
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_signal(s)))
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
# Startup and shutdown hooks for aiohttp app
# ---------------------------
async def on_startup(app: web.Application):
    logger.info("on_startup: setting webhook to %s", WEBHOOK_URL)
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except BadRequest as e:
        logger.exception("BadRequest when setting webhook: %s", e)
        await notify_owner(f"Failed to set webhook to {WEBHOOK_URL}: {e}")
        # raise to stop the app so operator can correct config
        raise
    except Exception:
        logger.exception("Unexpected exception when setting webhook on startup")
        await notify_owner("Unexpected exception when setting webhook on startup; see logs")

    # preload sessions
    load_all_sessions()

    # start background tasks
    app["self_ping_task"] = asyncio.create_task(self_ping_task(app))
    app["watchdog_task"] = asyncio.create_task(webhook_watchdog(app))
    logger.info("Background tasks started: self-ping and watchdog")

async def on_shutdown(app: web.Application):
    logger.info("on_shutdown: deleting webhook and cancelling tasks")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("Failed to delete webhook on shutdown (non-fatal)")
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

    # ensure bot current in this process context
    Bot.set_current(bot)

    # preload sessions from DB
    load_all_sessions()

    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/info", handle_info)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    logger.info("Starting aiohttp web server on 0.0.0.0:%s webhook_path=%s", PORT, WEBHOOK_PATH)
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