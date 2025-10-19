#!/usr/bin/env python3
# bot.py
# Webhook-only "video cover" bot
# - aiogram==2.25.0
# - aiohttp==3.8.6
#
# Behavior:
#  - Send a photo -> sets it as your session cover (no command required)
#  - /show_cover -> shows your current cover (if any)
#  - /del_cover -> deletes your cover
#  - Send video(s) -> bot re-sends each video (in received order) with your cover attached
#  - /cancel -> cancel queued videos for your session
#  - Queue processed FIFO, one video at a time per user session
#  - Webhook mode, watchdog, self-ping, persistent per-user cover (SQLite)
#
# Save as bot.py and run with required env:
# BOT_TOKEN, OWNER_ID, WEBHOOK_HOST (https://...), PORT (10000), WEBHOOK_PATH (optional)
#
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
from typing import Dict, List, Optional
from dataclasses import dataclass, field
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
# File paths and persistence
# ---------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "videos_cover_bot.sqlite"
LAST_UPDATE_PATH = DATA_DIR / "last_update.json"

for d in (DATA_DIR, LOGS_DIR):
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
logger = logging.getLogger("videos_cover_bot")

# ---------------------------
# Tunables and constants
# ---------------------------
MAX_QUEUE_PER_USER = 500            # generous upper bound for queued videos
SEND_TIMEOUT = 300                  # seconds for bot.send_video
SELF_PING_INTERVAL = 60             # seconds
WATCHDOG_INTERVAL = 120             # seconds
PROGRESS_BAR_LEN = 24
PERF_HISTORY_LIMIT = 8

# ---------------------------
# Dataclasses
# ---------------------------
@dataclass
class QueuedVideo:
    file_id: str
    caption: Optional[str]
    file_unique_id: Optional[str] = None
    file_size: Optional[int] = None
    ts_added: int = field(default_factory=lambda: int(time.time()))

    def to_json(self):
        return {
            "file_id": self.file_id,
            "caption": self.caption,
            "file_unique_id": self.file_unique_id,
            "file_size": self.file_size,
            "ts_added": self.ts_added,
        }

    @staticmethod
    def from_json(obj):
        return QueuedVideo(
            file_id=obj.get("file_id"),
            caption=obj.get("caption"),
            file_unique_id=obj.get("file_unique_id"),
            file_size=obj.get("file_size"),
            ts_added=obj.get("ts_added", int(time.time()))
        )

@dataclass
class UserSession:
    user_id: int
    cover_file_id: Optional[str] = None   # telegram file_id of the cover image
    queue: List[QueuedVideo] = field(default_factory=list)
    processing: bool = False

    # runtime lock (not persisted)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def to_json(self):
        return {
            "user_id": self.user_id,
            "cover_file_id": self.cover_file_id,
            "queue": [q.to_json() for q in self.queue],
            "processing": self.processing,
        }

    @staticmethod
    def from_json(obj):
        s = UserSession(user_id=obj["user_id"])
        s.cover_file_id = obj.get("cover_file_id")
        s.queue = [QueuedVideo.from_json(q) for q in obj.get("queue", [])]
        s.processing = obj.get("processing", False)
        return s

# ---------------------------
# DB (sqlite) helpers
# ---------------------------
def init_db():
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

def save_session_to_db(session: UserSession):
    try:
        ts = int(time.time())
        cur = db.cursor()
        cur.execute("INSERT OR REPLACE INTO sessions (user_id, json, updated_at) VALUES (?, ?, ?)",
                    (session.user_id, json.dumps(session.to_json()), ts))
        db.commit()
    except Exception:
        logger.exception("save_session_to_db failed for %s", session.user_id)

def load_session_from_db(user_id: int) -> Optional[UserSession]:
    try:
        cur = db.cursor()
        cur.execute("SELECT json FROM sessions WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        return UserSession.from_json(json.loads(row[0]))
    except Exception:
        logger.exception("load_session_from_db failed for %s", user_id)
        return None

def delete_session_from_db(user_id: int):
    try:
        cur = db.cursor()
        cur.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        db.commit()
    except Exception:
        logger.exception("delete_session_from_db failed for %s", user_id)

# ---------------------------
# Bot & Dispatcher
# ---------------------------
bot = Bot(token=BOT_TOKEN)
# ensure current bot is set (fixes context in webhook handlers)
Bot.set_current(bot)

dp = Dispatcher(bot)

# ---------------------------
# Runtime state
# ---------------------------
sessions: Dict[int, UserSession] = {}  # in-memory cache
perf_history: Dict[int, List[float]] = {}  # per-user recent send durations

# ---------------------------
# Utilities
# ---------------------------
def human_time(secs: float) -> str:
    secs = int(round(secs))
    if secs < 60:
        return f"{secs}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m{s}s".replace("m0s", "m0s")
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h}h{m}m"
    days, h = divmod(h, 24)
    return f"{days}d{h}h"

def progress_bar(percent: float, length: int = PROGRESS_BAR_LEN) -> str:
    percent = max(0.0, min(1.0, percent))
    filled = int(round(length * percent))
    return "▰" * filled + "▱" * (length - filled)

def get_session(user_id: int) -> UserSession:
    s = sessions.get(user_id)
    if not s:
        obj = load_session_from_db(user_id)
        if obj:
            s = obj
        else:
            s = UserSession(user_id=user_id)
        s.lock = asyncio.Lock()
        sessions[user_id] = s
    return s

def persist_session(session: UserSession):
    try:
        save_session_to_db(session)
    except Exception:
        logger.exception("persist_session failed for %s", session.user_id)

async def notify_owner(text: str):
    try:
        await bot.send_message(OWNER_ID, text)
    except Exception:
        logger.exception("notify_owner failed")

# ---------------------------
# Access decorator (public bot)
# ---------------------------
def safe_handler(func):
    @wraps(func)
    async def inner(message: types.Message, *args, **kwargs):
        # ensure bot context for handlers under webhook mode
        Bot.set_current(bot)
        try:
            return await func(message, *args, **kwargs)
        except Exception:
            logger.exception("Unhandled exception in handler %s", func.__name__)
            # don't crash — notify owner with short traceback
            tb = traceback.format_exc()
            try:
                await notify_owner(f"Handler {func.__name__} failed:\n{tb[:2000]}")
            except Exception:
                pass
            # respond with a polite error message but don't expose details
            try:
                await message.reply("⚠️ An internal error occurred. The owner has been notified.")
            except Exception:
                logger.debug("Failed to send error reply to user")
    return inner

# ---------------------------
# Commands
# ---------------------------
HELP = (
    "📽️ *Video Cover Bot*\n\n"
    "• Send a *photo* to set it as your cover (no command needed).\n"
    "• Send a *video* (or video file) — bot will return it with your cover attached.\n"
    "• /show_cover — see your current cover.\n"
    "• /del_cover — delete your cover.\n"
    "• /cancel — clear queued videos for your session.\n"
)

@dp.message_handler(commands=["start", "help"])
@safe_handler
async def cmd_start(message: types.Message):
    await message.reply(HELP, parse_mode="Markdown")

@dp.message_handler(commands=["show_cover"])
@safe_handler
async def cmd_show_cover(message: types.Message):
    s = get_session(message.from_user.id)
    if not s.cover_file_id:
        await message.reply("❗ You don't have a cover set. Send a photo to set your cover.")
        return
    try:
        await bot.send_photo(chat_id=message.chat.id, photo=s.cover_file_id, caption="📸 Your current cover")
    except Exception:
        # fallback: notify
        logger.exception("Failed to send cover preview to %s", message.from_user.id)
        await message.reply("❗ Failed to show cover. It may be expired or invalid. Try sending a new photo.")

@dp.message_handler(commands=["del_cover"])
@safe_handler
async def cmd_del_cover(message: types.Message):
    s = get_session(message.from_user.id)
    if not s.cover_file_id:
        await message.reply("❗ No cover to delete.")
        return
    s.cover_file_id = None
    persist_session(s)
    await message.reply("🗑️ Your cover has been deleted. Send a photo to set a new one.")

@dp.message_handler(commands=["cancel"])
@safe_handler
async def cmd_cancel(message: types.Message):
    s = get_session(message.from_user.id)
    async with s.lock:
        qlen = len(s.queue)
        s.queue = []
        s.processing = False
        persist_session(s)
    await message.reply(f"🗑️ Cleared {qlen} queued video(s) for your session.")

# ---------------------------
# Photo handler: set cover
# ---------------------------
@dp.message_handler(content_types=ContentType.PHOTO)
@safe_handler
async def handle_photo(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    # pick highest-res photo
    photo = message.photo[-1]
    s.cover_file_id = photo.file_id
    s.processing = False  # safe default
    persist_session(s)
    await message.reply("✅ Cover set. Now send a video and I'll return it with this cover applied. Use /show_cover to preview or /del_cover to delete.")

# ---------------------------
# Document handler: accept image-as-document (cover) or video-as-document
# ---------------------------
@dp.message_handler(content_types=ContentType.DOCUMENT)
@safe_handler
async def handle_document(message: types.Message):
    user_id = message.from_user.id
    doc = message.document
    mime = (doc.mime_type or "").lower()
    s = get_session(user_id)
    # image document => cover
    if mime.startswith("image/"):
        s.cover_file_id = doc.file_id
        persist_session(s)
        await message.reply("✅ Cover set (from image document). Send video(s) and I'll attach this cover.")
        return
    # video document => queue as video
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
        if len(s.queue) >= MAX_QUEUE_PER_USER:
            await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Process or /cancel first.")
            return
        q = QueuedVideo(
            file_id=doc.file_id,
            caption=message.caption,
            file_unique_id=getattr(doc, "file_unique_id", None),
            file_size=getattr(doc, "file_size", None)
        )
        s.queue.append(q)
        persist_session(s)
        await message.reply(f"✅ Video queued ({len(s.queue)} in queue). I will send it with your cover when processing.")
        # start processor if not running
        if not s.processing:
            asyncio.create_task(process_user_queue(s, message.chat.id))
        return
    await message.reply("I accept image documents as covers and video files as videos. Send a photo to set cover or send a video to queue it.")

# ---------------------------
# Video handler: queue and process
# ---------------------------
@dp.message_handler(content_types=ContentType.VIDEO)
@safe_handler
async def handle_video(message: types.Message):
    user_id = message.from_user.id
    s = get_session(user_id)
    video = message.video
    if len(s.queue) >= MAX_QUEUE_PER_USER:
        await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Process or /cancel first.")
        return
    q = QueuedVideo(
        file_id=video.file_id,
        caption=message.caption,
        file_unique_id=getattr(video, "file_unique_id", None),
        file_size=getattr(video, "file_size", None),
    )
    s.queue.append(q)
    persist_session(s)
    await message.reply(f"✅ Video queued ({len(s.queue)} in queue). I will send it with your cover when processing.")
    if not s.processing:
        asyncio.create_task(process_user_queue(s, message.chat.id))

# ---------------------------
# Core: process user queue sequentially
# ---------------------------
async def process_user_queue(session: UserSession, reply_chat_id: int):
    async with session.lock:
        # if already processing elsewhere, exit
        if session.processing:
            return
        session.processing = True
        persist_session(session)

        total_start = len(session.queue)
        processed = 0
        start_all = time.time()
        hist = perf_history.get(session.user_id, [])

        # work on a snapshot to handle new incoming videos appended during processing
        while session.queue:
            q = session.queue[0]  # FIFO
            processed += 1
            # progress message
            try:
                percent = (processed - 1) / (total_start or 1)
                await bot.send_message(reply_chat_id, f"⏳ Starting {processed}/{total_start} — {progress_bar(percent)}")
            except Exception:
                logger.debug("Failed to send progress start message")

            # prepare thumb param
            thumb_param = session.cover_file_id

            t0 = time.time()
            try:
                # Try sending by file_id with provided thumb param.
                # This is usually fast since it avoids a local download.
                await bot.send_video(chat_id=reply_chat_id, video=q.file_id, caption=q.caption or "", thumb=thumb_param, timeout=SEND_TIMEOUT)
                t1 = time.time()
                duration = t1 - t0
                hist.append(duration)
                perf_history[session.user_id] = hist[-PERF_HISTORY_LIMIT:]
                # pop finished
                session.queue.pop(0)
                persist_session(session)
                remaining = len(session.queue)
                avg = (sum(hist) / len(hist)) if hist else duration
                eta = avg * remaining
                percent_done = processed / (total_start or 1)
                await bot.send_message(reply_chat_id, f"✅ Sent {processed}/{total_start} — {progress_bar(percent_done)} ETA: {human_time(eta)}")
            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter while sending video for user %s: wait %s", session.user_id, wait)
                # don't remove from queue, sleep then retry
                await asyncio.sleep(wait + 1)
                continue
            except TelegramAPIError as te:
                logger.exception("TelegramAPIError when sending video for user %s: %s", session.user_id, te)
                # notify user that sending failed for this video, then pop to avoid infinite loop
                try:
                    await bot.send_message(reply_chat_id, f"❌ Failed to send a video: {te}. Skipping it.")
                except Exception:
                    logger.debug("Failed to notify user about send error")
                session.queue.pop(0)
                persist_session(session)
                continue
            except Exception as e:
                logger.exception("Unexpected error sending video for user %s: %s", session.user_id, e)
                # as last resort notify user and pop to avoid locking
                try:
                    await bot.send_message(reply_chat_id, f"❌ Unexpected error sending a video. Skipping. Owner notified.")
                    await notify_owner(f"Unexpected error sending user {session.user_id} video: {e}\n{traceback.format_exc()[:2000]}")
                except Exception:
                    pass
                session.queue.pop(0)
                persist_session(session)
                continue

        # finished processing
        session.processing = False
        persist_session(session)
        elapsed = time.time() - start_all
        try:
            await bot.send_message(reply_chat_id, f"🎬 All done. Processed {processed} video(s) in {human_time(elapsed)}.")
        except Exception:
            logger.debug("Could not send final completion message to user")

# ---------------------------
# Webhook handler & debug
# ---------------------------
async def handle_webhook(request):
    # read raw body (and save for debugging)
    try:
        raw = await request.text()
    except Exception:
        return web.Response(status=400, text="Bad request - cannot read body")

    try:
        with open(LAST_UPDATE_PATH, "w", encoding="utf-8") as f:
            f.write(raw)
    except Exception:
        logger.debug("Could not write last_update.json (non-fatal)")

    # ensure bot context for webhook processing
    try:
        Bot.set_current(bot)
        # attempt dispatcher.set_current if available (some aiogram builds)
        try:
            Dispatcher.set_current(dp)  # type: ignore[attr-defined]
        except Exception:
            # fallback
            try:
                dp._bot = bot  # type: ignore[attr-defined]
            except Exception:
                pass
    except Exception:
        logger.exception("Failed to set current bot/dispatcher context")

    # parse update
    try:
        data = json.loads(raw)
    except Exception:
        return web.Response(status=400, text="Bad request - invalid json")

    try:
        # use de_json to bind properly
        update = types.Update.de_json(data, bot)
    except Exception:
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse Update")
            return web.Response(status=400, text="Bad request - invalid update")

    # Process update but swallow uncaught handler exceptions and return 200 to Telegram (prevents backoff)
    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Exception during dp.process_update (handler error)")
        # inform owner (short)
        try:
            tb = traceback.format_exc()
            await notify_owner(f"Handler error: {tb[:1500]}")
        except Exception:
            pass
        # return 200 OK to Telegram
        return web.Response(text="OK")
    return web.Response(text="OK")

# ---------------------------
# Health endpoints
# ---------------------------
async def handle_health(request):
    return web.json_response({
        "status": "ok",
        "owner_id": OWNER_ID,
        "sessions_loaded": len(sessions),
    })

async def handle_info(request):
    return web.json_response({
        "status": "ok",
        "owner_id": OWNER_ID,
        "sessions_loaded": len(sessions),
    })

# ---------------------------
# Self-ping and watchdog
# ---------------------------
async def self_ping_task(app, interval: int = SELF_PING_INTERVAL):
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
                    logger.debug("self_ping: /health status %s", resp.status)
        except Exception:
            logger.debug("self_ping: health check failed (non-fatal)")
        await asyncio.sleep(interval)

async def webhook_watchdog(app, interval: int = WATCHDOG_INTERVAL):
    await asyncio.sleep(5)
    while True:
        try:
            info = await bot.get_webhook_info()
            url = getattr(info, "url", None)
            if not url or url.rstrip("/") != WEBHOOK_URL.rstrip("/"):
                logger.warning("watchdog: webhook mismatch (got=%s want=%s). resetting.", url, WEBHOOK_URL)
                try:
                    await bot.delete_webhook(drop_pending_updates=False)
                except Exception:
                    logger.debug("watchdog: delete_webhook failed (continuing)")
                try:
                    await bot.set_webhook(WEBHOOK_URL)
                    logger.info("watchdog: webhook re-set to %s", WEBHOOK_URL)
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
# Load sessions into memory startup
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
        logger.info("Loaded %d sessions from DB", len(sessions))
    except Exception:
        logger.exception("load_all_sessions failed")

# ---------------------------
# Exception handlers
# ---------------------------
def excepthook(exc_type, exc, tb):
    tbtext = "".join(traceback.format_exception(exc_type, exc, tb))
    logger.critical("Uncaught exception: %s", tbtext)
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(notify_owner(f"Uncaught exception: {tbtext[:2000]}"))
    except Exception:
        pass

sys.excepthook = excepthook

def task_exception_handler(loop, context):
    msg = context.get("exception", context.get("message"))
    logger.error("Asyncio task exception: %s", msg)
    try:
        loop.create_task(notify_owner(f"Task exception: {msg}"))
    except Exception:
        pass

# ---------------------------
# Startup/shutdown hooks
# ---------------------------
async def on_startup(app):
    logger.info("on_startup: setting webhook to %s", WEBHOOK_URL)
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except BadRequest as e:
        logger.exception("BadRequest setting webhook: %s", e)
        await notify_owner(f"BadRequest setting webhook: {e}")
        raise
    except Exception:
        logger.exception("Unexpected error setting webhook")
        await notify_owner("Unexpected error setting webhook")

    load_all_sessions()
    app["self_ping"] = asyncio.create_task(self_ping_task(app))
    app["watchdog"] = asyncio.create_task(webhook_watchdog(app))
    logger.info("Background tasks started")

async def on_shutdown(app):
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
# Signal handlers
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
        pass
    os._exit(0)

# ---------------------------
# Main runner
# ---------------------------
def main():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda l, ctx: task_exception_handler(l, ctx))
    setup_signal_handlers(loop)
    # ensure Bot context
    Bot.set_current(bot)
    load_all_sessions()
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/info", handle_info)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    logger.info("Starting webhook webserver 0.0.0.0:%s webhook_path=%s", PORT, WEBHOOK_PATH)
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