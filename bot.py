#!/usr/bin/env python3
# bot.py
# Thumbnail-only Video Bot (final)
# aiogram==2.25.0
# aiohttp==3.8.6
#
# This bot's sole job:
#   - Per session, accept a thumbnail (image)
#   - Accept multiple videos (1..99) in that session
#   - On /done, process videos in order, re-send each video using Telegram file_id
#     while attaching the session thumbnail so the video shows that thumbnail.
#   - No re-encoding, no large disk downloads (so very large files supported)
#   - Progress reporting (bar, completed/remaining, ETA)
#   - Owner + approved users (/adduser)
#   - Self-pinging /health endpoint for Render
#   - Webhook using aiohttp.web.run_app (not executor.start_webhook)
#
# Save as bot.py and run in an environment with:
#   aiogram==2.25.0
#   aiohttp==3.8.6
#   python-dotenv
#   Pillow (optional if we want to process thumbs locally)
#
# Make sure .env contains:
#   BOT_TOKEN=...
#   OWNER_ID=...
#   WEBHOOK_HOST=https://your-render-app.onrender.com
#   PORT=10000
#   (optional) WEBHOOK_PATH=/webhook
#
# Author: generated for user
# Date: 2025-10-16

import os
import sys
import json
import time
import math
import sqlite3
import asyncio
import logging
import traceback
import pathlib
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from functools import wraps
from contextlib import suppress

from dotenv import load_dotenv
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError, BadRequest
from aiogram.types import ContentType, InputFile

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN not set in environment")
    sys.exit(1)

OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
if not OWNER_ID_RAW:
    print("ERROR: OWNER_ID not set in environment")
    sys.exit(1)
try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    print("ERROR: OWNER_ID must be numeric")
    sys.exit(1)

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "").strip()
if not WEBHOOK_HOST:
    print("ERROR: WEBHOOK_HOST not set in environment (must be public HTTPS URL)")
    sys.exit(1)

PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "").strip() or "/webhook"
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH
WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

# ---------------------------
# Paths and persistence
# ---------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
SESSIONS_DIR = DATA_DIR / "sessions"
LOGS_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "bot_sessions.sqlite"
APPROVED_USERS_FILE = DATA_DIR / "approved_users.json"

for d in (DATA_DIR, SESSIONS_DIR, LOGS_DIR):
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
logger = logging.getLogger("thumb_bot_final")

# ---------------------------
# Constants
# ---------------------------
MAX_VIDEOS_PER_SESSION = 99
PROGRESS_BAR_LEN = 24
PERF_HISTORY_LIMIT = 8  # number of per-file durations to average for ETA
SELF_PING_INTERVAL = 60  # seconds

# ---------------------------
# Dataclasses
# ---------------------------

@dataclass
class VideoItem:
    file_id: str
    caption: Optional[str]
    order: int
    file_unique_id: Optional[str] = None
    file_size: Optional[int] = None

    def to_dict(self):
        return {
            "file_id": self.file_id,
            "caption": self.caption,
            "order": self.order,
            "file_unique_id": self.file_unique_id,
            "file_size": self.file_size,
        }

    @staticmethod
    def from_dict(d):
        return VideoItem(
            file_id=d.get("file_id"),
            caption=d.get("caption"),
            order=d.get("order", 0),
            file_unique_id=d.get("file_unique_id"),
            file_size=d.get("file_size"),
        )

@dataclass
class Session:
    user_id: int
    thumb_file_id: Optional[str] = None   # telegram file_id for thumbnail photo
    thumb_local_path: Optional[str] = None  # optional local processed thumb (not required)
    videos: List[VideoItem] = field(default_factory=list)
    expecting_thumb: bool = False
    processing: bool = False

    # non-persistent
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def to_json(self):
        return {
            "user_id": self.user_id,
            "thumb_file_id": self.thumb_file_id,
            "thumb_local_path": self.thumb_local_path,
            "videos": [v.to_dict() for v in self.videos],
            "expecting_thumb": self.expecting_thumb,
            "processing": self.processing,
        }

    @staticmethod
    def from_json(obj):
        s = Session(user_id=obj["user_id"])
        s.thumb_file_id = obj.get("thumb_file_id")
        s.thumb_local_path = obj.get("thumb_local_path")
        s.videos = [VideoItem.from_dict(v) for v in obj.get("videos", [])]
        s.expecting_thumb = obj.get("expecting_thumb", False)
        s.processing = obj.get("processing", False)
        return s

# ---------------------------
# DB (sqlite) for sessions
# ---------------------------

def init_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            user_id INTEGER PRIMARY KEY,
            session_json TEXT,
            updated_at INTEGER
        )
    """)
    conn.commit()
    return conn

db_conn = init_db()
db_lock = asyncio.Lock()

def save_session_db(user_id: int, session_json: dict):
    try:
        ts = int(time.time())
        cur = db_conn.cursor()
        cur.execute("INSERT OR REPLACE INTO sessions (user_id, session_json, updated_at) VALUES (?, ?, ?)",
                    (user_id, json.dumps(session_json), ts))
        db_conn.commit()
    except Exception:
        logger.exception("Failed to save session to DB for %s", user_id)

def load_session_db(user_id: int) -> Optional[dict]:
    try:
        cur = db_conn.cursor()
        cur.execute("SELECT session_json FROM sessions WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0])
    except Exception:
        logger.exception("Failed to load session DB for %s", user_id)
        return None

def delete_session_db(user_id: int):
    try:
        cur = db_conn.cursor()
        cur.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        db_conn.commit()
    except Exception:
        logger.exception("Failed to delete session DB for %s", user_id)

# ---------------------------
# Approved users persistence (simple JSON)
# ---------------------------
approved_users_lock = asyncio.Lock()

def load_approved_users() -> Dict[int, dict]:
    if APPROVED_USERS_FILE.exists():
        try:
            with open(APPROVED_USERS_FILE, "r", encoding="utf-8") as f:
                obj = json.load(f)
            return {int(k): v for k, v in obj.items()}
        except Exception:
            logger.exception("Failed to load approved users JSON")
            return {}
    else:
        # default: owner only
        return {OWNER_ID: {"username": None, "added_by": OWNER_ID, "added_at": int(time.time())}}

def save_approved_users(approved: Dict[int, dict]):
    try:
        with open(APPROVED_USERS_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in approved.items()}, f, indent=2)
    except Exception:
        logger.exception("Failed to save approved users JSON")

# ---------------------------
# Global runtime state
# ---------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

sessions: Dict[int, Session] = {}
approved_users: Dict[int, dict] = load_approved_users()
# ensure owner present
if OWNER_ID not in approved_users:
    approved_users[OWNER_ID] = {"username": None, "added_by": OWNER_ID, "added_at": int(time.time())}
    save_approved_users(approved_users)

# performance history for ETA (per-user)
perf_history: Dict[int, List[float]] = {}

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

def make_progress_bar(percent: float, length: int = PROGRESS_BAR_LEN) -> str:
    percent = max(0.0, min(1.0, percent))
    filled = int(round(length * percent))
    empty = length - filled
    return "▰" * filled + "▱" * empty

async def notify_owner(text: str):
    try:
        await bot.send_message(OWNER_ID, text)
    except Exception:
        logger.exception("Failed to notify owner")

def session_for_user(user_id: int) -> Session:
    s = sessions.get(user_id)
    if not s:
        obj = load_session_db(user_id)
        if obj:
            try:
                s = Session.from_json(obj)
            except Exception:
                logger.exception("Failed to load session JSON for %s", user_id)
                s = Session(user_id=user_id)
        else:
            s = Session(user_id=user_id)
        s.lock = asyncio.Lock()
        sessions[user_id] = s
    return s

def persist_session(session: Session):
    try:
        save_session_db(session.user_id, session.to_json())
    except Exception:
        logger.exception("Failed to persist session for %s", session.user_id)

# user approval decorator
def require_approved(func):
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        uid = message.from_user.id
        if uid not in approved_users:
            try:
                await message.reply("⛔️ You are not authorized to use this bot. Ask the owner to /adduser you.")
            except Exception:
                logger.debug("Couldn't notify unauthorized user %s", uid)
            return
        return await func(message, *args, **kwargs)
    return wrapper

def owner_only(func):
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.from_user.id != OWNER_ID:
            await message.reply("⛔️ Only the owner can use this command.")
            return
        return await func(message, *args, **kwargs)
    return wrapper

# ---------------------------
# Command Handlers
# ---------------------------

HELP_TEXT = """Thumbnail Session Video Bot — Help

Commands:
/start - show this help
/thumb - reply to an image with /thumb (or send /thumb then image) to set session thumbnail
/done - process queued videos (one-by-one). Each finished video will be sent immediately
/cancel - cancel session and clear queued videos
/adduser <id|@username> - owner only: approve a user
/removeuser <id|@username> - owner only: remove approved user
/users - owner only: view approved users
/session - owner only: inspect sessions
/health - HTTP endpoint for uptime monitors
"""

@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.reply("👋 Welcome!\n\n" + HELP_TEXT)

@dp.message_handler(commands=["thumb"])
@require_approved
async def cmd_thumb(message: types.Message):
    user_id = message.from_user.id
    s = session_for_user(user_id)
    # If user replied to photo, set immediately
    if message.reply_to_message and message.reply_to_message.photo:
        photo = message.reply_to_message.photo[-1]
        s.thumb_file_id = photo.file_id
        s.expecting_thumb = False
        s.thumb_local_path = None
        s.videos = []
        s.processing = False
        persist_session(s)
        await message.reply("✅ Thumbnail set for this session (from replied photo). Send videos now.")
        return
    # Otherwise enter expecting mode
    s.expecting_thumb = True
    s.thumb_file_id = None
    s.thumb_local_path = None
    s.videos = []
    s.processing = False
    persist_session(s)
    await message.reply("📸 Please send the thumbnail image now (photo or image document). This will be used for this session's videos.")

@dp.message_handler(commands=["cancel"])
@require_approved
async def cmd_cancel(message: types.Message):
    user_id = message.from_user.id
    # clear session
    sessions.pop(user_id, None)
    try:
        delete_session_db(user_id)
    except Exception:
        logger.exception("Failed to delete session DB for %s", user_id)
    await message.reply("🗑️ Session canceled and cleared. Use /thumb to start a new session.")

@dp.message_handler(commands=["done"])
@require_approved
async def cmd_done(message: types.Message):
    user_id = message.from_user.id
    s = session_for_user(user_id)
    if s.processing:
        await message.reply("⏳ Session already processing. Wait until it finishes.")
        return
    if not s.thumb_file_id and not s.thumb_local_path:
        await message.reply("❗ No thumbnail set. Use /thumb and send an image first.")
        return
    if not s.videos:
        await message.reply("❗ No videos queued for this session. Send videos first.")
        return
    # start processing
    s.processing = True
    persist_session(s)
    asyncio.create_task(process_session_worker(s, message.chat.id))
    await message.reply(f"🚀 Started processing {len(s.videos)} video(s). I will send each finished video immediately.")

@dp.message_handler(commands=["adduser"])
@owner_only
async def cmd_adduser(message: types.Message):
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /adduser <numeric_id or @username>")
        return
    target = args.split()[0]
    user_id = None
    username = None
    if target.startswith("@"):
        try:
            chat = await bot.get_chat(target)
            user_id = chat.id
            username = chat.username or target[1:]
        except Exception:
            await message.reply("Could not resolve username. Provide numeric id if the user hasn't started the bot.")
            return
    else:
        try:
            user_id = int(target)
            try:
                chat = await bot.get_chat(user_id)
                username = chat.username
            except Exception:
                username = None
        except ValueError:
            await message.reply("Invalid id. Use numeric id or @username.")
            return
    async with approved_users_lock:
        approved_users[user_id] = {"username": username, "added_by": message.from_user.id, "added_at": int(time.time())}
        save_approved_users(approved_users)
    await message.reply(f"✅ Approved user {user_id} (@{username if username else 'N/A'})")

@dp.message_handler(commands=["removeuser"])
@owner_only
async def cmd_removeuser(message: types.Message):
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /removeuser <numeric_id or @username>")
        return
    target = args.split()[0]
    if target.startswith("@"):
        username = target[1:]
        found = None
        for uid, info in list(approved_users.items()):
            if info.get("username") and info["username"].lower() == username.lower():
                found = uid
                break
        if not found:
            await message.reply("User not found in approved list.")
            return
        async with approved_users_lock:
            approved_users.pop(found, None)
            save_approved_users(approved_users)
        await message.reply(f"✅ Removed @{username} (id={found})")
    else:
        try:
            uid = int(target)
        except ValueError:
            await message.reply("Invalid id.")
            return
        async with approved_users_lock:
            if uid in approved_users:
                approved_users.pop(uid, None)
                save_approved_users(approved_users)
                await message.reply(f"✅ Removed user id={uid}")
            else:
                await message.reply("User id not approved.")

@dp.message_handler(commands=["users"])
@owner_only
async def cmd_users(message: types.Message):
    lines = []
    async with approved_users_lock:
        for uid, info in approved_users.items():
            uname = info.get("username") or "N/A"
            lines.append(f"- {uid} (@{uname}) added_by={info.get('added_by')} ts={info.get('added_at')}")
    await message.reply("Approved users:\n" + "\n".join(lines))

@dp.message_handler(commands=["session"])
@owner_only
async def cmd_session(message: types.Message):
    args = message.get_args().strip()
    if args:
        try:
            uid = int(args)
            s = session_for_user(uid)
            await message.reply(json.dumps(s.to_json(), indent=2))
        except Exception:
            await message.reply("Usage: /session [user_id]")
    else:
        lines = []
        for uid, s in sessions.items():
            lines.append(f"- {uid}: videos={len(s.videos)} proc={s.processing} expecting_thumb={s.expecting_thumb}")
        await message.reply("Sessions:\n" + "\n".join(lines))

# ---------------------------
# Media handlers
# ---------------------------
@dp.message_handler(content_types=ContentType.PHOTO)
@require_approved
async def handle_photo(message: types.Message):
    user_id = message.from_user.id
    s = session_for_user(user_id)
    if not s.expecting_thumb:
        await message.reply("I wasn't expecting a thumbnail. Use /thumb to start a session.")
        return
    photo = message.photo[-1]
    s.thumb_file_id = photo.file_id
    s.expecting_thumb = False
    persist_session(s)
    await message.reply("✅ Thumbnail set for this session. Now send videos (up to 99).")

@dp.message_handler(content_types=ContentType.DOCUMENT)
@require_approved
async def handle_document(message: types.Message):
    user_id = message.from_user.id
    s = session_for_user(user_id)
    doc = message.document
    mime = (doc.mime_type or "").lower()
    # treat as thumbnail if expecting and image mime
    if s.expecting_thumb and mime.startswith("image/"):
        s.thumb_file_id = doc.file_id
        s.expecting_thumb = False
        persist_session(s)
        await message.reply("✅ Thumbnail (document) set for this session. Send videos.")
        return
    # accept video documents
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
        if len(s.videos) >= MAX_VIDEOS_PER_SESSION:
            await message.reply(f"❗ Session already has {MAX_VIDEOS_PER_SESSION} videos.")
            return
        idx = len(s.videos) + 1
        vi = VideoItem(file_id=doc.file_id, caption=message.caption, order=idx, file_unique_id=doc.file_unique_id, file_size=doc.file_size)
        s.videos.append(vi)
        persist_session(s)
        await message.reply(f"✅ Video {idx} received (document). Send more or /done.")
        return
    await message.reply("I accept images as thumbnails or video files (video or document).")

@dp.message_handler(content_types=ContentType.VIDEO)
@require_approved
async def handle_video(message: types.Message):
    user_id = message.from_user.id
    s = session_for_user(user_id)
    if s.expecting_thumb:
        await message.reply("Please send thumbnail first (you started /thumb).")
        return
    if len(s.videos) >= MAX_VIDEOS_PER_SESSION:
        await message.reply(f"❗ Session already has {MAX_VIDEOS_PER_SESSION} videos.")
        return
    video = message.video
    idx = len(s.videos) + 1
    vi = VideoItem(file_id=video.file_id, caption=message.caption, order=idx, file_unique_id=video.file_unique_id, file_size=video.file_size)
    s.videos.append(vi)
    persist_session(s)
    await message.reply(f"✅ Video {idx} queued. Send more or /done when ready.")

# ---------------------------
# Worker: process session sequentially
# ---------------------------
async def process_session_worker(session: Session, reply_chat_id: int):
    async with session.lock:
        if session.processing:
            logger.info("Session %s already processing", session.user_id)
            return
        session.processing = True
        persist_session(session)

        total = len(session.videos)
        if total == 0:
            session.processing = False
            persist_session(session)
            await safe_send(reply_chat_id, "❗ No videos to process.")
            return

        # prepare thumb param (InputFile or file_id string)
        thumb_param = None
        if session.thumb_local_path and os.path.exists(session.thumb_local_path):
            try:
                thumb_param = InputFile(session.thumb_local_path)
            except Exception:
                logger.exception("Failed to create InputFile from local thumb, falling back to file_id")
                thumb_param = session.thumb_file_id
        else:
            thumb_param = session.thumb_file_id

        await safe_send(reply_chat_id, f"🔁 Processing {total} video(s) — sending each as it completes.")

        processed = 0
        start_all = time.time()
        hist = perf_history.get(session.user_id, [])
        hist = hist[-PERF_HISTORY_LIMIT:]

        # use snapshot to iterate safely
        videos_snapshot = list(session.videos)

        for n, vi in enumerate(videos_snapshot, start=1):
            processed += 1
            # pre-update
            percent_before = (processed - 1) / total
            bar_before = make_progress_bar(percent_before)
            try:
                await safe_send(reply_chat_id, f"⏳ Starting video {processed}/{total}... Overall: {bar_before} {processed-1}/{total}")
            except Exception:
                pass

            start_file = time.time()
            try:
                # Re-send using file_id — no local download; Telegram will attach thumb
                await bot.send_video(
                    chat_id=reply_chat_id,
                    video=vi.file_id,
                    caption=vi.caption or "",
                    thumb=thumb_param,
                    timeout=180
                )
                end_file = time.time()
                duration = end_file - start_file
                hist.append(duration)
                perf_history[session.user_id] = hist[-PERF_HISTORY_LIMIT:]
                # remove this video from session.videos by matching file_id+order
                try:
                    session.videos = [v for v in session.videos if not (v.file_id == vi.file_id and v.order == vi.order)]
                except Exception:
                    logger.exception("Failed to remove sent video from session.videos")
                persist_session(session)

                # compute ETA from average of hist
                avg = (sum(hist) / len(hist)) if hist else duration
                remaining = total - processed
                eta = avg * remaining
                percent_done = processed / total
                bar = make_progress_bar(percent_done)
                elapsed = time.time() - start_all
                await safe_send(reply_chat_id,
                                f"✅ Sent {processed}/{total} videos.\n{bar} {int(percent_done*100)}%\nElapsed: {human_time(elapsed)} | ETA: {human_time(eta)}")
            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter while sending video for user %s: wait %s", session.user_id, wait)
                await asyncio.sleep(wait + 1)
                # try once more
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=vi.file_id, caption=vi.caption or "", thumb=thumb_param, timeout=180)
                except Exception:
                    logger.exception("Failed to resend after RetryAfter")
                    await safe_send(reply_chat_id, f"❌ Failed to send video {processed} after retry.")
            except TelegramAPIError as te:
                logger.exception("TelegramAPIError sending video %s: %s", vi.file_id, te)
                await safe_send(reply_chat_id, f"❌ Telegram API error sending video {processed}: {te}")
            except Exception as e:
                logger.exception("Unexpected error sending video %s: %s", vi.file_id, e)
                await safe_send(reply_chat_id, f"❌ Unexpected error sending video {processed}: {e}")

        # finished
        session.processing = False
        session.videos = []
        persist_session(session)
        await safe_send(reply_chat_id, f"🎬 All {total} video(s) processed and sent. Session thumbnail remains until you /thumb again or /cancel.")

# ---------------------------
# Safe send wrapper
# ---------------------------
async def safe_send(chat_id: int, text: str):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        logger.exception("Failed to send message to %s: %s", chat_id, text)
        try:
            await notify_owner(f"Failed to send message to {chat_id}: {text[:1000]}")
        except Exception:
            pass

# ---------------------------
# Webhook handling (aiohttp)
# ---------------------------
async def handle_webhook(request):
    # Receive update from Telegram and pass to aiogram Dispatcher
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="Bad request")
    try:
        update = types.Update(**data)
        await dp.process_update(update)
        return web.Response(text="OK")
    except Exception:
        logger.exception("Failed to process update")
        return web.Response(status=500, text="Internal Server Error")

# Health and info endpoints
async def handle_health(request):
    return web.json_response({"status": "ok", "owner_id": OWNER_ID, "sessions": len(sessions)})

async def handle_info(request):
    try:
        return web.json_response({
            "status": "ok",
            "owner_id": OWNER_ID,
            "active_sessions": sum(1 for s in sessions.values() if s.videos or s.expecting_thumb),
            "approved_users_count": len(approved_users),
        })
    except Exception:
        return web.json_response({"status": "error"}, status=500)

# ---------------------------
# Self-ping to keep service alive
# ---------------------------
async def self_ping_loop(app, interval: int = SELF_PING_INTERVAL):
    await asyncio.sleep(2)  # give service a little time to start
    while True:
        try:
            # call bot.get_me to keep bot session active
            await bot.get_me()
        except Exception:
            logger.exception("Self-ping bot.get_me failed")
        # also do a local health fetch to keep server active internally
        try:
            # simple GET to our own /health
            import aiohttp
            async with aiohttp.ClientSession() as session_http:
                url = WEBHOOK_HOST.rstrip("/") + "/health"
                # do a HEAD to be lightweight if allowed
                async with session_http.get(url, timeout=10) as resp:
                    resp_text = await resp.text()
                    logger.debug("Self-ping health status: %s", resp.status)
        except Exception:
            logger.debug("Self-ping health GET failed (this is okay in some environments)")
        await asyncio.sleep(interval)

# ---------------------------
# Startup/shutdown hooks
# ---------------------------
async def on_startup(app):
    logger.info("on_startup: setting webhook to %s", WEBHOOK_URL)
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except BadRequest as e:
        logger.exception("BadRequest when setting webhook: %s", e)
        await notify_owner(f"Failed to set webhook to {WEBHOOK_URL}: {e}")
        raise
    except Exception:
        logger.exception("Unexpected exception setting webhook")
        await notify_owner(f"Unexpected error setting webhook: see logs")
    # start self-ping task
    app["self_ping_task"] = asyncio.create_task(self_ping_loop(app, interval=SELF_PING_INTERVAL))
    logger.info("Self-ping task started")

async def on_shutdown(app):
    logger.info("on_shutdown: removing webhook")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("Failed to delete webhook on shutdown")
    # cancel self-ping task
    task = app.get("self_ping_task")
    if task:
        task.cancel()
        with suppress(Exception):
            await task
    logger.info("Shutdown complete")

# ---------------------------
# Uncaught exceptions handlers
# ---------------------------
def excepthook(exc_type, exc, tb):
    tbtext = "".join(traceback.format_exception(exc_type, exc, tb))
    logger.critical("Uncaught exception: %s", tbtext)
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(notify_owner(f"Uncaught exception in bot:\n{tbtext[:3000]}"))
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
# Helper: load sessions into memory on startup
# ---------------------------
def load_all_sessions_into_memory():
    try:
        cur = db_conn.cursor()
        cur.execute("SELECT user_id, session_json FROM sessions")
        rows = cur.fetchall()
        for row in rows:
            try:
                uid = int(row[0])
                obj = json.loads(row[1])
                s = Session.from_json(obj)
                s.lock = asyncio.Lock()
                sessions[uid] = s
            except Exception:
                logger.exception("Failed to load session row for %s", row[0])
        logger.info("Loaded %d sessions into memory", len(sessions))
    except Exception:
        logger.exception("Failed to load sessions from DB")

# ---------------------------
# Main: build aiohttp app and run
# ---------------------------
def main():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda l, ctx: task_exception_handler(l, ctx))
    # set sys excepthook
    # load sessions & approved users into memory
    load_all_sessions_into_memory()
    # create aiohttp app
    app = web.Application()
    # webhook route
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    # health routes
    app.router.add_get("/health", handle_health)
    app.router.add_get("/info", handle_info)
    # startup/shutdown
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    # set signal handlers if available
    try:
        import signal
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(shutdown_signal(sig)))
    except Exception:
        logger.debug("Signal handlers not set (platform limitation)")

    logger.info("Starting aiohttp web server at 0.0.0.0:%s webhook_path=%s", PORT, WEBHOOK_PATH)
    try:
        web.run_app(app, host="0.0.0.0", port=PORT)
    except Exception:
        logger.exception("Exception from web.run_app")
        try:
            loop.run_until_complete(notify_owner("Failed to start webhook server. See logs."))
        except Exception:
            pass

async def shutdown_signal(sig):
    logger.info("Received signal %s, shutting down", sig)
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    os._exit(0)

if __name__ == "__main__":
    main()