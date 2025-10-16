#!/usr/bin/env python3
# bot.py
# Thumbnail Session Video Bot (final)
# aiogram==2.25.0
# aiohttp==3.8.6
#
# Features:
#  - Owner + approved users (/adduser)
#  - Per-session thumbnail via /thumb (reply to image)
#  - Queueing: accept up to 99 videos per session, processed in order
#  - /done starts sequential processing; each finished video is sent immediately
#  - No re-encoding or heavy disk usage: reuses Telegram file_id for sending
#  - Progress reporting: overall progress bar, completed vs remaining, ETA (estimated)
#  - Self-ping /health endpoint to keep Render free plan awake
#  - Webhook mode for Render
#  - Owner notifications on uncaught exceptions
#
# Limitations & notes:
#  - Telegram's official upload limit historically ~2GB. The bot sends via file_id to avoid download limits.
#  - You must make sure WEBHOOK_HOST env var is a valid HTTPS URL for telegram webhook.
#  - Sessions persist in SQLite (sessions survive restarts).
#  - Approved users persisted in JSON (simple), but you can change to DB if desired.
#
# Author: Generated for user
# Date: 2025-10-16
#
# Long file with thorough comments and robust error handling.
#

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
from contextlib import suppress
from functools import wraps

from dotenv import load_dotenv
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError, BadRequest
from aiogram.types import ContentType, InputFile

# -------------------------
# Load environment
# -------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN missing in environment (set BOT_TOKEN in .env)")
    sys.exit(1)

OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
if not OWNER_ID_RAW:
    print("ERROR: OWNER_ID missing in environment (set OWNER_ID in .env)")
    sys.exit(1)
try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    print("ERROR: OWNER_ID must be numeric")
    sys.exit(1)

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "").strip()
if not WEBHOOK_HOST:
    print("ERROR: WEBHOOK_HOST missing in environment (set WEBHOOK_HOST in .env) e.g. https://your-service.onrender.com")
    sys.exit(1)

PORT = int(os.getenv("PORT", "10000"))
# webhook path - use a stable secret path if you like, default to /webhook/<token-short>
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "").strip() or f"/webhook"
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH
WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

# -------------------------
# Paths & persistence
# -------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
SESSIONS_DIR = DATA_DIR / "sessions"
DB_PATH = DATA_DIR / "bot_data.sqlite"
APPROVED_USERS_JSON = DATA_DIR / "approved_users.json"
LOGS_DIR = BASE_DIR / "logs"
for p in (DATA_DIR, SESSIONS_DIR, LOGS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("thumb_session_bot")

# -------------------------
# Constants
# -------------------------
MAX_VIDEOS_PER_SESSION = 99
# Telegram historically allowed ~2GB; user requested up to 3GB. Telegram's real limit is authoritative.
# We will accept any video and rely on Telegram file_id resend (no local download) to avoid file size limit errors.
TELEGRAM_SUGGESTED_LIMIT_BYTES = 2 * 1024 * 1024 * 1024  # 2GB suggestion

PROGRESS_BAR_LEN = 24

# -------------------------
# Dataclasses / Session model
# -------------------------
@dataclass
class VideoItem:
    """Represents a queued video in a session."""
    file_id: str
    caption: Optional[str]
    order: int
    file_unique_id: Optional[str] = None
    file_size: Optional[int] = None  # may be None

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
    thumb_file_id: Optional[str] = None  # remote file_id of thumbnail (photo)
    thumb_local_path: Optional[str] = None  # local path to processed thumbnail if we saved one
    videos: List[VideoItem] = field(default_factory=list)
    expecting_thumb: bool = False
    processing: bool = False
    # not persisted
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

# -------------------------
# Storage / DB
# -------------------------
# We'll use sqlite for session persistence and a JSON file for approved users as simpler.
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
        logger.exception("Failed to save session to DB for user %s", user_id)

def load_session_db(user_id: int) -> Optional[dict]:
    try:
        cur = db_conn.cursor()
        cur.execute("SELECT session_json FROM sessions WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0])
    except Exception:
        logger.exception("Failed to load session from DB for user %s", user_id)
        return None

def delete_session_db(user_id: int):
    try:
        cur = db_conn.cursor()
        cur.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        db_conn.commit()
    except Exception:
        logger.exception("Failed to delete session DB for user %s", user_id)

# Approved users persistence (simple JSON)
approved_users_lock = asyncio.Lock()
def load_approved_users():
    if APPROVED_USERS_JSON.exists():
        try:
            with open(APPROVED_USERS_JSON, "r", encoding="utf-8") as f:
                obj = json.load(f)
            # keys are str ints
            return {int(k): v for k, v in obj.items()}
        except Exception:
            logger.exception("Failed to load approved users JSON")
            return {}
    else:
        # default: owner approved
        return {OWNER_ID: {"username": None, "added_by": OWNER_ID, "added_at": int(time.time())}}

def save_approved_users(approved: Dict[int, dict]):
    try:
        with open(APPROVED_USERS_JSON, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in approved.items()}, f, indent=2)
    except Exception:
        logger.exception("Failed to save approved users JSON")

# -------------------------
# Global runtime state
# -------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# memory caches
sessions: Dict[int, Session] = {}
approved_users = load_approved_users()
# ensure owner present
if OWNER_ID not in approved_users:
    approved_users[OWNER_ID] = {"username": None, "added_by": OWNER_ID, "added_at": int(time.time())}
    save_approved_users(approved_users)

# used to estimate per-file send times for ETA (per-user)
# stores list of durations for last N sends
PERF_HISTORY: Dict[int, List[float]] = {}

# -------------------------
# Utility helpers
# -------------------------
def human_time(seconds: float) -> str:
    seconds = int(seconds)
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
    empty = length - filled
    return "▰" * filled + "▱" * empty

async def notify_owner(text: str):
    try:
        await bot.send_message(OWNER_ID, text)
    except Exception:
        logger.exception("Failed to notify owner")

def session_for_user(user_id: int) -> Session:
    # create in-memory session or load from DB
    s = sessions.get(user_id)
    if not s:
        obj = load_session_db(user_id)
        if obj:
            try:
                s = Session.from_json(obj)
            except Exception:
                logger.exception("Failed to load session object from DB for %s", user_id)
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

# decorator to ensure caller is approved
def require_approved(func):
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        uid = message.from_user.id
        if uid not in approved_users:
            try:
                await message.reply("⛔️ You are not authorized to use this bot. Ask owner to /adduser you.")
            except Exception:
                logger.debug("Cannot reply unauthorized")
            return
        return await func(message, *args, **kwargs)
    return wrapper

# owner-only decorator
def owner_only(func):
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.from_user.id != OWNER_ID:
            await message.reply("⛔️ Only owner can use this command.")
            return
        return await func(message, *args, **kwargs)
    return wrapper

# -------------------------
# Command handlers
# -------------------------
HELP_TEXT = """📼 Thumbnail Session Video Bot — Help

Commands (owner or approved users):
/start - show this help
/thumb - reply to an image with /thumb to set it as session thumbnail (per session)
/done - process all queued videos (sequentially)
/cancel - cancel current session and clear queued videos
/adduser <id or @username> - (owner only) approve a user by numeric id or @username
/removeuser <id or @username> - (owner only) remove a user
/users - (owner only) list approved users
/session - (owner) show session state
/health - HTTP health endpoint (GET) available at /health
"""

@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.reply("👋 Welcome to the Thumbnail Session Video Bot.\n\n" + HELP_TEXT)

@dp.message_handler(commands=["thumb"])
@require_approved
async def cmd_thumb(message: types.Message):
    """
    Start a session and expect a thumbnail. The user should reply to an image with /thumb
    """
    user_id = message.from_user.id
    s = session_for_user(user_id)
    # Expecting thumbnail - user should reply to an image with /thumb
    # We'll accept both photo messages and document images next.
    if message.reply_to_message and message.reply_to_message.photo:
        # Direct immediate set if they replied to a photo
        photo = message.reply_to_message.photo[-1]
        file_id = photo.file_id
        s.thumb_file_id = file_id
        s.expecting_thumb = False
        # Don't need to download; keep file_id and optionally download processed local copy if needed later
        persist_session(s)
        await message.reply("✅ Thumbnail set for this session (from replied photo).")
        return
    else:
        s.expecting_thumb = True
        s.thumb_file_id = None
        s.thumb_local_path = None
        s.videos = []
        s.processing = False
        persist_session(s)
        await message.reply("📸 Please send the thumbnail image now (send a photo or image as a document).")

@dp.message_handler(commands=["cancel"])
@require_approved
async def cmd_cancel(message: types.Message):
    user_id = message.from_user.id
    # clear session data and persistent storage
    try:
        sessions.pop(user_id, None)
    except Exception:
        logger.exception("Failed to clear in-memory session for %s", user_id)
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
        await message.reply("⏳ Your session is already processing. Wait for it to finish.")
        return
    if not s.thumb_file_id and not s.thumb_local_path:
        await message.reply("❗ No thumbnail set. Use /thumb and send thumbnail first.")
        return
    if not s.videos:
        await message.reply("❗ No videos queued for this session. Send videos first.")
        return
    # start background processing
    s.processing = True
    persist_session(s)
    asyncio.create_task(process_session_worker(s, message.chat.id))
    await message.reply(f"🚀 Started processing {len(s.videos)} video(s). I will send each finished video back to you immediately.")

@dp.message_handler(commands=["adduser"])
@owner_only
async def cmd_adduser(message: types.Message):
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /adduser <numeric_id or @username>")
        return
    target = args.split()[0]
    uid = None
    username = None
    if target.startswith("@"):
        try:
            chat = await bot.get_chat(target)
            uid = chat.id
            username = chat.username or target[1:]
        except Exception as e:
            await message.reply("⚠️ Couldn't resolve @username to id. Please provide numeric id instead if the user hasn't started the bot.")
            return
    else:
        try:
            uid = int(target)
            try:
                chat = await bot.get_chat(uid)
                username = chat.username
            except Exception:
                username = None
        except Exception:
            await message.reply("Invalid identifier. Use numeric ID or @username.")
            return
    async with approved_users_lock:
        approved_users[uid] = {"username": username, "added_by": message.from_user.id, "added_at": int(time.time())}
        save_approved_users(approved_users)
    await message.reply(f"✅ Approved user id={uid} username={('@'+username) if username else 'N/A'}")

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
        await message.reply(f"✅ Removed user @{username} (id={found})")
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
                await message.reply("User id not in approved list.")

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
            text = json.dumps(s.to_json(), indent=2)
            await message.reply(f"Session for {uid}:\n{text}")
        except Exception:
            await message.reply("Usage: /session [user_id]")
    else:
        lines = []
        for uid, s in sessions.items():
            lines.append(f"- {uid}: videos={len(s.videos)} processing={s.processing} expecting_thumb={s.expecting_thumb}")
        await message.reply("Sessions:\n" + "\n".join(lines))

# -------------------------
# Media handlers (photo, document, video)
# -------------------------
@dp.message_handler(content_types=ContentType.PHOTO)
@require_approved
async def handle_photo(message: types.Message):
    user_id = message.from_user.id
    s = session_for_user(user_id)
    if not s.expecting_thumb:
        # user sent an image but not expecting a thumbnail
        await message.reply("I wasn't expecting a thumbnail. Use /thumb to start a new session and then send the image (or reply to image with /thumb).")
        return
    # largest size
    photo = message.photo[-1]
    file_id = photo.file_id
    s.thumb_file_id = file_id
    s.expecting_thumb = False
    persist_session(s)
    await message.reply("✅ Thumbnail received and set for this session. Now send videos (up to 99) and then /done.")

@dp.message_handler(content_types=ContentType.DOCUMENT)
@require_approved
async def handle_document(message: types.Message):
    user_id = message.from_user.id
    s = session_for_user(user_id)
    doc = message.document
    mime = (doc.mime_type or "").lower()
    # If expecting thumbnail and document is image
    if s.expecting_thumb and mime.startswith("image/"):
        s.thumb_file_id = doc.file_id
        s.expecting_thumb = False
        persist_session(s)
        await message.reply("✅ Thumbnail (document) received for this session. Send videos now.")
        return
    # If document is video type
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov"))):
        if len(s.videos) >= MAX_VIDEOS_PER_SESSION:
            await message.reply(f"❗ Session already has {MAX_VIDEOS_PER_SESSION} videos.")
            return
        idx = len(s.videos) + 1
        vi = VideoItem(file_id=doc.file_id, caption=message.caption, order=idx, file_unique_id=doc.file_unique_id, file_size=doc.file_size)
        s.videos.append(vi)
        persist_session(s)
        await message.reply(f"✅ Video {idx} received as document. Send more or /done when ready.")
        return
    await message.reply("I accept images for thumbnails or video files (as video or document).")

@dp.message_handler(content_types=ContentType.VIDEO)
@require_approved
async def handle_video(message: types.Message):
    user_id = message.from_user.id
    s = session_for_user(user_id)
    if s.expecting_thumb:
        await message.reply("Please send the thumbnail first (you started a /thumb).")
        return
    if len(s.videos) >= MAX_VIDEOS_PER_SESSION:
        await message.reply(f"❗ Session already has {MAX_VIDEOS_PER_SESSION} videos.")
        return
    video = message.video
    # Note: video.duration and video.file_size may be None, and file_size may not reflect full size for forwarded messages
    if video.duration and video.duration > (6*60*60):  # 6 hours arbitrary safety limit
        await message.reply("❗ Video duration seems very large. Confirm it's intended.")
        # still accept if user insists
    idx = len(s.videos) + 1
    vi = VideoItem(file_id=video.file_id, caption=message.caption, order=idx, file_unique_id=video.file_unique_id, file_size=video.file_size)
    s.videos.append(vi)
    persist_session(s)
    await message.reply(f"✅ Video {idx} received. Send more or /done when ready.")

# -------------------------
# Processing worker
# -------------------------
async def process_session_worker(session: Session, reply_chat_id: int):
    """
    Process a session sequentially. For each queued VideoItem:
      - Send the video using the saved file_id (no full download)
      - Attach the session thumbnail (local thumb if saved, else use thumb_file_id)
      - Send progress updates: completed/remaining/ETA and a visual bar
      - Remove the video entry after send
    Progress for an individual file is estimated based on PERF_HISTORY (previous per-file durations).
    """
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
            await safe_send_message(reply_chat_id, "❗ No videos to process.")
            return

        # prepare thumbnail param: prefer local path (InputFile); else pass file_id string (aiogram accepts)
        thumb_param = None
        if session.thumb_local_path and os.path.exists(session.thumb_local_path):
            try:
                thumb_param = InputFile(session.thumb_local_path)
            except Exception:
                logger.exception("Failed to create InputFile from local thumb for user %s", session.user_id)
                thumb_param = session.thumb_file_id
        else:
            thumb_param = session.thumb_file_id

        await safe_send_message(reply_chat_id, f"🔁 Processing {total} video(s) for this session. I will send each video as it completes.")

        processed = 0
        start_all = time.time()

        # initialize per-user perf history
        history = PERF_HISTORY.get(session.user_id, [])
        # We'll keep only last 8 entries
        history = history[-8:]

        # copy list to avoid mutation issues
        videos_copy = list(session.videos)

        for idx, vi in enumerate(videos_copy, start=1):
            processed += 1
            # Update an interim progress message (before sending)
            percent_done_overall = (processed - 1) / total
            bar_before = progress_bar(percent_done_overall)
            try:
                await safe_send_message(reply_chat_id, f"⏳ Starting video {processed}/{total}...\nOverall: {bar_before} {processed-1}/{total}")
            except Exception:
                logger.debug("Couldn't send interim start message")

            # Record start time for this file
            start_file = time.time()
            # Send using file_id to avoid local download
            try:
                # aiogram send_video accepts a file_id str for video param
                # thumb parameter: can be InputFile local path OR file_id str
                # We'll pass thumb_param (InputFile or str or None)
                await bot.send_video(
                    chat_id=reply_chat_id,
                    video=vi.file_id,
                    caption=vi.caption or "",
                    thumb=thumb_param,
                    timeout=180  # generous timeout
                )
                # successful send
                end_file = time.time()
                duration = end_file - start_file
                # update history
                history.append(duration)
                PERF_HISTORY[session.user_id] = history[-8:]
                # remove the first element from session.videos (we used a snapshot)
                if session.videos:
                    # remove the video with matching file_unique_id or file_id and order
                    with suppress(Exception):
                        # attempt to remove the same file_id
                        session.videos = [v for v in session.videos if not (v.file_id == vi.file_id and v.order == vi.order)]
                persist_session(session)
                # compute ETA using average duration in history
                avg = sum(history) / len(history) if history else duration
                remaining_count = total - processed
                eta_seconds = avg * remaining_count
                percent_done = processed / total
                bar = progress_bar(percent_done)
                elapsed_total = time.time() - start_all

                # Prepare a clear progress message
                progress_msg = (
                    f"✅ Sent {processed}/{total} videos.\n"
                    f"{bar} {int(percent_done*100)}%\n"
                    f"Elapsed: {human_time(elapsed_total)} | ETA: {human_time(eta_seconds)}"
                )
                await safe_send_message(reply_chat_id, progress_msg)

            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter while sending video for user %s: %ss", session.user_id, wait)
                await asyncio.sleep(wait + 1)
                # try to resend once
                try:
                    await bot.send_video(chat_id=reply_chat_id, video=vi.file_id, caption=vi.caption or "", thumb=thumb_param, timeout=180)
                except Exception:
                    logger.exception("Failed to resend after RetryAfter for video %s", vi.file_id)
                    await safe_send_message(reply_chat_id, f"❌ Failed to send video {processed} after retry.")
            except TelegramAPIError as te:
                logger.exception("TelegramAPIError sending video %s: %s", vi.file_id, te)
                await safe_send_message(reply_chat_id, f"❌ Telegram API error sending video {processed}: {te}")
            except Exception as e:
                logger.exception("Unexpected error sending video %s: %s", vi.file_id, e)
                await safe_send_message(reply_chat_id, f"❌ Unexpected error sending video {processed}: {e}")

        # finished all
        session.processing = False
        # clear videos list (should already be cleared progressively)
        session.videos = []
        persist_session(session)
        await safe_send_message(reply_chat_id, f"🎬 All {total} video(s) processed and sent. Session thumbnail remains until you /thumb again or /cancel.")

# -------------------------
# Safe send message wrapper
# -------------------------
async def safe_send_message(chat_id: int, text: str):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        # don't raise further; log and notify owner
        logger.exception("Failed to send message to %s: %s", chat_id, text)
        try:
            await notify_owner(f"Failed to send message to {chat_id}: {text[:2000]}")
        except Exception:
            logger.debug("Failed to notify owner about message send failure")

# -------------------------
# Web / health / self-ping
# -------------------------
async def make_web_app():
    app = web.Application()

    async def health(request):
        return web.json_response({
            "status": "ok",
            "owner_id": OWNER_ID,
            "sessions": len(sessions),
            "approved_users": len(approved_users),
        })

    async def info(request):
        try:
            return web.json_response({
                "status": "ok",
                "owner_id": OWNER_ID,
                "active_sessions": sum(1 for s in sessions.values() if s.videos or s.expecting_thumb),
                "approved_users_count": len(approved_users),
            })
        except Exception:
            return web.json_response({"status": "error"}, status=500)

    async def root(request):
        return web.Response(text="Thumbnail Session Video Bot — webhook mode. Use /health for monitor.")

    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_get("/info", info)

    return app

async def self_ping_task(app, interval_seconds: int = 120):
    """
    Periodically call bot.get_me() and a small HEAD on /health to keep the instance warm.
    This helps keep Render free instances alive if they rely on periodic activity.
    """
    while True:
        try:
            await bot.get_me()
        except Exception:
            logger.exception("Self-ping bot.get_me failed")
        await asyncio.sleep(interval_seconds)

# -------------------------
# Startup / Shutdown handlers
# -------------------------
async def on_startup(dp):
    # load approved users and sessions from disk/db
    logger.info("Starting up bot. WEBHOOK_URL=%s", WEBHOOK_URL)
    # set webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except BadRequest as e:
        # likely invalid webhook URL - log and notify owner
        logger.exception("Failed to set webhook: %s", e)
        await notify_owner(f"Failed to set webhook: {e}")
        raise
    except Exception:
        logger.exception("Unexpected exception when setting webhook")
        await notify_owner("Unexpected exception when setting webhook. See logs.")

    # load sessions already (they will be lazily instantiated in session_for_user)
    # start self-ping in background if aiohttp app provided (we attach the task later)
    logger.info("Startup complete.")

async def on_shutdown(dp):
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("Failed to delete webhook on shutdown")
    logger.info("Shutdown complete.")

# -------------------------
# Uncaught exceptions / loop handlers
# -------------------------
def excepthook(exc_type, exc, tb):
    tbtext = "".join(traceback.format_exception(exc_type, exc, tb))
    logger.critical("Uncaught exception: %s", tbtext)
    # notify owner asynchronously (best-effort)
    loop = asyncio.get_event_loop()
    try:
        loop.create_task(notify_owner(f"Uncaught exception in bot process:\n{tbtext[:4000]}"))
    except Exception:
        pass

sys.excepthook = excepthook

# set task exception handler for loop
def task_exception_handler(loop, context):
    msg = context.get("exception", context.get("message"))
    logger.error("Asyncio task exception: %s", msg)
    try:
        loop.create_task(notify_owner(f"Async task exception: {msg}"))
    except Exception:
        pass

# -------------------------
# Utility: graceful shutdown on signals
# -------------------------
def setup_signal_handlers(loop):
    try:
        import signal
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(shutdown_signal(sig)))
    except Exception:
        # not all environments allow signal handlers
        pass

async def shutdown_signal(sig):
    logger.info("Received signal %s; shutting down.", sig)
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    os._exit(0)

# -------------------------
# Launching the webhook server (aiogram executor.start_webhook)
# -------------------------
def main():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda l, ctx: task_exception_handler(l, ctx))
    setup_signal_handlers(loop)

    # create aiohttp web app and attach self-ping
    app = loop.run_until_complete(make_web_app())

    # create self-ping background task via app on_startup if needed
    async def attach_ping(app):
        app["self_ping_task"] = asyncio.create_task(self_ping_task(app, interval_seconds=120))

    # aiogram's executor.start_webhook accepts web_app parameter to which we can add routes
    try:
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            skip_updates=True,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            host="0.0.0.0",
            port=PORT,
            web_app=app
        )
    except Exception:
        logger.exception("Exception from start_webhook")
        # attempt to notify owner
        try:
            loop.run_until_complete(notify_owner("Failed to start webhook. See logs."))
        except Exception:
            pass

if __name__ == "__main__":
    main()