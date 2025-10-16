#!/usr/bin/env python3
# bot.py
# Thumbnail Session Video Bot - Webhook mode (Render-ready)
# aiogram==2.25.0, aiohttp==3.8.6
#
# Features added per user's request:
# 1) Automatic thumbnail resize/compression (Pillow) preserving visual clarity
# 2) Persistent session storage using SQLite (sessions survive restarts)
# 3) Improved owner/admin error notifications (owner gets Telegram messages on exceptions)
# 4) Existing features: owner + approved users, /thumb, send up to 99 videos/session, /done processing sequentially, /adduser
#
# Notes:
# - Keep thumbnails as JPEG. The code will convert PNG to JPEG when necessary.
# - Telegram accepts thumbnail as either file_id or local file upload; we save processed thumbnail locally and use it when uploading videos.
# - The system uses sqlite for approved users and sessions persistence.
#
# Author: generated for user (MS)
# Date: 2025-10-16

import os
import sys
import io
import json
import time
import math
import sqlite3
import asyncio
import logging
import pathlib
import traceback
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

from dotenv import load_dotenv
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError
from aiogram.types import ContentType, InputFile

# Pillow for image processing
from PIL import Image, ImageOps

# -----------------------
# Configuration & Setup
# -----------------------

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN not set in environment.")
    sys.exit(1)

OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
if not OWNER_ID_RAW:
    print("ERROR: OWNER_ID not set in environment.")
    sys.exit(1)
try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    print("ERROR: OWNER_ID must be numeric.")
    sys.exit(1)

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "").strip()
if not WEBHOOK_HOST:
    print("ERROR: WEBHOOK_HOST not set in environment (public HTTPS URL required).")
    sys.exit(1)

WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "").strip() or f"/{BOT_TOKEN}"
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH

PORT = int(os.getenv("PORT", "8000"))

# directories
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
SESSIONS_DIR = DATA_DIR / "sessions"
DB_DIR = DATA_DIR
LOGS_DIR = BASE_DIR / "logs"
for d in (DATA_DIR, SESSIONS_DIR, DB_DIR, LOGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = DB_DIR / "bot_data.sqlite"
APPROVED_USERS_DB_TABLE = "approved_users"
SESSIONS_DB_TABLE = "sessions"  # persistent session table

# constants
MAX_VIDEOS_PER_SESSION = 99
MAX_VIDEO_DURATION_SECS = 4 * 60 * 60  # 4 hours
TELEGRAM_THUMB_MAX_BYTES = 200 * 1024  # ~200KB recommended

# Pillow processing parameters (tuned to keep quality)
THUMB_MAX_DIMENSION = 1280  # maximum width or height before downscale (keeps image large but reasonable)
THUMB_TARGET_MAX_BYTES = TELEGRAM_THUMB_MAX_BYTES
THUMB_MIN_QUALITY = 45  # don't drop quality below this unless absolutely necessary
THUMB_START_QUALITY = 95
THUMB_QUALITY_STEP = 5
THUMB_RESIZE_STEP = 0.9  # scale factor when we need to progressively reduce size

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("thumbnail_session_bot")

# -----------------------
# Database helpers
# -----------------------

def init_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    cur = conn.cursor()
    # approved users table: user_id (primary), username (nullable), added_by, added_at
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {APPROVED_USERS_DB_TABLE} (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        added_by INTEGER,
        added_at INTEGER
    )
    """)
    # sessions table: user_id (primary), session_json (text), updated_at
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {SESSIONS_DB_TABLE} (
        user_id INTEGER PRIMARY KEY,
        session_json TEXT,
        updated_at INTEGER
    )
    """)
    conn.commit()
    return conn

db_conn = init_db()
db_lock = asyncio.Lock()

def add_approved_user_db(user_id: int, username: Optional[str], added_by: int):
    ts = int(time.time())
    cur = db_conn.cursor()
    cur.execute(f"""
    INSERT OR REPLACE INTO {APPROVED_USERS_DB_TABLE} (user_id, username, added_by, added_at)
    VALUES (?, ?, ?, ?)
    """, (user_id, username, added_by, ts))
    db_conn.commit()

def remove_approved_user_db(user_id: int):
    cur = db_conn.cursor()
    cur.execute(f"DELETE FROM {APPROVED_USERS_DB_TABLE} WHERE user_id = ?", (user_id,))
    db_conn.commit()

def get_all_approved_users_db() -> Dict[int, Dict[str, Any]]:
    cur = db_conn.cursor()
    cur.execute(f"SELECT user_id, username, added_by, added_at FROM {APPROVED_USERS_DB_TABLE}")
    rows = cur.fetchall()
    return {int(row[0]): {"username": row[1], "added_by": row[2], "added_at": row[3]} for row in rows}

def get_approved_user_db(user_id: int) -> Optional[Dict[str, Any]]:
    cur = db_conn.cursor()
    cur.execute(f"SELECT user_id, username, added_by, added_at FROM {APPROVED_USERS_DB_TABLE} WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row:
        return None
    return {"user_id": row[0], "username": row[1], "added_by": row[2], "added_at": row[3]}

def save_session_db(user_id: int, session_data: dict):
    ts = int(time.time())
    cur = db_conn.cursor()
    cur.execute(f"""
    INSERT OR REPLACE INTO {SESSIONS_DB_TABLE} (user_id, session_json, updated_at)
    VALUES (?, ?, ?)
    """, (user_id, json.dumps(session_data), ts))
    db_conn.commit()

def load_session_db(user_id: int) -> Optional[dict]:
    cur = db_conn.cursor()
    cur.execute(f"SELECT session_json FROM {SESSIONS_DB_TABLE} WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None

def delete_session_db(user_id: int):
    cur = db_conn.cursor()
    cur.execute(f"DELETE FROM {SESSIONS_DB_TABLE} WHERE user_id = ?", (user_id,))
    db_conn.commit()

# -----------------------
# Dataclasses & in-memory objects
# -----------------------

@dataclass
class VideoItem:
    file_id: str
    caption: Optional[str] = None
    order: int = 0
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
    thumb_file_id: Optional[str] = None
    thumb_local_path: Optional[str] = None
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

# in-memory cached sessions (kept for speed; backed up to sqlite)
sessions: Dict[int, Session] = {}
sessions_lock = asyncio.Lock()

# load persisted sessions into memory on startup
def load_all_sessions_into_memory():
    cur = db_conn.cursor()
    cur.execute(f"SELECT user_id, session_json FROM {SESSIONS_DB_TABLE}")
    rows = cur.fetchall()
    for row in rows:
        try:
            uid = int(row[0])
            obj = json.loads(row[1])
            s = Session.from_json(obj)
            # ensure fresh lock
            s.lock = asyncio.Lock()
            sessions[uid] = s
        except Exception:
            logger.exception("Failed to load session row for user %s", row[0])

# approved users in-memory is loaded from DB on startup
approved_users = {}
async def load_approved_users_into_memory():
    global approved_users
    approved_users = get_all_approved_users_db()
    # ensure owner is approved (persist if missing)
    if OWNER_ID not in approved_users:
        add_approved_user_db(OWNER_ID, None, OWNER_ID)
        approved_users = get_all_approved_users_db()

# -----------------------
# Bot & Dispatcher
# -----------------------

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# -----------------------
# Utilities
# -----------------------

async def notify_owner(text: str):
    """
    Send a short notification message to the owner. Fail silently if owner can't be reached.
    """
    try:
        await bot.send_message(OWNER_ID, text)
    except Exception:
        logger.exception("Failed to notify owner: %s", text)

def safe_filename_for_session(user_id: int, name: str):
    base = SESSIONS_DIR / str(user_id)
    base.mkdir(parents=True, exist_ok=True)
    # ensure name doesn't include path separators
    name = name.replace("/", "_").replace("\\", "_")
    return str(base / name)

async def is_approved(user_id: int) -> bool:
    # quick in-memory check
    return user_id in approved_users

async def require_approved(message: types.Message) -> bool:
    if await is_approved(message.from_user.id):
        return True
    else:
        try:
            await message.reply("⛔️ You are not authorized to use this bot. Ask the owner for access.")
        except Exception:
            logger.warning("Failed to reply unauthorised to %s", message.from_user.id)
        return False

def persist_session(session: Session):
    """
    Persist session to sqlite DB. Called synchronously (no awaiting).
    """
    try:
        save_session_db(session.user_id, session.to_json())
    except Exception:
        logger.exception("Failed to persist session for user %s", session.user_id)

async def persist_session_async(session: Session):
    # wrap in loop's executor to avoid blocking
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, persist_session, session)

def delete_session_persisted(user_id: int):
    try:
        delete_session_db(user_id)
    except Exception:
        logger.exception("Failed to delete session for user %s", user_id)

# -----------------------
# Thumbnail processing (Pillow)
# -----------------------

def process_image_bytes_to_jpeg_bytes(src_bytes: bytes,
                                     max_bytes: int = THUMB_TARGET_MAX_BYTES,
                                     max_dimension: int = THUMB_MAX_DIMENSION,
                                     start_quality: int = THUMB_START_QUALITY,
                                     min_quality: int = THUMB_MIN_QUALITY) -> bytes:
    """
    Convert image bytes (PNG, JPEG, etc.) -> JPEG bytes under max_bytes while preserving quality.
    Strategy:
      - Load image with Pillow and convert to RGB.
      - If image larger than max_dimension, downscale proportionally to max_dimension.
      - Try saving with start_quality as JPEG; if size <= max_bytes -> done.
      - Otherwise, iteratively reduce quality in steps until min_quality.
      - If still too large, iteratively downscale using THUMB_RESIZE_STEP and retry quality loop.
      - This preserves as much visual quality as possible while meeting size constraints.
    """
    try:
        img = Image.open(io.BytesIO(src_bytes))
    except Exception:
        raise

    # Convert to RGB (JPEG requires no alpha)
    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        # convert with white background (or could use black)
        alpha = img.convert("RGBA").split()[-1]
        bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
        bg.paste(img, mask=alpha)
        img = bg.convert("RGB")
    else:
        img = img.convert("RGB")

    # initial downscale if too large dimension
    def scale_to_max_dimension(image):
        w, h = image.size
        max_side = max(w, h)
        if max_side <= max_dimension:
            return image
        scale = max_dimension / float(max_side)
        new_w = int(round(w * scale))
        new_h = int(round(h * scale))
        return image.resize((new_w, new_h), Image.LANCZOS)

    working = scale_to_max_dimension(img)

    # function to attempt JPEG saving with quality and return bytes
    def attempt_save(image, q):
        bio = io.BytesIO()
        # optimize=True may help reduce size
        image.save(bio, format="JPEG", quality=q, optimize=True, progressive=True)
        return bio.getvalue()

    # first attempt with starting quality
    quality = start_quality
    jpeg_bytes = attempt_save(working, quality)
    if len(jpeg_bytes) <= max_bytes:
        return jpeg_bytes

    # attempt decreasing quality
    while quality >= min_quality:
        jpeg_bytes = attempt_save(working, quality)
        if len(jpeg_bytes) <= max_bytes:
            return jpeg_bytes
        quality -= THUMB_QUALITY_STEP

    # if quality reduction wasn't enough, start reducing dimensions progressively
    scaled_image = working
    scale_factor = THUMB_RESIZE_STEP
    while True:
        # reduce size
        w, h = scaled_image.size
        new_w = max(1, int(round(w * scale_factor)))
        new_h = max(1, int(round(h * scale_factor)))
        if new_w < 16 or new_h < 16:
            # too small, stop
            break
        scaled_image = scaled_image.resize((new_w, new_h), Image.LANCZOS)
        # attempt from start_quality down to min_quality again
        quality = start_quality
        while quality >= min_quality:
            jpeg_bytes = attempt_save(scaled_image, quality)
            if len(jpeg_bytes) <= max_bytes:
                return jpeg_bytes
            quality -= THUMB_QUALITY_STEP
        # continue shrinking
        if new_w <= 64 or new_h <= 64:
            # avoid extremely small thumbnails
            break

    # As a last resort return the last jpeg bytes even if it exceeds max_bytes
    return jpeg_bytes

async def save_and_process_thumbnail(session: Session, file_id: str) -> str:
    """
    Download the thumbnail by file_id, process it with Pillow to ensure it's a JPEG under the size limit, save locally, update session.thumb_local_path and persist session.
    Returns the local path.
    """
    # download file bytes
    f = await bot.get_file(file_id)
    bio = io.BytesIO()
    await bot.download_file(f.file_path, bio)
    src_bytes = bio.getvalue()
    # process bytes
    try:
        jpeg_bytes = await asyncio.get_event_loop().run_in_executor(
            None,
            process_image_bytes_to_jpeg_bytes,
            src_bytes
        )
    except Exception:
        # fallback: try saving original file as JPEG with high quality
        try:
            img = Image.open(io.BytesIO(src_bytes)).convert("RGB")
            out_b = io.BytesIO()
            img.save(out_b, format="JPEG", quality=85, optimize=True, progressive=True)
            jpeg_bytes = out_b.getvalue()
        except Exception:
            # give up, write raw bytes to disk as-is (could be png)
            jpeg_bytes = src_bytes

    # save to local path
    dest_path = safe_filename_for_session(session.user_id, f"thumb_{int(time.time())}.jpg")
    with open(dest_path, "wb") as of:
        of.write(jpeg_bytes)
    session.thumb_local_path = dest_path
    session.thumb_file_id = file_id  # still keep file_id
    persist_session(session)
    logger.info("Saved processed thumbnail for user %s to %s (size=%d)", session.user_id, dest_path, os.path.getsize(dest_path))
    return dest_path

# -----------------------
# File downloading for videos (to local path)
# -----------------------

async def download_video_to_path(file_id: str, user_id: int, index: int) -> str:
    """
    Download a video by file_id to sessions/<user_id>/video_<index>_<ts>.mp4
    Returns local path.
    """
    out_file = safe_filename_for_session(user_id, f"video_{index}_{int(time.time())}.mp4")
    f = await bot.get_file(file_id)
    # download to file in streaming manner
    # aiogram's bot.download_file can accept file path
    with open(out_file, "wb") as wf:
        await bot.download_file(f.file_path, wf)
    logger.info("Downloaded video for user %s to %s", user_id, out_file)
    return out_file

# -----------------------
# Session management helpers
# -----------------------

def session_for_user_create_if_missing(user_id: int) -> Session:
    s = sessions.get(user_id)
    if not s:
        # try load from DB
        obj = load_session_db(user_id)
        if obj:
            s = Session.from_json(obj)
        else:
            s = Session(user_id=user_id)
        s.lock = asyncio.Lock()
        sessions[user_id] = s
    return s

async def ensure_session_loaded(user_id: int) -> Session:
    async with sessions_lock:
        return session_for_user_create_if_missing(user_id)

async def clear_session(user_id: int):
    async with sessions_lock:
        s = sessions.get(user_id)
        if s:
            # attempt to remove local files in session directory
            try:
                base = SESSIONS_DIR / str(user_id)
                # careful to only remove files in that directory
                if base.exists() and base.is_dir():
                    for f in base.iterdir():
                        try:
                            f.unlink()
                        except Exception:
                            pass
                # remove directory optionally
            except Exception:
                logger.exception("Failed to cleanup session files for user %s", user_id)
            # remove session from in-memory and db
            sessions[user_id] = Session(user_id=user_id)
            delete_session_persisted(user_id)
        else:
            # ensure no persisted either
            delete_session_persisted(user_id)

# -----------------------
# Commands & Handlers
# -----------------------

HELP_TEXT = """📼 Thumbnail Session Video Bot — Help

Commands:
/start - Show help and info
/thumb - Start a session (bot will ask for a thumbnail image next)
/done - Begin processing queued videos for this session (one-by-one)
/cancel - Cancel current session and clear queued videos
/adduser <id or @username> - (owner only) approve a user by numeric id or @username
/removeuser <id or @username> - (owner only) remove an approved user
/users - (owner only) list approved users
/session [user_id] - (owner only) debug session state for a user
/health - HTTP health endpoint is available at /health

Notes:
- Each session is per-user. The thumbnail you set with /thumb will apply to videos uploaded after that until you start a new session or /cancel.
- You can send up to 99 videos per session. Videos may be sent as native video or as document (for large files).
- The bot processes videos sequentially (one at a time) when you call /done.
- Thumbnails are processed to ensure they are JPEG and generally under Telegram size limits while preserving quality.
"""

@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.reply("👋 Welcome! This bot attaches your session thumbnail to videos and returns them. Only approved users may use processing commands.\n\n" + HELP_TEXT)

@dp.message_handler(commands=["thumb"])
async def cmd_thumb(message: types.Message):
    if not await require_approved(message):
        return
    session = await ensure_session_loaded(message.from_user.id)
    # start a fresh session by resetting videos and expecting a thumb
    session.expecting_thumb = True
    session.thumb_file_id = None
    # do not delete old local thumb unless user explicitly cancels
    session.videos = []
    session.processing = False
    # persist
    persist_session(session)
    await message.reply("📸 Send the thumbnail image (JPG/PNG). This thumbnail will be used for the current session's videos.")

@dp.message_handler(commands=["cancel"])
async def cmd_cancel(message: types.Message):
    if not await require_approved(message):
        return
    await clear_session(message.from_user.id)
    await message.reply("🗑️ Session canceled and cleared. Use /thumb to start a new session.")

@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    if not await require_approved(message):
        return
    session = await ensure_session_loaded(message.from_user.id)
    if session.processing:
        await message.reply("⏳ Already processing this session. Wait until current processing finishes.")
        return
    if not session.thumb_file_id and not session.thumb_local_path:
        await message.reply("❗ No thumbnail set for this session. Use /thumb and send an image first.")
        return
    if not session.videos:
        await message.reply("❗ No videos in this session. Send videos first, then /done.")
        return

    # Launch background task to process session
    asyncio.create_task(process_session_task(session, message.chat.id))
    await message.reply(f"🚀 Processing started for {len(session.videos)} video(s). I'll send them back one by one when done.")

@dp.message_handler(commands=["adduser"])
async def cmd_adduser(message: types.Message):
    # owner only
    if message.from_user.id != OWNER_ID:
        await message.reply("⛔ Only the owner can add users.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /adduser <numeric_id or @username>")
        return
    target = args.split()[0]
    user_id = None
    username = None
    if target.startswith("@"):
        username_candidate = target[1:]
        try:
            chat = await bot.get_chat(target)
            user_id = chat.id
            username = chat.username or username_candidate
        except Exception:
            # If username hasn't started the bot, we can't resolve ID. Ask owner to provide numeric id.
            await message.reply("⚠️ Could not resolve username to id. Please provide numeric user ID if the user hasn't started the bot.")
            return
    else:
        try:
            user_id = int(target)
            # try to fetch username
            try:
                chat = await bot.get_chat(user_id)
                username = chat.username
            except Exception:
                username = None
        except ValueError:
            await message.reply("Invalid identifier. Use numeric ID or @username.")
            return

    # add to DB
    add_approved_user_db(user_id, username, message.from_user.id)
    approved_users[user_id] = {"username": username, "added_by": message.from_user.id, "added_at": int(time.time())}
    await message.reply(f"✅ User approved: id={user_id} username={('@'+username) if username else 'N/A'}")

@dp.message_handler(commands=["removeuser"])
async def cmd_removeuser(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("⛔ Only the owner can remove users.")
        return
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
        remove_approved_user_db(found)
        approved_users.pop(found, None)
        await message.reply(f"✅ Removed user @{username} (id={found})")
        return
    else:
        try:
            uid = int(target)
        except ValueError:
            await message.reply("Invalid identifier.")
            return
        if uid in approved_users:
            remove_approved_user_db(uid)
            info = approved_users.pop(uid, None)
            await message.reply(f"✅ Removed user id={uid} username={('@'+info['username']) if info and info.get('username') else 'N/A'}")
        else:
            await message.reply("User id not in approved list.")

@dp.message_handler(commands=["users"])
async def cmd_users(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("⛔ Only the owner can view approved users.")
        return
    if not approved_users:
        await message.reply("No approved users.")
        return
    lines = []
    for uid, info in approved_users.items():
        uname = info.get("username") or "N/A"
        added_by = info.get("added_by")
        added_at = info.get("added_at")
        lines.append(f"- {uid} (@{uname if uname!='N/A' else 'N/A'}) added_by={added_by} ts={added_at}")
    await message.reply("Approved users:\n" + "\n".join(lines))

@dp.message_handler(commands=["session"])
async def cmd_session(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("⛔ Only owner can view sessions.")
        return
    args = message.get_args().strip()
    if args:
        try:
            uid = int(args)
            s = sessions.get(uid)
            if not s:
                await message.reply(f"No session for user {uid}")
                return
            text = (
                f"Session for {uid}:\n"
                f" expecting_thumb: {s.expecting_thumb}\n"
                f" processing: {s.processing}\n"
                f" thumb_file_id: {s.thumb_file_id}\n"
                f" thumb_local_path: {s.thumb_local_path}\n"
                f" queued_videos: {len(s.videos)}"
            )
            await message.reply(text)
        except Exception:
            await message.reply("Usage: /session [user_id]")
    else:
        lines = []
        for uid, s in sessions.items():
            lines.append(f"- {uid}: videos={len(s.videos)} proc={s.processing} expecting_thumb={s.expecting_thumb}")
        await message.reply("Sessions:\n" + "\n".join(lines))

# -----------------------
# Media handlers: photo, document, video
# -----------------------

@dp.message_handler(content_types=ContentType.PHOTO)
async def handle_photo(message: types.Message):
    """
    Accepts photo as a thumbnail if the user is expecting thumbnail.
    """
    if not await require_approved(message):
        return
    s = await ensure_session_loaded(message.from_user.id)
    if not s.expecting_thumb:
        await message.reply("I wasn't expecting a thumbnail. Use /thumb to start a session and then send the image.")
        return

    # pick largest size
    photo = message.photo[-1]
    file_id = photo.file_id
    try:
        await message.reply("✅ Thumbnail received. Processing it now...")
        # process thumbnail and save local copy (JPEG under limit)
        await save_and_process_thumbnail(s, file_id)
        s.expecting_thumb = False
        persist_session(s)
        await message.reply("✅ Thumbnail set for this session. Now send your videos (up to 99). When ready send /done.")
    except Exception as e:
        logger.exception("Error processing thumbnail for user %s: %s", s.user_id, e)
        await message.reply("❌ Failed to set thumbnail. Try sending a different image.")
        # notify owner for debugging
        await notify_owner(f"Error processing thumbnail for user {s.user_id}: {e}")

@dp.message_handler(content_types=ContentType.DOCUMENT)
async def handle_document(message: types.Message):
    """
    Document may be:
      - thumbnail (image mime type) when expecting thumb
      - video document (mp4/mkv)
      - other file: ignored
    """
    if not await require_approved(message):
        return
    s = await ensure_session_loaded(message.from_user.id)
    doc = message.document
    mime = (doc.mime_type or "").lower()
    # treat as thumbnail if expecting_thumb and image mime
    if s.expecting_thumb and mime.startswith("image/"):
        await message.reply("✅ Thumbnail (document) received. Processing now...")
        try:
            await save_and_process_thumbnail(s, doc.file_id)
            s.expecting_thumb = False
            persist_session(s)
            await message.reply("✅ Thumbnail set for this session. Now send your videos.")
        except Exception as e:
            logger.exception("Failed processing thumbnail document for user %s: %s", s.user_id, e)
            await message.reply("❌ Failed to set thumbnail from document.")
            await notify_owner(f"Thumbnail doc processing fail for user {s.user_id}: {e}")
        return

    # else treat as video if mime says video or filename suggests video
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".avi", ".webm"))):
        if len(s.videos) >= MAX_VIDEOS_PER_SESSION:
            await message.reply(f"❗ This session already has {MAX_VIDEOS_PER_SESSION} videos. Use /done or /cancel.")
            return
        idx = len(s.videos) + 1
        vi = VideoItem(file_id=doc.file_id, caption=message.caption, order=idx, file_unique_id=doc.file_unique_id, file_size=doc.file_size)
        s.videos.append(vi)
        persist_session(s)
        await message.reply(f"✅ Video {idx} received (document). Send more or /done when ready.")
        return

    await message.reply("I accept images as thumbnails or video files (as video or document).")

@dp.message_handler(content_types=ContentType.VIDEO)
async def handle_video(message: types.Message):
    """
    Handle native video messages.
    """
    if not await require_approved(message):
        return
    s = await ensure_session_loaded(message.from_user.id)
    if s.expecting_thumb:
        await message.reply("Please send thumbnail first (you started /thumb). You sent a video instead.")
        return
    if len(s.videos) >= MAX_VIDEOS_PER_SESSION:
        await message.reply(f"❗ Session already has {MAX_VIDEOS_PER_SESSION} videos.")
        return
    video = message.video
    if video.duration and video.duration > MAX_VIDEO_DURATION_SECS:
        await message.reply(f"❗ Video duration exceeds maximum allowed of {MAX_VIDEO_DURATION_SECS} seconds.")
        return
    idx = len(s.videos) + 1
    vi = VideoItem(file_id=video.file_id, caption=message.caption, order=idx, file_unique_id=video.file_unique_id, file_size=video.file_size)
    s.videos.append(vi)
    persist_session(s)
    await message.reply(f"✅ Video {idx} received. Send more or /done when ready.")

# -----------------------
# Background processing (session processing)
# -----------------------

async def process_session_task(session: Session, reply_chat_id: int):
    """
    Main worker function to process all videos in a session sequentially.
    Ensures exclusive access via session.lock.
    """
    # Acquire lock to prevent duplicate processing
    async with session.lock:
        if session.processing:
            logger.info("Session %s already processing; skipping", session.user_id)
            return
        session.processing = True
        persist_session(session)
        try:
            await safe_send_message(reply_chat_id, f"🔁 Processing {len(session.videos)} video(s). This may take time for large files.")
            # Ensure thumbnail local path exists; if not, try to create it using thumb_file_id
            if not session.thumb_local_path or not os.path.exists(session.thumb_local_path):
                if session.thumb_file_id:
                    try:
                        await save_and_process_thumbnail(session, session.thumb_file_id)
                    except Exception:
                        logger.exception("Failed to regenerate local thumb for user %s", session.user_id)

            # for each video in order
            for idx, vi in enumerate(list(session.videos), start=1):
                try:
                    await safe_send_message(reply_chat_id, f"⏳ Processing video {idx}/{len(session.videos)} ...")
                    local_video_path = await download_video_to_path(vi.file_id, session.user_id, idx)
                    # prepare thumb: prefer local path
                    thumb_param = None
                    if session.thumb_local_path and os.path.exists(session.thumb_local_path):
                        thumb_param = InputFile(session.thumb_local_path)
                    elif session.thumb_file_id:
                        # fallback to remote file_id for thumbnail
                        thumb_param = session.thumb_file_id
                    else:
                        thumb_param = None

                    # send the video back with original caption
                    caption = vi.caption or ""
                    # open file and send
                    with open(local_video_path, "rb") as vf:
                        try:
                            # ensure long timeout
                            await bot.send_video(chat_id=reply_chat_id, video=vf, caption=caption, thumb=thumb_param, timeout=180)
                            await safe_send_message(reply_chat_id, f"✅ Sent video {idx}/{len(session.videos)}")
                        except RetryAfter as r:
                            wait = int(getattr(r, "timeout", 5))
                            logger.warning("RetryAfter while sending video: waiting %s", wait)
                            await asyncio.sleep(wait)
                            # try again
                            vf.seek(0)
                            await bot.send_video(chat_id=reply_chat_id, video=vf, caption=caption, thumb=thumb_param, timeout=180)
                        except TelegramAPIError as te:
                            logger.exception("TelegramAPIError sending video %s: %s", vi.file_id, te)
                            await safe_send_message(reply_chat_id, f"❌ Telegram error sending video {idx}. Skipping.")
                        except Exception:
                            logger.exception("Unexpected error sending video %s", vi.file_id)
                            await safe_send_message(reply_chat_id, f"❌ Unexpected error sending video {idx}. Skipping.")
                    # remove local video file to conserve disk
                    try:
                        os.remove(local_video_path)
                    except Exception:
                        logger.debug("Could not remove local video %s", local_video_path)
                except Exception as e:
                    logger.exception("Error processing video %s for user %s: %s", vi.file_id, session.user_id, e)
                    await safe_send_message(reply_chat_id, f"❌ Failed processing a video: {e}")
            # finished
            # clear queued videos (keep thumbnail available until /thumb or /cancel)
            session.videos = []
            persist_session(session)
            await safe_send_message(reply_chat_id, f"✅ All videos processed for this session.")
        except Exception as e:
            logger.exception("Exception in process_session_task for user %s: %s", session.user_id, e)
            # notify owner with traceback
            tb = traceback.format_exc()
            await notify_owner(f"Error while processing session for user {session.user_id}:\n{e}\n\n{tb[:4000]}")
            await safe_send_message(reply_chat_id, "❌ Error occurred during processing. Owner has been notified.")
        finally:
            session.processing = False
            persist_session(session)

# -----------------------
# Safe message sending wrapper (catches errors and notifies owner)
# -----------------------

async def safe_send_message(chat_id: int, text: str):
    try:
        await bot.send_message(chat_id, text)
    except Exception as e:
        logger.exception("Failed to send message to %s: %s", chat_id, e)
        # notify owner about failure to deliver to user
        try:
            await notify_owner(f"Failed to send message to {chat_id}: {e}")
        except Exception:
            logger.exception("Failed to notify owner about message send failure.")

# -----------------------
# Web / health endpoints (aiohttp app)
# -----------------------

def make_web_app():
    app = web.Application()

    async def health(request):
        return web.json_response({"status": "ok", "owner_id": OWNER_ID, "sessions": len(sessions)})

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
        return web.Response(text="Thumbnail Session Video Bot — webhook mode. /health for healthcheck.")

    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_get("/info", info)
    return app

# -----------------------
# Startup & shutdown
# -----------------------

async def on_startup(dp):
    logger.info("Bot startup: loading approved users and sessions from DB")
    try:
        await load_approved_users_into_memory()
        load_all_sessions_into_memory()
        # set webhook
        webhook_url = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
        await bot.set_webhook(webhook_url)
        logger.info("Webhook set to %s", webhook_url)
    except Exception as e:
        logger.exception("Exception in on_startup: %s", e)
        await notify_owner(f"Bot startup exception: {e}")

async def on_shutdown(dp):
    logger.info("Bot on_shutdown called: cleaning up")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("Failed deleting webhook")
    # persist all sessions
    for uid, sess in sessions.items():
        try:
            persist_session(sess)
        except Exception:
            logger.exception("Failed persisting session for %s", uid)
    logger.info("Shutdown complete.")

# -----------------------
# Unhandled exception hook & task exception monitoring
# -----------------------

def excepthook(exc_type, exc, tb):
    tbtext = "".join(traceback.format_exception(exc_type, exc, tb))
    logger.critical("Uncaught exception: %s", tbtext)
    # Try to notify owner synchronously (we can't await here)
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(notify_owner(f"Uncaught exception in bot process:\n{tbtext[:4000]}"))
    except Exception:
        pass

sys.excepthook = excepthook

# Monitor asyncio tasks for exceptions
def task_exception_handler(loop, context):
    msg = context.get("exception", context.get("message"))
    logger.error("Asyncio task exception: %s", msg)
    try:
        loop.create_task(notify_owner(f"Async task exception: {msg}"))
    except Exception:
        pass

# -----------------------
# Main entrypoint
# -----------------------

def main():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda l, ctx: task_exception_handler(l, ctx))
    # prepare web app for aiogram executor
    app = make_web_app()
    # register startup/shutdown
    try:
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            skip_updates=True,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            host="0.0.0.0",
            port=PORT,
            web_app=app,
        )
    except Exception:
        logger.exception("Exception in start_webhook")
        # try to notify owner
        try:
            loop.run_until_complete(notify_owner("Failed to start webhook. See logs."))
        except Exception:
            pass

if __name__ == "__main__":
    main()