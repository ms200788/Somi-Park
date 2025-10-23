#!/usr/bin/env python3
"""
bot.py - Permanent thumbnail + Pillow processing, single-global-worker, webhook mode.

Requirements:
  - aiogram==2.25.0
  - aiohttp==3.8.6
  - python-dotenv
  - Pillow

Environment (.env):
  BOT_TOKEN, OWNER_ID, WEBHOOK_HOST, PORT (default 10000), WEBHOOK_PATH (default /webhook)
  MAX_REUPLOAD_BYTES (optional) - default 4500000000 (4.5GB)
  SELF_PING_INTERVAL (optional) - seconds to self-ping health endpoint, default 60
  WATCHDOG_INTERVAL (optional) - seconds, default 120

Notes:
  - Bot processes videos one at a time globally (to safely handle large files).
  - Uses streaming download and streaming re-upload with InputFile to minimize memory usage.
  - Pillow is used to prepare a Telegram-friendly thumbnail (<=320x320 and size-ish <=200KB typically).
  - Settings and mappings are stored in SQLite under ./data to persist across restarts.
  - If host cannot hold the file, the bot will instruct forwarding to a private channel where bot is admin.
"""

import os
import sys
import json
import time
import sqlite3
import asyncio
import logging
import traceback
import tempfile
import pathlib
import shutil
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from functools import wraps
from contextlib import suppress

from dotenv import load_dotenv
from aiohttp import web, ClientSession, ClientTimeout
from aiogram import Bot, Dispatcher, types
from aiogram.types import ContentType, InputFile
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError, BadRequest

from PIL import Image, ImageOps

# -------------------------
# Load environment
# -------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN not set in .env")
    sys.exit(1)

OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
if not OWNER_ID_RAW:
    print("ERROR: OWNER_ID not set in .env")
    sys.exit(1)
try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    print("ERROR: OWNER_ID must be numeric")
    sys.exit(1)

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "").strip()
if not WEBHOOK_HOST:
    print("ERROR: WEBHOOK_HOST not set in .env (must be public https url)")
    sys.exit(1)

PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH
WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

MAX_REUPLOAD_BYTES = int(os.getenv("MAX_REUPLOAD_BYTES", str(4_500_000_000)))  # 4.5GB default
SELF_PING_INTERVAL = int(os.getenv("SELF_PING_INTERVAL", "60"))
WATCHDOG_INTERVAL = int(os.getenv("WATCHDOG_INTERVAL", "120"))

# Tunables
DOWNLOAD_CHUNK = int(os.getenv("DOWNLOAD_CHUNK", str(2 * 1024 * 1024)))  # 2MB
PROGRESS_BAR_LEN = int(os.getenv("PROGRESS_BAR_LEN", "24"))
MAX_QUEUE_PER_USER = int(os.getenv("MAX_QUEUE_PER_USER", "1000"))
MEDIA_GROUP_AGG_WINDOW = float(os.getenv("MEDIA_GROUP_AGG_WINDOW", "0.7"))

# File limits for Telegram thumbnail (we'll try to make small jpeg)
THUMB_MAX_DIM = 320
THUMB_FORMAT = "JPEG"
THUMB_QUALITY = 85  # Pillow quality setting (may be adjusted to stay under size limit)

# -------------------------
# Paths & prepare dirs
# -------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
TMP_DIR = DATA_DIR / "tmp"
DB_PATH = DATA_DIR / "coverbot_data.sqlite"
LAST_UPDATE_PATH = DATA_DIR / "last_update.json"

for d in (DATA_DIR, LOGS_DIR, TMP_DIR):
    d.mkdir(parents=True, exist_ok=True)

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
logger = logging.getLogger("thumb_cover_bot")

# -------------------------
# Dataclasses
# -------------------------
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
    queue: List[QueuedVideo] = field(default_factory=list)
    processing: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def to_json(self) -> dict:
        return {"user_id": self.user_id, "queue": [q.to_json() for q in self.queue], "processing": self.processing}

    @staticmethod
    def from_json(obj: dict) -> "UserSession":
        s = UserSession(user_id=obj["user_id"])
        s.queue = [QueuedVideo.from_json(q) for q in obj.get("queue", [])]
        s.processing = obj.get("processing", False)
        return s

@dataclass
class OwnerSettings:
    owner_id: int
    thumb_file_id: Optional[str] = None
    prefix: str = ""
    suffix: str = ""

    def to_json(self) -> dict:
        return {"owner_id": self.owner_id, "thumb_file_id": self.thumb_file_id, "prefix": self.prefix, "suffix": self.suffix}

    @staticmethod
    def from_json(obj: dict) -> "OwnerSettings":
        return OwnerSettings(owner_id=obj.get("owner_id"), thumb_file_id=obj.get("thumb_file_id"), prefix=obj.get("prefix", ""), suffix=obj.get("suffix", ""))

# -------------------------
# Database helpers (SQLite)
# -------------------------
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS owner_settings (
            owner_id INTEGER PRIMARY KEY,
            json TEXT,
            updated_at INTEGER
        )
    """)
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

def save_owner_settings(settings: OwnerSettings):
    try:
        ts = int(time.time())
        cur = db.cursor()
        cur.execute("INSERT OR REPLACE INTO owner_settings (owner_id, json, updated_at) VALUES (?, ?, ?)",
                    (settings.owner_id, json.dumps(settings.to_json()), ts))
        db.commit()
    except Exception:
        logger.exception("save_owner_settings failed")

def load_owner_settings() -> OwnerSettings:
    try:
        cur = db.cursor()
        cur.execute("SELECT json FROM owner_settings WHERE owner_id = ?", (OWNER_ID,))
        row = cur.fetchone()
        if not row:
            s = OwnerSettings(owner_id=OWNER_ID)
            save_owner_settings(s)
            return s
        return OwnerSettings.from_json(json.loads(row[0]))
    except Exception:
        logger.exception("load_owner_settings failed; returning default")
        return OwnerSettings(owner_id=OWNER_ID)

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

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN)
Bot.set_current(bot)
dp = Dispatcher(bot)

# -------------------------
# Runtime caches & global worker lock
# -------------------------
owner_settings = load_owner_settings()
sessions: Dict[int, UserSession] = {}
perf_history: Dict[int, List[float]] = {}
_media_group_buffers: Dict[str, List[types.Message]] = {}
_media_group_timers: Dict[str, asyncio.Handle] = {}

# Global lock ensuring only a single video is processed at once across all users
global_processing_lock = asyncio.Lock()

# -------------------------
# Utilities
# -------------------------
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
    d, h = divmod(h, 24)
    return f"{d}d {h}h"

def progress_bar(percent: float, length: int = PROGRESS_BAR_LEN) -> str:
    percent = max(0.0, min(1.0, percent))
    filled = int(round(length * percent))
    return "▰" * filled + "▱" * (length - filled)

def owner_allowed_at() -> Optional[str]:
    for src in (owner_settings.prefix, owner_settings.suffix):
        if src:
            toks = src.split()
            for t in toks:
                if t.startswith("@"):
                    return t
    return None

def clean_at_mentions(text: str, allowed_at: Optional[str]) -> str:
    if not text:
        return text
    parts = text.split()
    out = []
    for p in parts:
        if p.startswith("@"):
            if allowed_at and p.lower() == allowed_at.lower():
                out.append(p)
            else:
                continue
        else:
            out.append(p)
    if allowed_at:
        seen = False
        final = []
        for p in out:
            if p.lower() == allowed_at.lower():
                if not seen:
                    final.append(p)
                    seen = True
                else:
                    continue
            else:
                final.append(p)
        out = final
    return " ".join(out).strip()

def sanitize_filename(original_name: Optional[str], prefix: str, suffix: str, owner_at: Optional[str]) -> str:
    if original_name:
        base, ext = os.path.splitext(original_name)
        if not ext:
            ext = ".mp4"
    else:
        base = "video"
        ext = ".mp4"
    base_clean = clean_at_mentions(base, owner_at) or "video"
    parts = []
    if prefix:
        parts.append(prefix.strip())
    parts.append(base_clean.strip())
    if suffix:
        parts.append(suffix.strip())
    name = " ".join([p for p in parts if p])
    name = " ".join(name.split())
    name = "".join(c for c in name if c not in '/\\?%*:|"<>')
    return f"{name}{ext}"

def apply_prefix_suffix_to_caption(orig_caption: Optional[str], prefix: str, suffix: str, owner_at: Optional[str]) -> str:
    text = orig_caption or ""
    text = clean_at_mentions(text, owner_at)
    parts = []
    if prefix:
        parts.append(prefix.strip())
    if text:
        parts.append(text.strip())
    if suffix:
        parts.append(suffix.strip())
    return " ".join([p for p in parts if p]).strip()

async def notify_owner(text: str):
    try:
        await bot.send_message(OWNER_ID, text)
    except Exception:
        logger.exception("notify_owner failed")

# -------------------------
# Thumbnail processing w/ Pillow
# -------------------------
def prepare_thumb_from_file(input_path: pathlib.Path) -> pathlib.Path:
    """
    Load image from input_path, convert/resize/crop to Telegram-friendly thumb, and save to tmp file.
    Returns path to thumbnail file (JPEG).
    """
    try:
        im = Image.open(input_path).convert("RGB")
    except Exception as e:
        logger.exception("Pillow open failed: %s", e)
        raise

    # create a square thumbnail by padding then resizing to THUMB_MAX_DIM
    # preserve aspect ratio, pad with black
    size = THUMB_MAX_DIM
    im_thumb = ImageOps.fit(im, (size, size), method=Image.LANCZOS)
    # optionally add subtle sharpening or watermark here (not required)
    out_tmp = tempfile.NamedTemporaryFile(prefix="thumb_", suffix=".jpg", dir=str(TMP_DIR), delete=False)
    out_path = pathlib.Path(out_tmp.name)
    out_tmp.close()

    # try saving and if file too big, reduce quality iteratively
    quality = THUMB_QUALITY
    for attempt in range(4):
        try:
            im_thumb.save(out_path, THUMB_FORMAT, quality=quality, optimize=True)
            # check size — prefer < 200 KB but Telegram accepts up to some limit; we try to be safe
            size_bytes = out_path.stat().st_size
            if size_bytes > 250 * 1024 and quality > 40:
                quality = max(40, quality - 15)
                continue
            break
        except Exception:
            quality = max(40, quality - 10)
            continue
    return out_path

# -------------------------
# Safe handler decorator ensures Bot.set_current
# -------------------------
def safe_handler(fn):
    @wraps(fn)
    async def wrapper(message: types.Message, *args, **kwargs):
        try:
            Bot.set_current(bot)
        except Exception:
            logger.debug("Bot.set_current failed (non-fatal)")
        try:
            return await fn(message, *args, **kwargs)
        except Exception:
            logger.exception("Handler exception %s", fn.__name__)
            tb = traceback.format_exc()
            try:
                await notify_owner(f"Handler {fn.__name__} exception:\n{tb[:2000]}")
            except Exception:
                pass
            try:
                await message.reply("⚠️ Internal error. Owner notified.")
            except Exception:
                pass
    return wrapper

# -------------------------
# Command handlers (owner-only settings)
# -------------------------
HELP_TEXT = (
    "🎬 Permanent-cover bot (single-file worker)\n\n"
    "Owner commands:\n"
    " /setthumb (reply to photo) — set permanent thumbnail\n"
    " /getthumb or /show_cover — preview thumbnail\n"
    " /delthumb — delete stored thumbnail\n"
    " /addpre <text> — set prefix applied to filenames & captions\n"
    " /addend <text> — set suffix applied to filenames & captions\n"
    " /viewtags — show prefix/suffix\n"
    " /deltags — clear prefix & suffix\n\n"
    "User commands:\n"
    " Send video(s) or video files — bot will process them one at a time.\n"
    " /cancel — clear your queued videos\n"
)

@dp.message_handler(commands=["start", "help"])
@safe_handler
async def cmd_start(message: types.Message):
    await message.reply(HELP_TEXT)

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

@dp.message_handler(commands=["setthumb"])
@safe_handler
async def cmd_setthumb(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("❌ Only owner can set the thumbnail.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a photo (or image document) with /setthumb to set the permanent thumbnail.")
        return
    replied = message.reply_to_message
    if replied.photo:
        # store file_id
        owner_settings.thumb_file_id = replied.photo[-1].file_id
        save_owner_settings(owner_settings)
        await message.reply("✅ Permanent thumbnail saved (by file_id).")
        return
    if replied.document and (replied.document.mime_type or "").startswith("image/"):
        owner_settings.thumb_file_id = replied.document.file_id
        save_owner_settings(owner_settings)
        await message.reply("✅ Permanent thumbnail saved (image document).")
        return
    await message.reply("Reply to a photo to set thumbnail.")

@dp.message_handler(commands=["getthumb", "show_cover"])
@safe_handler
async def cmd_getthumb(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("❌ Only owner can view the thumbnail.")
        return
    if not owner_settings.thumb_file_id:
        await message.reply("❗ No thumbnail set.")
        return
    try:
        await bot.send_photo(chat_id=message.chat.id, photo=owner_settings.thumb_file_id, caption="📸 Permanent thumbnail (stored)")
    except Exception:
        logger.exception("Failed to show stored thumbnail")
        await message.reply("Failed to show thumbnail.")

@dp.message_handler(commands=["delthumb"])
@safe_handler
async def cmd_delthumb(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("❌ Only owner can delete thumbnail.")
        return
    owner_settings.thumb_file_id = None
    save_owner_settings(owner_settings)
    await message.reply("🗑 Permanent thumbnail removed.")

@dp.message_handler(commands=["addpre"])
@safe_handler
async def cmd_addpre(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("❌ Only owner can set prefix.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /addpre <text>")
        return
    owner_settings.prefix = args
    save_owner_settings(owner_settings)
    await message.reply(f"✅ Prefix set: {owner_settings.prefix}")

@dp.message_handler(commands=["addend"])
@safe_handler
async def cmd_addend(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("❌ Only owner can set suffix.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /addend <text>")
        return
    owner_settings.suffix = args
    save_owner_settings(owner_settings)
    await message.reply(f"✅ Suffix set: {owner_settings.suffix}")

@dp.message_handler(commands=["viewtags"])
@safe_handler
async def cmd_viewtags(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("❌ Only owner can view tags.")
        return
    pre = owner_settings.prefix or "(none)"
    suf = owner_settings.suffix or "(none)"
    await message.reply(f"Prefix: {pre}\nSuffix: {suf}")

@dp.message_handler(commands=["deltags"])
@safe_handler
async def cmd_deltags(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("❌ Only owner can clear tags.")
        return
    owner_settings.prefix = ""
    owner_settings.suffix = ""
    save_owner_settings(owner_settings)
    await message.reply("✅ Prefix and suffix cleared.")

# -------------------------
# Queue & message handlers
# -------------------------
@dp.message_handler(commands=["cancel"])
@safe_handler
async def cmd_cancel(message: types.Message):
    s = get_session(message.from_user.id)
    async with s.lock:
        qlen = len(s.queue)
        s.queue = []
        s.processing = False
        save_session_db(s)
    await message.reply(f"🗑 Cleared {qlen} queued videos.")

@dp.message_handler(content_types=ContentType.PHOTO)
@safe_handler
async def handle_photo(message: types.Message):
    await message.reply("Reply to this photo with /setthumb to set it as permanent thumbnail (owner only).")

@dp.message_handler(content_types=ContentType.DOCUMENT)
@safe_handler
async def handle_document(message: types.Message):
    if message.document and (message.document.mime_type or "").startswith("image/"):
        await message.reply("Reply with /setthumb to store this image as permanent thumbnail (owner only).")
        return
    doc = message.document
    mime = (doc.mime_type or "").lower()
    if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
        s = get_session(message.from_user.id)
        if len(s.queue) >= MAX_QUEUE_PER_USER:
            await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel.")
            return
        q = QueuedVideo(file_id=doc.file_id, caption=message.caption, file_unique_id=getattr(doc, "file_unique_id", None), file_size=getattr(doc, "file_size", None))
        s.queue.append(q)
        save_session_db(s)
        await message.reply(f"✅ Video queued ({len(s.queue)} in queue).")
        # start global worker if idle
        asyncio.create_task(global_queue_worker())
        return
    await message.reply("I accept video documents and photos for thumbnail.")

@dp.message_handler(content_types=ContentType.VIDEO)
@safe_handler
async def handle_video(message: types.Message):
    s = get_session(message.from_user.id)
    video = message.video
    if len(s.queue) >= MAX_QUEUE_PER_USER:
        await message.reply(f"❗ Queue full (max {MAX_QUEUE_PER_USER}). Use /cancel.")
        return
    q = QueuedVideo(file_id=video.file_id, caption=message.caption, file_unique_id=getattr(video, "file_unique_id", None), file_size=getattr(video, "file_size", None))
    s.queue.append(q)
    save_session_db(s)
    await message.reply(f"✅ Video queued ({len(s.queue)} in queue).")
    asyncio.create_task(global_queue_worker())

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
        h = loop.call_later(MEDIA_GROUP_AGG_WINDOW, _flush)
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
    user = buf[0].from_user
    s = get_session(user.id)
    queued = 0
    for msg in buf:
        if msg.video:
            if len(s.queue) < MAX_QUEUE_PER_USER:
                q = QueuedVideo(file_id=msg.video.file_id, caption=msg.caption, file_unique_id=getattr(msg.video, "file_unique_id", None), file_size=getattr(msg.video, "file_size", None))
                s.queue.append(q)
                queued += 1
        elif msg.document:
            doc = msg.document
            mime = (doc.mime_type or "").lower()
            if mime.startswith("video/") or (doc.file_name and doc.file_name.lower().endswith((".mp4", ".mkv", ".mov", ".webm"))):
                if len(s.queue) < MAX_QUEUE_PER_USER:
                    q = QueuedVideo(file_id=doc.file_id, caption=msg.caption, file_unique_id=getattr(doc, "file_unique_id", None), file_size=getattr(doc, "file_size", None))
                    s.queue.append(q)
                    queued += 1
    save_session_db(s)
    if queued:
        asyncio.create_task(global_queue_worker())

# -------------------------
# Session helpers
# -------------------------
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

# -------------------------
# Networking helpers
# -------------------------
async def get_cdn_file_url(file_id: str) -> Optional[str]:
    try:
        f = await bot.get_file(file_id)
        fp = getattr(f, "file_path", None)
        if not fp:
            return None
        return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{fp}"
    except RetryAfter as r:
        wait = int(getattr(r, "timeout", 5))
        logger.warning("get_file RetryAfter %s", wait)
        await asyncio.sleep(wait + 1)
        return None
    except TelegramAPIError as te:
        logger.warning("get_file TelegramAPIError: %s", te)
        return None
    except Exception:
        logger.exception("get_file failed unexpectedly")
        return None

async def stream_download(url: str, dest_path: pathlib.Path, expected_size: Optional[int] = None, progress_cb=None):
    timeout = ClientTimeout(total=None, sock_connect=30, sock_read=600)
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

# -------------------------
# Single-global-worker orchestration
# -------------------------
processing_task: Optional[asyncio.Task] = None

async def global_queue_worker():
    """
    Single global worker loop runner. If already running, this function returns immediately.
    It finds the next queued video across all sessions in FIFO by session -> queue order.
    Processes exactly one video at a time (global lock ensures exclusivity).
    """
    global processing_task
    if processing_task and not processing_task.done():
        # worker already active
        return
    loop = asyncio.get_event_loop()
    processing_task = loop.create_task(_global_worker_main())

async def _global_worker_main():
    """
    Main loop: keep scanning sessions for any queued items and process them one by one.
    Stops when no queued items exist.
    """
    while True:
        # pick the next queued item across sessions in send order (approx: by session.user_id insertion time)
        next_pair = None  # (session, queued_video)
        # simple fair selection: iterate sessions by insertion order in dict
        for uid, sess in list(sessions.items()):
            if sess.queue:
                next_pair = (sess, sess.queue[0])
                break
        if not next_pair:
            # no work
            return
        sess, q = next_pair
        # process single queued video under a global lock to ensure only one large job at a time
        async with global_processing_lock:
            await process_single_video(sess, q)
        # after processing, continue loop to find next
        await asyncio.sleep(0.1)

# -------------------------
# Core: process one queued video
# -------------------------
async def process_single_video(session: UserSession, q: QueuedVideo):
    """
    Processes one QueuedVideo:
      - If mapping exists: send mapped new_file_id instantly
      - Else try get_file -> CDN URL -> send_video(video=file_url, thumb=thumb_prepared)
         if Telegram attaches thumb -> save mapping
         else if file_size <= MAX_REUPLOAD_BYTES -> stream download -> prepare thumb via Pillow -> re-upload with thumb -> save mapping
         else instruct to forward to private channel or provide URL
    This function ensures single-file processing and gives progress updates.
    """
    user_chat = session.user_id  # reply back to user in their own chat
    # mark session as processing to avoid concurrent modifications
    async with session.lock:
        session.processing = True
        save_session_db(session)

    processed_time = time.time()
    # announce start
    try:
        await bot.send_message(user_chat, f"⏳ Starting processing of your video...")
    except Exception:
        logger.debug("Failed to send starting message")

    # fast-path mapping
    if q.file_unique_id:
        mapping = load_mapping(q.file_unique_id)
        if mapping and mapping.get("new_file_id"):
            mapped_file_id = mapping["new_file_id"]
            try:
                await bot.send_video(chat_id=user_chat, video=mapped_file_id, caption=(q.caption or ""))
                # remove from queue
                async with session.lock:
                    if session.queue and session.queue[0].file_unique_id == q.file_unique_id:
                        session.queue.pop(0)
                        save_session_db(session)
                try:
                    await bot.send_message(user_chat, "✅ Sent cached processed video.")
                except Exception:
                    pass
                return
            except RetryAfter as r:
                wait = int(getattr(r, "timeout", 5))
                logger.warning("RetryAfter when sending mapped file: %s", wait)
                await asyncio.sleep(wait + 1)
            except Exception:
                logger.exception("Failed to send mapped file -> will attempt recreation")

    # get CDN url
    file_url = await get_cdn_file_url(q.file_id)
    if not file_url:
        # notify and remove from queue
        try:
            await bot.send_message(user_chat,
                "⚠️ Bot cannot access this file via Bot API (Telegram blocked or file is too big). "
                "To allow re-upload and permanent thumbnail, forward the original message to a private channel where this bot is admin, then send it from that channel. Alternatively, provide a direct URL."
            )
        except Exception:
            pass
        async with session.lock:
            if session.queue and session.queue[0].file_id == q.file_id:
                session.queue.pop(0)
                save_session_db(session)
        return

    # attempt server-side send using CDN URL & owner's thumb file_id if set
    try:
        # if owner has set a thumb_file_id, try send with it; this may work fast without reupload
        sent = None
        try:
            if owner_settings.thumb_file_id:
                sent = await bot.send_video(chat_id=user_chat, video=file_url, caption=(q.caption or ""), thumb=owner_settings.thumb_file_id)
            else:
                # if no owner thumb, just send a copy (we still may want to rename caption/filename)
                sent = await bot.send_video(chat_id=user_chat, video=file_url, caption=(q.caption or ""))
        except RetryAfter as r:
            wait = int(getattr(r, "timeout", 5))
            logger.warning("RetryAfter when trying server-side send: %s", wait)
            await asyncio.sleep(wait + 1)
            sent = None
        except BadRequest as br:
            logger.warning("BadRequest during server-side send: %s", br)
            sent = None
        except TelegramAPIError as te:
            logger.exception("TelegramAPIError during server-side send: %s", te)
            sent = None
        except Exception as e:
            logger.exception("Unexpected exception during server-side send: %s", e)
            sent = None

        # if sent and thumb applied (if we asked for a thumb), save mapping
        applied = False
        if sent:
            try:
                if getattr(sent, "video", None) and getattr(sent.video, "thumb", None):
                    applied = True
            except Exception:
                applied = False

        if sent and (not owner_settings.thumb_file_id or applied):
            # If no owner thumb, we still consider sent OK (but not "permanent thumb")
            # Save mapping if thumb is applied or if we reuploaded previously mapped
            try:
                if getattr(sent, "video", None):
                    new_file_id = sent.video.file_id
                    new_file_unique = getattr(sent.video, "file_unique_id", None)
                    if q.file_unique_id and new_file_id:
                        save_mapping(q.file_unique_id, new_file_id, new_file_unique)
            except Exception:
                logger.exception("Failed to save mapping after server-side send")
            # pop queue
            async with session.lock:
                if session.queue and session.queue[0].file_id == q.file_id:
                    session.queue.pop(0)
                    save_session_db(session)
            if applied:
                await bot.send_message(user_chat, "✅ Sent (server-side) and thumbnail attached.")
            else:
                await bot.send_message(user_chat, "✅ Sent (server-side) but Telegram didn't attach thumbnail. The bot will now attempt re-upload to ensure permanence.")
                # fall through to re-upload attempt
        else:
            # no server-side success or no thumb attached; attempt reupload for permanence
            pass

    except Exception:
        logger.exception("Unexpected error during server-side attempt")

    # Next: decide if we should download+re-upload
    # get file_size (from q or via HEAD)
    file_size = q.file_size
    if not file_size:
        try:
            async with ClientSession() as sess:
                async with sess.head(file_url) as resp:
                    cl = resp.headers.get("Content-Length")
                    if cl:
                        file_size = int(cl)
        except Exception:
            file_size = None

    if not file_size or file_size > MAX_REUPLOAD_BYTES:
        # can't automatically reupload due to unknown size or too big (exceeds MAX)
        try:
            await bot.send_message(user_chat,
                "⚠️ I cannot automatically download & re-upload this file (size unknown or exceeds MAX_REUPLOAD_BYTES). "
                "To allow attachment of a permanent thumbnail, forward the original message to a private channel where this bot is an admin and send from there, or provide a direct URL."
            )
        except Exception:
            pass
        async with session.lock:
            if session.queue and session.queue[0].file_id == q.file_id:
                session.queue.pop(0)
                save_session_db(session)
        return

    # Else: proceed to stream download to temporary file and re-upload with thumb created by Pillow
    tmp_file = None
    tmp_file_path = None
    thumb_tmp_path = None
    try:
        # allocate temp file
        tmp = tempfile.NamedTemporaryFile(prefix="video_dl_", suffix=".mp4", dir=str(TMP_DIR), delete=False)
        tmp_file_path = pathlib.Path(tmp.name)
        tmp.close()

        # progress callback
        last_progress_msg_ts = 0
        def progress_cb(written, total):
            nonlocal last_progress_msg_ts
            now = time.time()
            # throttle logging/stats updates
            if now - last_progress_msg_ts > 8:
                pct = (written / total) if total else 0.0
                logger.info("Downloading: %.2f%% (%s/%s)", pct * 100, written, total)
                last_progress_msg_ts = now

        await bot.send_message(user_chat, f"🔁 Downloading ({file_size} bytes) — will stream to disk and then re-upload with permanent thumbnail. This may take a while.")

        await stream_download(file_url, tmp_file_path, expected_size=file_size, progress_cb=progress_cb)

        # Prepare thumbnail: if owner has set a thumb_file_id, we can fetch that file and process; else we instruct to set thumb
        if owner_settings.thumb_file_id:
            # get file for owner thumb, download it into TMP, then process via Pillow
            thumb_url = await get_cdn_file_url(owner_settings.thumb_file_id)
            thumb_src_path = None
            if thumb_url:
                tf = tempfile.NamedTemporaryFile(prefix="owner_thumb_src_", suffix=".img", dir=str(TMP_DIR), delete=False)
                thumb_src_path = pathlib.Path(tf.name)
                tf.close()
                try:
                    await stream_download(thumb_url, thumb_src_path, expected_size=None)
                except Exception:
                    logger.exception("Failed to download owner thumb via CDN")
                    thumb_src_path.unlink(missing_ok=True)
                    thumb_src_path = None
            # If we couldn't get owner thumb as image file, we cannot prepare Pillow thumb; but we can try to use file_id directly as thumb param (Telegram accepts file_id)
            if thumb_src_path and thumb_src_path.exists():
                thumb_tmp_path = prepare_thumb_from_file(thumb_src_path)
                # cleanup original downloaded thumb src
                with suppress(Exception):
                    thumb_src_path.unlink()
            else:
                # fallback: we will attempt to use owner_settings.thumb_file_id directly as thumb param (works if Telegram accepts it)
                thumb_tmp_path = None

        else:
            # No owner thumb set: we can't prepare a custom thumbnail — ask owner to set one and abort re-upload
            await bot.send_message(user_chat, "❗ No owner thumbnail set. Ask owner to /setthumb (reply to an image). Aborting re-upload.")
            async with session.lock:
                if session.queue and session.queue[0].file_id == q.file_id:
                    session.queue.pop(0)
                    save_session_db(session)
            return

        # Build final filename based on prefix/suffix and original (best-effort)
        original_name = f"video_{int(time.time())}.mp4"
        final_filename = sanitize_filename(original_name, owner_settings.prefix, owner_settings.suffix, owner_allowed_at())
        final_caption = apply_prefix_suffix_to_caption(q.caption, owner_settings.prefix, owner_settings.suffix, owner_allowed_at())

        # re-upload via InputFile streaming from disk
        await bot.send_message(user_chat, "🔼 Re-uploading to Telegram (this uploads the full file).")

        with open(tmp_file_path, "rb") as fh:
            input_file = InputFile(fh, filename=final_filename)
            # choose thumb param: prefer prepared thumb file if available else owner_settings.thumb_file_id file_id
            thumb_param = None
            if thumb_tmp_path and thumb_tmp_path.exists():
                thumb_param = InputFile(open(thumb_tmp_path, "rb"))
            else:
                thumb_param = owner_settings.thumb_file_id  # file_id

            sent2 = await bot.send_video(chat_id=user_chat, video=input_file, caption=final_caption, thumb=thumb_param)

        # check result and persist mapping
        applied2 = False
        try:
            if getattr(sent2, "video", None) and getattr(sent2.video, "thumb", None):
                applied2 = True
        except Exception:
            applied2 = False

        if applied2:
            try:
                new_id = sent2.video.file_id
                new_unique = getattr(sent2.video, "file_unique_id", None)
                if q.file_unique_id:
                    save_mapping(q.file_unique_id, new_id, new_unique)
            except Exception:
                logger.exception("Failed to save mapping after reupload")
            await bot.send_message(user_chat, "✅ Re-upload complete — permanent thumbnail applied.")
        else:
            await bot.send_message(user_chat, "⚠️ Re-upload finished but Telegram didn't attach the thumbnail (unexpected). Owner notified.")
            await notify_owner(f"Re-upload finished for user {session.user_id} but no thumb attached. Check logs.")
    except RetryAfter as r:
        wait = int(getattr(r, "timeout", 5))
        logger.warning("RetryAfter in download/reupload: %s", wait)
        await asyncio.sleep(wait + 1)
        await bot.send_message(user_chat, f"⏳ Rate limit hit, retrying in {wait+1}s")
    except Exception:
        logger.exception("Exception during download/reupload")
        try:
            await bot.send_message(user_chat, "❌ Failed to download/reupload. Owner will be notified.")
        except Exception:
            pass
        await notify_owner(f"Download/reupload failed for user {session.user_id} file {q.file_unique_id or q.file_id}:\n{traceback.format_exc()[:2000]}")
    finally:
        # cleanup temp files
        with suppress(Exception):
            if tmp_file_path and tmp_file_path.exists():
                tmp_file_path.unlink()
        with suppress(Exception):
            if thumb_tmp_path and thumb_tmp_path.exists():
                thumb_tmp_path.unlink()
        # remove first queue element
        async with session.lock:
            if session.queue and session.queue[0].file_id == q.file_id:
                session.queue.pop(0)
                save_session_db(session)
            session.processing = False
            save_session_db(session)

# -------------------------
# Helper to notify user when get_file fails (more verbose)
# -------------------------
async def notify_user_getfile_failed(user_id: int, chat_id: int, q: QueuedVideo):
    try:
        await bot.send_message(chat_id,
            "⚠️ I couldn't access that video via the Bot API (Telegram blocked the file or it is too large). "
            "To apply a permanent thumbnail, forward the original message to a private channel where this bot is admin and then send it from there, or provide a direct URL for the file."
        )
    except Exception:
        logger.exception("notify_user_getfile_failed failed")

# -------------------------
# Webhook handler
# -------------------------
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
    except Exception:
        logger.debug("Bot.set_current failed (non-fatal)")

    try:
        data = json.loads(raw)
    except Exception:
        return web.Response(status=400, text="Bad JSON")

    try:
        update = types.Update.de_json(data, bot)
    except Exception:
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse Update")
            return web.Response(status=400, text="Bad Update")

    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Exception during dp.process_update")
        try:
            await notify_owner(f"Handler exception:\n{traceback.format_exc()[:2000]}")
        except Exception:
            pass
        return web.Response(text="OK")
    return web.Response(text="OK")

# -------------------------
# Health endpoints & tasks
# -------------------------
async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "owner_id": OWNER_ID,
        "sessions": len(sessions),
        "runtime_thumb": bool(owner_settings.thumb_file_id)
    })

async def self_ping_task(app: web.Application, interval: int = SELF_PING_INTERVAL):
    await asyncio.sleep(2)
    while True:
        try:
            await bot.get_me()
        except Exception:
            logger.exception("self_ping get_me failed")
        try:
            async with ClientSession() as sess:
                url = WEBHOOK_HOST.rstrip("/") + "/health"
                async with sess.get(url, timeout=10) as resp:
                    logger.debug("self_ping status %s", resp.status)
        except Exception:
            logger.debug("self_ping failed (non-fatal)")
        await asyncio.sleep(interval)

async def watchdog_task(app: web.Application, interval: int = WATCHDOG_INTERVAL):
    await asyncio.sleep(5)
    while True:
        try:
            info = await bot.get_webhook_info()
            url = getattr(info, "url", None)
            if not url or url.rstrip("/") != WEBHOOK_URL.rstrip("/"):
                logger.warning("watchdog detected webhook mismatch; resetting")
                try:
                    await bot.delete_webhook(drop_pending_updates=False)
                except Exception:
                    logger.debug("delete_webhook failed (non-fatal)")
                try:
                    await bot.set_webhook(WEBHOOK_URL)
                    logger.info("watchdog set webhook to %s", WEBHOOK_URL)
                except Exception:
                    logger.exception("watchdog failed to set webhook")
                    try:
                        await notify_owner(f"Watchdog failed to set webhook to {WEBHOOK_URL}")
                    except Exception:
                        pass
        except Exception:
            logger.exception("watchdog unexpected error")
        await asyncio.sleep(interval)

# -------------------------
# Load sessions at startup
# -------------------------
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

# -------------------------
# Exception hooks & graceful shutdown
# -------------------------
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

def setup_signal_handlers(loop):
    try:
        import signal
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(shutdown_signal(sig)))
    except Exception:
        logger.debug("Signals not set (platform limitation)")

async def shutdown_signal(sig):
    logger.info("Received signal %s, shutting down", sig)
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("delete_webhook failed during shutdown")
    os._exit(0)

# -------------------------
# Startup/shutdown hooks
# -------------------------
async def on_startup(app: web.Application):
    logger.info("on_startup: setting webhook to %s", WEBHOOK_URL)
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set")
    except BadRequest as e:
        logger.exception("BadRequest setting webhook: %s", e)
        await notify_owner(f"Failed to set webhook: {e}")
        raise
    except Exception:
        logger.exception("Unexpected error setting webhook")
        await notify_owner("Unexpected error setting webhook on startup; check logs")
    load_all_sessions()
    app["self_ping"] = asyncio.create_task(self_ping_task(app))
    app["watchdog"] = asyncio.create_task(watchdog_task(app))
    logger.info("Background tasks started")

async def on_shutdown(app: web.Application):
    logger.info("on_shutdown: deleting webhook and cancelling tasks")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("delete_webhook failed during shutdown")
    for key in ("self_ping", "watchdog"):
        t = app.get(key)
        if t:
            t.cancel()
            with suppress(Exception):
                await t
    logger.info("Shutdown complete")

# -------------------------
# Main entrypoint
# -------------------------
def main():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda l, ctx: task_exception_handler(l, ctx))
    setup_signal_handlers(loop)
    Bot.set_current(bot)
    load_all_sessions()
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/info", lambda req: web.json_response({"status":"ok","owner":OWNER_ID}))
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    logger.info("Starting aiohttp server on 0.0.0.0:%s webhook_path=%s", PORT, WEBHOOK_PATH)
    try:
        web.run_app(app, host="0.0.0.0", port=PORT)
    except Exception:
        logger.exception("web.run_app failed")
        try:
            loop.run_until_complete(notify_owner("Server failed to start; check logs"))
        except Exception:
            pass

if __name__ == "__main__":
    main()