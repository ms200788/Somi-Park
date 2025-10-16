import os
import asyncio
import logging
import math
import time
from typing import Dict, List
from functools import wraps
from dotenv import load_dotenv

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import InputFile, InputMediaVideo
from aiogram.utils.executor import start_webhook

# ------------------ Load environment ------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")  # e.g. https://your-app-name.onrender.com
PORT = int(os.getenv("PORT", 10000))
WEBHOOK_PATH = f"/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

# ------------------ Logging ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------ Bot / Dispatcher ------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# ------------------ In-memory storage ------------------
SESSIONS: Dict[int, Dict] = {}  # user_id -> session
APPROVED_USERS: List[int] = [OWNER_ID]  # initially only owner
VIDEO_QUEUE: Dict[int, List[types.Message]] = {}  # user_id -> list of videos
THUMBNAILS: Dict[int, str] = {}  # user_id -> path to current session thumbnail
PROCESSING: Dict[int, bool] = {}  # user_id -> is processing

# ------------------ Utils ------------------
def owner_only(func):
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.from_user.id != OWNER_ID:
            await message.reply("❌ Only owner can use this command.")
            return
        return await func(message, *args, **kwargs)
    return wrapper

def approved_only(func):
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.from_user.id not in APPROVED_USERS:
            await message.reply("❌ You are not approved to use this bot.")
            return
        return await func(message, *args, **kwargs)
    return wrapper

def format_progress_bar(percent: float, length: int = 20) -> str:
    filled = int(length * percent)
    empty = length - filled
    return "[" + "█" * filled + "─" * empty + "]"

# ------------------ Commands ------------------

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.reply(
        "👋 Welcome!\n\n"
        "Use /thumb to set your session thumbnail.\n"
        "Send videos (1-99 per session).\n"
        "When ready, use /done to start processing.\n"
        "Owner can use /adduser <id|@username> to approve new users."
    )

@dp.message_handler(commands=["adduser"])
@owner_only
async def cmd_adduser(message: types.Message):
    args = message.get_args().split()
    if not args:
        await message.reply("Usage: /adduser <id|@username>")
        return
    for arg in args:
        try:
            if arg.startswith("@"):
                user = await bot.get_chat(arg)
                uid = user.id
            else:
                uid = int(arg)
            if uid not in APPROVED_USERS:
                APPROVED_USERS.append(uid)
        except Exception as e:
            await message.reply(f"❌ Failed to add {arg}: {e}")
            continue
    await message.reply(f"✅ Approved users: {APPROVED_USERS}")

@dp.message_handler(commands=["thumb"])
@approved_only
async def cmd_thumb(message: types.Message):
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.reply("📌 Reply to a photo with /thumb to set it as thumbnail.")
        return
    photo = message.reply_to_message.photo[-1]
    file_path = f"thumb_{message.from_user.id}.jpg"
    await photo.download(destination_file=file_path)
    THUMBNAILS[message.from_user.id] = file_path
    await message.reply("✅ Thumbnail saved for this session.")

@dp.message_handler(commands=["done"])
@approved_only
async def cmd_done(message: types.Message):
    user_id = message.from_user.id
    if user_id not in VIDEO_QUEUE or not VIDEO_QUEUE[user_id]:
        await message.reply("❌ No videos queued. Send videos first.")
        return
    if PROCESSING.get(user_id, False):
        await message.reply("⚠ Already processing videos.")
        return
    PROCESSING[user_id] = True
    await message.reply("🚀 Starting video processing...")
    asyncio.create_task(process_videos(user_id, message.chat.id))

# ------------------ Video Handling ------------------

@dp.message_handler(content_types=[types.ContentType.VIDEO])
@approved_only
async def handle_video(message: types.Message):
    user_id = message.from_user.id
    if user_id not in VIDEO_QUEUE:
        VIDEO_QUEUE[user_id] = []
    VIDEO_QUEUE[user_id].append(message)
    await message.reply(f"✅ Video queued. Total queued: {len(VIDEO_QUEUE[user_id])}")

async def process_videos(user_id: int, chat_id: int):
    queue = VIDEO_QUEUE[user_id]
    total = len(queue)
    processed = 0
    start_time = time.time()
    thumb_path = THUMBNAILS.get(user_id)

    while queue:
        msg = queue.pop(0)
        processed += 1
        file_id = msg.video.file_id
        caption = msg.caption or ""
        file_name = f"video_{user_id}_{int(time.time())}.mp4"

        # Download video
        video_file = await bot.get_file(file_id)
        await video_file.download(destination_file=file_name)

        # Send video with thumbnail
        try:
            thumb_file = InputFile(thumb_path) if thumb_path else None
            await bot.send_video(
                chat_id=chat_id,
                video=InputFile(file_name),
                caption=caption,
                thumb=thumb_file
            )
        except Exception as e:
            logger.error(f"Failed to send video: {e}")
        finally:
            # Delete temp video
            if os.path.exists(file_name):
                os.remove(file_name)

        # Progress calculation
        elapsed = time.time() - start_time
        remaining = total - processed
        est_time = (elapsed / processed) * remaining if processed else 0
        percent = processed / total
        progress_bar = format_progress_bar(percent)

        await bot.send_message(
            chat_id,
            f"📊 Progress: {progress_bar} {processed}/{total}\n"
            f"⏱ Elapsed: {int(elapsed)}s | ⏳ Remaining: {int(est_time)}s"
        )

    # Session cleanup
    PROCESSING[user_id] = False
    VIDEO_QUEUE[user_id] = []
    await bot.send_message(chat_id, "✅ All videos processed!")

# ------------------ Health check & self-ping ------------------

async def health_check(request):
    return web.Response(text="OK")

async def self_ping():
    while True:
        try:
            await bot.get_me()
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")
        await asyncio.sleep(60)  # ping every 60s

# ------------------ Webhook ------------------

async def on_startup(app):
    # Set webhook
    await bot.set_webhook(WEBHOOK_URL)
    # Start self-ping task
    app["ping_task"] = asyncio.create_task(self_ping())

async def on_shutdown(app):
    # Delete webhook
    await bot.delete_webhook()
    app["ping_task"].cancel()

app = web.Application()
app.router.add_post(WEBHOOK_PATH, lambda request: dp.start_polling())
app.router.add_get("/health", health_check)

# ------------------ Run App ------------------
if __name__ == "__main__":
    start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host="0.0.0.0",
        port=PORT
    )