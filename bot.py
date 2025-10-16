import os
import asyncio
import logging
import time
from functools import wraps
from typing import Dict, List
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import InputFile
from aiogram.utils.executor import start_webhook
from dotenv import load_dotenv

# ------------------ Load environment ------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")  # https://your-app-name.onrender.com
PORT = int(os.getenv("PORT", 10000))
WEBHOOK_PATH = "/webhook"
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
APPROVED_USERS: List[int] = [OWNER_ID]
VIDEO_QUEUE: Dict[int, List[types.Message]] = {}
THUMBNAILS: Dict[int, str] = {}
PROCESSING: Dict[int, bool] = {}

# ------------------ Utilities ------------------
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
        "Use /done to process videos.\n"
        "Owner can /adduser <id|@username> to approve new users."
    )

@dp.message_handler(commands=["adduser"])
@owner_only
async def cmd_adduser(message: types.Message):
    args = message.get_args().split()
    if not args:
        await message.reply("Usage: /adduser <id|@username>")
        return
    added = []
    for arg in args:
        try:
            if arg.startswith("@"):
                user = await bot.get_chat(arg)
                uid = user.id
            else:
                uid = int(arg)
            if uid not in APPROVED_USERS:
                APPROVED_USERS.append(uid)
                added.append(uid)
        except Exception as e:
            await message.reply(f"❌ Failed to add {arg}: {e}")
    await message.reply(f"✅ Approved users: {APPROVED_USERS}\nAdded: {added}")

@dp.message_handler(commands=["thumb"])
@approved_only
async def cmd_thumb(message: types.Message):
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.reply("📌 Reply to a photo with /thumb to set as session thumbnail.")
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
    thumb_file = InputFile(thumb_path) if thumb_path else None

    while queue:
        msg = queue.pop(0)
        processed += 1
        percent = processed / total
        elapsed = time.time() - start_time
        remaining = total - processed
        est_time = (elapsed / processed) * remaining if processed else 0
        progress_bar = format_progress_bar(percent)

        # Send video using file_id with thumbnail
        try:
            await bot.send_video(
                chat_id=chat_id,
                video=msg.video.file_id,
                caption=msg.caption or "",
                thumb=thumb_file
            )
        except Exception as e:
            logger.error(f"Failed to send video: {e}")
            await bot.send_message(chat_id, f"❌ Failed to send video: {e}")

        # Send progress update
        await bot.send_message(
            chat_id,
            f"📊 Progress: {progress_bar} {processed}/{total}\n"
            f"⏱ Elapsed: {int(elapsed)}s | ⏳ Remaining: {int(est_time)}s"
        )

    # Cleanup
    PROCESSING[user_id] = False
    VIDEO_QUEUE[user_id] = []
    await bot.send_message(chat_id, "✅ All videos processed!")

# ------------------ Health Check / Self-Ping ------------------
async def health_check(request):
    return web.Response(text="OK")

async def self_ping():
    while True:
        try:
            await bot.get_me()
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")
        await asyncio.sleep(60)

# ------------------ Webhook Setup ------------------
async def on_startup(app):
    await bot.set_webhook(WEBHOOK_URL)
    app["ping_task"] = asyncio.create_task(self_ping())

async def on_shutdown(app):
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