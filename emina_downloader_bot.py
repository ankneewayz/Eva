"""
EminaDownloader
Single-file Telegram downloader bot using a self-hosted Cobalt API.

Environment variables:
BOT_TOKEN=your_telegram_bot_token
COBALT_API=https://your-cobalt-service.onrender.com
OWNER_ID=8598993143

Optional:
REQUIRED_CHANNEL=@J4KERS
REQUIRED_GROUP=@ankneewayzgrp
FREE_DAILY_LIMIT=10
MAX_FILE_SIZE_MB=50
MAX_GLOBAL_DOWNLOADS=3
DATABASE_PATH=emina.db
CACHE_DIR=cache
REQUIRE_MEMBERSHIP=true
START_VIDEO_PINTEREST_URL=https://www.pinterest.com/...
"""

import asyncio
import html
import json
import logging
import os
import re
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import aiosqlite
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ============================================================
# CONFIG
# ============================================================

def env_int(name, default):
    try:
        return int(os.getenv(name, str(default)).strip())
    except (TypeError, ValueError):
        return default


def env_bool(name, default=True):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
COBALT_API = os.getenv(
    "COBALT_API",
    "https://cobalt-latest-gg37.onrender.com",
).strip().rstrip("/")

OWNER_ID = env_int("OWNER_ID", 8598993143)

REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "@J4KERS").strip()
REQUIRED_GROUP = os.getenv("REQUIRED_GROUP", "@ankneewayzgrp").strip()
REQUIRE_MEMBERSHIP = env_bool("REQUIRE_MEMBERSHIP", True)

FREE_DAILY_LIMIT = env_int("FREE_DAILY_LIMIT", 10)
MAX_FILE_SIZE_MB = env_int("MAX_FILE_SIZE_MB", 50)
MAX_GLOBAL_DOWNLOADS = env_int("MAX_GLOBAL_DOWNLOADS", 3)

DATABASE_PATH = os.getenv("DATABASE_PATH", "emina.db").strip()
CACHE_DIR = os.getenv("CACHE_DIR", "cache").strip()

START_VIDEO_PINTEREST_URL = os.getenv(
    "START_VIDEO_PINTEREST_URL", ""
).strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required.")

if not COBALT_API:
    raise RuntimeError("COBALT_API environment variable is required.")

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("EminaDownloader")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ============================================================
# DATABASE
# ============================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    joined_at TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    plan TEXT NOT NULL DEFAULT 'FREE',
    premium_until TEXT,
    usage_date TEXT,
    downloads_today INTEGER NOT NULL DEFAULT 0,
    total_downloads INTEGER NOT NULL DEFAULT 0,
    banned INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS downloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    status TEXT NOT NULL,
    filename TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_downloads_user
ON downloads(user_id);

CREATE INDEX IF NOT EXISTS idx_users_plan
ON users(plan);
"""

DB_LOCK = asyncio.Lock()


def now_utc():
    return datetime.now(timezone.utc)


def now_iso():
    return now_utc().isoformat()


def today():
    return now_utc().strftime("%Y-%m-%d")


async def init_db():
    Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()

    logger.info("Database initialized")


async def get_or_create_user(user_id, username=None, first_name=None):
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row

            cur = await db.execute(
                "SELECT * FROM users WHERE user_id=?",
                (user_id,),
            )
            row = await cur.fetchone()

            current = now_iso()

            if row is None:
                await db.execute(
                    """
                    INSERT INTO users (
                        user_id, username, first_name,
                        joined_at, last_seen, plan,
                        premium_until, usage_date,
                        downloads_today, total_downloads, banned
                    )
                    VALUES (?, ?, ?, ?, ?, 'FREE', NULL, ?, 0, 0, 0)
                    """,
                    (
                        user_id,
                        username,
                        first_name,
                        current,
                        current,
                        today(),
                    ),
                )
            else:
                await db.execute(
                    """
                    UPDATE users
                    SET username=?, first_name=?, last_seen=?
                    WHERE user_id=?
                    """,
                    (username, first_name, current, user_id),
                )

            await db.commit()

            cur = await db.execute(
                "SELECT * FROM users WHERE user_id=?",
                (user_id,),
            )
            row = await cur.fetchone()

            return dict(row)


async def get_user(user_id):
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row

            cur = await db.execute(
                "SELECT * FROM users WHERE user_id=?",
                (user_id,),
            )
            row = await cur.fetchone()

            if not row:
                return {}

            user = dict(row)

            # Daily reset
            if user["usage_date"] != today():
                await db.execute(
                    """
                    UPDATE users
                    SET usage_date=?, downloads_today=0
                    WHERE user_id=?
                    """,
                    (today(), user_id),
                )
                await db.commit()
                user["usage_date"] = today()
                user["downloads_today"] = 0

            # Premium expiration
            if (
                user["plan"] == "PREMIUM"
                and user["premium_until"]
                and user_id != OWNER_ID
            ):
                try:
                    expiry = datetime.fromisoformat(
                        user["premium_until"]
                    )

                    if expiry <= now_utc():
                        await db.execute(
                            """
                            UPDATE users
                            SET plan='FREE', premium_until=NULL
                            WHERE user_id=?
                            """,
                            (user_id,),
                        )
                        await db.commit()

                        user["plan"] = "FREE"
                        user["premium_until"] = None

                except ValueError:
                    pass

            if user_id == OWNER_ID:
                user["plan"] = "OWNER"

            return user


async def increment_download(user_id):
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute(
                """
                UPDATE users
                SET downloads_today=downloads_today+1,
                    total_downloads=total_downloads+1
                WHERE user_id=?
                """,
                (user_id,),
            )
            await db.commit()


async def record_download(user_id, url, status, filename=None):
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute(
                """
                INSERT INTO downloads
                (user_id, url, status, filename, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    url,
                    status,
                    filename,
                    now_iso(),
                ),
            )
            await db.commit()


async def is_banned(user_id):
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            cur = await db.execute(
                "SELECT banned FROM users WHERE user_id=?",
                (user_id,),
            )
            row = await cur.fetchone()
            return bool(row and row[0])


async def set_ban(user_id, value):
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            cur = await db.execute(
                "SELECT user_id FROM users WHERE user_id=?",
                (user_id,),
            )

            if not await cur.fetchone():
                return False

            await db.execute(
                "UPDATE users SET banned=? WHERE user_id=?",
                (1 if value else 0, user_id),
            )
            await db.commit()

            return True


async def add_premium(user_id, days):
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row

            cur = await db.execute(
                "SELECT * FROM users WHERE user_id=?",
                (user_id,),
            )
            row = await cur.fetchone()

            if not row:
                return False, "User not found. Ask them to /start first."

            user = dict(row)
            start = now_utc()

            if user["premium_until"]:
                try:
                    existing = datetime.fromisoformat(
                        user["premium_until"]
                    )
                    if existing > start:
                        start = existing
                except ValueError:
                    pass

            expiry = start + timedelta(days=days)

            await db.execute(
                """
                UPDATE users
                SET plan='PREMIUM', premium_until=?
                WHERE user_id=?
                """,
                (expiry.isoformat(), user_id),
            )

            await db.commit()

            return True, expiry.strftime("%d %b %Y")


async def remove_premium(user_id):
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            cur = await db.execute(
                "SELECT user_id FROM users WHERE user_id=?",
                (user_id,),
            )

            if not await cur.fetchone():
                return False

            await db.execute(
                """
                UPDATE users
                SET plan='FREE', premium_until=NULL
                WHERE user_id=?
                """,
                (user_id,),
            )
            await db.commit()

            return True


async def get_all_user_ids():
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            cur = await db.execute("SELECT user_id FROM users")
            rows = await cur.fetchall()
            return [row[0] for row in rows]


async def stats():
    async with DB_LOCK:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            result = {}

            queries = {
                "users": "SELECT COUNT(*) FROM users",
                "premium": "SELECT COUNT(*) FROM users WHERE plan='PREMIUM'",
                "banned": "SELECT COUNT(*) FROM users WHERE banned=1",
                "downloads": "SELECT COUNT(*) FROM downloads",
                "success": "SELECT COUNT(*) FROM downloads WHERE status='success'",
                "failed": "SELECT COUNT(*) FROM downloads WHERE status='failed'",
            }

            for key, query in queries.items():
                cur = await db.execute(query)
                result[key] = (await cur.fetchone())[0]

            cur = await db.execute(
                """
                SELECT COUNT(*) FROM downloads
                WHERE status='success' AND created_at LIKE ?
                """,
                (today() + "%",),
            )
            result["today"] = (await cur.fetchone())[0]

            return result


# ============================================================
# HELPERS
# ============================================================

URL_RE = re.compile(r"^https?://", re.I)


def valid_url(value):
    if not URL_RE.match(value.strip()):
        return False

    try:
        parsed = urlparse(value.strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except ValueError:
        return False


def safe_filename(filename):
    filename = os.path.basename(filename or "")
    filename = filename.replace("\x00", "")
    filename = re.sub(
        r"[^A-Za-z0-9._ \-]",
        "_",
        filename,
    )
    filename = filename.strip(" .")

    if not filename:
        filename = "emina_" + uuid.uuid4().hex[:10]

    return filename[:150]


def esc(value):
    return html.escape(str(value or ""))


def is_owner(user_id):
    return user_id == OWNER_ID


def plan(user):
    if user.get("user_id") == OWNER_ID:
        return "OWNER"

    return user.get("plan", "FREE")


def can_download(user):
    current_plan = plan(user)

    if current_plan in ("OWNER", "PREMIUM"):
        return True

    return user.get("downloads_today", 0) < FREE_DAILY_LIMIT


# ============================================================
# HTTP / COBALT
# ============================================================

HTTP_SESSION = None


async def get_session():
    global HTTP_SESSION

    if HTTP_SESSION is None or HTTP_SESSION.closed:
        timeout = aiohttp.ClientTimeout(
            total=90,
            connect=15,
        )

        HTTP_SESSION = aiohttp.ClientSession(
            timeout=timeout,
        )

    return HTTP_SESSION


@dataclass
class CobaltResult:
    ok: bool
    kind: str | None = None
    url: str | None = None
    filename: str | None = None
    picker: list | None = None
    error: str | None = None


async def cobalt_download(source_url):
    payload = {
        "url": source_url,
        "downloadMode": "auto",
        "videoQuality": "1080",
        "audioFormat": "mp3",
        "audioBitrate": "128",
        "filenameStyle": "basic",
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    session = await get_session()

    try:
        async with session.post(
            f"{COBALT_API}/",
            json=payload,
            headers=headers,
        ) as response:

            try:
                data = await response.json(
                    content_type=None
                )
            except (ValueError, json.JSONDecodeError):
                return CobaltResult(
                    False,
                    error="⚠ Cobalt returned an invalid response.",
                )

            if response.status == 429:
                return CobaltResult(
                    False,
                    error="⌛ Downloader is busy. Try again shortly.",
                )

            if response.status in (401, 403):
                return CobaltResult(
                    False,
                    error="⚠ Cobalt rejected this request.",
                )

            if response.status >= 500:
                return CobaltResult(
                    False,
                    error="⚠ Downloader service is temporarily unavailable.",
                )

            if not isinstance(data, dict):
                return CobaltResult(
                    False,
                    error="⚠ Unexpected Cobalt response.",
                )

            status = data.get("status")

            if status == "error":
                error = data.get("error", {})
                code = (
                    error.get("code", "")
                    if isinstance(error, dict)
                    else str(error)
                )

                logger.info(
                    "Cobalt error: %s",
                    code,
                )

                return CobaltResult(
                    False,
                    error="❌ This link could not be downloaded.",
                )

            if status == "picker":
                items = data.get("picker") or []

                if not items:
                    return CobaltResult(
                        False,
                        error="❌ No downloadable media found.",
                    )

                return CobaltResult(
                    True,
                    kind="picker",
                    picker=items,
                )

            if status in ("redirect", "tunnel"):
                media_url = data.get("url")

                if not media_url:
                    return CobaltResult(
                        False,
                        error="❌ Cobalt did not return a media URL.",
                    )

                return CobaltResult(
                    True,
                    kind=status,
                    url=media_url,
                    filename=data.get("filename"),
                )

            return CobaltResult(
                False,
                error="⚠ Unknown Cobalt response.",
            )

    except asyncio.TimeoutError:
        return CobaltResult(
            False,
            error="⌛ Cobalt took too long to respond.",
        )

    except aiohttp.ClientError as exc:
        logger.warning(
            "Cobalt connection error: %s",
            exc,
        )

        return CobaltResult(
            False,
            error="⚠ Could not connect to Cobalt.",
        )


# ============================================================
# MEDIA DOWNLOAD
# ============================================================

@dataclass
class DownloadedFile:
    path: str
    size: int
    filename: str


async def fetch_media(media_url, directory, suggested_name=None):
    filename = safe_filename(
        suggested_name or
        f"emina_{uuid.uuid4().hex[:10]}.mp4"
    )

    path = os.path.join(
        directory,
        filename,
    )

    max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    total = 0

    session = await get_session()

    try:
        async with session.get(media_url) as response:

            if response.status != 200:
                logger.warning(
                    "Media returned HTTP %s",
                    response.status,
                )
                return None

            content_length = response.headers.get(
                "Content-Length"
            )

            if content_length:
                try:
                    if int(content_length) > max_bytes:
                        return DownloadedFile(
                            "",
                            int(content_length),
                            filename,
                        )
                except ValueError:
                    pass

            with open(path, "wb") as file:
                async for chunk in response.content.iter_chunked(
                    256 * 1024
                ):
                    total += len(chunk)

                    if total > max_bytes:
                        file.close()

                        try:
                            os.remove(path)
                        except OSError:
                            pass

                        return DownloadedFile(
                            "",
                            total,
                            filename,
                        )

                    file.write(chunk)

    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None

    return DownloadedFile(
        path,
        total,
        filename,
    )


# ============================================================
# CONCURRENCY
# ============================================================

GLOBAL_SEMAPHORE = asyncio.Semaphore(
    MAX_GLOBAL_DOWNLOADS
)

ACTIVE_USERS = set()


class DownloadBusy(Exception):
    pass


class GlobalBusy(Exception):
    pass


class DownloadGuard:
    def __init__(self, user_id):
        self.user_id = user_id
        self.locked = False

    async def __aenter__(self):
        if self.user_id in ACTIVE_USERS:
            raise DownloadBusy()

        try:
            await asyncio.wait_for(
                GLOBAL_SEMAPHORE.acquire(),
                timeout=0.01,
            )
        except asyncio.TimeoutError:
            raise GlobalBusy()

        self.locked = True
        ACTIVE_USERS.add(self.user_id)

        return self

    async def __aexit__(self, exc_type, exc, tb):
        ACTIVE_USERS.discard(self.user_id)

        if self.locked:
            GLOBAL_SEMAPHORE.release()


# ============================================================
# MEMBERSHIP
# ============================================================

MEMBER_STATUSES = {
    ChatMemberStatus.MEMBER,
    ChatMemberStatus.ADMINISTRATOR,
    ChatMemberStatus.OWNER,
}


async def member_check(bot, chat, user_id):
    try:
        member = await bot.get_chat_member(
            chat_id=chat,
            user_id=user_id,
        )

        return member.status in MEMBER_STATUSES

    except (BadRequest, Forbidden, TelegramError) as exc:
        logger.warning(
            "Membership check failed: %s",
            exc,
        )
        return False


async def has_required_membership(bot, user_id):
    if not REQUIRE_MEMBERSHIP:
        return True

    if is_owner(user_id):
        return True

    channel = await member_check(
        bot,
        REQUIRED_CHANNEL,
        user_id,
    )

    group = await member_check(
        bot,
        REQUIRED_GROUP,
        user_id,
    )

    return channel and group


# ============================================================
# UI
# ============================================================

def main_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "⚡ Download",
                callback_data="download",
            ),
            InlineKeyboardButton(
                "👑 Premium",
                callback_data="premium",
            ),
        ],
        [
            InlineKeyboardButton(
                "📊 Stats",
                callback_data="stats",
            ),
            InlineKeyboardButton(
                "❓ Help",
                callback_data="help",
            ),
        ],
    ])


def membership_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "📢 Join Channel",
                url="https://t.me/J4KERS",
            )
        ],
        [
            InlineKeyboardButton(
                "👥 Join Group",
                url="https://t.me/ankneewayzgrp",
            )
        ],
        [
            InlineKeyboardButton(
                "✓ Verify",
                callback_data="verify",
            )
        ],
    ])


def premium_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "💎 Premium — ₹50/month",
                callback_data="contact_owner",
            )
        ],
        [
            InlineKeyboardButton(
                "👤 Contact Owner",
                url=f"tg://user?id={OWNER_ID}",
            )
        ],
        [
            InlineKeyboardButton(
                "« Back",
                callback_data="back",
            )
        ],
    ])


def back_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "« Back",
                callback_data="back",
            )
        ]
    ])


def limit_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "👑 Get Premium",
                callback_data="premium",
            )
        ]
    ])


def premium_text():
    return (
        "╭──── ✦ <b>EMINA PREMIUM</b> ✦ ────╮\n\n"
        "💎 <b>₹50 / MONTH</b>\n\n"
        "∞ Unlimited downloads\n"
        "⚡ No daily limit\n"
        "✦ Premium access\n\n"
        "To activate Premium, contact the owner.\n\n"
        "╰────────────────────────────╯"
    )


def start_text(user):
    current_plan = plan(user)

    if current_plan == "OWNER":
        usage = "Unlimited"
        plan_text = "✦ Owner"
    elif current_plan == "PREMIUM":
        usage = "Unlimited"
        plan_text = "👑 Premium"
    else:
        usage = (
            f"{user.get('downloads_today', 0)}"
            f"/{FREE_DAILY_LIMIT}"
        )
        plan_text = "Free"

    return (
        "✦ <b>EMINA DOWNLOADER</b>\n"
        "<i>Fast • Clean • Premium</i>\n\n"
        "Send a supported link and Emina will\n"
        "handle the download for you.\n\n"
        f"Plan: <b>{plan_text}</b>\n"
        f"Today: <b>{usage}</b>"
    )


# ============================================================
# START VIDEO
# ============================================================

async def get_start_video():
    if not START_VIDEO_PINTEREST_URL:
        return None

    cache = Path(CACHE_DIR)
    cache.mkdir(parents=True, exist_ok=True)

    existing = list(
        cache.glob("start_video.*")
    )

    for file in existing:
        if file.is_file() and file.stat().st_size > 0:
            return str(file)

    result = await cobalt_download(
        START_VIDEO_PINTEREST_URL
    )

    if not result.ok or not result.url:
        logger.warning(
            "Start video Cobalt fetch failed: %s",
            result.error,
        )
        return None

    filename = safe_filename(
        result.filename or "start_video.mp4"
    )

    extension = (
        Path(filename).suffix.lower()
        or ".mp4"
    )

    destination = cache / (
        "start_video" + extension
    )

    downloaded = await fetch_media(
        result.url,
        str(cache),
        "start_video" + extension,
    )

    if not downloaded or not downloaded.path:
        return None

    try:
        shutil.move(
            downloaded.path,
            destination,
        )
    except OSError:
        return downloaded.path

    return str(destination)


# ============================================================
# START
# ============================================================

async def start(update, context):
    user = update.effective_user

    if not user:
        return

    await get_or_create_user(
        user.id,
        user.username,
        user.first_name,
    )

    if await is_banned(user.id):
        await update.effective_message.reply_text(
            "🚫 Access restricted."
        )
        return

    if not await has_required_membership(
        context.bot,
        user.id,
    ):
        await update.effective_message.reply_text(
            "╭──── ✦ <b>WELCOME TO EMINA</b> ✦ ────╮\n\n"
            "Join our channel and group to unlock\n"
            "EminaDownloader.\n\n"
            "After joining, tap <b>Verify</b>.\n\n"
            "╰────────────────────────────╯",
            parse_mode=ParseMode.HTML,
            reply_markup=membership_keyboard(),
        )
        return

    profile = await get_user(user.id)

    text = start_text(profile)
    keyboard = main_keyboard()

    video = None

    try:
        video = await get_start_video()
    except Exception:
        logger.exception(
            "Unable to fetch start video"
        )

    if video:
        try:
            with open(video, "rb") as file:
                await update.effective_message.reply_video(
                    video=InputFile(file),
                    caption=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
            return
        except TelegramError:
            logger.warning(
                "Start video send failed"
            )

    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


# ============================================================
# DOWNLOAD
# ============================================================

async def send_file(update, context, downloaded):
    if not downloaded.path:
        return False

    filename = downloaded.filename.lower()
    chat_id = update.effective_chat.id

    try:
        if filename.endswith(
            (".mp4", ".mov", ".mkv", ".webm")
        ):
            try:
                with open(downloaded.path, "rb") as file:
                    await context.bot.send_video(
                        chat_id=chat_id,
                        video=InputFile(file),
                        filename=downloaded.filename,
                    )
                return True
            except TelegramError:
                pass

        if filename.endswith(
            (".mp3", ".m4a", ".ogg", ".wav", ".flac")
        ):
            try:
                with open(downloaded.path, "rb") as file:
                    await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=InputFile(file),
                        filename=downloaded.filename,
                    )
                return True
            except TelegramError:
                pass

        with open(downloaded.path, "rb") as file:
            await context.bot.send_document(
                chat_id=chat_id,
                document=InputFile(file),
                filename=downloaded.filename,
            )

        return True

    except TelegramError as exc:
        logger.warning(
            "Telegram send failed: %s",
            exc,
        )
        return False

    finally:
        try:
            os.remove(downloaded.path)
        except OSError:
            pass


async def process_download(update, context, user_id, url):
    status = await update.effective_message.reply_text(
        "⌁ <b>Receiving link...</b>",
        parse_mode=ParseMode.HTML,
    )

    try:
        await status.edit_text(
            "⚡ <b>Connecting to Emina...</b>",
            parse_mode=ParseMode.HTML,
        )

        result = await cobalt_download(url)

        if not result.ok:
            await status.edit_text(
                result.error or "❌ Download failed.",
            )

            await record_download(
                user_id,
                url,
                "failed",
            )
            return

        if result.kind == "picker":
            items = result.picker[:8]

            keyboard = []

            for index, item in enumerate(items):
                if not isinstance(item, dict):
                    continue

                item_type = str(
                    item.get("type", "Media")
                ).title()

                keyboard.append([
                    InlineKeyboardButton(
                        f"◈ {item_type} {index + 1}",
                        callback_data=f"pick:{index}",
                    )
                ])

            context.user_data["picker"] = {
                "items": items,
                "source_url": url,
            }

            await status.edit_text(
                "◈ <b>Choose media</b>\n\n"
                "This link contains multiple downloadable items.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    keyboard
                ),
            )
            return

        await status.edit_text(
            "◈ <b>Processing media...</b>",
            parse_mode=ParseMode.HTML,
        )

        with tempfile.TemporaryDirectory(
            prefix="emina_"
        ) as directory:

            downloaded = await fetch_media(
                result.url,
                directory,
                result.filename,
            )

            if not downloaded:
                await status.edit_text(
                    "❌ Could not download the media."
                )

                await record_download(
                    user_id,
                    url,
                    "failed",
                )
                return

            if not downloaded.path:
                await status.edit_text(
                    f"❌ File is larger than "
                    f"{MAX_FILE_SIZE_MB} MB."
                )

                await record_download(
                    user_id,
                    url,
                    "failed",
                    downloaded.filename,
                )
                return

            await status.edit_text(
                "⬇ <b>Sending media...</b>",
                parse_mode=ParseMode.HTML,
            )

            sent = await send_file(
                update,
                context,
                downloaded,
            )

            if not sent:
                await status.edit_text(
                    "❌ Telegram couldn't send this file."
                )

                await record_download(
                    user_id,
                    url,
                    "failed",
                    downloaded.filename,
                )
                return

            await increment_download(user_id)

            await record_download(
                user_id,
                url,
                "success",
                downloaded.filename,
            )

            try:
                await status.delete()
            except TelegramError:
                pass

    except Exception:
        logger.exception(
            "Download processing error"
        )

        try:
            await status.edit_text(
                "❌ Something went wrong. Try again."
            )
        except TelegramError:
            pass

        await record_download(
            user_id,
            url,
            "failed",
        )


async def url_handler(update, context):
    user = update.effective_user
    text = (
        update.effective_message.text or ""
    ).strip()

    if not user or not text:
        return

    if await is_banned(user.id):
        await update.effective_message.reply_text(
            "🚫 Access restricted."
        )
        return

    if not valid_url(text):
        await update.effective_message.reply_text(
            "❌ Please send a valid http/https link."
        )
        return

    await get_or_create_user(
        user.id,
        user.username,
        user.first_name,
    )

    if not await has_required_membership(
        context.bot,
        user.id,
    ):
        await update.effective_message.reply_text(
            "🔒 <b>Join required</b>\n\n"
            "Join both the channel and group before downloading.",
            parse_mode=ParseMode.HTML,
            reply_markup=membership_keyboard(),
        )
        return

    profile = await get_user(user.id)

    if not can_download(profile):
        await update.effective_message.reply_text(
            "╭──── ✦ <b>DAILY LIMIT</b> ✦ ────╮\n\n"
            f"You used all {FREE_DAILY_LIMIT} free downloads today.\n\n"
            "👑 Premium gives unlimited downloads.\n"
            "Premium: <b>₹50/month</b>\n\n"
            "╰────────────────────────────╯",
            parse_mode=ParseMode.HTML,
            reply_markup=limit_keyboard(),
        )
        return

    try:
        async with DownloadGuard(user.id):
            await process_download(
                update,
                context,
                user.id,
                text,
            )

    except DownloadBusy:
        await update.effective_message.reply_text(
            "⏳ You already have a download running."
        )

    except GlobalBusy:
        await update.effective_message.reply_text(
            "⌛ Emina is busy right now. Try again shortly."
        )


# ============================================================
# PICKER
# ============================================================

async def picker_callback(update, context, index):
    query = update.callback_query
    user = update.effective_user

    picker = context.user_data.get("picker")

    if not picker:
        await query.edit_message_text(
            "❌ Selection expired. Send the link again."
        )
        return

    items = picker["items"]

    if index >= len(items):
        await query.edit_message_text(
            "❌ Invalid selection."
        )
        return

    if await is_banned(user.id):
        await query.edit_message_text(
            "🚫 Access restricted."
        )
        return

    profile = await get_user(user.id)

    if not can_download(profile):
        await query.edit_message_text(
            f"❌ Your {FREE_DAILY_LIMIT}-download daily limit is reached."
        )
        return

    item = items[index]

    if not isinstance(item, dict):
        await query.edit_message_text(
            "❌ Invalid media item."
        )
        return

    media_url = item.get("url")

    if not media_url:
        await query.edit_message_text(
            "❌ Media URL unavailable."
        )
        return

    try:
        async with DownloadGuard(user.id):

            await query.edit_message_text(
                "⬇ <b>Preparing media...</b>",
                parse_mode=ParseMode.HTML,
            )

            with tempfile.TemporaryDirectory(
                prefix="emina_"
            ) as directory:

                downloaded = await fetch_media(
                    media_url,
                    directory,
                    item.get("filename"),
                )

                if not downloaded:
                    await query.edit_message_text(
                        "❌ Download failed."
                    )
                    return

                if not downloaded.path:
                    await query.edit_message_text(
                        f"❌ File exceeds {MAX_FILE_SIZE_MB} MB."
                    )
                    return

                sent = await send_file(
                    update,
                    context,
                    downloaded,
                )

                if sent:
                    await increment_download(
                        user.id
                    )

                    await record_download(
                        user.id,
                        picker["source_url"],
                        "success",
                        downloaded.filename,
                    )

                    await query.edit_message_text(
                        "✓ <b>Complete</b>",
                        parse_mode=ParseMode.HTML,
                    )
                else:
                    await query.edit_message_text(
                        "❌ Telegram couldn't send this file."
                    )

    except DownloadBusy:
        await query.answer(
            "You already have a download running.",
            show_alert=True,
        )

    except GlobalBusy:
        await query.answer(
            "Emina is busy. Try again shortly.",
            show_alert=True,
        )

    finally:
        context.user_data.pop(
            "picker",
            None,
        )


# ============================================================
# PROFILE / PREMIUM / HELP
# ============================================================

async def show_stats(update, context):
    user = update.effective_user
    profile = await get_user(user.id)

    current_plan = plan(profile)

    if current_plan == "OWNER":
        plan_name = "✦ Owner"
        usage = "Unlimited"
        expiry = "Never"

    elif current_plan == "PREMIUM":
        plan_name = "👑 Premium"
        usage = "Unlimited"

        try:
            expiry = datetime.fromisoformat(
                profile["premium_until"]
            ).strftime("%d %b %Y")
        except (TypeError, ValueError):
            expiry = "Unknown"

    else:
        plan_name = "Free"
        usage = (
            f"{profile.get('downloads_today', 0)}"
            f"/{FREE_DAILY_LIMIT}"
        )
        expiry = "Not active"

    text = (
        "╭──── ✦ <b>EMINA PROFILE</b> ✦ ────╮\n\n"
        f"Plan\n<b>{plan_name}</b>\n\n"
        f"Today\n<b>{usage}</b>\n\n"
        f"Total downloads\n<b>{profile.get('total_downloads', 0)}</b>\n\n"
        f"Premium\n<b>{expiry}</b>\n\n"
        "╰────────────────────────────╯"
    )

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard(),
        )
    else:
        await update.effective_message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard(),
        )


async def show_premium(update, context):
    if update.callback_query:
        await update.callback_query.edit_message_text(
            premium_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=premium_keyboard(),
        )
    else:
        await update.effective_message.reply_text(
            premium_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=premium_keyboard(),
        )


async def show_help(update, context):
    text = (
        "❓ <b>EMINA DOWNLOADER</b>\n\n"
        "• Send a supported social-media link.\n"
        f"• Free users get {FREE_DAILY_LIMIT} downloads/day.\n"
        "• Premium users get unlimited downloads.\n"
        "• Premium is ₹50/month.\n"
        "• Contact the owner to activate Premium.\n"
        "• Channel + group membership is required."
    )

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard(),
        )
    else:
        await update.effective_message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard(),
        )


# ============================================================
# CALLBACKS
# ============================================================

async def callbacks(update, context):
    query = update.callback_query
    data = query.data or ""

    await query.answer()

    if data == "download":
        await query.edit_message_text(
            "⌁ <b>Send your link</b>\n\n"
            "Paste a supported URL here.",
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard(),
        )

    elif data == "premium":
        await show_premium(update, context)

    elif data == "stats":
        await show_stats(update, context)

    elif data == "help":
        await show_help(update, context)

    elif data == "back":
        profile = await get_user(
            update.effective_user.id
        )

        await query.edit_message_text(
            start_text(profile),
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
        )

    elif data == "verify":
        user_id = update.effective_user.id

        if await has_required_membership(
            context.bot,
            user_id,
        ):
            profile = await get_user(user_id)

            await query.edit_message_text(
                "✓ <b>Access unlocked.</b>\n\n"
                + start_text(profile),
                parse_mode=ParseMode.HTML,
                reply_markup=main_keyboard(),
            )
        else:
            await query.answer(
                "Join both the channel and group first.",
                show_alert=True,
            )

    elif data == "contact_owner":
        await query.answer(
            "Contact the owner to activate Premium.",
            show_alert=True,
        )

    elif data.startswith("pick:"):
        try:
            index = int(
                data.split(":", 1)[1]
            )
        except ValueError:
            return

        await picker_callback(
            update,
            context,
            index,
        )


# ============================================================
# ADMIN
# ============================================================

async def admin(update, context):
    if not is_owner(
        update.effective_user.id
    ):
        return

    s = await stats()

    text = (
        "╭──── ✦ <b>EMINA ADMIN</b> ✦ ────╮\n\n"
        f"Users: <b>{s['users']:,}</b>\n"
        f"Premium: <b>{s['premium']:,}</b>\n"
        f"Banned: <b>{s['banned']:,}</b>\n\n"
        f"Downloads today: <b>{s['today']:,}</b>\n"
        f"Total downloads: <b>{s['downloads']:,}</b>\n"
        f"Successful: <b>{s['success']:,}</b>\n"
        f"Failed: <b>{s['failed']:,}</b>\n\n"
        "╰────────────────────────────╯"
    )

    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
    )


async def addpremium_cmd(update, context):
    if not is_owner(update.effective_user.id):
        return

    if len(context.args) != 2:
        await update.effective_message.reply_text(
            "Usage: /addpremium USER_ID DAYS"
        )
        return

    try:
        user_id = int(context.args[0])
        days = int(context.args[1])

        if days <= 0:
            raise ValueError

    except ValueError:
        await update.effective_message.reply_text(
            "Usage: /addpremium USER_ID DAYS"
        )
        return

    ok, info = await add_premium(
        user_id,
        days,
    )

    if not ok:
        await update.effective_message.reply_text(
            "❌ " + info
        )
        return

    await update.effective_message.reply_text(
        f"✓ Premium activated for {user_id}\n"
        f"Expires: {info}"
    )

    try:
        await context.bot.send_message(
            user_id,
            f"👑 <b>Emina Premium activated</b>\n\n"
            f"Expires: {info}",
            parse_mode=ParseMode.HTML,
        )
    except TelegramError:
        pass


async def removepremium_cmd(update, context):
    if not is_owner(update.effective_user.id):
        return

    if len(context.args) != 1:
        await update.effective_message.reply_text(
            "Usage: /removepremium USER_ID"
        )
        return

    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text(
            "Usage: /removepremium USER_ID"
        )
        return

    if await remove_premium(user_id):
        await update.effective_message.reply_text(
            "✓ Premium removed."
        )
    else:
        await update.effective_message.reply_text(
            "❌ User not found."
        )


async def ban_cmd(update, context):
    if not is_owner(update.effective_user.id):
        return

    if len(context.args) != 1:
        await update.effective_message.reply_text(
            "Usage: /ban USER_ID"
        )
        return

    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text(
            "Usage: /ban USER_ID"
        )
        return

    if await set_ban(user_id, True):
        await update.effective_message.reply_text(
            "✓ User banned."
        )
    else:
        await update.effective_message.reply_text(
            "❌ User not found."
        )


async def unban_cmd(update, context):
    if not is_owner(update.effective_user.id):
        return

    if len(context.args) != 1:
        await update.effective_message.reply_text(
            "Usage: /unban USER_ID"
        )
        return

    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text(
            "Usage: /unban USER_ID"
        )
        return

    if await set_ban(user_id, False):
        await update.effective_message.reply_text(
            "✓ User unbanned."
        )
    else:
        await update.effective_message.reply_text(
            "❌ User not found."
        )


async def broadcast_cmd(update, context):
    if not is_owner(update.effective_user.id):
        return

    context.user_data["broadcast"] = True

    await update.effective_message.reply_text(
        "📢 Send the message to broadcast."
    )


async def handle_broadcast(update, context):
    if not context.user_data.get("broadcast"):
        return False

    if not is_owner(update.effective_user.id):
        return False

    context.user_data.pop("broadcast", None)

    user_ids = await get_all_user_ids()

    sent = 0
    failed = 0

    status = await update.effective_message.reply_text(
        f"📢 Broadcasting to {len(user_ids)} users..."
    )

    for user_id in user_ids:
        try:
            await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=update.effective_chat.id,
                message_id=update.effective_message.message_id,
            )
            sent += 1

        except RetryAfter as exc:
            await asyncio.sleep(
                exc.retry_after
            )

            try:
                await context.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=update.effective_chat.id,
                    message_id=update.effective_message.message_id,
                )
                sent += 1
            except TelegramError:
                failed += 1

        except TelegramError:
            failed += 1

        await asyncio.sleep(0.05)

    await status.edit_text(
        f"✓ Broadcast complete\n\n"
        f"Sent: {sent}\n"
        f"Failed: {failed}"
    )

    return True


# ============================================================
# COMMANDS
# ============================================================

async def stats_cmd(update, context):
    await show_stats(update, context)


async def premium_cmd(update, context):
    await show_premium(update, context)


async def help_cmd(update, context):
    await show_help(update, context)


async def users_cmd(update, context):
    if not is_owner(update.effective_user.id):
        return

    s = await stats()

    await update.effective_message.reply_text(
        f"Users: {s['users']}\n"
        f"Premium: {s['premium']}\n"
        f"Banned: {s['banned']}"
    )


async def text_router(update, context):
    if await handle_broadcast(
        update,
        context,
    ):
        return

    await url_handler(
        update,
        context,
    )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(update, context):
    logger.error(
        "Unhandled error",
        exc_info=context.error,
    )


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================

async def post_init(application):
    await init_db()

    Path(CACHE_DIR).mkdir(
        parents=True,
        exist_ok=True,
    )

    await get_session()

    logger.info(
        "EminaDownloader online | Cobalt=%s | Owner=%s",
        COBALT_API,
        OWNER_ID,
    )


async def post_shutdown(application):
    global HTTP_SESSION

    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()


def build_application():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(
        CommandHandler("start", start)
    )

    application.add_handler(
        CommandHandler("stats", stats_cmd)
    )

    application.add_handler(
        CommandHandler("premium", premium_cmd)
    )

    application.add_handler(
        CommandHandler("help", help_cmd)
    )

    application.add_handler(
        CommandHandler("admin", admin)
    )

    application.add_handler(
        CommandHandler("users", users_cmd)
    )

    application.add_handler(
        CommandHandler("broadcast", broadcast_cmd)
    )

    application.add_handler(
        CommandHandler("addpremium", addpremium_cmd)
    )

    application.add_handler(
        CommandHandler("removepremium", removepremium_cmd)
    )

    application.add_handler(
        CommandHandler("ban", ban_cmd)
    )

    application.add_handler(
        CommandHandler("unban", unban_cmd)
    )

    application.add_handler(
        CallbackQueryHandler(callbacks)
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_router,
        )
    )

    application.add_error_handler(
        error_handler
    )

    return application


def main():
    application = build_application()

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
