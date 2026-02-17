import os
import random
import asyncio
import re
import difflib
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from database import Database
import logging
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    ReplyKeyboardMarkup, 
    KeyboardButton,
    FSInputFile,
    Message,
    CallbackQuery,
    ErrorEvent
)
from aiogram.exceptions import TelegramNetworkError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode, ChatMemberStatus


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi. Iltimos .env faylga BOT_TOKEN yozing yoki env o'rnating.")
ADMIN_IDS = [7903688837, 6274852941]  # Admin ID'larini kiriting


PREMIUM_PRICE_MONTHLY = 5000
CARD_NUMBER = "8600 0000 0000 0000"
CARD_OWNER = "Ozodbbek Mamatov"

class UserStates(StatesGroup):
    waiting_search = State()
    waiting_episode_choice = State()
    waiting_payment = State()

class AdminStates(StatesGroup):

    add_channel_waiting_id = State()
    add_channel_waiting_name = State()
    add_channel_waiting_type = State()
    add_channel_waiting_invite = State()
    
    
    add_movie_waiting_title = State()
    add_movie_waiting_code = State()
    add_movie_waiting_type = State()
    add_movie_waiting_category = State()
    add_movie_waiting_description = State()
    add_movie_waiting_year = State()
    add_movie_waiting_rating = State()
    add_movie_waiting_file = State()
    
   
    add_series_waiting_episode = State()
    add_series_waiting_file = State()
    update_premium_price = State()
    update_card_number = State()
    update_card_owner = State()
    
   
    broadcast_waiting_message = State()
    scan_waiting_lines = State()
    delete_movie_waiting_code = State()

# Initialize database
db = Database(default_premium_price=PREMIUM_PRICE_MONTHLY, default_card_number=CARD_NUMBER, default_card_owner=CARD_OWNER)

def get_premium_price_monthly() -> int:
    value = db.get_setting("premium_price_monthly", str(PREMIUM_PRICE_MONTHLY))
    try:
        return int(str(value).replace(" ", "").replace(",", ""))
    except Exception:
        return PREMIUM_PRICE_MONTHLY

def get_card_number() -> str:
    return db.get_setting("card_number", CARD_NUMBER) or CARD_NUMBER

def get_card_owner() -> str:
    return db.get_setting("card_owner", CARD_OWNER) or CARD_OWNER

# ================================
# KEYBOARDS
# ================================
def get_main_keyboard(is_premium: bool = False, is_admin: bool = False):
    buttons = [
        [KeyboardButton(text="🔍 Qidirish"), KeyboardButton(text="🎬 Kategoriyalar")],
        [KeyboardButton(text="🔥 Trend"), KeyboardButton(text="⭐ Tavsiyalar")],
    ]
    
    if not is_premium:
        buttons.append([KeyboardButton(text="💎 Premium"), KeyboardButton(text="ℹ️ Yordam")])
    else:
        buttons.append([KeyboardButton(text="💎 Premium ✅"), KeyboardButton(text="ℹ️ Yordam")])
    
    if is_admin:
        buttons.append([KeyboardButton(text="Admin panel")])

    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_admin_keyboard():
    buttons = [
        [KeyboardButton(text="👥 Statistika"), KeyboardButton(text="📊 Top qidiruvlar")],
        [KeyboardButton(text="Majburiy kanal qo'shish"), KeyboardButton(text="Majburiy kanal o'chirish")],
        [KeyboardButton(text="🎬 Kino qo'shish"), KeyboardButton(text="Kino o'chirish")],
        [KeyboardButton(text="💳 Premium sozlamalar"), KeyboardButton(text="📢 Broadcast")],
        [KeyboardButton(text="🔙 Orqaga")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_mandatory_channel_delete_keyboard(channels: List[tuple]) -> InlineKeyboardMarkup:
    buttons = []
    for channel in channels:
        row_id = channel[0]
        channel_name = (channel[2] or str(channel[1]) or "Kanal").strip()
        short_name = channel_name if len(channel_name) <= 28 else channel_name[:25] + "..."
        buttons.append(
            [InlineKeyboardButton(text=f"❌ {short_name}", callback_data=f"mandatory_del_id_{row_id}")]
        )

    buttons.append([InlineKeyboardButton(text="Bekor qilish", callback_data="mandatory_del_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_premium_settings_keyboard():
    buttons = [
        [KeyboardButton(text="💰 Narx"), KeyboardButton(text="💳 Karta raqami")],
        [KeyboardButton(text="👤 Karta egasi"), KeyboardButton(text="Admin panel")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_categories_keyboard():
    buttons = [
        [InlineKeyboardButton(text="🎬 Kino", callback_data="cat_kino")],
        [InlineKeyboardButton(text="🎌 Anime", callback_data="cat_anime")],
        [InlineKeyboardButton(text="🇰🇷 Dorama", callback_data="cat_dorama")],
        [InlineKeyboardButton(text="🧒 Multfilm", callback_data="cat_multfilm")],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_channel_url(channel: tuple) -> Optional[str]:
    channel_id = channel[1]
    channel_username = channel[3]
    invite_link = channel[7] if len(channel) > 7 else None

    if invite_link:
        return invite_link
    if channel_username:
        return f"https://t.me/{channel_username}"
    if str(channel_id).startswith("@"):
        return f"https://t.me/{str(channel_id).lstrip('@')}"
    return None


def get_subscription_keyboard(channels: List[tuple]) -> InlineKeyboardMarkup:
    buttons = []
    for idx, channel in enumerate(channels, 1):
        channel_name = (channel[2] or str(channel[1]) or "Kanal").strip()
        short_name = channel_name if len(channel_name) <= 28 else channel_name[:25] + "..."
        url = get_channel_url(channel)
        if not url:
            continue
        buttons.append([InlineKeyboardButton(text=f"{idx}. {short_name}", url=url)])

    buttons.append([InlineKeyboardButton(text="✅ Obunani tekshirish", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_subscription_text(channels: List[tuple], statuses: Optional[Dict[str, bool]] = None) -> str:
    text = "🔒 <b>Majburiy obuna</b>\n\n"
    text += "Botdan foydalanishdan oldin quyidagi kanallarga obuna bo'ling:\n\n"

    for idx, channel in enumerate(channels, 1):
        channel_id = str(channel[1])
        channel_name = channel[2] or channel_id
        if statuses is None:
            text += f"{idx}. {channel_name}\n"
        else:
            status_icon = "✅" if statuses.get(channel_id, False) else "❌"
            text += f"{idx}. {status_icon} {channel_name}\n"

    if statuses is not None:
        left_count = sum(1 for ok in statuses.values() if not ok)
        if left_count > 0:
            text += f"\nQolgan kanallar: <b>{left_count}</b>"

    text += "\n\nObuna bo'lgandan keyin pastdagi tugmani bosing."
    return text

def get_movie_keyboard(movie_id: int, category: str, is_series: bool = False):
    """Create keyboard for movie"""
    buttons = []
    
    if is_series:
        buttons.append([InlineKeyboardButton(text="📺 Barcha qismlar", callback_data=f"episodes_{movie_id}")])
    else:
        # Similar movies
        similar = db.get_similar_movies(movie_id, category, 3)
        for movie in similar:
            buttons.append([InlineKeyboardButton(
                text=f"🎬 {movie[1]}", 
                callback_data=f"movie_{movie[0]}"
            )])
    
    buttons.append([InlineKeyboardButton(text="🔙 Asosiy menyu", callback_data="back_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_episodes_keyboard(movie_id: int, episodes: List[tuple], page: int = 1, per_page: int = 10):
    """Create numeric keypad for series episodes (paged)"""
    ep_nums = [e[2] for e in episodes]
    total = len(ep_nums)
    if total == 0:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Orqaga", callback_data=f"movie_{movie_id}")]])

    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_eps = ep_nums[start:start + per_page]

    rows = []
    row = []
    for n in page_eps:
        row.append(InlineKeyboardButton(text=str(n), callback_data=f"ep_{movie_id}_{n}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"episodes_{movie_id}_{page-1}"))
    nav.append(InlineKeyboardButton(text="❌", callback_data=f"movie_{movie_id}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"episodes_{movie_id}_{page+1}"))
    rows.append(nav)

    return InlineKeyboardMarkup(inline_keyboard=rows)

# ================================
# BOT INITIALIZATION
# ================================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

# ================================
# HELPER FUNCTIONS
# ================================
async def check_subscription(user_id: int, channel_id: str) -> bool:
    """Check if user is subscribed to channel"""
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        return False


async def get_subscription_statuses(user_id: int, channels: List[tuple]) -> Tuple[bool, Dict[str, bool]]:
    statuses: Dict[str, bool] = {}
    all_subscribed = True

    for channel in channels:
        channel_id = str(channel[1])
        is_subscribed = await check_subscription(user_id, channel_id)
        statuses[channel_id] = is_subscribed
        if is_subscribed:
            db.mark_subscription(user_id, channel_id)
        else:
            all_subscribed = False

    return all_subscribed, statuses


async def check_all_subscriptions(user_id: int, channels: List[tuple]) -> bool:
    all_subscribed, _ = await get_subscription_statuses(user_id, channels)
    return all_subscribed


async def resolve_channel_id(channel_id: str) -> Tuple[str, Optional[str]]:
    if channel_id and str(channel_id).startswith("@"):
        try:
            chat = await bot.get_chat(channel_id)
            resolved_id = str(chat.id)
            username = chat.username or channel_id.lstrip("@")
            return resolved_id, username
        except Exception:
            return channel_id, channel_id.lstrip("@")
    return channel_id, None


def filter_clickable_channels(channels: List[tuple]) -> List[tuple]:
    filtered = []
    for ch in channels:
        if get_channel_url(ch):
            filtered.append(ch)
    return filtered


async def enforce_subscription(message: Message, user_id: int) -> bool:
    """Enforce mandatory subscription."""
    if db.is_premium(user_id):
        return True

    channels = db.get_daily_channels(user_id)

    if not channels:
        await message.answer(
            "Hozircha majburiy obuna kanallari mavjud emas.\n"
            "Admin bilan bog'laning."
        )
        return True

    clickable_channels = filter_clickable_channels(channels)
    if not clickable_channels:
        await message.answer(
            "Majburiy obuna kanallari noto'g'ri sozlangan. "
            "Admin kanallarga invite link qo'shishi kerak."
        )
        return True

    all_subscribed, statuses = await get_subscription_statuses(user_id, clickable_channels)
    if all_subscribed:
        return True

    await message.answer(
        build_subscription_text(clickable_channels, statuses),
        reply_markup=get_subscription_keyboard(clickable_channels)
    )
    return False


def format_movie_info(movie: tuple) -> str:
    """Format movie information"""
    # movie tuple fields may include source_chat_id, source_message_id at the end
    movie_id, title, code, file_id, file_type, media_type, category, description, year, rating, views, added_date, is_active, *rest = movie
    source_chat_id = rest[0] if len(rest) > 0 else None
    source_message_id = rest[1] if len(rest) > 1 else None
    
    text = f"🎬 <b>{title}</b>\n\n"
    
    if description:
        text += f"📝 {description}\n\n"
    
    if year:
        text += f"📅 Yil: {year}\n"
    
    if rating:
        text += f"⭐ Reyting: {rating}/10\n"
    
    text += f"👁 Ko'rildi: {views} marta\n"
    text += f"🔢 Kod: <code>{code}</code>\n"
    text += f"📂 Kategoriya: {category.capitalize()}"
    
    return text


async def send_media(chat_id: int, file_id: str, file_type: str, caption: str, reply_markup: InlineKeyboardMarkup):
    if file_type == "document":
        return await bot.send_document(chat_id=chat_id, document=file_id, caption=caption, reply_markup=reply_markup)
    if file_type == "animation":
        return await bot.send_animation(chat_id=chat_id, animation=file_id, caption=caption, reply_markup=reply_markup)
    if file_type == "photo":
        return await bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption, reply_markup=reply_markup)
    # default: video
    return await bot.send_video(chat_id=chat_id, video=file_id, caption=caption, reply_markup=reply_markup)


def parse_tme_c_links(text: str) -> List[Tuple[str, int]]:
    links: List[Tuple[str, int]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Private channel link: t.me/c/<internal_id>/<message_id>
        m = re.search(r"(?:https?://)?t\.me/c/(\d+)/(\d+)", line)
        if m:
            internal_id = m.group(1)
            msg_id = int(m.group(2))
            chat_id = f"-100{internal_id}"
            links.append((chat_id, msg_id))
            continue

        # Public channel link: t.me/<username>/<message_id>
        m = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]{3,})/(\d+)", line)
        if m:
            username = m.group(1)
            if username.lower() in ("c", "joinchat"):
                continue
            msg_id = int(m.group(2))
            chat_id = f"@{username}"
            links.append((chat_id, msg_id))
            continue

    return links

def parse_channel_input(text: str) -> Optional[str]:
    if not text:
        return None
    raw = text.strip()

    # Match -100... IDs
    m_id = re.search(r"-100\d{5,}", raw)
    if m_id:
        return m_id.group(0)

    # Match t.me/c/<internal_id> (private)
    m_priv = re.search(r"(?:https?://)?t\.me/c/(\d{5,})", raw)
    if m_priv:
        return f"-100{m_priv.group(1)}"

    # Match t.me/username links
    m_link = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]{3,})", raw)
    if m_link:
        username = m_link.group(1)
        if username.lower() not in ("c", "joinchat"):
            return f"@{username}"

    # Match @username or plain username
    m_user = re.search(r"@([A-Za-z0-9_]{3,})", raw)
    if m_user:
        return f"@{m_user.group(1)}"

    token = raw.split()[0].strip()
    token = re.sub(r"[^A-Za-z0-9_]", "", token)
    if re.match(r"^[A-Za-z0-9_]{3,}$", token):
        return f"@{token}"

    return None


def parse_invite_link(text: str) -> Optional[str]:
    if not text:
        return None
    raw = text.strip()
    m = re.search(r"(?:https?://)?t\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+", raw)
    if not m:
        return None
    link = m.group(0)
    if not link.startswith("http"):
        link = f"https://{link}"
    return link


def normalize_title(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned

def guess_category(text: str) -> str:
    t = text.lower()
    if "anime" in t or "#anime" in t:
        return "anime"
    if "dorama" in t or "#dorama" in t:
        return "dorama"
    if "multfilm" in t or "mult" in t or "#multfilm" in t:
        return "multfilm"
    return "kino"

def parse_caption_template(text: str) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[str]]:
    """Parse caption by templates to extract title, episode, media_type, category."""
    if not text:
        return None, None, None, None

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    title = None
    episode = None
    media_type = None
    category = None

    # Hashtag based category
    if "#anime" in text.lower():
        category = "anime"
    elif "#dorama" in text.lower():
        category = "dorama"
    elif "#multfilm" in text.lower() or "#mult" in text.lower():
        category = "multfilm"
    elif "#kino" in text.lower():
        category = "kino"

    for line in lines:
        # Title line
        m = re.match(r"(?i)^(nomi|title)\s*[:\-]\s*(.+)$", line)
        if m and not title:
            title = normalize_title(m.group(2))
            continue
        # Explicit media type
        m = re.match(r"(?i)^(type|media|tur|turi)\s*[:\-]\s*(movie|kino|serial|series)$", line)
        if m and not media_type:
            v = m.group(2).lower()
            media_type = "series" if v in ("serial", "series") else "movie"
            continue
        # Category line
        m = re.match(r"(?i)^(kategoriya|category)\s*[:\-]\s*(.+)$", line)
        if m and not category:
            category = guess_category(m.group(2))
            continue
        # Episode line
        m = re.match(r"(?i)^(qism|episode|ep)\s*[:\-]?\s*(\d{1,3})$", line)
        if m and episode is None:
            episode = int(m.group(2))
            media_type = media_type or "series"
            continue
        # Media-specific prefix
        m = re.match(r"(?i)^(kino|serial)\s*[:\-]\s*(.+)$", line)
        if m and not title:
            title = normalize_title(m.group(2))
            media_type = "series" if m.group(1).lower() == "serial" else "movie"
            continue

    if not title and lines:
        # Fallback to first line parsing
        title, episode = parse_title_and_episode(lines[0])

    return title, episode, media_type, category

def parse_title_and_episode(text: str) -> Tuple[Optional[str], Optional[int]]:
    if not text:
        return None, None
    # Use first non-empty line as title
    line = ""
    for raw in text.splitlines():
        if raw.strip():
            line = raw.strip()
            break
    if not line:
        return None, None
    # Remove common file extensions
    line = re.sub(r"\.(mp4|mkv|avi|mov|wmv|flv|webm)$", "", line, flags=re.IGNORECASE)

    # Detect episode patterns like "12-qism", "12 qism", "ep 12", "episode 12"
    m = re.search(r"(?i)\b(\d{1,3})\s*(?:-|\s)?\s*(qism|q\.|ep|episode)\b", line)
    if m:
        ep = int(m.group(1))
        title = re.sub(r"(?i)\b(\d{1,3})\s*(?:-|\s)?\s*(qism|q\.|ep|episode)\b", "", line)
        return normalize_title(title.strip(" -–—|:")), ep

    m = re.search(r"(?i)\b(qism|q\.|ep|episode)\s*(\d{1,3})\b", line)
    if m:
        ep = int(m.group(2))
        title = re.sub(r"(?i)\b(qism|q\.|ep|episode)\s*(\d{1,3})\b", "", line)
        return normalize_title(title.strip(" -–—|:")), ep

    return normalize_title(line), None

def generate_code_from_title(title: str) -> str:
    base = re.sub(r"[^A-Za-z0-9]", "", title.upper())[:6]
    if not base:
        base = "MOV"
    for _ in range(10):
        code = f"{base}{random.randint(100, 999)}"
        if not db.is_code_exists(code):
            return code
    return f"{base}{random.randint(1000, 9999)}"

def get_search_results_keyboard(movies: List[tuple]) -> InlineKeyboardMarkup:
    buttons = []
    for i, movie in enumerate(movies, 1):
        movie_id = movie[0]
        title = movie[1]
        media_type = movie[5]
        tag = "Serial" if media_type == "series" else "Kino"
        buttons.append([InlineKeyboardButton(text=f"{i}. {title} ({tag})", callback_data=f"movie_{movie_id}")])
    buttons.append([InlineKeyboardButton(text="🔙 Asosiy menyu", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def parse_scan_line(line: str) -> Optional[Dict]:
    """
    Line format:
    https://t.me/c/123/456 | Nomi: Title | Qism: 12 | Type: serial | Category: anime
    or
    https://t.me/c/123/456 | Title | 12-qism
    """
    if "t.me/c/" not in line:
        return None

    parts = [p.strip() for p in line.split("|") if p.strip()]
    if not parts:
        return None

    link = parts[0]
    links = parse_tme_c_links(link)
    if not links:
        return None
    chat_id, msg_id = links[0]

    meta_text = "\n".join(parts[1:]) if len(parts) > 1 else ""
    title, ep_num, media_type, category = parse_caption_template(meta_text)
    if not title:
        title, ep_num = parse_title_and_episode(meta_text)

    if not category and meta_text:
        category = guess_category(meta_text)

    return {
        "chat_id": chat_id,
        "msg_id": msg_id,
        "title": title,
        "episode": ep_num,
        "media_type": media_type,
        "category": category or "kino"
    }

# ================================
# ERROR HANDLING
# ================================
@router.errors()
async def handle_telegram_network_errors(event: ErrorEvent):
    if isinstance(event.exception, TelegramNetworkError):
        logger.warning(f"Telegram network error: {event.exception}")
        return True
    return False

# ================================
# HANDLERS - START & MAIN
# ================================
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user = message.from_user
    db.add_user(user.id, user.username, user.first_name, user.last_name)
    
    if not await enforce_subscription(message, user.id):
        return
    
    is_premium = db.is_premium(user.id)

    # Clear any previous state on /start
    await state.clear()

    # /start payload support (e.g. "/start CODE123")
    payload = None
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            payload = parts[1].strip()
    if payload:
        await run_search(message, payload, state)
        return
    
    await message.answer(
        f"👋 Assalomu alaykum, <b>{user.first_name}</b>!\n\n"
        f"🎬 <b>Kino va Seriallar Bot</b>ga xush kelibsiz!\n\n"
        f"Bu botda minglab kino, serial, anime va doramalarni topishingiz mumkin.\n\n"
        f"🔍 <b>Qidirish:</b> Kino nomi yoki kodini yuboring\n"
        f"📂 <b>Kategoriyalar:</b> Turli kategoriyalarni ko'ring\n"
        f"🔥 <b>Trend:</b> Eng mashhur kinolarni toping\n"
        f"{'💎 <b>Premium:</b> Premium obunangiz faol!' if is_premium else '💎 <b>Premium:</b> Reklamasiz va tez yuklab olish'}",
        reply_markup=get_main_keyboard(is_premium, message.from_user.id in ADMIN_IDS)
    )

@router.callback_query(F.data == "check_sub")
async def callback_check_subscription(callback: CallbackQuery):
    user_id = callback.from_user.id
    channels = db.get_daily_channels(user_id)
    clickable_channels = filter_clickable_channels(channels)

    if not clickable_channels:
        await callback.answer("Kanallar noto'g'ri sozlangan", show_alert=True)
        return

    all_subscribed, statuses = await get_subscription_statuses(user_id, clickable_channels)
    if all_subscribed:
        try:
            await callback.message.edit_text(
                "Obuna tasdiqlandi. Endi botdan to'liq foydalanishingiz mumkin."
            )
        except Exception:
            await callback.message.answer("Obuna tasdiqlandi. Endi botdan to'liq foydalanishingiz mumkin.")
        await callback.message.answer(
            "Qidirish uchun kino nomi yoki kodini yuboring.",
            reply_markup=get_main_keyboard(db.is_premium(user_id), user_id in ADMIN_IDS)
        )
        await callback.answer("Tasdiqlandi")
    else:
        updated_text = build_subscription_text(clickable_channels, statuses)
        updated_markup = get_subscription_keyboard(clickable_channels)
        try:
            await callback.message.edit_text(updated_text, reply_markup=updated_markup)
        except Exception:
            await callback.message.answer(updated_text, reply_markup=updated_markup)
        await callback.answer("Siz hali barcha kanallarga obuna bo'lmagansiz", show_alert=True)


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    if not current:
        await message.answer("Bekor qilinadigan amal yo'q")
        return

    await state.clear()
    if message.from_user.id in ADMIN_IDS:
        await message.answer("Bekor qilindi", reply_markup=get_admin_keyboard())
    else:
        await message.answer(
            "Bekor qilindi",
            reply_markup=get_main_keyboard(db.is_premium(message.from_user.id), message.from_user.id in ADMIN_IDS)
        )


# ================================
# HANDLERS - SEARCH
# ================================
@router.message(F.text == "🔍 Qidirish")
async def search_menu(message: Message, state: FSMContext):
    if not await enforce_subscription(message, message.from_user.id):
        return
    
    await message.answer(
        "🔍 <b>Qidirish</b>\n\n"
        "Kino, serial, anime yoki dorama nomini yoki kodini yuboring.\n\n"
        "<i>Masalan: Spiderman, SPID001</i>"
    )
    await state.set_state(UserStates.waiting_search)

@router.message(UserStates.waiting_search)
async def process_search(message: Message, state: FSMContext):
    await run_search(message, message.text.strip(), state)

async def run_search(message: Message, query: str, state: FSMContext | None = None):
    if not query:
        if state:
            await state.clear()
        return

    results = db.search_movies(query, limit=6)

    if not results:
        # Fuzzy search fallback
        results = db.search_movies_fuzzy(query, limit=6)
        if results:
            keyboard = get_search_results_keyboard(results)
            await message.answer(
                "🔎 <b>O‘xshash natijalar topildi</b>\n\n"
                "Keraklisini tanlang:",
                reply_markup=keyboard
            )
            db.add_search_stat(message.from_user.id, query, True)
            if state:
                await state.clear()
            return

        db.add_search_stat(message.from_user.id, query, False)
        await message.answer(
            "😕 <b>Bu media hozircha bazada yo'q</b>\n\n"
            "Iltimos, boshqa nom yoki kod bilan qidirib ko'ring.",
            reply_markup=get_main_keyboard(db.is_premium(message.from_user.id), message.from_user.id in ADMIN_IDS)
        )
        if state:
            await state.clear()
        return

    db.add_search_stat(message.from_user.id, query, True)

    if len(results) > 1:
        keyboard = get_search_results_keyboard(results)
        await message.answer(
            "🔎 <b>Bir nechta natija topildi</b>\n\n"
            "Keraklisini tanlang:",
            reply_markup=keyboard
        )
        if state:
            await state.clear()
        return

    movie = results[0]
    db.increment_movie_views(movie[0])
    db.add_view_stat(message.from_user.id, movie[0])

    movie_id, title, code, file_id, file_type, media_type, category, description, year, rating, views, added_date, is_active, *rest = movie
    source_chat_id = rest[0] if len(rest) > 0 else None
    source_message_id = rest[1] if len(rest) > 1 else None

    caption = format_movie_info(movie)

    is_series = (media_type == "series")
    if is_series:
        episodes = db.get_series_episodes(movie_id)
        keyboard = get_episodes_keyboard(movie_id, episodes, page=1)
        text = f"📺 <b>{title}</b>\n\nQismni tanlang:"
        await message.answer(text, reply_markup=keyboard)
    else:
        keyboard = get_movie_keyboard(movie_id, category, is_series)
        try:
            if file_type == "channel" and source_chat_id and source_message_id:
                from_chat = int(source_chat_id) if str(source_chat_id).lstrip('-').isdigit() else source_chat_id
                await bot.copy_message(
                    chat_id=message.chat.id,
                    from_chat_id=from_chat,
                    message_id=int(source_message_id),
                    reply_markup=keyboard
                )
                await message.answer(caption, reply_markup=keyboard)
            else:
                await send_media(message.chat.id, file_id, file_type, caption, keyboard)
        except Exception as e:
            logger.error(f"Error sending video: {e}")
            await message.answer("❌ Video yuborilmadi. Kanalga bot qo‘shilganini tekshiring.")

    if state:
        await state.clear()

MENU_TEXTS = {
    "🔍 Qidirish",
    "🎬 Kategoriyalar",
    "🔥 Trend",
    "⭐ Tavsiyalar",
    "💎 Premium",
    "💎 Premium ✅",
    "ℹ️ Yordam",
    "Admin panel",
    "💳 Premium sozlamalar",
    "💰 Narx",
    "💳 Karta raqami",
    "👤 Karta egasi",
    "👥 Statistika",
    "📊 Top qidiruvlar",
    "➕ Kanal qo'shish",
    "Majburiy kanal qo'shish",
    "Majburiy kanal o'chirish",
    "Kino o'chirish",
    "🎬 Kino qo'shish",
    "📢 Broadcast",
    "📥 Kanalni skan qilish",
    "🔙 Orqaga",
}

@router.message(StateFilter(None), F.text & ~F.text.in_(MENU_TEXTS) & ~F.text.startswith("/"))
async def quick_search(message: Message, state: FSMContext):
    # Only handle plain text searches when no state is active
    if await state.get_state() is not None:
        return
    text = message.text.strip()
    if not text:
        return
    if not await enforce_subscription(message, message.from_user.id):
        return
    await run_search(message, text, state)

# ================================
# CHANNEL AUTO-INDEXING
# ================================
@router.channel_post()
async def handle_channel_post(message: Message):
    # Auto-indexing is disabled intentionally.
    return

# ================================
# HANDLERS - CATEGORIES
# ================================
@router.message(F.text == "🎬 Kategoriyalar")
async def categories_menu(message: Message):
    if not await enforce_subscription(message, message.from_user.id):
        return
    
    await message.answer(
        "🎬 <b>Kategoriyalar</b>\n\n"
        "Qaysi kategoriyani ko'rmoqchisiz?",
        reply_markup=get_categories_keyboard()
    )

@router.callback_query(F.data.startswith("cat_"))
async def callback_category(callback: CallbackQuery):
    category = callback.data.split("_")[1]
    
    movies = db.get_movies_by_category(category, 10)
    
    if not movies:
        await callback.answer("Bu kategoriyada hozircha kino yo'q", show_alert=True)
        return
    
    text = f"🎬 <b>{category.capitalize()}</b>\n\n"
    
    buttons = []
    for i, movie in enumerate(movies, 1):
        movie_id = movie[0]
        title = movie[1]
        views = movie[10]
        text += f"{i}. {title} - 👁 {views}\n"
        buttons.append([InlineKeyboardButton(text=f"{i}. {title}", callback_data=f"movie_{movie_id}")])
    
    buttons.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="back_categories")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()

@router.callback_query(F.data.regexp(r"^movie_\d+$"))
async def callback_movie(callback: CallbackQuery):
    movie_id = int(callback.data.split("_")[1])
    
    movie = db.get_movie_by_id(movie_id)
    
    if not movie:
        await callback.answer("Kino topilmadi", show_alert=True)
        return
    
    db.increment_movie_views(movie_id)
    db.add_view_stat(callback.from_user.id, movie_id)
    
    movie_id, title, code, file_id, file_type, media_type, category, description, year, rating, views, added_date, is_active, *rest = movie
    source_chat_id = rest[0] if len(rest) > 0 else None
    source_message_id = rest[1] if len(rest) > 1 else None
    
    caption = format_movie_info(movie)

    is_series = (media_type == "series")
    if is_series:
        episodes = db.get_series_episodes(movie_id)
        keyboard = get_episodes_keyboard(movie_id, episodes, page=1)
        text = f"📺 <b>{title}</b>\n\nQismni tanlang:"
        try:
            await callback.message.delete()
        except Exception:
            pass
        await bot.send_message(callback.message.chat.id, text, reply_markup=keyboard)
    else:
        keyboard = get_movie_keyboard(movie_id, category, is_series)
        try:
            await callback.message.delete()
        except Exception:
            pass
        try:
            if file_type == "channel" and source_chat_id and source_message_id:
                from_chat = int(source_chat_id) if str(source_chat_id).lstrip('-').isdigit() else source_chat_id
                await bot.copy_message(
                    chat_id=callback.message.chat.id,
                    from_chat_id=from_chat,
                    message_id=int(source_message_id),
                    reply_markup=keyboard
                )
                await bot.send_message(callback.message.chat.id, caption, reply_markup=keyboard)
            else:
                await send_media(callback.message.chat.id, file_id, file_type, caption, keyboard)
        except Exception as e:
            logger.error(f"Error sending video: {e}")
            await callback.message.answer("❌ Video yuborilmadi. Kanalga bot qo‘shilganini tekshiring.")
    
    try:
        await callback.answer()
    except Exception:
        pass

# ================================
# HANDLERS - SERIES
# ================================
@router.callback_query(F.data.startswith("episodes_"))
async def callback_episodes(callback: CallbackQuery):
    parts = callback.data.split("_")
    movie_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
    
    episodes = db.get_series_episodes(movie_id)
    
    if not episodes:
        await callback.answer("Qismlar topilmadi", show_alert=True)
        return
    
    series_title = db.get_movie_title(movie_id)
    if not series_title:
        await callback.answer("Serial topilmadi", show_alert=True)
        return
    
    text = f"📺 <b>{series_title}</b>\n\n"
    text += f"Jami qismlar: {len(episodes)}\n\n"
    text += "Qismni tanlang:"
    
    keyboard = get_episodes_keyboard(movie_id, episodes, page=page)
    
    # If the original message has no text (e.g., it's a video with caption),
    # edit_caption should be used instead of edit_text.
    if callback.message.text:
        await callback.message.edit_text(text, reply_markup=keyboard)
    elif callback.message.caption is not None:
        await callback.message.edit_caption(caption=text, reply_markup=keyboard)
    else:
        await callback.message.answer(text, reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("ep_"))
async def callback_episode(callback: CallbackQuery):
    parts = callback.data.split("_")
    movie_id = int(parts[1])
    episode_num = int(parts[2])
    
    episode = db.get_episode(movie_id, episode_num)
    
    if not episode:
        await callback.answer("Qism topilmadi", show_alert=True)
        return
    
    ep_id = episode[0]
    m_id = episode[1]
    ep_num = episode[2]
    ep_title = episode[3]
    file_id = episode[4]
    file_type = episode[5]
    source_chat_id = episode[7] if len(episode) > 7 else None
    source_message_id = episode[8] if len(episode) > 8 else None
    
    series_info = db.get_movie_title_and_code(movie_id)
    if not series_info:
        await callback.answer("Serial topilmadi", show_alert=True)
        return
    series_title, series_code = series_info
    
    caption = f"📺 <b>{series_title}</b>\n"
    caption += f"▶️ {ep_title or f'{ep_num}-qism'}\n\n"
    caption += f"🔢 Kod: <code>{series_code}</code>"
    
    # Next episode button
    buttons = []
    next_episode = db.get_episode(movie_id, episode_num + 1)
    if next_episode:
        buttons.append([InlineKeyboardButton(
            text=f"▶️ Keyingi qism ({episode_num + 1})",
            callback_data=f"ep_{movie_id}_{episode_num + 1}"
        )])
    
    buttons.append([InlineKeyboardButton(text="📺 Barcha qismlar", callback_data=f"episodes_{movie_id}")])
    buttons.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data=f"movie_{movie_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        await callback.message.delete()
    except Exception:
        pass
    try:
        if file_type == "channel" and source_chat_id and source_message_id:
            from_chat = int(source_chat_id) if str(source_chat_id).lstrip('-').isdigit() else source_chat_id
            await bot.copy_message(
                chat_id=callback.message.chat.id,
                from_chat_id=from_chat,
                message_id=int(source_message_id),
                reply_markup=keyboard
            )
            await bot.send_message(callback.message.chat.id, caption, reply_markup=keyboard)
        else:
            await send_media(callback.message.chat.id, file_id, file_type, caption, keyboard)
    except Exception as e:
        logger.error(f"Error sending episode: {e}")
        await callback.message.answer("❌ Qism yuborilmadi. Kanalga bot qo‘shilganini tekshiring.")
    
    await callback.answer()

@router.message(F.text == "🔥 Trend")
async def trending_menu(message: Message):
    if not await enforce_subscription(message, message.from_user.id):
        return
    
    trending = db.get_trending_movies(7, 10)
    
    if not trending:
        await message.answer("Hozircha trend medialar yo'q")
        return
    
    text = "🔥 <b>TOP 10 Trend Media</b>\n"
    text += "<i>(So'nggi 7 kun)</i>\n\n"
    
    buttons = []
    for i, movie in enumerate(trending, 1):
        movie_id = movie[0]
        title = movie[1]
        recent_views = movie[-1] if len(movie) > 0 else 0
        if recent_views is None:
            recent_views = 0
        text += f"{i}. {title} - 👁 {recent_views}\n"
        buttons.append([InlineKeyboardButton(text=f"{i}. {title}", callback_data=f"movie_{movie_id}")])
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# ================================
# HANDLERS - RECOMMENDATIONS
# ================================
@router.message(F.text == "⭐ Tavsiyalar")
async def recommendations_menu(message: Message):
    if not await enforce_subscription(message, message.from_user.id):
        return
    
    # Get random movies from each category
    categories = ['kino', 'anime', 'dorama', 'multfilm']
    text = "⭐ <b>Sizga tavsiyalar</b>\n\n"
    
    buttons = []
    
    for category in categories:
        movies = db.get_movies_by_category(category, 1)
        if movies:
            movie = movies[0]
            movie_id = movie[0]
            title = movie[1]
            emoji = {"kino": "🎬", "anime": "🎌", "dorama": "🇰🇷", "multfilm": "🧒"}
            text += f"{emoji.get(category, '🎬')} {category.capitalize()}: {title}\n"
            buttons.append([InlineKeyboardButton(
                text=f"{emoji.get(category, '🎬')} {title}",
                callback_data=f"movie_{movie_id}"
            )])
    
    if not buttons:
        await message.answer("Hozircha tavsiyalar yo'q")
        return
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# ================================
# HANDLERS - PREMIUM
# ================================
@router.message(F.text.in_(["💎 Premium", "💎 Premium ✅"]))
async def premium_menu(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if db.is_premium(user_id):
        user = db.get_user(user_id)
        premium_until = datetime.fromisoformat(user[6]).strftime("%d.%m.%Y %H:%M")
        
        await message.answer(
            f"✅ <b>Siz Premium foydalanuvchisiz!</b>\n\n"
            f"💎 Premium muddati: <code>{premium_until}</code> gacha\n\n"
            f"<b>Premium imkoniyatlar:</b>\n"
            f"✅ Majburiy obuna yo'q\n"
            f"🚀 Tez yuklab olish\n"
            f"🎯 Maxsus AI tavsiyalar\n"
            f"🔔 Reklamasiz"
        )
    else:
        buttons = [
            [InlineKeyboardButton(text=f"💳 Premium sotib olish ({get_premium_price_monthly():,} so'm)", callback_data="buy_premium")]
        ]
        await message.answer(
            f"💎 <b>Premium xizmat</b>\n\n"
            f"<b>Premium imkoniyatlar:</b>\n"
            f"✅ Majburiy obuna yo'q\n"
            f"🚀 Tez yuklab olish\n"
            f"🎯 Maxsus AI tavsiyalar\n"
            f"🔔 Reklamasiz\n\n"
            f"💰 <b>Narx:</b> {get_premium_price_monthly():,} so'm/oy\n\n"
            f"💳 <b>Karta:</b> {get_card_number()}\n"
            f"👤 <b>Ism:</b> {get_card_owner()}\n\n"
            f"Chekni yuboring.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        await state.set_state(UserStates.waiting_payment)

@router.callback_query(F.data == "buy_premium")
async def callback_buy_premium(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        f"💎 <b>Premium xizmat</b>\n\n"
        f"💰 <b>Narx:</b> {get_premium_price_monthly():,} so'm/oy\n"
        f"💳 <b>Karta:</b> {get_card_number()}\n"
        f"👤 <b>Ism:</b> {get_card_owner()}\n\n"
        f"Chekni yuboring."
    )
    await state.set_state(UserStates.waiting_payment)
    try:
        await callback.answer()
    except Exception:
        pass

@router.message(UserStates.waiting_payment, F.photo)
async def payment_photo(message: Message, state: FSMContext):
    payment_id = db.create_payment(message.from_user.id, get_premium_price_monthly(), "card")
    for admin_id in ADMIN_IDS:
        try:
            await bot.forward_message(admin_id, message.chat.id, message.message_id)
            buttons = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"pay_ok_{payment_id}"),
                InlineKeyboardButton(text="❌ Rad etish", callback_data=f"pay_no_{payment_id}")
            ]])
            await bot.send_message(
                admin_id,
                f"Chek\nUser: {message.from_user.id}\nSumma: {get_premium_price_monthly():,} so'm",
                reply_markup=buttons
            )
        except Exception:
            pass
    await message.answer("Tekshiruvda")
    await state.clear()

@router.message(UserStates.waiting_payment, F.text)
async def payment_cancel_or_text(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Bekor qilindi", reply_markup=get_main_keyboard(db.is_premium(message.from_user.id), message.from_user.id in ADMIN_IDS))
        return
    await message.answer("❌ Chekni rasm yoki fayl ko‘rinishida yuboring. Bekor qilish: /cancel")

@router.message(UserStates.waiting_payment, F.document)
async def payment_doc(message: Message, state: FSMContext):
    payment_id = db.create_payment(message.from_user.id, get_premium_price_monthly(), "card")
    for admin_id in ADMIN_IDS:
        try:
            await bot.forward_message(admin_id, message.chat.id, message.message_id)
            buttons = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"pay_ok_{payment_id}"),
                InlineKeyboardButton(text="❌ Rad etish", callback_data=f"pay_no_{payment_id}")
            ]])
            await bot.send_message(
                admin_id,
                f"Chek\nUser: {message.from_user.id}\nSumma: {get_premium_price_monthly():,} so'm",
                reply_markup=buttons
            )
        except Exception:
            pass
    await message.answer("Tekshiruvda")
    await state.clear()

@router.callback_query(F.data.startswith("pay_ok_") | F.data.startswith("pay_no_"))
async def payment_decision(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        try:
            await callback.answer()
        except Exception:
            pass
        return
    action, payment_id = callback.data.split("_", 2)[1], callback.data.split("_", 2)[2]
    pay = db.get_payment(int(payment_id))
    if not pay:
        await callback.message.answer("Topilmadi")
        try:
            await callback.answer()
        except Exception:
            pass
        return
    user_id = pay[1]
    if action == "ok":
        db.update_payment_status(int(payment_id), "approved")
        db.add_premium(user_id, days=30)
        await bot.send_message(user_id, "✅ Premium aktiv")
        await callback.message.answer("✅ Tasdiqlandi")
    else:
        db.update_payment_status(int(payment_id), "denied")
        await bot.send_message(user_id, "❌ Chek rad")
        await callback.message.answer("❌ Rad etildi")
    try:
        await callback.answer()
    except Exception:
        pass

# ================================
# HANDLERS - HELP
# ================================
@router.message(F.text == "ℹ️ Yordam")
async def help_menu(message: Message):
    await message.answer(
        "ℹ️ <b>Bot haqida yordam</b>\n\n"
        
        "<b>🔍 Qidirish:</b>\n"
        "Kino nomi yoki kodini yuboring. Masalan:\n"
        "• Spiderman\n"
        "• SPID001\n\n"
        
        "<b>📂 Kategoriyalar:</b>\n"
        "Turli kategoriyalardan kinolarni ko'ring:\n"
        "• 🎬 Kino\n"
        "• 🎌 Anime\n"
        "• 🇰🇷 Dorama\n"
        "• 🧒 Multfilm\n\n"
        
        "<b>🔥 Trend:</b>\n"
        "Eng ko'p ko'rilayotgan medialarni toping\n\n"
        
        "<b>⭐ Tavsiyalar:</b>\n"
        "Sizga maxsus tavsiyalar\n\n"
        
        "<b>💎 Premium:</b>\n"
        "Premium obuna uchun ma'lumot\n\n"
        
        "<b>📞 Murojaat:</b>\n"
        "Savol va takliflar uchun: @admin"
    )

# ================================
# HANDLERS - ADMIN
# ================================
@router.message(Command("admin"))
@router.message(F.text == "Admin panel")
async def admin_menu(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Sizda admin huquqi yo'q!")
        return

    await message.answer(
        "👨‍💼 <b>Admin Panel</b>\n\n"
        "Quyidagi amallarni tanlang:",
        reply_markup=get_admin_keyboard()
    )

@router.message(F.text == "💳 Premium sozlamalar")
async def admin_premium_settings(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    text = (
        "💳 <b>Premium sozlamalar</b>\n\n"
        f"💰 <b>Narx:</b> {get_premium_price_monthly():,} so'm/oy\n"
        f"💳 <b>Karta:</b> {get_card_number()}\n"
        f"👤 <b>Ism:</b> {get_card_owner()}\n\n"
        "Quyidan birini tanlang:"
    )
    await message.answer(text, reply_markup=get_premium_settings_keyboard())

@router.message(F.text == "💰 Narx")
async def admin_premium_price_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "Yangi premium narxini kiriting (faqat raqam).\nBekor qilish: /cancel"
    )
    await state.set_state(AdminStates.update_premium_price)

@router.message(F.text == "💳 Karta raqami")
async def admin_card_number_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "Yangi karta raqamini kiriting.\nBekor qilish: /cancel"
    )
    await state.set_state(AdminStates.update_card_number)

@router.message(F.text == "👤 Karta egasi")
async def admin_card_owner_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "Yangi karta egasi ismini kiriting.\nBekor qilish: /cancel"
    )
    await state.set_state(AdminStates.update_card_owner)

@router.message(AdminStates.update_premium_price)
async def admin_premium_price_save(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text in ("/cancel", "Admin panel", "🔙 Orqaga"):
        await state.clear()
        await message.answer("Admin panel", reply_markup=get_admin_keyboard())
        return
    raw = message.text.strip().replace(" ", "").replace(",", "")
    if not raw.isdigit():
        await message.answer("❌ Noto'g'ri format! Faqat raqam kiriting.")
        return
    db.set_setting("premium_price_monthly", raw)
    await state.clear()
    await message.answer("✅ Premium narxi yangilandi.", reply_markup=get_premium_settings_keyboard())

@router.message(AdminStates.update_card_number)
async def admin_card_number_save(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text in ("/cancel", "Admin panel", "🔙 Orqaga"):
        await state.clear()
        await message.answer("Admin panel", reply_markup=get_admin_keyboard())
        return
    value = message.text.strip()
    if not value:
        await message.answer("❌ Karta raqami bo'sh bo'lishi mumkin emas.")
        return
    db.set_setting("card_number", value)
    await state.clear()
    await message.answer("✅ Karta raqami yangilandi.", reply_markup=get_premium_settings_keyboard())

@router.message(AdminStates.update_card_owner)
async def admin_card_owner_save(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text in ("/cancel", "Admin panel", "🔙 Orqaga"):
        await state.clear()
        await message.answer("Admin panel", reply_markup=get_admin_keyboard())
        return
    value = message.text.strip()
    if not value:
        await message.answer("❌ Ism bo'sh bo'lishi mumkin emas.")
        return
    db.set_setting("card_owner", value)
    await state.clear()
    await message.answer("✅ Karta egasi yangilandi.", reply_markup=get_premium_settings_keyboard())

@router.message(F.text == "👥 Statistika")
async def admin_statistics(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    stats = db.get_statistics()
    
    text = "📊 <b>Bot Statistikasi</b>\n\n"
    text += f"👥 Jami foydalanuvchilar: <b>{stats['total_users']}</b>\n"
    text += f"💎 Premium foydalanuvchilar: <b>{stats['premium_users']}</b>\n"
    text += f"📈 Bugungi faol foydalanuvchilar: <b>{stats['today_active']}</b>\n\n"
    
    text += f"🎬 Jami kinolar: <b>{stats['total_movies']}</b>\n"
    text += f"📺 Jami seriallar: <b>{stats['total_series']}</b>\n\n"
    
    text += f"🔍 Jami qidiruvlar: <b>{stats['total_searches']}</b>\n"
    text += f"👁 Jami ko'rishlar: <b>{stats['total_views']}</b>\n\n"
    
    text += f"📢 Faol kanallar: <b>{stats['total_channels']}</b>"
    
    await message.answer(text)

@router.message(F.text == "📊 Top qidiruvlar")
async def admin_top_searches(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    searches = db.get_top_searches(10)
    
    if not searches:
        await message.answer("Hozircha qidiruvlar yo'q")
        return
    
    text = "📊 <b>TOP 10 Qidiruvlar</b>\n"
    text += "<i>(So'nggi 7 kun)</i>\n\n"
    
    for i, (query, count) in enumerate(searches, 1):
        text += f"{i}. <code>{query}</code> - {count} marta\n"
    
    await message.answer(text)

# ===== ADMIN: ADD CHANNEL =====
@router.message(F.text == "Majburiy kanal qo'shish")
@router.message(F.text == "➕ Kanal qo'shish")
async def admin_add_channel_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "📢 <b>Majburiy kanal qo'shish</b>\n\n"
        "Kanal ID'sini yuboring.\n"
        "Masalan: <code>-1001234567890</code> yoki <code>@channel_username</code>\n\n"
        "Bekor qilish: /cancel"
    )
    await state.set_state(AdminStates.add_channel_waiting_id)

@router.message(AdminStates.add_channel_waiting_id)
async def admin_add_channel_id(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("? Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    raw_text = (message.text or "").strip()
    if raw_text in ("?? Orqaga", "Admin panel") or not raw_text:
        await message.answer("? Kanal ID noto?g?ri. @username yoki -100... kiriting.")
        return

    invite_link = parse_invite_link(raw_text)
    channel_id = parse_channel_input(raw_text)

    # If only invite link provided, ask for channel id/username separately
    if invite_link and not channel_id:
        await state.update_data(invite_link=invite_link)
        await message.answer(
            "? Invite link saqlandi. Endi kanal ID yoki @username yuboring:\n"
            "Masalan: <code>-1001234567890</code> yoki <code>@channel_username</code>"
        )
        return

    if not channel_id:
        await message.answer("? Kanal ID noto?g?ri. @username yoki -100... kiriting.")
        return

    data = await state.get_data()
    if not invite_link:
        invite_link = data.get("invite_link")

    await state.update_data(channel_id=channel_id, invite_link=invite_link)
    await message.answer(
        "?? Kanal nomini kiriting:\n\n"
        "Bekor qilish: /cancel"
    )
    await state.set_state(AdminStates.add_channel_waiting_name)

@router.message(AdminStates.add_channel_waiting_name)


async def admin_add_channel_name(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("? Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    channel_name = (message.text or "").strip()
    if not channel_name:
        await message.answer("? Kanal nomi bo'sh bo'lishi mumkin emas.")
        return

    await state.update_data(channel_name=channel_name)

    buttons = [
        [InlineKeyboardButton(text="?? Zayafka", callback_data="channel_type_zayafka")],
        [InlineKeyboardButton(text="?? Ommaviy", callback_data="channel_type_public")],
        [InlineKeyboardButton(text="? Bekor qilish", callback_data="channel_type_cancel")]
    ]

    await message.answer(
        "?? Kanal turini tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await state.set_state(AdminStates.add_channel_waiting_type)

@router.callback_query(F.data.startswith("channel_type_"))
async def admin_add_channel_type(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split("_")[-1]

    if action == "cancel":
        await state.clear()
        await callback.message.edit_text("Bekor qilindi")
        await callback.message.answer("Admin panel", reply_markup=get_admin_keyboard())
        await callback.answer()
        return

    channel_type = action
    data = await state.get_data()

    channel_id = data.get('channel_id')
    channel_name = data.get('channel_name')
    invite_link = data.get('invite_link')

    if not channel_id or not channel_name:
        await state.clear()
        await callback.message.edit_text("Ma'lumotlar yetarli emas. Qaytadan urinib ko'ring.")
        await callback.message.answer("Admin panel", reply_markup=get_admin_keyboard())
        await callback.answer()
        return

    # If private channel id (-100...) and no invite link, ask for invite link
    if str(channel_id).startswith("-100") and not invite_link:
        await state.update_data(channel_type=channel_type)
        await callback.message.edit_text(
            "Kanal uchun invite link yuboring:\n"
            "Masalan: https://t.me/+xxxx yoki https://t.me/joinchat/xxxx\n\n"
            "Bekor qilish: /cancel"
        )
        await state.set_state(AdminStates.add_channel_waiting_invite)
        await callback.answer()
        return

    resolved_id, resolved_username = await resolve_channel_id(str(channel_id))
    channel_username = resolved_username if resolved_username else (resolved_id.replace('@', '') if str(resolved_id).startswith('@') else None)
    channel_id = resolved_id

    success = db.add_channel(channel_id, channel_name, channel_username, channel_type, invite_link=invite_link)

    if success:
        await callback.message.edit_text(
            f"Kanal qo'shildi!\n\n"
            f"Nom: {channel_name}\n"
            f"ID: <code>{channel_id}</code>\n"
            f"Tur: {channel_type}"
        )
    else:
        await callback.message.edit_text("Kanal allaqachon mavjud!")

    await state.clear()
    await callback.message.answer("Admin panel", reply_markup=get_admin_keyboard())
    await callback.answer()

@router.message(AdminStates.add_channel_waiting_invite)
async def admin_add_channel_invite(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    invite_link = parse_invite_link(message.text)
    if not invite_link:
        await message.answer(
            "Invite link noto?g?ri. Quyidagi formatdan foydalaning:\n"
            "https://t.me/+xxxx yoki https://t.me/joinchat/xxxx"
        )
        return

    data = await state.get_data()
    channel_id = data.get('channel_id')
    channel_name = data.get('channel_name')
    channel_type = data.get('channel_type')

    if not channel_id or not channel_name or not channel_type:
        await state.clear()
        await message.answer("Ma'lumotlar yetarli emas. Qaytadan urinib ko'ring.", reply_markup=get_admin_keyboard())
        return

    resolved_id, resolved_username = await resolve_channel_id(str(channel_id))
    channel_username = resolved_username if resolved_username else (resolved_id.replace('@', '') if str(resolved_id).startswith('@') else None)
    channel_id = resolved_id

    success = db.add_channel(channel_id, channel_name, channel_username, channel_type, invite_link=invite_link)

    if success:
        await message.answer(
            f"Kanal qo'shildi!\n\n"
            f"Nom: {channel_name}\n"
            f"ID: <code>{channel_id}</code>\n"
            f"Tur: {channel_type}",
            reply_markup=get_admin_keyboard()
        )
    else:
        await message.answer("Kanal allaqachon mavjud!", reply_markup=get_admin_keyboard())

    await state.clear()


@router.message(F.text == "Majburiy kanal o'chirish")
async def admin_delete_channel_start(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    channels = db.get_all_channels()
    if not channels:
        await message.answer("Majburiy obuna kanallari hozircha yo'q.", reply_markup=get_admin_keyboard())
        return

    await message.answer(
        "O'chirmoqchi bo'lgan majburiy kanalni tanlang:",
        reply_markup=get_mandatory_channel_delete_keyboard(channels),
    )


@router.callback_query(F.data == "mandatory_del_cancel")
async def admin_delete_channel_cancel(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Sizda huquq yo'q", show_alert=True)
        return

    await callback.message.edit_text("Bekor qilindi.")
    await callback.answer()


@router.callback_query(F.data.startswith("mandatory_del_id_"))
async def admin_delete_channel_confirm(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Sizda huquq yo'q", show_alert=True)
        return

    raw_id = callback.data.replace("mandatory_del_id_", "", 1)
    if not raw_id.isdigit():
        await callback.answer("Noto'g'ri so'rov", show_alert=True)
        return

    row_id = int(raw_id)
    channels = db.get_all_channels()
    selected = next((channel for channel in channels if str(channel[0]) == str(row_id)), None)

    if not selected:
        await callback.message.edit_text("Kanal topilmadi yoki allaqachon o'chirilgan.")
        await callback.answer("Topilmadi", show_alert=True)
        return

    channel_id = str(selected[1])
    channel_name = selected[2] or channel_id
    db.delete_channel(channel_id)

    await callback.message.edit_text(
        f"✅ Majburiy kanal o'chirildi.\n\n"
        f"Nom: {channel_name}\n"
        f"ID: <code>{channel_id}</code>"
    )

    remaining_channels = db.get_all_channels()
    if remaining_channels:
        await callback.message.answer(
            "Yana o'chirish uchun kanal tanlang:",
            reply_markup=get_mandatory_channel_delete_keyboard(remaining_channels),
        )
    else:
        await callback.message.answer(
            "Majburiy obuna kanallari tugadi.",
            reply_markup=get_admin_keyboard(),
        )

    await callback.answer("O'chirildi")


# ===== ADMIN: DELETE MOVIE =====
@router.message(F.text == "Kino o'chirish")
async def admin_delete_movie_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer(
        "<b>Kino/serial o'chirish</b>\n\n"
        "O'chirish uchun kodni yuboring.\n"
        "Masalan: <code>SPID001</code>\n\n"
        "Bekor qilish: /cancel"
    )
    await state.set_state(AdminStates.delete_movie_waiting_code)


@router.message(AdminStates.delete_movie_waiting_code)
async def admin_delete_movie_process(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    raw = (message.text or "").strip()
    if raw == "/cancel":
        await state.clear()
        await message.answer("Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    if not raw:
        await message.answer("Kod bo'sh bo'lishi mumkin emas. Masalan: SPID001")
        return

    movie = db.deactivate_movie_by_code(raw)
    if not movie:
        await message.answer(
            "Bu kod bo'yicha faol media topilmadi.\n"
            "Kodni tekshirib qayta yuboring yoki /cancel bosing."
        )
        return

    movie_id, title, code, file_id, file_type, media_type, category, *_ = movie
    deleted_episodes = 0
    if media_type == "series":
        deleted_episodes = db.delete_series_episodes(movie_id)

    text = (
        "Media o'chirildi.\n\n"
        f"Nomi: {title}\n"
        f"Kod: <code>{code}</code>\n"
        f"Tur: {media_type}\n"
        f"Kategoriya: {category}"
    )
    if deleted_episodes:
        text += f"\nO'chirilgan qismlar: {deleted_episodes}"

    await state.clear()
    await message.answer(text, reply_markup=get_admin_keyboard())


# ===== ADMIN: ADD MOVIE =====


@router.message(F.text == "🎬 Kino qo'shish")
async def admin_add_movie_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "🎬 <b>Kino qo'shish</b>\n\n"
        "Kino nomini kiriting:\n\n"
        "Bekor qilish: /cancel"
    )
    await state.set_state(AdminStates.add_movie_waiting_title)

@router.message(AdminStates.add_movie_waiting_title)
async def admin_add_movie_title(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("? Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    title = (message.text or "").strip()
    if not title:
        await message.answer("? Kino nomi bo'sh bo'lishi mumkin emas.")
        return

    await state.update_data(title=title)
    await message.answer("?? Kino kodini kiriting (masalan: SPID001):")
    await state.set_state(AdminStates.add_movie_waiting_code)

@router.message(AdminStates.add_movie_waiting_code)
async def admin_add_movie_code(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("? Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    code = (message.text or "").strip().upper()
    if not code:
        await message.answer("? Kod bo'sh bo'lishi mumkin emas.")
        return

    await state.update_data(code=code)

    buttons = [
        [InlineKeyboardButton(text="?? Kino", callback_data="movie_type_movie")],
        [InlineKeyboardButton(text="?? Serial", callback_data="movie_type_series")],
        [InlineKeyboardButton(text="? Bekor qilish", callback_data="movie_type_cancel")]
    ]

    await message.answer(
        "?? Media turini tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await state.set_state(AdminStates.add_movie_waiting_type)

@router.callback_query(F.data.startswith("movie_type_"))
async def admin_add_movie_type(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split("_")[-1]
    
    if action == "cancel":
        await state.clear()
        await callback.message.edit_text("❌ Bekor qilindi")
        await callback.message.answer("Admin panel", reply_markup=get_admin_keyboard())
        await callback.answer()
        return
    
    await state.update_data(media_type=action)
    
    buttons = [
        [InlineKeyboardButton(text="🎬 Kino", callback_data="admin_cat_kino")],
        [InlineKeyboardButton(text="🎌 Anime", callback_data="admin_cat_anime")],
        [InlineKeyboardButton(text="🇰🇷 Dorama", callback_data="admin_cat_dorama")],
        [InlineKeyboardButton(text="🧒 Multfilm", callback_data="admin_cat_multfilm")],
        [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin_cat_cancel")]
    ]
    
    await callback.message.edit_text(
        "📂 Kategoriyani tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await state.set_state(AdminStates.add_movie_waiting_category)
    await callback.answer()

@router.callback_query(F.data.startswith("admin_cat_"))
async def admin_add_movie_category(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split("_")[-1]
    
    if action == "cancel":
        await state.clear()
        await callback.message.edit_text("❌ Bekor qilindi")
        await callback.message.answer("Admin panel", reply_markup=get_admin_keyboard())
        await callback.answer()
        return
    
    await state.update_data(category=action)
    await callback.message.edit_text("📝 Tavsif kiriting (yoki /skip):")
    await state.set_state(AdminStates.add_movie_waiting_description)
    await callback.answer()

@router.message(AdminStates.add_movie_waiting_description)
async def admin_add_movie_description(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Bekor qilindi", reply_markup=get_admin_keyboard())
        return
    
    description = None if message.text == "/skip" else message.text.strip()
    await state.update_data(description=description)
    
    await message.answer("📅 Yilni kiriting (yoki /skip):")
    await state.set_state(AdminStates.add_movie_waiting_year)

@router.message(AdminStates.add_movie_waiting_year)
async def admin_add_movie_year(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("? Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    year = None
    if message.text != "/skip":
        try:
            year = int(message.text.strip())
        except:
            await message.answer("? Noto'g'ri format! Raqam kiriting yoki /skip")
            return

    await state.update_data(year=year)
    await message.answer("? Reytingni kiriting (1-10 yoki /skip):")
    await state.set_state(AdminStates.add_movie_waiting_rating)

@router.message(AdminStates.add_movie_waiting_rating)
async def admin_add_movie_rating(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    rating = None
    if message.text != "/skip":
        try:
            rating = float(message.text.strip())
            if rating < 1 or rating > 10:
                await message.answer("Reyting 1-10 oralig'ida bo'lishi kerak")
                return
        except Exception:
            await message.answer("Noto'g'ri format. Raqam kiriting yoki /skip")
            return

    await state.update_data(rating=rating)
    await message.answer("Video, document yoki kanal post link yuboring")
    await state.set_state(AdminStates.add_movie_waiting_file)

@router.message(AdminStates.add_movie_waiting_file, F.video | F.document | F.animation)
async def admin_add_movie_file(message: Message, state: FSMContext):
    data = await state.get_data()

    file_id = None
    file_type = None
    if message.video:
        file_id = message.video.file_id
        file_type = "video"
    elif message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.animation:
        file_id = message.animation.file_id
        file_type = "animation"

    if not file_id:
        await message.answer("Fayl turi noto?g?ri. Video yoki dokument yuboring.")
        return

    movie_id = db.add_movie(
        title=data['title'],
        code=data['code'],
        file_id=file_id,
        file_type=file_type,
        media_type=data['media_type'],
        category=data['category'],
        description=data.get('description'),
        year=data.get('year'),
        rating=data.get('rating')
    )

    if movie_id:
        text = (
            f"Kino qo'shildi!\n\n"
            f"Nom: {data['title']}\n"
            f"Kod: <code>{data['code']}</code>\n"
            f"Kategoriya: {data['category']}\n"
            f"Tur: {data['media_type']}"
        )
        if data['media_type'] == 'series':
            text += "\n<i>Endi qismlarni qo'shishingiz mumkin.</i>"
            await state.update_data(current_movie_id=movie_id)
            await message.answer(text)
            await message.answer(
                "Birinchi qism raqamini kiriting yoki linklarni yuboring (har qatorda bitta link).",
                reply_markup=get_admin_keyboard()
            )
            await state.set_state(AdminStates.add_series_waiting_episode)
        else:
            await message.answer(text, reply_markup=get_admin_keyboard())
            await state.clear()
    else:
        await message.answer("Xatolik! Kod allaqachon mavjud.", reply_markup=get_admin_keyboard())
        await state.clear()

@router.message(AdminStates.add_movie_waiting_file, F.text)
async def admin_add_movie_links(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    data = await state.get_data()
    links = parse_tme_c_links(message.text or "")

    if data.get("media_type") == "movie":
        if not links:
            await message.answer("Link yuboring. Masalan: https://t.me/c/xxxx/yyyy")
            return
        chat_id, msg_id = links[0]
        movie_id = db.add_movie(
            title=data['title'],
            code=data['code'],
            file_id="channel",
            file_type="channel",
            media_type="movie",
            category=data['category'],
            description=data.get('description'),
            year=data.get('year'),
            rating=data.get('rating'),
            source_chat_id=chat_id,
            source_message_id=msg_id
        )
        if not movie_id:
            await message.answer("Xatolik: kod allaqachon mavjud", reply_markup=get_admin_keyboard())
            await state.clear()
            return
        await message.answer("Kino linki qo'shildi", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    if not links:
        await message.answer("Serial uchun link yuboring. Har qatorda bitta link bo'lsin")
        return

    movie_id = db.add_movie(
        title=data['title'],
        code=data['code'],
        file_id="series",
        file_type="series",
        media_type="series",
        category=data['category'],
        description=data.get('description'),
        year=data.get('year'),
        rating=data.get('rating')
    )
    if not movie_id:
        await message.answer("Xatolik: kod allaqachon mavjud", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    ep_num = 1
    for chat_id, msg_id in links:
        db.add_series_episode(
            movie_id=movie_id,
            episode_number=ep_num,
            episode_title=f"{ep_num}-qism",
            file_id="channel",
            file_type="channel",
            source_chat_id=chat_id,
            source_message_id=msg_id,
        )
        ep_num += 1

    await message.answer("Serial linklari qo'shildi", reply_markup=get_admin_keyboard())
    await state.clear()

# ===== ADMIN: ADD SERIES EPISODES =====
@router.message(AdminStates.add_series_waiting_episode)
async def admin_add_episode_number(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    if message.text == "/done":
        await state.clear()
        await message.answer("Serial qo'shish yakunlandi", reply_markup=get_admin_keyboard())
        return

    data = await state.get_data()
    movie_id = data.get('current_movie_id')
    if not movie_id:
        await state.clear()
        await message.answer("Session yo'qolgan. Qayta boshlang", reply_markup=get_admin_keyboard())
        return

    links = parse_tme_c_links(message.text or "")
    if links:
        ep_num = 1
        for chat_id, msg_id in links:
            db.add_series_episode(
                movie_id=movie_id,
                episode_number=ep_num,
                episode_title=f"{ep_num}-qism",
                file_id="channel",
                file_type="channel",
                source_chat_id=chat_id,
                source_message_id=msg_id,
            )
            ep_num += 1
        await message.answer("Qismlar qo'shildi", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    try:
        episode_number = int((message.text or "").strip())
    except Exception:
        await message.answer("Noto'g'ri format. Raqam yoki link yuboring")
        return

    await state.update_data(episode_number=episode_number)
    await message.answer(f"{episode_number}-qism uchun video yoki document yuboring")
    await state.set_state(AdminStates.add_series_waiting_file)

@router.message(AdminStates.add_series_waiting_file, F.video | F.document | F.animation)
async def admin_add_episode_file(message: Message, state: FSMContext):
    data = await state.get_data()

    movie_id = data['current_movie_id']
    episode_number = data['episode_number']

    file_id = None
    file_type = None
    if message.video:
        file_id = message.video.file_id
        file_type = "video"
    elif message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.animation:
        file_id = message.animation.file_id
        file_type = "animation"

    if not file_id:
        await message.answer("Fayl turi noto?g?ri. Video yoki dokument yuboring.")
        return

    episode_title = f"{episode_number}-qism"

    success = db.add_series_episode(movie_id, episode_number, episode_title, file_id, file_type=file_type)

    if success:
        await message.answer(
            f"{episode_number}-qism qo'shildi!\n\n"
            "Keyingi qism raqamini kiriting yoki /done:"
        )
        await state.set_state(AdminStates.add_series_waiting_episode)
    else:
        await message.answer("Bu qism allaqachon mavjud!")
        await state.set_state(AdminStates.add_series_waiting_episode)

@router.message(AdminStates.add_series_waiting_file, F.text)
async def admin_add_episode_links(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    links = parse_tme_c_links(message.text or "")
    if not links:
        await message.answer("Linklarni yuboring. Har qatorda bitta link bo'lsin")
        return

    data = await state.get_data()
    movie_id = data.get('current_movie_id')
    if not movie_id:
        await state.clear()
        await message.answer("Session yo'qolgan. Qayta boshlang", reply_markup=get_admin_keyboard())
        return

    ep_num = data.get('episode_number', 1)
    for chat_id, msg_id in links:
        success = db.add_series_episode(
            movie_id=movie_id,
            episode_number=ep_num,
            episode_title=f"{ep_num}-qism",
            file_id="channel",
            file_type="channel",
            source_chat_id=chat_id,
            source_message_id=msg_id,
        )
        if success:
            ep_num += 1

    await message.answer("Qismlar qo'shildi. Keyingi qism raqamini kiriting yoki /done")
    await state.set_state(AdminStates.add_series_waiting_episode)

# ===== ADMIN: BROADCAST =====
@router.message(F.text == "📢 Broadcast")
async def admin_broadcast_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "📢 <b>Broadcast</b>\n\n"
        "Barcha foydalanuvchilarga yuborish uchun xabarni yuboring:\n\n"
        "Bekor qilish: /cancel"
    )
    await state.set_state(AdminStates.broadcast_waiting_message)

# ===== ADMIN: MANUAL CHANNEL SCAN =====
@router.message(F.text == "📥 Kanalni skan qilish")
async def admin_scan_channel_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer(
        "Kanalni skan qilish funksiyasi o'chirildi.",
        reply_markup=get_admin_keyboard()
    )
    await state.clear()
    return

    await message.answer(
        "📥 <b>Eski postlarni qo‘lda skan qilish</b>\n\n"
        "Har qatorda bitta link yuboring.\n"
        "Format (tavsiya):\n"
        "https://t.me/c/123/456 | Nomi: Avatar | Qism: 1 | Type: serial | Category: anime\n\n"
        "Yoki qisqa:\n"
        "https://t.me/c/123/456 | Avatar 1-qism\n\n"
        "Bekor qilish: /cancel"
    )
    await state.set_state(AdminStates.scan_waiting_lines)

@router.message(AdminStates.scan_waiting_lines)
async def admin_scan_channel_process(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Kanalni skan qilish funksiyasi o'chirildi.",
        reply_markup=get_admin_keyboard()
    )
    return

    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Bekor qilindi", reply_markup=get_admin_keyboard())
        return

    lines = [l.strip() for l in message.text.splitlines() if l.strip()]
    if not lines:
        await message.answer("❌ Hech narsa topilmadi. Linklarni yuboring.")
        return

    added = 0
    skipped = 0
    for line in lines:
        data = parse_scan_line(line)
        if not data or not data.get("title"):
            skipped += 1
            continue

        chat_id = data["chat_id"]
        msg_id = data["msg_id"]
        title = data["title"]
        ep_num = data["episode"]
        media_type = data["media_type"]
        category = data["category"]

        # Avoid duplicates
        if db.get_movie_by_source(chat_id, msg_id) or db.get_episode_by_source(chat_id, msg_id):
            skipped += 1
            continue

        if ep_num is not None or media_type == "series":
            series = db.find_series_by_title(title)
            if not series:
                code = generate_code_from_title(title)
                series_id = db.add_movie(
                    title=title,
                    code=code,
                    file_id="series",
                    file_type="series",
                    media_type="series",
                    category=category,
                    description=None,
                    year=None,
                    rating=None
                )
                if not series_id:
                    skipped += 1
                    continue
                movie_id = series_id
            else:
                movie_id = series[0]

            if ep_num is None:
                skipped += 1
                continue

            ok = db.add_series_episode(
                movie_id=movie_id,
                episode_number=ep_num,
                episode_title=f"{ep_num}-qism",
                file_id="channel",
                file_type="channel",
                source_chat_id=chat_id,
                source_message_id=msg_id
            )
            if ok:
                added += 1
            else:
                skipped += 1
        else:
            code = generate_code_from_title(title)
            movie_id = db.add_movie(
                title=title,
                code=code,
                file_id="channel",
                file_type="channel",
                media_type="movie",
                category=category,
                description=None,
                year=None,
                rating=None,
                source_chat_id=chat_id,
                source_message_id=msg_id
            )
            if movie_id:
                added += 1
            else:
                skipped += 1

    await message.answer(
        f"✅ Yakunlandi!\n\n"
        f"✅ Qo‘shildi: {added}\n"
        f"⚠️ O‘tkazildi: {skipped}",
        reply_markup=get_admin_keyboard()
    )
    await state.clear()

@router.message(AdminStates.broadcast_waiting_message)
async def admin_broadcast_send(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Bekor qilindi", reply_markup=get_admin_keyboard())
        return
    
    users = db.get_all_user_ids()
    
    success = 0
    failed = 0
    
    status_message = await message.answer(f"📤 Yuborilmoqda... 0/{len(users)}")
    
    for i, user_id in enumerate(users, 1):
        try:
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            success += 1
        except Exception as e:
            failed += 1
            logger.error(f"Broadcast error for user {user_id}: {e}")
        
        if i % 10 == 0:
            await status_message.edit_text(f"📤 Yuborilmoqda... {i}/{len(users)}")
        
        await asyncio.sleep(0.05)
    
    await status_message.edit_text(
        f"✅ <b>Broadcast yakunlandi!</b>\n\n"
        f"✅ Muvaffaqiyatli: {success}\n"
        f"❌ Xatolik: {failed}"
    )
    
    await state.clear()

@router.message(F.text == "🔙 Orqaga")
async def admin_back(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "🏠 Asosiy menyu",
        reply_markup=get_main_keyboard(db.is_premium(message.from_user.id), message.from_user.id in ADMIN_IDS)
    )

# ================================
# CALLBACKS - NAVIGATION
# ================================
@router.callback_query(F.data == "back_main")
async def callback_back_main(callback: CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(
        "🏠 Asosiy menyu",
        reply_markup=get_main_keyboard(db.is_premium(callback.from_user.id), callback.from_user.id in ADMIN_IDS)
    )
    await callback.answer()

@router.callback_query(F.data == "back_categories")
async def callback_back_categories(callback: CallbackQuery):
    await callback.message.edit_text(
        "🎬 <b>Kategoriyalar</b>\n\n"
        "Qaysi kategoriyani ko'rmoqchisiz?",
        reply_markup=get_categories_keyboard()
    )
    await callback.answer()

# ================================
# MAIN
# ================================
async def on_startup():
    logger.info("🤖 Bot ishga tushmoqda...")
    logger.info(f"✅ Database initialized")
    logger.info(f"📊 Total users: {db.get_statistics()['total_users']}")
    logger.info(f"🎬 Total movies: {db.get_statistics()['total_movies']}")
    logger.info(f"📢 Total channels: {db.get_statistics()['total_channels']}")
    logger.info("✅ Bot tayyor!")

async def on_shutdown():
    logger.info("🔴 Bot to'xtatilmoqda...")

async def main():
    dp.include_router(router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🔴 Bot to'xtatildi")
