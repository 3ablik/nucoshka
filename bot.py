import asyncio
import json
import logging
import os
import html
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile, CallbackQuery, ChatJoinRequest, ChatMemberUpdated,
    ChatPermissions, Message, ReplyParameters,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from aiohttp import ClientSession, ClientTimeout, web
from dotenv import load_dotenv

from aiohttp import web

load_dotenv()
logging.basicConfig(level=logging.INFO)

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()

TZ = ZoneInfo(os.getenv("TIMEZONE", "UTC"))
SCHEDULE_FILE = Path("schedule.json")
CHATS_FILE = Path("chats.json")
GROUP_TYPES = {"group", "supergroup"}


# ---------- хранилище (простые json-файлы) ----------

# ---------- хранилище: PostgreSQL (или json-файлы, если DATABASE_URL не задан) ----------

DATABASE_URL = os.getenv("DATABASE_URL")
STORAGE_FILES = [
    "schedule.json", "chats.json", "users.json", "settings.json",
    "warns.json", "words.json", "pins.json",
]
SLOW_KEYS = {"users.json"}  # меняется часто: пишем в БД не чаще раза в 10 минут
SLOW_FLUSH_SECONDS = 600

_cache: dict = {}    # имя файла -> JSON-строка (копия того, что лежит в БД)
_pending: dict = {}  # имя файла -> JSON-строка, которую ещё нужно записать в БД
_lock = threading.Lock()
_write_lock = threading.Lock()
_wakeup = threading.Event()


def _db_connect():
    return psycopg.connect(DATABASE_URL, connect_timeout=15)


def _db_init() -> None:
    """Создаёт таблицу, грузит всё в память и переносит локальные json-файлы, которых ещё нет в БД."""
    for attempt in range(5):
        try:
            with _db_connect() as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS bot_storage "
                    "(key TEXT PRIMARY KEY, data JSONB NOT NULL)"
                )
                for key, raw in conn.execute("SELECT key, data::text FROM bot_storage").fetchall():
                    _cache[key] = raw
            break
        except Exception as e:
            logging.error("БД недоступна (попытка %s): %s", attempt + 1, e)
            time.sleep(3)
    else:
        raise RuntimeError("Не удалось подключиться к базе данных")

    for name in STORAGE_FILES:
        if name not in _cache and Path(name).exists():
            payload = json.dumps(
                json.loads(Path(name).read_text(encoding="utf-8")), ensure_ascii=False
            )
            _cache[name] = payload
            _pending[name] = payload


def _flush_once() -> bool:
    """Записывает накопившиеся изменения в БД. True, если всё записано."""
    with _write_lock:
        with _lock:
            batch = dict(_pending)
            _pending.clear()
        if not batch:
            return True
        try:
            with _db_connect() as conn:
                for key, payload in batch.items():
                    conn.execute(
                        "INSERT INTO bot_storage (key, data) VALUES (%s, %s::jsonb) "
                        "ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data",
                        (key, payload),
                    )
            return True
        except Exception as e:
            logging.error("БД: не удалось сохранить %s: %s", list(batch), e)
            with _lock:
                for key, payload in batch.items():
                    _pending.setdefault(key, payload)  # более свежая версия остаётся главной
            return False


def _writer_loop() -> None:
    while True:
        _wakeup.wait(timeout=SLOW_FLUSH_SECONDS)
        _wakeup.clear()
        if not _flush_once():
            time.sleep(5)
            _wakeup.set()  # повторим позже


def flush_storage() -> None:
    """Дописывает несохранённое в БД. Вызывается при остановке бота."""
    if not DATABASE_URL:
        return
    for _ in range(3):
        if _flush_once():
            return
        time.sleep(1)


def load_json(path: Path, default):
    if DATABASE_URL:
        with _lock:
            raw = _cache.get(path.name)
        return json.loads(raw) if raw is not None else default
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def save_json(path: Path, data) -> None:
    if not DATABASE_URL:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return
    payload = json.dumps(data, ensure_ascii=False)
    with _lock:
        _cache[path.name] = payload
        _pending[path.name] = payload
    if path.name not in SLOW_KEYS:
        _wakeup.set()


if DATABASE_URL:
    import psycopg
    _db_init()
    threading.Thread(target=_writer_loop, daemon=True).start()
    if _pending:
        _wakeup.set()
    logging.info("Хранилище: PostgreSQL")
else:
    logging.info("Хранилище: локальные json-файлы")


def remember_chat(chat) -> None:
    chats = load_json(CHATS_FILE, {})
    if chats.get(str(chat.id), {}).get("title") != chat.title:
        chats[str(chat.id)] = {"title": chat.title}
        save_json(CHATS_FILE, chats)


def forget_chat(chat_id: int) -> None:
    chats = load_json(CHATS_FILE, {})
    if chats.pop(str(chat_id), None) is not None:
        save_json(CHATS_FILE, chats)


def get_title(chat_id: int) -> str:
    return load_json(CHATS_FILE, {}).get(str(chat_id), {}).get("title", str(chat_id))


# ---------- права ----------

async def user_is_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in ("creator", "administrator")


# ---------- построение экранов панели ----------

async def build_chat_list(user_id: int):
    chats = load_json(CHATS_FILE, {})
    kb = InlineKeyboardBuilder()
    count = 0
    for chat_id, info in chats.items():
        if await user_is_admin(int(chat_id), user_id):
            kb.button(text=info["title"], callback_data=f"chat:{chat_id}")
            count += 1
    kb.adjust(1)
    if count == 0:
        return (
            "Не нашёл групп, где ты админ и есть я.\n\n"
            "Добавь меня в группу (я должен быть админом) и напиши там "
            "любую команду, например /panel. Потом вернись сюда и снова нажми /panel.",
            None,
        )
    return "Выбери группу:", kb.as_markup()


def chat_menu(chat_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="⏰ Расписание", callback_data=f"sched:{chat_id}")
    kb.button(text="⚠️ Предупреждения", callback_data=f"warn:{chat_id}")
    kb.button(text="🚫 Запретные слова", callback_data=f"fw:{chat_id}")
    kb.button(text="📥 Заявки в чат", callback_data=f"jn:{chat_id}")
    kb.button(text="⬅️ К списку групп", callback_data="chats")
    kb.adjust(1)
    return f"Группа: {get_title(chat_id)}\nЧто настроим?", kb.as_markup()


def schedule_view(chat_id: int):
    items = [i for i in load_json(SCHEDULE_FILE, []) if i["chat_id"] == chat_id]
    items.sort(key=lambda i: i["time"])

    kb = InlineKeyboardBuilder()
    for i in items:
        preview = i["text"].replace("\n", " ")[:30]
        kb.button(text=f"🗑 {i['time']} — {preview}", callback_data=f"del:{chat_id}:{i['id']}")
    kb.button(text="➕ Добавить", callback_data=f"add:{chat_id}")
    kb.button(text="⬅️ Назад", callback_data=f"chat:{chat_id}")
    kb.adjust(1)

    if items:
        text = "⏰ Расписание. Нажми на сообщение, чтобы удалить его:"
    else:
        text = "⏰ Расписание пока пустое."
    return text, kb.as_markup()


# ---------- состояния (пошаговый ввод) ----------

class AddSchedule(StatesGroup):
    time = State()
    text = State()


# ---------- личка: вход в панель ----------

@dp.message(CommandStart(), F.chat.type == "private")
@dp.message(Command("panel"), F.chat.type == "private")
async def cmd_panel(message: Message, state: FSMContext):
    await state.clear()
    text, markup = await build_chat_list(message.from_user.id)
    await message.answer(text, reply_markup=markup)


@dp.message(Command("cancel"), F.chat.type == "private")
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено. Открыть панель: /panel")


# ---------- кнопки ----------

@dp.callback_query(F.data == "chats")
async def cb_chats(call: CallbackQuery, state: FSMContext):
    await state.clear()
    text, markup = await build_chat_list(call.from_user.id)
    await call.message.edit_text(text, reply_markup=markup)
    await call.answer()


@dp.callback_query(F.data.startswith("chat:"))
async def cb_chat(call: CallbackQuery, state: FSMContext):
    await state.clear()
    chat_id = int(call.data.split(":")[1])
    if not await user_is_admin(chat_id, call.from_user.id):
        await call.answer("Нет прав в этой группе", show_alert=True)
        return
    text, markup = chat_menu(chat_id)
    await call.message.edit_text(text, reply_markup=markup)
    await call.answer()


@dp.callback_query(F.data.startswith("sched:"))
async def cb_sched(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])
    if not await user_is_admin(chat_id, call.from_user.id):
        await call.answer("Нет прав в этой группе", show_alert=True)
        return
    text, markup = schedule_view(chat_id)
    await call.message.edit_text(text, reply_markup=markup)
    await call.answer()


@dp.callback_query(F.data.startswith("del:"))
async def cb_del(call: CallbackQuery):
    _, chat_id, item_id = call.data.split(":")
    chat_id, item_id = int(chat_id), int(item_id)
    if not await user_is_admin(chat_id, call.from_user.id):
        await call.answer("Нет прав в этой группе", show_alert=True)
        return
    items = load_json(SCHEDULE_FILE, [])
    items = [i for i in items if not (i["id"] == item_id and i["chat_id"] == chat_id)]
    save_json(SCHEDULE_FILE, items)
    text, markup = schedule_view(chat_id)
    await call.message.edit_text(text, reply_markup=markup)
    await call.answer("Удалено")


@dp.callback_query(F.data.startswith("add:"))
async def cb_add(call: CallbackQuery, state: FSMContext):
    chat_id = int(call.data.split(":")[1])
    if not await user_is_admin(chat_id, call.from_user.id):
        await call.answer("Нет прав в этой группе", show_alert=True)
        return
    await state.set_state(AddSchedule.time)
    await state.update_data(chat_id=chat_id)
    await call.message.edit_text(
        "Во сколько отправлять каждый день? Формат ЧЧ:ММ, например 07:00\n\n"
        "Отмена: /cancel"
    )
    await call.answer()


# ---------- ввод времени и текста ----------

@dp.message(AddSchedule.time, F.text, F.chat.type == "private")
async def got_time(message: Message, state: FSMContext):
    try:
        time_str = datetime.strptime(message.text.strip(), "%H:%M").strftime("%H:%M")
    except ValueError:
        await message.answer("Не понял время. Формат ЧЧ:ММ, например 07:00")
        return
    await state.update_data(time=time_str)
    await state.set_state(AddSchedule.text)
    await message.answer("Теперь пришли текст сообщения, которое бот будет отправлять.")


@dp.message(AddSchedule.text, F.text, F.chat.type == "private")
async def got_text(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["chat_id"]

    if not await user_is_admin(chat_id, message.from_user.id):
        await state.clear()
        await message.answer("Нет прав в этой группе.")
        return

    items = load_json(SCHEDULE_FILE, [])
    new_id = max((i["id"] for i in items), default=0) + 1
    items.append({
        "id": new_id,
        "chat_id": chat_id,
        "time": data["time"],
        "text": message.text,
        "last_sent": None,
    })
    save_json(SCHEDULE_FILE, items)
    await state.clear()

    text, markup = schedule_view(chat_id)
    await message.answer(f"✅ Добавлено на {data['time']}.\n\n{text}", reply_markup=markup)


# ---------- модерация: мут / кик / бан ----------

USERS_FILE = Path("users.json")
# {chat_id: {user_id: {"username": ..., "name": ...}}}: кого бот видел в группе
seen_users: dict = load_json(USERS_FILE, {})

DURATION_RE = re.compile(r"^(\d{1,4})([mhdw])$")
UNITS = {
    "m": ("minutes", "мин"),
    "h": ("hours", "ч"),
    "d": ("days", "дн"),
    "w": ("weeks", "нед"),
}

# все права True = Telegram снимает ограничения с пользователя
FULL_PERMISSIONS = ChatPermissions(
    can_send_messages=True, can_send_audios=True, can_send_documents=True,
    can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
    can_send_voice_notes=True, can_send_polls=True, can_send_other_messages=True,
    can_add_web_page_previews=True, can_change_info=True, can_invite_users=True,
    can_pin_messages=True, can_manage_topics=True,
)


class Target:
    """Цель наказания: ID и красивое упоминание для ответа."""
    def __init__(self, user_id: int, mention: str):
        self.id = user_id
        self.mention = mention


def user_link(user_id: int, name: str) -> str:
    return f'<a href="tg://user?id={user_id}">{html.escape(name)}</a>'


# ---------- память о пользователях ----------

def remember_user(chat_id: int, user) -> None:
    chat_users = seen_users.setdefault(str(chat_id), {})
    username = user.username.lower() if user.username else None
    entry = {"username": username, "name": user.full_name}
    if chat_users.get(str(user.id)) == entry:
        return  # ничего не изменилось, файл не трогаем

    # если юзернейм раньше был у другого человека, у него он уже не актуален
    if username:
        for other_id, other in chat_users.items():
            if other_id != str(user.id) and other.get("username") == username:
                other["username"] = None

    chat_users[str(user.id)] = entry
    save_json(USERS_FILE, seen_users)


@dp.message.outer_middleware()
async def track_users(handler, event: Message, data):
    # срабатывает на каждое сообщение, не мешая остальным обработчикам
    if event.chat.type in GROUP_TYPES:
        if event.from_user:
            remember_user(event.chat.id, event.from_user)
        for member in event.new_chat_members or []:
            remember_user(event.chat.id, member)
    return await handler(event, data)


def find_cached_user(chat_id: int, username: str):
    username = username.lower().lstrip("@")
    for uid, info in seen_users.get(str(chat_id), {}).items():
        if info.get("username") == username:
            return int(uid), info["name"]
    return None


# ---------- вспомогательные функции ----------

def parse_duration_and_reason(args: str | None):
    """'2h спам' -> (timedelta, 'на 2 ч', 'спам'); 'спам' -> (None, 'навсегда', 'спам')"""
    args = (args or "").strip()
    first, _, rest = args.partition(" ")
    m = DURATION_RE.match(first.lower())
    if m:
        n, unit = int(m.group(1)), m.group(2)
        delta = timedelta(**{UNITS[unit][0]: n})
        return delta, f"на {n} {UNITS[unit][1]}", rest.strip()
    return None, "навсегда", args


def reason_line(reason: str) -> str:
    return f"\nПричина: {html.escape(reason)}" if reason else ""


async def is_group_admin(message: Message) -> bool:
    # админ, который пишет анонимно от имени группы
    if message.sender_chat and message.sender_chat.id == message.chat.id:
        return True
    if not message.from_user:
        return False
    return await user_is_admin(message.chat.id, message.from_user.id)


async def find_target(message: Message, command: CommandObject):
    """Ищет цель: ответ на сообщение, @username или ID.
    Возвращает (Target, остаток_аргументов) или (None, None)."""
    args = (command.args or "").strip()

    # 1) команда в ответ на сообщение
    reply = message.reply_to_message
    if reply and not reply.forum_topic_created:
        if reply.sender_chat:
            await message.reply("Это сообщение отправлено от имени канала или группы, автора не определить.")
            return None, None
        if reply.from_user:
            u = reply.from_user
            return Target(u.id, user_link(u.id, u.full_name)), args

    # 2) @username или ID в тексте команды
    first, _, rest = args.partition(" ")
    rest = rest.strip()

    if first.startswith("@") and len(first) > 1:
        found = find_cached_user(message.chat.id, first)
        if found:
            return Target(found[0], user_link(found[0], found[1])), rest
        try:  # запасной вариант, иногда срабатывает
            chat = await bot.get_chat(first)
            if chat.type == "private":
                return Target(chat.id, user_link(chat.id, first)), rest
        except Exception:
            pass
        await message.reply(
            f"Не нашёл {html.escape(first)} среди тех, кого видел. "
            "Ответь на его сообщение или укажи ID числом.",
            parse_mode="HTML",
        )
        return None, None

    if first.isdigit():
        return Target(int(first), f"<code>{first}</code>"), rest

    await message.reply(
        "Ответь на сообщение человека или укажи его @username либо ID.\n"
        "Пример: /ban @username спам"
    )
    return None, None


async def prepare_target(message: Message, command: CommandObject):
    """Проверка прав + поиск цели + защита админов и бота."""
    if not await is_group_admin(message):
        return None, None  # от не-админов команды молча игнорируем

    target, args = await find_target(message, command)
    if not target:
        return None, None

    if target.id == (await bot.me()).id:
        await message.reply("Себя я наказывать не буду 🙂")
        return None, None
    if await user_is_admin(message.chat.id, target.id):
        await message.reply("Админов я не трогаю.")
        return None, None
    return target, args


async def fail(message: Message):
    await message.reply(
        "Не получилось. Проверь, что я админ группы и у меня есть право "
        "«Блокировка пользователей», а человек состоит в этой группе."
    )


# ---------- команды ----------

@dp.message(Command("mute"), F.chat.type.in_(GROUP_TYPES))
async def cmd_mute(message: Message, command: CommandObject):
    target, args = await prepare_target(message, command)
    if not target:
        return
    delta, label, reason = parse_duration_and_reason(args)
    if delta and delta > timedelta(days=366):
        await message.reply("Максимальный срок: 366 дней. Или не указывай срок, тогда навсегда.")
        return
    until = datetime.now(TZ) + delta if delta else None
    try:
        await bot.restrict_chat_member(
            message.chat.id, target.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until,
        )
    except Exception as e:
        logging.error("mute: %s", e)
        await fail(message)
        return
    await message.reply(
        f"🔇 {target.mention} в муте {label}.{reason_line(reason)}",
        parse_mode="HTML",
    )


@dp.message(Command("unmute"), F.chat.type.in_(GROUP_TYPES))
async def cmd_unmute(message: Message, command: CommandObject):
    target, _ = await prepare_target(message, command)
    if not target:
        return
    try:
        await bot.restrict_chat_member(message.chat.id, target.id, permissions=FULL_PERMISSIONS)
    except Exception as e:
        logging.error("unmute: %s", e)
        await fail(message)
        return
    await message.reply(f"🔊 {target.mention} снова может писать.", parse_mode="HTML")


@dp.message(Command("kick"), F.chat.type.in_(GROUP_TYPES))
async def cmd_kick(message: Message, command: CommandObject):
    target, args = await prepare_target(message, command)
    if not target:
        return
    try:
        # кик = бан и сразу разбан, человек вылетает, но может вернуться по ссылке
        await bot.ban_chat_member(message.chat.id, target.id)
        await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    except Exception as e:
        logging.error("kick: %s", e)
        await fail(message)
        return
    await message.reply(
        f"👢 {target.mention} исключён из чата.{reason_line(args)}",
        parse_mode="HTML",
    )


@dp.message(Command("ban"), F.chat.type.in_(GROUP_TYPES))
async def cmd_ban(message: Message, command: CommandObject):
    target, args = await prepare_target(message, command)
    if not target:
        return
    delta, label, reason = parse_duration_and_reason(args)
    if delta and delta > timedelta(days=366):
        await message.reply("Максимальный срок: 366 дней. Или не указывай срок, тогда навсегда.")
        return
    until = datetime.now(TZ) + delta if delta else None
    try:
        await bot.ban_chat_member(message.chat.id, target.id, until_date=until)
    except Exception as e:
        logging.error("ban: %s", e)
        await fail(message)
        return
    await message.reply(
        f"🚫 {target.mention} (ID: <code>{target.id}</code>) забанен {label}."
        f"{reason_line(reason)}",
        parse_mode="HTML",
    )


@dp.message(Command("unban"), F.chat.type.in_(GROUP_TYPES))
async def cmd_unban(message: Message, command: CommandObject):
    if not await is_group_admin(message):
        return
    target, _ = await find_target(message, command)
    if not target:
        return
    try:
        await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    except Exception as e:
        logging.error("unban: %s", e)
        await fail(message)
        return
    await message.reply(f"✅ {target.mention} разбанен.", parse_mode="HTML")


# ---------- предупреждения (warn) ----------

SETTINGS_FILE = Path("settings.json")
WARNS_FILE = Path("warns.json")

DEFAULT_SETTINGS = {
    "warn_limit": 3,              # сколько предупреждений до наказания
    "warn_action": "kick",        # kick / ban / mute
    "warn_limit_mute_min": 1440,  # срок мута при лимите (минуты)
    "warn_mute_min": 60,          # мут после каждого предупреждения (минуты), 0 = нет
    "warn_expire_min": 0,         # через сколько предупреждение сгорает (минуты), 0 = никогда
}

ACTIONS = {"kick": "👢 Кик", "ban": "🚫 Бан", "mute": "🔇 Мут"}

MINUTES_IN = {"m": 1, "h": 60, "d": 1440, "w": 10080}

# настройки-сроки: все меняются одним и тем же экраном выбора
DURATIONS = {
    "pm": {  # мут после каждого предупреждения
        "setting": "warn_mute_min",
        "title": "На сколько мутить после каждого предупреждения?",
        "presets": [0, 15, 60, 180, 720, 1440, 10080],
        "zero": "Без мута",
        "max": 366 * 1440,
        "back": "warn",
    },
    "lm": {  # мут, когда набран лимит
        "setting": "warn_limit_mute_min",
        "title": "На сколько мутить, когда набран лимит предупреждений?",
        "presets": [60, 360, 1440, 4320, 10080, 43200],
        "zero": None,
        "max": 366 * 1440,
        "back": "wa",
    },
    "ex": {  # сгорание предупреждений
        "setting": "warn_expire_min",
        "title": "Через сколько предупреждение сгорает?",
        "presets": [0, 10080, 20160, 43200, 86400, 129600],
        "zero": "Никогда",
        "max": 3650 * 1440,
        "back": "warn",
    },
}


class SetDuration(StatesGroup):
    value = State()


# ---------- хранилище настроек и предупреждений ----------

def get_settings(chat_id: int) -> dict:
    saved = dict(load_json(SETTINGS_FILE, {}).get(str(chat_id), {}))
    # старые значения наказания (до появления своего срока мута)
    if saved.get("warn_action") == "mute1d":
        saved["warn_action"], saved["warn_limit_mute_min"] = "mute", 1440
    elif saved.get("warn_action") == "mute7d":
        saved["warn_action"], saved["warn_limit_mute_min"] = "mute", 10080
    return {**DEFAULT_SETTINGS, **saved}


def set_setting(chat_id: int, key: str, value) -> None:
    data = load_json(SETTINGS_FILE, {})
    data.setdefault(str(chat_id), {})[key] = value
    save_json(SETTINGS_FILE, data)


def get_warn_stamps(chat_id: int, user_id: int) -> list:
    """Список времён (unix) действующих предупреждений. Сгоревшие отбрасываются."""
    raw = load_json(WARNS_FILE, {}).get(str(chat_id), {}).get(str(user_id), [])
    if isinstance(raw, int):  # старый формат: просто число, отсчёт сгорания пойдёт с этого момента
        raw = [int(time.time())] * raw
    expire = get_settings(chat_id)["warn_expire_min"]
    if expire > 0:
        border = time.time() - expire * 60
        raw = [t for t in raw if t > border]
    return raw


def save_warn_stamps(chat_id: int, user_id: int, stamps: list) -> None:
    data = load_json(WARNS_FILE, {})
    chat = data.setdefault(str(chat_id), {})
    if stamps:
        chat[str(user_id)] = stamps
    else:
        chat.pop(str(user_id), None)
    save_json(WARNS_FILE, data)


def get_warns(chat_id: int, user_id: int) -> int:
    return len(get_warn_stamps(chat_id, user_id))


# ---------- форматирование ----------

def fmt_minutes(minutes: int, zero: str = "нет") -> str:
    if minutes <= 0:
        return zero
    if minutes % 10080 == 0:
        return f"{minutes // 10080} нед"
    if minutes % 1440 == 0:
        return f"{minutes // 1440} дн"
    if minutes % 60 == 0:
        return f"{minutes // 60} ч"
    return f"{minutes} мин"


def fmt_left(minutes: int) -> str:
    days, rest = divmod(minutes, 1440)
    hours, mins = divmod(rest, 60)
    parts = []
    if days:
        parts.append(f"{days} дн")
    if hours:
        parts.append(f"{hours} ч")
    if mins and not days:
        parts.append(f"{mins} мин")
    return " ".join(parts) or "меньше минуты"


def action_label(s: dict) -> str:
    if s["warn_action"] == "mute":
        return f"🔇 Мут на {fmt_minutes(s['warn_limit_mute_min'])}"
    return ACTIONS[s["warn_action"]]


async def mute_user(chat_id: int, user_id: int, delta: timedelta) -> None:
    await bot.restrict_chat_member(
        chat_id, user_id,
        permissions=ChatPermissions(can_send_messages=False),
        until_date=datetime.now(TZ) + delta,
    )


# ---------- команды ----------

@dp.message(Command("warn"), F.chat.type.in_(GROUP_TYPES))
async def cmd_warn(message: Message, command: CommandObject):
    target, reason = await prepare_target(message, command)
    if not target:
        return

    chat_id = message.chat.id
    s = get_settings(chat_id)
    limit = s["warn_limit"]

    stamps = get_warn_stamps(chat_id, target.id) + [int(time.time())]
    save_warn_stamps(chat_id, target.id, stamps)
    count = len(stamps)

    head = f"⚠️ {target.mention} получил предупреждение {count}/{limit}.{reason_line(reason)}"

    # лимит достигнут: наказание
    if count >= limit:
        action = s["warn_action"]
        try:
            if action == "kick":
                await bot.ban_chat_member(chat_id, target.id)
                await bot.unban_chat_member(chat_id, target.id, only_if_banned=True)
            elif action == "ban":
                await bot.ban_chat_member(chat_id, target.id)
            else:
                await mute_user(chat_id, target.id, timedelta(minutes=s["warn_limit_mute_min"]))
        except Exception as e:
            logging.error("warn limit: %s", e)
            await fail(message)
            return
        save_warn_stamps(chat_id, target.id, [])
        await message.reply(
            f"{head}\nЛимит достигнут: {action_label(s)}.", parse_mode="HTML"
        )
        return

    # лимит не достигнут: короткий мут, если он включён
    mute_min = s["warn_mute_min"]
    if mute_min > 0:
        try:
            await mute_user(chat_id, target.id, timedelta(minutes=mute_min))
            head += f"\n🔇 Мут на {fmt_minutes(mute_min)}."
        except Exception as e:
            logging.error("warn mute: %s", e)
            head += "\n(Мут выдать не получилось, проверь мои права.)"
    if s["warn_expire_min"] > 0:
        head += f"\n⏳ Предупреждение сгорит через {fmt_minutes(s['warn_expire_min'])}."
    await message.reply(head, parse_mode="HTML")


@dp.message(Command("unwarn"), F.chat.type.in_(GROUP_TYPES))
async def cmd_unwarn(message: Message, command: CommandObject):
    target, _ = await prepare_target(message, command)
    if not target:
        return
    chat_id = message.chat.id
    stamps = get_warn_stamps(chat_id, target.id)
    if not stamps:
        await message.reply(f"У {target.mention} нет предупреждений.", parse_mode="HTML")
        return
    save_warn_stamps(chat_id, target.id, stamps[:-1])  # снимаем самое свежее
    limit = get_settings(chat_id)["warn_limit"]
    await message.reply(
        f"✅ С {target.mention} снято предупреждение ({len(stamps) - 1}/{limit}).\n"
        "Если нужно снять мут, используй /unmute.",
        parse_mode="HTML",
    )


@dp.message(Command("warns"), F.chat.type.in_(GROUP_TYPES))
async def cmd_warns(message: Message, command: CommandObject):
    if not await is_group_admin(message):
        return
    target, _ = await find_target(message, command)
    if not target:
        return
    chat_id = message.chat.id
    s = get_settings(chat_id)
    stamps = get_warn_stamps(chat_id, target.id)
    text = f"{target.mention}: предупреждений {len(stamps)}/{s['warn_limit']}"
    if stamps and s["warn_expire_min"] > 0:
        left = int((min(stamps) + s["warn_expire_min"] * 60 - time.time()) / 60)
        text += f"\nБлижайшее сгорит через {fmt_left(max(left, 0))}."
    await message.reply(text, parse_mode="HTML")


# ---------- панель: экран настроек ----------

async def safe_edit(call: CallbackQuery, text: str, markup=None) -> None:
    try:
        await call.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):  # то же самое содержимое: не ошибка
            raise


async def check_callback_admin(call: CallbackQuery, chat_id: int) -> bool:
    if await user_is_admin(chat_id, call.from_user.id):
        return True
    await call.answer("Нет прав в этой группе", show_alert=True)
    return False


def warn_settings_view(chat_id: int):
    s = get_settings(chat_id)
    text = (
        f"⚠️ Предупреждения. Группа: {get_title(chat_id)}\n\n"
        f"• Лимит предупреждений: {s['warn_limit']}\n"
        f"• Наказание при лимите: {action_label(s)}\n"
        f"• Мут после каждого предупреждения: {fmt_minutes(s['warn_mute_min'])}\n"
        f"• Предупреждения сгорают через: {fmt_minutes(s['warn_expire_min'], 'никогда')}"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="➖", callback_data=f"wl:{chat_id}:-1")
    kb.button(text=f"Лимит: {s['warn_limit']}", callback_data="noop")
    kb.button(text="➕", callback_data=f"wl:{chat_id}:1")
    kb.button(text=f"Наказание: {action_label(s)}", callback_data=f"wa:{chat_id}")
    kb.button(text=f"Мут после варна: {fmt_minutes(s['warn_mute_min'])}", callback_data=f"dur:pm:{chat_id}")
    kb.button(text=f"Сгорание: {fmt_minutes(s['warn_expire_min'], 'никогда')}", callback_data=f"dur:ex:{chat_id}")
    kb.button(text="⬅️ Назад", callback_data=f"chat:{chat_id}")
    kb.adjust(3, 1, 1, 1, 1)
    return text, kb.as_markup()


@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@dp.callback_query(F.data.startswith("warn:"))
async def cb_warn(call: CallbackQuery, state: FSMContext):
    await state.clear()
    chat_id = int(call.data.split(":")[1])
    if not await check_callback_admin(call, chat_id):
        return
    text, markup = warn_settings_view(chat_id)
    await safe_edit(call, text, markup)
    await call.answer()


# лимит предупреждений: ➖ / ➕
@dp.callback_query(F.data.startswith("wl:"))
async def cb_warn_limit(call: CallbackQuery):
    _, chat_id, step = call.data.split(":")
    chat_id = int(chat_id)
    if not await check_callback_admin(call, chat_id):
        return
    new_limit = max(2, min(10, get_settings(chat_id)["warn_limit"] + int(step)))
    set_setting(chat_id, "warn_limit", new_limit)
    text, markup = warn_settings_view(chat_id)
    await safe_edit(call, text, markup)
    await call.answer()


# наказание при лимите: кик / бан / мут
@dp.callback_query(F.data.startswith("wa:"))
async def cb_warn_action(call: CallbackQuery):
    parts = call.data.split(":")
    chat_id = int(parts[1])
    if not await check_callback_admin(call, chat_id):
        return
    if len(parts) == 3 and parts[2] in ACTIONS:
        set_setting(chat_id, "warn_action", parts[2])

    s = get_settings(chat_id)
    kb = InlineKeyboardBuilder()
    for key, label in ACTIONS.items():
        mark = "✅ " if key == s["warn_action"] else ""
        kb.button(text=f"{mark}{label}", callback_data=f"wa:{chat_id}:{key}")
    if s["warn_action"] == "mute":
        kb.button(
            text=f"⏱ Срок мута: {fmt_minutes(s['warn_limit_mute_min'])}",
            callback_data=f"dur:lm:{chat_id}",
        )
    kb.button(text="⬅️ Назад", callback_data=f"warn:{chat_id}")
    kb.adjust(1)
    await safe_edit(call, "Что делать, когда набран лимит предупреждений?", kb.as_markup())
    await call.answer()


# универсальный выбор срока: мут после варна, мут на лимите, сгорание
@dp.callback_query(F.data.startswith("dur:"))
async def cb_duration(call: CallbackQuery):
    parts = call.data.split(":")  # dur:<ключ>:<chat_id>[:<минуты>]
    cfg = DURATIONS.get(parts[1])
    if not cfg:
        await call.answer()
        return
    chat_id = int(parts[2])
    if not await check_callback_admin(call, chat_id):
        return
    if len(parts) == 4 and parts[3].isdigit() and int(parts[3]) in cfg["presets"]:
        set_setting(chat_id, cfg["setting"], int(parts[3]))

    current = get_settings(chat_id)[cfg["setting"]]
    zero_text = cfg["zero"] or "нет"
    kb = InlineKeyboardBuilder()
    for minutes in cfg["presets"]:
        label = fmt_minutes(minutes, zero_text)
        mark = "✅ " if minutes == current else ""
        kb.button(text=f"{mark}{label}", callback_data=f"dur:{parts[1]}:{chat_id}:{minutes}")
    kb.button(text="✏️ Свой срок", callback_data=f"durc:{parts[1]}:{chat_id}")
    kb.button(text="⬅️ Назад", callback_data=f"{cfg['back']}:{chat_id}")
    n = len(cfg["presets"])
    kb.adjust(*([2] * (n // 2) + [1] * (n % 2) + [1, 1]))
    await safe_edit(
        call,
        f"{cfg['title']}\nСейчас: {fmt_minutes(current, zero_text)}",
        kb.as_markup(),
    )
    await call.answer()


# свой срок: пошаговый ввод
@dp.callback_query(F.data.startswith("durc:"))
async def cb_duration_custom(call: CallbackQuery, state: FSMContext):
    _, key, chat_id = call.data.split(":")
    chat_id = int(chat_id)
    cfg = DURATIONS.get(key)
    if not cfg or not await check_callback_admin(call, chat_id):
        return
    await state.set_state(SetDuration.value)
    await state.update_data(chat_id=chat_id, key=key)
    zero_hint = "\n0 — выключить." if cfg["zero"] else ""
    await safe_edit(
        call,
        f"{cfg['title']}\n\nПришли свой срок: 30m, 2h, 1d или 1w "
        f"(минуты, часы, дни, недели).{zero_hint}\n\nОтмена: /cancel",
    )
    await call.answer()


@dp.message(SetDuration.value, F.text, F.chat.type == "private")
async def got_duration(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id, cfg = data["chat_id"], DURATIONS[data["key"]]
    if not await user_is_admin(chat_id, message.from_user.id):
        await state.clear()
        await message.answer("Нет прав в этой группе.")
        return

    raw = message.text.strip().lower()
    if raw == "0":
        if cfg["zero"] is None:
            await message.answer("Здесь ноль нельзя. Примеры: 30m, 2h, 1d, 1w")
            return
        minutes = 0
    else:
        m = DURATION_RE.match(raw)
        if not m:
            await message.answer("Не понял. Примеры: 30m, 2h, 1d, 1w")
            return
        minutes = int(m.group(1)) * MINUTES_IN[m.group(2)]
        if minutes < 1 or minutes > cfg["max"]:
            await message.answer(f"Срок должен быть не больше {fmt_minutes(cfg['max'])}.")
            return

    set_setting(chat_id, cfg["setting"], minutes)
    await state.clear()
    text, markup = warn_settings_view(chat_id)
    await message.answer(f"✅ Сохранено.\n\n{text}", reply_markup=markup)


# ---------- запретные слова ----------

WORDS_FILE = Path("words.json")
# {chat_id: {"1": [слова], "2": [...], "3": [...]}}
forbidden: dict = load_json(WORDS_FILE, {})

LIST_NUMBERS = (1, 2, 3)
MAX_WORD_LEN = 100
LIST_DUR_PRESETS = [60, 360, 1440, 10080, 43200, 0]  # минуты, 0 = навсегда

FW_ACTIONS = {
    "delete": "🗑 Только удалить",
    "warn": "⚠️ Варн",
    "mute": "🔇 Мут",
    "ban": "🚫 Бан",
}

# настройки каждого списка живут в settings.json рядом с остальными
for _n in LIST_NUMBERS:
    DEFAULT_SETTINGS[f"fw{_n}_action"] = "delete"
    DEFAULT_SETTINGS[f"fw{_n}_dur_min"] = 1440
    DEFAULT_SETTINGS[f"fw{_n}_whole"] = False


class AddWords(StatesGroup):
    text = State()


class DelWords(StatesGroup):
    text = State()


class SetListDuration(StatesGroup):
    value = State()


# ---------- хранилище слов ----------

def normalize(text: str) -> str:
    return " ".join(text.lower().replace("ё", "е").split())


def get_words(chat_id: int, n: int) -> list:
    return forbidden.get(str(chat_id), {}).get(str(n), [])


def set_words(chat_id: int, n: int, words: list) -> None:
    forbidden.setdefault(str(chat_id), {})[str(n)] = words
    save_json(WORDS_FILE, forbidden)


def parse_lines(raw: str) -> list:
    """Каждая строка = одно слово (или фраза). Пустые и повторы отбрасываются."""
    result = []
    for line in raw.splitlines():
        word = normalize(line)
        if word and word not in result:
            result.append(word)
    return result


def term_text(minutes: int) -> str:
    return "навсегда" if minutes == 0 else f"на {fmt_minutes(minutes)}"


def fw_label(s: dict, n: int) -> str:
    action = s[f"fw{n}_action"]
    label = FW_ACTIONS[action]
    if action in ("mute", "ban"):
        dur = s[f"fw{n}_dur_min"]
        label += " " + term_text(dur)
    return label


# ---------- проверка сообщений ----------

def find_forbidden(chat_id: int, text: str):
    """Возвращает номер списка, в котором нашлось запретное слово, либо None."""
    chat_lists = forbidden.get(str(chat_id), {})
    if not any(chat_lists.values()):
        return None
    s = get_settings(chat_id)
    norm = normalize(text)
    for n in LIST_NUMBERS:
        whole = s[f"fw{n}_whole"]
        for word in chat_lists.get(str(n), []):
            if whole:
                if re.search(rf"(?<!\w){re.escape(word)}(?!\w)", norm):
                    return n
            elif word in norm:
                return n
    return None


background_tasks: set = set()


async def delete_later(msg: Message, seconds: int) -> None:
    await asyncio.sleep(seconds)
    try:
        await msg.delete()
    except Exception:
        pass


async def notify(chat_id: int, text: str, delete_after: int | None = None) -> None:
    try:
        msg = await bot.send_message(chat_id, text, parse_mode="HTML")
    except Exception as e:
        logging.error("notify: %s", e)
        return
    if delete_after:
        task = asyncio.create_task(delete_later(msg, delete_after))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)


async def apply_warn(chat_id: int, user_id: int, mention: str, reason: str) -> str:
    """Выдаёт предупреждение по настройкам группы и возвращает текст для чата."""
    s = get_settings(chat_id)
    limit = s["warn_limit"]
    stamps = get_warn_stamps(chat_id, user_id) + [int(time.time())]
    save_warn_stamps(chat_id, user_id, stamps)
    count = len(stamps)
    head = f"⚠️ {mention} получил предупреждение {count}/{limit}.{reason_line(reason)}"

    if count >= limit:
        action = s["warn_action"]
        try:
            if action == "kick":
                await bot.ban_chat_member(chat_id, user_id)
                await bot.unban_chat_member(chat_id, user_id, only_if_banned=True)
            elif action == "ban":
                await bot.ban_chat_member(chat_id, user_id)
            else:
                await mute_user(chat_id, user_id, timedelta(minutes=s["warn_limit_mute_min"]))
        except Exception as e:
            logging.error("apply_warn limit: %s", e)
            return head + "\n(Наказание за лимит применить не получилось, проверь мои права.)"
        save_warn_stamps(chat_id, user_id, [])
        return f"{head}\nЛимит достигнут: {action_label(s)}."

    mute_min = s["warn_mute_min"]
    if mute_min > 0:
        try:
            await mute_user(chat_id, user_id, timedelta(minutes=mute_min))
            head += f"\n🔇 Мут на {fmt_minutes(mute_min)}."
        except Exception as e:
            logging.error("apply_warn mute: %s", e)
    if s["warn_expire_min"] > 0:
        head += f"\n⏳ Предупреждение сгорит через {fmt_minutes(s['warn_expire_min'])}."
    return head


def censor_token(token: str) -> str:
    """Оставляет первую и последнюю букву, середину заменяет на *. Короткие скрывает целиком."""
    if len(token) <= 2:
        return "*" * len(token)
    return token[0] + "*" * (len(token) - 2) + token[-1]


def censor_match(match: re.Match) -> str:
    # у фраз из нескольких слов каждое слово цензурится отдельно, пробелы сохраняются
    return re.sub(r"\S+", lambda m: censor_token(m.group(0)), match.group(0))


def word_regex(word: str, whole: bool) -> str:
    """Регулярка для поиска слова в исходном тексте (регистр, ё и повторные пробелы не важны)."""
    parts = []
    for ch in word:
        if ch == " ":
            parts.append(r"\s+")
        elif ch == "е":
            parts.append("[её]")
        else:
            parts.append(re.escape(ch))
    body = "".join(parts)
    return rf"(?<!\w){body}(?!\w)" if whole else body


def censor_text(chat_id: int, text: str) -> str:
    s = get_settings(chat_id)
    entries = []
    for n in LIST_NUMBERS:
        whole = s[f"fw{n}_whole"]
        for word in get_words(chat_id, n):
            entries.append((word, whole))
    entries.sort(key=lambda e: len(e[0]), reverse=True)  # сначала длинные слова
    for word, whole in entries:
        text = re.sub(word_regex(word, whole), censor_match, text, flags=re.IGNORECASE)
    return text


def censored_quote(chat_id: int, text: str) -> str:
    censored = censor_text(chat_id, text)
    if len(censored) > 3000:
        censored = censored[:3000] + "…"
    # <pre>: внутри не оживают ссылки и @упоминания
    return f"\n\nБыло написано:\n<pre>{html.escape(censored)}</pre>"


async def handle_forbidden(message: Message) -> bool:
    """True, если сообщение нарушило правила и обработано (дальше его пускать не надо)."""
    chat_id = message.chat.id
    user = message.from_user
    if not user or user.is_bot:
        return False
    text = message.text or message.caption
    if not text:
        return False

    # скрытые ссылки в тексте тоже проверяем
    entities = message.entities or message.caption_entities or []
    extra = [e.url for e in entities if e.type == "text_link" and e.url]
    n = find_forbidden(chat_id, "\n".join([text] + extra))
    if n is None:
        return False
    if await is_group_admin(message):
        return False

    s = get_settings(chat_id)
    action = s[f"fw{n}_action"]
    dur = s[f"fw{n}_dur_min"]
    until = datetime.now(TZ) + timedelta(minutes=dur) if dur else None
    mention = user_link(user.id, user.full_name)

    deleted = False
    try:
        await message.delete()
        deleted = True
    except Exception as e:
        logging.error("не удалось удалить сообщение: %s", e)
    quote = censored_quote(chat_id, text) if deleted else ""

    if action == "delete":
        await notify(
            chat_id,
            f"🗑 Сообщение {mention} удалено: запретное слово.{quote}",
            delete_after=60,
        )
        return True

    if action == "warn":
        warn_text = await apply_warn(chat_id, user.id, mention, "запретное слово")
        await notify(chat_id, warn_text + quote)
        return True

    try:
        if action == "mute":
            await bot.restrict_chat_member(
                chat_id, user.id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=until,
            )
            text_out = f"🔇 {mention} в муте {term_text(dur)}."
        else:
            await bot.ban_chat_member(chat_id, user.id, until_date=until)
            text_out = f"🚫 {mention} забанен {term_text(dur)}."
    except Exception as e:
        logging.error("forbidden punish: %s", e)
        await notify(
            chat_id,
            "Не получилось наказать за запретное слово. Проверь мои права: "
            "удаление сообщений и блокировка пользователей.",
        )
        return True
    await notify(chat_id, f"{text_out}\nПричина: запретное слово.{quote}")
    return True

async def forbidden_words_guard(handler, event: Message, data):
    # срабатывает до остальных обработчиков, нарушившее сообщение дальше не идёт
    if event.chat.type in GROUP_TYPES and await handle_forbidden(event):
        return None
    return await handler(event, data)


dp.message.outer_middleware(forbidden_words_guard)
dp.edited_message.outer_middleware(forbidden_words_guard)


# ---------- панель: экраны ----------

def fw_overview(chat_id: int):
    s = get_settings(chat_id)
    lines = [f"🚫 Запретные слова. Группа: {get_title(chat_id)}", ""]
    kb = InlineKeyboardBuilder()
    for n in LIST_NUMBERS:
        count = len(get_words(chat_id, n))
        label = fw_label(s, n)
        lines.append(f"Список {n}: слов {count}, {label}")
        kb.button(text=f"Список {n} ({count}) — {label}", callback_data=f"fl:{chat_id}:{n}")
    lines += ["", "Если слово есть в нескольких списках, сработает список с меньшим номером."]
    kb.button(text="⬅️ Назад", callback_data=f"chat:{chat_id}")
    kb.adjust(1)
    return "\n".join(lines), kb.as_markup()


def fw_list_view(chat_id: int, n: int):
    s = get_settings(chat_id)
    mode = "целое слово" if s[f"fw{n}_whole"] else "по вхождению"
    text = (
        f"🚫 Список {n}. Группа: {get_title(chat_id)}\n\n"
        f"• Слов: {len(get_words(chat_id, n))}\n"
        f"• Наказание: {fw_label(s, n)}\n"
        f"• Поиск: {mode}\n\n"
        "«По вхождению» ловит слово и внутри других слов, «целое слово» только "
        "отдельное слово. Сообщение с запретным словом удаляется всегда."
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить слова", callback_data=f"fa:{chat_id}:{n}")
    kb.button(text="➖ Удалить слова", callback_data=f"fr:{chat_id}:{n}")
    kb.button(text="📋 Показать слова", callback_data=f"fs:{chat_id}:{n}")
    kb.button(text="⚖️ Наказание", callback_data=f"fp:{chat_id}:{n}")
    kb.button(text=f"🔎 Поиск: {mode}", callback_data=f"fm:{chat_id}:{n}")
    kb.button(text="⬅️ Назад", callback_data=f"fw:{chat_id}")
    kb.adjust(2, 1, 1, 1, 1)
    return text, kb.as_markup()


async def fw_ref(call: CallbackQuery):
    """Разбирает callback вида xx:<chat_id>:<номер списка>[:...] и проверяет права."""
    parts = call.data.split(":")
    try:
        chat_id, n = int(parts[1]), int(parts[2])
    except (IndexError, ValueError):
        await call.answer()
        return None
    if n not in LIST_NUMBERS or not await check_callback_admin(call, chat_id):
        return None
    return chat_id, n, parts


def back_button(callback_data: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data=callback_data)
    return kb.as_markup()


# ---------- панель: навигация ----------

@dp.callback_query(F.data.startswith("fw:"))
async def cb_fw(call: CallbackQuery, state: FSMContext):
    await state.clear()
    chat_id = int(call.data.split(":")[1])
    if not await check_callback_admin(call, chat_id):
        return
    text, markup = fw_overview(chat_id)
    await safe_edit(call, text, markup)
    await call.answer()


@dp.callback_query(F.data.startswith("fl:"))
async def cb_fl(call: CallbackQuery, state: FSMContext):
    await state.clear()
    ref = await fw_ref(call)
    if not ref:
        return
    chat_id, n, _ = ref
    text, markup = fw_list_view(chat_id, n)
    await safe_edit(call, text, markup)
    await call.answer()


@dp.callback_query(F.data.startswith("fm:"))
async def cb_fm(call: CallbackQuery):
    ref = await fw_ref(call)
    if not ref:
        return
    chat_id, n, _ = ref
    key = f"fw{n}_whole"
    set_setting(chat_id, key, not get_settings(chat_id)[key])
    text, markup = fw_list_view(chat_id, n)
    await safe_edit(call, text, markup)
    await call.answer()


@dp.callback_query(F.data.startswith("fs:"))
async def cb_fs(call: CallbackQuery):
    ref = await fw_ref(call)
    if not ref:
        return
    chat_id, n, _ = ref
    words = get_words(chat_id, n)
    if not words:
        await call.answer("Список пуст", show_alert=True)
        return

    chunks, chunk = [], ""
    for word in words:
        if len(chunk) + len(word) + 1 > 3500:
            chunks.append(chunk)
            chunk = ""
        chunk += word + "\n"
    chunks.append(chunk)

    for i, part in enumerate(chunks):
        header = f"Список {n} (слов: {len(words)}):\n\n" if i == 0 else ""
        await call.message.answer(header + part)
    await call.answer()


# ---------- панель: наказание списка ----------

@dp.callback_query(F.data.startswith("fp:"))
async def cb_fp(call: CallbackQuery):
    ref = await fw_ref(call)
    if not ref:
        return
    chat_id, n, parts = ref
    if len(parts) == 4 and parts[3] in FW_ACTIONS:
        set_setting(chat_id, f"fw{n}_action", parts[3])

    s = get_settings(chat_id)
    kb = InlineKeyboardBuilder()
    for key, label in FW_ACTIONS.items():
        mark = "✅ " if key == s[f"fw{n}_action"] else ""
        kb.button(text=f"{mark}{label}", callback_data=f"fp:{chat_id}:{n}:{key}")
    if s[f"fw{n}_action"] in ("mute", "ban"):
        dur = s[f"fw{n}_dur_min"]
        kb.button(text=f"⏱ Срок: {term_text(dur)}", callback_data=f"fd:{chat_id}:{n}")
    kb.button(text="⬅️ Назад", callback_data=f"fl:{chat_id}:{n}")
    kb.adjust(1)
    await safe_edit(
        call,
        f"Что делать с тем, кто написал слово из списка {n}?\n"
        "Сообщение удаляется в любом случае.\n\n"
        "«Варн» работает по настройкам из раздела «Предупреждения» "
        "(лимит, мут, сгорание).",
        kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("fd:"))
async def cb_fd(call: CallbackQuery):
    ref = await fw_ref(call)
    if not ref:
        return
    chat_id, n, parts = ref
    key = f"fw{n}_dur_min"
    if len(parts) == 4 and parts[3].isdigit() and int(parts[3]) in LIST_DUR_PRESETS:
        set_setting(chat_id, key, int(parts[3]))

    current = get_settings(chat_id)[key]
    kb = InlineKeyboardBuilder()
    for minutes in LIST_DUR_PRESETS:
        mark = "✅ " if minutes == current else ""
        kb.button(
            text=f"{mark}{fmt_minutes(minutes, 'Навсегда')}",
            callback_data=f"fd:{chat_id}:{n}:{minutes}",
        )
    kb.button(text="✏️ Свой срок", callback_data=f"fdc:{chat_id}:{n}")
    kb.button(text="⬅️ Назад", callback_data=f"fp:{chat_id}:{n}")
    kb.adjust(2, 2, 2, 1, 1)
    await safe_edit(
        call,
        f"На сколько наказывать за слово из списка {n}?\nСейчас: {term_text(current)}",
        kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("fdc:"))
async def cb_fdc(call: CallbackQuery, state: FSMContext):
    ref = await fw_ref(call)
    if not ref:
        return
    chat_id, n, _ = ref
    await state.set_state(SetListDuration.value)
    await state.update_data(chat_id=chat_id, n=n)
    await safe_edit(
        call,
        "Пришли срок наказания: 30m, 2h, 1d или 1w (минуты, часы, дни, недели).\n"
        "0 — навсегда.\n\nОтмена: /cancel",
    )
    await call.answer()


@dp.message(SetListDuration.value, F.text, F.chat.type == "private")
async def got_list_duration(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id, n = data["chat_id"], data["n"]
    if not await user_is_admin(chat_id, message.from_user.id):
        await state.clear()
        await message.answer("Нет прав в этой группе.")
        return

    raw = message.text.strip().lower()
    if raw == "0":
        minutes = 0
    else:
        m = DURATION_RE.match(raw)
        if not m:
            await message.answer("Не понял. Примеры: 30m, 2h, 1d, 1w или 0")
            return
        minutes = int(m.group(1)) * MINUTES_IN[m.group(2)]
        if minutes < 1 or minutes > 366 * 1440:
            await message.answer("Срок должен быть от 1 минуты до 366 дней.")
            return

    set_setting(chat_id, f"fw{n}_dur_min", minutes)
    await state.clear()
    await message.answer("✅ Сохранено.", reply_markup=back_button(f"fp:{chat_id}:{n}"))


# ---------- панель: добавление и удаление слов ----------

@dp.callback_query(F.data.startswith("fa:"))
async def cb_fa(call: CallbackQuery, state: FSMContext):
    ref = await fw_ref(call)
    if not ref:
        return
    chat_id, n, _ = ref
    await state.set_state(AddWords.text)
    await state.update_data(chat_id=chat_id, n=n)
    await safe_edit(
        call,
        f"➕ Список {n}. Пришли слова, каждое с новой строки. Можно сразу много.\n"
        "Словом считается любой набор символов, хоть фраза или ссылка.\n"
        "Можно присылать несколько сообщений подряд.\n\n"
        "Когда закончишь, нажми «Назад».",
        back_button(f"fl:{chat_id}:{n}"),
    )
    await call.answer()


@dp.message(AddWords.text, F.text, F.chat.type == "private")
async def got_add_words(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id, n = data["chat_id"], data["n"]
    if not await user_is_admin(chat_id, message.from_user.id):
        await state.clear()
        await message.answer("Нет прав в этой группе.")
        return

    words = list(get_words(chat_id, n))
    added = existed = too_long = 0
    for word in parse_lines(message.text):
        if len(word) > MAX_WORD_LEN:
            too_long += 1
        elif word in words:
            existed += 1
        else:
            words.append(word)
            added += 1
    set_words(chat_id, n, words)

    report = f"✅ Добавлено: {added}"
    if existed:
        report += f"\nУже были в списке: {existed}"
    if too_long:
        report += f"\nСлишком длинные (больше {MAX_WORD_LEN} символов), пропущены: {too_long}"
    report += f"\nВсего в списке {n}: {len(words)}\n\nМожешь прислать ещё или нажать «Готово»."

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Готово", callback_data=f"fl:{chat_id}:{n}")
    await message.answer(report, reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("fr:"))
async def cb_fr(call: CallbackQuery, state: FSMContext):
    ref = await fw_ref(call)
    if not ref:
        return
    chat_id, n, _ = ref
    await state.set_state(DelWords.text)
    await state.update_data(chat_id=chat_id, n=n)
    await safe_edit(
        call,
        f"➖ Список {n}. Пришли слова, которые нужно убрать, каждое с новой строки.\n"
        "Можно присылать несколько сообщений подряд.\n\n"
        "Когда закончишь, нажми «Назад».",
        back_button(f"fl:{chat_id}:{n}"),
    )
    await call.answer()


@dp.message(DelWords.text, F.text, F.chat.type == "private")
async def got_del_words(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id, n = data["chat_id"], data["n"]
    if not await user_is_admin(chat_id, message.from_user.id):
        await state.clear()
        await message.answer("Нет прав в этой группе.")
        return

    words = list(get_words(chat_id, n))
    removed = missing = 0
    for word in parse_lines(message.text):
        if word in words:
            words.remove(word)
            removed += 1
        else:
            missing += 1
    set_words(chat_id, n, words)

    report = f"✅ Удалено: {removed}"
    if missing:
        report += f"\nНе нашёл в списке: {missing}"
    report += f"\nОсталось в списке {n}: {len(words)}\n\nМожешь прислать ещё или нажать «Готово»."

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Готово", callback_data=f"fl:{chat_id}:{n}")
    await message.answer(report, reply_markup=kb.as_markup())


# ---------- сообщения от имени бота: /message, /pin, /editpin ----------

PINS_FILE = Path("pins.json")
# {chat_id: message_id}: последнее сообщение бота, закреплённое командой
last_bot_pin: dict = load_json(PINS_FILE, {})

# виды оформления Telegram, которые считаем форматированием
FORMAT_TYPES = {
    "bold", "italic", "underline", "strikethrough", "spoiler", "code", "pre",
    "text_link", "text_mention", "blockquote", "expandable_blockquote", "custom_emoji",
}

SEND_FAIL = (
    "Не получилось. Проверь, что текст не длиннее 4096 символов и у меня есть "
    "права на отправку и закрепление сообщений."
)


def extract_payload(message: Message, command: CommandObject):
    """Достаёт текст после команды.
    Возвращает (текст, entities). entities заполнены, если админ использовал
    форматирование Telegram. Иначе None, и текст считается HTML."""
    args = command.args
    if not args:
        return None, None
    source = message.text or message.caption or ""
    start = len(source) - len(args)  # где в исходном тексте начинается текст после команды

    shifted = []
    for e in message.entities or message.caption_entities or []:
        end = e.offset + e.length
        if end <= start:
            continue  # относится к самой команде
        begin = max(e.offset, start)
        shifted.append(e.model_copy(update={"offset": begin - start, "length": end - begin}))

    if any(e.type in FORMAT_TYPES for e in shifted):
        return args, shifted
    return args, None


async def send_payload(chat_id: int, text: str, entities, thread_id=None, reply_to=None):
    """Отправляет текст. Возвращает (сообщение, html_не_распознан)."""
    kwargs = {"message_thread_id": thread_id}
    if reply_to:
        kwargs["reply_parameters"] = ReplyParameters(
            message_id=reply_to, allow_sending_without_reply=True
        )
    if entities:
        return await bot.send_message(chat_id, text, entities=entities, **kwargs), False
    try:
        return await bot.send_message(chat_id, text, parse_mode="HTML", **kwargs), False
    except TelegramBadRequest as e:
        if "parse entities" not in str(e):
            raise
    return await bot.send_message(chat_id, text, parse_mode=None, **kwargs), True


async def edit_payload(chat_id: int, message_id: int, text: str, entities) -> bool:
    """Меняет текст сообщения. Возвращает True, если HTML не распознан."""
    if entities:
        await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, entities=entities)
        return False
    try:
        await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, parse_mode="HTML")
        return False
    except TelegramBadRequest as e:
        if "parse entities" not in str(e):
            raise
    await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, parse_mode=None)
    return True


def remember_pin(chat_id: int, message_id: int) -> None:
    last_bot_pin[str(chat_id)] = message_id
    save_json(PINS_FILE, last_bot_pin)


async def try_delete(message: Message) -> None:
    try:
        await message.delete()
    except Exception:
        pass


def thread_of(message: Message):
    return message.message_thread_id if message.is_topic_message else None


def reply_target(message: Message):
    reply = message.reply_to_message
    if reply and not reply.forum_topic_created:
        return reply
    return None


# ---------- команды ----------

@dp.message(Command("message"), F.chat.type.in_(GROUP_TYPES))
async def cmd_message(message: Message, command: CommandObject):
    if not await is_group_admin(message):
        return
    text, entities = extract_payload(message, command)
    if not text:
        await message.reply(
            "Напиши /message и текст. Оформление Telegram сохранится, "
            "либо можно писать HTML-теги: <b>жирный</b>, <i>курсив</i>, "
            "<a href=\"https://...\">ссылка</a>.\n"
            "Если написать команду в ответ на сообщение, бот ответит именно на него."
        )
        return

    reply = reply_target(message)
    try:
        _, html_failed = await send_payload(
            message.chat.id, text, entities,
            thread_id=thread_of(message),
            reply_to=reply.message_id if reply else None,
        )
    except Exception as e:
        logging.error("message: %s", e)
        await message.reply(SEND_FAIL)
        return

    await try_delete(message)
    if html_failed:
        await notify(message.chat.id, "ℹ️ HTML не распознан, отправил как обычный текст.", delete_after=15)


@dp.message(Command("pin"), F.chat.type.in_(GROUP_TYPES))
async def cmd_pin(message: Message, command: CommandObject):
    if not await is_group_admin(message):
        return
    chat_id = message.chat.id
    text, entities = extract_payload(message, command)
    reply = reply_target(message)
    html_failed = False

    if text:
        # новое сообщение от бота и сразу закрепить
        try:
            sent, html_failed = await send_payload(
                chat_id, text, entities,
                thread_id=thread_of(message),
                reply_to=reply.message_id if reply else None,
            )
        except Exception as e:
            logging.error("pin send: %s", e)
            await message.reply(SEND_FAIL)
            return
        try:
            await bot.pin_chat_message(chat_id, sent.message_id)
        except Exception as e:
            logging.error("pin: %s", e)
            await message.reply(
                "Сообщение отправил, но закрепить не получилось. "
                "Проверь, что у меня есть право «Закрепление сообщений»."
            )
            return
        remember_pin(chat_id, sent.message_id)

    elif reply:
        # закрепить сообщение, на которое ответили
        try:
            await bot.pin_chat_message(chat_id, reply.message_id)
        except Exception as e:
            logging.error("pin reply: %s", e)
            await message.reply(
                "Не получилось закрепить. Проверь, что у меня есть право "
                "«Закрепление сообщений»."
            )
            return
        if reply.from_user and reply.from_user.id == (await bot.me()).id:
            remember_pin(chat_id, reply.message_id)  # своё сообщение можно потом править через /editpin

    else:
        await message.reply(
            "Ответь командой /pin на сообщение, чтобы закрепить его, "
            "или напиши /pin и текст: я отправлю его и сразу закреплю."
        )
        return

    await try_delete(message)
    if html_failed:
        await notify(chat_id, "ℹ️ HTML не распознан, отправил как обычный текст.", delete_after=15)


@dp.message(Command("editpin"), F.chat.type.in_(GROUP_TYPES))
async def cmd_editpin(message: Message, command: CommandObject):
    if not await is_group_admin(message):
        return
    chat_id = message.chat.id
    text, entities = extract_payload(message, command)
    if not text:
        await message.reply("Напиши /editpin и новый текст для последнего закреплённого мной сообщения.")
        return

    pin_id = last_bot_pin.get(str(chat_id))
    if not pin_id:
        await message.reply("Я пока не закреплял сообщений в этом чате. Сначала используй /pin текст.")
        return

    try:
        html_failed = await edit_payload(chat_id, pin_id, text, entities)
    except TelegramBadRequest as e:
        if "not modified" in str(e):
            await message.reply("Текст не изменился.")
        else:
            logging.error("editpin: %s", e)
            await message.reply(
                "Не получилось изменить. Возможно, сообщение уже удалено "
                "или текст длиннее 4096 символов."
            )
        return

    await try_delete(message)
    if html_failed:
        await notify(chat_id, "ℹ️ HTML не распознан, отправил как обычный текст.", delete_after=15)


# ---------- заявки на вступление ----------

MAX_FILE_BYTES = 10 * 1024 * 1024  # больше 10 МБ не принимаем: бесплатная база небольшая

DEFAULT_GREETING = (
    "Привет, {name}! 👋\n\n"
    "Я - админ-бот группы «{chat}». Я помогаю управлять группой.\n\n"
    "Так как ты подал(а) заявку в «{chat}». Чтобы мы быстрее её рассмотрели, "
    "отправь мне в ответ на это сообщение свой оффер в виде картинки "
    "png/jpg или файлом pdf."
    "Это необходимо что бы убедиться что ты дейтсвительно принадлежишь "
    "этому потоку. Я передам это все живому админу и после проверки "
    "вы будете принято в чат. Спасибо за понимание!"
)
DEFAULT_SETTINGS["join_enabled"] = False
DEFAULT_SETTINGS["join_greeting"] = DEFAULT_GREETING

STATUS_LABELS = {
    "pending": "⏳ ждёт решения",
    "approved": "✅ принята",
    "declined": "❌ отклонена",
    "closed": "🔒 закрыта",
}


class EditGreeting(StatesGroup):
    text = State()


# ---------- база данных (в отдельных потоках, чтобы не тормозить бота) ----------

APP_COLUMNS = (
    "a.id, a.chat_id, a.user_id, a.username, a.full_name, a.status, a.requested_at, "
    "(SELECT count(*) FROM application_items i WHERE i.application_id = a.id)"
)


def db_init_apps() -> None:
    with _db_connect() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS join_applications ("
            "id BIGSERIAL PRIMARY KEY, chat_id BIGINT NOT NULL, user_id BIGINT NOT NULL, "
            "username TEXT, full_name TEXT, status TEXT NOT NULL DEFAULT 'pending', "
            "requested_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (chat_id, user_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS application_items ("
            "id BIGSERIAL PRIMARY KEY, "
            "application_id BIGINT NOT NULL REFERENCES join_applications(id) ON DELETE CASCADE, "
            "kind TEXT NOT NULL, text TEXT, file_name TEXT, mime_type TEXT, file_data BYTEA, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )


def db_save_request(chat_id: int, user_id: int, username, full_name) -> int:
    with _db_connect() as conn:
        row = conn.execute(
            "INSERT INTO join_applications (chat_id, user_id, username, full_name, status, requested_at) "
            "VALUES (%s, %s, %s, %s, 'pending', now()) "
            "ON CONFLICT (chat_id, user_id) DO UPDATE SET username = EXCLUDED.username, "
            "full_name = EXCLUDED.full_name, status = 'pending', requested_at = now() "
            "RETURNING id",
            (chat_id, user_id, username, full_name),
        ).fetchone()
    return row[0]


def db_find_pending(user_id: int):
    with _db_connect() as conn:
        row = conn.execute(
            "SELECT id FROM join_applications WHERE user_id = %s AND status = 'pending' "
            "ORDER BY requested_at DESC LIMIT 1",
            (user_id,),
        ).fetchone()
    return row[0] if row else None


def db_add_item(app_id: int, kind: str, text, file_name, mime_type, data) -> None:
    with _db_connect() as conn:
        conn.execute(
            "INSERT INTO application_items (application_id, kind, text, file_name, mime_type, file_data) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (app_id, kind, text, file_name, mime_type, data),
        )


def db_list_apps(chat_id: int, limit: int = 15) -> list:
    with _db_connect() as conn:
        return conn.execute(
            f"SELECT {APP_COLUMNS} FROM join_applications a "
            "WHERE a.chat_id = %s ORDER BY a.requested_at DESC LIMIT %s",
            (chat_id, limit),
        ).fetchall()


def db_get_app(app_id: int):
    with _db_connect() as conn:
        return conn.execute(
            f"SELECT {APP_COLUMNS} FROM join_applications a WHERE a.id = %s", (app_id,)
        ).fetchone()


def db_get_items(app_id: int) -> list:
    with _db_connect() as conn:
        return conn.execute(
            "SELECT kind, text, file_name, mime_type, file_data FROM application_items "
            "WHERE application_id = %s ORDER BY id",
            (app_id,),
        ).fetchall()


def db_set_status(app_id: int, status: str) -> None:
    with _db_connect() as conn:
        conn.execute("UPDATE join_applications SET status = %s WHERE id = %s", (status, app_id))


def db_count_pending(chat_id: int) -> int:
    with _db_connect() as conn:
        return conn.execute(
            "SELECT count(*) FROM join_applications WHERE chat_id = %s AND status = 'pending'",
            (chat_id,),
        ).fetchone()[0]


async def init_applications() -> None:
    if not DATABASE_URL:
        logging.warning("DATABASE_URL не задан: приём заявок отключён")
        return
    await asyncio.to_thread(db_init_apps)


# ---------- заявка: приветствие в личку ----------

def render_greeting(template: str, first_name: str, chat_title: str) -> str:
    return (
        template
        .replace("{name}", html.escape(first_name or ""))
        .replace("{chat}", html.escape(chat_title or ""))
    )


@dp.chat_join_request()
async def on_join_request(event: ChatJoinRequest):
    chat_id = event.chat.id
    s = get_settings(chat_id)
    if not s["join_enabled"] or not DATABASE_URL:
        return
    remember_chat(event.chat)

    user = event.from_user
    try:
        await asyncio.to_thread(db_save_request, chat_id, user.id, user.username, user.full_name)
    except Exception as e:
        logging.error("заявка: не удалось сохранить: %s", e)
        return

    text = render_greeting(s["join_greeting"], user.first_name, event.chat.title)
    try:
        # user_chat_id: личный чат с заявителем, писать можно без /start
        await send_payload(event.user_chat_id, text, None)
    except Exception as e:
        logging.error("заявка: не удалось написать заявителю %s: %s", user.id, e)


# ---------- панель: заявки ----------

def ids_from(call: CallbackQuery) -> list:
    return [int(x) for x in call.data.split(":")[1:]]


async def join_menu_view(chat_id: int):
    s = get_settings(chat_id)
    status = "включён ✅" if s["join_enabled"] else "выключен ⛔"
    pending = "—"
    if DATABASE_URL:
        try:
            pending = str(await asyncio.to_thread(db_count_pending, chat_id))
        except Exception as e:
            logging.error("заявки: %s", e)
            pending = "?"
    text = (
        f"📥 Заявки в чат. Группа: {get_title(chat_id)}\n\n"
        f"• Приём заявок: {status}\n"
        f"• Ждут решения: {pending}\n\n"
        "Бот пишет каждому, кто подал заявку, и сохраняет его ответы "
        "(фото, PDF, текст). В приветствии можно использовать {name} (имя заявителя) "
        "и {chat} (название группы)."
    )
    if not DATABASE_URL:
        text += "\n\n⚠️ База данных не подключена, приём заявок не работает."

    kb = InlineKeyboardBuilder()
    kb.button(
        text="🔕 Выключить приём" if s["join_enabled"] else "🔔 Включить приём",
        callback_data=f"jt:{chat_id}",
    )
    kb.button(text="✏️ Изменить приветствие", callback_data=f"jg:{chat_id}")
    kb.button(text="👁 Показать приветствие", callback_data=f"jv:{chat_id}")
    kb.button(text="📋 Заявки", callback_data=f"jl:{chat_id}")
    kb.button(text="⬅️ Назад", callback_data=f"chat:{chat_id}")
    kb.adjust(1)
    return text, kb.as_markup()


async def apps_list_view(chat_id: int):
    rows = await asyncio.to_thread(db_list_apps, chat_id, 15)
    kb = InlineKeyboardBuilder()
    for app_id, _, user_id, username, full_name, status, _, items in rows:
        icon = STATUS_LABELS.get(status, "❔").split()[0]
        label = f"{icon} {full_name or user_id}"
        if username:
            label += f" (@{username})"
        label += f" · 📎{items}"
        kb.button(text=label[:60], callback_data=f"jo:{chat_id}:{app_id}")
    kb.button(text="⬅️ Назад", callback_data=f"jn:{chat_id}")
    kb.adjust(1)
    text = "📋 Последние заявки (до 15):" if rows else "Заявок пока нет."
    return text, kb.as_markup()


async def app_view(chat_id: int, app_id: int):
    row = await asyncio.to_thread(db_get_app, app_id)
    if not row or row[1] != chat_id:
        return "Заявка не найдена.", back_button(f"jl:{chat_id}")
    _, _, user_id, username, full_name, status, requested_at, items = row
    when = requested_at.astimezone(TZ).strftime("%d.%m.%Y %H:%M")
    who = full_name or "—"
    if username:
        who += f" (@{username})"
    text = (
        f"Заявка №{app_id}\n\n"
        f"👤 {who}\nID: {user_id}\n"
        f"Статус: {STATUS_LABELS.get(status, status)}\n"
        f"Подана: {when}\n"
        f"Материалов: {items}"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="📎 Показать материалы", callback_data=f"jm:{chat_id}:{app_id}")
    kb.button(text="✅ Принять", callback_data=f"jy:{chat_id}:{app_id}")
    kb.button(text="❌ Отклонить", callback_data=f"jx:{chat_id}:{app_id}")
    kb.button(text="⬅️ Назад", callback_data=f"jl:{chat_id}")
    kb.adjust(1, 2, 1)
    return text, kb.as_markup()


@dp.callback_query(F.data.startswith("jn:"))
async def cb_jn(call: CallbackQuery, state: FSMContext):
    await state.clear()
    (chat_id,) = ids_from(call)
    if not await check_callback_admin(call, chat_id):
        return
    text, markup = await join_menu_view(chat_id)
    await safe_edit(call, text, markup)
    await call.answer()


@dp.callback_query(F.data.startswith("jt:"))
async def cb_jt(call: CallbackQuery):
    (chat_id,) = ids_from(call)
    if not await check_callback_admin(call, chat_id):
        return
    set_setting(chat_id, "join_enabled", not get_settings(chat_id)["join_enabled"])
    text, markup = await join_menu_view(chat_id)
    await safe_edit(call, text, markup)
    await call.answer()


@dp.callback_query(F.data.startswith("jv:"))
async def cb_jv(call: CallbackQuery):
    (chat_id,) = ids_from(call)
    if not await check_callback_admin(call, chat_id):
        return
    text = render_greeting(
        get_settings(chat_id)["join_greeting"], call.from_user.first_name, get_title(chat_id)
    )
    await send_payload(call.message.chat.id, text, None)
    await call.answer("Так приветствие увидит заявитель")


@dp.callback_query(F.data.startswith("jg:"))
async def cb_jg(call: CallbackQuery, state: FSMContext):
    (chat_id,) = ids_from(call)
    if not await check_callback_admin(call, chat_id):
        return
    await state.set_state(EditGreeting.text)
    await state.update_data(chat_id=chat_id)
    await safe_edit(
        call,
        "Пришли новый текст приветствия. Оформление Telegram сохранится, "
        "либо можно писать HTML-теги.\n"
        "{name} заменится на имя заявителя, {chat} на название группы.\n\n"
        "Отмена: кнопка «Назад» или /cancel",
        back_button(f"jn:{chat_id}"),
    )
    await call.answer()


@dp.message(EditGreeting.text, F.text, F.chat.type == "private")
async def got_greeting(message: Message, state: FSMContext):
    chat_id = (await state.get_data())["chat_id"]
    if not await user_is_admin(chat_id, message.from_user.id):
        await state.clear()
        await message.answer("Нет прав в этой группе.")
        return

    has_format = any(e.type in FORMAT_TYPES for e in (message.entities or []))
    greeting = message.html_text if has_format else message.text
    if len(greeting) > 3000:
        await message.answer("Слишком длинно, сократи до 3000 символов.")
        return

    preview = render_greeting(greeting, message.from_user.first_name, get_title(chat_id))
    try:
        _, html_failed = await send_payload(message.chat.id, preview, None)
    except Exception as e:
        logging.error("приветствие: %s", e)
        await message.answer("Не получилось отправить такой текст, возможно, он слишком длинный.")
        return
    if html_failed:
        await message.answer("HTML распознать не удалось. Проверь теги и пришли текст ещё раз.")
        return

    set_setting(chat_id, "join_greeting", greeting)
    await state.clear()
    await message.answer(
        "✅ Сохранено. Выше показано, как приветствие увидит заявитель.",
        reply_markup=back_button(f"jn:{chat_id}"),
    )


@dp.callback_query(F.data.startswith("jl:"))
async def cb_jl(call: CallbackQuery):
    (chat_id,) = ids_from(call)
    if not await check_callback_admin(call, chat_id):
        return
    if not DATABASE_URL:
        await call.answer("База данных не подключена", show_alert=True)
        return
    text, markup = await apps_list_view(chat_id)
    await safe_edit(call, text, markup)
    await call.answer()


@dp.callback_query(F.data.startswith("jo:"))
async def cb_jo(call: CallbackQuery):
    chat_id, app_id = ids_from(call)
    if not await check_callback_admin(call, chat_id):
        return
    text, markup = await app_view(chat_id, app_id)
    await safe_edit(call, text, markup)
    await call.answer()


@dp.callback_query(F.data.startswith("jm:"))
async def cb_jm(call: CallbackQuery):
    chat_id, app_id = ids_from(call)
    if not await check_callback_admin(call, chat_id):
        return
    row = await asyncio.to_thread(db_get_app, app_id)
    if not row or row[1] != chat_id:
        await call.answer("Заявка не найдена", show_alert=True)
        return
    items = await asyncio.to_thread(db_get_items, app_id)
    if not items:
        await call.answer("Заявитель пока ничего не прислал", show_alert=True)
        return

    await call.answer()
    await call.message.answer(f"📎 Материалы заявки №{app_id}: {row[4] or row[2]}")
    for kind, text, file_name, mime, data in items[:30]:
        try:
            if kind == "text":
                await call.message.answer(f"💬 {(text or '')[:4000]}")
            else:
                file = BufferedInputFile(bytes(data), filename=file_name or "file")
                caption = text[:1000] if text else None
                if kind == "photo":
                    await call.message.answer_photo(file, caption=caption)
                else:
                    await call.message.answer_document(file, caption=caption)
        except Exception as e:
            logging.error("заявка: не удалось показать материал: %s", e)
            await call.message.answer("(не удалось показать один из материалов)")
    if len(items) > 30:
        await call.message.answer(f"Показаны первые 30 из {len(items)}.")


async def decide_application(call: CallbackQuery, approve: bool):
    chat_id, app_id = ids_from(call)
    if not await check_callback_admin(call, chat_id):
        return
    row = await asyncio.to_thread(db_get_app, app_id)
    if not row or row[1] != chat_id:
        await call.answer("Заявка не найдена", show_alert=True)
        return

    user_id = row[2]
    new_status = "approved" if approve else "declined"
    try:
        if approve:
            await bot.approve_chat_join_request(chat_id, user_id)
        else:
            await bot.decline_chat_join_request(chat_id, user_id)
    except TelegramBadRequest as e:
        err = str(e)
        if "USER_ALREADY_PARTICIPANT" in err:
            new_status = "approved"  # уже в группе, например принят вручную
        elif "HIDE_REQUESTER_MISSING" in err:
            new_status = "closed"    # заявка уже обработана или отозвана
        else:
            logging.error("заявка: %s", e)
            await call.answer(
                "Не получилось. Проверь, что у меня есть право приглашать пользователей.",
                show_alert=True,
            )
            return

    await asyncio.to_thread(db_set_status, app_id, new_status)
    text, markup = await app_view(chat_id, app_id)
    await safe_edit(call, text, markup)
    await call.answer(
        {"approved": "Принята ✅", "declined": "Отклонена ❌", "closed": "Заявка уже закрыта"}[new_status]
    )


@dp.callback_query(F.data.startswith("jy:"))
async def cb_jy(call: CallbackQuery):
    await decide_application(call, approve=True)


@dp.callback_query(F.data.startswith("jx:"))
async def cb_jx(call: CallbackQuery):
    await decide_application(call, approve=False)


# ---------- ответы заявителей в личке (держать в конце, после всех личных обработчиков) ----------

@dp.message(F.chat.type == "private", F.photo | F.document | F.text)
async def applicant_message(message: Message):
    user = message.from_user
    if not DATABASE_URL or not user:
        return
    if message.text and message.text.startswith("/"):
        return
    app_id = await asyncio.to_thread(db_find_pending, user.id)
    if not app_id:
        return  # это не заявитель

    file_obj = file_name = mime = None
    if message.photo:
        file_obj = message.photo[-1]  # самый большой размер
        kind = "photo"
        file_name = f"photo_{file_obj.file_unique_id}.jpg"
        mime = "image/jpeg"
    elif message.document:
        doc = message.document
        mime = doc.mime_type or ""
        if not (mime == "application/pdf" or mime.startswith("image/")):
            await message.answer(
                "Файлы такого типа я не принимаю. Пришли фото или PDF, "
                "а текстом можно написать что угодно."
            )
            return
        kind = "document"
        file_obj = doc
        file_name = doc.file_name or f"file_{doc.file_unique_id}"
    else:
        kind = "text"

    too_big = f"Файл слишком большой, максимум {MAX_FILE_BYTES // (1024 * 1024)} МБ."
    data = None
    if file_obj:
        if file_obj.file_size and file_obj.file_size > MAX_FILE_BYTES:
            await message.answer(too_big)
            return
        try:
            data = (await bot.download(file_obj)).getvalue()
        except Exception as e:
            logging.error("заявка: не удалось скачать файл: %s", e)
            await message.answer("Не получилось получить файл, попробуй отправить ещё раз.")
            return
        if len(data) > MAX_FILE_BYTES:
            await message.answer(too_big)
            return

    text = message.text if kind == "text" else message.caption
    try:
        await asyncio.to_thread(db_add_item, app_id, kind, text, file_name, mime, data)
    except Exception as e:
        logging.error("заявка: не удалось сохранить материал: %s", e)
        await message.answer("Не получилось сохранить, попробуй ещё раз чуть позже.")
        return
    await message.answer("✅ Получил, передал администраторам.")


# ---------- группы: запоминаем, где есть бот ----------

@dp.my_chat_member(F.chat.type.in_(GROUP_TYPES))
async def on_bot_status(event: ChatMemberUpdated):
    status = event.new_chat_member.status
    if status in ("member", "administrator"):
        remember_chat(event.chat)
    elif status in ("left", "kicked"):
        forget_chat(event.chat.id)


@dp.message(F.chat.type.in_(GROUP_TYPES))
async def group_seen(message: Message):
    remember_chat(message.chat)
    if message.text and message.text.startswith("/panel"):
        await message.reply("Управление настройками теперь в личке со мной 🙂")


# ---------- планировщик ----------

async def scheduler():
    while True:
        now = datetime.now(TZ)
        current_time = now.strftime("%H:%M")
        today = now.strftime("%Y-%m-%d")

        items = load_json(SCHEDULE_FILE, [])
        changed = False
        for item in items:
            if item["time"] == current_time and item.get("last_sent") != today:
                try:
                    await bot.send_message(item["chat_id"], item["text"])
                except Exception as e:
                    logging.error("Не удалось отправить №%s: %s", item["id"], e)
                item["last_sent"] = today
                changed = True

        if changed:
            save_json(SCHEDULE_FILE, items)

        await asyncio.sleep(20)


# ---------- не даём Render усыпить сервис ----------

KEEPALIVE_URL = os.getenv("RENDER_EXTERNAL_URL")  # Render задаёт сам для веб-сервисов
WEB_PORT = os.getenv("PORT")                      # Render тоже (по умолчанию 10000)
KEEPALIVE_SECONDS = 300


async def start_web_server() -> None:
    """Мини-сервер: Render ждёт открытый порт, и на нём же отвечаем на пинги."""
    if not WEB_PORT:
        return  # локальный запуск: сервер не нужен

    async def ok(request):
        return web.Response(text="bot is running")

    app = web.Application()
    app.router.add_get("/", ok)
    app.router.add_get("/health", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        await web.TCPSite(runner, "0.0.0.0", int(WEB_PORT)).start()
        logging.info("Мини-сервер запущен на порту %s", WEB_PORT)
    except OSError as e:
        logging.warning("Порт %s уже занят (%s), мини-сервер не нужен", WEB_PORT, e)


async def keepalive() -> None:
    """Раз в несколько минут обращаемся к собственному публичному адресу."""
    if not KEEPALIVE_URL:
        return  # локальный запуск
    await asyncio.sleep(60)  # даём серверу время запуститься
    async with ClientSession(timeout=ClientTimeout(total=30)) as session:
        while True:
            try:
                async with session.get(KEEPALIVE_URL) as resp:
                    logging.info("keepalive: ответ %s", resp.status)
            except Exception as e:
                logging.warning("keepalive не удался: %s", e)
            await asyncio.sleep(KEEPALIVE_SECONDS)


async def main():
    asyncio.create_task(scheduler())
    asyncio.create_task(keepalive())
    await start_web_server()
    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message", "edited_message", "callback_query", "my_chat_member"],
        )
    finally:
        flush_storage()


if __name__ == "__main__":
    asyncio.run(main())