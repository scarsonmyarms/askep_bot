"""
Telegram-бот для розсилки повідомлень у пули закритих чатів за розкладом.

Можливості:
- автоматична реєстрація чатів, куди додали бота (групи, супергрупи, канали);
- пули чатів (іменовані набори чатів для розсилки);
- адмін-панель в самому боті (/admin): створення розсилок, керування пулами й чатами;
- розклади: одразу, один раз у заданий час, кожні N хвилин, щодня о ГГ:ХХ (кілька разів на день);
- розсилка через copy_message: підтримує текст, фото, відео, документи, голосові тощо
  зі збереженням форматування;
- обробка лімітів Telegram (RetryAfter), видалення бота з чату, міграції групи в супергрупу;
- звіти про виконання адміністраторам.
"""

import asyncio
import html
import io
import json
import logging
import os
import secrets
import time
import zipfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramMigrateToChat,
    TelegramRetryAfter,
)
from aiogram.filters import (
    JOIN_TRANSITION,
    LEAVE_TRANSITION,
    ChatMemberUpdatedFilter,
    Command,
    CommandObject,
    CommandStart,
    Filter,
    StateFilter,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    CallbackQuery,
    ChatAdministratorRights,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    KeyboardButton,
    KeyboardButtonRequestChat,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

# ─────────────────────────── Конфігурація ───────────────────────────

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x}
TZ = ZoneInfo(os.getenv("TZ_NAME", "Europe/Kyiv"))
DB_PATH = os.getenv("DB_PATH", "bot.db")
NOTIFY_ADMINS = os.getenv("NOTIFY_ADMINS", "1") == "1"
RESTRICT_ADD = os.getenv("RESTRICT_ADD", "1") == "1"  # бот виходить з чатів, куди його додав не адмін
SEND_DELAY = float(os.getenv("SEND_DELAY", "0.1"))  # пауза між відправками, сек
COLLECT_LOGS = os.getenv("COLLECT_LOGS", "1") == "1"  # збирати повідомлення з чатів, що входять у пули
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "0"))  # 0 — зберігати без обмеження строку

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не задано (див. .env.example)")
if not ADMIN_IDS:
    raise SystemExit("ADMIN_IDS не задано (див. .env.example)")

DT_STORE = "%Y-%m-%d %H:%M"
DT_UI = "%d.%m.%Y %H:%M"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("broadcast-bot")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone=TZ)
_background: set[asyncio.Task] = set()
# "Режим прийому": чати, куди адмін додає бота протягом певного часу, автоматично потрапляють у пул.
# Використовується для каналів (Telegram не передає параметр у посиланні) і для масового додавання
# скриптом-юзерботом. {admin_id: {"pool_id", "expires", "all_types", "added"}}
_pending_pool: dict[int, dict] = {}
PENDING_CHANNEL_TTL = timedelta(minutes=30)
BULK_TTL = timedelta(minutes=int(os.getenv("BULK_MINUTES", "120")))
POOL_PAYLOAD_PREFIX = "p_"


def esc(value) -> str:
    return html.escape(str(value if value is not None else ""))


def spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)


# ─────────────────────────── База даних ───────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
    chat_id   INTEGER PRIMARY KEY,
    title     TEXT NOT NULL,
    type      TEXT NOT NULL,
    active    INTEGER NOT NULL DEFAULT 1,
    added_at  TEXT
);
CREATE TABLE IF NOT EXISTS pools (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS pool_chats (
    pool_id  INTEGER NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    chat_id  INTEGER NOT NULL REFERENCES chats(chat_id) ON DELETE CASCADE,
    PRIMARY KEY (pool_id, chat_id)
);
CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pool_id         INTEGER NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    src_chat_id     INTEGER NOT NULL,
    src_message_id  INTEGER NOT NULL,
    preview         TEXT,
    schedule_type   TEXT NOT NULL,      -- once | interval | daily
    schedule_value  TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_by      INTEGER,
    created_at      TEXT,
    last_run        TEXT
);
-- Логи повідомлень з чатів пулів. Дані про авторів НЕ зберігаються.
CREATE TABLE IF NOT EXISTS chat_logs (
    chat_id              INTEGER NOT NULL,
    message_id           INTEGER NOT NULL,
    thread_id            INTEGER,
    date_utc             TEXT NOT NULL,     -- YYYY-MM-DD HH:MM:SS (UTC)
    edit_date_utc        TEXT,
    content_type         TEXT NOT NULL,
    text                 TEXT,
    reply_to_message_id  INTEGER,
    media_group_id       TEXT,
    is_forward           INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_chat_logs_date ON chat_logs(date_utc);
"""


def new_pool_token() -> str:
    """Секретна частина посилання: не дає вгадати посилання іншого пулу."""
    return secrets.token_urlsafe(9)  # 12 символів A-Z a-z 0-9 _ -


class DB:
    def __init__(self, path: str):
        self.path = path

    async def _connect(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self.path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys = ON")
        return conn

    async def init(self) -> None:
        conn = await self._connect()
        try:
            await conn.executescript(SCHEMA)
            # Міграція: секретний токен пулу для посилань додавання
            async with conn.execute("PRAGMA table_info(pools)") as cur:
                columns = {row["name"] for row in await cur.fetchall()}
            if "invite_token" not in columns:
                await conn.execute("ALTER TABLE pools ADD COLUMN invite_token TEXT")
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pools_token ON pools(invite_token)")
            async with conn.execute("SELECT id FROM pools WHERE invite_token IS NULL") as cur:
                missing = [row["id"] for row in await cur.fetchall()]
            for pool_id in missing:
                await conn.execute("UPDATE pools SET invite_token=? WHERE id=?", (new_pool_token(), pool_id))
            await conn.commit()
        finally:
            await conn.close()

    async def execute(self, sql: str, params: tuple = ()) -> int:
        conn = await self._connect()
        try:
            cur = await conn.execute(sql, params)
            await conn.commit()
            return cur.lastrowid
        finally:
            await conn.close()

    async def run_many(self, statements: list[tuple[str, tuple]]) -> None:
        """Кілька запитів в одній транзакції."""
        conn = await self._connect()
        try:
            for sql, params in statements:
                await conn.execute(sql, params)
            await conn.commit()
        finally:
            await conn.close()

    async def fetchall(self, sql: str, params: tuple = ()) -> list[dict]:
        conn = await self._connect()
        try:
            async with conn.execute(sql, params) as cur:
                return [dict(r) for r in await cur.fetchall()]
        finally:
            await conn.close()

    async def fetchone(self, sql: str, params: tuple = ()) -> dict | None:
        rows = await self.fetchall(sql, params)
        return rows[0] if rows else None


db = DB(DB_PATH)


async def upsert_chat(chat_id: int, title: str, chat_type: str) -> None:
    await db.execute(
        """INSERT INTO chats(chat_id, title, type, active, added_at) VALUES(?, ?, ?, 1, ?)
           ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, type=excluded.type, active=1""",
        (chat_id, title, chat_type, datetime.now(TZ).strftime(DT_STORE)),
    )


async def set_chat_active(chat_id: int, active: bool) -> None:
    await db.execute("UPDATE chats SET active=? WHERE chat_id=?", (int(active), chat_id))


async def migrate_chat(old_id: int, new_id: int) -> None:
    """Група стала супергрупою — переносимо чат і його членство в пулах на новий ID."""
    await db.run_many([
        ("""INSERT OR IGNORE INTO chats(chat_id, title, type, active, added_at)
            SELECT ?, title, 'supergroup', 1, added_at FROM chats WHERE chat_id=?""", (new_id, old_id)),
        ("""INSERT OR IGNORE INTO pool_chats(pool_id, chat_id)
            SELECT pool_id, ? FROM pool_chats WHERE chat_id=?""", (new_id, old_id)),
        ("UPDATE OR IGNORE chat_logs SET chat_id=? WHERE chat_id=?", (new_id, old_id)),
        ("DELETE FROM chats WHERE chat_id=?", (old_id,)),
    ])
    log.info("Chat migrated %s -> %s", old_id, new_id)


async def count_active_in_pool(pool_id: int) -> int:
    row = await db.fetchone(
        """SELECT COUNT(*) AS n FROM pool_chats pc JOIN chats c ON c.chat_id = pc.chat_id
           WHERE pc.pool_id=? AND c.active=1""",
        (pool_id,),
    )
    return row["n"] if row else 0


# ─────────────────────────── Розклад ───────────────────────────

def parse_once(value: str) -> datetime:
    return datetime.strptime(value, DT_STORE).replace(tzinfo=TZ)


def parse_schedule(stype: str, raw: str) -> str:
    """Перевіряє введення адміна й повертає значення для збереження в БД."""
    raw = raw.strip()
    if stype == "once":
        try:
            dt = datetime.strptime(raw, DT_UI).replace(tzinfo=TZ)
        except ValueError:
            raise ValueError("Неправильний формат. Потрібно ДД.ММ.РРРР ГГ:ХХ")
        if dt <= datetime.now(TZ):
            raise ValueError("Дата й час мають бути в майбутньому")
        return dt.strftime(DT_STORE)
    if stype == "interval":
        if not raw.isdigit() or int(raw) < 1:
            raise ValueError("Потрібне ціле число хвилин, не менше 1")
        return str(int(raw))
    if stype == "daily":
        times = []
        for part in raw.replace(" ", "").split(","):
            try:
                t = datetime.strptime(part, "%H:%M")
            except ValueError:
                raise ValueError(f"Неправильний час «{part}». Потрібно ГГ:ХХ")
            times.append(t.strftime("%H:%M"))
        if not times:
            raise ValueError("Вкажіть хоча б один час")
        return ",".join(sorted(set(times)))
    raise ValueError("Невідомий тип розкладу")


def build_trigger(stype: str, value: str):
    if stype == "once":
        return DateTrigger(run_date=parse_once(value), timezone=TZ)
    if stype == "interval":
        return IntervalTrigger(minutes=int(value), timezone=TZ)
    if stype == "daily":
        crons = []
        for t in value.split(","):
            h, m = t.split(":")
            crons.append(CronTrigger(hour=int(h), minute=int(m), timezone=TZ))
        return crons[0] if len(crons) == 1 else OrTrigger(crons)
    raise ValueError(stype)


def describe_schedule(stype: str, value: str) -> str:
    if stype == "once":
        return f"🕐 Один раз: {parse_once(value).strftime(DT_UI)}"
    if stype == "interval":
        return f"🔁 Кожні {value} хв"
    if stype == "daily":
        return f"📅 Щодня о {value.replace(',', ', ')}"
    return value


def job_id(task_id: int) -> str:
    return f"task_{task_id}"


def schedule_task(task: dict) -> None:
    scheduler.add_job(
        run_task,
        build_trigger(task["schedule_type"], task["schedule_value"]),
        args=[task["id"]],
        id=job_id(task["id"]),
        replace_existing=True,
        misfire_grace_time=600,  # якщо бот "проспав" запуск до 10 хв — все одно виконати
        coalesce=True,
        max_instances=1,
    )


def unschedule_task(task_id: int) -> None:
    try:
        scheduler.remove_job(job_id(task_id))
    except JobLookupError:
        pass


def next_run(task_id: int) -> str:
    job = scheduler.get_job(job_id(task_id))
    if job and job.next_run_time:
        return job.next_run_time.astimezone(TZ).strftime(DT_UI)
    return "—"


async def restore_jobs() -> None:
    now = datetime.now(TZ)
    for t in await db.fetchall("SELECT * FROM tasks WHERE enabled=1"):
        if t["schedule_type"] == "once" and parse_once(t["schedule_value"]) <= now:
            await db.execute("UPDATE tasks SET enabled=0 WHERE id=?", (t["id"],))
            log.warning("Task #%s missed its one-time run while bot was offline, disabled", t["id"])
            continue
        schedule_task(t)
    log.info("Restored %d jobs", len(scheduler.get_jobs()))


# ─────────────────────────── Розсилка ───────────────────────────

async def notify(user_ids, text: str) -> None:
    for uid in user_ids:
        try:
            await bot.send_message(uid, text)
        except Exception as e:  # noqa: BLE001
            log.warning("Cannot notify %s: %s", uid, e)


async def send_copy(chat_id: int, from_chat_id: int, message_id: int) -> str | None:
    """Повертає None при успіху або текст помилки."""
    for _ in range(3):
        try:
            await bot.copy_message(chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id)
            return None
        except TelegramRetryAfter as e:
            log.warning("Flood control for %s, sleeping %ss", chat_id, e.retry_after)
            await asyncio.sleep(e.retry_after + 1)
        except TelegramMigrateToChat as e:
            await migrate_chat(chat_id, e.migrate_to_chat_id)
            chat_id = e.migrate_to_chat_id
        except TelegramForbiddenError:
            await set_chat_active(chat_id, False)
            return "немає доступу (бота видалено з чату?) — чат позначено неактивним"
        except TelegramBadRequest as e:
            return e.message
        except Exception as e:  # noqa: BLE001
            log.exception("Unexpected error sending to %s", chat_id)
            return str(e)
    return "перевищено кількість спроб"


async def run_task(task_id: int, force: bool = False, report_to: int | None = None) -> None:
    task = await db.fetchone("SELECT * FROM tasks WHERE id=?", (task_id,))
    if not task or (not task["enabled"] and not force):
        return

    chats = await db.fetchall(
        """SELECT c.chat_id, c.title FROM chats c JOIN pool_chats pc ON pc.chat_id = c.chat_id
           WHERE pc.pool_id=? AND c.active=1""",
        (task["pool_id"],),
    )
    ok, errors = 0, []
    for chat in chats:
        err = await send_copy(chat["chat_id"], task["src_chat_id"], task["src_message_id"])
        if err is None:
            ok += 1
        else:
            errors.append(f"• {esc(chat['title'])}: {esc(err)}")
        await asyncio.sleep(SEND_DELAY)

    now = datetime.now(TZ)
    await db.execute("UPDATE tasks SET last_run=? WHERE id=?", (now.strftime(DT_STORE), task_id))
    if task["schedule_type"] == "once" and parse_once(task["schedule_value"]) <= now:
        await db.execute("UPDATE tasks SET enabled=0 WHERE id=?", (task_id,))

    log.info("Task #%s done: %s/%s", task_id, ok, len(chats))
    recipients = set(ADMIN_IDS) if NOTIFY_ADMINS else set()
    if report_to:
        recipients.add(report_to)
    if recipients:
        text = f"📤 Розсилка #{task_id} виконана: <b>{ok}/{len(chats)}</b>"
        if errors:
            text += "\n\nПомилки:\n" + "\n".join(errors[:15])
            if len(errors) > 15:
                text += f"\n…та ще {len(errors) - 15}"
        await notify(recipients, text)


# ─────────────────────────── Клавіатури ───────────────────────────

def main_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📨 Нова розсилка", callback_data="new")
    kb.button(text="📋 Розсилки", callback_data="tasks")
    kb.button(text="👥 Пули чатів", callback_data="pools")
    kb.button(text="💬 Чати", callback_data="chats")
    kb.button(text="📊 Логи чатів", callback_data="logs")
    kb.adjust(1, 1, 2, 1)
    return kb.as_markup()


def single_kb(callback: str, text: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=text, callback_data=callback)
    return kb.as_markup()


def cancel_kb() -> InlineKeyboardMarkup:
    return single_kb("cancel", "❌ Скасувати")


def confirm_kb(yes: str, no: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Так", callback_data=yes)
    kb.button(text="❌ Ні", callback_data=no)
    return kb.as_markup()


async def safe_edit(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup | None = None) -> None:
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            await cb.message.answer(text, reply_markup=kb)
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass


MAIN_TEXT = "🛠 <b>Адмін-панель розсилок</b>\nОберіть дію:"


# ─────────────────────────── Фільтри та стани ───────────────────────────

class IsAdmin(Filter):
    async def __call__(self, event) -> bool:
        user = getattr(event, "from_user", None)
        return bool(user and user.id in ADMIN_IDS)


class PoolForm(StatesGroup):
    name = State()


class TaskForm(StatesGroup):
    message = State()
    pool = State()
    schedule_type = State()
    schedule_value = State()
    confirm = State()


groups_router = Router(name="groups")
admin_router = Router(name="admin")
admin_router.message.filter(F.chat.type == ChatType.PRIVATE, IsAdmin())
admin_router.callback_query.filter(IsAdmin())
public_router = Router(name="public")


# ─────────────────────────── Реєстрація чатів ───────────────────────────

@groups_router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=JOIN_TRANSITION))
async def on_bot_added(event: ChatMemberUpdated):
    if event.chat.type == ChatType.PRIVATE:
        return
    title = event.chat.title or str(event.chat.id)
    adder = event.from_user
    if RESTRICT_ADD and adder.id not in ADMIN_IDS:
        log.warning("Bot added to %s by non-admin %s — leaving", event.chat.id, adder.id)
        try:
            await bot.leave_chat(event.chat.id)
        except TelegramBadRequest:
            pass
        await notify(ADMIN_IDS, f"⚠️ Не-адмін (<code>{adder.id}</code>) додав бота в «{esc(title)}». Бот вийшов з чату.")
        return
    await upsert_chat(event.chat.id, title, event.chat.type)
    # Тому, хто додав бота, окреме сповіщення не потрібне — він отримає підтвердження у процесі додавання
    await notify(ADMIN_IDS - {adder.id}, f"➕ Бота додано в чат <b>{esc(title)}</b> (<code>{event.chat.id}</code>).\n"
                            f"<i>Якщо чат не потрапить у пул автоматично — призначте його в /admin → «Пули чатів».</i>")

    pending = _pending_pool.get(adder.id)
    if pending and pending["expires"] > datetime.now(TZ) and \
            (pending["all_types"] or event.chat.type == ChatType.CHANNEL):
        # У масовому режимі не спамимо сповіщенням на кожен чат — лише рахуємо
        if await add_chat_to_pool(pending["pool_id"], event.chat.id, title, notify_admins=not pending["all_types"]):
            pending["added"] += 1


async def add_chat_to_pool(pool_id: int, chat_id: int, title: str, notify_admins: bool = True) -> bool:
    """Додає чат у пул. Повертає True, якщо чат щойно додано (а не був там раніше)."""
    pool = await db.fetchone("SELECT name FROM pools WHERE id=?", (pool_id,))
    if not pool:
        return False
    if await db.fetchone("SELECT 1 FROM pool_chats WHERE pool_id=? AND chat_id=?", (pool_id, chat_id)):
        return False
    await db.execute("INSERT OR IGNORE INTO pool_chats(pool_id, chat_id) VALUES(?, ?)", (pool_id, chat_id))
    invalidate_pooled_cache()
    if notify_admins:
        await notify(ADMIN_IDS, f"📥 Чат <b>{esc(title)}</b> автоматично додано до пулу <b>{esc(pool['name'])}</b>.")
    return True


@groups_router.message(CommandStart(deep_link=True), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def on_group_start_link(message: Message, command: CommandObject):
    """Бота додали в групу за посиланням пулу: t.me/<bot>?startgroup=p_<token>."""
    payload = command.args or ""
    if not payload.startswith(POOL_PAYLOAD_PREFIX):
        return
    if RESTRICT_ADD and (not message.from_user or message.from_user.id not in ADMIN_IDS):
        return  # бот уже виходить з чату в on_bot_added
    pool = await db.fetchone("SELECT id, name FROM pools WHERE invite_token=?",
                             (payload[len(POOL_PAYLOAD_PREFIX):],))
    if not pool:
        await message.reply("⚠️ Посилання недійсне або застаріле. Попросіть адміністратора надіслати нове.")
        return
    title = message.chat.title or str(message.chat.id)
    await upsert_chat(message.chat.id, title, message.chat.type)
    await add_chat_to_pool(pool["id"], message.chat.id, title)
    await message.reply("✅ Чат підключено до розсилок.")


@groups_router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=LEAVE_TRANSITION))
async def on_bot_removed(event: ChatMemberUpdated):
    if event.chat.type == ChatType.PRIVATE:
        return
    await set_chat_active(event.chat.id, False)
    await notify(ADMIN_IDS, f"➖ Бота видалено з чату <b>{esc(event.chat.title)}</b> (<code>{event.chat.id}</code>). "
                            f"Чат позначено неактивним.")


@groups_router.message(Command("register"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}), IsAdmin())
async def cmd_register(message: Message):
    """Для чатів, куди бота додали ще до запуску цієї версії."""
    await upsert_chat(message.chat.id, message.chat.title or str(message.chat.id), message.chat.type)
    await message.reply("✅ Чат зареєстровано для розсилок.")


@groups_router.message(F.migrate_to_chat_id)
async def on_migrate(message: Message):
    await migrate_chat(message.chat.id, message.migrate_to_chat_id)


# ─────────────────────────── Збір логів чатів ───────────────────────────
# Зберігаються лише повідомлення з чатів, що входять хоча б в один пул.
# Дані про авторів (ID, імена, username, джерело пересилання) не зберігаються.
# Контакти, геолокації та місця зберігаються лише як тип повідомлення, без вмісту.

LOGGED_TYPES = {"text", "photo", "video", "animation", "document", "audio", "voice", "video_note",
                "sticker", "poll", "location", "venue", "contact", "dice", "story"}
NO_CONTENT_TYPES = {"contact", "location", "venue"}
_pooled_cache: dict = {"ids": set(), "at": 0.0}


def invalidate_pooled_cache() -> None:
    _pooled_cache["at"] = 0.0


async def is_pooled_chat(chat_id: int) -> bool:
    if time.monotonic() - _pooled_cache["at"] > 30:
        rows = await db.fetchall("SELECT DISTINCT chat_id FROM pool_chats")
        _pooled_cache["ids"] = {r["chat_id"] for r in rows}
        _pooled_cache["at"] = time.monotonic()
    return chat_id in _pooled_cache["ids"]


def utc_str(dt: datetime | int | None) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, (int, float)):  # aiogram віддає edit_date як Unix-час
        dt = datetime.fromtimestamp(dt, tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


async def log_message(message: Message) -> None:
    if not COLLECT_LOGS or not await is_pooled_chat(message.chat.id):
        return
    ctype = str(message.content_type.value if hasattr(message.content_type, "value") else message.content_type)
    if ctype not in LOGGED_TYPES:
        return  # службові повідомлення (вхід/вихід учасників, закріплення тощо) не зберігаємо
    text = None
    if ctype not in NO_CONTENT_TYPES:
        text = message.text or message.caption or (message.poll.question if message.poll else None)
    reply_to = message.reply_to_message.message_id if message.reply_to_message else None
    await db.execute(
        """INSERT INTO chat_logs(chat_id, message_id, thread_id, date_utc, edit_date_utc, content_type, text,
                                 reply_to_message_id, media_group_id, is_forward)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(chat_id, message_id) DO UPDATE SET
               edit_date_utc=excluded.edit_date_utc, content_type=excluded.content_type, text=excluded.text""",
        (message.chat.id, message.message_id, message.message_thread_id, utc_str(message.date),
         utc_str(message.edit_date), ctype, text, reply_to, message.media_group_id,
         int(message.forward_origin is not None)),
    )


GROUP_TYPES = {ChatType.GROUP, ChatType.SUPERGROUP}


@groups_router.message(F.chat.type.in_(GROUP_TYPES))
@groups_router.edited_message(F.chat.type.in_(GROUP_TYPES))
@groups_router.channel_post()
@groups_router.edited_channel_post()
async def on_chat_message(message: Message):
    try:
        await log_message(message)
    except Exception:  # noqa: BLE001 — збій логування не повинен впливати на роботу бота
        log.exception("Failed to log message %s in %s", message.message_id, message.chat.id)


async def cleanup_logs() -> None:
    if LOG_RETENTION_DAYS <= 0:
        return
    border = utc_str(datetime.now(timezone.utc) - timedelta(days=LOG_RETENTION_DAYS))
    await db.execute("DELETE FROM chat_logs WHERE date_utc < ?", (border,))
    log.info("Chat logs older than %s days removed", LOG_RETENTION_DAYS)


# ─────────────────────────── Адмін: загальне ───────────────────────────

@admin_router.message(CommandStart())
@admin_router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(MAIN_TEXT, reply_markup=main_menu_kb())


@admin_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Скасовано.\n\n" + MAIN_TEXT, reply_markup=main_menu_kb())


@admin_router.callback_query(F.data == "cancel")
async def cb_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(cb, "Скасовано.\n\n" + MAIN_TEXT, main_menu_kb())


@admin_router.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(cb, MAIN_TEXT, main_menu_kb())


# ─────────────────────────── Адмін: чати ───────────────────────────

@admin_router.callback_query(F.data == "chats")
async def cb_chats(cb: CallbackQuery):
    chats = await db.fetchall("SELECT * FROM chats ORDER BY active DESC, title")
    kb = InlineKeyboardBuilder()
    if not chats:
        text = ("💬 Чатів ще немає.\n\nДодайте бота в закриту групу або канал (у канал — адміністратором "
                "з правом публікації). Якщо бот уже в групі — надішліть там /register.")
    else:
        lines = [f"{'🟢' if c['active'] else '🔴'} {esc(c['title'])} — <code>{c['chat_id']}</code>" for c in chats]
        text = "💬 <b>Зареєстровані чати</b> (🔴 — бот не має доступу):\n\n" + "\n".join(lines)
        text += "\n\nНатисніть на чат, щоб прибрати його зі списку."
        for c in chats:
            kb.button(text=f"🗑 {c['title'][:40]}", callback_data=f"cd:{c['chat_id']}")
    kb.button(text="⬅️ Меню", callback_data="menu")
    kb.adjust(1)
    await safe_edit(cb, text, kb.as_markup())


@admin_router.callback_query(F.data.startswith("cd:"))
async def cb_chat_delete_ask(cb: CallbackQuery):
    chat_id = int(cb.data.split(":")[1])
    chat = await db.fetchone("SELECT * FROM chats WHERE chat_id=?", (chat_id,))
    if not chat:
        return await cb_chats(cb)
    await safe_edit(cb, f"Прибрати чат <b>{esc(chat['title'])}</b> зі списку та з усіх пулів?\n"
                        f"<i>Бот залишиться в чаті; щоб повернути — /register у групі.</i>",
                    confirm_kb(f"cdok:{chat_id}", "chats"))


@admin_router.callback_query(F.data.startswith("cdok:"))
async def cb_chat_delete(cb: CallbackQuery):
    await db.execute("DELETE FROM chats WHERE chat_id=?", (int(cb.data.split(":")[1]),))
    await cb_chats(cb)


# ─────────────────────────── Адмін: пули ───────────────────────────

@admin_router.callback_query(F.data == "pools")
async def cb_pools(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    pools = await db.fetchall(
        """SELECT p.id, p.name, COUNT(pc.chat_id) AS n FROM pools p
           LEFT JOIN pool_chats pc ON pc.pool_id = p.id GROUP BY p.id ORDER BY p.name""")
    kb = InlineKeyboardBuilder()
    for p in pools:
        kb.button(text=f"👥 {p['name']} ({p['n']})", callback_data=f"pool:{p['id']}")
    kb.button(text="➕ Створити пул", callback_data="poolnew")
    kb.button(text="⬅️ Меню", callback_data="menu")
    kb.adjust(1)
    text = "👥 <b>Пули чатів</b>\nПул — це набір чатів, у які йде розсилка." if pools else \
        "👥 Пулів ще немає. Створіть перший пул і додайте до нього чати."
    await safe_edit(cb, text, kb.as_markup())


async def render_pool(pool_id: int) -> tuple[str, InlineKeyboardMarkup]:
    pool = await db.fetchone("SELECT * FROM pools WHERE id=?", (pool_id,))
    if not pool:
        return "Пул не знайдено.", single_kb("pools", "⬅️ До пулів")
    chats = await db.fetchall(
        """SELECT c.*, (pc.pool_id IS NOT NULL) AS in_pool FROM chats c
           LEFT JOIN pool_chats pc ON pc.chat_id = c.chat_id AND pc.pool_id = ?
           ORDER BY c.active DESC, c.title""",
        (pool_id,),
    )
    tasks_n = (await db.fetchone("SELECT COUNT(*) AS n FROM tasks WHERE pool_id=?", (pool_id,)))["n"]
    in_pool = sum(1 for c in chats if c["in_pool"])
    text = (f"👥 Пул <b>{esc(pool['name'])}</b>\nЧатів у пулі: {in_pool} · Розсилок: {tasks_n}\n\n"
            "Натисніть на чат, щоб додати (✅) або прибрати його з пулу:")
    if not chats:
        text += "\n\n<i>Немає зареєстрованих чатів — спочатку додайте бота в чати.</i>"
    kb = InlineKeyboardBuilder()
    for c in chats:
        mark = "✅" if c["in_pool"] else "▫️"
        suffix = "" if c["active"] else " (неактивний)"
        kb.button(text=f"{mark} {c['title'][:40]}{suffix}", callback_data=f"pt:{pool_id}:{c['chat_id']}")
    kb.button(text="🔗 Посилання для додавання", callback_data=f"pl:{pool_id}")
    kb.button(text="🤖 Масове додавання (режим прийому)", callback_data=f"pa:{pool_id}")
    kb.button(text="☑️ Додати в пул усі активні чати", callback_data=f"pall:{pool_id}")
    kb.button(text="🗑 Видалити пул", callback_data=f"pd:{pool_id}")
    kb.button(text="⬅️ До пулів", callback_data="pools")
    kb.adjust(1)
    return text, kb.as_markup()


@admin_router.callback_query(F.data.startswith("pool:"))
async def cb_pool(cb: CallbackQuery):
    text, kb = await render_pool(int(cb.data.split(":")[1]))
    await safe_edit(cb, text, kb)


@admin_router.callback_query(F.data.startswith("pt:"))
async def cb_pool_toggle(cb: CallbackQuery):
    _, pid, cid = cb.data.split(":")
    pid, cid = int(pid), int(cid)
    exists = await db.fetchone("SELECT 1 FROM pool_chats WHERE pool_id=? AND chat_id=?", (pid, cid))
    if exists:
        await db.execute("DELETE FROM pool_chats WHERE pool_id=? AND chat_id=?", (pid, cid))
    else:
        await db.execute("INSERT OR IGNORE INTO pool_chats(pool_id, chat_id) VALUES(?, ?)", (pid, cid))
    invalidate_pooled_cache()
    text, kb = await render_pool(pid)
    await safe_edit(cb, text, kb)


async def render_pool_links(pool_id: int, admin_id: int) -> tuple[str, InlineKeyboardMarkup]:
    pool = await db.fetchone("SELECT * FROM pools WHERE id=?", (pool_id,))
    if not pool:
        return "Пул не знайдено.", single_kb("pools", "⬅️ До пулів")
    username = (await bot.me()).username
    group_link = f"https://t.me/{username}?startgroup={POOL_PAYLOAD_PREFIX}{pool['invite_token']}"
    channel_link = f"https://t.me/{username}?startchannel&admin=post_messages"
    # Каналам параметр не передається — запам'ятовуємо пул для цього адміна на обмежений час
    current = _pending_pool.get(admin_id)
    if not (current and current["all_types"] and current["expires"] > datetime.now(TZ)):
        # не перебиваємо активний масовий режим
        _pending_pool[admin_id] = {"pool_id": pool_id, "expires": datetime.now(TZ) + PENDING_CHANNEL_TTL,
                                   "all_types": False, "added": 0}
    minutes = int(PENDING_CHANNEL_TTL.total_seconds() // 60)
    text = (f"🔗 <b>Додавання чатів у пул «{esc(pool['name'])}»</b>\n\n"
            f"<b>Групи:</b> натисніть кнопку або відкрийте посилання, оберіть групу — бот додасться "
            f"і чат одразу потрапить у цей пул. Повторюйте для кожної групи.\n"
            f"<code>{esc(group_link)}</code>\n\n"
            f"<b>Канали:</b> бот додається адміністратором з правом публікації. Канали, які ви додасте "
            f"протягом {minutes} хв, потраплять у цей пул (для каналів Telegram не передає параметр у посиланні, "
            f"тому прив'язка діє лише для вас і лише в цей проміжок часу).\n"
            f"<code>{esc(channel_link)}</code>")
    if RESTRICT_ADD:
        text += "\n\n<i>Посиланнями можуть користуватися лише адміністратори бота — з чатів, куди бота додасть " \
                "інша людина, він вийде автоматично.</i>"
    else:
        text += "\n\n<i>⚠️ RESTRICT_ADD вимкнено: будь-хто з посиланням на групу зможе додати свій чат у пул. " \
                "Якщо посилання потрапило не туди — натисніть «Оновити посилання».</i>"
    text = ("📲 <b>Рекомендовано:</b> кнопка «Обирати чати тут» — ви лишаєтесь у чаті з ботом і можете "
            "додавати групи одну за одною без переходів.\n\n") + text
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 Обирати чати тут (без переходів)", callback_data=f"pk:{pool_id}")
    kb.button(text="➕ Додати в групу", url=group_link)
    kb.button(text="📢 Додати в канал", url=channel_link)
    kb.button(text="🔄 Оновити посилання (старе перестане діяти)", callback_data=f"plr:{pool_id}")
    kb.button(text="⬅️ До пулу", callback_data=f"pool:{pool_id}")
    kb.adjust(1, 2, 1, 1)
    return text, kb.as_markup()


@admin_router.callback_query(F.data.startswith("pl:"))
async def cb_pool_links(cb: CallbackQuery):
    text, kb = await render_pool_links(int(cb.data.split(":")[1]), cb.from_user.id)
    await safe_edit(cb, text, kb)


@admin_router.callback_query(F.data.startswith("plr:"))
async def cb_pool_links_regenerate(cb: CallbackQuery):
    pool_id = int(cb.data.split(":")[1])
    await db.execute("UPDATE pools SET invite_token=? WHERE id=?", (new_pool_token(), pool_id))
    await cb.answer("Посилання оновлено, старе більше не діє", show_alert=True)
    text, kb = await render_pool_links(pool_id, cb.from_user.id)
    await safe_edit(cb, text, kb)


async def render_bulk(pool_id: int, admin_id: int) -> tuple[str, InlineKeyboardMarkup]:
    pool = await db.fetchone("SELECT name FROM pools WHERE id=?", (pool_id,))
    if not pool:
        return "Пул не знайдено.", single_kb("pools", "⬅️ До пулів")
    pending = _pending_pool.get(admin_id)
    active = bool(pending and pending["all_types"] and pending["pool_id"] == pool_id
                  and pending["expires"] > datetime.now(TZ))
    kb = InlineKeyboardBuilder()
    if active:
        text = (f"🤖 <b>Режим прийому увімкнено</b> для пулу «{esc(pool['name'])}»\n"
                f"Діє до: {pending['expires'].strftime('%H:%M')}\n"
                f"Додано чатів за цей час: <b>{pending['added']}</b>\n\n"
                "Усі групи й канали, куди <b>ваш акаунт</b> додає бота, автоматично потрапляють у цей пул. "
                "Запустіть скрипт <code>add_bot_bulk.py</code> — або додавайте бота вручну.")
        kb.button(text="🔄 Оновити статус", callback_data=f"pa:{pool_id}")
        kb.button(text="⏹ Вимкнути режим", callback_data=f"pa0:{pool_id}")
    else:
        text = (f"🤖 <b>Масове додавання в пул «{esc(pool['name'])}»</b>\n\n"
                f"1. Увімкніть режим прийому (діє {int(BULK_TTL.total_seconds() // 60)} хв).\n"
                "2. Запустіть на комп'ютері скрипт <code>add_bot_bulk.py</code> від імені свого акаунта — "
                "він додасть бота в обрані чати без ручних кліків.\n"
                "3. Усі ці чати автоматично потраплять у пул.\n\n"
                "<i>Режим діє лише для вашого акаунта. Інструкція — у README.</i>")
        kb.button(text="▶️ Увімкнути режим прийому", callback_data=f"pa1:{pool_id}")
    kb.button(text="⬅️ До пулу", callback_data=f"pool:{pool_id}")
    kb.adjust(1)
    return text, kb.as_markup()


@admin_router.callback_query(F.data.startswith("pa:"))
async def cb_bulk(cb: CallbackQuery):
    text, kb = await render_bulk(int(cb.data.split(":")[1]), cb.from_user.id)
    await safe_edit(cb, text, kb)


@admin_router.callback_query(F.data.startswith("pa1:"))
async def cb_bulk_on(cb: CallbackQuery):
    pool_id = int(cb.data.split(":")[1])
    _pending_pool[cb.from_user.id] = {"pool_id": pool_id, "expires": datetime.now(TZ) + BULK_TTL,
                                      "all_types": True, "added": 0}
    text, kb = await render_bulk(pool_id, cb.from_user.id)
    await safe_edit(cb, text, kb)


@admin_router.callback_query(F.data.startswith("pa0:"))
async def cb_bulk_off(cb: CallbackQuery):
    pool_id = int(cb.data.split(":")[1])
    pending = _pending_pool.pop(cb.from_user.id, None)
    added = pending["added"] if pending else 0
    await cb.answer(f"Режим вимкнено. Додано чатів: {added}", show_alert=True)
    text, kb = await render_pool(pool_id)
    await safe_edit(cb, text, kb)


@admin_router.callback_query(F.data.startswith("pall:"))
async def cb_pool_add_all(cb: CallbackQuery):
    pool_id = int(cb.data.split(":")[1])
    before = await count_active_in_pool(pool_id)
    await db.execute("INSERT OR IGNORE INTO pool_chats(pool_id, chat_id) SELECT ?, chat_id FROM chats WHERE active=1",
                     (pool_id,))
    invalidate_pooled_cache()
    await cb.answer(f"Додано чатів: {await count_active_in_pool(pool_id) - before}", show_alert=True)
    text, kb = await render_pool(pool_id)
    await safe_edit(cb, text, kb)


@admin_router.callback_query(F.data.startswith("pd:"))
async def cb_pool_delete_ask(cb: CallbackQuery):
    pid = int(cb.data.split(":")[1])
    n = (await db.fetchone("SELECT COUNT(*) AS n FROM tasks WHERE pool_id=?", (pid,)))["n"]
    await safe_edit(cb, f"Видалити пул разом із розсилками, прив'язаними до нього ({n})?",
                    confirm_kb(f"pdok:{pid}", f"pool:{pid}"))


@admin_router.callback_query(F.data.startswith("pdok:"))
async def cb_pool_delete(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    for t in await db.fetchall("SELECT id FROM tasks WHERE pool_id=?", (pid,)):
        unschedule_task(t["id"])
    await db.execute("DELETE FROM pools WHERE id=?", (pid,))
    await cb_pools(cb, state)


@admin_router.callback_query(F.data == "poolnew")
async def cb_pool_new(cb: CallbackQuery, state: FSMContext):
    await state.set_state(PoolForm.name)
    await safe_edit(cb, "Введіть назву нового пулу:", cancel_kb())


@admin_router.message(PoolForm.name, F.text)
async def got_pool_name(message: Message, state: FSMContext):
    name = message.text.strip()[:64]
    if not name or name.startswith("/"):
        return await message.answer("Назва не може бути порожньою. Спробуйте ще раз:", reply_markup=cancel_kb())
    try:
        pid = await db.execute("INSERT INTO pools(name, invite_token) VALUES(?, ?)", (name, new_pool_token()))
    except aiosqlite.IntegrityError:
        return await message.answer("Пул з такою назвою вже існує. Введіть іншу назву:", reply_markup=cancel_kb())
    await state.clear()
    text, kb = await render_pool(pid)
    await message.answer(text, reply_markup=kb)


# ─────────────────────────── Адмін: створення розсилки ───────────────────────────

@admin_router.callback_query(F.data == "new")
async def cb_new(cb: CallbackQuery, state: FSMContext):
    if not (await db.fetchone("SELECT COUNT(*) AS n FROM pools"))["n"]:
        return await cb.answer("Спочатку створіть пул чатів", show_alert=True)
    await state.clear()
    await state.set_state(TaskForm.message)
    await safe_edit(cb, "📨 <b>Нова розсилка</b>\n\nНадішліть повідомлення для розсилки: текст, фото, відео, "
                        "документ, голосове тощо. Форматування збережеться.\n\n"
                        "<i>Не видаляйте це повідомлення з чату з ботом — розсилка копіює саме його.</i>",
                    cancel_kb())


@admin_router.message(TaskForm.message)
async def got_task_message(message: Message, state: FSMContext):
    preview = (message.text or message.caption or f"[{message.content_type}]").replace("\n", " ")[:60]
    await state.update_data(src_chat_id=message.chat.id, src_message_id=message.message_id, preview=preview)
    await state.set_state(TaskForm.pool)
    pools = await db.fetchall(
        """SELECT p.id, p.name, COUNT(c.chat_id) AS n FROM pools p
           LEFT JOIN pool_chats pc ON pc.pool_id = p.id
           LEFT JOIN chats c ON c.chat_id = pc.chat_id AND c.active = 1
           GROUP BY p.id ORDER BY p.name""")
    kb = InlineKeyboardBuilder()
    for p in pools:
        kb.button(text=f"👥 {p['name']} ({p['n']} чатів)", callback_data=f"tp:{p['id']}")
    kb.button(text="❌ Скасувати", callback_data="cancel")
    kb.adjust(1)
    note = "\n\n⚠️ Альбоми не підтримуються — буде розіслано лише одне це медіа." if message.media_group_id else ""
    await message.answer("Кому надсилати? Оберіть пул:" + note, reply_markup=kb.as_markup())


@admin_router.callback_query(TaskForm.pool, F.data.startswith("tp:"))
async def cb_task_pool(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    if await count_active_in_pool(pid) == 0:
        return await cb.answer("У цьому пулі немає активних чатів", show_alert=True)
    await state.update_data(pool_id=pid)
    await state.set_state(TaskForm.schedule_type)
    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 Надіслати зараз", callback_data="ts:now")
    kb.button(text="🕐 Один раз у заданий час", callback_data="ts:once")
    kb.button(text="🔁 Кожні N хвилин", callback_data="ts:interval")
    kb.button(text="📅 Щодня о певний час", callback_data="ts:daily")
    kb.button(text="❌ Скасувати", callback_data="cancel")
    kb.adjust(1)
    await safe_edit(cb, "Коли надсилати?", kb.as_markup())


def schedule_prompt(stype: str) -> str:
    example = datetime.now(TZ).replace(second=0, microsecond=0)
    example = example.replace(hour=(example.hour + 1) % 24).strftime(DT_UI)
    return {
        "once": f"Введіть дату й час у форматі <code>ДД.ММ.РРРР ГГ:ХХ</code>\nНаприклад: <code>{example}</code>",
        "interval": "Введіть інтервал у хвилинах (ціле число ≥ 1).\nНаприклад: <code>60</code> — щогодини, "
                    "<code>1440</code> — раз на добу.\n<i>Перша відправка — через вказаний інтервал.</i>",
        "daily": "Введіть час у форматі <code>ГГ:ХХ</code>. Можна кілька через кому.\n"
                 "Наприклад: <code>09:00, 18:30</code>",
    }[stype] + f"\n\nЧасовий пояс: {TZ.key}"


@admin_router.callback_query(TaskForm.schedule_type, F.data.startswith("ts:"))
async def cb_task_schedule_type(cb: CallbackQuery, state: FSMContext):
    stype = cb.data.split(":")[1]
    if stype == "now":
        await state.update_data(schedule_type="once", now=True,
                                schedule_value=datetime.now(TZ).strftime(DT_STORE))
        await state.set_state(TaskForm.confirm)
        text, kb = await confirm_view(state)
        return await safe_edit(cb, text, kb)
    await state.update_data(schedule_type=stype, now=False)
    await state.set_state(TaskForm.schedule_value)
    await safe_edit(cb, schedule_prompt(stype), cancel_kb())


@admin_router.message(TaskForm.schedule_value, F.text)
async def got_schedule_value(message: Message, state: FSMContext):
    data = await state.get_data()
    try:
        value = parse_schedule(data["schedule_type"], message.text)
    except ValueError as e:
        return await message.answer(f"❌ {esc(e)}\n\n{schedule_prompt(data['schedule_type'])}",
                                    reply_markup=cancel_kb())
    await state.update_data(schedule_value=value)
    await state.set_state(TaskForm.confirm)
    text, kb = await confirm_view(state)
    await message.answer(text, reply_markup=kb)


async def confirm_view(state: FSMContext) -> tuple[str, InlineKeyboardMarkup]:
    d = await state.get_data()
    pool = await db.fetchone("SELECT name FROM pools WHERE id=?", (d["pool_id"],))
    n = await count_active_in_pool(d["pool_id"])
    when = "🚀 Одразу після підтвердження" if d.get("now") else describe_schedule(d["schedule_type"], d["schedule_value"])
    text = (f"<b>Перевірте розсилку:</b>\n\n"
            f"Пул: {esc(pool['name'] if pool else '?')} ({n} активних чатів)\n"
            f"Розклад: {when}\n"
            f"Повідомлення: <i>{esc(d['preview'])}</i>\n\nСтворити?")
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Створити", callback_data="tc:ok")
    kb.button(text="❌ Скасувати", callback_data="cancel")
    return text, kb.as_markup()


@admin_router.callback_query(TaskForm.confirm, F.data == "tc:ok")
async def cb_task_confirm(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.clear()
    tid = await db.execute(
        """INSERT INTO tasks(pool_id, src_chat_id, src_message_id, preview, schedule_type, schedule_value,
                             enabled, created_by, created_at) VALUES(?, ?, ?, ?, ?, ?, 1, ?, ?)""",
        (d["pool_id"], d["src_chat_id"], d["src_message_id"], d["preview"], d["schedule_type"],
         d["schedule_value"], cb.from_user.id, datetime.now(TZ).strftime(DT_STORE)),
    )
    if d.get("now"):
        spawn(run_task(tid, force=True, report_to=cb.from_user.id))
        text = f"🚀 Розсилку #{tid} запущено. Звіт надійде після завершення."
    else:
        schedule_task(await db.fetchone("SELECT * FROM tasks WHERE id=?", (tid,)))
        text = f"✅ Розсилку #{tid} створено.\nНаступний запуск: <b>{next_run(tid)}</b>"
    await safe_edit(cb, text + "\n\n" + MAIN_TEXT, main_menu_kb())


# ─────────────────────────── Адмін: список розсилок ───────────────────────────

@admin_router.callback_query(F.data == "tasks")
async def cb_tasks(cb: CallbackQuery):
    tasks = await db.fetchall("SELECT * FROM tasks ORDER BY enabled DESC, id DESC LIMIT 50")
    kb = InlineKeyboardBuilder()
    for t in tasks:
        kb.button(text=f"{'🟢' if t['enabled'] else '⚪️'} #{t['id']} {(t['preview'] or '')[:30]}",
                  callback_data=f"task:{t['id']}")
    kb.button(text="➕ Нова розсилка", callback_data="new")
    kb.button(text="⬅️ Меню", callback_data="menu")
    kb.adjust(1)
    text = "📋 <b>Розсилки</b> (🟢 активні, ⚪️ вимкнені/завершені):" if tasks else "📋 Розсилок ще немає."
    await safe_edit(cb, text, kb.as_markup())


async def render_task(tid: int) -> tuple[str, InlineKeyboardMarkup]:
    t = await db.fetchone(
        "SELECT t.*, p.name AS pool_name FROM tasks t JOIN pools p ON p.id = t.pool_id WHERE t.id=?", (tid,))
    if not t:
        return "Розсилку не знайдено.", single_kb("tasks", "⬅️ До списку")
    last = datetime.strptime(t["last_run"], DT_STORE).strftime(DT_UI) if t["last_run"] else "—"
    text = (f"📨 <b>Розсилка #{t['id']}</b>\n\n"
            f"Статус: {'🟢 активна' if t['enabled'] else '⚪️ вимкнена'}\n"
            f"Пул: {esc(t['pool_name'])} ({await count_active_in_pool(t['pool_id'])} активних чатів)\n"
            f"Розклад: {describe_schedule(t['schedule_type'], t['schedule_value'])}\n"
            f"Наступний запуск: {next_run(tid) if t['enabled'] else '—'}\n"
            f"Останній запуск: {last}\n"
            f"Повідомлення: <i>{esc(t['preview'])}</i>")
    kb = InlineKeyboardBuilder()
    kb.button(text="⏸ Призупинити" if t["enabled"] else "▶️ Увімкнути", callback_data=f"tt:{tid}")
    kb.button(text="🚀 Надіслати зараз", callback_data=f"tr:{tid}")
    kb.button(text="👁 Показати повідомлення", callback_data=f"tv:{tid}")
    kb.button(text="🗑 Видалити", callback_data=f"td:{tid}")
    kb.button(text="⬅️ До списку", callback_data="tasks")
    kb.adjust(2, 1, 1, 1)
    return text, kb.as_markup()


@admin_router.callback_query(F.data.startswith("task:"))
async def cb_task(cb: CallbackQuery):
    text, kb = await render_task(int(cb.data.split(":")[1]))
    await safe_edit(cb, text, kb)


@admin_router.callback_query(F.data.startswith("tt:"))
async def cb_task_toggle(cb: CallbackQuery):
    tid = int(cb.data.split(":")[1])
    t = await db.fetchone("SELECT * FROM tasks WHERE id=?", (tid,))
    if not t:
        return await cb_tasks(cb)
    if t["enabled"]:
        unschedule_task(tid)
        await db.execute("UPDATE tasks SET enabled=0 WHERE id=?", (tid,))
    else:
        if t["schedule_type"] == "once" and parse_once(t["schedule_value"]) <= datetime.now(TZ):
            return await cb.answer("Час одноразової розсилки вже минув. Створіть нову.", show_alert=True)
        await db.execute("UPDATE tasks SET enabled=1 WHERE id=?", (tid,))
        schedule_task(t)
    text, kb = await render_task(tid)
    await safe_edit(cb, text, kb)


@admin_router.callback_query(F.data.startswith("tr:"))
async def cb_task_run_now(cb: CallbackQuery):
    tid = int(cb.data.split(":")[1])
    spawn(run_task(tid, force=True, report_to=cb.from_user.id))
    await cb.answer("🚀 Розсилку запущено, звіт надійде окремим повідомленням", show_alert=True)


@admin_router.callback_query(F.data.startswith("tv:"))
async def cb_task_view_message(cb: CallbackQuery):
    t = await db.fetchone("SELECT * FROM tasks WHERE id=?", (int(cb.data.split(":")[1]),))
    if not t:
        return await cb.answer("Розсилку не знайдено", show_alert=True)
    try:
        await bot.copy_message(cb.from_user.id, t["src_chat_id"], t["src_message_id"])
        await cb.answer()
    except TelegramBadRequest:
        await cb.answer("Оригінал повідомлення видалено — розсилка не спрацює. Створіть нову.", show_alert=True)


@admin_router.callback_query(F.data.startswith("td:"))
async def cb_task_delete_ask(cb: CallbackQuery):
    tid = int(cb.data.split(":")[1])
    await safe_edit(cb, f"Видалити розсилку #{tid}?", confirm_kb(f"tdok:{tid}", f"task:{tid}"))


@admin_router.callback_query(F.data.startswith("tdok:"))
async def cb_task_delete(cb: CallbackQuery):
    tid = int(cb.data.split(":")[1])
    unschedule_task(tid)
    await db.execute("DELETE FROM tasks WHERE id=?", (tid,))
    await cb_tasks(cb)


# ─────────────────────────── Адмін: вибір чатів кнопкою request_chat ───────────────────────────

DONE_TEXT = "✅ Готово"


def rights(**enabled) -> ChatAdministratorRights:
    base = dict(is_anonymous=False, can_manage_chat=False, can_delete_messages=False,
                can_manage_video_chats=False, can_restrict_members=False, can_promote_members=False,
                can_change_info=False, can_invite_users=False, can_post_stories=False,
                can_edit_stories=False, can_delete_stories=False, can_send_welcome_messages=False)
    base.update(enabled)
    return ChatAdministratorRights(**base)


def pick_chats_kb(pool_id: int, pool_name: str) -> ReplyKeyboardMarkup:
    """Кнопки вибору чату. request_id кодує ID пулу (має бути унікальним у межах клавіатури):
    група = pool_id * 2, канал = pool_id * 2 + 1. Декодування: pool_id = request_id // 2."""
    group_rights = rights(can_manage_chat=True)
    channel_rights = rights(can_manage_chat=True, can_post_messages=True)
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=f"➕ Група → «{pool_name[:25]}»", request_chat=KeyboardButtonRequestChat(
                request_id=pool_id * 2, chat_is_channel=False, request_title=True,
                user_administrator_rights=group_rights, bot_administrator_rights=group_rights))],
            [KeyboardButton(text=f"📢 Канал → «{pool_name[:25]}»", request_chat=KeyboardButtonRequestChat(
                request_id=pool_id * 2 + 1, chat_is_channel=True, request_title=True,
                user_administrator_rights=channel_rights, bot_administrator_rights=channel_rights))],
            [KeyboardButton(text=DONE_TEXT)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Оберіть чат кнопкою нижче",
    )


@admin_router.callback_query(F.data.startswith("pk:"))
async def cb_pick_chats(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    pool_id = int(cb.data.split(":")[1])
    pool = await db.fetchone("SELECT name FROM pools WHERE id=?", (pool_id,))
    if not pool:
        return await cb.answer("Пул не знайдено", show_alert=True)
    await cb.answer()
    await cb.message.answer(
        f"📲 Додавання чатів у пул <b>{esc(pool['name'])}</b>\n\n"
        "Натисніть кнопку внизу, оберіть групу чи канал і підтвердьте додавання бота. "
        "Ви залишитеся тут, тож одразу можна обирати наступний чат.\n\n"
        f"Коли закінчите — натисніть «{DONE_TEXT}».\n"
        "<i>Список показує лише чати, де ви адміністратор.</i>",
        reply_markup=pick_chats_kb(pool_id, pool["name"]))


@admin_router.message(F.chat_shared)
async def on_chat_shared(message: Message):
    shared = message.chat_shared
    pool = await db.fetchone("SELECT id, name FROM pools WHERE id=?", (shared.request_id // 2,))
    if not pool:
        return await message.answer("⚠️ Пул не знайдено (можливо, його видалено).")
    try:
        me = await bot.me()
        member = await bot.get_chat_member(shared.chat_id, me.id)
        chat = await bot.get_chat(shared.chat_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        return await message.answer("⚠️ Бот не отримав доступу до цього чату. Спробуйте ще раз і підтвердьте "
                                    "додавання бота.")
    if member.status in ("left", "kicked"):
        return await message.answer("⚠️ Бота не додано в цей чат. Спробуйте ще раз і підтвердьте додавання.")
    title = chat.title or shared.title or str(shared.chat_id)
    await upsert_chat(shared.chat_id, title, chat.type)
    added = await add_chat_to_pool(pool["id"], shared.chat_id, title, notify_admins=False)
    n = await count_active_in_pool(pool["id"])
    status = "додано до пулу" if added else "вже є в пулі"
    await message.answer(f"✅ <b>{esc(title)}</b> {status} <b>{esc(pool['name'])}</b>. "
                         f"Чатів у пулі: {n}.\nОбирайте наступний або натисніть «{DONE_TEXT}».")
    others = ADMIN_IDS - {message.from_user.id}
    if added and others:
        await notify(others, f"📥 Чат <b>{esc(title)}</b> додано до пулу <b>{esc(pool['name'])}</b>.")


@admin_router.message(F.text == DONE_TEXT)
async def on_pick_done(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Готово.", reply_markup=ReplyKeyboardRemove())
    await message.answer(MAIN_TEXT, reply_markup=main_menu_kb())


# ─────────────────────────── Адмін: логи чатів ───────────────────────────

@admin_router.callback_query(F.data == "logs")
async def cb_logs(cb: CallbackQuery):
    stats = await db.fetchone("SELECT COUNT(*) AS n, MIN(date_utc) AS first, COUNT(DISTINCT chat_id) AS chats "
                              "FROM chat_logs")
    pools = await db.fetchall("SELECT id, name FROM pools ORDER BY name")
    first = "—"
    if stats["first"]:
        first = datetime.strptime(stats["first"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) \
            .astimezone(TZ).strftime(DT_UI)
    retention = f"{LOG_RETENTION_DAYS} дн." if LOG_RETENTION_DAYS > 0 else "без обмеження"
    text = (f"📊 <b>Логи чатів</b>\n\n"
            f"Збір: {'🟢 увімкнено' if COLLECT_LOGS else '⚪️ вимкнено (COLLECT_LOGS=0)'}\n"
            f"Повідомлень: <b>{stats['n']}</b> з {stats['chats']} чатів\n"
            f"Найстаріше: {first}\nСтрок зберігання: {retention}\n\n"
            "Збираються нові повідомлення з чатів, що входять у пули. Дані про авторів не зберігаються, "
            "але тексти можуть містити персональні дані — поводьтеся з експортом обережно.\n\n"
            "<i>Щоб бот бачив усі повідомлення в групах, де він не адміністратор, вимкніть Privacy Mode: "
            "@BotFather → /setprivacy → Disable.</i>\n\nОберіть, що експортувати в JSON:")
    kb = InlineKeyboardBuilder()
    for p in pools:
        kb.button(text=f"📥 Пул «{p['name'][:30]}»", callback_data=f"lg:{p['id']}")
    kb.button(text="📥 Усі чати", callback_data="lg:0")
    kb.button(text="🗑 Очистити всі логи", callback_data="lc")
    kb.button(text="⬅️ Меню", callback_data="menu")
    kb.adjust(1)
    await safe_edit(cb, text, kb.as_markup())


@admin_router.callback_query(F.data.startswith("lg:"))
async def cb_logs_period(cb: CallbackQuery):
    pid = int(cb.data.split(":")[1])
    kb = InlineKeyboardBuilder()
    for days, label in ((1, "За добу"), (7, "За 7 днів"), (30, "За 30 днів"), (0, "За весь час")):
        kb.button(text=label, callback_data=f"le:{pid}:{days}")
    kb.button(text="⬅️ Назад", callback_data="logs")
    kb.adjust(2, 2, 1)
    await safe_edit(cb, "За який період експортувати?", kb.as_markup())


async def build_logs_export(pool_id: int, days: int) -> tuple[bytes, dict]:
    now_utc = datetime.now(timezone.utc)
    since = utc_str(now_utc - timedelta(days=days)) if days else "0000-00-00 00:00:00"
    pool = None
    if pool_id:
        pool = await db.fetchone("SELECT id, name FROM pools WHERE id=?", (pool_id,))
        chats = await db.fetchall(
            "SELECT c.chat_id, c.title, c.type FROM chats c JOIN pool_chats pc ON pc.chat_id=c.chat_id "
            "WHERE pc.pool_id=? ORDER BY c.title", (pool_id,))
    else:
        chats = await db.fetchall(
            "SELECT DISTINCT c.chat_id, c.title, c.type FROM chats c "
            "WHERE c.chat_id IN (SELECT chat_id FROM chat_logs) ORDER BY c.title")
    chat_ids = [c["chat_id"] for c in chats]
    messages = []
    if chat_ids:
        marks = ",".join("?" * len(chat_ids))
        messages = await db.fetchall(
            f"SELECT * FROM chat_logs WHERE chat_id IN ({marks}) AND date_utc >= ? ORDER BY chat_id, date_utc",
            (*chat_ids, since))

    def iso(value):
        if not value:
            return None
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) \
            .astimezone(TZ).isoformat(timespec="seconds")

    counts: dict[int, int] = {}
    for m in messages:
        counts[m["chat_id"]] = counts.get(m["chat_id"], 0) + 1
    export = {
        "exported_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "timezone": TZ.key,
        "pool": {"id": pool["id"], "name": pool["name"]} if pool else None,
        "period_days": days or None,
        "note": "Дані про авторів повідомлень не зберігаються.",
        "chats": [{"chat_id": c["chat_id"], "title": c["title"], "type": c["type"],
                   "messages_count": counts.get(c["chat_id"], 0)} for c in chats],
        "messages": [{"chat_id": m["chat_id"], "message_id": m["message_id"], "thread_id": m["thread_id"],
                      "date": iso(m["date_utc"]), "edit_date": iso(m["edit_date_utc"]),
                      "content_type": m["content_type"], "text": m["text"],
                      "reply_to_message_id": m["reply_to_message_id"], "media_group_id": m["media_group_id"],
                      "is_forward": bool(m["is_forward"])} for m in messages],
    }
    data = json.dumps(export, ensure_ascii=False, indent=1).encode("utf-8")
    return data, {"messages": len(messages), "chats": len(chats)}


@admin_router.callback_query(F.data.startswith("le:"))
async def cb_logs_export(cb: CallbackQuery):
    _, pid, days = cb.data.split(":")
    pid, days = int(pid), int(days)
    await safe_edit(cb, "⏳ Готую файл…")
    data, info = await build_logs_export(pid, days)
    stamp = datetime.now(TZ).strftime("%Y%m%d_%H%M")
    name = f"chat_logs_{'pool' + str(pid) if pid else 'all'}_{stamp}.json"
    if len(data) > 45 * 1024 * 1024:  # ліміт Telegram на файл від бота — 50 МБ
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(name, data)
        data, name = buf.getvalue(), name.replace(".json", ".zip")
    if len(data) > 49 * 1024 * 1024:
        await cb.message.answer("⚠️ Файл завеликий для Telegram навіть у стиснутому вигляді. "
                                "Оберіть коротший період або окремий пул.", reply_markup=main_menu_kb())
        return
    await bot.send_document(cb.from_user.id, BufferedInputFile(data, filename=name),
                            caption=f"📊 Повідомлень: {info['messages']}, чатів: {info['chats']}")
    await cb.message.answer(MAIN_TEXT, reply_markup=main_menu_kb())


@admin_router.callback_query(F.data == "lc")
async def cb_logs_clear_ask(cb: CallbackQuery):
    await safe_edit(cb, "Видалити <b>всі</b> зібрані логи чатів? Цю дію не можна скасувати.",
                    confirm_kb("lcok", "logs"))


@admin_router.callback_query(F.data == "lcok")
async def cb_logs_clear(cb: CallbackQuery):
    await db.execute("DELETE FROM chat_logs")
    await cb.answer("Логи видалено", show_alert=True)
    await cb_logs(cb)


@admin_router.message(StateFilter(None))
async def admin_fallback(message: Message):
    await message.answer("Щоб відкрити панель, надішліть /admin")


# ─────────────────────────── Для не-адмінів ───────────────────────────

@public_router.message(F.chat.type == ChatType.PRIVATE)
async def not_admin(message: Message):
    await message.answer(f"⛔ Доступ заборонено.\nВаш Telegram ID: <code>{message.from_user.id}</code>")


@public_router.callback_query()
async def not_admin_cb(cb: CallbackQuery):
    await cb.answer("⛔ Доступ заборонено", show_alert=True)


# ─────────────────────────── Запуск ───────────────────────────

async def main() -> None:
    await db.init()
    scheduler.start()
    await restore_jobs()
    if LOG_RETENTION_DAYS > 0:
        await cleanup_logs()
        scheduler.add_job(cleanup_logs, CronTrigger(hour=3, minute=30, timezone=TZ), id="cleanup_logs",
                          replace_existing=True)
    dp.include_routers(groups_router, admin_router, public_router)
    await bot.set_my_commands(
        [BotCommand(command="admin", description="Адмін-панель"),
         BotCommand(command="cancel", description="Скасувати дію")],
        scope=BotCommandScopeAllPrivateChats())
    await bot.set_my_commands(
        [BotCommand(command="register", description="Зареєструвати чат для розсилок")],
        scope=BotCommandScopeAllGroupChats())
    # Бот працює через polling, тож webhook (якщо його колись встановили для цього токена) треба вимкнути,
    # інакше Telegram повертає TelegramConflictError.
    webhook = await bot.get_webhook_info()
    if webhook.url:
        log.warning("Active webhook found (%s) — deleting it to use polling", webhook.url)
        await bot.delete_webhook(drop_pending_updates=False)
    log.info("Bot started. Admins: %s, TZ: %s", ADMIN_IDS, TZ.key)
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())