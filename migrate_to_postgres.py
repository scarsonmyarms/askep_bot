"""
Перенесення даних бота з SQLite (bot.db) у PostgreSQL (Neon / Supabase).

  DATABASE_URL="postgresql://..." python migrate_to_postgres.py --sqlite bot.db

Переносяться чати, пули, розсилки, логи чатів, адміністратори й автопересилання.
Цільова база має бути порожньою (інакше скрипт зупиниться; --force — додати поверх, пропускаючи дублікати).
Перед перенесенням ЗУПИНІТЬ бота, щоб дані не змінювалися під час копіювання.
"""

import argparse
import asyncio
import os

from dotenv import load_dotenv

from storage import PostgresDatabase, SQLiteDatabase, mask_dsn

load_dotenv()

# Порядок важливий: спершу таблиці, на які посилаються інші
TABLES = [
    ("chats", ["chat_id", "title", "type", "active", "added_at"]),
    ("pools", ["id", "name", "invite_token"]),
    ("pool_chats", ["pool_id", "chat_id"]),
    ("tasks", ["id", "pool_id", "src_chat_id", "src_message_id", "preview", "schedule_type", "schedule_value",
               "enabled", "created_by", "created_at", "last_run"]),
    ("chat_logs", ["chat_id", "message_id", "thread_id", "date_utc", "edit_date_utc", "content_type", "text",
                   "reply_to_message_id", "media_group_id", "is_forward"]),
    ("admins", ["user_id", "name", "added_by", "added_at"]),
    ("relays", ["id", "source_chat_id", "pool_id", "mode", "enabled", "posts_count", "last_post_at", "last_result",
                "created_at"]),
]
IDENTITY_TABLES = ["pools", "tasks", "relays"]


async def migrate(sqlite_path: str, dsn: str, force: bool) -> None:
    if not os.path.exists(sqlite_path):
        raise SystemExit(f"Файл {sqlite_path} не знайдено")
    src = SQLiteDatabase(sqlite_path)
    await src.init()  # оновлює схему старих баз до поточної версії
    dst = PostgresDatabase(dsn)
    await dst.init()
    print(f"Звідки: {os.path.abspath(sqlite_path)}\nКуди:   {mask_dsn(dsn)}\n")
    try:
        existing = (await dst.fetchone("SELECT (SELECT COUNT(*) FROM chats) + (SELECT COUNT(*) FROM pools) AS n"))["n"]
        if existing and not force:
            raise SystemExit("Цільова база не порожня. Запустіть з --force, щоб додати дані поверх (дублікати "
                             "буде пропущено), або очистіть базу.")
        async with dst.pool.acquire() as conn:
            async with conn.transaction():
                for table, cols in TABLES:
                    rows = await src.fetchall(f"SELECT {', '.join(cols)} FROM {table}")
                    if rows:
                        marks = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
                        await conn.executemany(
                            f"INSERT INTO {table}({', '.join(cols)}) VALUES({marks}) ON CONFLICT DO NOTHING",
                            [tuple(r[c] for c in cols) for r in rows])
                    print(f"  {table:<11} {len(rows)}")
                # Лічильники id мають продовжуватися після перенесених значень
                for table in IDENTITY_TABLES:
                    await conn.execute(
                        f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                        f"COALESCE((SELECT MAX(id) FROM {table}), 1), (SELECT MAX(id) FROM {table}) IS NOT NULL)")
        print("\nГотово. Тепер задайте DATABASE_URL для бота й запустіть його.")
    finally:
        await dst.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Перенесення даних бота з SQLite у PostgreSQL")
    parser.add_argument("--sqlite", default=os.getenv("DB_PATH", "bot.db"), help="шлях до bot.db")
    parser.add_argument("--force", action="store_true", help="додати дані в непорожню базу")
    args = parser.parse_args()
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("Задайте DATABASE_URL (рядок підключення до PostgreSQL)")
    asyncio.run(migrate(args.sqlite, url, args.force))