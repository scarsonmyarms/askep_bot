"""
Масове додавання бота розсилок у ваші групи й канали від імені ВАШОГО акаунта Telegram.

Як це працює:
  1. Скрипт входить у ваш акаунт через Telegram API (Telethon).
  2. Показує групи й канали, куди ви можете додати бота і де його ще немає.
  3. Ви обираєте потрібні (номери, діапазони або all) — і скрипт по черзі додає бота:
       • канали          — адміністратором з правом публікації (потрібне ваше право «Додавати адміністраторів»);
       • супергрупи      — учасником (якщо ви адмін із правом запрошувати або учасникам дозволено запрошувати);
       • звичайні групи  — учасником (так само).
  4. Якщо в боті увімкнено «Режим прийому» для пулу — усі ці чати автоматично потраплять у пул.

Діагностика:
  python add_bot_bulk.py --diagnose
  Показує ВСІ ваші групи й канали з причиною, чому чат є або немає в списку, а також ID чатів,
  де бот уже є — їх можна вставити в боті: «💬 Чати» → «➕ Зареєструвати вручну».

Важливо:
  • Акаунт, від імені якого працює скрипт, має бути власником або адміністратором бота (інакше бот вийде з чатів).
  • Файл сесії (*.session) дає повний доступ до акаунта — зберігайте його як пароль, не передавайте.
  • Telegram обмежує масові дії. Не зменшуйте затримку без потреби: при занадто частих запитах
    акаунт може отримати тимчасові обмеження.

Запуск:
  pip install -r requirements-userbot.txt
  python add_bot_bulk.py              # інтерактивний вибір
  python add_bot_bulk.py --dry-run    # лише показати, що буде зроблено
  python add_bot_bulk.py --diagnose   # причини видимості кожного чату + ID чатів, де бот уже є
"""

import argparse
import asyncio
import os
import random

from dotenv import load_dotenv
from telethon import TelegramClient, errors, functions, types, utils

load_dotenv()

API_ID = os.getenv("TG_API_ID", "").strip()
API_HASH = os.getenv("TG_API_HASH", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")
SESSION = os.getenv("TG_SESSION", "userbot")
DELAY = float(os.getenv("BULK_DELAY", "15"))  # базова пауза між чатами, сек


def is_broadcast(entity) -> bool:
    return isinstance(entity, types.Channel) and bool(entity.broadcast)


def kind_of(entity) -> str:
    if isinstance(entity, types.Channel):
        return "канал" if entity.broadcast else "супергрупа"
    return "група"


def eligibility(entity) -> tuple[bool, str]:
    """Чи можна додати бота в чат і чому."""
    if getattr(entity, "left", False):
        return False, "ви вийшли з чату"
    if isinstance(entity, types.Chat) and getattr(entity, "deactivated", False):
        return False, "стару групу перетворено на супергрупу (шукайте її нову версію в списку)"
    if getattr(entity, "creator", False):
        return True, "ви власник"
    rights = getattr(entity, "admin_rights", None)
    if is_broadcast(entity):
        if rights and rights.add_admins:
            return True, "ви адмін із правом додавати адміністраторів"
        if rights:
            return False, "ви адмін каналу, але без права «Додавати адміністраторів»"
        return False, "у каналі ви не адміністратор"
    if rights and (rights.invite_users or rights.add_admins):
        return True, "ви адмін із правом запрошувати"
    banned = getattr(entity, "default_banned_rights", None)
    if banned is None or not banned.invite_users:
        return True, "учасникам дозволено запрошувати"
    return False, "немає права додавати учасників (ні адмін, ні дозволу для учасників)"


async def bot_membership(client: TelegramClient, entity, bot) -> bool | None:
    """True — бот уже в чаті, False — немає, None — перевірити не вдалося (спробуємо додати)."""
    for _ in range(2):
        try:
            if isinstance(entity, types.Channel):
                await client(functions.channels.GetParticipantRequest(entity, bot))
                return True
            full = await client(functions.messages.GetFullChatRequest(entity.id))
            participants = getattr(full.full_chat.participants, "participants", [])
            return any(p.user_id == bot.user_id for p in participants)
        except errors.UserNotParticipantError:
            return False
        except errors.FloodWaitError as e:
            print(f"  ⏳ Telegram просить зачекати {e.seconds} с…")
            await asyncio.sleep(e.seconds + 2)
        except errors.RPCError:
            # Напр., у супергрупі без прав адміна список учасників може бути недоступний
            return None
    return None


async def add_bot(client: TelegramClient, entity, bot) -> None:
    if is_broadcast(entity):
        await client(functions.channels.EditAdminRequest(
            channel=entity, user_id=bot,
            admin_rights=types.ChatAdminRights(post_messages=True),
            rank="",
        ))
    elif isinstance(entity, types.Channel):
        await client(functions.channels.InviteToChannelRequest(channel=entity, users=[bot]))
    else:
        await client(functions.messages.AddChatUserRequest(chat_id=entity.id, user_id=bot, fwd_limit=0))


def parse_selection(raw: str, total: int) -> list[int]:
    raw = raw.strip().lower()
    if raw in ("all", "усі", "все"):
        return list(range(total))
    result = set()
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            result.update(range(int(a) - 1, int(b)))
        else:
            result.add(int(part) - 1)
    return sorted(i for i in result if 0 <= i < total)


async def scan(client: TelegramClient, bot, diagnose: bool):
    """Повертає (кандидати, чати з ботом). У режиму діагностики друкує причину для кожного чату."""
    candidates, with_bot = [], []
    async for dialog in client.iter_dialogs():  # включно з архівом і папками
        entity = dialog.entity
        if not isinstance(entity, (types.Chat, types.Channel)):
            if diagnose and isinstance(entity, (types.ChatForbidden, types.ChannelForbidden)):
                print(f"  ⛔ [{dialog.name}] — вас видалено або чат недоступний")
            continue
        ok, reason = eligibility(entity)
        membership = await bot_membership(client, entity, bot) if not getattr(entity, "left", False) else None
        label = f"[{kind_of(entity)}] {entity.title}"
        if membership:
            with_bot.append(entity)
            if diagnose:
                print(f"  🤖 {label} — бот уже є (ID {utils.get_peer_id(entity)})")
            continue
        if ok:
            candidates.append((entity, membership is None))
            if diagnose:
                note = " (наявність бота перевірити не вдалося)" if membership is None else ""
                print(f"  ✅ {label} — можна додати: {reason}{note}")
        elif diagnose:
            print(f"  ⛔ {label} — {reason}")
    return candidates, with_bot


async def main(dry_run: bool, diagnose: bool) -> None:
    if not (API_ID and API_HASH and BOT_USERNAME):
        raise SystemExit("Заповніть TG_API_ID, TG_API_HASH і BOT_USERNAME у .env (див. README)")

    client = TelegramClient(SESSION, int(API_ID), API_HASH)
    await client.start()  # при першому запуску попросить номер телефону й код з Telegram
    me = await client.get_me()
    bot = await client.get_input_entity(BOT_USERNAME)
    print(f"Увійшли як {me.first_name} (ID {me.id}). Бот: @{BOT_USERNAME}\n")
    print("Сканую ваші групи й канали (включно з архівом)…\n")

    candidates, with_bot = await scan(client, bot, diagnose)

    if with_bot:
        ids = ", ".join(str(utils.get_peer_id(e)) for e in with_bot)
        print(f"\n🤖 Бот уже є в {len(with_bot)} чатах. Якщо якихось із них немає в списку бота —\n"
              f"вставте ці ID у боті: «💬 Чати» → «➕ Зареєструвати вручну»:\n{ids}\n")

    if diagnose:
        print(f"Можна додати: {len(candidates)}. Діагностику завершено, нічого не змінено.")
        await client.disconnect()
        return

    if not candidates:
        print("Немає чатів, куди можна додати бота. Запустіть з --diagnose, щоб побачити причини.")
        await client.disconnect()
        return

    print(f"Можна додати бота в {len(candidates)} чатів:\n")
    for i, (entity, unknown) in enumerate(candidates, 1):
        print(f"  {i:>3}. [{kind_of(entity)}] {entity.title}{'  (?) можливо, бот уже є' if unknown else ''}")
    print("\nНемає потрібного чату? Запустіть з --diagnose — скрипт покаже причину для кожного.")

    raw = input("\nОберіть чати: номери через кому, діапазони (1-10) або all. Порожньо — вихід: ")
    selected = [candidates[i][0] for i in parse_selection(raw, len(candidates))] if raw.strip() else []
    if not selected:
        print("Нічого не обрано.")
        await client.disconnect()
        return

    print(f"\nОбрано {len(selected)} чатів. Орієнтовний час: ~{len(selected) * DELAY / 60:.0f} хв.")
    if dry_run:
        print("Режим --dry-run: нічого не змінено.")
        await client.disconnect()
        return
    print("Переконайтеся, що в боті увімкнено «🤖 Масове додавання → Режим прийому» для потрібного пулу.")
    if input("Почати? (так/ні): ").strip().lower() not in ("так", "yes", "y", "т"):
        print("Скасовано.")
        await client.disconnect()
        return

    ok, failed = 0, []
    for n, entity in enumerate(selected, 1):
        while True:
            try:
                await add_bot(client, entity, bot)
                ok += 1
                print(f"  ✅ {n}/{len(selected)} {entity.title}")
                break
            except errors.UserAlreadyParticipantError:
                ok += 1
                print(f"  ✅ {n}/{len(selected)} {entity.title} — бот уже був у чаті")
                break
            except errors.FloodWaitError as e:
                print(f"  ⏳ Telegram просить зачекати {e.seconds} с…")
                await asyncio.sleep(e.seconds + 5)
            except errors.RPCError as e:
                failed.append((entity.title, e.__class__.__name__))
                print(f"  ❌ {n}/{len(selected)} {entity.title}: {e.__class__.__name__}")
                break
        if n < len(selected):
            await asyncio.sleep(DELAY + random.uniform(0, DELAY / 3))

    print(f"\nГотово: успішно {ok}, помилок {len(failed)}.")
    if failed:
        print("Не вдалося додати:")
        for title, err in failed:
            print(f"  • {title}: {err}")
        print("\nПоширені причини: BotGroupsBlockedError — у @BotFather вимкнено «Allow Groups»; "
              "ChatAdminRequiredError — недостатньо прав; UserBannedInChannelError — бота заблоковано в чаті; "
              "UserPrivacyRestrictedError / UserNotMutualContactError — обмеження на запрошення в цьому чаті.")
    await client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Масове додавання бота в групи й канали")
    parser.add_argument("--dry-run", action="store_true", help="лише показати список, нічого не змінювати")
    parser.add_argument("--diagnose", action="store_true",
                        help="показати причину видимості кожного чату та ID чатів, де бот уже є")
    args = parser.parse_args()
    asyncio.run(main(args.dry_run, args.diagnose))