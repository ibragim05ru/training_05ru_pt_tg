import asyncio
import html
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = "@training_05ru_pt"   # например: @pt_training_channel
ADMIN_IDS = {314601893}                 # сюда вставь свой Telegram user id

DB_PATH = Path("questions.db")
RESTORE_WAITING_USERS = set()
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

dp = Dispatcher()


def now_moscow() -> datetime:
    return datetime.now(MOSCOW_TZ)


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            options_json TEXT NOT NULL,
            correct_index INTEGER NOT NULL,
            explanation TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            question_id INTEGER NOT NULL,
            selected_index INTEGER NOT NULL,
            is_correct INTEGER NOT NULL,
            is_counted INTEGER NOT NULL DEFAULT 1,
            answered_at TEXT NOT NULL
        )
    """)

    cur.execute("PRAGMA table_info(answers)")
    columns = [row[1] for row in cur.fetchall()]
    if "is_counted" not in columns:
        cur.execute("ALTER TABLE answers ADD COLUMN is_counted INTEGER NOT NULL DEFAULT 1")

    conn.commit()
    conn.close()


def make_user_link(user_id: int, username: str | None, full_name: str | None) -> str:
    label = html.escape(full_name or (f"@{username}" if username else str(user_id)))
    if username:
        return f"<a href='https://t.me/{html.escape(username)}'>{label}</a>"
    return f"<a href='tg://user?id={user_id}'>{label}</a>"


def extract_user_arg(message: types.Message) -> str | None:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    return parts[1].strip()


def resolve_user_identifier(user_input: str) -> tuple[int | None, str | None, str | None]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if user_input.isdigit():
        cur.execute(
            """
            SELECT user_id, username, full_name
            FROM answers
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(user_input),),
        )
    else:
        username = user_input.lstrip("@")
        cur.execute(
            """
            SELECT user_id, username, full_name
            FROM answers
            WHERE LOWER(COALESCE(username, '')) = LOWER(?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (username,),
        )

    row = cur.fetchone()
    conn.close()

    if not row:
        return None, None, None

    return row[0], row[1], row[2]


def save_question(
    question: str,
    options: list[str],
    correct_index: int,
    explanation: str,
    created_by: int,
) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO questions (question, options_json, correct_index, explanation, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            question,
            json.dumps(options, ensure_ascii=False),
            correct_index,
            explanation,
            created_by,
            now_moscow().isoformat()
        ),
    )
    question_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(question_id)


def get_question(question_id: int) -> dict | None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, question, options_json, correct_index, explanation
        FROM questions
        WHERE id = ?
        """,
        (question_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "question": row[1],
        "options": json.loads(row[2]),
        "correct_index": row[3],
        "explanation": row[4],
    }


def has_counted_answer(user_id: int, question_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM answers
        WHERE user_id = ? AND question_id = ? AND is_counted = 1
        LIMIT 1
        """,
        (user_id, question_id),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def save_answer(
    user_id: int,
    username: str | None,
    full_name: str,
    question_id: int,
    selected_index: int,
    is_correct: bool,
    is_counted: bool,
) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO answers (
            user_id, username, full_name, question_id, selected_index, is_correct, is_counted, answered_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            username,
            full_name,
            question_id,
            selected_index,
            1 if is_correct else 0,
            1 if is_counted else 0,
            now_moscow().isoformat()
        ),
    )
    conn.commit()
    conn.close()


def make_keyboard(question_id: int, options: list[str]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=option, callback_data=f"q:{question_id}:a:{idx}")]
            for idx, option in enumerate(options)
        ]
    )


def get_stats(start_dt: datetime, end_dt: datetime, target_user_id: int | None = None) -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    params = [start_dt.isoformat(), end_dt.isoformat()]
    user_filter = ""
    if target_user_id is not None:
        user_filter = "AND user_id = ?"
        params.append(target_user_id)

    cur.execute(f"""
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1 {user_filter}
    """, params)
    total_answers = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(DISTINCT user_id)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1 {user_filter}
    """, params)
    unique_users = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_correct = 1 AND is_counted = 1 {user_filter}
    """, params)
    correct_answers = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_correct = 0 AND is_counted = 1 {user_filter}
    """, params)
    wrong_answers = cur.fetchone()[0]

    cur.execute(f"""
        SELECT
            user_id,
            COALESCE(username, '') AS username,
            COALESCE(full_name, '') AS full_name,
            COUNT(*) AS answers_count
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1 {user_filter}
        GROUP BY user_id, username, full_name
        ORDER BY answers_count DESC
        LIMIT 10
    """, params)
    top_users = cur.fetchall()

    conn.close()

    return {
        "total_answers": total_answers,
        "unique_users": unique_users,
        "correct_answers": correct_answers,
        "wrong_answers": wrong_answers,
        "top_users": top_users,
    }


def format_stats(title: str, stats: dict) -> str:
    lines = [
        f"📊 {title}",
        "",
        f"Всего ответов: {stats['total_answers']}",
        f"Уникальных пользователей: {stats['unique_users']}",
        f"Правильных ответов: {stats['correct_answers']}",
        f"Неправильных ответов: {stats['wrong_answers']}",
    ]

    if stats["top_users"]:
        lines.append("")
        lines.append("Топ активных:")
        for i, (user_id, username, full_name, answers_count) in enumerate(stats["top_users"], start=1):
            user_link = make_user_link(user_id, username or None, full_name or None)
            lines.append(f"{i}. {user_link} — {answers_count}")
    else:
        lines.append("")
        lines.append("Пока нет данных.")

    return "\n".join(lines)


def get_user_stats(start_dt: datetime, end_dt: datetime, user_id: int) -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    start_str = start_dt.isoformat()
    end_str = end_dt.isoformat()

    cur.execute("""
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ?
          AND is_counted = 1
          AND user_id = ?
    """, (start_str, end_str, user_id))
    total_answers = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ?
          AND is_counted = 1
          AND is_correct = 1
          AND user_id = ?
    """, (start_str, end_str, user_id))
    correct_answers = cur.fetchone()[0]

    wrong_answers = total_answers - correct_answers
    accuracy = round((correct_answers / total_answers) * 100, 1) if total_answers else 0.0

    cur.execute("""
        SELECT
            user_id,
            COALESCE(NULLIF(full_name, ''), username, CAST(user_id AS TEXT)) AS user_label,
            COALESCE(username, '') AS username,
            COUNT(*) AS total_answers,
            SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) AS correct_answers,
            ROUND(
                100.0 * SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) / COUNT(*),
                1
            ) AS accuracy
        FROM answers
        WHERE answered_at >= ? AND answered_at < ?
          AND is_counted = 1
        GROUP BY user_id, username, full_name
        HAVING COUNT(*) > 0
        ORDER BY correct_answers DESC, accuracy DESC, total_answers DESC, user_label ASC
    """, (start_str, end_str))
    leaderboard_rows = cur.fetchall()

    rank = None
    user_label = None
    username = None
    for idx, row in enumerate(leaderboard_rows, start=1):
        row_user_id, row_user_label, row_username, *_ = row
        if row_user_id == user_id:
            rank = idx
            user_label = row_user_label
            username = row_username or None
            break

    if user_label is None:
        cur.execute("""
            SELECT
                COALESCE(NULLIF(full_name, ''), username, CAST(user_id AS TEXT)) AS user_label,
                COALESCE(username, '') AS username
            FROM answers
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        if row:
            user_label, username = row[0], (row[1] or None)
        else:
            user_label, username = str(user_id), None

    conn.close()

    return {
        "user_id": user_id,
        "user_label": user_label,
        "username": username,
        "total_answers": total_answers,
        "correct_answers": correct_answers,
        "wrong_answers": wrong_answers,
        "accuracy": accuracy,
        "rank": rank,
    }


def format_user_stats(title: str, stats: dict) -> str:
    user_link = make_user_link(stats["user_id"], stats["username"], stats["user_label"])
    lines = [
        f"📊 {title}",
        "",
        f"Сотрудник: {user_link}",
        f"Всего ответов: {stats['total_answers']}",
        f"Правильных ответов: {stats['correct_answers']}",
        f"Неправильных ответов: {stats['wrong_answers']}",
        f"Точность: {stats['accuracy']}%",
    ]
    if stats["rank"] is not None:
        lines.append(f"Место в рейтинге: {stats['rank']}")
    return "\n".join(lines)


def get_leaderboard(start_dt: datetime | None = None, end_dt: datetime | None = None) -> list[tuple]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if start_dt and end_dt:
        cur.execute("""
            SELECT
                user_id,
                COALESCE(username, '') AS username,
                COALESCE(full_name, '') AS full_name,
                COUNT(*) AS total_answers,
                SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) AS correct_answers,
                ROUND(
                    100.0 * SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) / COUNT(*),
                    1
                ) AS accuracy
            FROM answers
            WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1
            GROUP BY user_id, username, full_name
            HAVING COUNT(*) > 0
            ORDER BY correct_answers DESC, accuracy DESC, total_answers DESC, full_name ASC
            LIMIT 20
        """, (start_dt.isoformat(), end_dt.isoformat()))
    else:
        cur.execute("""
            SELECT
                user_id,
                COALESCE(username, '') AS username,
                COALESCE(full_name, '') AS full_name,
                COUNT(*) AS total_answers,
                SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) AS correct_answers,
                ROUND(
                    100.0 * SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) / COUNT(*),
                    1
                ) AS accuracy
            FROM answers
            WHERE is_counted = 1
            GROUP BY user_id, username, full_name
            HAVING COUNT(*) > 0
            ORDER BY correct_answers DESC, accuracy DESC, total_answers DESC, full_name ASC
            LIMIT 20
        """)

    rows = cur.fetchall()
    conn.close()
    return rows


def format_leaderboard(title: str, rows: list[tuple]) -> str:
    lines = [f"🏆 {title}", ""]

    if not rows:
        lines.append("Пока нет данных.")
        return "\n".join(lines)

    for i, row in enumerate(rows, start=1):
        user_id, username, full_name, total, correct, acc = row
        user_link = make_user_link(user_id, username or None, full_name or None)
        lines.append(f"{i}. {user_link} — {correct}/{total} ({acc}%)")

    return "\n".join(lines)


def get_question_answer_distribution(question_id: int) -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM answers
        WHERE question_id = ? AND is_counted = 1
    """, (question_id,))
    total_answers = cur.fetchone()[0]

    cur.execute("""
        SELECT selected_index, COUNT(*)
        FROM answers
        WHERE question_id = ? AND is_counted = 1
        GROUP BY selected_index
    """, (question_id,))
    rows = cur.fetchall()
    conn.close()

    distribution = {selected_index: count for selected_index, count in rows}
    return {"total_answers": total_answers, "distribution": distribution}


def get_activity_by_hour(start_dt: datetime, end_dt: datetime, target_user_id: int | None = None) -> list[int]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    params = [start_dt.isoformat(), end_dt.isoformat()]
    user_filter = ""
    if target_user_id is not None:
        user_filter = "AND user_id = ?"
        params.append(target_user_id)

    cur.execute(f"""
        SELECT answered_at
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1 {user_filter}
    """, params)

    buckets = [0] * 24
    for (answered_at,) in cur.fetchall():
        dt = datetime.fromisoformat(answered_at)
        buckets[dt.hour] += 1

    conn.close()
    return buckets


def get_activity_by_day(start_dt: datetime, end_dt: datetime, target_user_id: int | None = None) -> list[tuple[str, int]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    params = [start_dt.isoformat(), end_dt.isoformat()]
    user_filter = ""
    if target_user_id is not None:
        user_filter = "AND user_id = ?"
        params.append(target_user_id)

    cur.execute(f"""
        SELECT answered_at
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1 {user_filter}
    """, params)

    counts: dict[str, int] = {}
    for (answered_at,) in cur.fetchall():
        dt = datetime.fromisoformat(answered_at)
        key = dt.strftime("%d.%m")
        counts[key] = counts.get(key, 0) + 1

    conn.close()

    result = []
    current = start_dt
    while current < end_dt:
        key = current.strftime("%d.%m")
        result.append((key, counts.get(key, 0)))
        current += timedelta(days=1)

    return result


def bar(value: int, max_value: int, width: int = 10) -> str:
    if max_value <= 0:
        return ""
    filled = round((value / max_value) * width)
    return "▓" * filled


def format_activity_today(title: str, data: list[int]) -> str:
    max_value = max(data) if data else 0
    lines = [f"📈 {title}", ""]
    for hour, value in enumerate(data):
        lines.append(f"{hour:02d}:00 — {value} {bar(value, max_value)}")
    return "\n".join(lines)


def format_activity_by_day(title: str, data: list[tuple[str, int]]) -> str:
    max_value = max((value for _, value in data), default=0)
    lines = [f"📈 {title}", ""]
    for label, value in data:
        lines.append(f"{label} — {value} {bar(value, max_value)}")
    return "\n".join(lines)


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    text = (
        "Бот работает.\n\n"
        "Команды:\n"
        "/help — инструкция\n"
        "/publish — опубликовать вопрос в канал\n"
        "/stats_today — статистика за сегодня\n"
        "/stats_yesterday — статистика за вчера\n"
        "/stats_week — статистика за 7 дней\n"
        "/stats_month — статистика за 30 дней\n"
        "/leaderboard_today — рейтинг за сегодня\n"
        "/leaderboard_week — рейтинг за 7 дней\n"
        "/leaderboard_month — рейтинг за 30 дней\n"
        "/leaderboard_all — рейтинг за всё время\n"
        "/activity_today — активность по часам за сегодня\n"
        "/activity_week — активность по дням за 7 дней\n"
        "/activity_month — активность по дням за 30 дней\n"
        "/backup_db — получить резервную копию базы\n"
        "/restore_db — восстановить базу из файла\n\n"
        "Для статистики по человеку можно добавить аргумент:\n"
        "/stats_today @username\n"
        "/stats_week 123456789\n"
        "/activity_week @username"
    )
    await message.answer(text)


@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    text = (
        "Как публиковать вопрос без фото:\n\n"
        "/publish\n"
        "Какой аргумент сильнее при продаже дорогого смартфона?\n"
        "Камера\n"
        "Цена\n"
        "Надёжность\n"
        "3\n"
        "В премиальном сегменте клиент чаще покупает уверенность в качестве и сроке службы.\n\n"
        "Как публиковать вопрос с фото:\n"
        "— прикрепи фото\n"
        "— в подпись вставь тот же текст\n\n"
        "Резервная копия:\n"
        "/backup_db — бот пришлёт файл базы\n"
        "/restore_db — после команды отправь боту файл questions.db\n\n"
        "Персональная статистика:\n"
        "/stats_today @username\n"
        "/stats_week 123456789\n"
        "/activity_month @username"
    )
    await message.answer(text)


@dp.message(Command("backup_db"))
async def cmd_backup_db(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    if not DB_PATH.exists():
        await message.answer("Файл базы пока не найден.")
        return

    await message.answer_document(
        FSInputFile(DB_PATH),
        caption="Резервная копия базы данных"
    )


@dp.message(Command("restore_db"))
async def cmd_restore_db(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    RESTORE_WAITING_USERS.add(message.from_user.id)
    await message.answer(
        "Теперь отправь мне файлом базу questions.db.\n\n"
        "Лучше делать это, когда ботом никто не пользуется."
    )


@dp.message(F.document)
async def handle_restore_document(message: types.Message, bot: Bot) -> None:
    if message.from_user.id not in ADMIN_IDS:
        return

    if message.from_user.id not in RESTORE_WAITING_USERS:
        return

    document = message.document
    if not document:
        await message.answer("Файл не найден.")
        return

    if not document.file_name or not document.file_name.endswith(".db"):
        await message.answer("Нужен файл базы .db")
        return

    temp_path = Path("uploaded_restore.db")

    try:
        file = await bot.get_file(document.file_id)
        await bot.download_file(file.file_path, destination=temp_path)

        test_conn = sqlite3.connect(temp_path)
        test_cur = test_conn.cursor()
        test_cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in test_cur.fetchall()}
        test_conn.close()

        if "questions" not in tables or "answers" not in tables:
            temp_path.unlink(missing_ok=True)
            await message.answer("Это не подходит: в базе нет нужных таблиц.")
            RESTORE_WAITING_USERS.discard(message.from_user.id)
            return

        DB_PATH.unlink(missing_ok=True)
        temp_path.replace(DB_PATH)

        init_db()
        RESTORE_WAITING_USERS.discard(message.from_user.id)
        await message.answer("База успешно восстановлена ✅")

    except Exception as e:
        temp_path.unlink(missing_ok=True)
        RESTORE_WAITING_USERS.discard(message.from_user.id)
        await message.answer(f"Не удалось восстановить базу: {e}")


@dp.message(Command("publish"))
async def cmd_publish(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    text = (message.text or message.caption or "").strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    if len(lines) < 6:
        await message.answer(
            "Неверный формат.\n\n"
            "Пример:\n"
            "/publish\n"
            "Какой аргумент сильнее при продаже дорогого смартфона?\n"
            "Камера\n"
            "Цена\n"
            "Надёжность\n"
            "3\n"
            "Клиент платит за уверенность и срок службы устройства"
        )
        return

    payload = lines[1:]
    question = payload[0]
    explanation = payload[-1]
    options = payload[1:-2]

    try:
        correct_index = int(payload[-2]) - 1
    except ValueError:
        await message.answer("Предпоследняя строка должна быть номером правильного ответа.")
        return

    if len(options) < 2:
        await message.answer("Нужно минимум 2 варианта ответа.")
        return

    if correct_index < 0 or correct_index >= len(options):
        await message.answer("Номер правильного ответа не совпадает с количеством вариантов.")
        return

    question_id = save_question(
        question=question,
        options=options,
        correct_index=correct_index,
        explanation=explanation,
        created_by=message.from_user.id,
    )

    keyboard = make_keyboard(question_id, options)

    if message.photo:
        photo = message.photo[-1].file_id
        await message.bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=photo,
            caption=question,
            reply_markup=keyboard
        )
    else:
        await message.bot.send_message(
            chat_id=CHANNEL_ID,
            text=question,
            reply_markup=keyboard
        )

    await message.answer("Опубликовано ✅")


@dp.callback_query(F.data.startswith("q:"))
async def handle_question_callback(callback: types.CallbackQuery) -> None:
    try:
        _, qid_raw, _, answer_raw = callback.data.split(":")
        question_id = int(qid_raw)
        selected_index = int(answer_raw)
    except Exception:
        await callback.answer("Ошибка данных кнопки.", show_alert=True)
        return

    question = get_question(question_id)
    if not question:
        await callback.answer(
            "Вопрос не найден в базе. Восстанови базу через /restore_db.",
            show_alert=True
        )
        return

    is_correct = selected_index == question["correct_index"]
    is_counted = not has_counted_answer(callback.from_user.id, question_id)

    save_answer(
        user_id=callback.from_user.id,
        username=callback.from_user.username,
        full_name=callback.from_user.full_name,
        question_id=question_id,
        selected_index=selected_index,
        is_correct=is_correct,
        is_counted=is_counted,
    )

    answer_stats = get_question_answer_distribution(question_id)
    total_answers = answer_stats["total_answers"]
    selected_count = answer_stats["distribution"].get(selected_index, 0)
    selected_percent = round((selected_count / total_answers) * 100) if total_answers > 0 else 0

    counted_note = ""
    if not is_counted:
        counted_note = "\n\nℹ️ Повторный ответ не засчитан в статистику и рейтинг."

    if is_correct:
        text = (
            f"✅ Верно\n\n"
            f"{question['explanation']}\n\n"
            f"Так ответили: {selected_percent}%"
            f"{counted_note}"
        )
    else:
        correct_text = question["options"][question["correct_index"]]
        text = (
            f"❌ Неверно\n"
            f"Правильный ответ: {correct_text}\n\n"
            f"{question['explanation']}\n\n"
            f"Так ответили: {selected_percent}%"
            f"{counted_note}"
        )

    await callback.answer(text, show_alert=True)


async def handle_stats_command(message: types.Message, period_name: str, start: datetime, end: datetime) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    user_arg = extract_user_arg(message)
    if user_arg:
        user_id, username, full_name = resolve_user_identifier(user_arg)
        if user_id is None:
            await message.answer("Пользователь не найден в статистике.")
            return

        stats = get_user_stats(start, end, user_id)
        await message.answer(format_user_stats(period_name, stats))
        return

    stats = get_stats(start, end)
    await message.answer(format_stats(period_name, stats))


@dp.message(Command("stats_today"))
async def cmd_stats_today(message: types.Message) -> None:
    now = now_moscow()
    start = datetime(now.year, now.month, now.day, tzinfo=MOSCOW_TZ)
    end = start + timedelta(days=1)
    await handle_stats_command(message, "Статистика за сегодня", start, end)


@dp.message(Command("stats_yesterday"))
async def cmd_stats_yesterday(message: types.Message) -> None:
    now = now_moscow()
    today_start = datetime(now.year, now.month, now.day, tzinfo=MOSCOW_TZ)
    start = today_start - timedelta(days=1)
    end = today_start
    await handle_stats_command(message, "Статистика за вчера", start, end)


@dp.message(Command("stats_week"))
async def cmd_stats_week(message: types.Message) -> None:
    end = now_moscow()
    start = end - timedelta(days=7)
    await handle_stats_command(message, "Статистика за 7 дней", start, end)


@dp.message(Command("stats_month"))
async def cmd_stats_month(message: types.Message) -> None:
    end = now_moscow()
    start = end - timedelta(days=30)
    await handle_stats_command(message, "Статистика за 30 дней", start, end)


@dp.message(Command("leaderboard_today"))
async def cmd_leaderboard_today(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    now = now_moscow()
    start = datetime(now.year, now.month, now.day, tzinfo=MOSCOW_TZ)
    end = start + timedelta(days=1)
    rows = get_leaderboard(start, end)
    await message.answer(format_leaderboard("Рейтинг за сегодня", rows))


@dp.message(Command("leaderboard_week"))
async def cmd_leaderboard_week(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    end = now_moscow()
    start = end - timedelta(days=7)
    rows = get_leaderboard(start, end)
    await message.answer(format_leaderboard("Рейтинг за 7 дней", rows))


@dp.message(Command("leaderboard_month"))
async def cmd_leaderboard_month(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    end = now_moscow()
    start = end - timedelta(days=30)
    rows = get_leaderboard(start, end)
    await message.answer(format_leaderboard("Рейтинг за 30 дней", rows))


@dp.message(Command("leaderboard_all"))
async def cmd_leaderboard_all(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    rows = get_leaderboard()
    await message.answer(format_leaderboard("Рейтинг за всё время", rows))


async def handle_activity_command(message: types.Message, title: str, start: datetime, end: datetime, by_hour: bool) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    user_arg = extract_user_arg(message)
    target_user_id = None

    if user_arg:
        target_user_id, username, full_name = resolve_user_identifier(user_arg)
        if target_user_id is None:
            await message.answer("Пользователь не найден в статистике.")
            return

        suffix = full_name or (f"@{username}" if username else str(target_user_id))
        title = f"{title} — {suffix}"

    if by_hour:
        data = get_activity_by_hour(start, end, target_user_id)
        await message.answer(format_activity_today(title, data))
    else:
        data = get_activity_by_day(start, end, target_user_id)
        await message.answer(format_activity_by_day(title, data))


@dp.message(Command("activity_today"))
async def cmd_activity_today(message: types.Message) -> None:
    now = now_moscow()
    start = datetime(now.year, now.month, now.day, tzinfo=MOSCOW_TZ)
    end = start + timedelta(days=1)
    await handle_activity_command(message, "Активность за сегодня", start, end, by_hour=True)


@dp.message(Command("activity_week"))
async def cmd_activity_week(message: types.Message) -> None:
    end = now_moscow()
    start = end - timedelta(days=7)
    await handle_activity_command(message, "Активность за 7 дней", start, end, by_hour=False)


@dp.message(Command("activity_month"))
async def cmd_activity_month(message: types.Message) -> None:
    end = now_moscow()
    start = end - timedelta(days=30)
    await handle_activity_command(message, "Активность за 30 дней", start, end, by_hour=False)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    if not TOKEN:
        raise ValueError("Переменная окружения TOKEN не задана")

    init_db()

    bot = Bot(
        token=TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    print("Бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
def save_question(
    question: str,
    options: list[str],
    correct_index: int,
    explanation: str,
    created_by: int,
) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO questions (question, options_json, correct_index, explanation, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            question,
            json.dumps(options, ensure_ascii=False),
            correct_index,
            explanation,
            created_by,
            now_moscow().isoformat()
        ),
    )
    question_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(question_id)


def get_question(question_id: int) -> dict | None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, question, options_json, correct_index, explanation
        FROM questions
        WHERE id = ?
        """,
        (question_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "question": row[1],
        "options": json.loads(row[2]),
        "correct_index": row[3],
        "explanation": row[4],
    }


def has_counted_answer(user_id: int, question_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM answers
        WHERE user_id = ? AND question_id = ? AND is_counted = 1
        LIMIT 1
        """,
        (user_id, question_id),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def save_answer(
    user_id: int,
    username: str | None,
    full_name: str,
    question_id: int,
    selected_index: int,
    is_correct: bool,
    is_counted: bool,
) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO answers (
            user_id, username, full_name, question_id, selected_index, is_correct, is_counted, answered_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            username,
            full_name,
            question_id,
            selected_index,
            1 if is_correct else 0,
            1 if is_counted else 0,
            now_moscow().isoformat()
        ),
    )
    conn.commit()
    conn.close()


def make_keyboard(question_id: int, options: list[str]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=option,
                    callback_data=f"q:{question_id}:a:{idx}"
                )
            ]
            for idx, option in enumerate(options)
        ]
    )


def get_stats(start_dt: datetime, end_dt: datetime) -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    start_str = start_dt.isoformat()
    end_str = end_dt.isoformat()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1
        """,
        (start_str, end_str),
    )
    total_answers = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(DISTINCT user_id)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1
        """,
        (start_str, end_str),
    )
    unique_users = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_correct = 1 AND is_counted = 1
        """,
        (start_str, end_str),
    )
    correct_answers = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_correct = 0 AND is_counted = 1
        """,
        (start_str, end_str),
    )
    wrong_answers = cur.fetchone()[0]

    cur.execute(
        """
        SELECT
            COALESCE(NULLIF(full_name, ''), username, CAST(user_id AS TEXT)) AS user_label,
            COUNT(*) as answers_count
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1
        GROUP BY user_id, username, full_name
        ORDER BY answers_count DESC
        LIMIT 10
        """,
        (start_str, end_str),
    )
    top_users = cur.fetchall()

    conn.close()

    return {
        "total_answers": total_answers,
        "unique_users": unique_users,
        "correct_answers": correct_answers,
        "wrong_answers": wrong_answers,
        "top_users": top_users,
    }


def format_stats(title: str, stats: dict) -> str:
    lines = [
        f"📊 {title}",
        "",
        f"Всего ответов: {stats['total_answers']}",
        f"Уникальных пользователей: {stats['unique_users']}",
        f"Правильных ответов: {stats['correct_answers']}",
        f"Неправильных ответов: {stats['wrong_answers']}",
    ]

    if stats["top_users"]:
        lines.append("")
        lines.append("Топ активных:")
        for i, (user_label, answers_count) in enumerate(stats["top_users"], start=1):
            lines.append(f"{i}. {user_label} — {answers_count}")
    else:
        lines.append("")
        lines.append("Пока нет данных.")

    return "\n".join(lines)


def get_leaderboard(start_dt: datetime | None = None, end_dt: datetime | None = None) -> list[tuple]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if start_dt and end_dt:
        cur.execute(
            """
            SELECT
                user_id,
                COALESCE(NULLIF(full_name, ''), username, CAST(user_id AS TEXT)) AS user_label,
                COUNT(*) AS total_answers,
                SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) AS correct_answers,
                ROUND(
                    100.0 * SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) / COUNT(*),
                    1
                ) AS accuracy
            FROM answers
            WHERE answered_at >= ? AND answered_at < ? AND is_counted = 1
            GROUP BY user_id, username, full_name
            HAVING COUNT(*) > 0
            ORDER BY correct_answers DESC, accuracy DESC, total_answers DESC, user_label ASC
            LIMIT 20
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
    else:
        cur.execute(
            """
            SELECT
                user_id,
                COALESCE(NULLIF(full_name, ''), username, CAST(user_id AS TEXT)) AS user_label,
                COUNT(*) AS total_answers,
                SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) AS correct_answers,
                ROUND(
                    100.0 * SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) / COUNT(*),
                    1
                ) AS accuracy
            FROM answers
            WHERE is_counted = 1
            GROUP BY user_id, username, full_name
            HAVING COUNT(*) > 0
            ORDER BY correct_answers DESC, accuracy DESC, total_answers DESC, user_label ASC
            LIMIT 20
            """
        )

    rows = cur.fetchall()
    conn.close()
    return rows


def format_leaderboard(title: str, rows: list[tuple]) -> str:
    lines = [f"🏆 {title}", ""]

    if not rows:
        lines.append("Пока нет данных.")
        return "\n".join(lines)

    for idx, (_, user_label, total_answers, correct_answers, accuracy) in enumerate(rows, start=1):
        lines.append(
            f"{idx}. {user_label} — {correct_answers}/{total_answers} "
            f"(точность {accuracy}%)"
        )

    return "\n".join(lines)


def get_question_answer_distribution(question_id: int) -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM answers
        WHERE question_id = ? AND is_counted = 1
        """,
        (question_id,),
    )
    total_answers = cur.fetchone()[0]

    cur.execute(
        """
        SELECT selected_index, COUNT(*)
        FROM answers
        WHERE question_id = ? AND is_counted = 1
        GROUP BY selected_index
        """,
        (question_id,),
    )
    rows = cur.fetchall()
    conn.close()

    distribution = {selected_index: count for selected_index, count in rows}
    return {
        "total_answers": total_answers,
        "distribution": distribution,
    }


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    text = (
        "Бот работает.\n\n"
        "Команды:\n"
        "/help — инструкция\n"
        "/publish — опубликовать вопрос в канал\n"
        "/stats_today — статистика за сегодня\n"
        "/stats_yesterday — статистика за вчера\n"
        "/stats_week — статистика за 7 дней\n"
        "/stats_month — статистика за 30 дней\n"
        "/leaderboard_today — рейтинг за сегодня\n"
        "/leaderboard_week — рейтинг за 7 дней\n"
        "/leaderboard_month — рейтинг за 30 дней\n"
        "/leaderboard_all — рейтинг за всё время"
    )
    await message.answer(text)


@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    text = (
        "Как публиковать вопрос без фото:\n\n"
        "/publish\n"
        "Какой аргумент сильнее при продаже дорогого смартфона?\n"
        "Камера\n"
        "Цена\n"
        "Надёжность\n"
        "3\n"
        "В премиальном сегменте клиент чаще покупает уверенность в качестве и сроке службы.\n\n"
        "Как публиковать вопрос с фото:\n"
        "— прикрепи фото\n"
        "— в подпись вставь тот же текст\n\n"
        "Правила:\n"
        "— первая строка: /publish\n"
        "— вторая строка: вопрос\n"
        "— потом варианты ответа\n"
        "— предпоследняя строка: номер правильного ответа\n"
        "— последняя строка: объяснение"
    )
    await message.answer(text)


@dp.message(Command("publish"))
async def cmd_publish(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    text = (message.text or message.caption or "").strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    if len(lines) < 6:
        await message.answer(
            "Неверный формат.\n\n"
            "Пример:\n"
            "/publish\n"
            "Какой аргумент сильнее при продаже дорогого смартфона?\n"
            "Камера\n"
            "Цена\n"
            "Надёжность\n"
            "3\n"
            "Клиент платит за уверенность и срок службы устройства"
        )
        return

    payload = lines[1:]
    question = payload[0]
    explanation = payload[-1]
    options = payload[1:-2]

    try:
        correct_index = int(payload[-2]) - 1
    except ValueError:
        await message.answer("Предпоследняя строка должна быть номером правильного ответа.")
        return

    if len(options) < 2:
        await message.answer("Нужно минимум 2 варианта ответа.")
        return

    if correct_index < 0 or correct_index >= len(options):
        await message.answer("Номер правильного ответа не совпадает с количеством вариантов.")
        return

    question_id = save_question(
        question=question,
        options=options,
        correct_index=correct_index,
        explanation=explanation,
        created_by=message.from_user.id,
    )

    keyboard = make_keyboard(question_id, options)

    if message.photo:
        photo = message.photo[-1].file_id
        await message.bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=photo,
            caption=question,
            reply_markup=keyboard
        )
    else:
        await message.bot.send_message(
            chat_id=CHANNEL_ID,
            text=question,
            reply_markup=keyboard
        )

    await message.answer("Опубликовано ✅")


@dp.callback_query(F.data.startswith("q:"))
async def handle_question_callback(callback: types.CallbackQuery) -> None:
    try:
        _, qid_raw, _, answer_raw = callback.data.split(":")
        question_id = int(qid_raw)
        selected_index = int(answer_raw)
    except Exception:
        await callback.answer("Ошибка данных кнопки.", show_alert=True)
        return

    question = get_question(question_id)
    if not question:
        await callback.answer("Вопрос не найден.", show_alert=True)
        return

    is_correct = selected_index == question["correct_index"]
    is_counted = not has_counted_answer(callback.from_user.id, question_id)

    save_answer(
        user_id=callback.from_user.id,
        username=callback.from_user.username,
        full_name=callback.from_user.full_name,
        question_id=question_id,
        selected_index=selected_index,
        is_correct=is_correct,
        is_counted=is_counted,
    )

    answer_stats = get_question_answer_distribution(question_id)
    total_answers = answer_stats["total_answers"]
    selected_count = answer_stats["distribution"].get(selected_index, 0)

    if total_answers > 0:
        selected_percent = round((selected_count / total_answers) * 100)
    else:
        selected_percent = 0

    counted_note = ""
    if not is_counted:
        counted_note = "\n\nℹ️ Повторный ответ не засчитан в статистику и рейтинг."

    if is_correct:
        text = (
            f"✅ Верно\n\n"
            f"{question['explanation']}\n\n"
            f"Так ответили: {selected_percent}%"
            f"{counted_note}"
        )
    else:
        correct_text = question["options"][question["correct_index"]]
        text = (
            f"❌ Неверно\n"
            f"Правильный ответ: {correct_text}\n\n"
            f"{question['explanation']}\n\n"
            f"Так ответили: {selected_percent}%"
            f"{counted_note}"
        )

    await callback.answer(text, show_alert=True)


@dp.message(Command("stats_today"))
async def cmd_stats_today(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    now = now_moscow()
    start = datetime(now.year, now.month, now.day, tzinfo=MOSCOW_TZ)
    end = start + timedelta(days=1)

    stats = get_stats(start, end)
    await message.answer(format_stats("Статистика за сегодня", stats))


@dp.message(Command("stats_yesterday"))
async def cmd_stats_yesterday(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    now = now_moscow()
    today_start = datetime(now.year, now.month, now.day, tzinfo=MOSCOW_TZ)
    start = today_start - timedelta(days=1)
    end = today_start

    stats = get_stats(start, end)
    await message.answer(format_stats("Статистика за вчера", stats))


@dp.message(Command("stats_week"))
async def cmd_stats_week(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    end = now_moscow()
    start = end - timedelta(days=7)

    stats = get_stats(start, end)
    await message.answer(format_stats("Статистика за 7 дней", stats))


@dp.message(Command("stats_month"))
async def cmd_stats_month(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    end = now_moscow()
    start = end - timedelta(days=30)

    stats = get_stats(start, end)
    await message.answer(format_stats("Статистика за 30 дней", stats))


@dp.message(Command("leaderboard_today"))
async def cmd_leaderboard_today(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    now = now_moscow()
    start = datetime(now.year, now.month, now.day, tzinfo=MOSCOW_TZ)
    end = start + timedelta(days=1)

    rows = get_leaderboard(start, end)
    await message.answer(format_leaderboard("Рейтинг за сегодня", rows))


@dp.message(Command("leaderboard_week"))
async def cmd_leaderboard_week(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    end = now_moscow()
    start = end - timedelta(days=7)

    rows = get_leaderboard(start, end)
    await message.answer(format_leaderboard("Рейтинг за 7 дней", rows))


@dp.message(Command("leaderboard_month"))
async def cmd_leaderboard_month(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    end = now_moscow()
    start = end - timedelta(days=30)

    rows = get_leaderboard(start, end)
    await message.answer(format_leaderboard("Рейтинг за 30 дней", rows))


@dp.message(Command("leaderboard_all"))
async def cmd_leaderboard_all(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У тебя нет доступа к этой команде.")
        return

    rows = get_leaderboard()
    await message.answer(format_leaderboard("Рейтинг за всё время", rows))


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    if not TOKEN:
        raise ValueError("Переменная окружения TOKEN не задана")

    init_db()

    bot = Bot(token=TOKEN)
    print("Бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
