import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = "@training_05ru_pt"   # например: @pt_training_channel
ADMIN_IDS = {314601893}                 # сюда вставь свой Telegram user id

DB_PATH = Path("questions.db")
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
            answered_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


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


def save_answer(
    user_id: int,
    username: str | None,
    full_name: str,
    question_id: int,
    selected_index: int,
    is_correct: bool,
) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO answers (
            user_id, username, full_name, question_id, selected_index, is_correct, answered_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            username,
            full_name,
            question_id,
            selected_index,
            1 if is_correct else 0,
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
        WHERE answered_at >= ? AND answered_at < ?
        """,
        (start_str, end_str),
    )
    total_answers = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(DISTINCT user_id)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ?
        """,
        (start_str, end_str),
    )
    unique_users = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_correct = 1
        """,
        (start_str, end_str),
    )
    correct_answers = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM answers
        WHERE answered_at >= ? AND answered_at < ? AND is_correct = 0
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
        WHERE answered_at >= ? AND answered_at < ?
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
            WHERE answered_at >= ? AND answered_at < ?
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
        WHERE question_id = ?
        """,
        (question_id,),
    )
    total_answers = cur.fetchone()[0]

    cur.execute(
        """
        SELECT selected_index, COUNT(*)
        FROM answers
        WHERE question_id = ?
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

    save_answer(
        user_id=callback.from_user.id,
        username=callback.from_user.username,
        full_name=callback.from_user.full_name,
        question_id=question_id,
        selected_index=selected_index,
        is_correct=is_correct,
    )

    answer_stats = get_question_answer_distribution(question_id)
    total_answers = answer_stats["total_answers"]
    selected_count = answer_stats["distribution"].get(selected_index, 0)

    if total_answers > 0:
        selected_percent = round((selected_count / total_answers) * 100)
    else:
        selected_percent = 0

    if is_correct:
        text = (
            f"✅ Верно\n\n"
            f"{question['explanation']}\n\n"
            f"Так ответили: {selected_percent}%"
        )
    else:
        correct_text = question["options"][question["correct_index"]]
        text = (
            f"❌ Неверно\n"
            f"Правильный ответ: {correct_text}\n\n"
            f"{question['explanation']}\n\n"
            f"Так ответили: {selected_percent}%"
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
