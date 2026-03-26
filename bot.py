import asyncio
import json
import logging
import os
import sqlite3
from pathlib import Path

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

TOKEN = "8726620490:AAGuZhJr0sCP2L9m_T0ZpRdi1qDe-Ab8pIc"
CHANNEL_ID = "@training_05ru_pt"   # например @pt_training_channel
ADMIN_IDS = {314601893}                 # сюда вставь свой Telegram user id

DB_PATH = Path("questions.db")

dp = Dispatcher()


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
            created_by INTEGER NOT NULL
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
        INSERT INTO questions (question, options_json, correct_index, explanation, created_by)
        VALUES (?, ?, ?, ?, ?)
        """,
        (question, json.dumps(options, ensure_ascii=False), correct_index, explanation, created_by),
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


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    text = (
        "Бот работает.\n\n"
        "Команды:\n"
        "/help — показать инструкцию\n"
        "/publish — опубликовать вопрос в канал\n\n"
        "Формат команды:\n"
        "/publish\n"
        "Вопрос\n"
        "Вариант 1\n"
        "Вариант 2\n"
        "Вариант 3\n"
        "1\n"
        "Объяснение\n\n"
        "Где:\n"
        "— первые строки после команды: вопрос и варианты\n"
        "— предпоследняя строка: номер правильного ответа\n"
        "— последняя строка: объяснение"
    )
    await message.answer(text)


@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    text = (
        "Как публиковать вопрос в канал:\n\n"
        "Отправь боту сообщение строго в таком виде:\n\n"
        "/publish\n"
        "Какой аргумент сильнее при продаже дорогого смартфона?\n"
        "Камера\n"
        "Цена\n"
        "Надёжность\n"
        "3\n"
        "В премиальном сегменте клиент чаще покупает уверенность в качестве и сроке службы.\n\n"
        "Правила:\n"
        "— минимум 2 варианта\n"
        "— номер правильного ответа считается с 1\n"
        "— объяснение короткое, лучше до 200 символов"
    )
    await message.answer(text)


@dp.message(Command("publish"))
async def cmd_publish(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа")
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
        correct = int(payload[-2]) - 1
    except ValueError:
        await message.answer("Предпоследняя строка должна быть номером правильного ответа.")
        return

    if len(options) < 2:
        await message.answer("Нужно минимум 2 варианта ответа.")
        return

    if correct < 0 or correct >= len(options):
        await message.answer("Номер правильного ответа не совпадает с количеством вариантов.")
        return

    question_id = save_question(
        question,
        options,
        correct,
        explanation,
        message.from_user.id
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

    if selected_index == question["correct_index"]:
        text = f"✅ Верно\n\n{question['explanation']}"
    else:
        correct_text = question["options"][question["correct_index"]]
        text = f"❌ Неверно\nПравильный ответ: {correct_text}\n\n{question['explanation']}"

    await callback.answer(text, show_alert=True)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    init_db()

    bot = Bot(token=TOKEN)
    print("Бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())