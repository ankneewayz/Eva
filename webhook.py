import os
import httpx
import urllib.parse
from collections import deque
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

START_IMAGE = "https://share.google/6NlfxAv4tsM0mfnHs"

START_CAPTION = (
    "𝙷𝚎𝚢. 𝙸’𝚖 𝚐𝚕𝚊𝚍 𝚢𝚘𝚞’𝚛𝚎 𝚑𝚎𝚛𝚎 ✨\n"
    "𝙰 𝚕𝚒𝚝𝚝𝚕𝚎 𝚌𝚘𝚗𝚟𝚎𝚛𝚜𝚊𝚝𝚒𝚘𝚗, 𝚊 𝚕𝚒𝚝𝚝𝚕𝚎 𝚙𝚕𝚊𝚢 — 𝚗𝚘𝚝𝚑𝚒𝚗𝚐 𝚛𝚞𝚜𝚑𝚎𝚍.\n\n"
    "𝙴𝚟𝚎𝚛𝚢𝚝𝚑𝚒𝚗𝚐 𝚋𝚎𝚕𝚘𝚠 𝚒𝚜 𝚜𝚒𝚖𝚙𝚕𝚎.\n"
    "𝙹𝚞𝚜𝚝 𝚌𝚑𝚘𝚘𝚜𝚎 𝚠𝚑𝚊𝚝 𝚢𝚘𝚞 𝚏𝚎𝚎𝚕 𝚕𝚒𝚔𝚎 𝚍𝚘𝚒𝚗𝚐.\n\n"
    "───\n"
    "👑 𝙾𝚠𝚗𝚎𝚛𝚜\n"
    "@ankneewayz\n"
    "@dhruvmarothia"
)

# ---------------- EVA PERSONALITY ----------------
SYSTEM_PROMPT = (
    "Your name is Eva.\n"
    "You are a calm, mature female presence.\n"
    "You speak English only.\n"
    "Soft, composed, quietly confident.\n"
    "Short, natural replies.\n"
    "Never say you are an AI, bot, or assistant.\n"
    "You don’t overexplain. You don’t rush.\n"
)

# ---------------- SHORT MEMORY (5–10 MESSAGES) ----------------
MEMORY_LIMIT = 10
memory = deque(maxlen=MEMORY_LIMIT)

# ---------------- GAME PROMPTS ----------------
GAME_PROMPTS = {
    "tod": "You are Eva playing Truth or Dare. Ask: Truth or Dare? One only. Safe.",
    "guess": "You are Eva playing Guess the Number between 1 and 100.",
    "riddle": "You are Eva, a riddle master. One riddle at a time.",
    "quiz": "You are Eva hosting a quiz. One MCQ at a time."
}

# ---------------- APP ----------------
app = Application.builder().token(BOT_TOKEN).build()

# ---------------- START ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🎮 Play Games", callback_data="games")],
        [InlineKeyboardButton("🎨 Generate Image", callback_data="image_help")],
        [InlineKeyboardButton("📜 Commands", callback_data="commands")]
    ]

    await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=START_IMAGE,
        caption=START_CAPTION,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ---------------- BUTTONS ----------------
async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "games":
        await query.message.reply_text(
            "/game tod\n"
            "/game guess\n"
            "/game riddle\n"
            "/game quiz"
        )

    elif query.data == "image_help":
        await query.message.reply_text(
            "Use:\n"
            "/image <description>\n\n"
            "Example:\n"
            "/image calm mature woman, soft light"
        )

    elif query.data == "commands":
        await query.message.reply_text(
            "/start – Welcome\n"
            "/image <prompt> – Generate image\n"
            "/game tod – Truth or Dare\n"
            "/game guess – Guess the Number\n"
            "/game riddle – Riddle\n"
            "/game quiz – Quiz"
        )

# ---------------- IMAGE GENERATOR ----------------
async def image_gen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Tell me what to generate.\nExample:\n/image calm woman, soft light"
        )
        return

    prompt = " ".join(context.args)
    encoded = urllib.parse.quote(prompt)
    image_url = f"https://image.pollinations.ai/prompt/{encoded}"

    keyboard = [
        [InlineKeyboardButton("🔄 Regenerate", callback_data=f"regen::{encoded}")]
    ]

    await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=image_url,
        caption="Here ✨",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def regenerate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    _, encoded = query.data.split("::", 1)
    image_url = f"https://image.pollinations.ai/prompt/{encoded}"

    await query.message.reply_photo(
        photo=image_url,
        caption="Another version ✨",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔄 Regenerate", callback_data=query.data)]]
        )
    )

# ---------------- CHAT ----------------
async def chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    # detect game command
    prompt = SYSTEM_PROMPT
    for g in GAME_PROMPTS:
        if f"/game {g}" in text.lower():
            prompt = GAME_PROMPTS[g]
            break

    memory.append({"role": "user", "content": text})

    messages = [{"role": "system", "content": prompt}]
    messages.extend(memory)

    async with httpx.AsyncClient(timeout=8) as client:
        res = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama3-70b-8192",
                "messages": messages,
                "temperature": 0.6,
                "max_tokens": 400
            }
        )

    reply = res.json()["choices"][0]["message"]["content"]
    memory.append({"role": "assistant", "content": reply})
    await update.message.reply_text(reply)

# ---------------- HANDLERS ----------------
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("image", image_gen))
app.add_handler(CallbackQueryHandler(buttons))
app.add_handler(CallbackQueryHandler(regenerate, pattern="^regen::"))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat))

# ---------------- WEBHOOK ----------------
async def handler(request):
    data = await request.json()
    update = Update.de_json(data, app.bot)
    await app.process_update(update)
    return {"ok": True}