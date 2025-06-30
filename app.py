import os, json, requests
import pandas as pd
from tqdm import tqdm
from zipfile import ZipFile, ZIP_DEFLATED
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Thread
from telegram import Update, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from flask import Flask

# Flask app for health check
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "✅ NFT Bot is running!"

# Telegram bot logic
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ACCESS_KEY = os.getenv("ACCESS_KEY")
AUTHORIZED_USERS_FILE = "authorized_users.json"

if os.path.exists(AUTHORIZED_USERS_FILE):
    with open(AUTHORIZED_USERS_FILE, "r") as f:
        authorized_users = set(json.load(f))
else:
    authorized_users = set()

def save_auth_users():
    with open(AUTHORIZED_USERS_FILE, "w") as f:
        json.dump(list(authorized_users), f)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Welcome! Use /auth <key> to access this bot.")

async def auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in authorized_users:
        await update.message.reply_text("✅ You are already authorized.")
        return
    if len(context.args) == 0:
        await update.message.reply_text("❗Usage: /auth <access_key>")
        return
    if context.args[0] == ACCESS_KEY:
        authorized_users.add(user_id)
        save_auth_users()
        await update.message.reply_text("✅ Access granted!")
    else:
        await update.message.reply_text("❌ Invalid key.")

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in authorized_users:
        await update.message.reply_text(
            "🚫 You are not authorized. Use /auth <key> first."
        )
        return

    await update.message.reply_text("✅ The /analyze command is working. You can now paste your logic here or use the full analyze code.")

# Start bot in background
def run_bot():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("auth", auth))
    app.add_handler(CommandHandler("analyze", analyze))
    app.run_polling()

if __name__ == "__main__":
    Thread(target=run_bot).start()
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
