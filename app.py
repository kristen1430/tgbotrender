import os
import json
import asyncio
import requests
import pandas as pd
from tqdm import tqdm
from zipfile import ZipFile, ZIP_DEFLATED
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from telegram import Update, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from flask import Flask

# Get environment variables
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ACCESS_KEY = os.getenv("ACCESS_KEY")
AUTHORIZED_USERS_FILE = "authorized_users.json"

# Load or initialize authorized users
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
        await update.message.reply_text("🚫 You are not authorized. Use /auth <key> first.")
        return

    if len(context.args) != 3:
        await update.message.reply_text("❗Usage: /analyze <CID> <start_id> <end_id>")
        return

    cid, start_id, end_id = context.args
    try:
        start = int(start_id)
        end = int(end_id)
    except:
        await update.message.reply_text("⚠️ Start and End IDs must be integers.")
        return

    await update.message.reply_text("🔄 Fetching metadata... Please wait.")

    gateways = [
        "https://ipfs.io/ipfs/",
        "https://cloudflare-ipfs.com/ipfs/",
        "https://gateway.pinata.cloud/ipfs/"
    ]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir = os.getcwd()
    output_csv = os.path.join(base_dir, f"rarity_report_{timestamp}.csv")
    output_zip = os.path.join(base_dir, f"nft_metadata_{timestamp}.zip")
    jsons = {}
    trait_counter = defaultdict(Counter)

    def fetch_metadata(token_id):
        for gw in gateways:
            for suffix in ["", ".json"]:
                try:
                    url = f"{gw}{cid}/{token_id}{suffix}"
                    r = requests.get(url, timeout=5)
                    if r.ok:
                        return token_id, r.json()
                except:
                    continue
        return token_id, None

    # Use ThreadPoolExecutor for concurrent fetch
    with ThreadPoolExecutor(max_workers=32) as executor:
        tasks = [executor.submit(fetch_metadata, i) for i in range(start, end + 1)]
        for fut in tqdm(as_completed(tasks), total=len(tasks)):
            token_id, data = fut.result()
            if data:
                jsons[token_id] = data
                for attr in data.get("attributes", []):
                    if isinstance(attr, dict) and "trait_type" in attr and "value" in attr:
                        trait_counter[attr["trait_type"]][attr["value"]] += 1

    if not jsons:
        await update.message.reply_text("❌ No metadata fetched. Check CID and token range.")
        return

    has_valid_attributes = any(
        isinstance(attr, dict) and "trait_type" in attr and "value" in attr
        for data in jsons.values()
        for attr in data.get("attributes", [])
    )
    if not has_valid_attributes:
        await update.message.reply_text("⚠️ Metadata fetched, but no valid attributes found in any token.")
        return

    total_tokens = len(jsons)
    rarity_data = []
    for token_id, data in jsons.items():
        score = 0.0
        flat_traits = {}

        attributes = data.get("attributes", [])
        if isinstance(attributes, dict):
            attributes = [{"trait_type": k, "value": v} for k, v in attributes.items()]

        for attr in attributes:
            if isinstance(attr, dict) and "trait_type" in attr and "value" in attr:
                trait = attr["trait_type"]
                val = attr["value"]
                freq = trait_counter[trait][val]
                rarity_score = 1 / (freq / total_tokens)
                flat_traits[trait] = val
                score += rarity_score
        rarity_data.append({
            "token_id": token_id,
            "rarity_score": round(score, 4),
            **flat_traits
        })

    df = pd.DataFrame(rarity_data)
    df["rarity_rank"] = df["rarity_score"].rank(ascending=False, method="min").astype(int)
    df = df.sort_values("rarity_rank")
    df.to_csv(output_csv, index=False)

    with ZipFile(output_zip, "w", ZIP_DEFLATED) as zipf:
        for token_id, data in jsons.items():
            zipf.writestr(f"{token_id}.json", json.dumps(data))

    await update.message.reply_text("✅ Analysis complete. Sending files...")
    with open(output_csv, "rb") as f1, open(output_zip, "rb") as f2:
        await update.message.reply_document(InputFile(f1, filename=os.path.basename(output_csv)))
        await update.message.reply_document(InputFile(f2, filename=os.path.basename(output_zip)))

    os.remove(output_csv)
    os.remove(output_zip)

# Flask app
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "✅ NFT Bot is running!"

# Async main entry
async def main():
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("auth", auth))
    application.add_handler(CommandHandler("analyze", analyze))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
