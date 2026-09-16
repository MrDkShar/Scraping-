import os
import re
import time
import random
import sqlite3
import threading
import requests
import telebot
from telebot import types
from datetime import datetime

# =========================================================
# Configuration & Constants
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "123456789").split(",")
    if x.strip().isdigit()
]

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# user_id -> {"url": str, "awaiting_url": bool, "awaiting_broadcast": bool}
user_states: dict = {}

# active extraction jobs: user_id -> True (running) / False (cancelled)
active_jobs: dict = {}

# =========================================================
# Database
# =========================================================
DB_FILE = "bot_database.db"
_db_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _db_lock:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id               INTEGER PRIMARY KEY,
                username              TEXT,
                first_name            TEXT,
                total_extractions     INTEGER DEFAULT 0,
                total_numbers_found   INTEGER DEFAULT 0,
                joined_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS extraction_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER,
                url             TEXT,
                cycles          INTEGER,
                unique_numbers  INTEGER,
                duplicate_count INTEGER,
                started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at    TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_history_user ON extraction_history(user_id);
            CREATE INDEX IF NOT EXISTS idx_history_date ON extraction_history(started_at);
        """)
        conn.commit()
        conn.close()


def register_user(user_id: int, username: str = None, first_name: str = None) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                (user_id, username, first_name),
            )
            conn.execute(
                """UPDATE users
                   SET last_active = CURRENT_TIMESTAMP,
                       username    = COALESCE(?, username),
                       first_name  = COALESCE(?, first_name)
                   WHERE user_id = ?""",
                (username, first_name, user_id),
            )
            conn.commit()
        finally:
            conn.close()


def update_user_stats(user_id: int, unique_count: int) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE users
                   SET total_extractions   = total_extractions + 1,
                       total_numbers_found = total_numbers_found + ?,
                       last_active         = CURRENT_TIMESTAMP
                   WHERE user_id = ?""",
                (unique_count, user_id),
            )
            conn.commit()
        finally:
            conn.close()


def save_history(
    user_id: int, url: str, cycles: int, unique_numbers: int, duplicate_count: int
) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO extraction_history
                       (user_id, url, cycles, unique_numbers, duplicate_count, completed_at)
                   VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (user_id, url, cycles, unique_numbers, duplicate_count),
            )
            conn.commit()
        finally:
            conn.close()


def get_user_stats(user_id: int) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_history(user_id: int, limit: int = 10) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT * FROM extraction_history
               WHERE user_id = ?
               ORDER BY started_at DESC
               LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_admin_stats() -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            """SELECT COUNT(*)            AS total_users,
                      SUM(total_extractions)   AS total_ex,
                      SUM(total_numbers_found) AS total_nums
               FROM users"""
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def get_all_user_ids() -> list[int]:
    conn = get_conn()
    try:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]
    finally:
        conn.close()


# =========================================================
# Keyboard Builders
# =========================================================
def main_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🔗 Send New Link"),
        types.KeyboardButton("📋 My History"),
        types.KeyboardButton("📊 My Stats"),
        types.KeyboardButton("❓ Help"),
        types.KeyboardButton("📞 Support"),
    )
    return markup


def extraction_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🧪 Test (1x)"),
        types.KeyboardButton("🚀 20 Times"),
        types.KeyboardButton("⚡ 50 Times"),
        types.KeyboardButton("💎 100 Times (Max)"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def admin_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("📈 Bot Stats"),
        types.KeyboardButton("📢 Broadcast"),
        types.KeyboardButton("👥 Recent Users"),
        types.KeyboardButton("🔙 Main Menu"),
    )
    return markup


# =========================================================
# WhatsApp Extraction Engine
# =========================================================
_WA_PATTERNS = [
    re.compile(r'wa\.me/(\d+)', re.IGNORECASE),
    re.compile(r'phone=(\d+)', re.IGNORECASE),
    re.compile(r'whatsapp://send\?phone=(\d+)', re.IGNORECASE),
    re.compile(r'api\.whatsapp\.com/send\?phone=(\d+)', re.IGNORECASE),
    re.compile(r'wa\.me/(?:message/[A-Z0-9]+.*?)?(\d{10,15})', re.IGNORECASE),
]

_CHROME_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def _add_cache_buster(url: str) -> str:
    cb = f"{random.randint(10000, 99999)}_{random.randint(1000, 9999)}"
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}cb={cb}"


def _extract_numbers(text: str) -> set[str]:
    found: set[str] = set()
    for pattern in _WA_PATTERNS:
        for match in pattern.findall(text):
            clean = re.sub(r'\D', '', match)
            if 10 <= len(clean) <= 15:
                found.add(clean)
    return found


def extraction_worker(
    chat_id: int,
    user_id: int,
    url: str,
    count: int,
    message_id: int,
) -> None:
    active_jobs[user_id] = True

    session = requests.Session()
    session.headers.update(_CHROME_HEADERS)

    found_numbers: set[str] = set()
    total_ok = 0
    errors = 0

    for i in range(1, count + 1):
        if not active_jobs.get(user_id, True):
            break

        target = _add_cache_buster(url)
        try:
            resp = session.get(target, allow_redirects=True, timeout=10)
            total_ok += 1
            found_numbers.update(_extract_numbers(resp.url))
            found_numbers.update(_extract_numbers(resp.text))
        except requests.exceptions.Timeout:
            errors += 1
        except requests.exceptions.ConnectionError:
            errors += 1
        except Exception:
            errors += 1

        if i % 10 == 0 or i == count:
            try:
                done = int((i / count) * 10)
                bar = "█" * done + "░" * (10 - done)
                pct = int((i / count) * 100)
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=(
                        f"⏳ *Extraction In Progress*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🔗 *URL:* `{url[:40]}{'...' if len(url) > 40 else ''}`\n"
                        f"📊 Progress: `[{bar}] {pct}%`\n"
                        f"🔄 Requests: `{i}/{count}`\n"
                        f"✅ Successful: `{total_ok}`\n"
                        f"❌ Failed: `{errors}`\n"
                        f"📱 Numbers Found So Far: `{len(found_numbers)}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"_Please wait..._"
                    ),
                    parse_mode="Markdown",
                )
            except Exception:
                pass  # Edit may fail on stale/deleted messages — non-fatal

        time.sleep(0.3)

    # ── Job Complete ────────────────────────────────────────────────────
    active_jobs.pop(user_id, None)

    unique_count = len(found_numbers)
    # Duplicates: how many extra times we saw a number we'd already collected.
    # total_ok requests ran; if we got 3 unique but 50 requests, 47 were repeats.
    duplicate_count = max(total_ok - unique_count, 0)

    update_user_stats(user_id, unique_count)
    save_history(user_id, url, count, unique_count, duplicate_count)

    bot.send_message(
        chat_id,
        (
            f"✅ *Extraction Complete!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 *Total Cycles Run:* `{count}`\n"
            f"✅ *Successful Requests:* `{total_ok}`\n"
            f"❌ *Failed Requests:* `{errors}`\n"
            f"🎯 *Unique WhatsApp Numbers:* `{unique_count}`\n"
            f"🔁 *Duplicates Skipped:* `{duplicate_count}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━"
        ),
        parse_mode="Markdown",
        reply_markup=main_keyboard(),
    )

    if unique_count > 0:
        file_name = f"whatsapp_numbers_{user_id}_{int(time.time())}.txt"
        try:
            with open(file_name, "w", encoding="utf-8") as f:
                f.write("DK Sharma Bot — WhatsApp Number Extractor\n")
                f.write(f"Source URL: {url}\n")
                f.write(f"Extracted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Unique Numbers: {unique_count}\n")
                f.write("=" * 40 + "\n\n")
                for num in sorted(found_numbers):
                    f.write(f"+{num}\n")

            with open(file_name, "rb") as doc:
                bot.send_document(
                    chat_id,
                    doc,
                    caption=(
                        f"📁 *Your WhatsApp Numbers File is Ready!*\n"
                        f"`Total Numbers: {unique_count}`\n"
                        f"_Made by DK Sharma Bot_ 🤖"
                    ),
                    parse_mode="Markdown",
                )
        except Exception as e:
            bot.send_message(
                chat_id,
                f"❌ File send error: `{str(e)}`",
                parse_mode="Markdown",
            )
        finally:
            if os.path.exists(file_name):
                os.remove(file_name)
    else:
        bot.send_message(
            chat_id,
            (
                "⚠️ *No WhatsApp numbers found.*\n\n"
                "Possible reasons:\n"
                "• Link doesn't redirect to WhatsApp\n"
                "• Website blocked the bot's requests\n"
                "• Link has expired\n\n"
                "_Try a different link or contact support._"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )


# =========================================================
# Command Handlers
# =========================================================
@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message) -> None:
    user = message.from_user
    register_user(user.id, user.username, user.first_name)
    bot.send_message(
        message.chat.id,
        (
            f"👋 *Welcome to DK Sharma Bot!*\n\n"
            f"🤖 *WhatsApp Number Extractor*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Made by *DK Sharma* 🎯\n\n"
            f"🔥 *What can this bot do?*\n"
            f"Extract hidden WhatsApp numbers from any rotating or redirect link — "
            f"automatically, in bulk.\n\n"
            f"📌 *How to use:*\n"
            f"1️⃣ Press *🔗 Send New Link*\n"
            f"2️⃣ Send your rotating/redirect URL\n"
            f"3️⃣ Choose how many times to run (20, 50, 100)\n"
            f"4️⃣ Get your `.txt` file with all unique numbers!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 *Choose an option below:*"
        ),
        reply_markup=main_keyboard(),
    )


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(
            message.chat.id,
            "❌ *Access Denied.* You are not an admin.",
            parse_mode="Markdown",
        )
        return
    bot.send_message(
        message.chat.id,
        "🔐 *Admin Panel — DK Sharma Bot*\n━━━━━━━━━━━━━━━━━━━━━\nChoose an action:",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )


# =========================================================
# Main Message Router
# =========================================================
_EXTRACTION_COUNT_MAP = {
    "🧪 Test (1x)": 1,
    "🚀 20 Times": 20,
    "⚡ 50 Times": 50,
    "💎 100 Times (Max)": 100,
}


@bot.message_handler(func=lambda m: True)
def handle_messages(message: types.Message) -> None:
    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()

    register_user(user.id, user.username, user.first_name)
    state = user_states.get(user.id, {})

    # ── Global cancel / main menu ───────────────────────────────────────
    if text in ("❌ Cancel", "🔙 Main Menu"):
        if text == "❌ Cancel":
            active_jobs[user.id] = False
        user_states[user.id] = {}
        bot.send_message(
            chat_id,
            "🏠 *Main Menu*",
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
        return

    # ── Admin broadcast input ──────────────────────────────────────────
    if user.id in ADMIN_IDS and state.get("awaiting_broadcast"):
        user_states[user.id] = {}
        all_users = get_all_user_ids()
        sent = failed = 0
        status_msg = bot.send_message(
            chat_id,
            f"🚀 *Broadcasting to {len(all_users)} users...*",
            parse_mode="Markdown",
        )
        for uid in all_users:
            try:
                bot.send_message(uid, text)
                sent += 1
                time.sleep(0.05)
            except Exception:
                failed += 1
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg.message_id,
            text=(
                f"✅ *Broadcast Complete!*\n"
                f"🎉 Sent: `{sent}`\n"
                f"❌ Failed: `{failed}`"
            ),
            parse_mode="Markdown",
        )
        bot.send_message(chat_id, "Admin Panel:", reply_markup=admin_keyboard())
        return

    # ── Admin panel buttons ────────────────────────────────────────────
    if user.id in ADMIN_IDS:
        if text == "📈 Bot Stats":
            stats = get_admin_stats()
            bot.send_message(
                chat_id,
                (
                    f"📈 *Bot System Stats*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👥 *Total Users:* `{stats.get('total_users') or 0}`\n"
                    f"🔄 *Total Extractions:* `{stats.get('total_ex') or 0}`\n"
                    f"📱 *Total Numbers Found:* `{stats.get('total_nums') or 0}`\n"
                    f"━━━━━━━━━━━━━━━━━━━━━"
                ),
                parse_mode="Markdown",
                reply_markup=admin_keyboard(),
            )
            return

        if text == "📢 Broadcast":
            user_states[user.id] = {"awaiting_broadcast": True}
            bot.send_message(
                chat_id,
                "📢 *Broadcast Message*\n\nType the message you want to send to ALL users:",
                parse_mode="Markdown",
            )
            return

        if text == "👥 Recent Users":
            conn = get_conn()
            try:
                rows = conn.execute(
                    """SELECT user_id, first_name, username, total_numbers_found, joined_at
                       FROM users
                       ORDER BY joined_at DESC
                       LIMIT 10"""
                ).fetchall()
            finally:
                conn.close()

            if not rows:
                bot.send_message(chat_id, "No users yet.", reply_markup=admin_keyboard())
                return

            msg = "👥 *Recent 10 Users*\n━━━━━━━━━━━━━━━━━━━━━\n\n"
            for i, r in enumerate(rows, 1):
                name = r["first_name"] or "N/A"
                uname = r["username"] or "no_username"
                msg += (
                    f"*#{i}* {name} (`{r['user_id']}`)\n"
                    f"📱 {r['total_numbers_found']} numbers | @{uname}\n\n"
                )
            bot.send_message(chat_id, msg, parse_mode="Markdown", reply_markup=admin_keyboard())
            return

    # ── Send New Link ──────────────────────────────────────────────────
    if text == "🔗 Send New Link":
        if active_jobs.get(user.id):
            bot.send_message(
                chat_id,
                "⚠️ *You already have an extraction running!* Please wait for it to finish.",
                parse_mode="Markdown",
            )
            return
        user_states[user.id] = {"awaiting_url": True}
        bot.send_message(
            chat_id,
            (
                "🔗 *Send Your Link*\n\n"
                "Please paste your rotating or redirect link below:\n\n"
                "_Example:_ `https://prismatic-daifuku.site.je/l/7u9vPK`"
            ),
            parse_mode="Markdown",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        return

    # ── My Stats ────────────────────────────────────────────────────────
    if text == "📊 My Stats":
        stats = get_user_stats(user.id)
        if not stats:
            bot.send_message(
                chat_id,
                "📊 No stats yet. Run your first extraction!",
                reply_markup=main_keyboard(),
            )
            return
        bot.send_message(
            chat_id,
            (
                f"📊 *Your Stats — DK Sharma Bot*\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 *User ID:* `{user.id}`\n"
                f"👤 *Name:* {stats.get('first_name') or 'Unknown'}\n"
                f"🔄 *Total Extractions Run:* `{stats.get('total_extractions', 0)}`\n"
                f"📱 *Total Numbers Found:* `{stats.get('total_numbers_found', 0)}`\n"
                f"📅 *Member Since:* `{stats.get('joined_at', 'N/A')}`\n"
                f"━━━━━━━━━━━━━━━━━━━━━"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
        return

    # ── My History ──────────────────────────────────────────────────────
    if text == "📋 My History":
        history = get_user_history(user.id, limit=10)
        if not history:
            bot.send_message(
                chat_id,
                "📋 *No extraction history yet.*\n\nRun your first extraction to see results here!",
                parse_mode="Markdown",
                reply_markup=main_keyboard(),
            )
            return
        msg = "📋 *Your Last 10 Extractions*\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, h in enumerate(history, 1):
            url_display = (h["url"][:30] + "...") if len(h["url"]) > 30 else h["url"]
            completed = (h.get("completed_at") or "N/A")[:16]
            msg += (
                f"*#{i}* | `{completed}`\n"
                f"🔗 `{url_display}`\n"
                f"🔄 Cycles: `{h['cycles']}` | "
                f"📱 Found: `{h['unique_numbers']}` | "
                f"🔁 Dupes: `{h['duplicate_count']}`\n\n"
            )
        bot.send_message(chat_id, msg, parse_mode="Markdown", reply_markup=main_keyboard())
        return

    # ── Help ────────────────────────────────────────────────────────────
    if text == "❓ Help":
        bot.send_message(
            chat_id,
            (
                "❓ *How to Use DK Sharma Bot*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                "*Step 1:* Press *🔗 Send New Link*\n"
                "*Step 2:* Paste your rotating/redirect link\n"
                "*Step 3:* Choose extraction count:\n"
                "  • `🧪 Test (1x)` — One quick test\n"
                "  • `🚀 20 Times` — Medium extraction\n"
                "  • `⚡ 50 Times` — Full extraction\n"
                "  • `💎 100 Times` — Maximum extraction\n"
                "*Step 4:* Wait for results\n"
                "*Step 5:* Download your `.txt` file!\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "🔄 *What is a rotating link?*\n"
                "A link that opens a website briefly, then redirects to WhatsApp with a "
                "phone number. The number may change each time — this bot catches all unique ones!\n\n"
                "✅ *Duplicate numbers are automatically removed.*"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
        return

    # ── Support ─────────────────────────────────────────────────────────
    if text == "📞 Support":
        bot.send_message(
            chat_id,
            (
                "📞 *Support & Help*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "Having issues? Contact the admin:\n\n"
                "👤 *Admin:* @YourAdminHandle\n\n"
                "_Made with ❤️ by DK Sharma_"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
        return

    # ── URL Capture ──────────────────────────────────────────────────────
    if state.get("awaiting_url") or text.startswith(("http://", "https://")):
        if not text.startswith(("http://", "https://")):
            bot.send_message(
                chat_id,
                (
                    "⚠️ *Invalid URL.*\n\n"
                    "Please send a valid URL starting with `http://` or `https://`"
                ),
                parse_mode="Markdown",
            )
            return

        user_states[user.id] = {"url": text}
        bot.send_message(
            chat_id,
            (
                f"✅ *Link Received!*\n\n"
                f"🔗 `{text}`\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 *How many times should the bot visit this link?*\n\n"
                f"• More visits = more chances to find different numbers\n"
                f"• Duplicates are automatically removed"
            ),
            parse_mode="Markdown",
            reply_markup=extraction_keyboard(),
        )
        return

    # ── Extraction Count Selection ────────────────────────────────────────
    if "url" in state:
        count = _EXTRACTION_COUNT_MAP.get(text)
        if count is not None:
            target_url = state["url"]
            user_states[user.id] = {}

            start_msg = bot.send_message(
                chat_id,
                (
                    f"⏳ *Starting extraction...*\n\n"
                    f"🔗 URL: `{target_url[:40]}{'...' if len(target_url) > 40 else ''}`\n"
                    f"🔄 Planned cycles: `{count}`\n\n"
                    f"_This may take a few moments. Do not close the chat._"
                ),
                parse_mode="Markdown",
                reply_markup=main_keyboard(),
            )

            threading.Thread(
                target=extraction_worker,
                args=(chat_id, user.id, target_url, count, start_msg.message_id),
                daemon=True,
            ).start()
            return

    # ── Fallback ─────────────────────────────────────────────────────────
    bot.send_message(
        chat_id,
        "❓ Please choose an option from the menu below.",
        reply_markup=main_keyboard(),
    )


# =========================================================
# Entry Point
# =========================================================
if __name__ == "__main__":
    init_db()

    print("🤖 DK Sharma Bot is starting...")
    print(f"Admin IDs configured: {ADMIN_IDS}")

    try:
        bot.remove_webhook()
        time.sleep(1)
        print("✅ Webhook removed successfully.")
    except Exception as e:
        print(f"⚠️  Webhook removal warning: {e}")

    print("✅ Bot is now polling for messages...")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"❌ Polling error: {e}")
            time.sleep(5)
            print("🔄 Reconnecting...")
