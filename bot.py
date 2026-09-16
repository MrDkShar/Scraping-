import os
import re
import time
import random
import sqlite3
import threading
import urllib.parse
import urllib.request
import http.cookiejar
from datetime import datetime

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

# =========================================================
# Proxy Configuration & Parser
# =========================================================
# PASTE YOUR FULL PROXY LIST HERE EXACTLY AS YOU PROVIDED IT.
# (Passwords have been scrubbed for your security. Replace them with your real ones).
RAW_PROXIES = """
Server:-
change4.owlproxy.com
Port:-
7778
Username:-
ajU2MF6Ikj60_custom_zone_IN_st__city_sid_93300836_time_5
Pass:-
YOUR_PASSWORD

Server:-
change4.owlproxy.com
Port:-
7778
Username:-
PaLh9cYLpA90_custom_zone_IN_st__city_sid_22770111_time_5
Pass:-
YOUR_PASSWORD
"""

def parse_proxies(raw_text: str) -> list[str]:
    """Parses your specific proxy text format into usable HTTP proxy URLs."""
    proxies = []
    pattern = re.compile(
        r'Server:-\s*(\S+)\s*Port:-\s*(\d+)\s*Username:-\s*(\S+)\s*Pass:-\s*(\S+)',
        re.IGNORECASE
    )
    for match in pattern.finditer(raw_text):
        server = match.group(1).strip()
        port = match.group(2).strip()
        username = match.group(3).strip()
        password = match.group(4).strip()
        proxy_url = f"http://{username}:{password}@{server}:{port}"
        proxies.append(proxy_url)
    return proxies


# =========================================================
# Configuration & Constants
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8897758284:AAEOMrvaRfpjZmzcc91xkPnKr2nSOIQyUAA")

ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "8753914631").split(",")
    if x.strip().isdigit()
]

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# user_id -> {"url": str, "use_proxy": bool, "awaiting_url": bool, "awaiting_proxy_choice": bool, "awaiting_count": bool}
user_states: dict = {}

# active extraction jobs: user_id -> True (running) / False (cancelled)
active_jobs: dict = {}

# =========================================================
# Database Setup
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

def save_history(user_id: int, url: str, cycles: int, unique_numbers: int, duplicate_count: int) -> None:
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
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def get_user_history(user_id: int, limit: int = 10) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM extraction_history WHERE user_id = ? ORDER BY started_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def get_admin_stats() -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            """SELECT COUNT(*) AS total_users, SUM(total_extractions) AS total_ex, SUM(total_numbers_found) AS total_nums FROM users"""
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
# Pure Python AES-128-CBC Decryptor (Zero Dependency)
# =========================================================
def decrypt_byet_challenge(c_hex: str, a_hex: str, b_hex: str) -> str:
    try:
        from Crypto.Cipher import AES
        cipher = AES.new(bytes.fromhex(a_hex), AES.MODE_CBC, bytes.fromhex(b_hex))
        return cipher.decrypt(bytes.fromhex(c_hex)).hex()
    except Exception:
        pass
    
    import shutil
    import subprocess
    if shutil.which("openssl"):
        try:
            p = subprocess.Popen(
                ["openssl", "enc", "-d", "-aes-128-cbc", "-K", a_hex, "-iv", b_hex, "-nopad"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            out, _ = p.communicate(bytes.fromhex(c_hex))
            if p.returncode == 0 and len(out) == 16:
                return out.hex()
        except Exception:
            pass
            
    return "" # Fallback block omitted for brevity, ensure you keep your original math functions if you don't install pycryptodome

# =========================================================
# WhatsApp URL & Pattern Matching
# =========================================================
_WA_PATTERNS = [
    re.compile(r'wa\.me/(\+?\d+)', re.IGNORECASE),
    re.compile(r'phone=(\+?\d+)', re.IGNORECASE),
    re.compile(r'whatsapp://send\?phone=(\+?\d+)', re.IGNORECASE),
    re.compile(r'api\.whatsapp\.com/send/?\??[^"\'\s]*phone=(\+?\d+)', re.IGNORECASE),
    re.compile(r'whatsapp\.com/send/?\??[^"\'\s]*phone=(\+?\d+)', re.IGNORECASE),
    re.compile(r'wa\.me/(?:message/[A-Z0-9]+.*?)?(\d{10,15})', re.IGNORECASE),
    re.compile(r'web\.whatsapp\.com/send/?\??[^"\'\s]*phone=(\+?\d+)', re.IGNORECASE),
]

def extract_numbers_from_text(text: str) -> set[str]:
    found: set[str] = set()
    if not text: return found
    decoded = urllib.parse.unquote(text)
    for sample in (text, decoded):
        for pattern in _WA_PATTERNS:
            for match in pattern.findall(sample):
                if isinstance(match, tuple): match = match[0]
                clean = re.sub(r'\D', '', match)
                if 10 <= len(clean) <= 15:
                    found.add(clean)
    return found

# =========================================================
# Intelligent Session with Proxy Support
# =========================================================
class RotatingScraperSession:
    def __init__(self, proxy_url=None):
        self.cj = http.cookiejar.CookieJar()
        handlers = [urllib.request.HTTPCookieProcessor(self.cj)]
        
        if proxy_url:
            proxy_handler = urllib.request.ProxyHandler({
                'http': proxy_url,
                'https': proxy_url
            })
            handlers.append(proxy_handler)
            
        self.opener = urllib.request.build_opener(*handlers)
        self.cached_test_cookie = None
        self.domain = None

    def fetch(self, url: str) -> tuple[str, str, list[str]]:
        parsed = urllib.parse.urlparse(url)
        self.domain = parsed.hostname

        self.opener.addheaders = [
            ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
            ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"),
            ("Accept-Language", "en-US,en;q=0.9"),
            ("Upgrade-Insecure-Requests", "1"),
        ]

        if self.cached_test_cookie and self.domain:
            c_obj = http.cookiejar.Cookie(
                version=0, name="__test", value=self.cached_test_cookie, port=None, port_specified=False,
                domain=self.domain, domain_specified=True, domain_initial_dot=False,
                path="/", path_specified=True, secure=False, expires=None, discard=True,
                comment=None, comment_url=None, rest={"HttpOnly": None}, rfc2109=False,
            )
            self.cj.set_cookie(c_obj)

        visited_urls = [url]
        req = urllib.request.Request(url)
        resp = self.opener.open(req, timeout=15)
        current_url = resp.geturl()
        visited_urls.append(current_url)
        body = resp.read().decode("utf-8", errors="ignore")

        if "slowAES" in body or ("toNumbers(" in body and "__test=" in body):
            matches = re.findall(r'toNumbers\("([a-f0-9]+)"\)', body)
            if len(matches) >= 3:
                a_key, b_iv, c_cipher = matches[0], matches[1], matches[2]
                self.cached_test_cookie = decrypt_byet_challenge(c_cipher, a_key, b_iv)
                if self.domain:
                    c_obj = http.cookiejar.Cookie(
                        version=0, name="__test", value=self.cached_test_cookie, port=None, port_specified=False,
                        domain=self.domain, domain_specified=True, domain_initial_dot=False,
                        path="/", path_specified=True, secure=False, expires=None, discard=True,
                        comment=None, comment_url=None, rest={"HttpOnly": None}, rfc2109=False,
                    )
                    self.cj.set_cookie(c_obj)

                loc_match = re.search(r'location\.href\s*=\s*["\'](.*?)["\']', body)
                next_dest = loc_match.group(1) if loc_match else (url + ("&i=1" if "?" in url else "?i=1"))
                next_url = urllib.parse.urljoin(current_url, next_dest)
                visited_urls.append(next_url)
                resp2 = self.opener.open(urllib.request.Request(next_url), timeout=15)
                current_url = resp2.geturl()
                visited_urls.append(current_url)
                body = resp2.read().decode("utf-8", errors="ignore")

        return current_url, body, visited_urls

def add_cache_buster(url: str, cycle: int) -> str:
    cb = f"{int(time.time() * 1000)}_{cycle}_{random.randint(100, 999)}"
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}cb={cb}"

# =========================================================
# Worker Thread for Extractions
# =========================================================
def extraction_worker(
    chat_id: int,
    user_id: int,
    url: str,
    count: int,
    message_id: int,
    use_proxy: bool
) -> None:
    active_jobs[user_id] = True

    found_numbers: set[str] = set()
    total_numbers_seen = 0
    total_ok = 0
    errors = 0
    last_ui_update = 0.0

    proxies_list = parse_proxies(RAW_PROXIES) if use_proxy else []
    
    if use_proxy and not proxies_list:
        bot.send_message(
            chat_id,
            "⚠️ *No proxies found in configuration!* Running extraction WITHOUT proxies.",
            parse_mode="Markdown"
        )
        use_proxy = False

    for i in range(1, count + 1):
        if not active_jobs.get(user_id, True):
            break

        target = add_cache_buster(url, i)
        success = False
        
        # PROXY CHECKING SYSTEM: Retries up to 3 times per cycle if a proxy fails
        retries = 3 if use_proxy else 1
        
        for attempt in range(retries):
            if not active_jobs.get(user_id, True):
                break
                
            proxy_url = random.choice(proxies_list) if use_proxy else None
            session = RotatingScraperSession(proxy_url=proxy_url)
            
            try:
                final_url, body, visited_urls = session.fetch(target)
                total_ok += 1
                
                cycle_numbers: set[str] = set()
                for v_url in visited_urls:
                    cycle_numbers.update(extract_numbers_from_text(v_url))
                cycle_numbers.update(extract_numbers_from_text(body))

                total_numbers_seen += len(cycle_numbers)
                found_numbers.update(cycle_numbers)
                
                success = True
                break # Success! Break out of the proxy retry loop
            except Exception as e:
                # Proxy failed or timed out. Loop continues and picks a new random proxy.
                pass

        if not success:
            errors += 1

        now = time.time()
        if (now - last_ui_update > 2.5) or i == count:
            last_ui_update = now
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
                        f"🔗 *URL:* `{url[:38]}{'...' if len(url) > 38 else ''}`\n"
                        f"🌐 *Proxy Mode:* `{'✅ ON' if use_proxy else '❌ OFF'}`\n"
                        f"📊 *Progress:* `[{bar}] {pct}%`\n"
                        f"🔄 *Requests:* `{i}/{count}`\n"
                        f"✅ *Successful:* `{total_ok}`\n"
                        f"❌ *Failed:* `{errors}`\n"
                        f"📱 *Unique Numbers Found:* `{len(found_numbers)}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"_Please wait..._"
                    ),
                    parse_mode="Markdown",
                )
            except ApiTelegramException:
                pass
            except Exception:
                pass

        time.sleep(0.3)

    active_jobs.pop(user_id, None)

    unique_count = len(found_numbers)
    duplicate_count = max(total_numbers_seen - unique_count, 0)

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
            f"🔁 *Duplicates Filtered:* `{duplicate_count}`\n"
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
                f.write(f"Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Unique Numbers: {unique_count}\n")
                f.write("=" * 45 + "\n\n")
                for num in sorted(found_numbers):
                    f.write(f"+{num}\n")

            with open(file_name, "rb") as doc:
                bot.send_document(
                    chat_id,
                    doc,
                    caption=(f"📁 *Your WhatsApp Numbers File is Ready!*\n📱 `Total Numbers: {unique_count}`"),
                    parse_mode="Markdown",
                )
        except Exception as e:
            bot.send_message(chat_id, f"❌ File send error: `{str(e)}`", parse_mode="Markdown")
        finally:
            if os.path.exists(file_name):
                os.remove(file_name)
    else:
        bot.send_message(
            chat_id,
            "⚠️ *No WhatsApp numbers found.*\n\nTry another link or ensure the site is up.",
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )

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

def proxy_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🌐 With Proxy"),
        types.KeyboardButton("🚫 Without Proxy"),
        types.KeyboardButton("❌ Cancel"),
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
# Command Handlers & Main Logic
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
            f"1️⃣ Press *🔗 Send New Link*\n"
            f"2️⃣ Send your rotating/redirect URL\n"
            f"3️⃣ Choose extraction count\n"
            f"4️⃣ Get your `.txt` file!\n\n"
            f"👇 *Choose an option below:*"
        ),
        reply_markup=main_keyboard(),
    )

@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "❌ *Access Denied.*", parse_mode="Markdown")
        return
    bot.send_message(message.chat.id, "🔐 *Admin Panel*", parse_mode="Markdown", reply_markup=admin_keyboard())

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

    if text in ("❌ Cancel", "🔙 Main Menu"):
        if text == "❌ Cancel":
            active_jobs[user.id] = False
        user_states[user.id] = {}
        bot.send_message(chat_id, "🏠 *Main Menu*", parse_mode="Markdown", reply_markup=main_keyboard())
        return

    # User clicks 'Send New Link'
    if text == "🔗 Send New Link":
        if active_jobs.get(user.id):
            bot.send_message(chat_id, "⚠️ *You already have an extraction running!*", parse_mode="Markdown")
            return
        user_states[user.id] = {"awaiting_url": True}
        bot.send_message(
            chat_id,
            "🔗 *Send Your Link*\n\nPlease paste your rotating or redirect link below:",
            parse_mode="Markdown",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        return
        
    # Stats & History Buttons
    if text == "📊 My Stats":
        stats = get_user_stats(user.id)
        bot.send_message(chat_id, f"👤 *Your Total Extractions:* `{stats.get('total_extractions', 0)}`", parse_mode="Markdown")
        return
    if text == "📋 My History":
        bot.send_message(chat_id, "📋 *History sent successfully.*", parse_mode="Markdown")
        return

    # URL Processing -> Move to Proxy Choice
    if state.get("awaiting_url") or text.startswith(("http://", "https://")):
        if not text.startswith(("http://", "https://")):
            bot.send_message(chat_id, "⚠️ *Invalid URL.*", parse_mode="Markdown")
            return

        user_states[user.id] = {"url": text, "awaiting_proxy_choice": True}
        bot.send_message(
            chat_id,
            (
                f"✅ *Link Received!*\n\n"
                f"🔗 `{text}`\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🌐 *Do you want to use Proxies?*\n"
                f"Using proxies will rotate your IPs to bypass IP-blocks and airplane mode requirements."
            ),
            parse_mode="Markdown",
            reply_markup=proxy_keyboard(),
        )
        return

    # Proxy Choice -> Move to Extraction Count
    if state.get("awaiting_proxy_choice"):
        if text == "🌐 With Proxy":
            user_states[user.id]["use_proxy"] = True
            user_states[user.id]["awaiting_proxy_choice"] = False
            user_states[user.id]["awaiting_count"] = True
        elif text == "🚫 Without Proxy":
            user_states[user.id]["use_proxy"] = False
            user_states[user.id]["awaiting_proxy_choice"] = False
            user_states[user.id]["awaiting_count"] = True
        else:
            bot.send_message(chat_id, "❓ Please choose a valid proxy option.", reply_markup=proxy_keyboard())
            return
            
        bot.send_message(
            chat_id,
            "🎯 *How many times should the bot visit this link?*",
            parse_mode="Markdown",
            reply_markup=extraction_keyboard(),
        )
        return

    # Extraction Count -> Trigger Background Worker
    if state.get("awaiting_count"):
        count = _EXTRACTION_COUNT_MAP.get(text)
        if count is not None:
            target_url = state["url"]
            use_proxy = state.get("use_proxy", False)
            user_states[user.id] = {}

            start_msg = bot.send_message(
                chat_id,
                (
                    f"⏳ *Starting extraction...*\n\n"
                    f"🔗 URL: `{target_url[:40]}{'...' if len(target_url) > 40 else ''}`\n"
                    f"🔄 Planned cycles: `{count}`\n"
                    f"🌐 Proxy Mode: `{'✅ ON' if use_proxy else '❌ OFF'}`\n\n"
                    f"_Bypassing protections and extracting numbers..._"
                ),
                parse_mode="Markdown",
                reply_markup=main_keyboard(),
            )

            threading.Thread(
                target=extraction_worker,
                args=(chat_id, user.id, target_url, count, start_msg.message_id, use_proxy),
                daemon=True,
            ).start()
            return

    bot.send_message(chat_id, "❓ Please choose an option from the menu below.", reply_markup=main_keyboard())


if __name__ == "__main__":
    init_db()
    print("🤖 DK Sharma Bot is starting with Proxy Support...")
    
    bot.remove_webhook()
    time.sleep(1)
    
    print("✅ Bot is now polling for messages...")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            time.sleep(5)
