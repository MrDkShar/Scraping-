import os
import re
import time
import random
import sqlite3
import threading
import urllib.parse
import telebot
from telebot import types
from datetime import datetime

# ── BeautifulSoup for HTML parsing ─────────────────────────────────────────
from bs4 import BeautifulSoup

# ── curl_cffi: real Chrome TLS fingerprint (bypasses Cloudflare) ───────────
from curl_cffi import requests as cffi_requests

# =========================================================
# Configuration & Constants
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8897758284:AAHXLHUjxLdH8ynWWsZpxmCwf8IsH1M69j0")

ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "8753914631").split(",")
    if x.strip().isdigit()
]

# Railway.app: /data/ is persistent volume; fallback to current dir
DB_DIR = "/data" if os.path.isdir("/data") else "."
DB_FILE = os.path.join(DB_DIR, "bot_database.db")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# user_id -> state dict
user_states: dict = {}

# active jobs: user_id -> True/False (False = cancel requested)
active_jobs: dict = {}

# =========================================================
# Database
# =========================================================
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


def save_history(user_id, url, cycles, unique_numbers, duplicate_count):
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


def get_user_stats(user_id: int):
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_history(user_id: int, limit: int = 10):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM extraction_history WHERE user_id = ? ORDER BY started_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_admin_stats():
    conn = get_conn()
    try:
        row = conn.execute(
            """SELECT COUNT(*) AS total_users,
                      SUM(total_extractions) AS total_ex,
                      SUM(total_numbers_found) AS total_nums
               FROM users"""
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def get_all_user_ids():
    conn = get_conn()
    try:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]
    finally:
        conn.close()


# =========================================================
# Keyboards
# =========================================================
def main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🔗 Send New Link"),
        types.KeyboardButton("📋 My History"),
        types.KeyboardButton("📊 My Stats"),
        types.KeyboardButton("❓ Help"),
        types.KeyboardButton("📞 Support"),
    )
    return markup


def extraction_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🧪 Test (1x)"),
        types.KeyboardButton("🚀 20 Times"),
        types.KeyboardButton("⚡ 50 Times"),
        types.KeyboardButton("💎 100 Times (Max)"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def admin_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("📈 Bot Stats"),
        types.KeyboardButton("📢 Broadcast"),
        types.KeyboardButton("👥 Recent Users"),
        types.KeyboardButton("🔙 Main Menu"),
    )
    return markup


# =========================================================
# WhatsApp Number Extraction Engine
# =========================================================

# Every known pattern where a WhatsApp number can appear
_WA_PATTERNS = [
    # wa.me/919876543210
    re.compile(r'wa\.me/(?:message/[A-Z0-9]+[^"\'&\s]*?)?(\d{10,15})', re.IGNORECASE),
    # ?phone=919876543210
    re.compile(r'[?&]phone=(\d{10,15})', re.IGNORECASE),
    # whatsapp://send?phone=
    re.compile(r'whatsapp://send\?phone=(\d{10,15})', re.IGNORECASE),
    # api.whatsapp.com/send?phone=
    re.compile(r'api\.whatsapp\.com/send/?\?(?:[^"\'<>\s]*&)?phone=(\d{10,15})', re.IGNORECASE),
    # JS window.location = "https://wa.me/..."
    re.compile(r'window\.location(?:\.href)?\s*=\s*["\']([^"\']*whatsapp[^"\']*)["\']', re.IGNORECASE),
    re.compile(r'location\.replace\s*\(\s*["\']([^"\']*whatsapp[^"\']*)["\']', re.IGNORECASE),
    re.compile(r'(?<!\w)location\.href\s*=\s*["\']([^"\']*whatsapp[^"\']*)["\']', re.IGNORECASE),
    # href attributes
    re.compile(r'href=["\']([^"\']*(?:wa\.me|whatsapp)[^"\']*)["\']', re.IGNORECASE),
    # data-* attributes
    re.compile(r'data-(?:url|href|link|action)=["\']([^"\']*(?:wa\.me|whatsapp)[^"\']*)["\']', re.IGNORECASE),
    # onclick / onload
    re.compile(r'on(?:click|load)=["\'][^"\']*(?:wa\.me|whatsapp)([^"\']*)["\']', re.IGNORECASE),
    # meta refresh
    re.compile(r'content=["\'][^"\']*URL=([^"\']*(?:wa\.me|whatsapp)[^"\']*)["\']', re.IGNORECASE),
    # JSON fields
    re.compile(r'"(?:url|link|redirect|whatsapp|href)"\s*:\s*"([^"]*(?:wa\.me|whatsapp)[^"]*)"', re.IGNORECASE),
    # catch-all: any whatsapp/wa.me token followed by digits
    re.compile(r'(?:wa\.me|whatsapp)[^\d]*(\d{10,15})', re.IGNORECASE),
]

# Real rotating browser User-Agents
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Android 14; Mobile; rv:125.0) Gecko/125.0 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]


def _get_headers() -> dict:
    """Rotate headers on every call to avoid fingerprinting."""
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.9", "en-IN,en;q=0.9,hi;q=0.8"]),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
    }


def _extract_numbers_from_text(text: str):
    """Run all regex patterns on raw text. Returns (set_of_numbers, raw_match_count)."""
    found = set()
    raw_count = 0
    for pattern in _WA_PATTERNS:
        for match in pattern.findall(text):
            for num_str in re.findall(r'\d+', match):
                clean = num_str.strip()
                if 10 <= len(clean) <= 15:
                    raw_count += 1
                    found.add(clean)
    return found, raw_count


def _deep_html_parse(html: str):
    """BeautifulSoup deep parse: <a>, <script>, <meta>, data-attrs, onclick."""
    found = set()
    raw_count = 0
    try:
        soup = BeautifulSoup(html, "html.parser")

        # <a href>
        for tag in soup.find_all("a", href=True):
            nums, cnt = _extract_numbers_from_text(tag["href"])
            found.update(nums); raw_count += cnt

        # <script> — JS redirects
        for script in soup.find_all("script"):
            nums, cnt = _extract_numbers_from_text(script.get_text() or "")
            found.update(nums); raw_count += cnt

        # <meta content> — meta-refresh
        for meta in soup.find_all("meta"):
            content = meta.get("content", "")
            if content:
                nums, cnt = _extract_numbers_from_text(content)
                found.update(nums); raw_count += cnt

        # All data-* / onclick / onload / action attrs
        for tag in soup.find_all(True):
            for attr_name, attr_val in tag.attrs.items():
                if isinstance(attr_val, str) and (
                    attr_name.startswith("data-") or
                    attr_name in ("onclick", "onload", "href", "action", "src")
                ):
                    nums, cnt = _extract_numbers_from_text(attr_val)
                    found.update(nums); raw_count += cnt

        # noscript tags
        for ns in soup.find_all("noscript"):
            nums, cnt = _extract_numbers_from_text(ns.get_text() or "")
            found.update(nums); raw_count += cnt

    except Exception:
        pass
    return found, raw_count


def _detect_js_redirect(html: str, base_url: str):
    """Detect JS / meta-refresh redirect URL from page source."""
    # meta http-equiv refresh
    m = re.search(
        r'<meta[^>]+http-equiv=["\']refresh["\'][^>]+content=["\'][^"\']*url=([^"\';\s]+)',
        html, re.IGNORECASE
    )
    if m:
        return urllib.parse.urljoin(base_url, m.group(1).strip())

    # window.location / window.location.href
    m = re.search(r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return urllib.parse.urljoin(base_url, m.group(1).strip())

    # location.replace(...)
    m = re.search(r'location\.replace\s*\(\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return urllib.parse.urljoin(base_url, m.group(1).strip())

    # location.href = ...
    m = re.search(r'(?<!\w)location\.href\s*=\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return urllib.parse.urljoin(base_url, m.group(1).strip())

    # setTimeout with location
    m = re.search(r'setTimeout\s*\([^,]*["\']([^"\']*(?:wa\.me|whatsapp)[^"\']*)["\']', html, re.IGNORECASE)
    if m:
        return urllib.parse.urljoin(base_url, m.group(1).strip())

    return None


def _visit_once(url: str, session) -> tuple:
    """
    Visit URL once with curl_cffi (Chrome TLS impersonation).
    Returns (found_numbers_set, raw_match_count, error_message_or_None).
    """
    found = set()
    raw_count = 0

    try:
        # Clear cookies → fresh independent visit every time
        session.cookies.clear()

        resp = session.get(
            url,
            impersonate="chrome120",       # Real Chrome TLS fingerprint → bypasses Cloudflare
            allow_redirects=True,
            timeout=20,
            headers=_get_headers(),
            verify=False,                  # Skip SSL verify (some redirect sites have bad certs)
        )

        # ── 1. Numbers in final resolved URL ──────────────────────────────
        nums, cnt = _extract_numbers_from_text(resp.url)
        found.update(nums); raw_count += cnt

        # ── 2. Numbers in every HTTP redirect hop ─────────────────────────
        for r in resp.history:
            nums, cnt = _extract_numbers_from_text(r.url)
            found.update(nums); raw_count += cnt
            # Also check Location header directly
            loc = r.headers.get("Location", "")
            if loc:
                nums, cnt = _extract_numbers_from_text(loc)
                found.update(nums); raw_count += cnt

        body = resp.text

        # ── 3. Raw regex on entire HTML body ──────────────────────────────
        nums, cnt = _extract_numbers_from_text(body)
        found.update(nums); raw_count += cnt

        # ── 4. Deep BeautifulSoup parse ───────────────────────────────────
        nums, cnt = _deep_html_parse(body)
        found.update(nums); raw_count += cnt

        # ── 5. Follow JS / meta-refresh redirect ──────────────────────────
        js_url = _detect_js_redirect(body, resp.url)
        if js_url and js_url != resp.url:
            nums, cnt = _extract_numbers_from_text(js_url)
            found.update(nums); raw_count += cnt

            # If it's not already a WhatsApp URL, fetch the redirect target too
            if not any(kw in js_url.lower() for kw in ("whatsapp", "wa.me")):
                try:
                    session.cookies.clear()
                    r2 = session.get(
                        js_url,
                        impersonate="chrome120",
                        allow_redirects=True,
                        timeout=15,
                        headers=_get_headers(),
                        verify=False,
                    )
                    for hop in r2.history:
                        nums, cnt = _extract_numbers_from_text(hop.url)
                        found.update(nums); raw_count += cnt
                        loc = hop.headers.get("Location", "")
                        if loc:
                            nums, cnt = _extract_numbers_from_text(loc)
                            found.update(nums); raw_count += cnt
                    nums, cnt = _extract_numbers_from_text(r2.url)
                    found.update(nums); raw_count += cnt
                    nums, cnt = _extract_numbers_from_text(r2.text)
                    found.update(nums); raw_count += cnt
                    nums, cnt = _deep_html_parse(r2.text)
                    found.update(nums); raw_count += cnt
                except Exception:
                    pass

        return found, raw_count, None

    except Exception as e:
        return set(), 0, str(e)


# =========================================================
# Extraction Worker Thread
# =========================================================
def extraction_worker(chat_id: int, user_id: int, url: str, count: int, message_id: int):
    active_jobs[user_id] = True

    # One session per job — connection pooling + shared TLS state
    session = cffi_requests.Session()

    all_found: set = set()
    total_ok = 0
    total_errors = 0
    total_raw = 0

    for i in range(1, count + 1):
        if not active_jobs.get(user_id, True):
            # User pressed Cancel
            break

        nums, raw, err = _visit_once(url, session)

        if err:
            total_errors += 1
        else:
            total_ok += 1
            total_raw += raw
            all_found.update(nums)

        # Update progress every 5 requests or on the last one
        if i % 5 == 0 or i == count:
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
                        f"🔗 *URL:* `{url[:45]}{'...' if len(url) > 45 else ''}`\n"
                        f"📊 Progress: `[{bar}] {pct}%`\n"
                        f"🔄 Requests: `{i}/{count}`\n"
                        f"✅ Successful: `{total_ok}`\n"
                        f"❌ Failed: `{total_errors}`\n"
                        f"📱 Numbers Found So Far: `{len(all_found)}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"_Please wait..._"
                    ),
                    parse_mode="Markdown",
                )
            except Exception:
                pass

        # Human-like random delay: 1.5s – 3.5s
        # This is critical — too fast = IP ban, too slow = waste of time
        delay = random.uniform(1.5, 3.5)
        # Slow down more if we keep getting errors (likely rate-limited)
        if total_errors > 3 and total_errors > total_ok:
            delay = random.uniform(4.0, 7.0)
        time.sleep(delay)

    # ── Cleanup & Results ─────────────────────────────────────────────
    active_jobs.pop(user_id, None)

    unique_count = len(all_found)
    # CORRECT math: raw_count includes re-appearances of the same number
    # duplicate_count = how many times numbers appeared again after first sight
    duplicate_count = max(0, total_raw - unique_count)

    update_user_stats(user_id, unique_count)
    save_history(user_id, url, count, unique_count, duplicate_count)

    bot.send_message(
        chat_id,
        (
            f"✅ *Extraction Complete!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 *Total Cycles Run:* `{count}`\n"
            f"✅ *Successful Requests:* `{total_ok}`\n"
            f"❌ *Failed Requests:* `{total_errors}`\n"
            f"🎯 *Unique WhatsApp Numbers:* `{unique_count}`\n"
            f"🔁 *Duplicate Appearances:* `{duplicate_count}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━"
        ),
        parse_mode="Markdown",
        reply_markup=main_keyboard(),
    )

    if unique_count > 0:
        # Generate and send .txt file
        file_name = f"wa_numbers_{user_id}_{int(time.time())}.txt"
        try:
            with open(file_name, "w", encoding="utf-8") as f:
                f.write("DK Sharma Bot — WhatsApp Number Extractor\n")
                f.write(f"Source URL : {url}\n")
                f.write(f"Extracted  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Unique: {unique_count}\n")
                f.write("=" * 45 + "\n\n")
                for num in sorted(all_found):
                    f.write(f"+{num}\n")

            with open(file_name, "rb") as doc:
                bot.send_document(
                    chat_id,
                    doc,
                    caption=(
                        f"📁 *WhatsApp Numbers File Ready!*\n"
                        f"`Total: {unique_count} unique numbers`\n"
                        f"_Made by DK Sharma Bot_ 🤖"
                    ),
                    parse_mode="Markdown",
                )
        except Exception as e:
            bot.send_message(chat_id, f"❌ File error: `{e}`", parse_mode="Markdown")
        finally:
            if os.path.exists(file_name):
                os.remove(file_name)
    else:
        bot.send_message(
            chat_id,
            (
                "⚠️ *No WhatsApp numbers found.*\n\n"
                "*Possible reasons:*\n"
                "• Website loads numbers via JavaScript AJAX after page load\n"
                "• Link has expired or is invalid\n"
                "• Website is heavily bot-protected (needs real browser)\n"
                "• Numbers are inside an iframe or dynamic popup\n\n"
                "_Try a different link or test with /debug command._"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )


# =========================================================
# Command Handlers
# =========================================================
@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message):
    user = message.from_user
    register_user(user.id, user.username, user.first_name)
    bot.send_message(
        message.chat.id,
        (
            f"👋 *Welcome to DK Sharma Bot!*\n\n"
            f"🤖 *WhatsApp Number Extractor*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Made by *DK Sharma* 🎯\n\n"
            f"🔥 *What this bot does:*\n"
            f"Extracts hidden WhatsApp numbers from rotating/redirect links "
            f"automatically, in bulk, with real Chrome browser impersonation.\n\n"
            f"📌 *How to use:*\n"
            f"1️⃣ Press *🔗 Send New Link*\n"
            f"2️⃣ Paste your rotating/redirect URL\n"
            f"3️⃣ Choose how many cycles (20, 50, 100)\n"
            f"4️⃣ Get a `.txt` file with all unique numbers!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 *Choose an option:*"
        ),
        reply_markup=main_keyboard(),
    )


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "❌ *Access Denied.*", parse_mode="Markdown")
        return
    bot.send_message(
        message.chat.id,
        "🔐 *Admin Panel — DK Sharma Bot*\n━━━━━━━━━━━━━━━━━━━━━\nChoose an action:",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )


@bot.message_handler(commands=["stats"])
def cmd_stats(message: types.Message):
    stats = get_user_stats(message.from_user.id)
    if not stats:
        bot.send_message(message.chat.id, "📊 No stats yet. Run your first extraction!")
        return
    bot.send_message(
        message.chat.id,
        (
            f"📊 *Your Stats*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔄 *Total Extractions:* `{stats.get('total_extractions', 0)}`\n"
            f"📱 *Total Numbers Found:* `{stats.get('total_numbers_found', 0)}`\n"
            f"📅 *Member Since:* `{stats.get('joined_at', 'N/A')}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━"
        ),
        parse_mode="Markdown",
        reply_markup=main_keyboard(),
    )


@bot.message_handler(commands=["debug"])
def cmd_debug(message: types.Message):
    """Debug command: test a URL manually and show raw output."""
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].startswith("http"):
        bot.send_message(
            message.chat.id,
            "🔧 *Debug Usage:*\n`/debug https://your-link-here`\n\n"
            "Shows exactly what the bot sees when it visits your URL.",
            parse_mode="Markdown",
        )
        return

    url = parts[1].strip()
    bot.send_message(message.chat.id, f"🔍 Testing URL:\n`{url}`\n\n_Please wait..._", parse_mode="Markdown")

    session = cffi_requests.Session()
    nums, raw, err = _visit_once(url, session)

    if err:
        bot.send_message(
            message.chat.id,
            f"❌ *Error visiting URL:*\n`{err}`\n\n"
            f"This means the site is blocking all automated requests.",
            parse_mode="Markdown",
        )
        return

    if nums:
        result = "\n".join(f"+{n}" for n in sorted(nums))
        bot.send_message(
            message.chat.id,
            f"✅ *Debug Result — Numbers Found: {len(nums)}*\n\n```\n{result}\n```",
            parse_mode="Markdown",
        )
    else:
        bot.send_message(
            message.chat.id,
            (
                f"⚠️ *Debug Result — No Numbers Found*\n\n"
                f"Raw regex matches: `{raw}`\n\n"
                f"The URL was visited but no WhatsApp number pattern was detected.\n"
                f"The number may be loaded via AJAX after page load."
            ),
            parse_mode="Markdown",
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
def handle_messages(message: types.Message):
    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()

    register_user(user.id, user.username, user.first_name)
    state = user_states.get(user.id, {})

    # ── Cancel / Main Menu ────────────────────────────────────────────
    if text in ("❌ Cancel", "🔙 Main Menu"):
        if text == "❌ Cancel":
            active_jobs[user.id] = False
        user_states[user.id] = {}
        bot.send_message(chat_id, "🏠 *Main Menu*", parse_mode="Markdown", reply_markup=main_keyboard())
        return

    # ── Admin Broadcast Input ─────────────────────────────────────────
    if user.id in ADMIN_IDS and state.get("awaiting_broadcast"):
        user_states[user.id] = {}
        all_users = get_all_user_ids()
        sent = failed = 0
        status_msg = bot.send_message(chat_id, f"🚀 *Broadcasting to {len(all_users)} users...*", parse_mode="Markdown")
        for uid in all_users:
            try:
                bot.send_message(uid, text, parse_mode="Markdown")
                sent += 1
                time.sleep(0.05)
            except Exception:
                failed += 1
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg.message_id,
            text=f"✅ *Broadcast Complete!*\n🎉 Sent: `{sent}`\n❌ Failed: `{failed}`",
            parse_mode="Markdown",
        )
        bot.send_message(chat_id, "Admin Panel:", reply_markup=admin_keyboard())
        return

    # ── Admin Panel Buttons ───────────────────────────────────────────
    if user.id in ADMIN_IDS:
        if text == "📈 Bot Stats":
            s = get_admin_stats()
            bot.send_message(
                chat_id,
                (
                    f"📈 *Bot System Stats*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👥 *Total Users:* `{s.get('total_users') or 0}`\n"
                    f"🔄 *Total Extractions:* `{s.get('total_ex') or 0}`\n"
                    f"📱 *Total Numbers Found:* `{s.get('total_nums') or 0}`\n"
                    f"━━━━━━━━━━━━━━━━━━━━━"
                ),
                parse_mode="Markdown",
                reply_markup=admin_keyboard(),
            )
            return

        if text == "📢 Broadcast":
            user_states[user.id] = {"awaiting_broadcast": True}
            bot.send_message(chat_id, "📢 *Broadcast:*\nType message to send to ALL users:", parse_mode="Markdown")
            return

        if text == "👥 Recent Users":
            conn = get_conn()
            try:
                rows = conn.execute(
                    "SELECT user_id, first_name, username, total_numbers_found, joined_at "
                    "FROM users ORDER BY joined_at DESC LIMIT 10"
                ).fetchall()
            finally:
                conn.close()
            if not rows:
                bot.send_message(chat_id, "No users yet.", reply_markup=admin_keyboard())
                return
            msg = "👥 *Recent 10 Users*\n━━━━━━━━━━━━━━━━━━━━━\n\n"
            for i, r in enumerate(rows, 1):
                msg += (
                    f"*#{i}* {r['first_name'] or 'N/A'} (`{r['user_id']}`)\n"
                    f"📱 {r['total_numbers_found']} numbers | @{r['username'] or 'no_username'}\n\n"
                )
            bot.send_message(chat_id, msg, parse_mode="Markdown", reply_markup=admin_keyboard())
            return

    # ── Send New Link ─────────────────────────────────────────────────
    if text == "🔗 Send New Link":
        if active_jobs.get(user.id):
            bot.send_message(chat_id, "⚠️ *Extraction already running!* Please wait.", parse_mode="Markdown")
            return
        user_states[user.id] = {"awaiting_url": True}
        bot.send_message(
            chat_id,
            (
                "🔗 *Send Your Link*\n\n"
                "Paste your rotating or redirect link below:\n\n"
                "_Example:_ `https://example.com/redirect/abc123`"
            ),
            parse_mode="Markdown",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        return

    # ── My Stats ─────────────────────────────────────────────────────
    if text == "📊 My Stats":
        stats = get_user_stats(user.id)
        if not stats:
            bot.send_message(chat_id, "📊 No stats yet. Run your first extraction!", reply_markup=main_keyboard())
            return
        bot.send_message(
            chat_id,
            (
                f"📊 *Your Stats — DK Sharma Bot*\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 *User ID:* `{user.id}`\n"
                f"👤 *Name:* {stats.get('first_name') or 'Unknown'}\n"
                f"🔄 *Total Extractions:* `{stats.get('total_extractions', 0)}`\n"
                f"📱 *Total Numbers Found:* `{stats.get('total_numbers_found', 0)}`\n"
                f"📅 *Member Since:* `{stats.get('joined_at', 'N/A')}`\n"
                f"━━━━━━━━━━━━━━━━━━━━━"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
        return

    # ── My History ───────────────────────────────────────────────────
    if text == "📋 My History":
        history = get_user_history(user.id, limit=10)
        if not history:
            bot.send_message(
                chat_id,
                "📋 *No history yet.*\n\nRun your first extraction!",
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
                f"🔄 Cycles: `{h['cycles']}` | 📱 Found: `{h['unique_numbers']}` | 🔁 Dupes: `{h['duplicate_count']}`\n\n"
            )
        bot.send_message(chat_id, msg, parse_mode="Markdown", reply_markup=main_keyboard())
        return

    # ── Help ─────────────────────────────────────────────────────────
    if text == "❓ Help":
        bot.send_message(
            chat_id,
            (
                "❓ *How to Use DK Sharma Bot*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                "*Step 1:* Press *🔗 Send New Link*\n"
                "*Step 2:* Paste your rotating/redirect link\n"
                "*Step 3:* Choose cycles:\n"
                "  • `🧪 Test (1x)` — Quick single test\n"
                "  • `🚀 20 Times` — Medium run\n"
                "  • `⚡ 50 Times` — Full extraction\n"
                "  • `💎 100 Times` — Maximum\n"
                "*Step 4:* Wait, then download your `.txt` file\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "🔧 *Debug a link:*\n"
                "`/debug https://your-link`\n\n"
                "⚙️ *How it works:*\n"
                "• Real Chrome TLS fingerprint (curl\\_cffi)\n"
                "• Follows HTTP + JS + meta-refresh redirects\n"
                "• Scans HTML, scripts, meta tags, data attrs\n"
                "• Rotates User-Agent every request\n"
                "• Clears cookies before every visit\n"
                "• Human-like 1.5–3.5s delays"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
        return

    # ── Support ──────────────────────────────────────────────────────
    if text == "📞 Support":
        bot.send_message(
            chat_id,
            (
                "📞 *Support & Help*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "Having issues? Contact:\n\n"
                "👤 *Admin:* @YourAdminHandle\n\n"
                "_Made with ❤️ by DK Sharma_"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
        return

    # ── URL Capture ───────────────────────────────────────────────────
    if state.get("awaiting_url") or text.startswith(("http://", "https://")):
        if not text.startswith(("http://", "https://")):
            bot.send_message(
                chat_id,
                "⚠️ *Invalid URL.*\nSend a link starting with `http://` or `https://`",
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
                f"🎯 *How many times to visit this link?*\n\n"
                f"More visits = more chances to find different numbers\n"
                f"Duplicates are automatically filtered out."
            ),
            parse_mode="Markdown",
            reply_markup=extraction_keyboard(),
        )
        return

    # ── Extraction Count Selection ────────────────────────────────────
    if "url" in state:
        count = _EXTRACTION_COUNT_MAP.get(text)
        if count is not None:
            target_url = state["url"]
            user_states[user.id] = {}
            start_msg = bot.send_message(
                chat_id,
                (
                    f"⏳ *Starting extraction...*\n\n"
                    f"🔗 `{target_url[:45]}{'...' if len(target_url) > 45 else ''}`\n"
                    f"🔄 Planned cycles: `{count}`\n\n"
                    f"_Please wait. Do not close the chat._"
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

    # ── Fallback ──────────────────────────────────────────────────────
    bot.send_message(chat_id, "❓ Please choose an option from the menu below.", reply_markup=main_keyboard())


# =========================================================
# Entry Point
# =========================================================
if __name__ == "__main__":
    init_db()
    print("🤖 DK Sharma Bot starting...")
    print(f"Admin IDs : {ADMIN_IDS}")
    print(f"DB path   : {DB_FILE}")

    try:
        bot.remove_webhook()
        time.sleep(1)
        print("✅ Webhook removed.")
    except Exception as e:
        print(f"⚠️ Webhook warning: {e}")

    print("✅ Polling started...")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"❌ Polling error: {e}")
            time.sleep(5)
            print("🔄 Reconnecting...")
