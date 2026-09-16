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
from bs4 import BeautifulSoup

# ── curl_cffi replaces requests for real browser impersonation ──────────────
from curl_cffi import requests

# =========================================================
# Configuration & Constants
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8897758284:AAGeJ9DGdqR_goPbmIOLWSIfkiaO8PQMSa4")

ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "8753914631").split(",")
    if x.strip().isdigit()
]

# Railway.app: use /data/ for persistent storage, else current dir
DB_DIR = "/data" if os.path.isdir("/data") else "."
DB_FILE = os.path.join(DB_DIR, "bot_database.db")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# user_id -> {url, awaiting_url, awaiting_broadcast}
user_states: dict = {}

# active extraction jobs: user_id -> True (running) / False (cancelled)
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
            """SELECT COUNT(*)                 AS total_users,
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

# Comprehensive regex patterns covering every known WhatsApp redirect format
_WA_PATTERNS = [
    # Direct wa.me links with number
    re.compile(r'wa\.me/(?:message/[A-Z0-9]+[^"\'&\s]*?)?(\d{10,15})', re.IGNORECASE),
    # query param ?phone=
    re.compile(r'[?&]phone=(\d{10,15})', re.IGNORECASE),
    # whatsapp:// deep link
    re.compile(r'whatsapp://send\?phone=(\d{10,15})', re.IGNORECASE),
    # api.whatsapp.com/send
    re.compile(r'api\.whatsapp\.com/send/?\?(?:[^"\'<>\s]*&)?phone=(\d{10,15})', re.IGNORECASE),
    # JS window.location redirect containing whatsapp
    re.compile(r'window\.location(?:\.href)?\s*=\s*["\']([^"\']*whatsapp[^"\']*)["\']', re.IGNORECASE),
    re.compile(r'location\.replace\s*\(\s*["\']([^"\']*whatsapp[^"\']*)["\']', re.IGNORECASE),
    re.compile(r'(?<!\w)location\.href\s*=\s*["\']([^"\']*whatsapp[^"\']*)["\']', re.IGNORECASE),
    # href attributes pointing to wa.me or whatsapp
    re.compile(r'href=["\']([^"\']*(?:wa\.me|whatsapp)[^"\']*)["\']', re.IGNORECASE),
    # data-* attributes
    re.compile(r'data-(?:url|href|link|action)=["\']([^"\']*(?:wa\.me|whatsapp)[^"\']*)["\']', re.IGNORECASE),
    # onclick / onload handlers
    re.compile(r'on(?:click|load)=["\'][^"\']*(?:wa\.me|whatsapp)([^"\']*)["\']', re.IGNORECASE),
    # meta refresh redirect to whatsapp
    re.compile(r'content=["\'][^"\']*URL=([^"\']*(?:wa\.me|whatsapp)[^"\']*)["\']', re.IGNORECASE),
    # JSON embedded URLs
    re.compile(r'"(?:url|link|redirect|whatsapp|href)"\s*:\s*"([^"]*(?:wa\.me|whatsapp)[^"]*)"', re.IGNORECASE),
    # Catch-all: any whatsapp/wa.me string followed by a digit sequence
    re.compile(r'(?:wa\.me|whatsapp)[^\d]*(\d{10,15})', re.IGNORECASE),
]

# Rotating real Chrome/Firefox User-Agents
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Android 13; Mobile; rv:109.0) Gecko/121.0 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.0.0",
]


def _get_random_headers() -> dict:
    """Generate randomised browser-like headers on every request."""
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice([
            "en-US,en;q=0.9",
            "en-GB,en;q=0.9",
            "en-IN,en;q=0.9,hi;q=0.8",
        ]),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _extract_numbers_from_text(text: str) -> tuple[set[str], int]:
    """
    Extract WhatsApp numbers from any raw text (URL string, HTML body, JS code).
    Returns (unique_numbers_set, raw_match_count).
    raw_match_count counts every time any number appears (including repeats).
    """
    found: set[str] = set()
    raw_count = 0

    for pattern in _WA_PATTERNS:
        for match in pattern.findall(text):
            # match can be a full URL like "https://wa.me/919876543210"
            # or a bare number "919876543210" — extract all digit runs
            for num_str in re.findall(r'\d+', match):
                clean = num_str.strip()
                if 10 <= len(clean) <= 15:
                    raw_count += 1
                    found.add(clean)

    return found, raw_count


def _extract_from_html_deep(html: str) -> tuple[set[str], int]:
    """
    Deep BeautifulSoup parse: finds WhatsApp numbers inside
    <a href>, <script>, <meta>, and data-* / onclick attributes.
    """
    found: set[str] = set()
    raw_count = 0

    try:
        soup = BeautifulSoup(html, "html.parser")

        # <a href="...">
        for tag in soup.find_all("a", href=True):
            nums, cnt = _extract_numbers_from_text(tag["href"])
            found.update(nums)
            raw_count += cnt

        # <script> contents (JS redirects)
        for script in soup.find_all("script"):
            script_text = script.get_text() or ""
            nums, cnt = _extract_numbers_from_text(script_text)
            found.update(nums)
            raw_count += cnt

        # <meta content="..."> (meta-refresh)
        for meta in soup.find_all("meta"):
            content = meta.get("content", "")
            if content:
                nums, cnt = _extract_numbers_from_text(content)
                found.update(nums)
                raw_count += cnt

        # Every tag: data-* and onclick/onload attributes
        for tag in soup.find_all(True):
            for attr_name, attr_val in tag.attrs.items():
                if isinstance(attr_val, str) and (
                    attr_name.startswith("data-")
                    or attr_name in ("onclick", "onload", "href", "action")
                ):
                    nums, cnt = _extract_numbers_from_text(attr_val)
                    found.update(nums)
                    raw_count += cnt

    except Exception:
        pass  # BeautifulSoup failure is non-fatal

    return found, raw_count


def _follow_js_redirect(html: str, base_url: str) -> str | None:
    """
    Detect and resolve JavaScript / meta-refresh redirects.
    Returns the absolute redirect URL or None.
    """
    # <meta http-equiv="refresh" content="0; URL=...">
    meta_match = re.search(
        r'<meta[^>]+http-equiv=["\']refresh["\'][^>]+content=["\'][^"\']*URL=([^"\']+)["\']',
        html, re.IGNORECASE,
    )
    if meta_match:
        return urllib.parse.urljoin(base_url, meta_match.group(1).strip())

    # window.location.href = "URL" or window.location = "URL"
    m = re.search(r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return urllib.parse.urljoin(base_url, m.group(1).strip())

    # location.replace("URL")
    m = re.search(r'location\.replace\s*\(\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return urllib.parse.urljoin(base_url, m.group(1).strip())

    # location.href = "URL"
    m = re.search(r'(?<!\w)location\.href\s*=\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return urllib.parse.urljoin(base_url, m.group(1).strip())

    return None


def _visit_url(url: str, session) -> tuple[set[str], int]:
    """
    Visit a URL with curl_cffi (impersonates Chrome120) and extract ALL
    WhatsApp numbers found in HTTP redirect chain + HTML body + JS redirects.

    Returns (found_numbers_set, raw_match_count).
    """
    found: set[str] = set()
    raw_count = 0

    # Clear cookies so every visit is a fresh, independent session
    session.cookies.clear()

    resp = session.get(
        url,
        impersonate="chrome120",   # ← Cloudflare bypass via TLS fingerprint
        allow_redirects=True,
        timeout=15,
        headers=_get_random_headers(),
    )

    # 1. Numbers in the final resolved URL (after all HTTP redirects)
    nums, cnt = _extract_numbers_from_text(resp.url)
    found.update(nums)
    raw_count += cnt

    # 2. Numbers in every intermediate redirect URL
    for r in resp.history:
        nums, cnt = _extract_numbers_from_text(r.url)
        found.update(nums)
        raw_count += cnt

    body = resp.text

    # 3. Raw regex pass over the entire HTML body
    nums, cnt = _extract_numbers_from_text(body)
    found.update(nums)
    raw_count += cnt

    # 4. Deep BeautifulSoup parse (<script>, <a>, <meta>, data-attrs)
    nums, cnt = _extract_from_html_deep(body)
    found.update(nums)
    raw_count += cnt

    # 5. Follow JS / meta-refresh redirect if present
    js_url = _follow_js_redirect(body, resp.url)
    if js_url and js_url != resp.url:
        nums, cnt = _extract_numbers_from_text(js_url)
        found.update(nums)
        raw_count += cnt

        # If the JS redirect is NOT itself a WhatsApp URL, fetch it too
        if not any(kw in js_url.lower() for kw in ("whatsapp", "wa.me")):
            try:
                session.cookies.clear()
                resp2 = session.get(
                    js_url,
                    impersonate="chrome120",
                    allow_redirects=True,
                    timeout=10,
                    headers=_get_random_headers(),
                )
                nums, cnt = _extract_numbers_from_text(resp2.url)
                found.update(nums)
                raw_count += cnt

                nums, cnt = _extract_numbers_from_text(resp2.text)
                found.update(nums)
                raw_count += cnt

                nums, cnt = _extract_from_html_deep(resp2.text)
                found.update(nums)
                raw_count += cnt
            except Exception:
                pass

    return found, raw_count


# =========================================================
# Extraction Worker Thread
# =========================================================
def extraction_worker(
    chat_id: int,
    user_id: int,
    url: str,
    count: int,
    message_id: int,
) -> None:
    active_jobs[user_id] = True

    # One curl_cffi session per job (connection pooling + shared TLS state)
    session = requests.Session()

    found_numbers: set[str] = set()
    total_ok = 0
    errors = 0
    # total_raw_matches counts every number appearance (incl. repeats across cycles)
    total_raw_matches = 0

    for i in range(1, count + 1):
        if not active_jobs.get(user_id, True):
            break

        try:
            # Visit the original URL unchanged (no cache-buster to avoid breaking links)
            nums, raw = _visit_url(url, session)
            total_ok += 1
            total_raw_matches += raw
            found_numbers.update(nums)
        except Exception:
            errors += 1

        # Progress update every 5 requests or on the very last one
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
                pass

        # Human-like delay: randomised between 1.5 – 3.0 seconds
        # This prevents IP-rate-limit blocks and mimics a real human browsing pace
        time.sleep(random.uniform(1.5, 3.0))

    # ── Job Complete ──────────────────────────────────────────────────
    active_jobs.pop(user_id, None)

    unique_count = len(found_numbers)

    # CORRECT duplicate math:
    # raw_matches = total times any number appeared across all cycles
    # unique_count = how many distinct numbers we found
    # duplicates = raw_matches minus one "first-seen" count per unique number
    duplicate_count = max(0, total_raw_matches - unique_count)

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
                "• Link uses an app-only deep link (not web-accessible)\n"
                "• Website loads the number via API after page render (AJAX)\n"
                "• Link has expired or is no longer valid\n"
                "• Target website uses an unknown redirect format\n\n"
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
            f"automatically, in bulk, with Cloudflare bypass.\n\n"
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


@bot.message_handler(commands=["stats"])
def cmd_stats(message: types.Message) -> None:
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

    # ── Global cancel / main menu ──────────────────────────────────────
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

    # ── Admin broadcast input ─────────────────────────────────────────
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
                bot.send_message(uid, text, parse_mode="Markdown")
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
                "📢 *Broadcast Message*\n\nType the message to send to ALL users:",
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

    # ── Send New Link ─────────────────────────────────────────────────
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
                "A link that briefly visits a website, then redirects to WhatsApp with a "
                "phone number. The number may change each time — this bot catches all unique ones!\n\n"
                "✅ *Duplicate numbers are automatically removed.*\n\n"
                "⚙️ *How the bot works:*\n"
                "• Uses curl\\_cffi to impersonate Chrome (bypasses Cloudflare)\n"
                "• Follows HTTP AND JavaScript/meta-refresh redirects\n"
                "• Scans HTML, inline scripts, and meta tags\n"
                "• Rotates User-Agent to avoid detection\n"
                "• Clears cookies between every request\n"
                "• Human-like delays between requests"
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
    print(f"Database path: {DB_FILE}")

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
