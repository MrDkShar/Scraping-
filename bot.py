import os
import re
import io
import time
import random
import sqlite3
import threading
import html
import urllib.parse
import urllib.request
import urllib.error
import http.cookiejar
from datetime import datetime

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

# =========================================================
# Configuration & Constants
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8267372667:AAFUCPQ9kv60DwkRqi54Ge7YasN3NYs2XNs")

ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "8753914631").split(",")
    if x.strip().isdigit()
]

# Using HTML parse mode for 100% stability against underscores, URLs, and symbols
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

MAINTENANCE_MODE = False

# user_id -> dict state
user_states: dict = {}

# active extraction jobs: user_id -> bool
active_jobs: dict = {}

# =========================================================
# Database Management
# =========================================================
DB_FILE = "bot_database.db"
_db_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _db_lock:
        conn = get_conn()
        try:
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
        finally:
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
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
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
                      COALESCE(SUM(total_extractions), 0)   AS total_ex,
                      COALESCE(SUM(total_numbers_found), 0) AS total_nums
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
# Pure Python AES-128-CBC Decryptor
# Solves ByetHost / InfinityFree / site.je __test Challenge
# =========================================================
_AES_SBOX = (
    0x63, 0x7C, 0x77, 0x7B, 0xF2, 0x6B, 0x6F, 0xC5, 0x30, 0x01, 0x67, 0x2B, 0xFE, 0xD7, 0xAB, 0x76,
    0xCA, 0x82, 0xC9, 0x7D, 0xFA, 0x59, 0x47, 0xF0, 0xAD, 0xD4, 0xA2, 0xAF, 0x9C, 0xA4, 0x72, 0xC0,
    0xB7, 0xFD, 0x93, 0x26, 0x36, 0x3F, 0xF7, 0xCC, 0x34, 0xA5, 0xE5, 0xF1, 0x71, 0xD8, 0x31, 0x15,
    0x04, 0xC7, 0x23, 0xC3, 0x18, 0x96, 0x05, 0x9A, 0x07, 0x12, 0x80, 0xE2, 0xEB, 0x27, 0xB2, 0x75,
    0x09, 0x83, 0x2C, 0x1A, 0x1B, 0x6E, 0x5A, 0xA0, 0x52, 0x3B, 0xD6, 0xB3, 0x29, 0xE3, 0x2F, 0x84,
    0x53, 0xD1, 0x00, 0xED, 0x20, 0xFC, 0xB1, 0x5B, 0x6A, 0xCB, 0xBE, 0x39, 0x4A, 0x4C, 0x58, 0xCF,
    0xD0, 0xEF, 0xAA, 0xFB, 0x43, 0x4D, 0x33, 0x85, 0x45, 0xF9, 0x02, 0x7F, 0x50, 0x3C, 0x9F, 0xA8,
    0x51, 0xA3, 0x40, 0x8F, 0x92, 0x9D, 0x38, 0xF5, 0xBC, 0xB6, 0xDA, 0x21, 0x10, 0xFF, 0xF3, 0xD2,
    0xCD, 0x0C, 0x13, 0xEC, 0x5F, 0x97, 0x44, 0x17, 0xC4, 0xA7, 0x7E, 0x3D, 0x64, 0x5D, 0x19, 0x73,
    0x60, 0x81, 0x4F, 0xDC, 0x22, 0x2A, 0x90, 0x88, 0x46, 0xEE, 0xB8, 0x14, 0xDE, 0x5E, 0x0B, 0xDB,
    0xE0, 0x32, 0x3A, 0x0A, 0x49, 0x06, 0x24, 0x5C, 0xC2, 0xD3, 0xAC, 0x62, 0x91, 0x95, 0xE4, 0x79,
    0xE7, 0xC8, 0x37, 0x6D, 0x8D, 0xD5, 0x4E, 0xA9, 0x6C, 0x56, 0xF4, 0xEA, 0x65, 0x7A, 0xAE, 0x08,
    0xBA, 0x78, 0x25, 0x2E, 0x1C, 0xA6, 0xB4, 0xC6, 0xE8, 0xDD, 0x74, 0x1F, 0x4B, 0xBD, 0x8B, 0x8A,
    0x70, 0x3E, 0xB5, 0x66, 0x48, 0x03, 0xF6, 0x0E, 0x61, 0x35, 0x57, 0xB9, 0x86, 0xC1, 0x1D, 0x9E,
    0xE1, 0xF8, 0x98, 0x11, 0x69, 0xD9, 0x8E, 0x94, 0x9B, 0x1E, 0x87, 0xE9, 0xCE, 0x55, 0x28, 0xDF,
    0x8C, 0xA1, 0x89, 0x0D, 0xBF, 0xE6, 0x42, 0x68, 0x41, 0x99, 0x2D, 0x0F, 0xB0, 0x54, 0xBB, 0x16,
)

_AES_INV_SBOX = [0] * 256
for _i, _v in enumerate(_AES_SBOX):
    _AES_INV_SBOX[_v] = _i

_AES_RCON = (0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1B, 0x36)


def _sub_word(w: int) -> int:
    return (
        (_AES_SBOX[(w >> 24) & 0xFF] << 24)
        | (_AES_SBOX[(w >> 16) & 0xFF] << 16)
        | (_AES_SBOX[(w >> 8) & 0xFF] << 8)
        | _AES_SBOX[w & 0xFF]
    )


def _rot_word(w: int) -> int:
    return ((w << 8) & 0xFFFFFFFF) | (w >> 24)


def _key_schedule(key_bytes: bytes) -> list[int]:
    w = []
    for i in range(4):
        w.append(
            (key_bytes[4 * i] << 24)
            | (key_bytes[4 * i + 1] << 16)
            | (key_bytes[4 * i + 2] << 8)
            | key_bytes[4 * i + 3]
        )
    for i in range(4, 44):
        temp = w[i - 1]
        if i % 4 == 0:
            temp = _sub_word(_rot_word(temp)) ^ (_AES_RCON[i // 4] << 24)
        w.append(w[i - 4] ^ temp)
    return w


def _xtime(a: int) -> int:
    return ((a << 1) ^ 0x1B) & 0xFF if (a & 0x80) else (a << 1)


def _gmul(a: int, b: int) -> int:
    p = 0
    for _ in range(8):
        if b & 1:
            p ^= a
        hi = a & 0x80
        a = (a << 1) & 0xFF
        if hi:
            a ^= 0x1B
        b >>= 1
    return p


def _inv_mix_col(c: list[int]) -> list[int]:
    return [
        _gmul(c[0], 0x0E) ^ _gmul(c[1], 0x0B) ^ _gmul(c[2], 0x0D) ^ _gmul(c[3], 0x09),
        _gmul(c[0], 0x09) ^ _gmul(c[1], 0x0E) ^ _gmul(c[2], 0x0B) ^ _gmul(c[3], 0x0D),
        _gmul(c[0], 0x0D) ^ _gmul(c[1], 0x09) ^ _gmul(c[2], 0x0E) ^ _gmul(c[3], 0x0B),
        _gmul(c[0], 0x0B) ^ _gmul(c[1], 0x0D) ^ _gmul(c[2], 0x09) ^ _gmul(c[3], 0x0E),
    ]


def _decrypt_single_block(block: bytes, w: list[int]) -> list[int]:
    state = [[block[r + 4 * c] for c in range(4)] for r in range(4)]
    for c in range(4):
        rk = w[40 + c]
        for r in range(4):
            state[r][c] ^= (rk >> (24 - 8 * r)) & 0xFF

    for round_num in range(9, 0, -1):
        state[1] = state[1][3:] + state[1][:3]
        state[2] = state[2][2:] + state[2][:2]
        state[3] = state[3][1:] + state[3][:1]

        for r in range(4):
            for c in range(4):
                state[r][c] = _AES_INV_SBOX[state[r][c]]

        for c in range(4):
            rk = w[round_num * 4 + c]
            for r in range(4):
                state[r][c] ^= (rk >> (24 - 8 * r)) & 0xFF

        for c in range(4):
            col = [state[r][c] for r in range(4)]
            new_col = _inv_mix_col(col)
            for r in range(4):
                state[r][c] = new_col[r]

    state[1] = state[1][3:] + state[1][:3]
    state[2] = state[2][2:] + state[2][:2]
    state[3] = state[3][1:] + state[3][:1]

    for r in range(4):
        for c in range(4):
            state[r][c] = _AES_INV_SBOX[state[r][c]]

    for c in range(4):
        rk = w[c]
        for r in range(4):
            state[r][c] ^= (rk >> (24 - 8 * r)) & 0xFF

    out = []
    for c in range(4):
        for r in range(4):
            out.append(state[r][c])
    return out


def decrypt_byet_challenge(c_hex: str, a_key_hex: str, b_iv_hex: str) -> str:
    """Zero-dependency AES-128-CBC decryption to solve InfinityFree/ByetHost test challenge."""
    c = bytes.fromhex(c_hex)
    a = bytes.fromhex(a_key_hex)
    b = bytes.fromhex(b_iv_hex)
    w = _key_schedule(a)
    dec = _decrypt_single_block(c, w)
    res = bytes([dec[i] ^ b[i] for i in range(16)])
    return res.hex()


# =========================================================
# Robust Number Extraction & Clean Normalization
# =========================================================
_WA_PATTERNS = [
    # Standard wa.me patterns (including /p/, /qr/, etc.)
    re.compile(r'wa\.me/(?:p/|qr/)?\+?(\d{10,15})', re.IGNORECASE),
    # Direct query string patterns (phone=, number=, to=)
    re.compile(r'(?:phone|number|mobile|to)=\+?(\d{10,15})', re.IGNORECASE),
    # whatsapp:// or intent:// schemes
    re.compile(r'(?:whatsapp|intent)://send\?.*?(?:phone|number)=\+?(\d{10,15})', re.IGNORECASE),
    # api.whatsapp.com or web.whatsapp.com
    re.compile(r'(?:api|web)\.whatsapp\.com/send/?\??.*?(?:phone|number)=\+?(\d{10,15})', re.IGNORECASE),
    # wa.me/message links with phone
    re.compile(r'wa\.me/(?:message/[A-Za-z0-9_-]+.*?)?\+?(\d{10,15})', re.IGNORECASE),
    # JSON or JS object assignments e.g. "whatsapp": "+919876543210"
    re.compile(r'["\'](?:whatsapp|wa_number|phone_number|phone)["\']\s*:\s*["\']\+?(\d{10,15})["\']', re.IGNORECASE),
    # HTML data attributes e.g. data-phone="919876543210" or data-number="..."
    re.compile(r'data-(?:phone|number|whatsapp)=["\']\+?(\d{10,15})["\']', re.IGNORECASE),
    # tel: links inside WhatsApp buttons or widgets
    re.compile(r'href=["\']tel:\+?(\d{10,15})["\']', re.IGNORECASE),
]


def clean_phone_number(raw: str) -> str | None:
    """Normalizes raw digits into a clean E.164-compatible unique representation."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    # Strip leading 00 (international call prefix)
    if digits.startswith("00") and len(digits) > 12:
        digits = digits[2:]
    # Valid WhatsApp numbers are between 10 and 15 digits
    if 10 <= len(digits) <= 15:
        return digits
    return None


def extract_numbers_from_text(text: str) -> set[str]:
    """Extracts all WhatsApp numbers from raw HTML, JavaScript, URL query strings, or text."""
    found: set[str] = set()
    if not text:
        return found

    # Check raw, unquoted, and unescaped variants
    samples = [
        text,
        urllib.parse.unquote(text),
        urllib.parse.unquote_plus(text),
        html.unescape(text),
    ]

    for sample in samples:
        for pattern in _WA_PATTERNS:
            for match in pattern.findall(sample):
                if isinstance(match, tuple):
                    match = match[0]
                cleaned = clean_phone_number(match)
                if cleaned:
                    found.add(cleaned)

    return found


# =========================================================
# Custom Redirect Handler (Captures whatsapp:// without crash)
# =========================================================
class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """
    Catches redirects to non-HTTP protocols (whatsapp://, intent://, tg://)
    and extracts destination URLs without crashing urllib.
    """
    def __init__(self):
        super().__init__()
        self.collected_redirect_targets: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.collected_redirect_targets.append(newurl)
        # If redirecting to a non-HTTP scheme, stop following and return None
        parsed = urllib.parse.urlparse(newurl)
        if parsed.scheme.lower() not in ("http", "https"):
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# =========================================================
# Rotating Scraper Session with Challenge Solver
# =========================================================
class UniversalRotatingScraper:
    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.redirect_handler = SafeRedirectHandler()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cj),
            self.redirect_handler,
        )
        self.cached_test_cookie = None

    def fetch(self, url: str) -> tuple[str, str, list[str]]:
        """
        Visits the URL, follows all HTTP and Client-Side redirects,
        solves ByetHost/InfinityFree test challenges, and returns:
        (final_url, response_body, all_visited_and_redirect_urls)
        """
        parsed = urllib.parse.urlparse(url)
        domain = parsed.hostname

        self.opener.addheaders = [
            ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
            ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"),
            ("Accept-Language", "en-US,en;q=0.9"),
            ("Cache-Control", "no-cache"),
            ("Pragma", "no-cache"),
            ("Sec-Ch-Ua", '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'),
            ("Sec-Ch-Ua-Mobile", "?0"),
            ("Sec-Ch-Ua-Platform", '"Windows"'),
            ("Upgrade-Insecure-Requests", "1"),
        ]

        # Reuse valid __test cookie if previously solved
        if self.cached_test_cookie and domain:
            c_obj = http.cookiejar.Cookie(
                version=0, name="__test", value=self.cached_test_cookie, port=None, port_specified=False,
                domain=domain, domain_specified=True, domain_initial_dot=False,
                path="/", path_specified=True, secure=False, expires=None, discard=True,
                comment=None, comment_url=None, rest={"HttpOnly": None}, rfc2109=False,
            )
            self.cj.set_cookie(c_obj)

        visited_urls: list[str] = [url]
        req = urllib.request.Request(url)

        try:
            resp = self.opener.open(req, timeout=12)
            current_url = resp.geturl()
            visited_urls.append(current_url)
            body = resp.read().decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as e:
            current_url = e.geturl() or url
            visited_urls.append(current_url)
            body = e.read().decode("utf-8", errors="ignore") if hasattr(e, "read") else ""
        except Exception:
            # Check if any redirect was collected before exception
            visited_urls.extend(self.redirect_handler.collected_redirect_targets)
            raise

        visited_urls.extend(self.redirect_handler.collected_redirect_targets)

        # ── 1. Check for ByetHost / InfinityFree slowAES __test challenge ──
        if "slowAES" in body or ("toNumbers(" in body and "__test=" in body):
            matches = re.findall(r'toNumbers\("([a-f0-9]+)"\)', body)
            if len(matches) >= 3:
                a_key, b_iv, c_cipher = matches[0], matches[1], matches[2]
                self.cached_test_cookie = decrypt_byet_challenge(c_cipher, a_key, b_iv)
                if domain:
                    c_obj = http.cookiejar.Cookie(
                        version=0, name="__test", value=self.cached_test_cookie, port=None, port_specified=False,
                        domain=domain, domain_specified=True, domain_initial_dot=False,
                        path="/", path_specified=True, secure=False, expires=None, discard=True,
                        comment=None, comment_url=None, rest={"HttpOnly": None}, rfc2109=False,
                    )
                    self.cj.set_cookie(c_obj)

                loc_match = re.search(r'location\.href\s*=\s*["\'](.*?)["\']', body)
                next_dest = loc_match.group(1) if loc_match else (url + ("&i=1" if "?" in url else "?i=1"))
                next_url = urllib.parse.urljoin(current_url, next_dest)

                visited_urls.append(next_url)
                resp2 = self.opener.open(urllib.request.Request(next_url), timeout=12)
                current_url = resp2.geturl()
                visited_urls.append(current_url)
                body = resp2.read().decode("utf-8", errors="ignore")

        # ── 2. Check for HTML Meta Refresh or JavaScript Redirections ──
        for _ in range(3):
            # Regex covering all meta refresh variations
            meta_refresh = re.search(
                r'<meta[^>]*?http-equiv\s*=\s*["\']?refresh["\']?[^>]*?content\s*=\s*["\']?[^"\'>]*?url\s*=\s*([^\s"\'\';>]+)',
                body,
                re.IGNORECASE,
            )
            if meta_refresh:
                dest = meta_refresh.group(1).strip()
                dest_url = urllib.parse.urljoin(current_url, dest)
                visited_urls.append(dest_url)
                if dest_url.lower().startswith(("http://", "https://")):
                    try:
                        resp = self.opener.open(urllib.request.Request(dest_url), timeout=12)
                        current_url = resp.geturl()
                        visited_urls.append(current_url)
                        body = resp.read().decode("utf-8", errors="ignore")
                        continue
                    except Exception:
                        pass
                break

            # Robust JavaScript redirect patterns: location.href = ..., location.replace(...), location.assign(...)
            js_match = re.search(
                r'(?:window\.|document\.|top\.)?location(?:\.href|\.replace|\.assign)?\s*(?:=|\()\s*["\'](https?://[^"\']+|whatsapp://[^"\']+|wa\.me/[^"\']+)["\']',
                body,
                re.IGNORECASE,
            )
            if js_match:
                dest_url = js_match.group(1).strip()
                visited_urls.append(dest_url)
                if dest_url.lower().startswith(("http://", "https://")):
                    try:
                        resp = self.opener.open(urllib.request.Request(dest_url), timeout=12)
                        current_url = resp.geturl()
                        visited_urls.append(current_url)
                        body = resp.read().decode("utf-8", errors="ignore")
                        continue
                    except Exception:
                        pass
                break

            break

        return current_url, body, visited_urls


def add_cache_buster(url: str, cycle: int) -> str:
    """Appends unique dynamic cache busting parameters to defeat proxy caching."""
    timestamp = int(time.time() * 1000)
    salt = random.randint(1000, 9999)
    cb = f"{timestamp}_{cycle}_{salt}"
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}_cb={cb}"


# =========================================================
# Result Messenger & Copy Button Generator
# =========================================================
def build_copy_keyboard(numbers_text: str) -> types.InlineKeyboardMarkup:
    """Builds inline keyboard with one-click copy button."""
    markup = types.InlineKeyboardMarkup()
    # Telegram Bot API 7.8+ direct copy button
    try:
        copy_btn = types.InlineKeyboardButton(
            text="📋 Copy All Numbers",
            copy_text=types.CopyTextButton(text=numbers_text),
        )
        markup.add(copy_btn)
    except Exception:
        # Fallback for older library builds
        markup.add(
            types.InlineKeyboardButton(
                text="📋 Copy All Numbers",
                switch_inline_query=numbers_text[:250],
            )
        )
    return markup


# =========================================================
# Worker Thread for Extractions
# =========================================================
def extraction_worker(
    chat_id: int,
    user_id: int,
    url: str,
    count: int,
    progress_message_id: int,
) -> None:
    active_jobs[user_id] = True
    session = UniversalRotatingScraper()

    found_numbers: list[str] = []
    found_set: set[str] = set()
    total_numbers_seen = 0
    total_ok = 0
    errors = 0
    last_ui_update = 0.0
    start_time = time.time()

    for i in range(1, count + 1):
        if not active_jobs.get(user_id, True):
            break

        target = add_cache_buster(url, i)
        newly_found_in_this_cycle: list[str] = []

        try:
            final_url, body, visited_urls = session.fetch(target)
            total_ok += 1

            cycle_numbers: set[str] = set()
            for v_url in visited_urls:
                cycle_numbers.update(extract_numbers_from_text(v_url))
            cycle_numbers.update(extract_numbers_from_text(body))

            total_numbers_seen += len(cycle_numbers)

            for num in sorted(cycle_numbers):
                if num not in found_set:
                    found_set.add(num)
                    found_numbers.append(num)
                    newly_found_in_this_cycle.append(num)

        except Exception:
            errors += 1

        # Real-time alert: Notify in chat as soon as a new number is discovered
        if newly_found_in_this_cycle:
            alert_text = "🎯 <b>New WhatsApp Number Discovered:</b>\n" + "\n".join(
                f"<code>+{n}</code>" for n in newly_found_in_this_cycle
            )
            try:
                bot.send_message(chat_id, alert_text)
            except Exception:
                pass

        # Smooth progress indicator with throttling to respect Telegram limits
        now = time.time()
        if (now - last_ui_update > 2.0) or i == count:
            last_ui_update = now
            elapsed = max(int(now - start_time), 1)
            pct = int((i / count) * 100)
            done = int((i / count) * 10)
            bar = "█" * done + "░" * (10 - done)

            recent_preview = ""
            if found_numbers:
                recent_five = found_numbers[-4:]
                preview_list = " | ".join(f"+{n}" for n in recent_five)
                recent_preview = f"\n📱 <b>Recent:</b> <code>{html.escape(preview_list)}</code>\n"

            progress_text = (
                f"⏳ <b>Extraction In Progress</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔗 <b>URL:</b> <code>{html.escape(url[:42])}{'...' if len(url) > 42 else ''}</code>\n"
                f"📊 <b>Progress:</b> <code>[{bar}] {pct}%</code>\n"
                f"🔄 <b>Cycles:</b> <code>{i}/{count}</code> (⚡ {elapsed}s elapsed)\n"
                f"✅ <b>Success:</b> <code>{total_ok}</code> | ❌ <b>Failed:</b> <code>{errors}</code>\n"
                f"🎯 <b>Unique Numbers:</b> <code>{len(found_numbers)}</code>"
                f"{recent_preview}"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Extracting rotating redirects...</i>"
            )

            try:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_message_id,
                    text=progress_text,
                )
            except ApiTelegramException:
                pass
            except Exception:
                pass

        time.sleep(0.2)

    # ── Job Completed ─────────────────────────────────────────────────
    active_jobs.pop(user_id, None)

    unique_count = len(found_numbers)
    duplicate_count = max(total_numbers_seen - unique_count, 0)
    sorted_numbers = sorted(found_numbers)

    # Persist stats & history
    update_user_stats(user_id, unique_count)
    save_history(user_id, url, count, unique_count, duplicate_count)

    if unique_count > 0:
        numbers_plain_text = "\n".join(f"+{num}" for num in sorted_numbers)

        # ── 1. Show extracted numbers CLEARLY AT THE TOP ──────────────
        results_header = (
            f"📱 <b>EXTRACTED WHATSAPP NUMBERS ({unique_count})</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
        )

        # Keep messages within Telegram's 4096-char ceiling
        CHUNK_LIMIT = 3000
        lines = [f"+{num}" for num in sorted_numbers]
        chunks = []
        cur_chunk = ""
        for line in lines:
            if len(cur_chunk) + len(line) + 1 > CHUNK_LIMIT:
                chunks.append(cur_chunk.strip())
                cur_chunk = line + "\n"
            else:
                cur_chunk += line + "\n"
        if cur_chunk.strip():
            chunks.append(cur_chunk.strip())

        copy_keyboard = build_copy_keyboard(numbers_plain_text)

        # Deliver results with numbers at the top + Copy All button
        for idx, chunk in enumerate(chunks):
            if idx == 0:
                body = (
                    f"{results_header}"
                    f"<code>{html.escape(chunk)}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🎯 <b>Total Unique:</b> <code>{unique_count}</code> | "
                    f"🔁 <b>Filtered Dupes:</b> <code>{duplicate_count}</code>\n"
                    f"✅ <b>Successful Visits:</b> <code>{total_ok}/{count}</code>"
                )
                try:
                    bot.send_message(
                        chat_id,
                        body,
                        reply_markup=copy_keyboard,
                    )
                except Exception:
                    bot.send_message(chat_id, f"Extracted Numbers:\n{chunk}")
            else:
                body = (
                    f"📱 <b>Numbers (Part {idx + 1})</b>\n\n"
                    f"<code>{html.escape(chunk)}</code>"
                )
                try:
                    bot.send_message(chat_id, body)
                except Exception:
                    bot.send_message(chat_id, chunk)

        # ── 2. Deliver as downloadable .txt file ───────────────────────
        try:
            file_content = (
                "DK Sharma Bot — WhatsApp Number Extractor Results\n"
                f"Source URL: {url}\n"
                f"Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Total Cycles: {count}\n"
                f"Successful Requests: {total_ok}\n"
                f"Failed Requests: {errors}\n"
                f"Unique Numbers Found: {unique_count}\n"
                f"Duplicates Filtered: {duplicate_count}\n"
                + ("=" * 48)
                + "\n\n"
                + numbers_plain_text
                + "\n"
            )

            file_stream = io.BytesIO(file_content.encode("utf-8"))
            file_stream.name = f"whatsapp_numbers_{int(time.time())}.txt"

            bot.send_document(
                chat_id,
                file_stream,
                caption=(
                    f"📁 <b>Downloadable Results File</b>\n"
                    f"📱 <code>Total Unique Numbers: {unique_count}</code>\n"
                    f"<i>Tap above to save or export your list</i>"
                ),
                reply_markup=main_keyboard(),
            )
        except Exception as e:
            bot.send_message(chat_id, f"⚠️ Notice: Could not send file ({html.escape(str(e))})", reply_markup=main_keyboard())

    else:
        # Fallback message when zero numbers were found
        bot.send_message(
            chat_id,
            (
                "⚠️ <b>No WhatsApp numbers found</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Requests completed:</b> <code>{total_ok}/{count}</code>\n"
                f"❌ <b>Errors encountered:</b> <code>{errors}</code>\n\n"
                "<b>Potential reasons:</b>\n"
                "• The destination server is offline or returned an error\n"
                "• The rotating link pool has expired\n"
                "• The page requires manual user interaction or unsupported captcha\n\n"
                "<i>Tip: Test the URL with 🧪 Test (1x) first to verify status.</i>"
            ),
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
    maintenance_label = "🔴 Maintenance: ON" if MAINTENANCE_MODE else "🟢 Maintenance: OFF"
    markup.add(
        types.KeyboardButton("📈 Bot Stats"),
        types.KeyboardButton("📢 Broadcast"),
        types.KeyboardButton("👥 Recent Users"),
        types.KeyboardButton(maintenance_label),
        types.KeyboardButton("🔙 Main Menu"),
    )
    return markup


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
            f"👋 <b>Welcome to DK Sharma Bot!</b>\n\n"
            f"🤖 <b>Universal WhatsApp Number Extractor</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Made by <b>DK Sharma</b> 🎯\n\n"
            f"🔥 <b>Capabilities:</b>\n"
            f"• Supports <b>any</b> HTTP/HTTPS domain or URL shortener\n"
            f"• Automatic redirect following (HTTP 301/302, JS, Meta Refresh)\n"
            f"• Intercepts <code>whatsapp://</code> & <code>wa.me</code> redirects without errors\n"
            f"• Bypasses anti-bot challenges (ByetHost/site.je AES-128)\n"
            f"• Displays numbers at the top with <b>Copy All</b> & downloadable <b>.txt</b>\n\n"
            f"📌 <b>Quick Start:</b>\n"
            f"1️⃣ Tap <b>🔗 Send New Link</b>\n"
            f"2️⃣ Paste your URL\n"
            f"3️⃣ Pick extraction cycles (1x, 20x, 50x, 100x)\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Select an option below:</b>"
        ),
        reply_markup=main_keyboard(),
    )


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "❌ <b>Access Denied.</b> You are not an admin.")
        return
    bot.send_message(
        message.chat.id,
        "🔐 <b>Admin Control Panel</b>\n━━━━━━━━━━━━━━━━━━━━━\nSelect an administrative task:",
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
    global MAINTENANCE_MODE

    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()

    register_user(user.id, user.username, user.first_name)
    state = user_states.get(user.id, {})

    # Maintenance Mode Check
    if MAINTENANCE_MODE and user.id not in ADMIN_IDS:
        bot.send_message(
            chat_id,
            (
                "🔧 <b>Bot Under Scheduled Maintenance</b>\n\n"
                "System updates are in progress. Please check back shortly!\n"
                "<i>— DK Sharma Bot</i>"
            ),
        )
        return

    # Cancel & Main Menu
    if text in ("❌ Cancel", "🔙 Main Menu"):
        active_jobs[user.id] = False
        user_states[user.id] = {}
        bot.send_message(
            chat_id,
            "🏠 <b>Main Menu</b>",
            reply_markup=main_keyboard(),
        )
        return

    # Admin: Broadcast Message
    if user.id in ADMIN_IDS and state.get("awaiting_broadcast"):
        user_states[user.id] = {}
        all_users = get_all_user_ids()
        sent = failed = 0
        status_msg = bot.send_message(
            chat_id,
            f"🚀 <b>Broadcasting message to {len(all_users)} users...</b>",
        )
        for uid in all_users:
            try:
                bot.send_message(uid, text)
                sent += 1
                time.sleep(0.04)
            except Exception:
                failed += 1
        try:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg.message_id,
                text=(
                    f"✅ <b>Broadcast Complete</b>\n"
                    f"🎉 Successfully Sent: <code>{sent}</code>\n"
                    f"❌ Failed: <code>{failed}</code>"
                ),
            )
        except Exception:
            pass
        bot.send_message(chat_id, "Admin Panel:", reply_markup=admin_keyboard())
        return

    # Admin Menu Actions
    if user.id in ADMIN_IDS:
        if text == "📈 Bot Stats":
            stats = get_admin_stats()
            bot.send_message(
                chat_id,
                (
                    f"📈 <b>Bot System Statistics</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👥 <b>Total Registered Users:</b> <code>{stats.get('total_users', 0)}</code>\n"
                    f"🔄 <b>Total Extractions Run:</b> <code>{stats.get('total_ex', 0)}</code>\n"
                    f"📱 <b>Total Numbers Discovered:</b> <code>{stats.get('total_nums', 0)}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━"
                ),
                reply_markup=admin_keyboard(),
            )
            return

        if text == "📢 Broadcast":
            user_states[user.id] = {"awaiting_broadcast": True}
            bot.send_message(
                chat_id,
                "📢 <b>Broadcast Console</b>\n\nEnter the message text to send to all registered users:",
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
                bot.send_message(chat_id, "No users registered yet.", reply_markup=admin_keyboard())
                return

            msg = "👥 <b>Recent 10 Users</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
            for i, r in enumerate(rows, 1):
                name = html.escape(r["first_name"] or "User")
                uname = html.escape(r["username"] or "none")
                msg += (
                    f"<b>#{i}</b> {name} (<code>{r['user_id']}</code>)\n"
                    f"📱 Found: {r['total_numbers_found']} | @{uname}\n\n"
                )
            bot.send_message(chat_id, msg, reply_markup=admin_keyboard())
            return

        if text in ("🟢 Maintenance: OFF", "🔴 Maintenance: ON"):
            MAINTENANCE_MODE = not MAINTENANCE_MODE
            status = "🔴 <b>ON</b> (Admins only)" if MAINTENANCE_MODE else "🟢 <b>OFF</b> (Publicly active)"
            bot.send_message(
                chat_id,
                f"🔧 <b>Maintenance Mode:</b> {status}",
                reply_markup=admin_keyboard(),
            )
            return

    # Action: Send New Link
    if text == "🔗 Send New Link":
        if active_jobs.get(user.id):
            bot.send_message(
                chat_id,
                "⚠️ <b>An extraction job is currently running!</b> Please wait for it to complete or tap ❌ Cancel.",
            )
            return
        user_states[user.id] = {"awaiting_url": True}
        bot.send_message(
            chat_id,
            (
                "🔗 <b>Submit Target URL</b>\n\n"
                "Send any valid rotating, redirect, or landing page link below:\n\n"
                "<i>Example:</i> <code>https://example.com/rotate/link</code>"
            ),
            reply_markup=types.ReplyKeyboardRemove(),
        )
        return

    # Action: My Stats
    if text == "📊 My Stats":
        stats = get_user_stats(user.id)
        if not stats:
            bot.send_message(chat_id, "📊 No stats recorded yet. Run your first extraction!", reply_markup=main_keyboard())
            return
        bot.send_message(
            chat_id,
            (
                f"📊 <b>Your User Statistics</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>Name:</b> {html.escape(stats.get('first_name') or 'User')}\n"
                f"🆔 <b>User ID:</b> <code>{user.id}</code>\n"
                f"🔄 <b>Total Extractions:</b> <code>{stats.get('total_extractions', 0)}</code>\n"
                f"📱 <b>Total Numbers Discovered:</b> <code>{stats.get('total_numbers_found', 0)}</code>\n"
                f"📅 <b>Member Since:</b> <code>{html.escape(str(stats.get('joined_at', 'N/A'))[:10])}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━"
            ),
            reply_markup=main_keyboard(),
        )
        return

    # Action: My History
    if text == "📋 My History":
        history = get_user_history(user.id, limit=8)
        if not history:
            bot.send_message(
                chat_id,
                "📋 <b>No extraction history recorded yet.</b>\n\nRun an extraction to view past reports!",
                reply_markup=main_keyboard(),
            )
            return
        msg = "📋 <b>Your Recent Extractions</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, h in enumerate(history, 1):
            url_disp = html.escape((h["url"][:32] + "...") if len(h["url"]) > 32 else h["url"])
            completed = html.escape(str(h.get("completed_at") or "")[:16])
            msg += (
                f"<b>#{i}</b> | <code>{completed}</code>\n"
                f"🔗 <code>{url_disp}</code>\n"
                f"🔄 Cycles: <code>{h['cycles']}</code> | 📱 Numbers: <code>{h['unique_numbers']}</code>\n\n"
            )
        bot.send_message(chat_id, msg, reply_markup=main_keyboard())
        return

    # Action: Help
    if text == "❓ Help":
        bot.send_message(
            chat_id,
            (
                "❓ <b>Help & Instructions</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "<b>What does this bot do?</b>\n"
                "Many promotional links redirect to different WhatsApp numbers on each visit. "
                "This bot visits the target link across multiple cycles, bypasses redirects/anti-bot protection, "
                "extracts all available phone numbers, and eliminates duplicates.\n\n"
                "<b>Steps:</b>\n"
                "1. Tap <b>🔗 Send New Link</b>\n"
                "2. Paste any valid HTTP/HTTPS link\n"
                "3. Select cycle depth (1x for test, 20x, 50x, or 100x max)\n"
                "4. Instantly copy all numbers or download the <code>.txt</code> file."
            ),
            reply_markup=main_keyboard(),
        )
        return

    # Action: Support
    if text == "📞 Support":
        bot.send_message(
            chat_id,
            (
                "📞 <b>Support & Contact</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "For inquiries, bug reports, or access questions, contact the admin:\n\n"
                "👤 <b>Admin:</b> @YourAdminHandle\n"
                "<i>Made by DK Sharma</i>"
            ),
            reply_markup=main_keyboard(),
        )
        return

    # Action: URL Input Detection
    url_candidate = text.strip()
    if state.get("awaiting_url") or url_candidate.lower().startswith(("http://", "https://")):
        if not url_candidate.lower().startswith(("http://", "https://")):
            bot.send_message(
                chat_id,
                (
                    "⚠️ <b>Invalid URL format</b>\n\n"
                    "Please ensure the link starts with <code>http://</code> or <code>https://</code>."
                ),
            )
            return

        user_states[user.id] = {"url": url_candidate}
        bot.send_message(
            chat_id,
            (
                f"✅ <b>Link Received</b>\n\n"
                f"🔗 <code>{html.escape(url_candidate)}</code>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 <b>Select Extraction Depth:</b>\n"
                f"• Higher cycles collect more rotating numbers\n"
                f"• Automatic deduplication is applied"
            ),
            reply_markup=extraction_keyboard(),
        )
        return

    # Action: Cycle Selection
    if "url" in state:
        count = _EXTRACTION_COUNT_MAP.get(text)
        if count is not None:
            target_url = state["url"]
            user_states[user.id] = {}

            start_msg = bot.send_message(
                chat_id,
                (
                    f"⏳ <b>Initializing Extraction...</b>\n\n"
                    f"🔗 URL: <code>{html.escape(target_url[:42])}{'...' if len(target_url) > 42 else ''}</code>\n"
                    f"🔄 Planned cycles: <code>{count}</code>\n\n"
                    f"<i>Connecting to server and analyzing link...</i>"
                ),
                reply_markup=main_keyboard(),
            )

            threading.Thread(
                target=extraction_worker,
                args=(chat_id, user.id, target_url, count, start_msg.message_id),
                daemon=True,
            ).start()
            return

    # Fallback response
    bot.send_message(
        chat_id,
        "❓ Please choose an action from the menu below.",
        reply_markup=main_keyboard(),
    )


# =========================================================
# Application Entry Point
# =========================================================
if __name__ == "__main__":
    init_db()

    print("🤖 DK Sharma Universal WhatsApp Extractor starting...")
    print(f"Configured Admins: {ADMIN_IDS}")

    try:
        bot.remove_webhook()
        time.sleep(1)
        print("✅ Webhook status cleared.")
    except Exception as e:
        print(f"⚠️ Webhook removal notice: {e}")

    print("✅ Bot is actively listening for messages...")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"❌ Polling exception: {e}")
            time.sleep(4)
            print("🔄 Reconnecting listener...")
