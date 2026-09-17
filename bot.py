#!/usr/bin/env python3
"""
DK Sharma Universal WhatsApp Extractor & Rotating Link Engine
Production-ready, highly optimized, multi-mode Telegram bot.
Compatible with standard VPS and Render deployment.
"""

import html
import http.cookiejar
import io
import os
import random
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

# =========================================================
# Configuration & Security (Environment-Driven)
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8267372667:AAFUCPQ9kv60DwkRqi54Ge7YasN3NYs2XNs").strip()
if not BOT_TOKEN:
    print("CRITICAL ERROR: BOT_TOKEN environment variable is not set.")
    print("Please set BOT_TOKEN in your environment (e.g. on Render / Docker / local .env).")
    sys.exit(1)

ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "8753914631").split(",")
    if x.strip().isdigit()
]

# Supported authorized proxy/gateway configurations:
# In addition to environment variables, proxies can be added dynamically via the Admin Panel!
RAW_PROXIES = os.environ.get("PROXY_ENDPOINTS", "") or os.environ.get("ROTATING_PROXIES", "")
SYSTEM_HTTP_PROXY = os.environ.get("HTTP_PROXY", "") or os.environ.get("http_proxy", "")
SYSTEM_HTTPS_PROXY = os.environ.get("HTTPS_PROXY", "") or os.environ.get("https_proxy", "")

# Initialize TeleBot with HTML parsing mode for maximum format stability
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# Global flags
MAINTENANCE_MODE = False

# State stores
user_states: dict = {}
active_jobs: dict = {}

# =========================================================
# Database Management (WAL Mode, Thread-Safe)
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
                    mode            TEXT DEFAULT 'NORMAL',
                    cycles          INTEGER,
                    unique_numbers  INTEGER,
                    duplicate_count INTEGER,
                    started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at    TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS admin_proxies (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint        TEXT UNIQUE NOT NULL,
                    added_by        INTEGER,
                    is_active       INTEGER DEFAULT 1,
                    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    user_id: int, url: str, mode: str, cycles: int, unique_numbers: int, duplicate_count: int
) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO extraction_history
                       (user_id, url, mode, cycles, unique_numbers, duplicate_count, completed_at)
                   VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (user_id, url, mode, cycles, unique_numbers, duplicate_count),
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


def get_user_history(user_id: int, limit: int = 8) -> list[dict]:
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
            """SELECT COUNT(*)                           AS total_users,
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


# Database Proxy Helpers
def db_add_proxy(endpoint: str, added_by: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO admin_proxies (endpoint, added_by, is_active) VALUES (?, ?, 1)",
                (endpoint, added_by),
            )
            conn.commit()
            return True
        except Exception:
            return False
        finally:
            conn.close()


def db_get_all_proxies() -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM admin_proxies WHERE is_active = 1 ORDER BY id ASC").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def db_delete_proxy(proxy_id: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute("DELETE FROM admin_proxies WHERE id = ?", (proxy_id,))
            conn.commit()
            return True
        except Exception:
            return False
        finally:
            conn.close()


def db_clear_all_proxies() -> int:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute("DELETE FROM admin_proxies")
            deleted = cur.rowcount
            conn.commit()
            return deleted
        finally:
            conn.close()


# =========================================================
# Proxy & Alternate Network Gateway Manager
# =========================================================
class NetworkEndpointManager:
    """
    Manages legitimate proxy / network endpoints provided by:
    1. Environment variables (PROXY_ENDPOINTS, ROTATING_PROXIES, HTTP_PROXY, etc.)
    2. Dynamic Admin Panel insertion stored in SQLite database.
    Never invents or fakes proxies. Tracks health and cool-down states.
    """
    def __init__(self):
        self.env_endpoints: list[str] = []
        self.failed_endpoints: dict[str, float] = {}  # endpoint -> timestamp of failure
        self.cooldown_seconds = 120.0  # 2 minutes cooldown for failed endpoints
        self._lock = threading.Lock()
        self._load_from_env()

    def _load_from_env(self):
        items = []
        if RAW_PROXIES:
            for p in RAW_PROXIES.split(","):
                p = p.strip()
                if p and p.startswith(("http://", "https://", "socks5://", "socks5h://")):
                    items.append(p)
        elif SYSTEM_HTTP_PROXY or SYSTEM_HTTPS_PROXY:
            if SYSTEM_HTTP_PROXY:
                items.append(SYSTEM_HTTP_PROXY.strip())
            if SYSTEM_HTTPS_PROXY and SYSTEM_HTTPS_PROXY != SYSTEM_HTTP_PROXY:
                items.append(SYSTEM_HTTPS_PROXY.strip())
        self.env_endpoints = list(dict.fromkeys(items))

    def get_all_endpoints(self) -> list[str]:
        """Combines environment proxies and database-persisted admin proxies."""
        with self._lock:
            db_records = db_get_all_proxies()
            db_items = [r["endpoint"] for r in db_records]
            combined = list(dict.fromkeys(self.env_endpoints + db_items))
            return combined

    def has_endpoints(self) -> bool:
        return len(self.get_all_endpoints()) > 0

    def get_endpoint_count(self) -> int:
        return len(self.get_all_endpoints())

    def get_next_endpoint(self, preferred_index: int = 0) -> str | None:
        with self._lock:
            all_eps = self.get_all_endpoints()
            if not all_eps:
                return None

            now = time.time()
            self.failed_endpoints = {
                ep: ts for ep, ts in self.failed_endpoints.items()
                if (now - ts) < self.cooldown_seconds
            }

            available = [ep for ep in all_eps if ep not in self.failed_endpoints]
            if not available:
                available = all_eps

            chosen = available[preferred_index % len(available)]
            return chosen

    def mark_failed(self, endpoint: str):
        with self._lock:
            if endpoint:
                self.failed_endpoints[endpoint] = time.time()

    @staticmethod
    def sanitize(endpoint: str | None) -> str:
        """Removes credentials before displaying endpoint in any message."""
        if not endpoint:
            return "None"
        try:
            parsed = urllib.parse.urlsplit(endpoint)
            netloc = parsed.hostname or "unknown"
            if parsed.port:
                netloc += f":{parsed.port}"
            return f"{parsed.scheme}://{netloc}"
        except Exception:
            return "gateway-endpoint"

    @staticmethod
    def test_proxy(endpoint: str, timeout: float = 6.0) -> tuple[bool, str]:
        """Actively tests connectivity through the proxy to verify it is working."""
        try:
            proxy_dict = {"http": endpoint, "https": endpoint}
            proxy_handler = urllib.request.ProxyHandler(proxy_dict)
            opener = urllib.request.build_opener(proxy_handler)
            opener.addheaders = [("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0")]
            
            start_t = time.time()
            # Fast test against public IP probe endpoint
            resp = opener.open("http://httpbin.org/ip", timeout=timeout)
            duration = round((time.time() - start_t) * 1000)
            if resp.status == 200:
                body = resp.read().decode("utf-8", errors="ignore")
                return True, f"Working ({duration}ms) | {body.strip()[:60]}"
            return True, f"Connected with HTTP {resp.status} ({duration}ms)"
        except Exception as e:
            return False, f"Connection failed: {str(e)[:70]}"


endpoint_manager = NetworkEndpointManager()

# =========================================================
# Pure Python AES-128-CBC Challenge Solver
# Solves InfinityFree / ByetHost challenge without crashing
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
    c = bytes.fromhex(c_hex)
    a = bytes.fromhex(a_key_hex)
    b = bytes.fromhex(b_iv_hex)
    w = _key_schedule(a)
    dec = _decrypt_single_block(c, w)
    res = bytes([dec[i] ^ b[i] for i in range(16)])
    return res.hex()


# =========================================================
# Targeted WhatsApp Phone Number Extraction & Normalization
# =========================================================
_WA_PATTERNS = [
    re.compile(r'wa\.me/(?:p/|qr/)?\+?(\d{10,15})', re.IGNORECASE),
    re.compile(r'(?:[?&]|\b)(?:phone|mobile|wa_number|whatsapp|send_to|to)=\+?(\d{10,15})', re.IGNORECASE),
    re.compile(r'(?:whatsapp|intent)://send\?.*?(?:phone|number)=\+?(\d{10,15})', re.IGNORECASE),
    re.compile(r'(?:api|web)\.whatsapp\.com/send/?\??.*?(?:phone|number)=\+?(\d{10,15})', re.IGNORECASE),
    re.compile(r'["\'](?:whatsapp|phone_number|mobile_number|wa)["\']\s*:\s*["\']\+?(\d{10,15})["\']', re.IGNORECASE),
    re.compile(r'data-(?:phone|whatsapp|number)=["\']\+?(\d{10,15})["\']', re.IGNORECASE),
    re.compile(r'href=["\'](?:tel:|whatsapp://send\?phone=)\+?(\d{10,15})["\']', re.IGNORECASE),
]


def clean_phone_number(raw: str) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("00") and len(digits) > 12:
        digits = digits[2:]
    if 10 <= len(digits) <= 15:
        return digits
    return None


def extract_numbers_from_text(text: str) -> set[str]:
    found: set[str] = set()
    if not text:
        return found

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
# Custom Redirect Handler
# Intercepts whatsapp:// and intent:// without failing urllib
# =========================================================
class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        super().__init__()
        self.collected_redirect_targets: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.collected_redirect_targets.append(newurl)
        parsed = urllib.parse.urlparse(newurl)
        if parsed.scheme.lower() not in ("http", "https"):
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# =========================================================
# High-Performance Request Engine
# =========================================================
class OptimizedExtractionSession:
    def __init__(self, proxy_endpoint: str | None = None, timeout: float = 10.0):
        self.proxy_endpoint = proxy_endpoint
        self.timeout = timeout
        self.cj = http.cookiejar.CookieJar()
        self.redirect_handler = SafeRedirectHandler()
        self.cached_test_cookie = None

        handlers: list[urllib.request.BaseHandler] = [
            urllib.request.HTTPCookieProcessor(self.cj),
            self.redirect_handler,
        ]

        if self.proxy_endpoint:
            proxy_dict = {
                "http": self.proxy_endpoint,
                "https": self.proxy_endpoint,
            }
            handlers.append(urllib.request.ProxyHandler(proxy_dict))

        self.opener = urllib.request.build_opener(*handlers)
        self.opener.addheaders = [
            ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
            ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
            ("Accept-Language", "en-US,en;q=0.9"),
            ("Cache-Control", "no-cache"),
            ("Pragma", "no-cache"),
            ("Upgrade-Insecure-Requests", "1"),
        ]

    def fetch(self, url: str) -> tuple[str, str, list[str]]:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.hostname

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
            resp = self.opener.open(req, timeout=self.timeout)
            current_url = resp.geturl()
            visited_urls.append(current_url)
            body = resp.read().decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as e:
            current_url = e.geturl() or url
            visited_urls.append(current_url)
            body = e.read().decode("utf-8", errors="ignore") if hasattr(e, "read") else ""
        except Exception:
            visited_urls.extend(self.redirect_handler.collected_redirect_targets)
            raise

        visited_urls.extend(self.redirect_handler.collected_redirect_targets)

        # Solve InfinityFree / ByetHost slowAES anti-bot challenge
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
                resp2 = self.opener.open(urllib.request.Request(next_url), timeout=self.timeout)
                current_url = resp2.geturl()
                visited_urls.append(current_url)
                body = resp2.read().decode("utf-8", errors="ignore")

        # Meta Refresh and JavaScript Redirections
        for _ in range(2):
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
                        resp = self.opener.open(urllib.request.Request(dest_url), timeout=self.timeout)
                        current_url = resp.geturl()
                        visited_urls.append(current_url)
                        body = resp.read().decode("utf-8", errors="ignore")
                        continue
                    except Exception:
                        pass
                break

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
                        resp = self.opener.open(urllib.request.Request(dest_url), timeout=self.timeout)
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
    timestamp = int(time.time() * 1000)
    salt = random.randint(100, 999)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}_cb={timestamp}_{cycle}_{salt}"


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


def mode_selection_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🟢 WITHOUT IP"),
        types.KeyboardButton("🌐 WITH IP ROTATION"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def extraction_cycles_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🧪 Test — 1 Visit"),
        types.KeyboardButton("🚀 20 Visits"),
        types.KeyboardButton("⚡ 50 Visits"),
        types.KeyboardButton("💎 100 Visits"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def admin_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    maintenance_label = "🔴 Maintenance: ON" if MAINTENANCE_MODE else "🟢 Maintenance: OFF"
    markup.add(
        types.KeyboardButton("🌐 Proxy Manager"),
        types.KeyboardButton("📈 Bot Stats"),
        types.KeyboardButton("📢 Broadcast"),
        types.KeyboardButton("👥 Recent Users"),
        types.KeyboardButton(maintenance_label),
        types.KeyboardButton("🔙 Main Menu"),
    )
    return markup


def proxy_manager_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("➕ Add Single Proxy"),
        types.KeyboardButton("📦 Bulk Add Proxies"),
        types.KeyboardButton("📋 List All Proxies"),
        types.KeyboardButton("🧪 Test All Proxies"),
        types.KeyboardButton("🗑️ Clear All Proxies"),
        types.KeyboardButton("🔙 Admin Panel"),
    )
    return markup


def build_copy_markup(numbers_text: str) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    try:
        copy_btn = types.InlineKeyboardButton(
            text="📋 Copy All Numbers",
            copy_text=types.CopyTextButton(text=numbers_text),
        )
        markup.add(copy_btn)
    except Exception:
        markup.add(
            types.InlineKeyboardButton(
                text="📋 Copy All Numbers",
                switch_inline_query=numbers_text[:250],
            )
        )
    return markup


# =========================================================
# Fast Extraction Worker Thread
# =========================================================
def extraction_worker(
    chat_id: int,
    user_id: int,
    url: str,
    mode: str,
    total_cycles: int,
    progress_message_id: int,
) -> None:
    active_jobs[user_id] = True

    found_numbers: list[str] = []
    found_set: set[str] = set()
    total_numbers_seen = 0
    total_ok = 0
    errors = 0
    last_ui_update = 0.0
    start_time = time.time()

    mode_display_name = "🌐 IP Rotation" if mode == "ROTATING" else "🟢 Normal Connection"

    shared_normal_session = OptimizedExtractionSession() if mode == "NORMAL" else None

    for cycle in range(1, total_cycles + 1):
        if not active_jobs.get(user_id, True):
            break

        target_url = add_cache_buster(url, cycle)
        session_to_use = shared_normal_session
        endpoint_used = None

        if mode == "ROTATING":
            endpoint_used = endpoint_manager.get_next_endpoint(cycle)
            session_to_use = OptimizedExtractionSession(proxy_endpoint=endpoint_used)

        try:
            final_url, body, visited_urls = session_to_use.fetch(target_url)
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

        except Exception:
            errors += 1
            if mode == "ROTATING" and endpoint_used:
                endpoint_manager.mark_failed(endpoint_used)

        # Throttled progress update (every ~1.8s)
        now = time.time()
        if (now - last_ui_update > 1.8) or (cycle == total_cycles):
            last_ui_update = now
            elapsed = max(int(now - start_time), 1)
            pct = int((cycle / total_cycles) * 100)
            done_ticks = int((cycle / total_cycles) * 10)
            bar = "█" * done_ticks + "░" * (10 - done_ticks)

            progress_text = (
                f"⏳ <b>EXTRACTION IN PROGRESS</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"<b>Mode:</b> {mode_display_name}\n"
                f"<b>Visits:</b> {cycle}/{total_cycles} ({pct}%)\n"
                f"<b>Progress:</b> <code>[{bar}]</code>\n"
                f"<b>Successful:</b> <code>{total_ok}</code>\n"
                f"<b>Failed:</b> <code>{errors}</code>\n"
                f"<b>Unique Numbers:</b> <code>{len(found_numbers)}</code>\n"
                f"<b>Elapsed:</b> {elapsed}s\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"<i>Collecting numbers silently...</i>"
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

        time.sleep(0.05)

    # ── Job Completed ───────────────────────────────────────────
    active_jobs.pop(user_id, None)

    unique_count = len(found_numbers)
    duplicate_count = max(total_numbers_seen - unique_count, 0)
    sorted_numbers = sorted(found_numbers)

    update_user_stats(user_id, unique_count)
    save_history(user_id, url, mode, total_cycles, unique_count, duplicate_count)

    # Send final result ONCE
    if unique_count > 0:
        numbers_plain_text = "\n".join(f"+{num}" for num in sorted_numbers)

        CHUNK_LIMIT = 3200
        lines = [f"+{num}" for num in sorted_numbers]
        chunks = []
        curr_chunk = ""
        for line in lines:
            if len(curr_chunk) + len(line) + 1 > CHUNK_LIMIT:
                chunks.append(curr_chunk.strip())
                curr_chunk = line + "\n"
            else:
                curr_chunk += line + "\n"
        if curr_chunk.strip():
            chunks.append(curr_chunk.strip())

        copy_markup = build_copy_markup(numbers_plain_text)

        header_text = (
            f"📱 <b>EXTRACTED WHATSAPP NUMBERS</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<code>{html.escape(chunks[0])}</code>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>Unique Numbers:</b> <code>{unique_count}</code>\n"
            f"🔄 <b>Visits:</b> <code>{total_cycles}</code>\n"
            f"✅ <b>Successful:</b> <code>{total_ok}</code>\n"
            f"❌ <b>Failed:</b> <code>{errors}</code>\n"
            f"🌐 <b>Mode:</b> {mode_display_name}"
        )

        try:
            bot.send_message(chat_id, header_text, reply_markup=copy_markup)
        except Exception:
            bot.send_message(chat_id, f"Extracted Numbers:\n{chunks[0]}")

        for extra_idx, extra_chunk in enumerate(chunks[1:], start=2):
            extra_text = (
                f"📱 <b>Extracted Numbers (Part {extra_idx})</b>\n\n"
                f"<code>{html.escape(extra_chunk)}</code>"
            )
            try:
                bot.send_message(chat_id, extra_text)
            except Exception:
                bot.send_message(chat_id, extra_chunk)

        try:
            file_data = (
                "DK Sharma WhatsApp Extractor — Final Results\n"
                f"Source URL: {url}\n"
                f"Mode: {mode_display_name}\n"
                f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Total Visits Requested: {total_cycles}\n"
                f"Successful Visits: {total_ok}\n"
                f"Failed Visits: {errors}\n"
                f"Total Unique Numbers: {unique_count}\n"
                f"Duplicates Filtered: {duplicate_count}\n"
                + ("=" * 45)
                + "\n\n"
                + numbers_plain_text
                + "\n"
            )

            file_stream = io.BytesIO(file_data.encode("utf-8"))
            file_stream.name = f"whatsapp_numbers_{int(time.time())}.txt"

            bot.send_document(
                chat_id,
                file_stream,
                caption=(
                    f"📁 <b>Extraction File Ready</b>\n"
                    f"📱 <code>Unique Numbers: {unique_count}</code>\n"
                    f"<i>Tap above to save or download all numbers</i>"
                ),
                reply_markup=main_keyboard(),
            )
        except Exception as e:
            bot.send_message(chat_id, f"⚠️ Note: File upload encountered an error ({html.escape(str(e))})", reply_markup=main_keyboard())

    else:
        bot.send_message(
            chat_id,
            (
                "⚠️ <b>No WhatsApp numbers found</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"🔄 <b>Visits Completed:</b> <code>{total_ok}/{total_cycles}</code>\n"
                f"❌ <b>Errors:</b> <code>{errors}</code>\n"
                f"🌐 <b>Mode:</b> {mode_display_name}\n\n"
                "<b>Notice:</b>\n"
                "• The target server may not have returned any WhatsApp redirects.\n"
                "• Check that the link is currently active.\n"
                "<i>Tip: Try running 🧪 Test — 1 Visit to inspect the link status.</i>"
            ),
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
            f"👋 <b>Welcome to DK Sharma Extractor!</b>\n\n"
            f"🤖 <b>Universal WhatsApp Number Extractor</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<b>How it works:</b>\n"
            f"1️⃣ Tap <b>🔗 Send New Link</b>\n"
            f"2️⃣ Paste any valid HTTP/HTTPS link\n"
            f"3️⃣ Choose extraction mode:\n"
            f"    • 🟢 <b>WITHOUT IP:</b> Normal fast connection\n"
            f"    • 🌐 <b>WITH IP ROTATION:</b> Via authorized network endpoints\n"
            f"4️⃣ Select visit count (1x, 20x, 50x, 100x)\n"
            f"5️⃣ Receive all unique numbers in one clean message with <b>Copy All</b> + <b>.txt download</b>!\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Select an option below:</b>"
        ),
        reply_markup=main_keyboard(),
    )


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "❌ <b>Access Denied.</b> You are not authorized as an admin.")
        return
    bot.send_message(
        message.chat.id,
        "🔐 <b>Admin Control Console</b>\n━━━━━━━━━━━━━━━━━━\nSelect an administrative function:",
        reply_markup=admin_keyboard(),
    )


# =========================================================
# Inline Callback Query Handler (Delete Proxy)
# =========================================================
@bot.callback_query_handler(func=lambda call: call.data.startswith("del_proxy_"))
def handle_proxy_delete_callback(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "Access denied.")
        return

    proxy_id_str = call.data.replace("del_proxy_", "")
    if proxy_id_str.isdigit():
        db_delete_proxy(int(proxy_id_str))
        bot.answer_callback_query(call.id, "Proxy deleted successfully.")
        try:
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=f"🗑️ <i>Proxy #{proxy_id_str} was deleted from database.</i>",
            )
        except Exception:
            pass


# =========================================================
# Message Router (Two-Mode Flow & Admin Features)
# =========================================================
_CYCLE_COUNT_MAP = {
    "🧪 Test — 1 Visit": 1,
    "🚀 20 Visits": 20,
    "⚡ 50 Visits": 50,
    "💎 100 Visits": 100,
}


@bot.message_handler(func=lambda m: True)
def handle_all_messages(message: types.Message) -> None:
    global MAINTENANCE_MODE

    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()

    register_user(user.id, user.username, user.first_name)
    state = user_states.get(user.id, {})

    # Maintenance Check
    if MAINTENANCE_MODE and user.id not in ADMIN_IDS:
        bot.send_message(
            chat_id,
            (
                "🔧 <b>Maintenance Mode Active</b>\n\n"
                "The bot is undergoing scheduled maintenance. Please check back shortly!\n"
                "<i>— DK Sharma Extractor</i>"
            ),
        )
        return

    # Cancel & Main Menu
    if text in ("❌ Cancel", "🔙 Main Menu"):
        active_jobs[user.id] = False
        user_states[user.id] = {}
        bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard())
        return

    # Back to Admin Panel
    if user.id in ADMIN_IDS and text == "🔙 Admin Panel":
        user_states[user.id] = {}
        bot.send_message(chat_id, "🔐 <b>Admin Control Console</b>", reply_markup=admin_keyboard())
        return

    # Admin Broadcast Handler
    if user.id in ADMIN_IDS and state.get("awaiting_broadcast"):
        user_states[user.id] = {}
        all_users = get_all_user_ids()
        sent = failed = 0
        status_msg = bot.send_message(chat_id, f"🚀 <b>Broadcasting to {len(all_users)} users...</b>")
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
                    f"✅ <b>Broadcast Completed</b>\n"
                    f"🎉 Successfully Sent: <code>{sent}</code>\n"
                    f"❌ Failed: <code>{failed}</code>"
                ),
            )
        except Exception:
            pass
        bot.send_message(chat_id, "Admin Console:", reply_markup=admin_keyboard())
        return

    # Admin Proxy Input Handlers
    if user.id in ADMIN_IDS and state.get("step") == "ADMIN_ADD_PROXY":
        user_states[user.id] = {}
        endpoint = text.strip()
        if not endpoint.startswith(("http://", "https://", "socks5://", "socks5h://")):
            bot.send_message(
                chat_id,
                (
                    "❌ <b>Invalid Proxy Format</b>\n\n"
                    "Proxy must start with <code>http://</code>, <code>https://</code>, or <code>socks5://</code>\n\n"
                    "<i>Examples:</i>\n"
                    "• <code>http://user:pass@1.2.3.4:8080</code>\n"
                    "• <code>socks5://1.2.3.4:1080</code>"
                ),
                reply_markup=proxy_manager_keyboard(),
            )
            return

        # Test proxy before adding
        test_msg = bot.send_message(chat_id, "⏳ <i>Verifying proxy connectivity...</i>")
        is_working, test_info = NetworkEndpointManager.test_proxy(endpoint)
        try:
            bot.delete_message(chat_id, test_msg.message_id)
        except Exception:
            pass

        added = db_add_proxy(endpoint, user.id)
        sanitized = NetworkEndpointManager.sanitize(endpoint)
        if added:
            status_badge = "✅ <b>Verified Active</b>" if is_working else "⚠️ <b>Added (Test Warning: Unresponsive/Slow)</b>"
            bot.send_message(
                chat_id,
                (
                    f"🎉 <b>Proxy Added Successfully!</b>\n\n"
                    f"📡 <b>Endpoint:</b> <code>{html.escape(sanitized)}</code>\n"
                    f"🔍 <b>Status:</b> {status_badge}\n"
                    f"ℹ️ <code>{html.escape(test_info)}</code>\n\n"
                    f"Total Configured Proxies: <code>{endpoint_manager.get_endpoint_count()}</code>"
                ),
                reply_markup=proxy_manager_keyboard(),
            )
        else:
            bot.send_message(
                chat_id,
                "⚠️ Could not add proxy (it might already exist).",
                reply_markup=proxy_manager_keyboard(),
            )
        return

    if user.id in ADMIN_IDS and state.get("step") == "ADMIN_BULK_ADD_PROXIES":
        user_states[user.id] = {}
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        added_count = 0
        invalid_count = 0

        status_msg = bot.send_message(chat_id, f"⏳ <i>Processing {len(lines)} proxies...</i>")

        for line in lines:
            if line.startswith(("http://", "https://", "socks5://", "socks5h://")):
                if db_add_proxy(line, user.id):
                    added_count += 1
            else:
                invalid_count += 1

        try:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg.message_id,
                text=(
                    f"✅ <b>Bulk Import Finished</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"➕ <b>Added/Updated:</b> <code>{added_count}</code>\n"
                    f"⚠️ <b>Invalid Skipped:</b> <code>{invalid_count}</code>\n"
                    f"📡 <b>Total Active Proxies:</b> <code>{endpoint_manager.get_endpoint_count()}</code>"
                ),
            )
        except Exception:
            pass

        bot.send_message(chat_id, "Proxy Manager:", reply_markup=proxy_manager_keyboard())
        return

    # Admin Menu Buttons
    if user.id in ADMIN_IDS:
        if text == "🌐 Proxy Manager":
            total_eps = endpoint_manager.get_endpoint_count()
            env_count = len(endpoint_manager.env_endpoints)
            db_count = len(db_get_all_proxies())
            bot.send_message(
                chat_id,
                (
                    f"🌐 <b>Proxy & Gateway Manager</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"📡 <b>Total Active Endpoints:</b> <code>{total_eps}</code>\n"
                    f"⚙️ <b>From Environment:</b> <code>{env_count}</code>\n"
                    f"💾 <b>From Admin Database:</b> <code>{db_count}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"Choose an option below to manage or test proxies:"
                ),
                reply_markup=proxy_manager_keyboard(),
            )
            return

        if text == "➕ Add Single Proxy":
            user_states[user.id] = {"step": "ADMIN_ADD_PROXY"}
            bot.send_message(
                chat_id,
                (
                    "➕ <b>Add Single Proxy</b>\n\n"
                    "Send the proxy in one of these formats:\n"
                    "• <code>http://ip:port</code>\n"
                    "• <code>http://username:password@ip:port</code>\n"
                    "• <code>socks5://ip:port</code>\n"
                    "• <code>socks5://username:password@ip:port</code>\n\n"
                    "<i>Credentials will be safely stored and never shown in public chat.</i>"
                ),
                reply_markup=types.ReplyKeyboardMarkup(resize_keyboard=True).add(types.KeyboardButton("🔙 Admin Panel")),
            )
            return

        if text == "📦 Bulk Add Proxies":
            user_states[user.id] = {"step": "ADMIN_BULK_ADD_PROXIES"}
            bot.send_message(
                chat_id,
                (
                    "📦 <b>Bulk Add Proxies</b>\n\n"
                    "Paste your proxies below (one proxy per line):\n\n"
                    "<code>http://user:pass@1.2.3.4:8080\n"
                    "http://5.6.7.8:8080\n"
                    "socks5://9.10.11.12:1080</code>"
                ),
                reply_markup=types.ReplyKeyboardMarkup(resize_keyboard=True).add(types.KeyboardButton("🔙 Admin Panel")),
            )
            return

        if text == "📋 List All Proxies":
            db_proxies = db_get_all_proxies()
            env_proxies = endpoint_manager.env_endpoints

            if not db_proxies and not env_proxies:
                bot.send_message(
                    chat_id,
                    "📋 <b>No proxies configured yet.</b>\nUse ➕ Add Single Proxy or 📦 Bulk Add Proxies to add some.",
                    reply_markup=proxy_manager_keyboard(),
                )
                return

            msg = f"📋 <b>Configured Proxies ({len(db_proxies) + len(env_proxies)})</b>\n━━━━━━━━━━━━━━━━━━\n\n"

            if env_proxies:
                msg += "⚙️ <b>Environment Proxies (Read-Only):</b>\n"
                for i, ep in enumerate(env_proxies, 1):
                    msg += f"• <code>{html.escape(NetworkEndpointManager.sanitize(ep))}</code>\n"
                msg += "\n"

            bot.send_message(chat_id, msg, reply_markup=proxy_manager_keyboard())

            if db_proxies:
                bot.send_message(chat_id, "💾 <b>Database Proxies (Can be deleted):</b>")
                for item in db_proxies:
                    sanitized = NetworkEndpointManager.sanitize(item['endpoint'])
                    created = str(item.get('created_at') or '')[:10]
                    card = f"🆔 #{item['id']} | <code>{html.escape(sanitized)}</code>\n📅 Added: {created}"

                    del_markup = types.InlineKeyboardMarkup()
                    del_markup.add(types.InlineKeyboardButton(f"🗑️ Delete #{item['id']}", callback_data=f"del_proxy_{item['id']}"))
                    try:
                        bot.send_message(chat_id, card, reply_markup=del_markup)
                    except Exception:
                        pass
            return

        if text == "🧪 Test All Proxies":
            all_eps = endpoint_manager.get_all_endpoints()
            if not all_eps:
                bot.send_message(chat_id, "⚠️ No proxies configured to test.", reply_markup=proxy_manager_keyboard())
                return

            status_msg = bot.send_message(chat_id, f"🧪 <i>Testing {len(all_eps)} proxies... Please wait.</i>")
            results = []
            working = 0
            failed = 0

            for idx, ep in enumerate(all_eps, 1):
                ok, note = NetworkEndpointManager.test_proxy(ep, timeout=5.0)
                sanitized = NetworkEndpointManager.sanitize(ep)
                if ok:
                    working += 1
                    results.append(f"✅ <code>{html.escape(sanitized)}</code> - {html.escape(note)}")
                else:
                    failed += 1
                    results.append(f"❌ <code>{html.escape(sanitized)}</code> - {html.escape(note)}")

            report = (
                f"🧪 <b>Proxy Diagnostic Test Results</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Working:</b> <code>{working}</code>\n"
                f"❌ <b>Unresponsive:</b> <code>{failed}</code>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                + "\n\n".join(results[:15])
            )
            if len(results) > 15:
                report += f"\n\n<i>...and {len(results) - 15} more.</i>"

            try:
                bot.edit_message_text(chat_id=chat_id, message_id=status_msg.message_id, text=report)
            except Exception:
                bot.send_message(chat_id, report)
            return

        if text == "🗑️ Clear All Proxies":
            deleted = db_clear_all_proxies()
            bot.send_message(
                chat_id,
                f"🗑️ <b>Deleted {deleted} database proxies.</b>\nEnvironment proxies (if any) remain in config.",
                reply_markup=proxy_manager_keyboard(),
            )
            return

        if text == "📈 Bot Stats":
            stats = get_admin_stats()
            proxy_count = endpoint_manager.get_endpoint_count()
            bot.send_message(
                chat_id,
                (
                    f"📈 <b>Bot System Statistics</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"👥 <b>Total Registered Users:</b> <code>{stats.get('total_users', 0)}</code>\n"
                    f"🔄 <b>Total Extractions Run:</b> <code>{stats.get('total_ex', 0)}</code>\n"
                    f"📱 <b>Total Numbers Extracted:</b> <code>{stats.get('total_nums', 0)}</code>\n"
                    f"🌐 <b>Active Proxy Gateways:</b> <code>{proxy_count}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━"
                ),
                reply_markup=admin_keyboard(),
            )
            return

        if text == "📢 Broadcast":
            user_states[user.id] = {"awaiting_broadcast": True}
            bot.send_message(chat_id, "📢 <b>Broadcast Console</b>\n\nType the message text to send to all registered users:")
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

            msg = "👥 <b>Recent 10 Users</b>\n━━━━━━━━━━━━━━━━━━\n\n"
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
            bot.send_message(chat_id, f"🔧 <b>Maintenance Mode:</b> {status}", reply_markup=admin_keyboard())
            return

    # Standard User Menu Options
    if text == "🔗 Send New Link":
        if active_jobs.get(user.id):
            bot.send_message(chat_id, "⚠️ <b>An extraction job is already in progress!</b> Please wait or tap ❌ Cancel.")
            return
        user_states[user.id] = {"step": "AWAITING_URL"}
        bot.send_message(
            chat_id,
            (
                "🔗 <b>Submit Target URL</b>\n\n"
                "Please send the rotating or redirect link below:\n\n"
                "<i>Example:</i> <code>https://example.com/rotate/wa</code>"
            ),
            reply_markup=types.ReplyKeyboardRemove(),
        )
        return

    if text == "📊 My Stats":
        stats = get_user_stats(user.id)
        if not stats:
            bot.send_message(chat_id, "📊 No stats recorded yet. Run your first extraction!", reply_markup=main_keyboard())
            return
        bot.send_message(
            chat_id,
            (
                f"📊 <b>Your User Statistics</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>Name:</b> {html.escape(stats.get('first_name') or 'User')}\n"
                f"🆔 <b>User ID:</b> <code>{user.id}</code>\n"
                f"🔄 <b>Total Extractions:</b> <code>{stats.get('total_extractions', 0)}</code>\n"
                f"📱 <b>Total Numbers Discovered:</b> <code>{stats.get('total_numbers_found', 0)}</code>\n"
                f"📅 <b>Member Since:</b> <code>{html.escape(str(stats.get('joined_at', 'N/A'))[:10])}</code>\n"
                f"━━━━━━━━━━━━━━━━━━"
            ),
            reply_markup=main_keyboard(),
        )
        return

    if text == "📋 My History":
        history = get_user_history(user.id, limit=8)
        if not history:
            bot.send_message(chat_id, "📋 <b>No extraction history yet.</b> Run an extraction to view past logs!", reply_markup=main_keyboard())
            return
        msg = "📋 <b>Your Recent Extractions</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        for i, h in enumerate(history, 1):
            url_disp = html.escape((h["url"][:30] + "...") if len(h["url"]) > 30 else h["url"])
            completed = html.escape(str(h.get("completed_at") or "")[:16])
            mode_lbl = "🌐 IP" if h.get("mode") == "ROTATING" else "🟢 Norm"
            msg += (
                f"<b>#{i}</b> | <code>{completed}</code> [{mode_lbl}]\n"
                f"🔗 <code>{url_disp}</code>\n"
                f"🔄 Visits: <code>{h['cycles']}</code> | 📱 Numbers: <code>{h['unique_numbers']}</code>\n\n"
            )
        bot.send_message(chat_id, msg, reply_markup=main_keyboard())
        return

    if text == "❓ Help":
        bot.send_message(
            chat_id,
            (
                "❓ <b>Help & Overview</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "<b>What this bot does:</b>\n"
                "Extracts rotating WhatsApp numbers from links that redirect to WhatsApp.\n\n"
                "<b>Two-Mode Selection:</b>\n"
                "• 🟢 <b>WITHOUT IP:</b> Direct connection using the server's network.\n"
                "• 🌐 <b>WITH IP ROTATION:</b> Uses authorized network endpoints configured in the deployment environment or admin panel.\n\n"
                "<b>Features:</b>\n"
                "• All numbers collected silently during extraction.\n"
                "• Single final result with 📋 <b>Copy All Numbers</b> button.\n"
                "• Downloadable <code>.txt</code> file."
            ),
            reply_markup=main_keyboard(),
        )
        return

    if text == "📞 Support":
        bot.send_message(
            chat_id,
            (
                "📞 <b>Support</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "For technical assistance or inquiries, reach out to the admin:\n\n"
                "👤 <b>Admin:</b> @YourAdminHandle\n"
                "<i>DK Sharma Extractor</i>"
            ),
            reply_markup=main_keyboard(),
        )
        return

    # ── Flow Step 1: URL Submission ─────────────────────────────
    if state.get("step") == "AWAITING_URL" or text.startswith(("http://", "https://")):
        if not text.startswith(("http://", "https://")):
            bot.send_message(
                chat_id,
                "⚠️ <b>Invalid URL</b>\n\nPlease submit a valid web address starting with <code>http://</code> or <code>https://</code>.",
            )
            return

        user_states[user.id] = {
            "step": "AWAITING_MODE",
            "url": text,
        }

        bot.send_message(
            chat_id,
            (
                f"🔗 <b>Link Received Successfully</b>\n\n"
                f"<code>{html.escape(text)}</code>\n\n"
                f"<b>Choose Extraction Mode:</b>\n\n"
                f"🟢 <b>WITHOUT IP</b>\n"
                f"Normal extraction using the server's normal network connection.\n\n"
                f"🌐 <b>WITH IP ROTATION</b>\n"
                f"Run extraction through legitimately available network/proxy/VPN endpoints and use a different endpoint between visits when possible."
            ),
            reply_markup=mode_selection_keyboard(),
        )
        return

    # ── Flow Step 2: Mode Selection (WITHOUT IP vs WITH IP ROTATION) ───
    if state.get("step") == "AWAITING_MODE":
        target_url = state.get("url")

        if text == "🟢 WITHOUT IP":
            user_states[user.id] = {
                "step": "AWAITING_CYCLES",
                "url": target_url,
                "mode": "NORMAL",
            }
            bot.send_message(
                chat_id,
                (
                    f"🟢 <b>Normal Connection Mode Selected</b>\n\n"
                    f"🔗 <code>{html.escape(target_url[:42])}</code>\n\n"
                    f"Select how many visits you want to run:"
                ),
                reply_markup=extraction_cycles_keyboard(),
            )
            return

        elif text == "🌐 WITH IP ROTATION":
            if not endpoint_manager.has_endpoints():
                bot.send_message(
                    chat_id,
                    (
                        "⚠️ <b>IP Rotation Unavailable</b>\n\n"
                        "No alternate network endpoint is configured for this bot.\n"
                        "Admins can add proxies via the <b>/admin -> 🌐 Proxy Manager</b> panel at any time!\n\n"
                        "<i>You can still extract using 🟢 WITHOUT IP mode below.</i>"
                    ),
                    reply_markup=mode_selection_keyboard(),
                )
                return

            user_states[user.id] = {
                "step": "AWAITING_CYCLES",
                "url": target_url,
                "mode": "ROTATING",
            }
            ep_count = endpoint_manager.get_endpoint_count()
            bot.send_message(
                chat_id,
                (
                    f"🌐 <b>IP Rotation Mode Selected</b>\n\n"
                    f"🔗 <code>{html.escape(target_url[:42])}</code>\n"
                    f"📡 Available Network Endpoints: <code>{ep_count}</code>\n\n"
                    f"Select how many visits you want to run:"
                ),
                reply_markup=extraction_cycles_keyboard(),
            )
            return

    # ── Flow Step 3: Cycles Selection & Execution ───────────────
    if state.get("step") == "AWAITING_CYCLES":
        count = _CYCLE_COUNT_MAP.get(text)
        if count is not None:
            target_url = state.get("url")
            mode = state.get("mode", "NORMAL")
            user_states[user.id] = {}

            mode_name = "🌐 IP Rotation" if mode == "ROTATING" else "🟢 Normal Connection"

            start_msg = bot.send_message(
                chat_id,
                (
                    f"⏳ <b>EXTRACTION IN PROGRESS</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"<b>Mode:</b> {mode_name}\n"
                    f"<b>Visits:</b> 0/{count}\n"
                    f"<b>Successful:</b> 0\n"
                    f"<b>Failed:</b> 0\n"
                    f"<b>Unique Numbers:</b> 0\n"
                    f"<b>Elapsed:</b> 0s\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"<i>Initializing engine...</i>"
                ),
                reply_markup=main_keyboard(),
            )

            threading.Thread(
                target=extraction_worker,
                args=(chat_id, user.id, target_url, mode, count, start_msg.message_id),
                daemon=True,
            ).start()
            return

    # Fallback
    bot.send_message(
        chat_id,
        "❓ Please select an option from the menu below or tap <b>🔗 Send New Link</b>.",
        reply_markup=main_keyboard(),
    )


# =========================================================
# Application Entry Point & Auto-Reconnection
# =========================================================
if __name__ == "__main__":
    init_db()

    print("=" * 60)
    print("DK Sharma Universal WhatsApp Extractor — Production Engine")
    print(f"Admins: {ADMIN_IDS}")
    print(f"Total Network Endpoints Configured: {endpoint_manager.get_endpoint_count()}")
    print("=" * 60)

    try:
        bot.remove_webhook()
        time.sleep(1)
        print("Webhook status reset.")
    except Exception as e:
        print(f"Webhook notice: {e}")

    print("Bot polling initiated...")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"Polling warning: {e}")
            time.sleep(4)
            print("Reconnecting...")
