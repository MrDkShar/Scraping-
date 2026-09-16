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
# Configuration & Constants
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "123456789").split(",")
    if x.strip().isdigit()
]

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# =========================================================
# Maintenance Mode (Admin can toggle ON/OFF)
# =========================================================
MAINTENANCE_MODE = False  # False = Bot active for all | True = Only admins can use

# user_id -> {"url": str, "awaiting_url": bool, "awaiting_broadcast": bool}
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
# Pure Python AES-128-CBC Decryptor (Zero Dependency)
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


def decrypt_byet_challenge(c_hex: str, a_hex: str, b_hex: str) -> str:
    """
    Solves ByetHost / InfinityFree / site.je slowAES.decrypt(c, 2, a, b).
    Mode 2 = AES-128-CBC. Decrypts ciphertext 'c' using key 'a' and IV 'b'.
    Returns hex string for document.cookie = '__test=' + hex.
    """
    # 1. Fast path: pycryptodome if installed
    try:
        from Crypto.Cipher import AES
        cipher = AES.new(bytes.fromhex(a_hex), AES.MODE_CBC, bytes.fromhex(b_hex))
        return cipher.decrypt(bytes.fromhex(c_hex)).hex()
    except Exception:
        pass

    # 2. Fast path: OpenSSL command if available
    import shutil
    import subprocess
    if shutil.which("openssl"):
        try:
            p = subprocess.Popen(
                ["openssl", "enc", "-d", "-aes-128-cbc", "-K", a_hex, "-iv", b_hex, "-nopad"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, _ = p.communicate(bytes.fromhex(c_hex))
            if p.returncode == 0 and len(out) == 16:
                return out.hex()
        except Exception:
            pass

    # 3. Guaranteed Pure Python path (zero dependencies, works anywhere)
    c = bytes.fromhex(c_hex)
    a = bytes.fromhex(a_hex)
    b = bytes.fromhex(b_hex)
    w = _key_schedule(a)
    dec = _decrypt_single_block(c, w)
    res = bytes([dec[i] ^ b[i] for i in range(16)])
    return res.hex()


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
    """Extracts valid WhatsApp phone numbers (10 to 15 digits) from raw or URL-encoded text."""
    found: set[str] = set()
    if not text:
        return found

    decoded = urllib.parse.unquote(text)
    for sample in (text, decoded):
        for pattern in _WA_PATTERNS:
            for match in pattern.findall(sample):
                if isinstance(match, tuple):
                    match = match[0]
                clean = re.sub(r'\D', '', match)
                if 10 <= len(clean) <= 15:
                    found.add(clean)
    return found


# =========================================================
# Intelligent Session with Anti-Bot Challenge Solving
# =========================================================
class RotatingScraperSession:
    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cj))
        self.cached_test_cookie = None
        self.domain = None

    def fetch(self, url: str) -> tuple[str, str, list[str]]:
        """
        Visits the URL, solves any ByetHost / InfinityFree AES challenges,
        follows all HTTP redirects and client-side JavaScript / meta redirects,
        and returns: (final_url, final_body, all_visited_urls).
        """
        parsed = urllib.parse.urlparse(url)
        self.domain = parsed.hostname

        self.opener.addheaders = [
            ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
            ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"),
            ("Accept-Language", "en-US,en;q=0.9"),
            ("Sec-Ch-Ua", '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"'),
            ("Sec-Ch-Ua-Mobile", "?0"),
            ("Sec-Ch-Ua-Platform", '"Windows"'),
            ("Upgrade-Insecure-Requests", "1"),
        ]

        # Reuse valid __test cookie if available for this domain
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
        resp = self.opener.open(req, timeout=12)
        current_url = resp.geturl()
        visited_urls.append(current_url)
        body = resp.read().decode("utf-8", errors="ignore")

        # Detect and solve InfinityFree / ByetHost slowAES anti-bot challenge
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

                # Find redirect destination (e.g. ?i=1)
                loc_match = re.search(r'location\.href\s*=\s*["\'](.*?)["\']', body)
                next_dest = loc_match.group(1) if loc_match else (url + ("&i=1" if "?" in url else "?i=1"))
                next_url = urllib.parse.urljoin(current_url, next_dest)

                visited_urls.append(next_url)
                resp2 = self.opener.open(urllib.request.Request(next_url), timeout=12)
                current_url = resp2.geturl()
                visited_urls.append(current_url)
                body = resp2.read().decode("utf-8", errors="ignore")

        # Check for HTML meta refresh or JavaScript redirection
        for _ in range(2):
            meta_refresh = re.search(r'<meta[^>]*http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\'>]+)', body, re.IGNORECASE)
            if meta_refresh:
                redirect_target = urllib.parse.urljoin(current_url, meta_refresh.group(1).strip())
                visited_urls.append(redirect_target)
                resp = self.opener.open(urllib.request.Request(redirect_target), timeout=12)
                current_url = resp.geturl()
                visited_urls.append(current_url)
                body = resp.read().decode("utf-8", errors="ignore")
                continue

            js_redirect = re.search(r'(?:window\.)?location(?:\.href|\.replace)\s*\(\s*["\'](https?://[^"\']+)["\']\s*\)', body, re.IGNORECASE)
            if js_redirect:
                redirect_target = js_redirect.group(1).strip()
                visited_urls.append(redirect_target)
                resp = self.opener.open(urllib.request.Request(redirect_target), timeout=12)
                current_url = resp.geturl()
                visited_urls.append(current_url)
                body = resp.read().decode("utf-8", errors="ignore")
                continue
            break

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
) -> None:
    active_jobs[user_id] = True
    session = RotatingScraperSession()

    found_numbers: set[str] = set()
    total_numbers_seen = 0
    total_ok = 0
    errors = 0
    last_ui_update = 0.0

    for i in range(1, count + 1):
        if not active_jobs.get(user_id, True):
            break

        target = add_cache_buster(url, i)
        try:
            final_url, body, visited_urls = session.fetch(target)
            total_ok += 1

            cycle_numbers: set[str] = set()
            for v_url in visited_urls:
                cycle_numbers.update(extract_numbers_from_text(v_url))
            cycle_numbers.update(extract_numbers_from_text(body))

            total_numbers_seen += len(cycle_numbers)
            found_numbers.update(cycle_numbers)
        except Exception:
            errors += 1

        # Periodic UI update with throttle to avoid Telegram rate limits
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

    # ── Extraction Complete ───────────────────────────────────────────
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
        # ── 1. Show numbers directly in chat (with copy button) ──────
        sorted_numbers = sorted(found_numbers)
        numbers_text = "\n".join(f"+{num}" for num in sorted_numbers)

        # Telegram message limit is 4096 chars; split if needed
        CHUNK_SIZE = 3800
        header = (
            f"📱 *Extracted WhatsApp Numbers*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 *Total Unique:* `{unique_count}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        )

        # Split numbers into chunks so each message fits Telegram limit
        lines = numbers_text.split("\n")
        chunks = []
        current_chunk = ""
        for line in lines:
            if len(current_chunk) + len(line) + 1 > CHUNK_SIZE:
                chunks.append(current_chunk.strip())
                current_chunk = line + "\n"
            else:
                current_chunk += line + "\n"
        if current_chunk.strip():
            chunks.append(current_chunk.strip())

        # Send first chunk with header and copy button (inline keyboard)
        copy_markup = types.InlineKeyboardMarkup()
        copy_markup.add(
            types.InlineKeyboardButton(
                text="📋 Copy All Numbers",
                switch_inline_query=numbers_text,
            )
        )

        for idx, chunk in enumerate(chunks):
            chunk_header = header if idx == 0 else f"📱 *Numbers (Part {idx + 1})*\n\n"
            msg_text = chunk_header + f"`{chunk}`"
            try:
                if idx == 0:
                    bot.send_message(
                        chat_id,
                        msg_text,
                        parse_mode="Markdown",
                        reply_markup=copy_markup,
                    )
                else:
                    bot.send_message(
                        chat_id,
                        msg_text,
                        parse_mode="Markdown",
                    )
            except Exception:
                # If markdown fails, send plain
                bot.send_message(chat_id, chunk)

        # ── 2. Also send as .txt file ─────────────────────────────────
        file_name = f"whatsapp_numbers_{user_id}_{int(time.time())}.txt"
        try:
            with open(file_name, "w", encoding="utf-8") as f:
                f.write("DK Sharma Bot — WhatsApp Number Extractor\n")
                f.write(f"Source URL: {url}\n")
                f.write(f"Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Unique Numbers: {unique_count}\n")
                f.write("=" * 45 + "\n\n")
                for num in sorted_numbers:
                    f.write(f"+{num}\n")

            with open(file_name, "rb") as doc:
                bot.send_document(
                    chat_id,
                    doc,
                    caption=(
                        f"📁 *Your WhatsApp Numbers File is Ready!*\n"
                        f"📱 `Total Numbers: {unique_count}`\n"
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
                "• Website is currently down\n"
                "• Link has expired\n"
                "• Rotation limit reached\n\n"
                "_Try another link or test with 1x first._"
            ),
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
            f"👋 *Welcome to DK Sharma Bot!*\n\n"
            f"🤖 *WhatsApp Number Extractor*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Made by *DK Sharma* 🎯\n\n"
            f"🔥 *What can this bot do?*\n"
            f"Extract hidden WhatsApp numbers from any rotating or redirect link — "
            f"even links with JavaScript & anti-bot protection (like ByetHost/site.je)!\n\n"
            f"📌 *How to use:*\n"
            f"1️⃣ Press *🔗 Send New Link*\n"
            f"2️⃣ Send your rotating/redirect URL\n"
            f"3️⃣ Choose extraction count (1, 20, 50, 100)\n"
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
    global MAINTENANCE_MODE

    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()

    register_user(user.id, user.username, user.first_name)
    state = user_states.get(user.id, {})

    # ── Maintenance Mode Check ────────────────────────────────────────
    if MAINTENANCE_MODE and user.id not in ADMIN_IDS:
        bot.send_message(
            chat_id,
            (
                "🔧 *Bot is under Maintenance*\n\n"
                "We are currently improving the bot for a better experience.\n"
                "Please try again later. Thank you for your patience! 🙏\n\n"
                "_— DK Sharma Bot_"
            ),
            parse_mode="Markdown",
        )
        return

    # Global Cancel / Back to Main Menu
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

    # Admin Broadcast
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
        try:
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
        except Exception:
            pass
        bot.send_message(chat_id, "Admin Panel:", reply_markup=admin_keyboard())
        return

    # Admin Buttons
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

        if text in ("🟢 Maintenance: OFF", "🔴 Maintenance: ON"):
            MAINTENANCE_MODE = not MAINTENANCE_MODE
            status = "🔴 *ON* — Users cannot use the bot now." if MAINTENANCE_MODE else "🟢 *OFF* — Bot is active for all users."
            bot.send_message(
                chat_id,
                (
                    f"🔧 *Maintenance Mode Updated!*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Status: {status}"
                ),
                parse_mode="Markdown",
                reply_markup=admin_keyboard(),
            )
            return

    # Send New Link
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

    # My Stats
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

    # My History
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

    # Help
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
                "A link that redirects to WhatsApp with a different phone number each time. "
                "This bot bypasses all anti-bot protections and saves all unique numbers!"
            ),
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
        return

    # Support
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

    # URL Submission
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
                f"• More visits = more rotating numbers collected\n"
                f"• Duplicates are automatically removed"
            ),
            parse_mode="Markdown",
            reply_markup=extraction_keyboard(),
        )
        return

    # Extraction Count Selection
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
                    f"_Bypassing protections and extracting numbers. Please wait..._"
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

    # Fallback
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
