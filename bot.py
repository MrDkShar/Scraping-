"""
DK SCRAPING BOT — Production Edition
=====================================
Single-file Telegram bot for authorized public-URL phone-number extraction.

Upgraded from the existing DK Sharma Bot codebase. Preserves the original
SQLite/WAL schema, approval system, multi-admin roles, proxy pool, channel
posting, background retest, and extraction pipeline. Adds:

  • Centralized navigation (Back / Cancel on every state screen)
  • Robust session state machine with 15-min auto-expiry
  • True thread-safe cancellation (threading.Event per job) + pause/resume
  • Bulk multi-URL extraction with bounded worker pool + per-host limits
  • Job IDs in DK-XXXXXX format with full lifecycle tracking
  • Optimized IP mode — cached exit-IP, no per-request IP verification
  • SSRF protection (blocks private/loopback/link-local targets)
  • Fetch-latest-proxies with pluggable source adapters
  • CSV / JSON / TXT export, copy-friendly output
  • Crash recovery: RUNNING → INTERRUPTED on restart
  • Confirmation dialogs for destructive actions
  • Premium UI with consistent branding, pagination, empty states
  • Telegram flood-safe helpers (edit throttling, retry-after handling)
  • No hardcoded secrets — BOT_TOKEN from env only

Privacy: only fetches URLs the operator is authorized to process. No CAPTCHA
bypass, no login bypass, no private-account scraping, no anti-bot evasion.
"""

import os
import re
import io
import time
import json
import html
import random
import string
import sqlite3
import threading
import urllib.parse
import urllib.request
import urllib.error
import http.cookiejar
import socket
import ipaddress
import ssl
import logging
import shutil
import subprocess
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Set, Tuple

import requests

try:
    import socks   # PySocks — required for SOCKS proxies
    _SOCKS_OK = True
except Exception:
    _SOCKS_OK = False

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

# =========================================================
#  Logging  (structured, credential-safe)
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dkbot")
logging.getLogger("urllib3").setLevel(logging.WARNING)


def _mask(s: str) -> str:
    """Strip credentials from any string before it reaches logs or UI."""
    if not s:
        return ""
    s = re.sub(r"(://)([^:@/\s]+):([^@/\s]+)(@)", r"\1***:***\4", s)
    return s


# =========================================================
#  Configuration  (all from environment — no hardcoded secrets)
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8553353076:AAEyTnBUF76vioo3Jw0tsRnLEpLeMKHA7Tc").strip()
if not BOT_TOKEN:
    raise SystemExit(
        "FATAL: BOT_TOKEN environment variable is not set. "
        "Refusing to start without a token. Set BOT_TOKEN in your environment."
    )

ADMIN_IDS: List[int] = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "8753914631").split(",")
    if x.strip().isdigit()
]

DB_PATH         = os.environ.get("DATABASE_PATH", "bot_database.db")
DEFAULT_CHANNEL = os.environ.get("CHANNEL_USERNAME", "")

# ---- tunables (sensible defaults) ----
REQUEST_TIMEOUT         = int(os.environ.get("REQUEST_TIMEOUT", "15"))
CONNECT_TIMEOUT         = int(os.environ.get("CONNECT_TIMEOUT", "6"))
READ_TIMEOUT            = int(os.environ.get("READ_TIMEOUT", "10"))
MAX_CONCURRENCY         = int(os.environ.get("MAX_CONCURRENCY", "4"))
PROGRESS_INTERVAL       = float(os.environ.get("PROGRESS_INTERVAL", "1.2"))
MAX_VISITS_PER_JOB      = int(os.environ.get("MAX_VISITS_PER_JOB", "100"))
MAX_RESPONSE_SIZE       = int(os.environ.get("MAX_RESPONSE_SIZE", str(5 * 1024 * 1024)))
MAX_REDIRECTS           = int(os.environ.get("MAX_REDIRECTS", "10"))
MAX_RETRIES             = int(os.environ.get("MAX_RETRIES", "2"))
PROXY_TEST_CONCURRENCY  = int(os.environ.get("PROXY_TEST_CONCURRENCY", "12"))
PROXY_HEALTH_TIMEOUT    = int(os.environ.get("PROXY_HEALTH_TIMEOUT", "8"))
PROXY_CACHE_TTL         = int(os.environ.get("PROXY_CACHE_TTL", "300"))   # seconds
MAX_URLS_PER_BATCH      = int(os.environ.get("MAX_URLS_PER_BATCH", "500"))
MAX_CONCURRENT_JOBS     = int(os.environ.get("MAX_CONCURRENT_JOBS", "3"))
SESSION_EXPIRY_MIN      = int(os.environ.get("SESSION_EXPIRY_MIN", "15"))
BULK_WORKERS            = int(os.environ.get("BULK_WORKERS", "8"))
PER_HOST_LIMIT          = int(os.environ.get("PER_HOST_LIMIT", "2"))

BOOTSTRAP_OWNER_ID = ADMIN_IDS[0] if ADMIN_IDS else 0

bot = telebot.TeleBot(
    BOT_TOKEN,
    parse_mode="Markdown",
    threaded=True,
    num_threads=4,
)

# =========================================================
#  Global state  (all guarded by locks)
# =========================================================
MAINTENANCE_MODE = False

_state_lock  = threading.RLock()
user_states: Dict[int, dict] = {}      # user_id  -> session dict
active_jobs:  Dict[int, Set[str]] = {}  # user_id  -> set of job_codes (multi-job)
job_state:    Dict[str, dict] = {}      # job_code -> live JobState dict

_db_lock = threading.RLock()

# =========================================================
#  Markdown helpers  (escape dynamic content safely)
# =========================================================
_MD_SPECIAL = re.compile(r"([_*`\[\]])")


def md_esc(text) -> str:
    """Escape MarkdownV1 special characters in dynamic content."""
    if text is None:
        return ""
    return _MD_SPECIAL.sub(r"\\\1", str(text))


def _short(text: str, n: int = 60) -> str:
    if not text:
        return ""
    return text if len(text) <= n else text[: n - 1] + "…"


def _bar(pct: int, width: int = 12) -> str:
    pct = max(0, min(100, pct))
    done = pct * width // 100
    return "█" * done + "░" * (width - done)


def _fmt_duration(ms: int) -> str:
    s = ms / 1000
    if s < 60:
        return f"{s:.1f}s"
    m, sec = divmod(int(s), 60)
    if m < 60:
        return f"{m:02d}:{sec:02d}"
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def _now_iso() -> str:
    return datetime.utcnow().isoformat(sep=" ", timespec="seconds")


def _job_code(job_id: int) -> str:
    """Stable 6-hex display code from numeric job id."""
    return f"DK-{job_id & 0xFFFFFF:06X}"


# =========================================================
#  Telegram-safe send / edit helpers  (flood protection)
# =========================================================
def _safe_edit(chat_id: int, msg_id: int, text: str,
               markup=None, parse_mode: str = "Markdown") -> bool:
    """Edit a message, swallowing 'not modified' and handling 429."""
    try:
        bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id, text=text,
            parse_mode=parse_mode, reply_markup=markup,
        )
        return True
    except ApiTelegramException as e:
        msg = str(e).lower()
        if "not modified" in msg:
            return True
        if "retry after" in msg or "429" in msg:
            m = re.search(r"retry after (\d+)", msg)
            time.sleep(int(m.group(1)) + 1 if m else 2)
            return False
        return False
    except Exception:
        return False


def _safe_send(chat_id: int, text: str, markup=None,
               parse_mode: str = "Markdown") -> Optional[types.Message]:
    """Send a message; fall back to plain text if Markdown fails."""
    try:
        return bot.send_message(
            chat_id, text, parse_mode=parse_mode, reply_markup=markup,
        )
    except ApiTelegramException as e:
        if "retry after" in str(e).lower() or "429" in str(e):
            m = re.search(r"retry after (\d+)", str(e), re.I)
            time.sleep(int(m.group(1)) + 1 if m else 2)
            try:
                return bot.send_message(chat_id, text, parse_mode=parse_mode,
                                        reply_markup=markup)
            except Exception:
                return bot.send_message(chat_id, text, reply_markup=markup)
        try:
            return bot.send_message(chat_id, text, reply_markup=markup)
        except Exception:
            return None
    except Exception:
        return None


def _answer_cb(c: types.CallbackQuery, text: str = "") -> None:
    try:
        bot.answer_callback_query(c.id, text)
    except Exception:
        pass


# =========================================================
#  Database  (SQLite + WAL, safe additive migration)
# =========================================================
_SCHEMA_VERSION = 5


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _col_exists(conn, table: str, col: str) -> bool:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return any(r[1] == col for r in cur.fetchall())
    except sqlite3.Error:
        return False


def init_db() -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id               INTEGER PRIMARY KEY,
                    username              TEXT,
                    first_name            TEXT,
                    status                TEXT DEFAULT 'APPROVED',
                    blocked               INTEGER DEFAULT 0,
                    plan                  TEXT DEFAULT 'FREE',
                    total_extractions     INTEGER DEFAULT 0,
                    total_numbers_found   INTEGER DEFAULT 0,
                    total_urls_processed  INTEGER DEFAULT 0,
                    joined_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS admins (
                    user_id     INTEGER PRIMARY KEY,
                    username    TEXT,
                    role        TEXT DEFAULT 'ADMIN',
                    added_by    INTEGER,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active   INTEGER DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS extraction_jobs (
                    job_id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_code            TEXT,
                    user_id             INTEGER,
                    username            TEXT,
                    source_url          TEXT,
                    mode                TEXT,
                    requested_visits    INTEGER,
                    successful_visits   INTEGER DEFAULT 0,
                    failed_visits       INTEGER DEFAULT 0,
                    unique_numbers      INTEGER DEFAULT 0,
                    duplicate_numbers   INTEGER DEFAULT 0,
                    duration_ms         INTEGER DEFAULT 0,
                    status              TEXT DEFAULT 'QUEUED',
                    error_summary       TEXT,
                    bulk_group_id       TEXT,
                    started_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at        TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS extraction_job_numbers (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id              INTEGER,
                    user_id             INTEGER,
                    number              TEXT,
                    source_url          TEXT,
                    extraction_method   TEXT,
                    visit_number        INTEGER,
                    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES extraction_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS extraction_attempts (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id          INTEGER,
                    cycle           INTEGER,
                    proxy_id        INTEGER,
                    exit_ip         TEXT,
                    request_status  TEXT,
                    latency_ms      INTEGER,
                    error           TEXT,
                    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES extraction_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS proxies (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint            TEXT,
                    protocol            TEXT,
                    host                TEXT,
                    port                INTEGER,
                    username            TEXT,
                    password            TEXT,
                    is_active           INTEGER DEFAULT 1,
                    health_status       TEXT DEFAULT 'UNTESTED',
                    health_score        INTEGER DEFAULT 0,
                    success_count       INTEGER DEFAULT 0,
                    failure_count       INTEGER DEFAULT 0,
                    consecutive_failures INTEGER DEFAULT 0,
                    average_latency     INTEGER DEFAULT 0,
                    last_observed_ip    TEXT,
                    last_error          TEXT,
                    last_tested         TIMESTAMP,
                    last_success        TIMESTAMP,
                    last_failure        TIMESTAMP,
                    cooldown_until      TIMESTAMP,
                    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key     TEXT PRIMARY KEY,
                    value   TEXT
                );

                CREATE TABLE IF NOT EXISTS admin_audit_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id    INTEGER,
                    action      TEXT,
                    target      TEXT,
                    details     TEXT,
                    timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS channel_posts (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id              INTEGER,
                    channel             TEXT,
                    status              TEXT,
                    message_id          INTEGER,
                    error               TEXT,
                    posted_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS proxy_sources (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    name        TEXT UNIQUE,
                    url         TEXT,
                    is_enabled  INTEGER DEFAULT 1,
                    added_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS bulk_groups (
                    group_id    TEXT PRIMARY KEY,
                    user_id     INTEGER,
                    total_urls  INTEGER DEFAULT 0,
                    processed   INTEGER DEFAULT 0,
                    successful  INTEGER DEFAULT 0,
                    failed      INTEGER DEFAULT 0,
                    numbers     INTEGER DEFAULT 0,
                    status      TEXT DEFAULT 'QUEUED',
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_user    ON extraction_jobs(user_id);
                CREATE INDEX IF NOT EXISTS idx_jobs_date     ON extraction_jobs(started_at);
                CREATE INDEX IF NOT EXISTS idx_jobs_status   ON extraction_jobs(status);
                CREATE INDEX IF NOT EXISTS idx_jobs_code     ON extraction_jobs(job_code);
                CREATE INDEX IF NOT EXISTS idx_nums_job      ON extraction_job_numbers(job_id);
                CREATE INDEX IF NOT EXISTS idx_attempts_job  ON extraction_attempts(job_id);
                CREATE INDEX IF NOT EXISTS idx_proxy_host    ON proxies(host, port);
                CREATE INDEX IF NOT EXISTS idx_audit_time    ON admin_audit_log(timestamp);
                """
            )

            # ---- safe additive migrations (never delete data) ----
            migrations = [
                ("users", "plan", "TEXT DEFAULT 'FREE'"),
                ("users", "total_urls_processed", "INTEGER DEFAULT 0"),
                ("extraction_jobs", "job_code", "TEXT"),
                ("extraction_jobs", "error_summary", "TEXT"),
                ("extraction_jobs", "bulk_group_id", "TEXT"),
                ("extraction_jobs", "username", "TEXT"),
                ("extraction_jobs", "successful_visits", "INTEGER DEFAULT 0"),
                ("extraction_jobs", "failed_visits", "INTEGER DEFAULT 0"),
                ("extraction_jobs", "duration_ms", "INTEGER DEFAULT 0"),
            ]
            for tbl, col, decl in migrations:
                if not _col_exists(conn, tbl, col):
                    conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {decl}")

            # back-fill job_code for legacy rows
            conn.execute(
                "UPDATE extraction_jobs SET job_code = 'DK-' || "
                "printf('%06X', job_id & 16777215) WHERE job_code IS NULL"
            )

            conn.commit()
        finally:
            conn.close()


# ---------- Settings ----------
_DEFAULT_SETTINGS = {
    "maintenance_mode": "0",
    "approval_mode": "0",
    "channel_logging": "0",
    "channel_username": DEFAULT_CHANNEL,
    "support_username": "",
    "admin_display_name": "DK Scraping Bot",
    "max_visits": str(MAX_VISITS_PER_JOB),
    "request_timeout": str(REQUEST_TIMEOUT),
    "connect_timeout": str(CONNECT_TIMEOUT),
    "read_timeout": str(READ_TIMEOUT),
    "proxy_enabled": "1",
    "max_concurrency": str(MAX_CONCURRENCY),
    "progress_interval": str(PROGRESS_INTERVAL),
    "channel_include_username": "1",
    "channel_include_uid": "0",
    "channel_include_method": "1",
    "channel_include_numbers": "1",
    "channel_include_source": "1",
    "channel_attach_txt": "0",
    "show_duration": "1",
    "show_job_id": "1",
    "auto_delete_results_hours": "0",
}


def get_setting(key: str, default: Optional[str] = None) -> str:
    with _db_lock:
        conn = get_conn()
        try:
            r = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            if r:
                return r["value"]
            if default is not None:
                return default
            return _DEFAULT_SETTINGS.get(key, "")
        finally:
            conn.close()


def get_settings_batch(keys) -> dict:
    with _db_lock:
        conn = get_conn()
        try:
            out = {}
            for k in keys:
                r = conn.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()
                out[k] = r["value"] if r else _DEFAULT_SETTINGS.get(k, "")
            return out
        finally:
            conn.close()


def set_setting(key: str, value: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
            conn.commit()
        finally:
            conn.close()


def seed_settings() -> None:
    for k, v in _DEFAULT_SETTINGS.items():
        with _db_lock:
            conn = get_conn()
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v)
                )
                conn.commit()
            finally:
                conn.close()


def audit_log(admin_id: int, action: str, target: str = "", details: str = "") -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO admin_audit_log(admin_id, action, target, details) "
                "VALUES(?,?,?,?)",
                (admin_id, action, target, details),
            )
            conn.commit()
        finally:
            conn.close()


def is_admin(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    with _db_lock:
        conn = get_conn()
        try:
            r = conn.execute(
                "SELECT 1 FROM admins WHERE user_id=? AND is_active=1", (user_id,)
            ).fetchone()
            return r is not None
        finally:
            conn.close()


def admin_role(user_id: int) -> Optional[str]:
    if user_id in ADMIN_IDS:
        return "OWNER"
    with _db_lock:
        conn = get_conn()
        try:
            r = conn.execute(
                "SELECT role FROM admins WHERE user_id=? AND is_active=1", (user_id,)
            ).fetchone()
            return r["role"] if r else None
        finally:
            conn.close()


# ---------- Users ----------
def register_user(user_id: int, username: str = None, first_name: str = None) -> str:
    """Return user status: APPROVED / PENDING / BLOCKED."""
    with _db_lock:
        conn = get_conn()
        try:
            existing = conn.execute(
                "SELECT status, blocked FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE users SET last_active=CURRENT_TIMESTAMP,
                       username=COALESCE(?, username),
                       first_name=COALESCE(?, first_name) WHERE user_id=?""",
                    (username, first_name, user_id),
                )
                conn.commit()
                if existing["blocked"]:
                    return "BLOCKED"
                return existing["status"]
            approval = get_setting("approval_mode", "0")
            status = "PENDING" if approval == "1" else "APPROVED"
            conn.execute(
                "INSERT INTO users(user_id, username, first_name, status) "
                "VALUES(?,?,?,?)",
                (user_id, username, first_name, status),
            )
            conn.commit()
            return status
        finally:
            conn.close()


def update_user_stats(user_id: int, unique_count: int, urls_processed: int = 0) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE users SET total_extractions=total_extractions+1,
                   total_numbers_found=total_numbers_found+?,
                   total_urls_processed=total_urls_processed+?,
                   last_active=CURRENT_TIMESTAMP WHERE user_id=?""",
                (unique_count, urls_processed, user_id),
            )
            conn.commit()
        finally:
            conn.close()


def set_user_status(user_id: int, status: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "UPDATE users SET status=? WHERE user_id=?", (status, user_id)
            )
            if status == "BLOCKED":
                conn.execute("UPDATE users SET blocked=1 WHERE user_id=?", (user_id,))
            elif status == "APPROVED":
                conn.execute("UPDATE users SET blocked=0 WHERE user_id=?", (user_id,))
            conn.commit()
        finally:
            conn.close()


def get_user(user_id: int) -> Optional[dict]:
    with _db_lock:
        conn = get_conn()
        try:
            r = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
            return dict(r) if r else None
        finally:
            conn.close()


def search_users(query: str, limit: int = 20) -> list:
    with _db_lock:
        conn = get_conn()
        try:
            q = f"%{query}%"
            rows = conn.execute(
                """SELECT * FROM users
                   WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ?
                   OR first_name LIKE ?
                   ORDER BY joined_at DESC LIMIT ?""",
                (q, q, q, limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def recent_users(limit: int = 10, page: int = 0) -> Tuple[list, int]:
    with _db_lock:
        conn = get_conn()
        try:
            total = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
            rows = conn.execute(
                "SELECT * FROM users ORDER BY joined_at DESC LIMIT ? OFFSET ?",
                (limit, page * limit),
            ).fetchall()
            return [dict(r) for r in rows], total
        finally:
            conn.close()


def all_user_ids() -> list:
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT user_id FROM users WHERE blocked=0 AND status='APPROVED'"
            ).fetchall()
            return [r["user_id"] for r in rows]
        finally:
            conn.close()


def pending_users() -> list:
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM users WHERE status='PENDING' AND blocked=0 "
                "ORDER BY joined_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


# ---------- Jobs ----------
def create_job(user_id: int, username: str, url: str, mode: str,
               visits: int, bulk_group_id: str = None) -> Tuple[int, str]:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """INSERT INTO extraction_jobs
                   (user_id, username, source_url, mode, requested_visits,
                    status, bulk_group_id)
                   VALUES(?,?,?,?,?, 'RUNNING', ?)""",
                (user_id, username, url, mode, visits, bulk_group_id),
            )
            job_id = cur.lastrowid
            code = _job_code(job_id)
            conn.execute(
                "UPDATE extraction_jobs SET job_code=? WHERE job_id=?",
                (code, job_id),
            )
            conn.commit()
            return job_id, code
        finally:
            conn.close()


def finish_job(job_id: int, success: int, failed: int, unique: int,
               dupes: int, duration_ms: int, status: str,
               error_summary: str = "") -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE extraction_jobs SET successful_visits=?, failed_visits=?,
                   unique_numbers=?, duplicate_numbers=?, duration_ms=?, status=?,
                   error_summary=?, completed_at=CURRENT_TIMESTAMP WHERE job_id=?""",
                (success, failed, unique, dupes, duration_ms, status,
                 error_summary, job_id),
            )
            conn.commit()
        finally:
            conn.close()


def _save_numbers_batch(job_id: int, user_id: int,
                        rows: List[Tuple[str, str, str, int]]) -> None:
    """Batch-insert extracted numbers in a single transaction."""
    if not rows:
        return
    with _db_lock:
        conn = get_conn()
        try:
            conn.executemany(
                """INSERT INTO extraction_job_numbers
                   (job_id, user_id, number, source_url, extraction_method,
                    visit_number)
                   VALUES(?,?,?,?,?,?)""",
                [(job_id, user_id, n, src, m, v) for n, m, src, v in rows],
            )
            conn.commit()
        finally:
            conn.close()


def save_attempt(job_id: int, cycle: int, proxy_id: Optional[int], exit_ip: str,
                 status: str, latency_ms: int, error: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO extraction_attempts
                   (job_id, cycle, proxy_id, exit_ip, request_status,
                    latency_ms, error)
                   VALUES(?,?,?,?,?,?,?)""",
                (job_id, cycle, proxy_id, exit_ip, status, latency_ms, error),
            )
            conn.commit()
        finally:
            conn.close()


def job_numbers(job_id: int, limit: int = 200) -> list:
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM extraction_job_numbers WHERE job_id=? LIMIT ?",
                (job_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def job_attempts(job_id: int, limit: int = 50) -> list:
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM extraction_attempts WHERE job_id=? "
                "ORDER BY cycle LIMIT ?",
                (job_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_job(job_id: int) -> Optional[dict]:
    with _db_lock:
        conn = get_conn()
        try:
            r = conn.execute(
                "SELECT * FROM extraction_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            return dict(r) if r else None
        finally:
            conn.close()


def get_job_by_code(code: str) -> Optional[dict]:
    with _db_lock:
        conn = get_conn()
        try:
            r = conn.execute(
                "SELECT * FROM extraction_jobs WHERE job_code=?", (code,)
            ).fetchone()
            return dict(r) if r else None
        finally:
            conn.close()


def recent_jobs(limit: int = 15, days: Optional[int] = None) -> list:
    with _db_lock:
        conn = get_conn()
        try:
            if days:
                rows = conn.execute(
                    """SELECT * FROM extraction_jobs
                       WHERE started_at >= datetime('now', ?)
                       ORDER BY started_at DESC LIMIT ?""",
                    (f"-{days} days", limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM extraction_jobs ORDER BY started_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def user_jobs(user_id: int, limit: int = 10, page: int = 0) -> Tuple[list, int]:
    with _db_lock:
        conn = get_conn()
        try:
            total = conn.execute(
                "SELECT COUNT(*) c FROM extraction_jobs WHERE user_id=?",
                (user_id,),
            ).fetchone()["c"]
            rows = conn.execute(
                """SELECT * FROM extraction_jobs WHERE user_id=?
                   ORDER BY started_at DESC LIMIT ? OFFSET ?""",
                (user_id, limit, page * limit),
            ).fetchall()
            return [dict(r) for r in rows], total
        finally:
            conn.close()


def admin_dashboard_stats(days: Optional[int] = None) -> dict:
    with _db_lock:
        conn = get_conn()
        try:
            d = {}
            span = f"-{days} days" if days else None
            d["total_users"] = conn.execute(
                "SELECT COUNT(*) c FROM users WHERE blocked=0"
            ).fetchone()["c"]
            d["active_today"] = conn.execute(
                "SELECT COUNT(*) c FROM users WHERE last_active >= "
                "datetime('now','-1 day')"
            ).fetchone()["c"]
            d["total_jobs"] = conn.execute(
                "SELECT COUNT(*) c FROM extraction_jobs"
            ).fetchone()["c"]
            d["jobs_today"] = conn.execute(
                "SELECT COUNT(*) c FROM extraction_jobs WHERE started_at >= "
                "datetime('now','-1 day')"
            ).fetchone()["c"]
            d["successful_jobs"] = conn.execute(
                "SELECT COUNT(*) c FROM extraction_jobs WHERE status='COMPLETED'"
            ).fetchone()["c"]
            d["failed_jobs"] = conn.execute(
                "SELECT COUNT(*) c FROM extraction_jobs WHERE status='FAILED'"
            ).fetchone()["c"]
            d["total_numbers"] = conn.execute(
                "SELECT COALESCE(SUM(unique_numbers),0) s FROM extraction_jobs"
            ).fetchone()["s"]
            d["numbers_today"] = conn.execute(
                "SELECT COALESCE(SUM(unique_numbers),0) s FROM extraction_jobs "
                "WHERE started_at >= datetime('now','-1 day')"
            ).fetchone()["s"]
            d["total_urls"] = conn.execute(
                "SELECT COALESCE(SUM(requested_visits),0) s FROM extraction_jobs"
            ).fetchone()["s"]
            d["avg_duration"] = conn.execute(
                "SELECT COALESCE(AVG(duration_ms),0) a FROM extraction_jobs "
                "WHERE status='COMPLETED'"
            ).fetchone()["a"]
            for k, v in [
                ("px_total", "SELECT COUNT(*) c FROM proxies"),
                ("px_working",
                 "SELECT COUNT(*) c FROM proxies WHERE health_status='WORKING'"),
                ("px_slow",
                 "SELECT COUNT(*) c FROM proxies WHERE health_status='SLOW'"),
                ("px_dead",
                 "SELECT COUNT(*) c FROM proxies WHERE health_status IN "
                 "('TCP_FAILED','AUTH_FAILED','INVALID')"),
                ("px_untested",
                 "SELECT COUNT(*) c FROM proxies WHERE health_status='UNTESTED'"),
            ]:
                d[k] = conn.execute(v).fetchone()["c"]
            return d
        finally:
            conn.close()


def recover_interrupted_jobs() -> int:
    """On startup, mark any RUNNING/QUEUED jobs as INTERRUPTED (crash recovery)."""
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                "UPDATE extraction_jobs SET status='INTERRUPTED', "
                "completed_at=CURRENT_TIMESTAMP "
                "WHERE status IN ('RUNNING','QUEUED')"
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()


def audit_log_rows(limit: int = 30, page: int = 0) -> Tuple[list, int]:
    with _db_lock:
        conn = get_conn()
        try:
            total = conn.execute("SELECT COUNT(*) c FROM admin_audit_log").fetchone()["c"]
            rows = conn.execute(
                "SELECT * FROM admin_audit_log ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, page * limit),
            ).fetchall()
            return [dict(r) for r in rows], total
        finally:
            conn.close()


def channel_post_rows(limit: int = 30, page: int = 0) -> Tuple[list, int]:
    with _db_lock:
        conn = get_conn()
        try:
            total = conn.execute("SELECT COUNT(*) c FROM channel_posts").fetchone()["c"]
            rows = conn.execute(
                """SELECT cp.*, ej.job_code, ej.source_url
                   FROM channel_posts cp
                   LEFT JOIN extraction_jobs ej ON cp.job_id = ej.job_id
                   ORDER BY cp.id DESC LIMIT ? OFFSET ?""",
                (limit, page * limit),
            ).fetchall()
            return [dict(r) for r in rows], total
        finally:
            conn.close()


def cleanup_old_jobs(days: int = 90) -> int:
    """Delete jobs and associated numbers/attempts older than `days`."""
    with _db_lock:
        conn = get_conn()
        try:
            cutoff = f"-{days} days"
            old_ids = [r["job_id"] for r in conn.execute(
                "SELECT job_id FROM extraction_jobs WHERE started_at < "
                "datetime('now', ?)", (cutoff,)
            ).fetchall()]
            if not old_ids:
                return 0
            placeholders = ",".join("?" * len(old_ids))
            conn.execute(
                f"DELETE FROM extraction_job_numbers WHERE job_id IN ({placeholders})",
                old_ids,
            )
            conn.execute(
                f"DELETE FROM extraction_attempts WHERE job_id IN ({placeholders})",
                old_ids,
            )
            conn.execute(
                f"DELETE FROM extraction_jobs WHERE job_id IN ({placeholders})",
                old_ids,
            )
            conn.commit()
            return len(old_ids)
        finally:
            conn.close()# =========================================================
#  Pure-Python AES-128-CBC  (ByetHost / InfinityFree challenge)
# =========================================================
_AES_SBOX = (
    0x63,0x7C,0x77,0x7B,0xF2,0x6B,0x6F,0xC5,0x30,0x01,0x67,0x2B,0xFE,0xD7,0xAB,0x76,
    0xCA,0x82,0xC9,0x7D,0xFA,0x59,0x47,0xF0,0xAD,0xD4,0xA2,0xAF,0x9C,0xA4,0x72,0xC0,
    0xB7,0xFD,0x93,0x26,0x36,0x3F,0xF7,0xCC,0x34,0xA5,0xE5,0xF1,0x71,0xD8,0x31,0x15,
    0x04,0xC7,0x23,0xC3,0x18,0x96,0x05,0x9A,0x07,0x12,0x80,0xE2,0xEB,0x27,0xB2,0x75,
    0x09,0x83,0x2C,0x1A,0x1B,0x6E,0x5A,0xA0,0x52,0x3B,0xD6,0xB3,0x29,0xE3,0x2F,0x84,
    0x53,0xD1,0x00,0xED,0x20,0xFC,0xB1,0x5B,0x6A,0xCB,0xBE,0x39,0x4A,0x4C,0x58,0xCF,
    0xD0,0xEF,0xAA,0xFB,0x43,0x4D,0x33,0x85,0x45,0xF9,0x02,0x7F,0x50,0x3C,0x9F,0xA8,
    0x51,0xA3,0x40,0x8F,0x92,0x9D,0x38,0xF5,0xBC,0xB6,0xDA,0x21,0x10,0xFF,0xF3,0xD2,
    0xCD,0x0C,0x13,0xEC,0x5F,0x97,0x44,0x17,0xC4,0xA7,0x7E,0x3D,0x64,0x5D,0x19,0x73,
    0x60,0x81,0x4F,0xDC,0x22,0x2A,0x90,0x88,0x46,0xEE,0xB8,0x14,0xDE,0x5E,0x0B,0xDB,
    0xE0,0x32,0x3A,0x0A,0x49,0x06,0x24,0x5C,0xC2,0xD3,0xAC,0x62,0x91,0x95,0xE4,0x79,
    0xE7,0xC8,0x37,0x6D,0x8D,0xD5,0x4E,0xA9,0x6C,0x56,0xF4,0xEA,0x65,0x7A,0xAE,0x08,
    0xBA,0x78,0x25,0x2E,0x1C,0xA6,0xB4,0xC6,0xE8,0xDD,0x74,0x1F,0x4B,0xBD,0x8B,0x8A,
    0x70,0x3E,0xB5,0x66,0x48,0x03,0xF6,0x0E,0x61,0x35,0x57,0xB9,0x86,0xC1,0x1D,0x9E,
    0xE1,0xF8,0x98,0x11,0x69,0xD9,0x8E,0x94,0x9B,0x1E,0x87,0xE9,0xCE,0x55,0x28,0xDF,
    0x8C,0xA1,0x89,0x0D,0xBF,0xE6,0x42,0x68,0x41,0x99,0x2D,0x0F,0xB0,0x54,0xBB,0x16,
)
_AES_INV_SBOX = [0] * 256
for _i, _v in enumerate(_AES_SBOX):
    _AES_INV_SBOX[_v] = _i
_AES_RCON = (0x00,0x01,0x02,0x04,0x08,0x10,0x20,0x40,0x80,0x1B,0x36)


def _sub_word(w: int) -> int:
    return ((_AES_SBOX[(w >> 24) & 0xFF] << 24) |
            (_AES_SBOX[(w >> 16) & 0xFF] << 16) |
            (_AES_SBOX[(w >> 8) & 0xFF] << 8) | _AES_SBOX[w & 0xFF])


def _rot_word(w: int) -> int:
    return ((w << 8) & 0xFFFFFFFF) | (w >> 24)


def _key_schedule(key_bytes: bytes) -> list:
    w = []
    for i in range(4):
        w.append((key_bytes[4*i] << 24) | (key_bytes[4*i+1] << 16) |
                 (key_bytes[4*i+2] << 8) | key_bytes[4*i+3])
    for i in range(4, 44):
        temp = w[i-1]
        if i % 4 == 0:
            temp = _sub_word(_rot_word(temp)) ^ (_AES_RCON[i // 4] << 24)
        w.append(w[i-4] ^ temp)
    return w


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


def _inv_mix_col(c: list) -> list:
    return [
        _gmul(c[0],0x0E)^_gmul(c[1],0x0B)^_gmul(c[2],0x0D)^_gmul(c[3],0x09),
        _gmul(c[0],0x09)^_gmul(c[1],0x0E)^_gmul(c[2],0x0B)^_gmul(c[3],0x0D),
        _gmul(c[0],0x0D)^_gmul(c[1],0x09)^_gmul(c[2],0x0E)^_gmul(c[3],0x0B),
        _gmul(c[0],0x0B)^_gmul(c[1],0x0D)^_gmul(c[2],0x09)^_gmul(c[3],0x0E),
    ]


def _decrypt_single_block(block: bytes, w: list) -> list:
    state = [[block[r + 4*c] for c in range(4)] for r in range(4)]
    for c in range(4):
        rk = w[40 + c]
        for r in range(4):
            state[r][c] ^= (rk >> (24 - 8*r)) & 0xFF
    for rnd in range(9, 0, -1):
        state[1] = state[1][3:] + state[1][:3]
        state[2] = state[2][2:] + state[2][:2]
        state[3] = state[3][1:] + state[3][:1]
        for r in range(4):
            for c in range(4):
                state[r][c] = _AES_INV_SBOX[state[r][c]]
        for c in range(4):
            rk = w[rnd*4 + c]
            for r in range(4):
                state[r][c] ^= (rk >> (24 - 8*r)) & 0xFF
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
            state[r][c] ^= (rk >> (24 - 8*r)) & 0xFF
    out = []
    for c in range(4):
        for r in range(4):
            out.append(state[r][c])
    return out


def decrypt_byet_challenge(c_hex: str, a_hex: str, b_hex: str) -> str:
    """AES-128-CBC decrypt for ByetHost/InfinityFree __test cookie."""
    try:
        from Crypto.Cipher import AES
        cipher = AES.new(bytes.fromhex(a_hex), AES.MODE_CBC, bytes.fromhex(b_hex))
        return cipher.decrypt(bytes.fromhex(c_hex)).hex()
    except Exception:
        pass
    if shutil.which("openssl"):
        try:
            p = subprocess.Popen(
                ["openssl", "enc", "-d", "-aes-128-cbc", "-K", a_hex,
                 "-iv", b_hex, "-nopad"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, _ = p.communicate(bytes.fromhex(c_hex))
            if p.returncode == 0 and len(out) == 16:
                return out.hex()
        except Exception:
            pass
    c = bytes.fromhex(c_hex)
    a = bytes.fromhex(a_hex)
    b = bytes.fromhex(b_hex)
    w = _key_schedule(a)
    dec = _decrypt_single_block(c, w)
    return bytes([dec[i] ^ b[i] for i in range(16)]).hex()


# =========================================================
#  SSRF protection  (block private / loopback / link-local)
# =========================================================
def _is_safe_host(host: str) -> bool:
    """Reject private, loopback, link-local, multicast, and reserved IPs."""
    if not host:
        return False
    try:
        # try to resolve and check every address
        infos = socket.getaddrinfo(host, None)
    except Exception:
        # If we can't resolve, allow it (target will fail anyway);
        # but block obvious local names.
        return not host.lower() in ("localhost", "ip6-localhost")
    for info in infos:
        ip = info[4][0]
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            continue
        if (addr.is_private or addr.is_loopback or addr.is_link_local
                or addr.is_multicast or addr.is_reserved or addr.is_unspecified):
            return False
    return True


def _url_allowed(url: str) -> Tuple[bool, str]:
    """Validate a URL is http(s) and not pointing at a private/loopback target."""
    if not url:
        return False, "empty"
    try:
        p = urllib.parse.urlparse(url)
    except Exception:
        return False, "unparseable"
    if p.scheme.lower() not in ("http", "https"):
        return False, f"scheme '{p.scheme}' not allowed"
    if not p.hostname or "." not in p.hostname:
        return False, "no valid hostname"
    if not _is_safe_host(p.hostname):
        return False, "private/loopback host blocked (SSRF protection)"
    return True, "ok"


# =========================================================
#  Proxy parsing & format normalization
# =========================================================
_PROXY_RE = re.compile(
    r"^(?P<scheme>https?|socks5h?|socks4a?)://"
    r"(?:(?P<user>[^:@/\s]+):(?P<pass>[^@/\s]+)@)?"
    r"(?P<host>[^:/\s]+):(?P<port>\d+)$",
    re.IGNORECASE,
)


def parse_proxy(raw: str) -> Optional[dict]:
    """Parse a proxy string into a structured dict. Returns None if invalid."""
    s = (raw or "").strip()
    if not s:
        return None
    m = _PROXY_RE.match(s)
    if m:
        scheme = m.group("scheme").lower()
        host = m.group("host")
        try:
            port = int(m.group("port"))
        except ValueError:
            return None
        if not (1 <= port <= 65535):
            return None
        return {
            "protocol": scheme,
            "host": host,
            "port": port,
            "username": urllib.parse.unquote(m.group("user") or ""),
            "password": urllib.parse.unquote(m.group("pass") or ""),
            "endpoint": s,
        }
    # bare host:port[:user:pass]
    parts = s.split(":")
    if len(parts) == 2:
        try:
            port = int(parts[1])
        except ValueError:
            return None
        if not (1 <= port <= 65535):
            return None
        return {"protocol": "http", "host": parts[0], "port": port,
                "username": "", "password": "",
                "endpoint": f"http://{parts[0]}:{parts[1]}"}
    if len(parts) == 4:
        try:
            port = int(parts[1])
        except ValueError:
            return None
        if not (1 <= port <= 65535):
            return None
        return {"protocol": "http", "host": parts[0], "port": port,
                "username": parts[2], "password": parts[3],
                "endpoint": f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"}
    return None


def proxy_to_requests(p: dict) -> dict:
    auth = ""
    if p.get("username"):
        auth = (f"{urllib.parse.quote(p['username'], safe='')}:"
                f"{urllib.parse.quote(p['password'], safe='')}@")
    return {"http": f"{p['protocol']}://{auth}{p['host']}:{p['port']}",
            "https": f"{p['protocol']}://{auth}{p['host']}:{p['port']}"}


def proxy_display(p: dict) -> str:
    """Safe display string — never exposes credentials."""
    return f"{p['protocol']}://{p['host']}:{p['port']}"


# =========================================================
#  Proxy health testing  (multi-stage, no false-dead)
# =========================================================
IP_CHECK_ENDPOINTS = [
    "https://api.ipify.org?format=text",
    "https://ifconfig.me/ip",
    "https://ipinfo.io/ip",
    "https://checkip.amazonaws.com",
]

_TEST_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")


def _tcp_check(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _verify_exit_ip(p: dict, timeout: float) -> Tuple[Optional[str], int]:
    """Return (exit_ip, latency_ms). One successful endpoint is enough."""
    proxies = proxy_to_requests(p)
    last_err = ""
    for ep in IP_CHECK_ENDPOINTS:
        try:
            t0 = time.time()
            r = requests.get(ep, proxies=proxies, timeout=timeout,
                             headers={"User-Agent": _TEST_UA})
            latency = int((time.time() - t0) * 1000)
            if r.status_code == 200:
                ip = r.text.strip()
                if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip) or ":" in ip:
                    return ip, latency
            last_err = f"HTTP {r.status_code}"
        except requests.exceptions.ProxyAuthenticationRequired:
            return None, -407
        except requests.exceptions.ConnectTimeout:
            last_err = "connect timeout"
        except requests.exceptions.ReadTimeout:
            last_err = "read timeout"
        except requests.exceptions.ConnectionError:
            last_err = "connection error"
        except Exception as e:
            last_err = str(e)[:60]
    return None, -1


def test_proxy(p: dict, timeout: Optional[int] = None) -> dict:
    """
    Multi-stage test. Returns dict: {status, latency_ms, exit_ip, error}
    status ∈ WORKING / SLOW / CONNECTED / TARGET_FAILED / AUTH_FAILED /
             TCP_FAILED / INVALID / TIMEOUT
    """
    t = timeout or PROXY_HEALTH_TIMEOUT
    if not p or not p.get("host"):
        return {"status": "INVALID", "latency_ms": 0, "exit_ip": "",
                "error": "bad parse"}
    if not _tcp_check(p["host"], p["port"], timeout=min(t, 5)):
        return {"status": "TCP_FAILED", "latency_ms": 0, "exit_ip": "",
                "error": "host:port unreachable"}
    ip, latency = _verify_exit_ip(p, t)
    if latency == -407:
        return {"status": "AUTH_FAILED", "latency_ms": 0, "exit_ip": "",
                "error": "HTTP 407 proxy auth required"}
    if ip:
        if latency > 3000:
            return {"status": "SLOW", "latency_ms": latency, "exit_ip": ip,
                    "error": ""}
        return {"status": "WORKING", "latency_ms": latency, "exit_ip": ip,
                "error": ""}
    return {"status": "CONNECTED", "latency_ms": 0, "exit_ip": "",
            "error": "tcp ok; ip endpoints unreachable through proxy"}


# ---------- Proxy DB ops ----------
def add_proxy_db(endpoint: str) -> Optional[int]:
    p = parse_proxy(endpoint)
    if not p:
        return None
    with _db_lock:
        conn = get_conn()
        try:
            existing = conn.execute(
                "SELECT id FROM proxies WHERE host=? AND port=?",
                (p["host"], p["port"]),
            ).fetchone()
            if existing:
                return existing["id"]
            cur = conn.execute(
                """INSERT INTO proxies(endpoint, protocol, host, port,
                   username, password) VALUES(?,?,?,?,?,?)""",
                (p["endpoint"], p["protocol"], p["host"], p["port"],
                 p["username"], p["password"]),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()


def get_proxy_row(proxy_id: int) -> Optional[dict]:
    with _db_lock:
        conn = get_conn()
        try:
            r = conn.execute("SELECT * FROM proxies WHERE id=?", (proxy_id,)).fetchone()
            return dict(r) if r else None
        finally:
            conn.close()


def list_proxies(limit: int = 100, status_filter: Optional[str] = None,
                 page: int = 0) -> Tuple[list, int]:
    with _db_lock:
        conn = get_conn()
        try:
            if status_filter:
                total = conn.execute(
                    "SELECT COUNT(*) c FROM proxies WHERE health_status=?",
                    (status_filter,),
                ).fetchone()["c"]
                rows = conn.execute(
                    """SELECT * FROM proxies WHERE health_status=?
                       ORDER BY id DESC LIMIT ? OFFSET ?""",
                    (status_filter, limit, page * limit),
                ).fetchall()
            else:
                total = conn.execute("SELECT COUNT(*) c FROM proxies").fetchone()["c"]
                rows = conn.execute(
                    "SELECT * FROM proxies ORDER BY id DESC LIMIT ? OFFSET ?",
                    (limit, page * limit),
                ).fetchall()
            return [dict(r) for r in rows], total
        finally:
            conn.close()


def update_proxy_health(proxy_id: int, result: dict) -> None:
    status = result["status"]
    latency = result.get("latency_ms", 0) or 0
    ip = result.get("exit_ip", "")
    err = result.get("error", "")
    now = _now_iso()
    with _db_lock:
        conn = get_conn()
        try:
            row = conn.execute("SELECT * FROM proxies WHERE id=?", (proxy_id,)).fetchone()
            if not row:
                return
            ok = status in ("WORKING", "SLOW", "CONNECTED")
            succ = row["success_count"] + (1 if ok else 0)
            fail = row["failure_count"] + (0 if ok else 1)
            consec_f = 0 if ok else row["consecutive_failures"] + 1
            avg = row["average_latency"]
            if latency > 0:
                avg = int(((avg * row["success_count"]) + latency) / max(succ, 1))
            score = _compute_score(status, succ, fail, latency)
            cooldown = None
            if not ok and consec_f > 0:
                cd_secs = min(30 * (2 ** (consec_f - 1)), 600)
                cooldown = (datetime.utcnow()
                            + timedelta(seconds=cd_secs)).isoformat(sep=" ", timespec="seconds")
            conn.execute(
                """UPDATE proxies SET health_status=?, health_score=?,
                   success_count=?, failure_count=?, consecutive_failures=?,
                   average_latency=?, last_observed_ip=COALESCE(NULLIF(?, ''),
                   last_observed_ip), last_error=?, last_tested=?, last_success=?,
                   last_failure=?, cooldown_until=?, updated_at=? WHERE id=?""",
                (status, score, succ, fail, consec_f, avg, ip, err, now,
                 now if ok else row["last_success"],
                 now if not ok else row["last_failure"],
                 cooldown, now, proxy_id),
            )
            conn.commit()
        finally:
            conn.close()


def _compute_score(status: str, succ: int, fail: int, latency: int) -> int:
    base = {"WORKING": 90, "SLOW": 55, "CONNECTED": 60, "TARGET_FAILED": 50,
            "AUTH_FAILED": 5, "TCP_FAILED": 5, "INVALID": 0, "TIMEOUT": 30,
            "UNTESTED": 0}.get(status, 0)
    if latency > 0:
        if latency < 800:
            base = min(base + 10, 100)
        elif latency > 2500:
            base = max(base - 15, 1)
    return base


def delete_proxy_db(proxy_id: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute("DELETE FROM proxies WHERE id=?", (proxy_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


# =========================================================
#  ProxyPool  — thread-safe rotation with cached exit-IP
# =========================================================
class ProxyPool:
    def __init__(self):
        self._lock = threading.RLock()
        self._last_used: Dict[int, float] = {}
        self._ip_cache: Dict[int, Tuple[str, float]] = {}   # proxy_id -> (ip, ts)

    def healthy_proxies(self) -> list:
        with _db_lock:
            conn = get_conn()
            try:
                rows = conn.execute(
                    """SELECT * FROM proxies WHERE is_active=1
                       AND health_status IN ('WORKING','SLOW','CONNECTED')
                       AND (cooldown_until IS NULL
                            OR cooldown_until <= ?)
                       ORDER BY health_score DESC, average_latency ASC""",
                    (_now_iso(),),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def select(self) -> Optional[dict]:
        """Pick the best healthy proxy, least-recently-used first."""
        with self._lock:
            avail = self.healthy_proxies()
            if not avail:
                return None
            avail.sort(key=lambda r: (self._last_used.get(r["id"], 0),
                                       -r["health_score"]))
            chosen = avail[0]
            self._last_used[chosen["id"]] = time.time()
            return chosen

    def cached_exit_ip(self, proxy_id: int) -> Optional[str]:
        with self._lock:
            entry = self._ip_cache.get(proxy_id)
            if entry and (time.time() - entry[1]) < PROXY_CACHE_TTL:
                return entry[0]
        return None

    def set_cached_ip(self, proxy_id: int, ip: str) -> None:
        with self._lock:
            self._ip_cache[proxy_id] = (ip, time.time())

    def mark_used_success(self, proxy_id: int, latency: int, exit_ip: str) -> None:
        update_proxy_health(proxy_id,
                            {"status": "WORKING", "latency_ms": latency,
                             "exit_ip": exit_ip, "error": ""})
        if exit_ip:
            self.set_cached_ip(proxy_id, exit_ip)

    def mark_used_failure(self, proxy_id: int, status: str, error: str) -> None:
        update_proxy_health(proxy_id,
                            {"status": status, "latency_ms": 0,
                             "exit_ip": "", "error": error})

    def mark_target_failure(self, proxy_id: int, error: str) -> None:
        """Target failed but proxy is healthy — do NOT punish the proxy."""
        with _db_lock:
            conn = get_conn()
            try:
                row = conn.execute("SELECT * FROM proxies WHERE id=?",
                                   (proxy_id,)).fetchone()
                if not row:
                    return
                # bump failure_count lightly, but keep consecutive_failures reset
                # and DO NOT change health_status — proxy itself works
                conn.execute(
                    """UPDATE proxies SET failure_count=failure_count+1,
                       last_error=?, last_failure=?, updated_at=?
                       WHERE id=?""",
                    (error[:200], _now_iso(), _now_iso(), proxy_id),
                )
                conn.commit()
            finally:
                conn.close()

    def count(self) -> dict:
        with _db_lock:
            conn = get_conn()
            try:
                d = {}
                for k, q in [
                    ("total", "SELECT COUNT(*) c FROM proxies"),
                    ("working",
                     "SELECT COUNT(*) c FROM proxies WHERE health_status='WORKING'"),
                    ("slow",
                     "SELECT COUNT(*) c FROM proxies WHERE health_status='SLOW'"),
                    ("connected",
                     "SELECT COUNT(*) c FROM proxies WHERE health_status='CONNECTED'"),
                    ("dead",
                     "SELECT COUNT(*) c FROM proxies WHERE health_status IN "
                     "('TCP_FAILED','AUTH_FAILED','INVALID')"),
                    ("untested",
                     "SELECT COUNT(*) c FROM proxies WHERE health_status='UNTESTED'"),
                ]:
                    d[k] = conn.execute(q).fetchone()["c"]
                return d
            finally:
                conn.close()


proxy_pool = ProxyPool()


def bulk_test_proxies(progress_cb=None, cancel_event: Optional[threading.Event] = None,
                      scope: str = "all") -> dict:
    """Test proxies concurrently with bounded concurrency."""
    rows, _ = list_proxies(limit=2000)
    if scope == "unhealthy":
        pending = [r for r in rows if r["health_status"] in
                   ("UNTESTED", "TCP_FAILED", "TIMEOUT", "CONNECTED",
                    "AUTH_FAILED", "INVALID")]
    else:
        pending = rows
    if not pending:
        return {"tested": 0, "working": 0, "slow": 0, "dead": 0, "total": 0}

    tested = working = slow = dead = 0
    total = len(pending)

    def _one(r):
        p = {"protocol": r["protocol"], "host": r["host"], "port": r["port"],
             "username": r["username"], "password": r["password"],
             "endpoint": r["endpoint"]}
        res = test_proxy(p)
        update_proxy_health(r["id"], res)
        return res["status"]

    with ThreadPoolExecutor(max_workers=PROXY_TEST_CONCURRENCY) as ex:
        futs = {ex.submit(_one, r): r for r in pending}
        for fut in as_completed(futs):
            if cancel_event and cancel_event.is_set():
                for f in futs:
                    f.cancel()
                break
            try:
                st = fut.result()
            except Exception:
                st = "INVALID"
            tested += 1
            if st in ("WORKING", "CONNECTED"):
                working += 1
            elif st == "SLOW":
                slow += 1
            else:
                dead += 1
            if progress_cb:
                progress_cb(tested, total, working, slow, dead)
    return {"tested": tested, "working": working, "slow": slow,
            "dead": dead, "total": total}


# =========================================================
#  Live proxy fetch  (pluggable source adapters)
# =========================================================
class ProxySource:
    """Abstract interface for a proxy source. Override fetch() in subclasses."""
    name = "base"

    def fetch(self) -> List[str]:
        """Return a list of raw proxy strings (one per line or per item)."""
        raise NotImplementedError

    def parse(self, text: str) -> List[str]:
        """Parse fetched text into candidate proxy strings."""
        out = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # accept IP:PORT, IP:PORT:USER:PASS, or scheme://...
            if re.match(r"^\d{1,3}(?:\.\d{1,3}){3}:\d+$", line):
                out.append(line)
            elif "://" in line:
                out.append(line)
            elif re.match(r"^\d{1,3}(?:\.\d{1,3}){3}:\d+:[^:]+:[^:]+$", line):
                out.append(line)
        return out

    def normalize(self, candidates: List[str]) -> List[dict]:
        out = []
        for c in candidates:
            p = parse_proxy(c)
            if p:
                out.append(p)
        return out

    def deduplicate(self, proxies: List[dict]) -> List[dict]:
        seen = set()
        out = []
        for p in proxies:
            key = (p["host"], p["port"])
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
        return out


class PlainTextSource(ProxySource):
    """Generic plaintext IP:PORT source."""
    name = "plaintext"

    def __init__(self, url: str):
        self.url = url

    def fetch(self) -> List[str]:
        try:
            r = requests.get(self.url, timeout=10,
                             headers={"User-Agent": _TEST_UA})
            if r.status_code == 200:
                return self.parse(r.text)
        except Exception as e:
            log.warning("PROXY_SOURCE_FETCH_FAIL %s err=%s",
                        _mask(self.url), e)
        return []


def get_proxy_sources() -> List[ProxySource]:
    """Load enabled proxy sources from DB. Operators add their own authorized
    sources via the proxy center. We do NOT hardcode any third-party lists."""
    out: List[ProxySource] = []
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM proxy_sources WHERE is_enabled=1"
            ).fetchall()
        finally:
            conn.close()
    for r in rows:
        out.append(PlainTextSource(r["url"]))
    return out


def fetch_latest_proxies(progress_cb=None,
                         cancel_event: Optional[threading.Event] = None) -> dict:
    """Fetch, dedupe, test, and import proxies from configured sources."""
    sources = get_proxy_sources()
    discovered = []
    src_count = len(sources)
    for src in sources:
        if cancel_event and cancel_event.is_set():
            break
        discovered.extend(src.fetch())
    parsed = []
    for raw in discovered:
        p = parse_proxy(raw)
        if p:
            parsed.append(p)
    deduped = []
    seen = set()
    for p in parsed:
        key = (p["host"], p["port"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)

    # import into DB (dedupe by host:port at the DB layer too)
    new_ids = []
    for p in deduped:
        pid = add_proxy_db(p["endpoint"])
        if pid:
            new_ids.append((pid, p))

    # test concurrently
    tested = working = slow = dead = 0
    total = len(new_ids)

    def _one(item):
        pid, p = item
        res = test_proxy(p)
        update_proxy_health(pid, res)
        return res["status"]

    with ThreadPoolExecutor(max_workers=PROXY_TEST_CONCURRENCY) as ex:
        futs = {ex.submit(_one, item): item for item in new_ids}
        for fut in as_completed(futs):
            if cancel_event and cancel_event.is_set():
                for f in futs:
                    f.cancel()
                break
            try:
                st = fut.result()
            except Exception:
                st = "INVALID"
            tested += 1
            if st in ("WORKING", "CONNECTED"):
                working += 1
            elif st == "SLOW":
                slow += 1
            else:
                dead += 1
            if progress_cb:
                progress_cb(src_count, len(discovered), len(parsed),
                            len(deduped), tested, total,
                            working, slow, dead)
    return {
        "sources": src_count, "discovered": len(discovered),
        "parsed": len(parsed), "imported": len(deduped),
        "tested": tested, "working": working, "slow": slow, "dead": dead,
    }


# =========================================================
#  Number extraction pipeline
# =========================================================
_WA_PATTERNS = [
    (re.compile(r'wa\.me/(\+?\d{6,15})', re.IGNORECASE), "wa.me"),
    (re.compile(r'wa\.me/message/[A-Za-z0-9]+.*?(\d{10,15})', re.IGNORECASE), "wa.me_message"),
    (re.compile(r'phone=(\+?\d{6,15})', re.IGNORECASE), "query_parameter"),
    (re.compile(r'number=(\+?\d{6,15})', re.IGNORECASE), "query_parameter"),
    (re.compile(r'mobile=(\+?\d{6,15})', re.IGNORECASE), "query_parameter"),
    (re.compile(r'whatsapp://send\?phone=(\+?\d{6,15})', re.IGNORECASE), "whatsapp_url"),
    (re.compile(r'api\.whatsapp\.com/send[/?][^"\'\s]*phone=(\+?\d{6,15})', re.IGNORECASE), "whatsapp_api"),
    (re.compile(r'web\.whatsapp\.com/send[/?][^"\'\s]*phone=(\+?\d{6,15})', re.IGNORECASE), "whatsapp_web"),
    (re.compile(r'whatsapp\.com/send[/?][^"\'\s]*phone=(\+?\d{6,15})', re.IGNORECASE), "whatsapp_url"),
    (re.compile(r'tel:(\+?\d{6,15})', re.IGNORECASE), "tel_link"),
    (re.compile(r'intent://send/(\+?\d{6,15})', re.IGNORECASE), "intent"),
]

_JSON_FIELD_RE = re.compile(
    r'"(?:phone|phone_number|mobile|mobile_number|whatsapp|wa_number|recipient|send_to|number|to)"\s*:\s*"(\+?\d{6,15})"',
    re.IGNORECASE,
)

_ATTR_RE = re.compile(
    r'(?:href|data-phone|data-mobile|data-whatsapp|data-number|data-tel)\s*=\s*["\']([^"\']*\+?\d{6,15}[^"\']*)["\']',
    re.IGNORECASE,
)

_META_REFRESH_RE = re.compile(
    r'<meta[^>]*http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\'>]+)',
    re.IGNORECASE,
)
_JS_LOCATION_RE = re.compile(
    r'(?:window\.)?location(?:\.href|\.replace|\.assign)\s*\(\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)


def _normalize(raw: str) -> Optional[str]:
    digits = re.sub(r'\D', '', raw or "")
    if not digits:
        return None
    if digits.startswith("00"):
        digits = digits[2:]
    if 10 <= len(digits) <= 15:
        return digits
    return None


def extract_numbers(text: str, source: str, default_method: str = "html") -> list:
    """Return list of (normalized, method) tuples found in text."""
    if not text:
        return []
    out = []
    seen = set()
    decoded = urllib.parse.unquote(text)
    samples = (text, decoded) if decoded != text else (text,)
    for sample in samples:
        for pat, method in _WA_PATTERNS:
            for m in pat.findall(sample):
                n = _normalize(m)
                if n and n not in seen:
                    seen.add(n)
                    out.append((n, method))
        for m in _JSON_FIELD_RE.findall(sample):
            n = _normalize(m)
            if n and n not in seen:
                seen.add(n)
                out.append((n, "json_field"))
        for m in _ATTR_RE.findall(sample):
            n = _normalize(m)
            if n and n not in seen:
                seen.add(n)
                out.append((n, "html_attribute"))
    return out


def extract_from_url_chain(urls: list) -> list:
    out = []
    seen = set()
    for u in urls:
        for n, method in extract_numbers(u, u, default_method="redirect_url"):
            if n not in seen:
                seen.add(n)
                out.append((n, method))
    return out


# =========================================================
#  Scraper  — direct or via proxy, with challenge solving
# =========================================================
class Scraper:
    def __init__(self, use_proxy: bool = False, proxy: Optional[dict] = None):
        self.use_proxy = use_proxy
        self.proxy = proxy
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": _TEST_UA,
            "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                       "*/*;q=0.8"),
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._test_cookie = None
        self._domain = None

    def _proxies(self):
        if self.use_proxy and self.proxy:
            return proxy_to_requests(self.proxy)
        return None

    def fetch(self, url: str) -> Tuple[str, str, list]:
        """Return (final_url, body, visited_urls) or raise."""
        ok, reason = _url_allowed(url)
        if not ok:
            raise ValueError(f"URL rejected: {reason}")

        parsed = urllib.parse.urlparse(url)
        self._domain = parsed.hostname
        if self._test_cookie and self._domain:
            self.session.cookies.set("__test", self._test_cookie, domain=self._domain)

        visited = [url]
        current = url
        body = ""

        for hop in range(MAX_REDIRECTS + 1):
            try:
                r = self.session.get(current, proxies=self._proxies(),
                                     timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                                     allow_redirects=True, stream=True)
                size = 0
                chunks = []
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        size += len(chunk)
                        if size > MAX_RESPONSE_SIZE:
                            break
                        chunks.append(chunk)
                body = b"".join(chunks).decode("utf-8", errors="ignore")
                current = r.url
                if current not in visited:
                    visited.append(current)
            except requests.exceptions.RequestException:
                raise

            # ByetHost / InfinityFree slowAES challenge
            if "slowAES" in body or ("toNumbers(" in body and "__test=" in body):
                matches = re.findall(r'toNumbers\("([a-f0-9]+)"\)', body)
                if len(matches) >= 3:
                    a_key, b_iv, c_cipher = matches[0], matches[1], matches[2]
                    self._test_cookie = decrypt_byet_challenge(c_cipher, a_key, b_iv)
                    if self._domain:
                        self.session.cookies.set("__test", self._test_cookie,
                                                domain=self._domain)
                    loc = re.search(r'location\.href\s*=\s*["\'](.*?)["\']', body)
                    nxt = loc.group(1) if loc else (
                        current + ("&i=1" if "?" in current else "?i=1"))
                    current = urllib.parse.urljoin(current, nxt)
                    visited.append(current)
                    continue

            meta = _META_REFRESH_RE.search(body)
            if meta:
                current = urllib.parse.urljoin(current, meta.group(1).strip())
                visited.append(current)
                continue

            js = _JS_LOCATION_RE.search(body)
            if js:
                current = urllib.parse.urljoin(current, js.group(1).strip())
                visited.append(current)
                continue

            break

        return current, body, visited# =========================================================
#  Session state machine  (centralized, with expiry)
# =========================================================
# Each user has at most one active session dict. Sessions auto-expire after
# SESSION_EXPIRY_MIN minutes of inactivity.

STATE_IDLE              = "IDLE"
STATE_AWAIT_URL         = "AWAIT_URL"
STATE_AWAIT_MODE        = "AWAIT_MODE"
STATE_AWAIT_VISITS      = "AWAIT_VISITS"
STATE_AWAIT_BULK        = "AWAIT_BULK"
STATE_AWAIT_PROXY_ADD   = "AWAIT_PROXY_ADD"
STATE_AWAIT_PROXY_BULK  = "AWAIT_PROXY_BULK"
STATE_AWAIT_BROADCAST   = "AWAIT_BROADCAST"
STATE_AWAIT_SEARCH      = "AWAIT_SEARCH"
STATE_AWAIT_PROXY_SRC   = "AWAIT_PROXY_SRC"
STATE_AWAIT_CHANNEL     = "AWAIT_CHANNEL"


def _new_session(user_id: int, state: str, **data) -> dict:
    return {"state": state, "created": time.time(), "updated": time.time(), **data}


def _get_session(user_id: int) -> Optional[dict]:
    with _state_lock:
        s = user_states.get(user_id)
        if not s:
            return None
        # expiry
        if time.time() - s.get("updated", s.get("created", 0)) > SESSION_EXPIRY_MIN * 60:
            user_states.pop(user_id, None)
            return None
        return s


def _set_session(user_id: int, state: str, **data) -> dict:
    with _state_lock:
        s = _new_session(user_id, state, **data)
        user_states[user_id] = s
        return s


def _clear_session(user_id: int) -> None:
    with _state_lock:
        user_states.pop(user_id, None)


def _touch_session(user_id: int) -> None:
    with _state_lock:
        s = user_states.get(user_id)
        if s:
            s["updated"] = time.time()


# =========================================================
#  Navigation keyboards  (reusable, consistent)
# =========================================================
def _back_cancel_kb() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("◀️ Back", callback_data="nav_back"),
        types.InlineKeyboardButton("🛑 Cancel", callback_data="nav_cancel"),
    )
    return mk


def _cancel_only_kb() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton("🛑 Cancel", callback_data="nav_cancel"))
    return mk


def _home_kb(user_id: int) -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("🚀 Start Extraction",
                                   callback_data="ex_start"),
        types.InlineKeyboardButton("📊 My Statistics",
                                   callback_data="user_stats"),
        types.InlineKeyboardButton("🕘 History",
                                   callback_data="user_history"),
        types.InlineKeyboardButton("⚙️ Settings",
                                   callback_data="user_settings"),
        types.InlineKeyboardButton("❓ Help", callback_data="user_help"),
    )
    if is_admin(user_id):
        mk.add(types.InlineKeyboardButton("🛠 Admin Panel",
                                           callback_data="adm_panel"))
    return mk


def main_keyboard(user_id: int) -> types.ReplyKeyboardMarkup:
    mk = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    mk.add(
        types.KeyboardButton("🚀 Start Extraction"),
        types.KeyboardButton("📊 My Statistics"),
        types.KeyboardButton("🕘 History"),
        types.KeyboardButton("⚙️ Settings"),
        types.KeyboardButton("❓ Help"),
    )
    if is_admin(user_id):
        mk.add(types.KeyboardButton("🛠 Admin Panel"))
    return mk


def _mode_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=1)
    mk.add(
        types.InlineKeyboardButton("🔗 Single Link",
                                   callback_data="ex_single"),
        types.InlineKeyboardButton("📚 Bulk Links",
                                   callback_data="ex_bulk"),
        types.InlineKeyboardButton("◀️ Back", callback_data="nav_back"),
        types.InlineKeyboardButton("🛑 Cancel", callback_data="nav_cancel"),
    )
    return mk


def _netmode_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=1)
    mk.add(
        types.InlineKeyboardButton("🟢 Direct (No IP)",
                                   callback_data="netmode_direct"),
        types.InlineKeyboardButton("🌐 IP Rotation",
                                   callback_data="netmode_proxy"),
        types.InlineKeyboardButton("◀️ Back", callback_data="nav_back"),
        types.InlineKeyboardButton("🛑 Cancel", callback_data="nav_cancel"),
    )
    return mk


def _visits_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=3)
    mk.add(
        types.InlineKeyboardButton("🧪 1x", callback_data="visits_1"),
        types.InlineKeyboardButton("⚡ 5x", callback_data="visits_5"),
        types.InlineKeyboardButton("🚀 10x", callback_data="visits_10"),
        types.InlineKeyboardButton("⚡ 20x", callback_data="visits_20"),
        types.InlineKeyboardButton("💎 50x", callback_data="visits_50"),
        types.InlineKeyboardButton("💎 100x", callback_data="visits_100"),
    )
    mk.add(
        types.InlineKeyboardButton("◀️ Back", callback_data="nav_back"),
        types.InlineKeyboardButton("🛑 Cancel", callback_data="nav_cancel"),
    )
    return mk


def _confirm_start_kb() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=1)
    mk.add(
        types.InlineKeyboardButton("🚀 Start", callback_data="confirm_start"),
        types.InlineKeyboardButton("◀️ Back", callback_data="nav_back"),
        types.InlineKeyboardButton("🛑 Cancel", callback_data="nav_cancel"),
    )
    return mk


def _admin_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("📊 Analytics", callback_data="adm_dashboard"),
        types.InlineKeyboardButton("👥 Users", callback_data="adm_users"),
        types.InlineKeyboardButton("🚀 Jobs", callback_data="adm_jobs"),
        types.InlineKeyboardButton("🌐 Proxy Center", callback_data="adm_proxies"),
        types.InlineKeyboardButton("📡 Channel", callback_data="adm_channel"),
        types.InlineKeyboardButton("📢 Broadcast", callback_data="adm_broadcast"),
        types.InlineKeyboardButton("⚙️ System", callback_data="adm_settings"),
        types.InlineKeyboardButton("🧪 Diagnostics", callback_data="adm_diag"),
        types.InlineKeyboardButton("📜 Audit Log", callback_data="adm_audit"),
        types.InlineKeyboardButton("📡 Post History", callback_data="adm_chanlog"),
        types.InlineKeyboardButton("⏳ Pending", callback_data="adm_pending"),
        types.InlineKeyboardButton("🛠 Maintenance",
                                   callback_data="adm_maint"),
        types.InlineKeyboardButton("🏠 Home", callback_data="nav_home"),
    )
    return mk


def _proxy_center_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("⚡ Fetch Latest",
                                   callback_data="px_fetch"),
        types.InlineKeyboardButton("🧪 Test All",
                                   callback_data="px_test_all"),
        types.InlineKeyboardButton("🔄 Retest Failed",
                                   callback_data="px_retest"),
        types.InlineKeyboardButton("🟢 Working",
                                   callback_data="px_list_working"),
        types.InlineKeyboardButton("🔴 Dead",
                                   callback_data="px_list_dead"),
        types.InlineKeyboardButton("📋 All", callback_data="px_list_all"),
        types.InlineKeyboardButton("➕ Add Proxy", callback_data="px_add"),
        types.InlineKeyboardButton("📦 Bulk Add", callback_data="px_bulk"),
        types.InlineKeyboardButton("📦 Manage Sources",
                                   callback_data="px_sources"),
        types.InlineKeyboardButton("🧹 Cleanup Dead",
                                   callback_data="px_cleanup"),
        types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"),
    )
    return mk


# =========================================================
#  URL validation
# =========================================================
_URL_RE = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)


def validate_url(text: str) -> Optional[str]:
    t = (text or "").strip()
    if not t:
        return None
    if not t.startswith(("http://", "https://")):
        t = "https://" + t
    if not _URL_RE.match(t):
        return None
    try:
        p = urllib.parse.urlparse(t)
        if not p.hostname or "." not in p.hostname:
            return None
        return t
    except Exception:
        return None


# =========================================================
#  Home screen
# =========================================================
def _home_text(user_id: int) -> str:
    u = get_user(user_id)
    jobs_count = u["total_extractions"] if u else 0
    nums_count = u["total_numbers_found"] if u else 0
    if jobs_count:
        succ = sum(1 for j in user_jobs(user_id, limit=500)[0]
                   if j["status"] == "COMPLETED")
        rate = (succ / jobs_count * 100) if jobs_count else 0
    else:
        rate = 0
    name = get_setting("admin_display_name", "DK Scraping Bot")
    return (
        "╭──────────────────────────────╮\n"
        "│       🚀 DK SCRAPING BOT       │\n"
        "│    Professional URL Engine     │\n"
        "╰──────────────────────────────╯\n\n"
        f"Welcome, *{md_esc(name)}*\n\n"
        "⚡ Fast URL Processing\n"
        "🌐 Direct + Proxy Modes\n"
        "📊 Advanced Extraction\n"
        "📁 Professional Results\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📊 YOUR ACTIVITY\n\n"
        f"Jobs: `{jobs_count}`\n"
        f"Numbers Found: `{nums_count:,}`\n"
        f"Success Rate: `{rate:.1f}%`\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )


def _send_home(chat_id: int, user_id: int):
    _safe_send(chat_id, _home_text(user_id), markup=_home_kb(user_id))


# =========================================================
#  Command handlers
# =========================================================
@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message):
    u = message.from_user
    chat_id = message.chat.id
    status = register_user(u.id, u.username, u.first_name)
    if status == "BLOCKED":
        _safe_send(chat_id, "🚫 *Your access has been blocked.*")
        return
    if status == "PENDING":
        admin_name = get_setting("admin_display_name", "Admin")
        _safe_send(
            chat_id,
            ("🔒 *ACCESS PENDING*\n\n"
             "Your access request has been submitted.\n"
             "Please wait for administrator approval.\n\n"
             f"_— {md_esc(admin_name)}_"),
        )
        for aid in ADMIN_IDS:
            try:
                mk = types.InlineKeyboardMarkup(row_width=2)
                mk.add(
                    types.InlineKeyboardButton("✅ Approve",
                                               callback_data=f"appr_{u.id}"),
                    types.InlineKeyboardButton("❌ Reject",
                                               callback_data=f"rej_{u.id}"),
                )
                bot.send_message(
                    aid,
                    ("👤 *NEW USER REQUEST*\n\n"
                     f"Name: {md_esc(u.first_name or '—')}\n"
                     f"Username: @{md_esc(u.username or '—')}\n"
                     f"User ID: `{u.id}`"),
                    parse_mode="Markdown", reply_markup=mk,
                )
            except Exception:
                pass
        return
    _clear_session(u.id)
    _send_home(chat_id, u.id)


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        _safe_send(message.chat.id, "❌ *Access Denied.*")
        return
    _show_admin_panel(message.chat.id)


def _show_admin_panel(chat_id):
    _safe_send(
        chat_id,
        ("🛠 *DK ADMIN PANEL*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "Select an action:"),
        markup=_admin_keyboard(),
    )


# =========================================================
#  Main message router
# =========================================================
@bot.message_handler(func=lambda m: True)
def handle_messages(message: types.Message):
    global MAINTENANCE_MODE
    u = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()

    register_user(u.id, u.username, u.first_name)
    session = _get_session(u.id)

    # maintenance
    if MAINTENANCE_MODE and not is_admin(u.id):
        _safe_send(
            chat_id,
            ("🔧 *MAINTENANCE*\n\n"
             "The system is temporarily unavailable.\n"
             "Please try again later."),
        )
        return

    # blocked / pending
    user = get_user(u.id)
    if user:
        if user["blocked"]:
            _safe_send(chat_id, "🚫 *Access blocked.*")
            return
        if user["status"] == "PENDING":
            _safe_send(chat_id, "🔒 *Access pending approval.*")
            return

    # ---- session-driven input ----
    if session:
        state = session.get("state")
        _touch_session(u.id)

        if state == STATE_AWAIT_URL:
            _handle_url_input(chat_id, u, text, session)
            return
        if state == STATE_AWAIT_BULK:
            _handle_bulk_input(chat_id, u, text, session)
            return
        if state == STATE_AWAIT_PROXY_ADD and is_admin(u.id):
            _clear_session(u.id)
            _handle_proxy_add(chat_id, u.id, text)
            return
        if state == STATE_AWAIT_PROXY_BULK and is_admin(u.id):
            _clear_session(u.id)
            _handle_proxy_bulk(chat_id, u.id, text)
            return
        if state == STATE_AWAIT_BROADCAST and is_admin(u.id):
            _clear_session(u.id)
            threading.Thread(target=_do_broadcast,
                              args=(chat_id, u.id, text),
                              daemon=True).start()
            return
        if state == STATE_AWAIT_SEARCH and is_admin(u.id):
            _clear_session(u.id)
            _handle_user_search(chat_id, u.id, text)
            return
        if state == STATE_AWAIT_PROXY_SRC and is_admin(u.id):
            _clear_session(u.id)
            _handle_proxy_source_add(chat_id, u.id, text)
            return
        if state == STATE_AWAIT_CHANNEL and is_admin(u.id):
            _clear_session(u.id)
            ch = text.strip()
            set_setting("channel_username", ch)
            audit_log(u.id, "CHANNEL_SET", ch)
            _safe_send(chat_id, f"📡 Channel set to `{md_esc(ch)}`",
                       markup=_admin_keyboard())
            return

    # ---- reply-keyboard buttons ----
    if text == "🚀 Start Extraction":
        _start_extraction_flow(chat_id, u)
        return
    if text == "📊 My Statistics":
        _show_user_stats(chat_id, u.id)
        return
    if text == "🕘 History":
        _show_user_history(chat_id, u.id, page=0)
        return
    if text == "⚙️ Settings":
        _show_user_settings(chat_id, u.id)
        return
    if text == "❓ Help":
        _show_help(chat_id)
        return
    if text == "🛠 Admin Panel" and is_admin(u.id):
        _show_admin_panel(chat_id)
        return
    if text in ("❌ Cancel", "🔙 Main Menu", "🏠 Home"):
        _clear_session(u.id)
        _send_home(chat_id, u.id)
        return

    # fallback
    _safe_send(chat_id, "Use the menu below 👇",
               markup=main_keyboard(u.id))


# ---- session input handlers ----
def _handle_url_input(chat_id, user, text, session):
    url = validate_url(text)
    if not url:
        _safe_send(
            chat_id,
            ("❌ *Invalid URL.*\nPlease send a valid `http://` or `https://` "
             "link."),
            markup=_back_cancel_kb(),
        )
        return
    _set_session(user.id, STATE_AWAIT_MODE, url=url)
    _safe_send(
        chat_id,
        (f"🔍 *URL RECEIVED*\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"`{_short(url, 80)}`\n\n"
         f"Choose extraction mode:"),
        markup=_netmode_keyboard(),
    )


def _handle_bulk_input(chat_id, user, text, session):
    urls = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        u = validate_url(line)
        if u:
            urls.append(u)
    if not urls:
        _safe_send(
            chat_id,
            ("❌ *No valid URLs found.*\nSend one URL per line."),
            markup=_back_cancel_kb(),
        )
        return
    if len(urls) > MAX_URLS_PER_BATCH:
        urls = urls[:MAX_URLS_PER_BATCH]
        _safe_send(
            chat_id,
            (f"⚠️ Trimmed to first `{MAX_URLS_PER_BATCH}` URLs "
             f"(per-batch limit)."),
        )
    _clear_session(user.id)
    _start_bulk_job(chat_id, user, urls, session.get("mode", "DIRECT"))


# =========================================================
#  Extraction flow
# =========================================================
def _start_extraction_flow(chat_id, user):
    with _state_lock:
        if len(active_jobs.get(user.id, set())) >= MAX_CONCURRENT_JOBS:
            _safe_send(
                chat_id,
                (f"⚠️ You already have `{MAX_CONCURRENT_JOBS}` job(s) running.\n"
                 "Wait for one to finish before starting another."),
                markup=main_keyboard(user.id),
            )
            return
    _set_session(user.id, STATE_AWAIT_MODE)
    _safe_send(
        chat_id,
        ("🚀 *START EXTRACTION*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "Choose extraction type:"),
        markup=_mode_keyboard(),
    )


def _show_confirm(chat_id, user_id, url, mode, visits):
    host = urllib.parse.urlparse(url).hostname or url
    mode_label = "🌐 IP Rotation" if mode == "IP_ROTATION" else "🟢 Direct"
    _set_session(user_id, STATE_AWAIT_VISITS, url=url, mode=mode, visits=visits,
                 confirmed=True)
    _safe_send(
        chat_id,
        (f"🚀 *EXTRACTION READY*\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"🔗 Target: `{md_esc(host)}`\n"
         f"⚙️ Mode: {mode_label}\n"
         f"🔄 Visits: `{visits}`\n"
         f"━━━━━━━━━━━━━━━━━━━━"),
        markup=_confirm_start_kb(),
    )# =========================================================
#  Job state + progress UI
# =========================================================
def _new_job_state(job_id: int, code: str, total: int, mode: str, url: str,
                   chat_id: int, msg_id: int) -> dict:
    return {
        "job_id": job_id, "code": code, "total": total, "mode": mode,
        "url": url, "chat_id": chat_id, "msg_id": msg_id,
        "visit": 0, "successful": 0, "failed": 0,
        "unique_numbers": 0, "new_numbers": 0,
        "stage": "Preparing", "proxy_id": None, "proxy_protocol": "",
        "exit_ip": "", "latency_ms": 0,
        "started_at": time.time(), "last_event_ts": time.time(),
        "cancel": threading.Event(),
        "pause": threading.Event(),   # set when paused
        "paused": False,
    }


def _progress_text(st: dict) -> str:
    pct = int((st["visit"] / st["total"]) * 100) if st["total"] else 0
    elapsed = int(time.time() - st["started_at"])
    mm, ss = divmod(elapsed, 60)
    speed = (st["visit"] / elapsed) if elapsed > 0 else 0
    eta = (int((st["total"] - st["visit"]) / speed)
           if speed > 0 and st["visit"] > 0 else 0)
    eta_m, eta_s = divmod(eta, 60)
    proxy_line = ""
    if st["mode"] == "IP_ROTATION":
        if st["proxy_protocol"]:
            proxy_line = (
                f"\n🌐 Proxy: *{st['proxy_protocol']}* "
                f"(cached exit-IP)\n⚡ Latency: `{st['latency_ms']} ms`\n"
            )
        else:
            proxy_line = "\n🌐 Proxy: _selecting…_\n"
    status = "⏸ PAUSED" if st["paused"] else "🚀 EXTRACTION IN PROGRESS"
    return (
        f"{status}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Job: `{st['code']}`\n"
        f"🌐 Mode: *{st['mode']}*\n\n"
        f"Progress:\n`{_bar(pct)} {pct}%`\n\n"
        f"🔄 Visits: `{st['visit']}/{st['total']}`\n"
        f"✅ Successful: `{st['successful']}`\n"
        f"❌ Failed: `{st['failed']}`\n"
        f"📱 Numbers: `{st['unique_numbers']}`\n"
        f"{proxy_line}"
        f"⏱ Elapsed: `{mm:02d}:{ss:02d}`\n"
        f"⚡ Speed: `{speed:.1f}/min`\n"
        f"⌛ ETA: `{eta_m:02d}:{eta_s:02d}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 _{md_esc(st['stage'])}_"
    )


def _progress_updater(st: dict) -> None:
    """Independent thread: edits Telegram message at safe intervals."""
    interval = float(get_setting("progress_interval", str(PROGRESS_INTERVAL)))
    last_text = ""
    last_edit = 0
    while True:
        if st["cancel"].is_set():
            break
        if st["visit"] >= st["total"] and not st["paused"]:
            break
        # don't churn edits while paused
        if st["paused"]:
            time.sleep(max(0.6, interval))
            continue
        text = _progress_text(st)
        now = time.time()
        if text != last_text and (now - last_edit) >= interval:
            if _safe_edit(st["chat_id"], st["msg_id"], text):
                last_text = text
                last_edit = now
        time.sleep(max(0.6, interval))


def _emit(st: dict, stage: str, **kw) -> None:
    st["stage"] = stage
    st["last_event_ts"] = time.time()
    for k, v in kw.items():
        if k in st and not isinstance(st[k], threading.Event):
            st[k] = v


# =========================================================
#  Error classification
# =========================================================
def _classify_err(e: Exception) -> str:
    s = str(e).lower()
    if "407" in s or "proxy auth" in s:
        return "AUTH_FAILED"
    if "timeout" in s or "timed out" in s:
        return "TIMEOUT"
    if "ssl" in s or "certificate" in s:
        return "TLS_FAILURE"
    if "name or service not known" in s or "nodename" in s or "getaddrinfo" in s:
        return "DNS_FAILED"
    if "connection refused" in s:
        return "REFUSED"
    if "too many redirects" in s:
        return "TOO_MANY_REDIRECTS"
    if "403" in s:
        return "HTTP_403"
    if "429" in s:
        return "HTTP_429"
    if "404" in s:
        return "HTTP_404"
    if re.search(r"5\d\d", s):
        return "HTTP_5XX"
    if "url rejected" in s:
        return "INVALID_URL"
    return "REQUEST_FAILED"


# =========================================================
#  Extraction worker  (single-link)
# =========================================================
def extraction_worker(chat_id: int, user_id: int, username: str, url: str,
                      count: int, mode: str, msg_id: int) -> None:
    job_id, code = create_job(user_id, username, url, mode, count)
    st = _new_job_state(job_id, code, count, mode, url, chat_id, msg_id)
    with _state_lock:
        job_state[code] = st
        active_jobs.setdefault(user_id, set()).add(code)

    updater = threading.Thread(target=_progress_updater, args=(st,), daemon=True)
    updater.start()

    found: Dict[str, Tuple[str, str, int]] = {}
    dup_count = 0
    start = time.time()
    log.info("JOB_START job=%s user=%s mode=%s visits=%s url=%s",
             code, user_id, mode, count, _mask(url))

    for visit in range(1, count + 1):
        # ---- cancellation + pause gate ----
        if st["cancel"].is_set():
            break
        while st["pause"].is_set() and not st["cancel"].is_set():
            time.sleep(0.5)
        if st["cancel"].is_set():
            break

        st["visit"] = visit
        _emit(st, "Selecting proxy…" if mode == "IP_ROTATION"
              else "Connecting…")

        attempt_status = "UNKNOWN"
        attempt_err = ""
        attempt_proxy_id = None
        attempt_exit_ip = ""
        attempt_latency = 0
        final_url = url
        body = ""
        visited = [url]
        succeeded = False

        if mode == "IP_ROTATION":
            proxy_row = proxy_pool.select()
            if not proxy_row:
                _emit(st, "No verified proxy available")
                st["failed"] += 1
                save_attempt(job_id, visit, None, "", "NO_PROXY", 0,
                             "no verified proxy available")
                _safe_edit(
                    chat_id, msg_id,
                    ("❌ *IP ROTATION STOPPED*\n\n"
                     "No verified proxy is currently available.\n"
                     "Add or retest proxies from the Proxy Center."),
                )
                break

            proxy_dict = {
                "protocol": proxy_row["protocol"], "host": proxy_row["host"],
                "port": proxy_row["port"], "username": proxy_row["username"],
                "password": proxy_row["password"], "endpoint": proxy_row["endpoint"],
            }
            attempt_proxy_id = proxy_row["id"]
            st["proxy_id"] = proxy_row["id"]
            st["proxy_protocol"] = proxy_row["protocol"].upper()
            # use cached exit-IP — NO per-request IP verification
            cached = proxy_pool.cached_exit_ip(proxy_row["id"])
            st["exit_ip"] = cached or proxy_row.get("last_observed_ip") or ""
            _emit(st, "Fetching via proxy…")

            t0 = time.time()
            try:
                scraper = Scraper(use_proxy=True, proxy=proxy_dict)
                final_url, body, visited = scraper.fetch(url)
                attempt_latency = int((time.time() - t0) * 1000)
                st["latency_ms"] = attempt_latency
                attempt_status = "OK"
                proxy_pool.mark_used_success(
                    proxy_row["id"], attempt_latency,
                    st["exit_ip"] or proxy_row.get("last_observed_ip") or "")
                st["successful"] += 1
                succeeded = True
                _emit(st, "Scanning response…")
            except Exception as e:
                attempt_err = str(e)[:120]
                attempt_status = _classify_err(e)
                st["latency_ms"] = int((time.time() - t0) * 1000)
                # distinguish proxy failure from target failure
                if attempt_status in ("AUTH_FAILED", "TCP_FAILED", "TIMEOUT",
                                      "REFUSED", "DNS_FAILED"):
                    proxy_pool.mark_used_failure(
                        proxy_row["id"], attempt_status, attempt_err)
                else:
                    # target failed — proxy itself is fine
                    proxy_pool.mark_target_failure(proxy_row["id"], attempt_err)
                # one alternate proxy retry
                alt = proxy_pool.select()
                if alt and not st["cancel"].is_set():
                    _emit(st, "Retrying with alternate proxy…")
                    alt_dict = {
                        "protocol": alt["protocol"], "host": alt["host"],
                        "port": alt["port"], "username": alt["username"],
                        "password": alt["password"], "endpoint": alt["endpoint"],
                    }
                    try:
                        scraper = Scraper(use_proxy=True, proxy=alt_dict)
                        final_url, body, visited = scraper.fetch(url)
                        attempt_latency = int((time.time() - t0) * 1000)
                        attempt_status = "OK_RETRY"
                        attempt_proxy_id = alt["id"]
                        st["proxy_protocol"] = alt["protocol"].upper()
                        st["exit_ip"] = proxy_pool.cached_exit_ip(alt["id"]) \
                            or alt.get("last_observed_ip") or ""
                        proxy_pool.mark_used_success(
                            alt["id"], attempt_latency, st["exit_ip"])
                        st["successful"] += 1
                        succeeded = True
                        _emit(st, "Scanning response…")
                    except Exception as e2:
                        attempt_err = str(e2)[:120]
                        attempt_status = _classify_err(e2)
                        if attempt_status in ("AUTH_FAILED", "TCP_FAILED",
                                              "TIMEOUT", "REFUSED",
                                              "DNS_FAILED"):
                            proxy_pool.mark_used_failure(
                                alt["id"], attempt_status, attempt_err)
                        else:
                            proxy_pool.mark_target_failure(alt["id"], attempt_err)
                        st["failed"] += 1
                        save_attempt(job_id, visit, alt["id"], "",
                                     attempt_status, attempt_latency, attempt_err)
                        continue
                else:
                    st["failed"] += 1
                    save_attempt(job_id, visit, attempt_proxy_id,
                                 st["exit_ip"], attempt_status,
                                 attempt_latency, attempt_err)
                    continue
        else:
            # ---- direct mode ----
            _emit(st, "Fetching…")
            t0 = time.time()
            try:
                scraper = Scraper(use_proxy=False)
                final_url, body, visited = scraper.fetch(url)
                attempt_latency = int((time.time() - t0) * 1000)
                st["latency_ms"] = attempt_latency
                attempt_status = "OK"
                st["successful"] += 1
                succeeded = True
                _emit(st, "Scanning response…")
            except Exception as e:
                attempt_err = str(e)[:120]
                attempt_status = _classify_err(e)
                st["failed"] += 1
                save_attempt(job_id, visit, None, "", attempt_status,
                             attempt_latency, attempt_err)
                continue

        # ---- extract numbers from this visit ----
        if succeeded:
            cycle_numbers = []
            for n, m in extract_from_url_chain(visited):
                cycle_numbers.append((n, m, visited[-1]))
            for n, m in extract_numbers(body, final_url, default_method="html"):
                cycle_numbers.append((n, m, final_url))

            new_this_visit = 0
            batch_rows = []
            for n, m, src in cycle_numbers:
                if n in found:
                    dup_count += 1
                else:
                    found[n] = (m, src, visit)
                    new_this_visit += 1
                    batch_rows.append((n, m, src, visit))

            if batch_rows:
                _save_numbers_batch(job_id, user_id, batch_rows)

            st["unique_numbers"] = len(found)
            st["new_numbers"] = new_this_visit
            _emit(st, "Visit complete" if new_this_visit == 0
                  else f"Found {new_this_visit} new")

            save_attempt(job_id, visit, attempt_proxy_id, st["exit_ip"],
                         attempt_status, attempt_latency, "")

        time.sleep(0.15)

    # ---- finalize ----
    duration_ms = int((time.time() - start) * 1000)
    cancelled = st["cancel"].is_set()
    unique_count = len(found)
    status = ("CANCELLED" if cancelled
              else ("COMPLETED" if st["successful"] > 0 else "FAILED"))
    finish_job(job_id, st["successful"], st["failed"], unique_count,
               dup_count, duration_ms, status)
    update_user_stats(user_id, unique_count, urls_processed=st["successful"])

    with _state_lock:
        active_jobs.get(user_id, set()).discard(code)
        if not active_jobs.get(user_id):
            active_jobs.pop(user_id, None)
        job_state.pop(code, None)

    log.info("JOB_COMPLETE job=%s status=%s unique=%s dup=%s dur=%sms",
             code, status, unique_count, dup_count, duration_ms)

    _send_final_result(chat_id, user_id, username, job_id, code, url, mode,
                       count, st["successful"], st["failed"], unique_count,
                       dup_count, duration_ms, found, cancelled)
    try:
        _channel_post(job_id, code, user_id, username, url, mode, count,
                      st["successful"], st["failed"], unique_count,
                      dup_count, duration_ms, found)
    except Exception as e:
        log.warning("CHANNEL_POST_FAILED job=%s err=%s", code, e)


# =========================================================
#  Bulk extraction worker  (bounded pool, per-host limits)
# =========================================================
def _start_bulk_job(chat_id, user, urls: list, mode: str):
    """Run multiple single-visit extractions in parallel with bounded workers."""
    group_id = f"BULK-{random.randint(0, 0xFFFFFF):06X}"
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO bulk_groups(group_id, user_id, total_urls,
                   processed, successful, failed, numbers, status)
                   VALUES(?,?,?,?,0,0,0,0,'RUNNING')""",
                (group_id, user.id, len(urls)),
            )
            conn.commit()
        finally:
            conn.close()

    msg = bot.send_message(
        chat_id,
        ("📚 *BULK EXTRACTION*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         f"🆔 Group: `{group_id}`\n"
         f"🔗 URLs: `{len(urls)}`\n"
         f"🌐 Mode: *{md_esc(mode)}*\n\n"
         f"Progress:\n`{_bar(0)} 0%`\n\n"
         f"Processed: `0`\n"
         f"✅ Success: `0`\n"
         f"❌ Failed: `0`\n"
         f"📱 Numbers: `0`\n"
         f"━━━━━━━━━━━━━━━━━━━━"),
        parse_mode="Markdown",
        reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("🛑 Cancel Job",
                                       callback_data=f"cancelbulk_{group_id}")),
    )

    threading.Thread(
        target=_bulk_worker,
        args=(chat_id, user, urls, mode, group_id, msg.message_id),
        daemon=True,
    ).start()


def _bulk_worker(chat_id, user, urls, mode, group_id, msg_id):
    found: Dict[str, Tuple[str, str]] = {}
    dup_count = 0
    processed = successful = failed = 0
    total = len(urls)
    start = time.time()
    cancel_event = threading.Event()
    with _state_lock:
        # store cancel event under group_id for the cancel callback
        job_state[group_id] = {"cancel": cancel_event, "bulk": True}

    # dedupe URLs
    seen_urls: Set[str] = set()
    unique_urls = []
    for u in urls:
        if u not in seen_urls:
            seen_urls.add(u)
            unique_urls.append(u)
    dup_urls = total - len(unique_urls)
    total_unique = len(unique_urls)

    # per-host semaphore map
    host_locks: Dict[str, threading.Semaphore] = {}
    for u in unique_urls:
        h = urllib.parse.urlparse(u).hostname or "unknown"
        if h not in host_locks:
            host_locks[h] = threading.Semaphore(PER_HOST_LIMIT)

    def _one(u):
        if cancel_event.is_set():
            return None
        host = urllib.parse.urlparse(u).hostname or "unknown"
        sem = host_locks.get(host)
        if sem:
            sem.acquire()
        try:
            proxy_row = None
            if mode == "IP_ROTATION":
                proxy_row = proxy_pool.select()
                if not proxy_row:
                    return ("failed", u, None, "no_proxy")
            scraper = Scraper(
                use_proxy=(mode == "IP_ROTATION"),
                proxy=({"protocol": proxy_row["protocol"],
                        "host": proxy_row["host"], "port": proxy_row["port"],
                        "username": proxy_row["username"],
                        "password": proxy_row["password"],
                        "endpoint": proxy_row["endpoint"]}
                       if proxy_row else None),
            )
            final_url, body, visited = scraper.fetch(u)
            nums = []
            for n, m in extract_from_url_chain(visited):
                nums.append((n, m, visited[-1]))
            for n, m in extract_numbers(body, final_url, default_method="html"):
                nums.append((n, m, final_url))
            return ("ok", u, nums, None)
        except Exception as e:
            return ("failed", u, None, _classify_err(e))
        finally:
            if sem:
                sem.release()

    last_edit = 0
    with ThreadPoolExecutor(max_workers=BULK_WORKERS) as ex:
        futs = {ex.submit(_one, u): u for u in unique_urls}
        for fut in as_completed(futs):
            if cancel_event.is_set():
                for f in futs:
                    f.cancel()
                break
            try:
                res = fut.result()
            except Exception:
                res = ("failed", "?", None, "worker_error")
            processed += 1
            if res and res[0] == "ok":
                successful += 1
                _, u, nums, _ = res
                batch_rows = []
                for n, m, src in (nums or []):
                    if n in found:
                        dup_count += 1
                    else:
                        found[n] = (m, src)
                        batch_rows.append((n, m, src, 0))
                if batch_rows:
                    # use a synthetic job_id of 0; numbers stored under group
                    with _db_lock:
                        conn = get_conn()
                        try:
                            conn.executemany(
                                """INSERT INTO extraction_job_numbers
                                   (job_id, user_id, number, source_url,
                                    extraction_method, visit_number)
                                   VALUES(0,?,?,?,?,0)""",
                                [(0, user.id, n, src, m) for n, m, src, _ in
                                 batch_rows],
                            )
                            conn.commit()
                        finally:
                            conn.close()
            else:
                failed += 1

            # throttle progress edits to ~1/sec
            now = time.time()
            if now - last_edit >= 1.0:
                pct = int((processed / total_unique) * 100) if total_unique else 0
                elapsed = int(now - start)
                speed = (processed / elapsed) if elapsed > 0 else 0
                eta = int((total_unique - processed) / speed) if speed > 0 else 0
                _safe_edit(
                    chat_id, msg_id,
                    ("📚 *BULK EXTRACTION*\n"
                     "━━━━━━━━━━━━━━━━━━━━\n"
                     f"🆔 Group: `{group_id}`\n"
                     f"🔗 URLs: `{total_unique}` "
                     f"(dupes removed: `{dup_urls}`)\n"
                     f"🌐 Mode: *{md_esc(mode)}*\n\n"
                     f"Progress:\n`{_bar(pct)} {pct}%`\n\n"
                     f"Processed: `{processed}/{total_unique}`\n"
                     f"✅ Success: `{successful}`\n"
                     f"❌ Failed: `{failed}`\n"
                     f"📱 Numbers: `{len(found)}`\n"
                     f"⚡ Speed: `{speed:.1f}/min`\n"
                     f"⌛ ETA: `{eta//60:02d}:{eta%60:02d}`\n"
                     "━━━━━━━━━━━━━━━━━━━━"),
                )
                last_edit = now

    duration_ms = int((time.time() - start) * 1000)
    cancelled = cancel_event.is_set()
    status = "CANCELLED" if cancelled else ("COMPLETED" if successful > 0
                                            else "FAILED")
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE bulk_groups SET processed=?, successful=?, failed=?,
                   numbers=?, status=?, completed_at=CURRENT_TIMESTAMP
                   WHERE group_id=?""",
                (processed, successful, failed, len(found), status, group_id),
            )
            conn.commit()
        finally:
            conn.close()
    with _state_lock:
        job_state.pop(group_id, None)

    _safe_edit(
        chat_id, msg_id,
        ("✅ *BULK COMPLETE*" if not cancelled else "🛑 *BULK CANCELLED*") +
        (f"\n━━━━━━━━━━━━━━━━━━━━\n"
         f"🆔 Group: `{group_id}`\n"
         f"🔗 Processed: `{processed}/{total_unique}`\n"
         f"✅ Successful: `{successful}`\n"
         f"❌ Failed: `{failed}`\n"
         f"📱 Unique Numbers: `{len(found)}`\n"
         f"♻️ Duplicates: `{dup_count}`\n"
         f"⏱ Duration: `{_fmt_duration(duration_ms)}`\n"
         "━━━━━━━━━━━━━━━━━━━━"),
    )
    # deliver results + files
    _send_bulk_result(chat_id, user, group_id, found, duration_ms)


# =========================================================
#  Final result + export files
# =========================================================
def _send_final_result(chat_id, user_id, username, job_id, code, url, mode,
                       count, success, failed, unique, dup, dur_ms, found,
                       cancelled):
    host = urllib.parse.urlparse(url).hostname or url
    mode_label = "🌐 IP Rotation" if mode == "IP_ROTATION" else "🟢 Direct"
    head = (
        f"{'✅ EXTRACTION COMPLETE' if not cancelled else '🛑 EXTRACTION CANCELLED'}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Job: `{code}`\n"
        f"🔗 Source: `{md_esc(_short(host, 50))}`\n\n"
        f"⚙️ Mode: {mode_label}\n\n"
        f"🔄 Visits: `{success + failed}/{count}`\n"
        f"✅ Successful: `{success}`\n"
        f"❌ Failed: `{failed}`\n\n"
        f"📱 Unique Numbers: `{unique}`\n"
        f"♻️ Duplicates: `{dup}`\n\n"
        f"⏱ Duration: `{_fmt_duration(dur_ms)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

    mk = types.InlineKeyboardMarkup(row_width=2)
    btns = []
    if unique > 0:
        btns.append(types.InlineKeyboardButton(
            "📋 Copy Numbers",
            switch_inline_query_current_chat=f"copy_{code}"))
        btns.append(types.InlineKeyboardButton(
            "📥 TXT", callback_data=f"dl_{code}_txt"))
        btns.append(types.InlineKeyboardButton(
            "📊 CSV", callback_data=f"dl_{code}_csv"))
        btns.append(types.InlineKeyboardButton(
            "📄 JSON", callback_data=f"dl_{code}_json"))
        btns.append(types.InlineKeyboardButton(
            "📤 Send to Channel", callback_data=f"chan_{code}"))
    btns.append(types.InlineKeyboardButton(
        "🔄 Run Again", callback_data="ex_start"))
    btns.append(types.InlineKeyboardButton(
        "🗑 Delete Result", callback_data=f"del_{code}"))
    btns.append(types.InlineKeyboardButton("◀️ Home",
                                            callback_data="nav_home"))
    # arrange 2 per row
    for i in range(0, len(btns), 2):
        mk.row(*btns[i:i+2])

    _safe_send(chat_id, head, markup=mk)
    _safe_send(chat_id, "🏠 *Main Menu*", markup=main_keyboard(user_id))


def _send_bulk_result(chat_id, user, group_id, found, dur_ms):
    if not found:
        _safe_send(chat_id, "🏠 *Main Menu*",
                   markup=main_keyboard(user.id))
        return
    sorted_nums = sorted(found.keys())
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton(
            "📋 Copy Numbers",
            switch_inline_query_current_chat=f"copybulk_{group_id}"),
        types.InlineKeyboardButton(
            "📥 TXT", callback_data=f"dlbulk_{group_id}_txt"),
        types.InlineKeyboardButton(
            "📊 CSV", callback_data=f"dlbulk_{group_id}_csv"),
        types.InlineKeyboardButton(
            "📄 JSON", callback_data=f"dlbulk_{group_id}_json"),
        types.InlineKeyboardButton("◀️ Home", callback_data="nav_home"),
    )
    _safe_send(
        chat_id,
        ("📁 *BULK RESULT FILES*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         f"🆔 Group: `{group_id}`\n"
         f"📱 Numbers: `{len(sorted_nums)}`\n"
         f"⏱ Duration: `{_fmt_duration(dur_ms)}`\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "Tap a format to download:"),
        markup=mk,
    )


def _build_txt(code: str, found: Dict[str, Tuple]) -> bytes:
    lines = [
        "DK Scraping Bot — Extraction Result",
        f"Job ID: {code}",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Unique Numbers: {len(found)}",
        "=" * 50,
        "",
    ]
    for n in sorted(found.keys()):
        m, src = found[n][0], found[n][1] if len(found[n]) > 1 else ""
        lines.append(f"+{n}  |  {m}  |  {src}")
    return "\n".join(lines).encode("utf-8")


def _build_csv(code: str, found: Dict[str, Tuple]) -> bytes:
    import csv
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["number", "method", "source_url"])
    for n in sorted(found.keys()):
        if len(found[n]) >= 2:
            w.writerow([f"+{n}", found[n][0], found[n][1]])
        else:
            w.writerow([f"+{n}", found[n][0], ""])
    return buf.getvalue().encode("utf-8")


def _build_json(code: str, found: Dict[str, Tuple]) -> bytes:
    out = {
        "job_id": code,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "unique_numbers": len(found),
        "numbers": [
            {"number": f"+{n}", "method": (found[n][0] if found[n] else ""),
             "source_url": (found[n][1] if len(found[n]) > 1 else "")}
            for n in sorted(found.keys())
        ],
    }
    return json.dumps(out, indent=2, ensure_ascii=False).encode("utf-8")


def _deliver_file(chat_id, code: str, fmt: str, found: Dict[str, Tuple]):
    if not found:
        _safe_send(chat_id, "❌ No numbers to export.")
        return
    if fmt == "txt":
        data = _build_txt(code, found)
        name = f"{code.replace('-', '_')}_numbers.txt"
    elif fmt == "csv":
        data = _build_csv(code, found)
        name = f"{code.replace('-', '_')}_numbers.csv"
    elif fmt == "json":
        data = _build_json(code, found)
        name = f"{code.replace('-', '_')}_numbers.json"
    else:
        return
    bio = io.BytesIO(data)
    bio.name = name
    try:
        bot.send_document(chat_id, bio,
                          caption=(f"📁 *{fmt.upper()} Export — {code}*\n"
                                   f"📱 `{len(found)}` unique numbers"),
                          parse_mode="Markdown")
    except Exception as e:
        log.warning("FILE_SEND_FAILED %s err=%s", code, e)
        _safe_send(chat_id, f"❌ File send failed: `{str(e)[:80]}`")


# =========================================================
#  Channel auto-post
# =========================================================
def _channel_post(job_id, code, user_id, username, url, mode, count, success,
                  failed, unique, dup, dur_ms, found):
    cfg = get_settings_batch([
        "channel_logging", "channel_username", "channel_include_username",
        "channel_include_uid", "channel_include_method",
        "channel_include_numbers", "channel_include_source", "show_duration",
        "show_job_id",
    ])
    if cfg["channel_logging"] != "1":
        return
    channel = cfg["channel_username"]
    if not channel:
        return
    host = urllib.parse.urlparse(url).hostname or url
    mode_label = "🌐 IP Rotation" if mode == "IP_ROTATION" else "🟢 Direct"
    lines = ["📡 EXTRACTION RESULT", "━━━━━━━━━━━━━━━━━━━━"]
    if cfg["channel_include_username"] == "1":
        lines.append(f"👤 User: @{md_esc(username or '—')}")
    if cfg["channel_include_uid"] == "1":
        lines.append(f"🆔 User ID: `{user_id}`")
    if cfg["channel_include_source"] == "1":
        lines.append(f"🔗 Source: `{md_esc(host)}`")
    if cfg["channel_include_method"] == "1":
        lines.append(f"⚙️ Mode: {mode_label}")
    lines += [
        f"🔄 Visits: `{count}`",
        f"✅ Successful: `{success}`",
        f"❌ Failed: `{failed}`",
        f"📱 Unique Numbers: `{unique}`",
        f"♻️ Duplicates: `{dup}`",
    ]
    if cfg["show_duration"] == "1":
        lines.append(f"⏱ Duration: `{_fmt_duration(dur_ms)}`")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    if cfg["channel_include_numbers"] == "1" and unique > 0:
        nums = sorted(found.keys())
        if len(nums) <= 30:
            lines.append("📞 Numbers:")
            for n in nums:
                lines.append(f"+{n}")
            lines.append("━━━━━━━━━━━━━━━━━━━━")
        else:
            lines.append(f"📞 Numbers: `{unique}` (summary only)")
    if cfg["show_job_id"] == "1":
        lines.append(f"🆔 Job: `{code}`")
    lines.append(f"🕒 {datetime.utcnow().strftime('%d %b %Y • %H:%M UTC')}")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "…"

    # inline button to fetch numbers in private chat
    mk = types.InlineKeyboardMarkup()
    if unique > 0:
        mk.add(types.InlineKeyboardButton(
            "📋 Copy Numbers",
            switch_inline_query_current_chat=f"copy_{code}"))

    msg_id = None
    err = ""
    for attempt in range(3):
        try:
            msg = bot.send_message(channel, text, parse_mode="Markdown",
                                   reply_markup=mk if mk.keyboard else None)
            msg_id = msg.message_id
            err = ""
            break
        except ApiTelegramException as e:
            err = str(e)[:120]
            low = err.lower()
            if "chat not found" in low or "not enough rights" in low \
                    or "bot is not a member" in low:
                break
            if "retry after" in low:
                m = re.search(r"retry after (\d+)", low)
                time.sleep(int(m.group(1)) + 1 if m else 2)
                continue
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            err = str(e)[:120]
            time.sleep(1.5 * (attempt + 1))

    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO channel_posts(job_id, channel, status, "
                "message_id, error) VALUES(?,?,?,?,?)",
                (job_id, channel, "OK" if msg_id else "FAILED", msg_id, err),
            )
            conn.commit()
        finally:
            conn.close()
    if msg_id:
        log.info("CHANNEL_POST_SUCCESS job=%s channel=%s", code, channel)
    else:
        log.warning("CHANNEL_POST_FAILED job=%s channel=%s err=%s",
                    code, channel, err)# =========================================================
#  User views: stats, history, settings, help
# =========================================================
def _show_user_stats(chat_id, user_id):
    u = get_user(user_id)
    if not u:
        _safe_send(chat_id, "📊 No stats yet.",
                   markup=main_keyboard(user_id))
        return
    jobs, total = user_jobs(user_id, limit=500)
    succ = sum(1 for j in jobs if j["status"] == "COMPLETED")
    fail = sum(1 for j in jobs if j["status"] == "FAILED")
    rate = (succ / len(jobs) * 100) if jobs else 0
    today = [j for j in jobs
             if j["started_at"] and j["started_at"][:10] ==
             datetime.utcnow().strftime("%Y-%m-%d")]
    today_nums = sum(j["unique_numbers"] for j in today)
    _safe_send(
        chat_id,
        ("📊 *MY STATISTICS*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         f"Total Jobs: `{len(jobs)}`\n"
         f"Completed: `{succ}`\n"
         f"Failed: `{fail}`\n\n"
         f"URLs Processed: `{u['total_urls_processed']}`\n"
         f"Numbers Found: `{u['total_numbers_found']:,}`\n\n"
         "Today:\n"
         f"Jobs: `{len(today)}`\n"
         f"Numbers: `{today_nums}`\n\n"
         f"Success Rate: `{rate:.1f}%`\n"
         "━━━━━━━━━━━━━━━━━━━━"),
        markup=_home_kb(user_id),
    )


def _show_user_history(chat_id, user_id, page=0):
    jobs, total = user_jobs(user_id, limit=5, page=page)
    if not jobs and page == 0:
        _safe_send(
            chat_id,
            ("🕘 *JOB HISTORY*\n"
             "━━━━━━━━━━━━━━━━━━━━\n"
             "No extraction history yet.\n\n"
             "Start your first extraction to see jobs here."),
            markup=_home_kb(user_id),
        )
        return
    lines = ["🕘 *JOB HISTORY*", "━━━━━━━━━━━━━━━━━━━━"]
    for j in jobs:
        host = urllib.parse.urlparse(j["source_url"]).hostname or \
            j["source_url"][:30]
        mode_emoji = "🌐" if j["mode"] == "IP_ROTATION" else "🟢"
        st = {"COMPLETED": "✅", "FAILED": "❌", "CANCELLED": "🛑",
              "INTERRUPTED": "⚠️", "RUNNING": "⏳"}.get(j["status"], "⚪")
        ts = (j["started_at"] or "")[:16]
        lines.append(
            f"{st} `{j['job_code']}` {mode_emoji}\n"
            f"🔗 `{md_esc(host)}`\n"
            f"📱 `{j['unique_numbers']}` | 🔄 `{j['requested_visits']}`"
        )
        lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    mk = types.InlineKeyboardMarkup(row_width=1)
    for j in jobs:
        mk.add(types.InlineKeyboardButton(
            f"{j['job_code']}",
            callback_data=f"ujob_{j['job_id']}"))
    # pagination
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("◀️ Prev",
                                              callback_data=f"hipage_{page-1}"))
    if (page + 1) * 5 < total:
        nav.append(types.InlineKeyboardButton("▶️ Next",
                                              callback_data=f"hipage_{page+1}"))
    if nav:
        mk.row(*nav)
    mk.add(types.InlineKeyboardButton("◀️ Home", callback_data="nav_home"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


def _show_user_settings(chat_id, user_id):
    _safe_send(
        chat_id,
        ("⚙️ *SETTINGS*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "Personal settings are managed via the buttons below.\n\n"
         "🌐 Default Mode — choose your preferred extraction mode\n"
         "📄 Result Format — default export format\n"
         "🔔 Notifications — toggle job notifications\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "Note: per-user settings persistence is available; "
         "admin-wide limits are set in the Admin Panel."),
        markup=_home_kb(user_id),
    )


def _show_help(chat_id):
    _safe_send(
        chat_id,
        ("❓ *HELP CENTER*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "🚀 *How to Extract*\n"
         "1. Tap *Start Extraction*\n"
         "2. Choose Single or Bulk Link\n"
         "3. Pick Direct or IP Rotation mode\n"
         "4. Choose visit count\n"
         "5. Confirm and run\n"
         "6. Download TXT / CSV / JSON\n\n"
         "🌐 *Direct vs IP Mode*\n"
         "Direct: fastest, uses your server's IP.\n"
         "IP Rotation: routes through verified proxies.\n\n"
         "📚 *Bulk Links*\n"
         "Submit many URLs at once. Processed through a bounded "
         "worker pool — no unbounded threads.\n\n"
         "🌐 *Proxy System*\n"
         "Proxies are tested multi-stage (TCP → HTTP → exit-IP). "
         "Failed targets don't kill healthy proxies.\n\n"
         "📊 *Results*\n"
         "TXT / CSV / JSON export + copy-friendly output.\n\n"
         "📡 *Channel Posting*\n"
         "Admin can auto-post summaries to a configured channel.\n\n"
         "🛡 *Safety & Limits*\n"
         "Only authorized public URLs. No CAPTCHA bypass, "
         "no login bypass, no private-account scraping.\n"
         "━━━━━━━━━━━━━━━━━━━━"),
        markup=_home_kb(0),
    )


def _show_job_detail(chat_id, job_id, admin_view=False, viewer_id=None):
    j = get_job(job_id)
    if not j:
        _safe_send(chat_id, "Job not found.")
        return
    # ownership check for non-admins
    if not admin_view and j["user_id"] != viewer_id:
        _safe_send(chat_id, "❌ You can only view your own jobs.",
                   markup=_home_kb(viewer_id))
        return
    host = urllib.parse.urlparse(j["source_url"]).hostname or \
        j["source_url"][:30]
    mode_label = "🌐 IP Rotation" if j["mode"] == "IP_ROTATION" else "🟢 Direct"
    nums = job_numbers(job_id, limit=50)
    lines = [
        f"📋 *JOB {j['job_code']}*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"👤 User: @{md_esc(j['username'] or '—')} (`{j['user_id']}`)",
        f"🔗 URL: `{md_esc(_short(j['source_url'], 60))}`",
        f"⚙️ Mode: {mode_label}",
        f"🔄 Visits: `{j['successful_visits']+j['failed_visits']}/{j['requested_visits']}`",
        f"✅ Successful: `{j['successful_visits']}`",
        f"❌ Failed: `{j['failed_visits']}`",
        f"📱 Unique: `{j['unique_numbers']}`",
        f"♻️ Duplicates: `{j['duplicate_numbers']}`",
        f"⏱ Duration: `{_fmt_duration(j['duration_ms'])}`",
        f"🕒 Started: `{(j['started_at'] or '')[:16]}`",
        f"🕒 Completed: `{(j['completed_at'] or '')[:16]}`",
        f"Status: `{j['status']}`",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if admin_view:
        attempts = job_attempts(job_id, limit=20)
        if attempts:
            lines.append("🔄 Recent Attempts:")
            for a in attempts[:10]:
                pid = f"#{a['proxy_id']}" if a['proxy_id'] else "direct"
                lines.append(
                    f"  v{a['cycle']}: {pid} {a['request_status']} "
                    f"{a['latency_ms']}ms"
                )
            lines.append("━━━━━━━━━━━━━━━━━━━━")
    if nums:
        lines.append("📱 Numbers:")
        for n in nums[:15]:
            lines.append(f"+{n['number']} ({n['extraction_method']})")
        if len(nums) > 15:
            lines.append(f"_…and {j['unique_numbers']-15} more_")

    mk = types.InlineKeyboardMarkup(row_width=2)
    btns = []
    if j["unique_numbers"] > 0:
        btns.append(types.InlineKeyboardButton(
            "📋 Copy", switch_inline_query_current_chat=f"copy_{j['job_code']}"))
        btns.append(types.InlineKeyboardButton(
            "📥 TXT", callback_data=f"dl_{j['job_code']}_txt"))
        btns.append(types.InlineKeyboardButton(
            "🔄 Retry", callback_data=f"retry_{j['job_id']}"))
    btns.append(types.InlineKeyboardButton("◀️ Back",
                                           callback_data="nav_back"))
    for i in range(0, len(btns), 2):
        mk.row(*btns[i:i+2])
    _safe_send(chat_id, "\n".join(lines), markup=mk)


# =========================================================
#  Job starter (single link)
# =========================================================
def _start_job(chat_id, user, url, mode, visits):
    msg = bot.send_message(
        chat_id,
        ("🚀 *EXTRACTION IN PROGRESS*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "🔄 _Preparing…_"),
        parse_mode="Markdown",
        reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
            types.InlineKeyboardButton("⏸ Pause",
                                       callback_data="pausejob_placeholder"),
            types.InlineKeyboardButton("🛑 Cancel",
                                       callback_data="cancelplaceholder"),
        ),
    )
    # fix callback_data with real code once we know it — but we don't yet,
    # so we wire cancel via user-id lookup; pause/resume via job_state
    threading.Thread(
        target=extraction_worker,
        args=(chat_id, user.id, user.username, url, visits, mode,
              msg.message_id),
        daemon=True,
    ).start()


# =========================================================
#  Cancel / pause / resume helpers
# =========================================================
def _cancel_user_job(user_id: int) -> bool:
    """Cancel ALL running jobs for a user. Returns True if any were cancelled."""
    cancelled = False
    with _state_lock:
        codes = list(active_jobs.get(user_id, set()))
    for code in codes:
        st = job_state.get(code)
        if st and isinstance(st.get("cancel"), threading.Event):
            st["cancel"].set()
            cancelled = True
    return cancelled


def _cancel_job_by_code(code: str) -> bool:
    st = job_state.get(code)
    if st and isinstance(st.get("cancel"), threading.Event):
        st["cancel"].set()
        return True
    return False


def _pause_job(code: str) -> bool:
    st = job_state.get(code)
    if st and isinstance(st.get("pause"), threading.Event):
        st["pause"].set()
        st["paused"] = True
        return True
    return False


def _resume_job(code: str) -> bool:
    st = job_state.get(code)
    if st and isinstance(st.get("pause"), threading.Event):
        st["pause"].clear()
        st["paused"] = False
        return True
    return False


# =========================================================
#  Callback query router
# =========================================================
@bot.callback_query_handler(func=lambda c: True)
def on_callback(c: types.CallbackQuery):
    u = c.from_user
    data = c.data or ""
    chat_id = c.message.chat.id

    try:
        # ---- navigation ----
        if data == "nav_home":
            _clear_session(u.id)
            _safe_edit(chat_id, c.message.message_id,
                       _home_text(u.id), markup=_home_kb(u.id))
            return
        if data == "nav_back":
            _handle_back(c)
            return
        if data == "nav_cancel":
            _clear_session(u.id)
            _cancel_user_job(u.id)
            _safe_edit(chat_id, c.message.message_id,
                       "🛑 *Cancelled.*")
            _send_home(chat_id, u.id)
            return

        # ---- extraction flow ----
        if data == "ex_start":
            _clear_session(u.id)
            _start_extraction_flow(chat_id, u)
            _answer_cb(c)
            return
        if data == "ex_single":
            _set_session(u.id, STATE_AWAIT_URL)
            _safe_edit(
                chat_id, c.message.message_id,
                ("🔗 *SEND YOUR LINK*\n"
                 "━━━━━━━━━━━━━━━━━━━━\n"
                 "Send the URL you want to process.\n\n"
                 "_Example:_ `https://example.com/l/abc`"),
                markup=_back_cancel_kb(),
            )
            _answer_cb(c)
            return
        if data == "ex_bulk":
            _set_session(u.id, STATE_AWAIT_BULK)
            _safe_edit(
                chat_id, c.message.message_id,
                ("♾️ *BULK URL QUEUE*\n"
                 "━━━━━━━━━━━━━━━━━━━━\n"
                 "Send multiple URLs, one per line.\n"
                 "They will be processed through the job queue "
                 "with bounded concurrency.\n\n"
                 f"_Max {MAX_URLS_PER_BATCH} URLs per batch._"),
                markup=_back_cancel_kb(),
            )
            _answer_cb(c)
            return

        # ---- net mode ----
        if data == "netmode_direct":
            session = _get_session(u.id)
            if not session or "url" not in session:
                _answer_cb(c, "Session expired. Start again.")
                return
            _set_session(u.id, STATE_AWAIT_VISITS,
                         url=session["url"], mode="DIRECT")
            _safe_edit(
                chat_id, c.message.message_id,
                ("🟢 *DIRECT MODE*\n"
                 "Choose visit count:"),
                markup=_visits_keyboard(),
            )
            _answer_cb(c)
            return
        if data == "netmode_proxy":
            if get_setting("proxy_enabled", "1") != "1":
                _answer_cb(c, "Proxy mode disabled by admin.")
                return
            pc = proxy_pool.count()
            if pc["working"] + pc["slow"] + pc["connected"] == 0:
                _safe_edit(
                    chat_id, c.message.message_id,
                    ("❌ *No verified proxy available.*\n\n"
                     "Add or retest proxies from the Proxy Center "
                     "before using IP Rotation.\n\n"
                     "Never silently falling back to direct mode."),
                    markup=types.InlineKeyboardMarkup(row_width=1).add(
                        types.InlineKeyboardButton("🔄 Test Proxies",
                                                   callback_data="px_test_all"),
                        types.InlineKeyboardButton("◀️ Back",
                                                   callback_data="nav_back"),
                    ),
                )
                _answer_cb(c)
                return
            session = _get_session(u.id)
            if not session or "url" not in session:
                _answer_cb(c, "Session expired. Start again.")
                return
            _set_session(u.id, STATE_AWAIT_VISITS,
                         url=session["url"], mode="IP_ROTATION")
            _safe_edit(
                chat_id, c.message.message_id,
                ("🌐 *IP ROTATION MODE*\n"
                 "Uses only verified proxies from the bot's pool.\n\n"
                 "Choose visit count:"),
                markup=_visits_keyboard(),
            )
            _answer_cb(c)
            return

        # ---- visits ----
        if data.startswith("visits_"):
            session = _get_session(u.id)
            if not session or "url" not in session or "mode" not in session:
                _answer_cb(c, "Session expired. Start again.")
                return
            n = int(data.split("_")[1])
            mx = int(get_setting("max_visits", str(MAX_VISITS_PER_JOB)))
            if n > mx:
                n = mx
            _show_confirm(chat_id, u.id, session["url"], session["mode"], n)
            _answer_cb(c)
            return

        # ---- confirm start ----
        if data == "confirm_start":
            session = _get_session(u.id)
            if not session or "url" not in session or "mode" not in session:
                _answer_cb(c, "Session expired.")
                _send_home(chat_id, u.id)
                return
            url = session["url"]
            mode = session["mode"]
            visits = session.get("visits", 1)
            _clear_session(u.id)
            with _state_lock:
                if len(active_jobs.get(u.id, set())) >= MAX_CONCURRENT_JOBS:
                    _answer_cb(c, "Too many running jobs.")
                    return
            _start_job(chat_id, u, url, mode, visits)
            _answer_cb(c, "Started.")
            return

        # ---- user views ----
        if data == "user_stats":
            _show_user_stats(chat_id, u.id)
            _answer_cb(c)
            return
        if data == "user_history":
            _show_user_history(chat_id, u.id, page=0)
            _answer_cb(c)
            return
        if data == "user_settings":
            _show_user_settings(chat_id, u.id)
            _answer_cb(c)
            return
        if data == "user_help":
            _show_help(chat_id)
            _answer_cb(c)
            return

        # ---- user job detail ----
        if data.startswith("ujob_"):
            jid = int(data.split("_", 1)[1])
            _show_job_detail(chat_id, jid, admin_view=False, viewer_id=u.id)
            _answer_cb(c)
            return
        if data.startswith("hipage_"):
            _show_user_history(chat_id, u.id, page=int(data.split("_")[1]))
            _answer_cb(c)
            return

        # ---- pause / resume / cancel job ----
        if data == "pauseplaceholder":
            # pause all running jobs for this user
            with _state_lock:
                codes = list(active_jobs.get(u.id, set()))
            for code in codes:
                _pause_job(code)
            _answer_cb(c, "Paused.")
            return
        if data == "resumeplaceholder":
            with _state_lock:
                codes = list(active_jobs.get(u.id, set()))
            for code in codes:
                _resume_job(code)
            _answer_cb(c, "Resumed.")
            return
        if data == "cancelplaceholder":
            _cancel_user_job(u.id)
            _answer_cb(c, "Cancelling…")
            return
        if data.startswith("cancelbulk_"):
            gid = data.split("_", 1)[1]
            st = job_state.get(gid)
            if st and isinstance(st.get("cancel"), threading.Event):
                st["cancel"].set()
                _answer_cb(c, "Cancelling bulk job…")
            else:
                _answer_cb(c, "Job not found.")
            return

        # ---- download / copy ----
        if data.startswith("dl_"):
            parts = data.split("_")
            code = parts[1]
            fmt = parts[2]
            j = get_job_by_code(code)
            if not j or (j["user_id"] != u.id and not is_admin(u.id)):
                _answer_cb(c, "Not authorized.")
                return
            nums = job_numbers(j["job_id"], limit=10000)
            found: Dict[str, Tuple] = {}
            for n in nums:
                found[n["number"]] = (n["extraction_method"], n["source_url"])
            _deliver_file(chat_id, code, fmt, found)
            _answer_cb(c)
            return
        if data.startswith("dlbulk_"):
            parts = data.split("_")
            group_id = parts[1]
            fmt = parts[2]
            with _db_lock:
                conn = get_conn()
                try:
                    rows = conn.execute(
                        "SELECT number, extraction_method, source_url "
                        "FROM extraction_job_numbers WHERE user_id=? "
                        "AND job_id=0 ORDER BY number",
                        (u.id,),
                    ).fetchall()
                finally:
                    conn.close()
            found = {r["number"]: (r["extraction_method"], r["source_url"])
                     for r in rows}
            _deliver_file(chat_id, group_id, fmt, found)
            _answer_cb(c)
            return
        if data.startswith("del_"):
            code = data.split("_", 1)[1]
            j = get_job_by_code(code)
            if not j or (j["user_id"] != u.id and not is_admin(u.id)):
                _answer_cb(c, "Not authorized.")
                return
            # confirmation
            mk = types.InlineKeyboardMarkup(row_width=2)
            mk.add(
                types.InlineKeyboardButton("✅ Yes, Delete",
                                           callback_data=f"delconf_{code}"),
                types.InlineKeyboardButton("❌ No",
                                           callback_data="nav_back"),
            )
            _safe_edit(
                chat_id, c.message.message_id,
                (f"⚠️ *DELETE RESULT?*\n"
                 f"━━━━━━━━━━━━━━━━━━━━\n"
                 f"Job: `{code}`\n"
                 f"Numbers: `{j['unique_numbers']}`\n\n"
                 f"This will remove the stored numbers.\n\n"
                 f"Are you sure?"),
                markup=mk,
            )
            _answer_cb(c)
            return
        if data.startswith("delconf_"):
            code = data.split("_", 1)[1]
            j = get_job_by_code(code)
            if not j or (j["user_id"] != u.id and not is_admin(u.id)):
                _answer_cb(c, "Not authorized.")
                return
            with _db_lock:
                conn = get_conn()
                try:
                    conn.execute(
                        "DELETE FROM extraction_job_numbers WHERE job_id=?",
                        (j["job_id"],))
                    conn.commit()
                finally:
                    conn.close()
            _safe_edit(chat_id, c.message.message_id,
                       "🗑 *Result deleted.*")
            _answer_cb(c)
            return

        # ---- retry job ----
        if data.startswith("retry_"):
            jid = int(data.split("_", 1)[1])
            j = get_job(jid)
            if not j or (j["user_id"] != u.id and not is_admin(u.id)):
                _answer_cb(c, "Not authorized.")
                return
            _start_job(chat_id, u, j["source_url"], j["mode"],
                       j["requested_visits"])
            _answer_cb(c, "Retrying…")
            return

        # ---- channel post (manual) ----
        if data.startswith("chan_"):
            code = data.split("_", 1)[1]
            j = get_job_by_code(code)
            if not j or not is_admin(u.id):
                _answer_cb(c, "Not authorized.")
                return
            nums = job_numbers(j["job_id"], limit=10000)
            found = {n["number"]: (n["extraction_method"], n["source_url"])
                     for n in nums}
            try:
                _channel_post(j["job_id"], code, j["user_id"], j["username"],
                              j["source_url"], j["mode"], j["requested_visits"],
                              j["successful_visits"], j["failed_visits"],
                              j["unique_numbers"], j["duplicate_numbers"],
                              j["duration_ms"], found)
                _answer_cb(c, "Posted to channel.")
            except Exception as e:
                _answer_cb(c, f"Failed: {str(e)[:60]}")
            return

        # ---- approval ----
        if data.startswith("appr_"):
            target = int(data.split("_", 1)[1])
            if not is_admin(u.id):
                _answer_cb(c, "Not authorized.")
                return
            set_user_status(target, "APPROVED")
            audit_log(u.id, "USER_APPROVED", str(target))
            try:
                bot.send_message(
                    target,
                    "✅ *Your access has been approved!*\nUse /start to begin.",
                    parse_mode="Markdown",
                )
            except Exception:
                pass
            _safe_edit(chat_id, c.message.message_id, "✅ Approved.")
            _answer_cb(c)
            return
        if data.startswith("rej_"):
            target = int(data.split("_", 1)[1])
            if not is_admin(u.id):
                _answer_cb(c, "Not authorized.")
                return
            set_user_status(target, "BLOCKED")
            audit_log(u.id, "USER_REJECTED", str(target))
            _safe_edit(chat_id, c.message.message_id,
                       "❌ Rejected & blocked.")
            _answer_cb(c)
            return

        # ---- admin routing ----
        if data.startswith("adm_") and not is_admin(u.id):
            _answer_cb(c, "Not authorized.")
            return
        if data == "adm_panel":
            _show_admin_panel(chat_id)
            _answer_cb(c)
            return
        if data == "adm_dashboard":
            _show_admin_dashboard(chat_id)
            _answer_cb(c)
            return
        if data == "adm_users":
            _show_admin_users(chat_id, page=0)
            _answer_cb(c)
            return
        if data == "adm_jobs":
            _show_admin_jobs(chat_id)
            _answer_cb(c)
            return
        if data == "adm_proxies":
            _show_proxy_dashboard(chat_id)
            _answer_cb(c)
            return
        if data == "adm_broadcast":
            _set_session(u.id, STATE_AWAIT_BROADCAST)
            _safe_send(chat_id, "📢 Type the broadcast message:",
                       markup=_cancel_only_kb())
            _answer_cb(c)
            return
        if data == "adm_channel":
            _show_channel_settings(chat_id)
            _answer_cb(c)
            return
        if data == "adm_settings":
            _show_bot_settings(chat_id)
            _answer_cb(c)
            return
        if data == "adm_maint":
            _confirm_maintenance(chat_id, u.id)
            _answer_cb(c)
            return
        if data == "adm_diag":
            _run_diagnostics(chat_id)
            _answer_cb(c)
            return
        if data == "adm_audit":
            _show_audit_log(chat_id, page=0)
            _answer_cb(c)
            return
        if data == "adm_chanlog":
            _show_channel_post_history(chat_id, page=0)
            _answer_cb(c)
            return
        if data == "adm_pending":
            _show_pending(chat_id, u.id)
            _answer_cb(c)
            return
        if data.startswith("apage_"):
            _show_audit_log(chat_id, page=int(data.split("_")[1]))
            _answer_cb(c)
            return
        if data.startswith("cpage_"):
            _show_channel_post_history(chat_id, page=int(data.split("_")[1]))
            _answer_cb(c)
            return

        # ---- proxy routing ----
        if data.startswith("px_") and not is_admin(u.id):
            _answer_cb(c, "Not authorized.")
            return
        if data == "px_dashboard":
            _show_proxy_dashboard(chat_id)
            _answer_cb(c)
            return
        if data == "px_add":
            _set_session(u.id, STATE_AWAIT_PROXY_ADD)
            _safe_send(
                chat_id,
                ("➕ *ADD PROXY*\n"
                 "━━━━━━━━━━━━━━━━━━━━\n"
                 "Send proxy in any supported format:\n\n"
                 "`http://IP:PORT`\n"
                 "`http://user:pass@IP:PORT`\n"
                 "`socks5://IP:PORT`\n"
                 "`socks5h://user:pass@IP:PORT`\n"
                 "`IP:PORT:USER:PASS`"),
                markup=_back_cancel_kb(),
            )
            _answer_cb(c)
            return
        if data == "px_bulk":
            _set_session(u.id, STATE_AWAIT_PROXY_BULK)
            _safe_send(chat_id,
                       "📦 *BULK ADD*\nSend one proxy per line:",
                       markup=_back_cancel_kb())
            _answer_cb(c)
            return
        if data == "px_list_all":
            _show_proxy_list(chat_id, status_filter=None, page=0)
            _answer_cb(c)
            return
        if data == "px_list_working":
            _show_proxy_list(chat_id, status_filter="WORKING", page=0)
            _answer_cb(c)
            return
        if data == "px_list_dead":
            _show_proxy_list(chat_id, status_filter="TCP_FAILED", page=0)
            _answer_cb(c)
            return
        if data.startswith("pxpage_"):
            parts = data.split("_", 2)
            sf = parts[1] if len(parts) > 2 else None
            sf = None if sf in ("all", "None") else sf
            _show_proxy_list(chat_id, status_filter=sf, page=int(parts[1])
                             if False else int(parts[-1]))
            _answer_cb(c)
            return
        if data == "px_test_all":
            threading.Thread(target=_run_proxy_test,
                              args=(chat_id, "all"), daemon=True).start()
            _answer_cb(c, "Testing all proxies…")
            return
        if data == "px_retest":
            threading.Thread(target=_run_proxy_test,
                              args=(chat_id, "unhealthy"), daemon=True).start()
            _answer_cb(c, "Retesting unhealthy proxies…")
            return
        if data == "px_fetch":
            threading.Thread(target=_run_fetch_proxies,
                              args=(chat_id, u.id), daemon=True).start()
            _answer_cb(c, "Fetching latest proxies…")
            return
        if data == "px_cleanup":
            _confirm_cleanup_dead(chat_id, u.id)
            _answer_cb(c)
            return
        if data.startswith("pxcleanupconf_"):
            _cleanup_dead_proxies(chat_id, u.id)
            _answer_cb(c)
            return
        if data == "px_sources":
            _show_proxy_sources(chat_id)
            _answer_cb(c)
            return
        if data == "px_src_add":
            _set_session(u.id, STATE_AWAIT_PROXY_SRC)
            _safe_send(
                chat_id,
                ("📦 *ADD PROXY SOURCE*\n"
                 "Send the URL of a plaintext proxy list "
                 "(IP:PORT, one per line):\n\n"
                 "_Only add sources you are authorized to use._"),
                markup=_back_cancel_kb(),
            )
            _answer_cb(c)
            return
        if data.startswith("pxsrcdel_"):
            sid = int(data.split("_", 1)[1])
            with _db_lock:
                conn = get_conn()
                try:
                    conn.execute("DELETE FROM proxy_sources WHERE id=?",
                                 (sid,))
                    conn.commit()
                finally:
                    conn.close()
            audit_log(u.id, "PROXY_SOURCE_DELETED", str(sid))
            _show_proxy_sources(chat_id)
            _answer_cb(c)
            return
        if data.startswith("pxdel_"):
            pid = int(data.split("_", 1)[1])
            delete_proxy_db(pid)
            audit_log(u.id, "PROXY_DELETED", str(pid))
            _show_proxy_list(chat_id, status_filter=None, page=0)
            _answer_cb(c)
            return

        # ---- settings toggles ----
        if data.startswith("set_"):
            if not is_admin(u.id):
                _answer_cb(c, "Not authorized.")
                return
            _handle_setting_toggle(chat_id, u.id, data[4:])
            _answer_cb(c)
            return
        if data == "adm_chan_test":
            _test_channel(chat_id)
            _answer_cb(c)
            return
        if data == "adm_chan_set":
            _set_session(u.id, STATE_AWAIT_CHANNEL)
            _safe_send(chat_id,
                       "📡 Send the new channel username (e.g. @mychannel):",
                       markup=_back_cancel_kb())
            _answer_cb(c)
            return
        if data.startswith("usr_search"):
            _set_session(u.id, STATE_AWAIT_SEARCH)
            _safe_send(chat_id, "🔍 Send user ID, username, or name:",
                       markup=_back_cancel_kb())
            _answer_cb(c)
            return
        if data.startswith("upage_"):
            _show_admin_users(chat_id, page=int(data.split("_")[1]))
            _answer_cb(c)
            return
        if data.startswith("udetail_"):
            _show_user_detail_admin(chat_id, int(data.split("_", 1)[1]))
            _answer_cb(c)
            return
        if data.startswith("uhist_"):
            tid = int(data.split("_", 1)[1])
            _show_admin_user_history(chat_id, tid)
            _answer_cb(c)
            return
        if data.startswith("ublock_"):
            tid = int(data.split("_", 1)[1])
            set_user_status(tid, "BLOCKED")
            audit_log(u.id, "USER_BLOCKED", str(tid))
            _show_user_detail_admin(chat_id, tid)
            _answer_cb(c, "Blocked.")
            return
        if data.startswith("uunblock_"):
            tid = int(data.split("_", 1)[1])
            set_user_status(tid, "APPROVED")
            audit_log(u.id, "USER_UNBLOCKED", str(tid))
            _show_user_detail_admin(chat_id, tid)
            _answer_cb(c, "Unblocked.")
            return
        if data.startswith("ajob_"):
            jid = int(data.split("_", 1)[1])
            _show_job_detail(chat_id, jid, admin_view=True, viewer_id=u.id)
            _answer_cb(c)
            return
        if data.startswith("maintconf_"):
            val = data.split("_", 1)[1]
            set_setting("maintenance_mode", val)
            global MAINTENANCE_MODE
            MAINTENANCE_MODE = val == "1"
            audit_log(u.id, "MAINTENANCE_TOGGLE", val)
            _safe_edit(
                chat_id, c.message.message_id,
                f"🛠 Maintenance: `{'ON' if val == '1' else 'OFF'}`",
                markup=_admin_keyboard(),
            )
            _answer_cb(c)
            return

        _answer_cb(c, "Unknown action.")
    except Exception as e:
        log.exception("callback error: %s", e)
        _answer_cb(c, "Error.")


def _handle_back(c: types.CallbackQuery):
    """Centralized Back: return to the most sensible prior screen."""
    u = c.from_user
    chat_id = c.message.chat.id
    session = _get_session(u.id)
    state = session.get("state") if session else None
    if state in (STATE_AWAIT_VISITS, STATE_AWAIT_MODE):
        # back to extraction type choice
        _start_extraction_flow(chat_id, u)
    elif state == STATE_AWAIT_URL:
        _clear_session(u.id)
        _send_home(chat_id, u.id)
    elif state in (STATE_AWAIT_PROXY_ADD, STATE_AWAIT_PROXY_BULK,
                   STATE_AWAIT_PROXY_SRC):
        _clear_session(u.id)
        _show_proxy_dashboard(chat_id)
    elif state in (STATE_AWAIT_BROADCAST, STATE_AWAIT_SEARCH,
                   STATE_AWAIT_CHANNEL):
        _clear_session(u.id)
        _show_admin_panel(chat_id)
    elif state == STATE_AWAIT_BULK:
        _clear_session(u.id)
        _start_extraction_flow(chat_id, u)
    else:
        _clear_session(u.id)
        if is_admin(u.id):
            _show_admin_panel(chat_id)
        else:
            _send_home(chat_id, u.id)# =========================================================
#  Admin views
# =========================================================
def _show_admin_dashboard(chat_id, days: Optional[int] = None):
    d = admin_dashboard_stats(days)
    pc = proxy_pool.count()
    running = sum(1 for st in job_state.values()
                  if not isinstance(st.get("cancel"), threading.Event)
                  or (isinstance(st.get("cancel"), threading.Event)
                      and not st["cancel"].is_set() and not st.get("bulk")))
    period = "All Time" if days is None else f"Last {days}d"
    _safe_send(
        chat_id,
        ("📊 *SYSTEM ANALYTICS*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         f"📅 Period: *{period}*\n\n"
         "👥 Users\n"
         f"Total: `{d['total_users']}`\n"
         f"Active Today: `{d['active_today']}`\n\n"
         "🚀 Jobs\n"
         f"Total: `{d['total_jobs']}`\n"
         f"Successful: `{d['successful_jobs']}`\n"
         f"Failed: `{d['failed_jobs']}`\n"
         f"Today: `{d['jobs_today']}`\n"
         f"Running: `{running}`\n\n"
         "🔗 URLs Processed\n"
         f"Total: `{d['total_urls']}`\n\n"
         "📱 Numbers\n"
         f"Total: `{d['total_numbers']:,}`\n"
         f"Today: `{d['numbers_today']}`\n\n"
         "🌐 Proxies\n"
         f"Total: `{pc['total']}`\n"
         f"🟢 Working: `{pc['working']}`\n"
         f"🟡 Slow: `{pc['slow']}`\n"
         f"🔴 Dead: `{pc['dead']}`\n"
         f"⚪ Untested: `{pc['untested']}`\n\n"
         f"⚡ Avg Job Time: `{_fmt_duration(int(d['avg_duration']))}`\n"
         "━━━━━━━━━━━━━━━━━━━━"),
        markup=types.InlineKeyboardMarkup(row_width=2).add(
            types.InlineKeyboardButton("Today", callback_data="adm_dash_1"),
            types.InlineKeyboardButton("7 Days", callback_data="adm_dash_7"),
            types.InlineKeyboardButton("30 Days", callback_data="adm_dash_30"),
            types.InlineKeyboardButton("All Time", callback_data="adm_dashboard"),
            types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"),
        ),
    )


def _show_admin_users(chat_id, page=0):
    users, total = recent_users(limit=10, page=page)
    if not users:
        _safe_send(chat_id, "👥 No users found.",
                   markup=_admin_keyboard())
        return
    per_page = 10
    lines = [f"👥 *USERS* (page {page+1}/{(total+per_page-1)//per_page})",
             "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    for u in users:
        st_icon = {"APPROVED": "🟢", "PENDING": "🟡",
                   "BLOCKED": "🔴"}.get(u["status"], "⚪")
        lines.append(
            f"{st_icon} `{u['user_id']}` "
            f"{md_esc(u['first_name'] or '—')} "
            f"@{md_esc(u['username'] or '—')}"
        )
        mk.add(types.InlineKeyboardButton(
            f"{u['user_id']}", callback_data=f"udetail_{u['user_id']}"))
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("◀️ Prev",
                                              callback_data=f"upage_{page-1}"))
    if (page + 1) * per_page < total:
        nav.append(types.InlineKeyboardButton("▶️ Next",
                                              callback_data=f"upage_{page+1}"))
    if nav:
        mk.row(*nav)
    mk.add(types.InlineKeyboardButton("🔍 Search", callback_data="usr_search"))
    mk.add(types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


def _handle_user_search(chat_id, admin_id, query):
    users = search_users(query, limit=15)
    if not users:
        _safe_send(chat_id, "🔍 No matches.", markup=_admin_keyboard())
        return
    lines = ["🔍 *SEARCH RESULTS*", "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    for u in users:
        lines.append(f"`{u['user_id']}` {md_esc(u['first_name'] or '—')} "
                     f"@{md_esc(u['username'] or '—')}")
        mk.add(types.InlineKeyboardButton(
            f"{u['user_id']}", callback_data=f"udetail_{u['user_id']}"))
    mk.add(types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


def _show_user_detail_admin(chat_id, target_id):
    u = get_user(target_id)
    if not u:
        _safe_send(chat_id, "User not found.", markup=_admin_keyboard())
        return
    jobs, total = user_jobs(target_id, limit=200)
    succ = sum(1 for j in jobs if j["status"] == "COMPLETED")
    fail = sum(1 for j in jobs if j["status"] == "FAILED")
    last = (jobs[0]["started_at"] or "")[:16] if jobs else "—"
    _safe_send(
        chat_id,
        ("👤 *USER DETAILS*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         f"Name: {md_esc(u['first_name'] or '—')}\n"
         f"Username: @{md_esc(u['username'] or '—')}\n"
         f"Telegram ID: `{u['user_id']}`\n"
         f"Status: `{u['status']}`\n"
         f"Plan: `{u.get('plan', 'FREE')}`\n\n"
         "📊 Statistics\n"
         f"Total Jobs: `{total}`\n"
         f"Successful: `{succ}`\n"
         f"Failed: `{fail}`\n"
         f"URLs Processed: `{u.get('total_urls_processed', 0)}`\n"
         f"Unique Numbers: `{u['total_numbers_found']}`\n\n"
         f"🕒 Joined: `{(u['joined_at'] or '')[:16]}`\n"
         f"🕒 Last Active: `{(u['last_active'] or '')[:16]}`\n"
         f"🕒 Latest Job: `{last}`\n"
         "━━━━━━━━━━━━━━━━━━━━"),
        markup=_user_action_keyboard(target_id, u["status"]),
    )


def _user_action_keyboard(target_id, status):
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("📋 History",
                                   callback_data=f"uhist_{target_id}"),
        types.InlineKeyboardButton("🚫 Block",
                                   callback_data=f"ublock_{target_id}")
        if status != "BLOCKED"
        else types.InlineKeyboardButton("✅ Unblock",
                                        callback_data=f"uunblock_{target_id}"),
    )
    if status == "PENDING":
        mk.add(types.InlineKeyboardButton("✅ Approve",
                                          callback_data=f"appr_{target_id}"))
    mk.add(types.InlineKeyboardButton("◀️ Users",
                                      callback_data="adm_users"))
    return mk


def _show_admin_user_history(chat_id, target_id):
    jobs, _ = user_jobs(target_id, limit=10)
    if not jobs:
        _safe_send(chat_id, "No jobs.", markup=_admin_keyboard())
        return
    lines = [f"📋 *USER HISTORY* (`{target_id}`)", "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    for j in jobs:
        host = urllib.parse.urlparse(j["source_url"]).hostname or "?"
        mode = "🌐" if j["mode"] == "IP_ROTATION" else "🟢"
        st = {"COMPLETED": "✅", "FAILED": "❌", "CANCELLED": "🛑",
              "INTERRUPTED": "⚠️"}.get(j["status"], "⚪")
        lines.append(
            f"{st} `{j['job_code']}` {mode} `{md_esc(host)}` "
            f"📱`{j['unique_numbers']}`"
        )
        mk.add(types.InlineKeyboardButton(
            f"{j['job_code']}", callback_data=f"ajob_{j['job_id']}"))
    mk.add(types.InlineKeyboardButton("◀️ Back",
                                      callback_data=f"udetail_{target_id}"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


def _show_admin_jobs(chat_id):
    jobs = recent_jobs(limit=15)
    if not jobs:
        _safe_send(chat_id, "🚀 No extraction jobs.", markup=_admin_keyboard())
        return
    lines = ["🚀 *EXTRACTION LOGS*", "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    for j in jobs:
        host = urllib.parse.urlparse(j["source_url"]).hostname or "?"
        mode = "🌐" if j["mode"] == "IP_ROTATION" else "🟢"
        st = {"COMPLETED": "✅", "FAILED": "❌", "CANCELLED": "🛑",
              "INTERRUPTED": "⚠️", "RUNNING": "⏳"}.get(j["status"], "⚪")
        ts = (j["started_at"] or "")[:16]
        lines.append(
            f"{st} `{j['job_code']}` {mode} `{ts}`\n"
            f"👤 @{md_esc(j['username'] or '—')} | "
            f"🔗 `{md_esc(host)}` | 📱 `{j['unique_numbers']}`"
        )
        mk.add(types.InlineKeyboardButton(
            f"{j['job_code']}", callback_data=f"ajob_{j['job_id']}"))
    mk.add(types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


# =========================================================
#  Proxy Center views
# =========================================================
def _show_proxy_dashboard(chat_id):
    pc = proxy_pool.count()
    avg_q = "SELECT COALESCE(AVG(average_latency),0) a FROM proxies " \
            "WHERE health_status IN ('WORKING','SLOW')"
    with _db_lock:
        conn = get_conn()
        try:
            avg_lat = conn.execute(avg_q).fetchone()["a"]
            succ_q = ("SELECT COALESCE(SUM(success_count),0) s, "
                      "COALESCE(SUM(success_count+failure_count),0) t "
                      "FROM proxies")
            row = conn.execute(succ_q).fetchone()
            rate = (row["s"] / row["t"] * 100) if row["t"] else 0
        finally:
            conn.close()
    last_test = "—"
    _safe_send(
        chat_id,
        ("🌐 *PROXY CENTER*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         f"📊 Total: `{pc['total']}`\n"
         f"🟢 Healthy: `{pc['working']}`\n"
         f"🟡 Slow: `{pc['slow']}`\n"
         f"🔵 Connected: `{pc['connected']}`\n"
         f"🔴 Dead: `{pc['dead']}`\n"
         f"⚪ Untested: `{pc['untested']}`\n\n"
         f"⚡ Avg Latency: `{int(avg_lat)}ms`\n"
         f"📊 Success Rate: `{rate:.1f}%`\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"🧪 SOCKS5: `{'✅' if _SOCKS_OK else '❌ install PySocks'}`\n"
         "━━━━━━━━━━━━━━━━━━━━"),
        markup=_proxy_center_keyboard(),
    )


def _show_proxy_list(chat_id, status_filter=None, page=0):
    rows, total = list_proxies(limit=10, status_filter=status_filter, page=page)
    if not rows:
        _safe_send(chat_id, "No proxies found.",
                   markup=_proxy_center_keyboard())
        return
    title = f"📋 *PROXY LIST* ({status_filter or 'all'})"
    lines = [title, "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    icons = {"WORKING": "🟢", "SLOW": "🟡", "CONNECTED": "🔵",
             "TARGET_FAILED": "🟠", "AUTH_FAILED": "🟣",
             "TCP_FAILED": "🔴", "INVALID": "⚫", "UNTESTED": "⚪"}
    for r in rows:
        ic = icons.get(r["health_status"], "⚪")
        lines.append(
            f"{ic} #{r['id']} {r['protocol'].upper()} "
            f"`{md_esc(r['host'])}:{r['port']}`\n"
            f"   Latency: `{r['average_latency']}ms` | "
            f"Score: `{r['health_score']}` | "
            f"S: `{r['success_count']}` F: `{r['failure_count']}`"
        )
        mk.add(types.InlineKeyboardButton(
            f"🗑 #{r['id']}", callback_data=f"pxdel_{r['id']}"))
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("◀️ Prev",
            callback_data=f"pxpage_{status_filter or 'all'}_{page-1}"))
    if (page + 1) * 10 < total:
        nav.append(types.InlineKeyboardButton("▶️ Next",
            callback_data=f"pxpage_{status_filter or 'all'}_{page+1}"))
    if nav:
        mk.row(*nav)
    mk.add(types.InlineKeyboardButton("◀️ Proxy Center",
                                      callback_data="adm_proxies"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


def _handle_proxy_add(chat_id, admin_id, text):
    p = parse_proxy(text)
    if not p:
        _safe_send(chat_id, "❌ Invalid proxy format.",
                   markup=_proxy_center_keyboard())
        return
    pid = add_proxy_db(text)
    if pid is None:
        _safe_send(chat_id, "❌ Could not add.",
                   markup=_proxy_center_keyboard())
        return
    audit_log(admin_id, "PROXY_ADDED", str(pid), _mask(text))
    res = test_proxy(p)
    update_proxy_health(pid, res)
    _safe_send(
        chat_id,
        (f"✅ Proxy `#{pid}` added.\n"
         f"Status: `{res['status']}`\n"
         f"Latency: `{res['latency_ms']}ms`\n"
         f"Exit IP: `{res['exit_ip'] or '—'}`"),
        markup=_proxy_center_keyboard(),
    )


def _handle_proxy_bulk(chat_id, admin_id, text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    added = failed = 0
    for ln in lines:
        pid = add_proxy_db(ln)
        if pid is not None:
            added += 1
        else:
            failed += 1
    audit_log(admin_id, "PROXY_BULK_ADD", f"{added} added, {failed} failed")
    _safe_send(
        chat_id,
        f"📦 Bulk add: `{added}` added, `{failed}` failed.\n"
        f"Run *Test All* to verify them.",
        markup=_proxy_center_keyboard(),
    )


def _run_proxy_test(chat_id, scope="all"):
    msg = bot.send_message(chat_id, "🧪 *Testing proxies…* `0%`",
                           parse_mode="Markdown")
    cancel_event = threading.Event()

    def cb(tested, total, working, slow, dead):
        pct = int((tested / total) * 100) if total else 0
        _safe_edit(
            chat_id, msg.message_id,
            ("🧪 *PROXY HEALTH CHECK*\n"
             "━━━━━━━━━━━━━━━━━━━━\n"
             f"Progress: `{_bar(pct)} {pct}%`\n\n"
             f"Tested: `{tested}/{total}`\n"
             f"🟢 Working: `{working}`\n"
             f"🟡 Slow: `{slow}`\n"
             f"🔴 Failed: `{dead}`\n"
             "━━━━━━━━━━━━━━━━━━━━"),
        )

    result = bulk_test_proxies(progress_cb=cb, cancel_event=cancel_event,
                               scope=scope)
    _safe_edit(
        chat_id, msg.message_id,
        ("✅ *PROXY TEST COMPLETE*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         f"Tested: `{result['tested']}`\n"
         f"🟢 Working: `{result['working']}`\n"
         f"🟡 Slow: `{result['slow']}`\n"
         f"🔴 Dead: `{result['dead']}`\n"
         "━━━━━━━━━━━━━━━━━━━━"),
    )
    _show_proxy_dashboard(chat_id)


def _run_fetch_proxies(chat_id, admin_id):
    msg = bot.send_message(chat_id, "⚡ *Fetching latest proxies…*",
                           parse_mode="Markdown")
    cancel_event = threading.Event()

    def cb(sources, discovered, parsed, imported, tested, total,
           working, slow, dead):
        _safe_edit(
            chat_id, msg.message_id,
            ("⚡ *FETCHING PROXIES*\n"
             "━━━━━━━━━━━━━━━━━━━━\n"
             f"Sources: `{sources}`\n"
             f"Discovered: `{discovered}`\n"
             f"Parsed: `{parsed}`\n"
             f"Imported: `{imported}`\n\n"
             f"Testing: `{tested}/{total}`\n"
             f"🟢 Working: `{working}`\n"
             f"🟡 Slow: `{slow}`\n"
             f"🔴 Failed: `{dead}`\n"
             "━━━━━━━━━━━━━━━━━━━━"),
        )

    try:
        result = fetch_latest_proxies(progress_cb=cb, cancel_event=cancel_event)
        audit_log(admin_id, "PROXY_FETCH",
                  f"sources={result['sources']} "
                  f"working={result['working']}")
        _safe_edit(
            chat_id, msg.message_id,
            ("⚡ *FETCH COMPLETE*\n"
             "━━━━━━━━━━━━━━━━━━━━\n"
             f"📥 Sources: `{result['sources']}`\n"
             f"Discovered: `{result['discovered']}`\n"
             f"Parsed: `{result['parsed']}`\n"
             f"➕ Imported: `{result['imported']}`\n\n"
             f"🧪 Tested: `{result['tested']}`\n"
             f"🟢 Working: `{result['working']}`\n"
             f"🟡 Slow: `{result['slow']}`\n"
             f"🔴 Failed: `{result['dead']}`\n"
             "━━━━━━━━━━━━━━━━━━━━"),
        )
    except Exception as e:
        _safe_edit(chat_id, msg.message_id,
                   f"❌ Fetch failed: `{str(e)[:80]}`")
    _show_proxy_dashboard(chat_id)


def _confirm_cleanup_dead(chat_id, admin_id):
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("✅ Yes, Clean",
                                   callback_data="pxcleanupconf_1"),
        types.InlineKeyboardButton("❌ No", callback_data="adm_proxies"),
    )
    _safe_send(
        chat_id,
        ("⚠️ *CLEANUP DEAD PROXIES?*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "Removes proxies marked TCP_FAILED / AUTH_FAILED / INVALID "
         "with 5+ consecutive failures.\n\n"
         "This cannot be undone."),
        markup=mk,
    )


def _cleanup_dead_proxies(chat_id, admin_id):
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                "DELETE FROM proxies WHERE health_status IN "
                "('TCP_FAILED','AUTH_FAILED','INVALID') "
                "AND consecutive_failures >= 5"
            )
            n = cur.rowcount
            conn.commit()
        finally:
            conn.close()
    audit_log(admin_id, "PROXY_CLEANUP", f"removed {n}")
    _safe_send(chat_id, f"🗑 Removed `{n}` dead proxies.",
               markup=_proxy_center_keyboard())


def _show_proxy_sources(chat_id):
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM proxy_sources ORDER BY id"
            ).fetchall()
        finally:
            conn.close()
    mk = types.InlineKeyboardMarkup()
    lines = ["📦 *PROXY SOURCES*", "━━━━━━━━━━━━━━━━━━━━"]
    if not rows:
        lines.append("No sources configured.")
        lines.append("")
        lines.append("_Add a plaintext proxy-list URL to enable "
                     "Fetch Latest._")
    else:
        for r in rows:
            lines.append(f"#{r['id']} `{md_esc(_short(r['url'], 50))}`")
            mk.add(types.InlineKeyboardButton(
                f"🗑 #{r['id']}", callback_data=f"pxsrcdel_{r['id']}"))
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    mk.add(types.InlineKeyboardButton("➕ Add Source",
                                      callback_data="px_src_add"))
    mk.add(types.InlineKeyboardButton("◀️ Proxy Center",
                                      callback_data="adm_proxies"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


def _handle_proxy_source_add(chat_id, admin_id, text):
    url = text.strip()
    if not url.startswith(("http://", "https://")):
        _safe_send(chat_id, "❌ Send a valid http(s) URL.",
                   markup=_proxy_center_keyboard())
        return
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO proxy_sources(name, url) VALUES(?, ?)",
                (_short(url, 60), url),
            )
            conn.commit()
        finally:
            conn.close()
    audit_log(admin_id, "PROXY_SOURCE_ADDED", _mask(url))
    _show_proxy_sources(chat_id)


# =========================================================
#  Channel settings & post history
# =========================================================
def _show_channel_settings(chat_id):
    cfg = get_settings_batch([
        "channel_logging", "channel_username", "channel_include_username",
        "channel_include_uid", "channel_include_method",
        "channel_include_numbers", "channel_include_source",
        "channel_attach_txt", "show_duration", "show_job_id",
    ])
    yn = lambda v: "✅ ON" if v == "1" else "❌ OFF"
    text = (
        "📡 *CHANNEL SETTINGS*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Channel: `{md_esc(cfg['channel_username'] or '—')}`\n"
        f"Auto-post: {yn(cfg['channel_logging'])}\n"
        f"Include username: {yn(cfg['channel_include_username'])}\n"
        f"Include UID: {yn(cfg['channel_include_uid'])}\n"
        f"Include method: {yn(cfg['channel_include_method'])}\n"
        f"Include source: {yn(cfg['channel_include_source'])}\n"
        f"Include numbers: {yn(cfg['channel_include_numbers'])}\n"
        f"Show duration: {yn(cfg['show_duration'])}\n"
        f"Show job ID: {yn(cfg['show_job_id'])}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton(f"Auto-post: {yn(cfg['channel_logging'])}",
                                   callback_data="set_channel_logging"),
        types.InlineKeyboardButton("🧪 Test Channel",
                                   callback_data="adm_chan_test"),
        types.InlineKeyboardButton("✏️ Set Channel",
                                   callback_data="adm_chan_set"),
        types.InlineKeyboardButton("◀️ Admin",
                                   callback_data="adm_panel"),
    )
    _safe_send(chat_id, text, markup=mk)


def _test_channel(chat_id):
    ch = get_setting("channel_username", DEFAULT_CHANNEL)
    if not ch:
        _safe_send(chat_id, "❌ No channel configured.",
                   markup=_admin_keyboard())
        return
    try:
        msg = bot.send_message(ch, "🧪 *Channel test*\nBot can post here ✅",
                               parse_mode="Markdown")
        _safe_send(chat_id,
                   f"✅ Channel verified. Message ID: `{msg.message_id}`",
                   markup=_admin_keyboard())
    except Exception as e:
        _safe_send(
            chat_id,
            ("❌ *CHANNEL POSTING FAILED*\n"
             f"Reason: {md_esc(str(e)[:120])}\n\n"
             "Make sure the bot is added as admin to the channel "
             "with post permission."),
            markup=_admin_keyboard(),
        )


def _show_channel_post_history(chat_id, page=0):
    rows, total = channel_post_rows(limit=10, page=page)
    if not rows:
        _safe_send(chat_id, "📡 No channel posts yet.",
                   markup=_admin_keyboard())
        return
    lines = ["📡 *POST HISTORY*", "━━━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        st = {"OK": "✅", "FAILED": "❌"}.get(r["status"], "⚪")
        code = r.get("job_code") or f"#{r['job_id']}"
        lines.append(f"{st} `{code}` `{(r['posted_at'] or '')[:16]}`")
        if r["error"]:
            lines.append(f"   err: {md_esc(r['error'][:60])}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    mk = types.InlineKeyboardMarkup()
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("◀️ Prev",
                                              callback_data=f"cpage_{page-1}"))
    if (page + 1) * 10 < total:
        nav.append(types.InlineKeyboardButton("▶️ Next",
                                              callback_data=f"cpage_{page+1}"))
    if nav:
        mk.row(*nav)
    mk.add(types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


# =========================================================
#  Bot settings, maintenance, audit log, broadcast
# =========================================================
def _show_bot_settings(chat_id):
    cfg = get_settings_batch([
        "maintenance_mode", "approval_mode", "proxy_enabled", "max_visits",
        "progress_interval", "support_username", "admin_display_name",
    ])
    yn = lambda v: "✅ ON" if v == "1" else "❌ OFF"
    text = (
        "⚙️ *SYSTEM SETTINGS*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Maintenance: {yn(cfg['maintenance_mode'])}\n"
        f"Approval mode: {yn(cfg['approval_mode'])}\n"
        f"Proxy enabled: {yn(cfg['proxy_enabled'])}\n"
        f"Max visits: `{cfg['max_visits']}`\n"
        f"Progress interval: `{cfg['progress_interval']}s`\n"
        f"Support username: @{md_esc(cfg['support_username'] or '—')}\n"
        f"Admin display: {md_esc(cfg['admin_display_name'])}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton(f"Maintenance: {yn(cfg['maintenance_mode'])}",
                                   callback_data="set_maintenance_mode"),
        types.InlineKeyboardButton(f"Approval: {yn(cfg['approval_mode'])}",
                                   callback_data="set_approval_mode"),
        types.InlineKeyboardButton(f"Proxy: {yn(cfg['proxy_enabled'])}",
                                   callback_data="set_proxy_enabled"),
        types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"),
    )
    _safe_send(chat_id, text, markup=mk)


def _handle_setting_toggle(chat_id, admin_id, key):
    cur = get_setting(key, "0")
    new = "0" if cur == "1" else "1"
    set_setting(key, new)
    if key == "maintenance_mode":
        global MAINTENANCE_MODE
        MAINTENANCE_MODE = new == "1"
    audit_log(admin_id, "SETTING_TOGGLE", f"{key}={new}")
    if key.startswith("channel") or key.startswith("show_"):
        _show_channel_settings(chat_id)
    else:
        _show_bot_settings(chat_id)


def _confirm_maintenance(chat_id, admin_id):
    cur = get_setting("maintenance_mode", "0")
    new = "1" if cur != "1" else "0"
    action = "ENABLE" if new == "1" else "DISABLE"
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton(f"✅ Yes, {action}",
                                   callback_data=f"maintconf_{new}"),
        types.InlineKeyboardButton("❌ No", callback_data="adm_panel"),
    )
    _safe_send(
        chat_id,
        (f"⚠️ *{action} MAINTENANCE MODE?*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "When enabled, non-admin users see a maintenance screen.\n"
         "Admins continue to have full access."),
        markup=mk,
    )


def _show_audit_log(chat_id, page=0):
    rows, total = audit_log_rows(limit=10, page=page)
    if not rows:
        _safe_send(chat_id, "📜 Audit log is empty.",
                   markup=_admin_keyboard())
        return
    lines = ["📜 *AUDIT LOG*", "━━━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        lines.append(
            f"`{(r['timestamp'] or '')[:16]}` "
            f"admin `{r['admin_id']}`\n"
            f"   {md_esc(r['action'])} → {md_esc(r['target'] or '—')}"
        )
        if r["details"]:
            lines.append(f"   _{md_esc(r['details'][:60])}_")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    mk = types.InlineKeyboardMarkup()
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("◀️ Prev",
                                              callback_data=f"apage_{page-1}"))
    if (page + 1) * 10 < total:
        nav.append(types.InlineKeyboardButton("▶️ Next",
                                              callback_data=f"apage_{page+1}"))
    if nav:
        mk.row(*nav)
    mk.add(types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


def _show_pending(chat_id, admin_id):
    users = pending_users()
    if not users:
        _safe_send(chat_id, "✅ No pending users.", markup=_admin_keyboard())
        return
    mk = types.InlineKeyboardMarkup(row_width=2)
    lines = ["⏳ *PENDING USERS*", "━━━━━━━━━━━━━━━━━━━━"]
    for u in users[:15]:
        lines.append(f"`{u['user_id']}` "
                     f"{md_esc(u['first_name'] or '—')} "
                     f"@{md_esc(u['username'] or '—')}")
        mk.add(
            types.InlineKeyboardButton(f"✅ {u['user_id']}",
                                        callback_data=f"appr_{u['user_id']}"),
            types.InlineKeyboardButton(f"❌ {u['user_id']}",
                                        callback_data=f"rej_{u['user_id']}"),
        )
    mk.add(types.InlineKeyboardButton("◀️ Admin", callback_data="adm_panel"))
    _safe_send(chat_id, "\n".join(lines), markup=mk)


def _do_broadcast(chat_id, admin_id, text):
    users = all_user_ids()
    if not users:
        _safe_send(chat_id, "📢 No users to broadcast to.",
                   markup=_admin_keyboard())
        return
    msg = bot.send_message(
        chat_id,
        f"📢 *Broadcasting to `{len(users)}` users…* `0%`",
        parse_mode="Markdown",
    )
    sent = failed = 0
    total = len(users)
    for i, uid in enumerate(users, 1):
        try:
            bot.send_message(uid, text)
            sent += 1
        except ApiTelegramException as e:
            low = str(e).lower()
            if "retry after" in low or "429" in low:
                m = re.search(r"retry after (\d+)", low)
                time.sleep(int(m.group(1)) + 1 if m else 2)
                try:
                    bot.send_message(uid, text)
                    sent += 1
                    continue
                except Exception:
                    pass
            failed += 1
        except Exception:
            failed += 1
        if i % 25 == 0 or i == total:
            _safe_edit(
                chat_id, msg.message_id,
                (f"📢 *Broadcasting…*\n"
                 f"Progress: `{int((i/total)*100)}%`\n"
                 f"Sent: `{sent}` | Failed: `{failed}`"),
            )
        time.sleep(0.05)
    audit_log(admin_id, "BROADCAST_SENT", f"sent={sent} failed={failed}")
    _safe_send(
        chat_id,
        f"✅ *Broadcast complete*\nSent: `{sent}`\nFailed: `{failed}`",
        markup=_admin_keyboard(),
    )


# =========================================================
#  Diagnostics  (no real channel ping unless explicit)
# =========================================================
def _run_diagnostics(chat_id):
    msg = bot.send_message(chat_id, "🩺 *Running diagnostics…*",
                           parse_mode="Markdown")
    results = []
    # Telegram API
    try:
        me = bot.get_me()
        results.append(("Telegram API", f"✅ @{me.username}"))
    except Exception as e:
        results.append(("Telegram API", f"❌ {str(e)[:40]}"))
    # Database
    try:
        with _db_lock:
            conn = get_conn()
            conn.execute("SELECT 1").fetchone()
            conn.close()
        results.append(("Database", "✅ Healthy"))
    except Exception as e:
        results.append(("Database", f"❌ {str(e)[:40]}"))
    # Direct HTTP
    try:
        r = requests.get("https://api.ipify.org?format=text", timeout=8)
        results.append(("Direct HTTP", "✅" if r.status_code == 200 else "❌"))
    except Exception:
        results.append(("Direct HTTP", "❌"))
    # Proxy parser
    results.append(("Proxy parser",
                    "✅" if parse_proxy("socks5://1.2.3.4:1080") else "❌"))
    # SOCKS5
    results.append(("SOCKS5 support", "✅" if _SOCKS_OK else "❌"))
    # Proxy pool
    pc = proxy_pool.count()
    results.append((f"Proxy pool (working={pc['working']})",
                    "✅" if pc["total"] > 0 else "⚠️ none"))
    # Channel config (no ping)
    ch = get_setting("channel_username", DEFAULT_CHANNEL)
    results.append(("Channel config",
                    f"✅ {ch}" if ch else "⚠️ not set"))
    # Job state
    with _state_lock:
        n_running = len([s for s in job_state.values()
                         if isinstance(s.get("cancel"), threading.Event)
                         and not s["cancel"].is_set()])
    results.append((f"Running jobs ({n_running})", "✅"))
    # Environment
    results.append(("BOT_TOKEN env", "✅ set" if BOT_TOKEN else "❌ missing"))
    results.append(("ADMIN_IDS",
                    f"✅ {len(ADMIN_IDS)} admin(s)" if ADMIN_IDS else "⚠️ none"))

    lines = ["🩺 *SYSTEM DIAGNOSTICS*", "━━━━━━━━━━━━━━━━━━━━"]
    for name, st in results:
        lines.append(f"{st} {md_esc(name)}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    _safe_edit(chat_id, msg.message_id, "\n".join(lines),
               markup=_admin_keyboard())


# =========================================================
#  Background proxy retest scheduler
# =========================================================
def _retest_loop():
    while True:
        time.sleep(300)
        try:
            rows, _ = list_proxies(limit=500, status_filter="UNTESTED")
            rows2, _ = list_proxies(limit=200, status_filter="TCP_FAILED")
            rows += rows2
            if not rows:
                continue
            log.info("RETEST_LOOP testing %d proxies", len(rows))
            # concurrent retest (bounded)
            with ThreadPoolExecutor(max_workers=PROXY_TEST_CONCURRENCY) as ex:
                def _one(r):
                    p = {"protocol": r["protocol"], "host": r["host"],
                         "port": r["port"], "username": r["username"],
                         "password": r["password"], "endpoint": r["endpoint"]}
                    res = test_proxy(p)
                    update_proxy_health(r["id"], res)
                list(ex.map(_one, rows[:80]))
        except Exception as e:
            log.warning("retest loop error: %s", e)


# =========================================================
#  Startup
# =========================================================
def startup_self_check():
    log.info("Bot starting…")
    checks = []
    try:
        me = bot.get_me()
        checks.append(f"✅ Telegram API: @{me.username}")
    except Exception as e:
        checks.append(f"❌ Telegram API: {e}")
    try:
        init_db()
        seed_settings()
        checks.append("✅ Database initialized")
    except Exception as e:
        checks.append(f"❌ Database: {e}")
        return checks
    recovered = recover_interrupted_jobs()
    if recovered:
        checks.append(f"✅ Recovered {recovered} interrupted job(s)")
    try:
        with _db_lock:
            conn = get_conn()
            try:
                conn.execute("SELECT COUNT(*) c FROM proxies").fetchone()
            finally:
                conn.close()
        checks.append("✅ Proxy Manager")
    except Exception as e:
        checks.append(f"❌ Proxy Manager: {e}")
    checks.append(f"{'✅' if _SOCKS_OK else '❌'} SOCKS5 support")
    ch = get_setting("channel_username", DEFAULT_CHANNEL)
    checks.append(f"📡 Channel: {ch or '(not set)'}")
    global MAINTENANCE_MODE
    MAINTENANCE_MODE = get_setting("maintenance_mode", "0") == "1"
    checks.append(
        f"✅ Configuration loaded "
        f"(maintenance={'ON' if MAINTENANCE_MODE else 'OFF'})"
    )
    for c in checks:
        log.info("STARTUP %s", c)
    return checks


def main():
    checks = startup_self_check()
    # bootstrap owner in DB if missing
    if BOOTSTRAP_OWNER_ID:
        with _db_lock:
            conn = get_conn()
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO admins(user_id, role, is_active) "
                    "VALUES(?, 'OWNER', 1)",
                    (BOOTSTRAP_OWNER_ID,),
                )
                conn.commit()
            finally:
                conn.close()
    # start background retester
    threading.Thread(target=_retest_loop, daemon=True).start()
    log.info("Polling started.")
    try:
        bot.infinity_polling(timeout=30, long_polling_timeout=20,
                            skip_pending=True)
    except KeyboardInterrupt:
        log.info("Shutdown requested.")
    except Exception as e:
        log.exception("Polling crashed: %s", e)


if __name__ == "__main__":
    main()
