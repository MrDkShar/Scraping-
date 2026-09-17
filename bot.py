import concurrent.futures
import html
import http.cookiejar
import io
import json
import logging
import os
import re
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Optional

# ── Third-party ──────────────────────────────────────────────────────────────
import requests
import requests.exceptions
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

try:
    import socks  # noqa: F401  — required by requests[socks] for SOCKS5
    _SOCKS5_AVAILABLE = True
except ImportError:
    _SOCKS5_AVAILABLE = False


# =========================================================
# Logging
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bot")


def _scrub(msg: str) -> str:
    """Strip anything that looks like credentials from log strings."""
    return re.sub(r"(://)[^@/\s]+@", r"\1****:****@", str(msg))


# =========================================================
# Configuration (environment-driven; secrets never hardcoded)
# =========================================================
BOT_TOKEN = (os.environ.get("BOT_TOKEN") or "8553353076:AAFgLdPCaSL_TfZds10qQS1_Hr5iGnn0e5M").strip()
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN environment variable is not set. Exiting.")
    sys.exit(1)

DB_FILE = os.environ.get("DATABASE_PATH", "bot_database.db")

# Bootstrap admins from env (numeric Telegram IDs only). The DB is the
# source of truth at runtime; env is the initial seed.
ADMIN_IDS: list[int] = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
]
OWNER_IDS: list[int] = [
    int(x.strip())
    for x in os.environ.get("OWNER_IDS", os.environ.get("ADMIN_IDS", "8753914631")).split(",")
    if x.strip().isdigit()
]

REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "12"))
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "6"))
READ_TIMEOUT = float(os.environ.get("READ_TIMEOUT", "10"))
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "4"))
MAX_RESPONSE_SIZE = int(os.environ.get("MAX_RESPONSE_SIZE", str(5 * 1024 * 1024)))
MAX_URL_LENGTH = int(os.environ.get("MAX_URL_LENGTH", "2048"))
MAX_REDIRECTS = int(os.environ.get("MAX_REDIRECTS", "10"))
MAX_VISITS_PER_JOB = int(os.environ.get("MAX_VISITS_PER_JOB", "100"))
PROGRESS_INTERVAL = float(os.environ.get("PROGRESS_INTERVAL", "1.0"))

RAW_PROXY_ENV = (
    os.environ.get("PROXY_ENDPOINTS", "")
    or os.environ.get("ROTATING_PROXIES", "")
    or ""
)

DEFAULT_CHANNEL = os.environ.get("CHANNEL_USERNAME", "@HshDkSharmaBotsmall").strip()
DEFAULT_ADMIN_DISPLAY = os.environ.get("ADMIN_DISPLAY_USERNAME", "Admin").strip()
DEFAULT_SUPPORT_USERNAME = os.environ.get("SUPPORT_USERNAME", "").strip()
DEFAULT_BOT_NAME = os.environ.get("BOT_NAME", "DK Scraping Bot").strip()

# Multi-endpoint exit-IP verification. A proxy is considered WORKING if ANY
# endpoint returns a public IP; it is only DEAD if the transport itself fails.
_IP_CHECK_URLS = [
    "https://api.ipify.org?format=json",
    "https://httpbin.org/ip",
    "http://ip-api.com/json/?fields=query",
]
# Plain HTTP targets used to confirm the proxy can actually carry a request
# even when every IP-info endpoint happens to be down.
_TRANSPORT_PROBE_URLS = [
    "https://www.example.com",
    "https://www.bing.com",
]

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ── Runtime state ─────────────────────────────────────────
user_states: dict[int, dict] = {}
active_jobs: dict[int, "JobContext"] = {}
_state_lock = threading.Lock()
_db_lock = threading.Lock()
_running_locks: dict[int, threading.Lock] = {}


def _user_lock(user_id: int) -> threading.Lock:
    with _state_lock:
        lk = _running_locks.get(user_id)
        if lk is None:
            lk = threading.Lock()
            _running_locks[user_id] = lk
        return lk


# =========================================================
# Database
# =========================================================
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return any(r[1] == col for r in cur.fetchall())
    except Exception:
        return False


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _add_col(conn, table: str, col: str, decl: str) -> None:
    if not _col_exists(conn, table, col):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        except Exception as exc:
            logger.warning("migrate %s.%s: %s", table, col, exc)


def init_db() -> None:
    """Safe migration: never drop existing tables/data; only add columns/tables."""
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
                    successful_extractions INTEGER DEFAULT 0,
                    failed_extractions    INTEGER DEFAULT 0,
                    joined_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_extraction_at    TIMESTAMP,
                    status                TEXT DEFAULT 'APPROVED',
                    blocked_reason        TEXT
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
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint         TEXT UNIQUE NOT NULL,
                    added_by         INTEGER,
                    is_active        INTEGER DEFAULT 1,
                    last_tested      TIMESTAMP,
                    last_success     TIMESTAMP,
                    last_failure     TIMESTAMP,
                    success_count    INTEGER DEFAULT 0,
                    failure_count    INTEGER DEFAULT 0,
                    consecutive_failures INTEGER DEFAULT 0,
                    consecutive_successes INTEGER DEFAULT 0,
                    average_latency  REAL DEFAULT 0,
                    last_error       TEXT,
                    last_observed_ip TEXT,
                    cooldown_until   TIMESTAMP,
                    health_status    TEXT DEFAULT 'UNTESTED',
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS extraction_jobs (
                    job_id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id           INTEGER,
                    username          TEXT,
                    source_url        TEXT,
                    mode              TEXT,
                    requested_visits  INTEGER,
                    successful_visits INTEGER DEFAULT 0,
                    failed_visits     INTEGER DEFAULT 0,
                    unique_numbers    INTEGER DEFAULT 0,
                    duplicate_numbers INTEGER DEFAULT 0,
                    started_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at      TIMESTAMP,
                    duration_ms       INTEGER DEFAULT 0,
                    status            TEXT DEFAULT 'RUNNING'
                );

                CREATE TABLE IF NOT EXISTS extracted_numbers (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id             INTEGER,
                    user_id            INTEGER,
                    number             TEXT,
                    source_url         TEXT,
                    extraction_method  TEXT,
                    visit_number       INTEGER,
                    observed_exit_ip   TEXT,
                    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES extraction_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS job_proxy_attempts (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id        INTEGER,
                    visit_number  INTEGER,
                    proxy_endpoint_safe TEXT,
                    proxy_protocol TEXT,
                    status        TEXT,
                    latency_ms    REAL,
                    observed_ip   TEXT,
                    error         TEXT,
                    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES extraction_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS admins (
                    user_id     INTEGER PRIMARY KEY,
                    username    TEXT,
                    role        TEXT DEFAULT 'ADMIN',
                    added_by    INTEGER,
                    is_active   INTEGER DEFAULT 1,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS bot_settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS admin_audit_log (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id   INTEGER,
                    action     TEXT,
                    target     TEXT,
                    details    TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_history_user ON extraction_history(user_id);
                CREATE INDEX IF NOT EXISTS idx_history_date ON extraction_history(started_at);
                CREATE INDEX IF NOT EXISTS idx_proxies_active ON admin_proxies(is_active);
                CREATE INDEX IF NOT EXISTS idx_jobs_user ON extraction_jobs(user_id);
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON extraction_jobs(status);
                CREATE INDEX IF NOT EXISTS idx_numbers_job ON extracted_numbers(job_id);
                CREATE INDEX IF NOT EXISTS idx_numbers_user ON extracted_numbers(user_id);
                CREATE INDEX IF NOT EXISTS idx_numbers_number ON extracted_numbers(number);
                CREATE INDEX IF NOT EXISTS idx_audit_admin ON admin_audit_log(admin_id);
            """)
            conn.commit()

            # ── Migrate legacy columns onto existing tables ──
            _add_col(conn, "users", "successful_extractions", "INTEGER DEFAULT 0")
            _add_col(conn, "users", "failed_extractions", "INTEGER DEFAULT 0")
            _add_col(conn, "users", "last_extraction_at", "TIMESTAMP")
            _add_col(conn, "users", "status", "TEXT DEFAULT 'APPROVED'")
            _add_col(conn, "users", "blocked_reason", "TEXT")
            _add_col(conn, "admin_proxies", "consecutive_failures", "INTEGER DEFAULT 0")
            _add_col(conn, "admin_proxies", "consecutive_successes", "INTEGER DEFAULT 0")
            _add_col(conn, "admin_proxies", "health_status", "TEXT DEFAULT 'UNTESTED'")
            _add_col(conn, "admin_proxies", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

            # ── Seed bootstrap admins / owner from env ──
            for uid in ADMIN_IDS:
                role = "OWNER" if uid in OWNER_IDS else "ADMIN"
                conn.execute(
                    """INSERT INTO admins (user_id, role, is_active)
                       VALUES (?, ?, 1)
                       ON CONFLICT(user_id) DO UPDATE SET is_active=1""",
                    (uid, role),
                )

            _seed_settings(conn)
            conn.commit()
        finally:
            conn.close()


# ── Settings (DB-backed, env provides defaults) ──────────
_DEFAULT_SETTINGS: dict[str, str] = {
    "channel_username": DEFAULT_CHANNEL,
    "channel_enabled": "0",
    "channel_post_summary": "1",
    "channel_post_numbers": "0",
    "channel_attach_txt": "1",
    "admin_display_username": DEFAULT_ADMIN_DISPLAY,
    "support_username": DEFAULT_SUPPORT_USERNAME,
    "bot_name": DEFAULT_BOT_NAME,
    "welcome_text": "Welcome to the URL Extraction Center.",
    "maintenance_mode": "0",
    "approval_mode": "0",
    "max_visits": str(MAX_VISITS_PER_JOB),
    "max_concurrency": str(MAX_CONCURRENCY),
    "request_timeout": str(REQUEST_TIMEOUT),
    "connect_timeout": str(CONNECT_TIMEOUT),
    "read_timeout": str(READ_TIMEOUT),
    "progress_interval": str(PROGRESS_INTERVAL),
    "proxy_enabled": "1",
    "proxy_test_concurrency": "8",
    "health_timeout": "6",
    "retest_interval": "300",
    "quarantine_cap": "300",
    "auto_retest": "1",
}


def _seed_settings(conn: sqlite3.Connection) -> None:
    for k, v in _DEFAULT_SETTINGS.items():
        conn.execute(
            "INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)", (k, v)
        )


def get_setting(key: str, default: Optional[str] = None) -> str:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT value FROM bot_settings WHERE key = ?", (key,)
        ).fetchone()
        if row:
            return row["value"]
        return default if default is not None else _DEFAULT_SETTINGS.get(key, "")
    finally:
        conn.close()


def get_setting_int(key: str, default: int = 0) -> int:
    try:
        return int(get_setting(key, str(default)) or default)
    except Exception:
        return default


def get_setting_float(key: str, default: float = 0.0) -> float:
    try:
        return float(get_setting(key, str(default)) or default)
    except Exception:
        return default


def get_setting_bool(key: str, default: bool = False) -> bool:
    v = get_setting(key, "1" if default else "0").strip().lower()
    return v in ("1", "true", "on", "yes")


def set_setting(key: str, value: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO bot_settings (key, value) VALUES (?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (key, str(value)),
            )
            conn.commit()
        finally:
            conn.close()


def maintenance_on() -> bool:
    return get_setting_bool("maintenance_mode", False)


def approval_required() -> bool:
    return get_setting_bool("approval_mode", False)


# ── Admin role helpers ────────────────────────────────────
def is_admin(user_id: int) -> bool:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT 1 FROM admins WHERE user_id = ? AND is_active = 1", (user_id,)
        ).fetchone()
        return row is not None or user_id in ADMIN_IDS
    finally:
        conn.close()


def admin_role(user_id: int) -> Optional[str]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT role FROM admins WHERE user_id = ? AND is_active = 1", (user_id,)
        ).fetchone()
        if row:
            return row["role"]
        if user_id in OWNER_IDS:
            return "OWNER"
        if user_id in ADMIN_IDS:
            return "ADMIN"
        return None
    finally:
        conn.close()


def is_owner(user_id: int) -> bool:
    return admin_role(user_id) == "OWNER"


def add_admin(target_id: int, role: str, added_by: int, username: str = "") -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO admins (user_id, username, role, added_by, is_active)
                   VALUES (?, ?, ?, ?, 1)
                   ON CONFLICT(user_id) DO UPDATE SET is_active=1, role=?""",
                (target_id, username, role, added_by, role),
            )
            conn.commit()
            audit_log(added_by, "ADD_ADMIN", str(target_id), f"role={role}")
            return True
        finally:
            conn.close()


def remove_admin(target_id: int, by: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                "UPDATE admins SET is_active=0 WHERE user_id=?", (target_id,)
            )
            conn.commit()
            audit_log(by, "REMOVE_ADMIN", str(target_id), "")
            return cur.rowcount > 0
        finally:
            conn.close()


def audit_log(admin_id: int, action: str, target: str, details: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO admin_audit_log (admin_id, action, target, details)
                   VALUES (?, ?, ?, ?)""",
                (admin_id, action[:60], target[:120], details[:300]),
            )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()


# ── User helpers ──────────────────────────────────────────
def register_user(user_id: int, username: str = None, first_name: str = None) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO users (user_id, username, first_name)
                   VALUES (?, ?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       username    = COALESCE(excluded.username, users.username),
                       first_name  = COALESCE(excluded.first_name, users.first_name),
                       last_active = CURRENT_TIMESTAMP""",
                (user_id, username, first_name),
            )
            conn.commit()
        finally:
            conn.close()


def user_status(user_id: int) -> str:
    """Return APPROVED / PENDING / BLOCKED / NEW."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT status FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
        if not row:
            return "NEW"
        return row["status"] or "APPROVED"
    finally:
        conn.close()


def set_user_status(user_id: int, status: str, reason: str = "", by: int = 0) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO users (user_id, status, blocked_reason)
                   VALUES (?, ?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET status=?, blocked_reason=?""",
                (user_id, status, reason, status, reason),
            )
            conn.commit()
            if by:
                audit_log(by, f"USER_{status}", str(user_id), reason)
        finally:
            conn.close()


def approve_user(user_id: int, by: int) -> None:
    set_user_status(user_id, "APPROVED", by=by)


def block_user(user_id: int, reason: str, by: int) -> None:
    set_user_status(user_id, "BLOCKED", reason=reason, by=by)


def unblock_user(user_id: int, by: int) -> None:
    set_user_status(user_id, "APPROVED", by=by)


def user_can_extract(user_id: int) -> tuple[bool, str]:
    """Returns (allowed, reason_if_not)."""
    if is_admin(user_id):
        return True, ""
    status = user_status(user_id)
    if status == "BLOCKED":
        return False, "🚫 Your access has been revoked. Contact the administrator."
    if status == "PENDING" or (status == "NEW" and approval_required()):
        return False, "🔒 Your access request is pending administrator approval."
    return True, ""


def update_user_stats(user_id: int, unique_count: int, success: bool) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                f"""UPDATE users
                    SET total_extractions        = total_extractions + 1,
                        total_numbers_found      = total_numbers_found + ?,
                        successful_extractions   = successful_extractions + ?,
                        failed_extractions       = failed_extractions + ?,
                        last_active              = CURRENT_TIMESTAMP,
                        last_extraction_at        = CURRENT_TIMESTAMP
                    WHERE user_id = ?""",
                (unique_count, 1 if success else 0, 0 if success else 1, user_id),
            )
            conn.commit()
        finally:
            conn.close()


def save_history(user_id, url, mode, cycles, unique, duplicates) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO extraction_history
                       (user_id, url, mode, cycles, unique_numbers, duplicate_count, completed_at)
                   VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (user_id, url, mode, cycles, unique, duplicates),
            )
            conn.commit()
        finally:
            conn.close()


def get_user_stats(user_id: int) -> Optional[dict]:
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
            """SELECT * FROM extraction_history WHERE user_id = ?
               ORDER BY started_at DESC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_admin_stats() -> dict:
    conn = get_conn()
    try:
        out = {}
        out["users"] = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        out["extractions"] = conn.execute(
            "SELECT COALESCE(SUM(total_extractions),0) c FROM users"
        ).fetchone()["c"]
        out["numbers"] = conn.execute(
            "SELECT COALESCE(SUM(total_numbers_found),0) c FROM users"
        ).fetchone()["c"]
        out["jobs_total"] = conn.execute(
            "SELECT COUNT(*) c FROM extraction_jobs"
        ).fetchone()["c"]
        out["jobs_success"] = conn.execute(
            "SELECT COUNT(*) c FROM extraction_jobs WHERE status='COMPLETED'"
        ).fetchone()["c"]
        out["jobs_failed"] = conn.execute(
            "SELECT COUNT(*) c FROM extraction_jobs WHERE status='FAILED'"
        ).fetchone()["c"]
        out["jobs_running"] = conn.execute(
            "SELECT COUNT(*) c FROM extraction_jobs WHERE status='RUNNING'"
        ).fetchone()["c"]
        out["active_today"] = conn.execute(
            "SELECT COUNT(*) c FROM users WHERE date(last_active)=date('now')"
        ).fetchone()["c"]
        out["jobs_today"] = conn.execute(
            "SELECT COUNT(*) c FROM extraction_jobs WHERE date(started_at)=date('now')"
        ).fetchone()["c"]
        out["numbers_today"] = conn.execute(
            "SELECT COUNT(*) c FROM extracted_numbers WHERE date(created_at)=date('now')"
        ).fetchone()["c"]
        out["numbers_week"] = conn.execute(
            "SELECT COUNT(*) c FROM extracted_numbers WHERE created_at >= datetime('now','-7 days')"
        ).fetchone()["c"]
        out["avg_duration"] = conn.execute(
            "SELECT COALESCE(AVG(duration_ms),0) c FROM extraction_jobs WHERE status='COMPLETED'"
        ).fetchone()["c"]
        return out
    finally:
        conn.close()


def get_all_user_ids() -> list[int]:
    conn = get_conn()
    try:
        rows = conn.execute("SELECT user_id FROM users WHERE status != 'BLOCKED'").fetchall()
        return [r["user_id"] for r in rows]
    finally:
        conn.close()


# ── Job DB helpers ────────────────────────────────────────
def create_job(user_id: int, username: str, url: str, mode: str, visits: int) -> int:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """INSERT INTO extraction_jobs
                       (user_id, username, source_url, mode, requested_visits, status)
                   VALUES (?, ?, ?, ?, ?, 'RUNNING')""",
                (user_id, username, url, mode, visits),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()


def update_job_counts(job_id: int, *, success: int = 0, failed: int = 0,
                      unique: int = 0, dup: int = 0) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE extraction_jobs
                   SET successful_visits = successful_visits + ?,
                       failed_visits     = failed_visits + ?,
                       unique_numbers    = unique_numbers + ?,
                       duplicate_numbers = duplicate_numbers + ?
                   WHERE job_id = ?""",
                (success, failed, unique, dup, job_id),
            )
            conn.commit()
        finally:
            conn.close()


def finish_job(job_id: int, status: str, duration_ms: int) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE extraction_jobs
                   SET status = ?, completed_at = CURRENT_TIMESTAMP, duration_ms = ?
                   WHERE job_id = ?""",
                (status, duration_ms, job_id),
            )
            conn.commit()
        finally:
            conn.close()


def insert_extracted_number(job_id: int, user_id: int, number: str, source_url: str,
                            method: str, visit: int, exit_ip: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO extracted_numbers
                       (job_id, user_id, number, source_url, extraction_method,
                        visit_number, observed_exit_ip)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (job_id, user_id, number, source_url[:500], method, visit, exit_ip or ""),
            )
            conn.commit()
        finally:
            conn.close()


def insert_proxy_attempt(job_id: int, visit: int, endpoint_safe: str,
                          protocol: str, status: str, latency_ms: float,
                          observed_ip: str, error: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO job_proxy_attempts
                       (job_id, visit_number, proxy_endpoint_safe, proxy_protocol,
                        status, latency_ms, observed_ip, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (job_id, visit, endpoint_safe[:200], protocol, status,
                 latency_ms, observed_ip or "", (error or "")[:200]),
            )
            conn.commit()
        finally:
            conn.close()


def get_job(job_id: int) -> Optional[dict]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM extraction_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_job_numbers(job_id: int, limit: int = 200) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT * FROM extracted_numbers WHERE job_id=?
               ORDER BY id ASC LIMIT ?""",
            (job_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_job_attempts(job_id: int, limit: int = 100) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM job_proxy_attempts WHERE job_id=? ORDER BY id ASC LIMIT ?",
            (job_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_jobs(limit: int = 20, offset: int = 0, where: str = "",
              params: tuple = ()) -> list[dict]:
    sql = "SELECT * FROM extraction_jobs"
    if where:
        sql += f" WHERE {where}"
    sql += " ORDER BY job_id DESC LIMIT ? OFFSET ?"
    conn = get_conn()
    try:
        rows = conn.execute(sql, params + (limit, offset)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def count_jobs(where: str = "", params: tuple = ()) -> int:
    sql = "SELECT COUNT(*) c FROM extraction_jobs"
    if where:
        sql += f" WHERE {where}"
    conn = get_conn()
    try:
        return conn.execute(sql, params).fetchone()["c"]
    finally:
        conn.close()


# ── Proxy DB helpers ──────────────────────────────────────
def db_add_proxy(endpoint: str, added_by: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """INSERT OR IGNORE INTO admin_proxies (endpoint, added_by, is_active)
                   VALUES (?, ?, 1)""",
                (endpoint, added_by),
            )
            conn.commit()
            return cur.rowcount > 0
        except Exception as exc:
            logger.warning("db_add_proxy: %s", exc)
            return False
        finally:
            conn.close()


def db_get_all_proxies(active_only: bool = True) -> list[dict]:
    conn = get_conn()
    try:
        sql = "SELECT * FROM admin_proxies"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id ASC"
        rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def db_delete_proxy(proxy_id: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute("DELETE FROM admin_proxies WHERE id = ?", (proxy_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


def db_clear_all_proxies() -> int:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute("DELETE FROM admin_proxies")
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()


def db_clear_dead_proxies() -> int:
    """Remove only irrecoverably dead proxies (no successes and many failures)."""
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """DELETE FROM admin_proxies
                   WHERE success_count = 0 AND failure_count >= 5"""
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()


def db_get_proxy_by_endpoint(endpoint: str) -> Optional[dict]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM admin_proxies WHERE endpoint = ?", (endpoint,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def db_update_proxy_success(proxy_id: int, latency_ms: float, observed_ip: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT average_latency, success_count FROM admin_proxies WHERE id = ?",
                (proxy_id,),
            ).fetchone()
            if not row:
                return
            old_avg = row["average_latency"] or 0.0
            old_cnt = row["success_count"] or 0
            new_cnt = old_cnt + 1
            new_avg = ((old_avg * old_cnt) + latency_ms) / new_cnt
            # Exponential decay: consecutive_successes +1, consecutive_failures reset
            conn.execute(
                """UPDATE admin_proxies
                   SET success_count         = success_count + 1,
                       consecutive_successes = consecutive_successes + 1,
                       consecutive_failures = 0,
                       last_success          = CURRENT_TIMESTAMP,
                       last_tested           = CURRENT_TIMESTAMP,
                       average_latency       = ?,
                       last_observed_ip      = ?,
                       cooldown_until        = NULL,
                       last_error            = NULL,
                       health_status         = ?,
                       updated_at            = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (new_avg, observed_ip,
                 "WORKING" if latency_ms < 1500 else "SLOW", proxy_id),
            )
            conn.commit()
        finally:
            conn.close()


def db_update_proxy_failure(proxy_id: int, error: str, reason_label: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT consecutive_failures FROM admin_proxies WHERE id = ?",
                (proxy_id,),
            ).fetchone()
            cf = (row["consecutive_failures"] if row else 0) + 1
            # Exponential backoff cooldown capped at quarantine_cap
            cap = get_setting_int("quarantine_cap", 300)
            base = 30
            cd_seconds = min(base * (2 ** min(cf - 1, 6)), cap)
            conn.execute(
                """UPDATE admin_proxies
                   SET failure_count         = failure_count + 1,
                       consecutive_failures = ?,
                       consecutive_successes = 0,
                       last_failure         = CURRENT_TIMESTAMP,
                       last_tested           = CURRENT_TIMESTAMP,
                       last_error            = ?,
                       cooldown_until        = datetime('now', ? || ' seconds'),
                       health_status         = ?,
                       updated_at            = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (cf, error[:200], f"+{cd_seconds}", reason_label, proxy_id),
            )
            conn.commit()
        finally:
            conn.close()


def db_proxy_stats() -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            """SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN health_status='UNTESTED' OR last_tested IS NULL THEN 1 ELSE 0 END) AS untested,
                SUM(CASE WHEN health_status='WORKING' THEN 1 ELSE 0 END) AS working,
                SUM(CASE WHEN health_status='SLOW' THEN 1 ELSE 0 END) AS slow,
                SUM(CASE WHEN health_status IN ('DEAD','TCP_FAILED','AUTH_FAILED','DNS_FAILED') THEN 1 ELSE 0 END) AS dead,
                SUM(CASE WHEN health_status='COOLDOWN' THEN 1 ELSE 0 END) AS cooldown,
                AVG(CASE WHEN success_count > 0 THEN average_latency END) AS avg_latency,
                MAX(last_tested) AS last_test_time
               FROM admin_proxies WHERE is_active = 1"""
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


# =========================================================
# Proxy Parser & Validator
# =========================================================
SUPPORTED_SCHEMES = ("http", "https", "socks5", "socks5h")


class ParsedProxy:
    __slots__ = ("raw", "scheme", "host", "port", "username", "password")

    def __init__(self, raw, scheme, host, port, username, password):
        self.raw = raw
        self.scheme = scheme
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    @property
    def display(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    @property
    def protocol_label(self) -> str:
        if self.scheme.startswith("socks5"):
            return "SOCKS5"
        return self.scheme.upper()

    def to_requests_proxies(self) -> Optional[dict]:
        if self.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
            return None
        if self.username and self.password:
            u = urllib.parse.quote(self.username, safe="")
            p = urllib.parse.quote(self.password, safe="")
            url = f"{self.scheme}://{u}:{p}@{self.host}:{self.port}"
        else:
            url = f"{self.scheme}://{self.host}:{self.port}"
        return {"http": url, "https": url}


def parse_proxy(raw: str) -> tuple[Optional[ParsedProxy], str]:
    """Parse a proxy string in any supported format. Returns (Parsed, "")."""
    raw = (raw or "").strip()
    if not raw:
        return None, "Empty proxy string"

    # Allow bare host:port and host:port:user:pass
    if "://" not in raw:
        parts = raw.split(":")
        if len(parts) == 2:
            raw = f"http://{raw}"
        elif len(parts) == 4:
            host, port, user, pwd = parts
            raw = f"http://{user}:{pwd}@{host}:{port}"
        elif len(parts) == 3:
            # ambiguous; assume host:port:user only if last is non-numeric
            host, port, x = parts
            if x.isdigit():
                return None, "Ambiguous 3-field format; use scheme://..."
            raw = f"http://{host}:{port}:{x}:x"  # invalid; fall through
            return None, "Use scheme://user:pass@host:port"

    try:
        parsed = urllib.parse.urlsplit(raw)
    except Exception:
        return None, "Malformed URL"

    scheme = (parsed.scheme or "").lower()
    if scheme not in SUPPORTED_SCHEMES:
        return None, f"Unsupported scheme '{scheme}'. Use: {', '.join(SUPPORTED_SCHEMES)}"

    host = parsed.hostname
    if not host:
        return None, "Missing hostname"
    port = parsed.port
    if port is None:
        return None, "Missing port"
    if not (1 <= port <= 65535):
        return None, f"Invalid port {port}"

    username = password = None
    if parsed.username:
        try:
            username = urllib.parse.unquote(parsed.username)
        except Exception:
            return None, "Bad username encoding"
    if parsed.password:
        try:
            password = urllib.parse.unquote(parsed.password)
        except Exception:
            return None, "Bad password encoding"
    if (username is None) != (password is None):
        return None, "Both username and password must be provided together"

    return ParsedProxy(raw, scheme, host, port, username, password), ""


# =========================================================
# Multi-stage Proxy Tester
# =========================================================
class ProxyTestResult:
    __slots__ = ("display", "scheme_label", "working", "latency_ms",
                 "observed_ip", "error_reason", "tested_at", "status_label")

    def __init__(self, display, scheme_label, working, latency_ms, observed_ip,
                 error_reason, status_label):
        self.display = display
        self.scheme_label = scheme_label
        self.working = working
        self.latency_ms = latency_ms
        self.observed_ip = observed_ip
        self.error_reason = error_reason
        self.status_label = status_label  # WORKING / SLOW / AUTH_FAILED / TCP_FAILED / DEAD
        self.tested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def to_telegram_card(self, index: Optional[int] = None) -> str:
        prefix = f"<b>#{index}</b>\n" if index is not None else ""
        icon = {"WORKING": "🟢", "SLOW": "🟡", "AUTH_FAILED": "🟠",
                "TCP_FAILED": "🔴", "DEAD": "🔴", "INVALID": "⚫"}.get(
            self.status_label, "⚪")
        lines = [prefix]
        lines.append(f"{icon} <b>{self.status_label}</b>")
        lines.append(f"🌐 {html.escape(self.scheme_label)}")
        lines.append(f"📡 <code>{html.escape(self.display)}</code>")
        if self.working:
            if self.latency_ms is not None:
                lines.append(f"⚡ Latency: <code>{self.latency_ms:.0f} ms</code>")
            if self.observed_ip:
                lines.append(f"🌍 Exit IP: <code>{html.escape(self.observed_ip)}</code>")
            else:
                lines.append("⚠️ IP verification skipped (transport OK)")
        elif self.error_reason:
            lines.append(f"Reason: {html.escape(self.error_reason)}")
        lines.append(f"🕒 {html.escape(self.tested_at)}")
        return "\n".join(lines)


def _tcp_check(host: str, port: int, timeout: float = 4.0) -> tuple[bool, str]:
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True, ""
    except socket.timeout:
        return False, "TCP timeout"
    except socket.gaierror as exc:
        return False, f"DNS failure"
    except ConnectionRefusedError:
        return False, "Connection refused"
    except OSError as exc:
        return False, str(exc)


def _classify_requests_error(exc: Exception) -> tuple[str, str]:
    """Return (reason_label, human_text)."""
    msg = str(exc)
    low = msg.lower()
    if "socks" in low and not _SOCKS5_AVAILABLE:
        return "DEAD", "SOCKS5 support not installed (requests[socks])"
    if "407" in msg:
        return "AUTH_FAILED", "Authentication failed (HTTP 407)"
    if "timeout" in low or "timed out" in low:
        return "DEAD", "Connection timeout"
    if "refused" in low:
        return "TCP_FAILED", "Connection refused"
    if "ssl" in low or "certificate" in low:
        return "DEAD", "TLS/SSL failure"
    if "name or service" in low or "nodename" in low or "name resolution" in low:
        return "DEAD", "DNS failure"
    if "proxy" in low:
        return "DEAD", msg[:120]
    return "DEAD", msg[:120]


def _extract_ip_from_text(text: str) -> Optional[str]:
    m = re.search(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b", text or "")
    return m.group(1) if m else None


def test_proxy(parsed: ParsedProxy, quick: bool = True) -> ProxyTestResult:
    """
    Multi-stage test:
      A) parse validation (done by caller)
      B) TCP connectivity to the proxy host:port
      C) real HTTP/HTTPS request through the proxy
      D) exit-IP verification against multiple endpoints (any one is enough)
    A proxy is only DEAD if the transport itself fails — not if a third-party
    IP-info site happens to be down.
    """
    connect_to = get_setting_float("connect_timeout", CONNECT_TIMEOUT)
    read_to = get_setting_float("read_timeout", READ_TIMEOUT)
    timeout = (min(connect_to, 6.0) if quick else connect_to, read_to)

    # Stage B — TCP
    tcp_ok, tcp_err = _tcp_check(parsed.host, parsed.port, timeout=min(timeout[0], 4.0))
    if not tcp_ok:
        return ProxyTestResult(parsed.display, parsed.protocol_label, False, None, None,
                               f"TCP connect failed: {tcp_err}", "TCP_FAILED")

    # Stage C/D — transport + exit-IP
    proxies_dict = parsed.to_requests_proxies()
    if proxies_dict is None:
        return ProxyTestResult(parsed.display, parsed.protocol_label, False, None, None,
                               "SOCKS5 requires requests[socks] — not installed", "DEAD")

    session = requests.Session()
    session.proxies = proxies_dict
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; ProxyTester/2.0)"})

    observed_ip: Optional[str] = None
    latency_ms: Optional[float] = None
    transport_ok = False
    last_err: Optional[str] = None
    last_label = "DEAD"

    # Stage D — try multiple IP-check endpoints first (best signal)
    for ip_url in _IP_CHECK_URLS:
        try:
            t0 = time.perf_counter()
            resp = session.get(ip_url, timeout=timeout, allow_redirects=True)
            latency_ms = (time.perf_counter() - t0) * 1000
            if resp.status_code == 200:
                transport_ok = True
                try:
                    data = resp.json()
                    observed_ip = (
                        data.get("ip")
                        or (data.get("origin", "").split(",")[0].strip()
                            if isinstance(data.get("origin"), str) else "")
                        or data.get("query")
                    )
                except Exception:
                    observed_ip = _extract_ip_from_text(resp.text)
                if observed_ip:
                    label = "WORKING" if latency_ms < 1500 else "SLOW"
                    session.close()
                    return ProxyTestResult(parsed.display, parsed.protocol_label, True,
                                           latency_ms, observed_ip, None, label)
            else:
                last_err = f"HTTP {resp.status_code} from {ip_url}"
        except requests.exceptions.RequestException as exc:
            label, txt = _classify_requests_error(exc)
            last_err, last_label = txt, label
            # AUTH_FAILED is a hard stop — no point trying other endpoints
            if label == "AUTH_FAILED":
                session.close()
                return ProxyTestResult(parsed.display, parsed.protocol_label, False,
                                       latency_ms, None, txt, "AUTH_FAILED")
        except Exception as exc:
            last_err = str(exc)[:100]

    # If transport worked but no IP-info endpoint returned an IP, verify
    # transport by hitting a generic site. A working request through the
    # proxy counts as WORKING even when all IP-info APIs are down.
    if not transport_ok:
        for probe in _TRANSPORT_PROBE_URLS:
            try:
                t0 = time.perf_counter()
                resp = session.get(probe, timeout=timeout, allow_redirects=False)
                latency_ms = (time.perf_counter() - t0) * 1000
                if resp.status_code < 500:
                    transport_ok = True
                    break
            except requests.exceptions.RequestException:
                continue
            except Exception:
                continue

    session.close()

    if transport_ok:
        label = "WORKING" if (latency_ms or 9999) < 1500 else "SLOW"
        return ProxyTestResult(parsed.display, parsed.protocol_label, True,
                               latency_ms, observed_ip or "",
                               "Transport OK; IP verification service unavailable", label)

    return ProxyTestResult(parsed.display, parsed.protocol_label, False, latency_ms,
                           None, last_err or "All endpoints failed", last_label)


# =========================================================
# Proxy Manager / Rotation Engine (job-aware, thread-safe)
# =========================================================
class ProxyManager:
    """
    Manages proxy endpoints from env + DB. Round-robin rotation with per-proxy
    exponential cooldown on failure. NEVER silently falls back to a direct
    connection: get_next_endpoint returns None when nothing is healthy.
    """

    def __init__(self):
        self._env_proxies: list[str] = []
        self._rotation_index = 0
        self._lock = threading.Lock()
        self._load_env()

    def _load_env(self) -> None:
        items = []
        for raw in RAW_PROXY_ENV.split(","):
            raw = raw.strip()
            if not raw:
                continue
            parsed, err = parse_proxy(raw)
            if parsed:
                items.append(raw)
            else:
                logger.warning("Skipping invalid env proxy '%s': %s", _scrub(raw), err)
        self._env_proxies = list(dict.fromkeys(items))
        logger.info("Loaded %d env proxy endpoint(s).", len(self._env_proxies))

    def get_all_raw(self) -> list[str]:
        with self._lock:
            db_rows = db_get_all_proxies(active_only=True)
            db_eps = [r["endpoint"] for r in db_rows
                      if (r.get("cooldown_until") is None
                          or self._cooldown_expired(r.get("cooldown_until")))]
            # Env proxies always eligible (cooldown tracked in-memory below)
            return list(dict.fromkeys(self._env_proxies + db_eps))

    @staticmethod
    def _cooldown_expired(cooldown_str: Optional[str]) -> bool:
        if not cooldown_str:
            return True
        try:
            # SQLite stores as 'YYYY-MM-DD HH:MM:SS' (UTC).
            cd = datetime.strptime(str(cooldown_str)[:19], "%Y-%m-%d %H:%M:%S")
            return cd.replace(tzinfo=timezone.utc) <= datetime.now(timezone.utc)
        except Exception:
            return True

    def has_endpoints(self) -> bool:
        return len(self.get_all_raw()) > 0

    def get_endpoint_count(self) -> int:
        return len(self.get_all_raw())

    def env_count(self) -> int:
        return len(self._env_proxies)

    def get_next_endpoint(self, exclude: Optional[set] = None) -> Optional[str]:
        """
        Round-robin selection over healthy proxies. Skips any endpoint in
        `exclude` (recently failed in this job). Returns None only when nothing
        healthy is available — caller MUST report honestly, never fall back.
        """
        with self._lock:
            all_eps = self.get_all_raw()
            if not all_eps:
                return None
            available = [ep for ep in all_eps if not (exclude and ep in exclude)]
            if not available:
                # If everything is excluded, allow retrying them rather than dying.
                available = all_eps
            idx = self._rotation_index % len(available)
            self._rotation_index += 1
            return available[idx]

    def mark_success(self, endpoint: str, latency_ms: float, observed_ip: str) -> None:
        row = db_get_proxy_by_endpoint(endpoint)
        if row:
            db_update_proxy_success(row["id"], latency_ms, observed_ip)

    def mark_failed(self, endpoint: str, error: str, label: str = "DEAD") -> None:
        row = db_get_proxy_by_endpoint(endpoint)
        if row:
            db_update_proxy_failure(row["id"], error, label)

    @staticmethod
    def sanitize_display(endpoint: str) -> str:
        parsed, _ = parse_proxy(endpoint)
        if parsed:
            return parsed.display
        try:
            p = urllib.parse.urlsplit(endpoint)
            netloc = f"{p.hostname}:{p.port}" if p.port else (p.hostname or "?")
            return f"{p.scheme}://{netloc}"
        except Exception:
            return "proxy-endpoint"


proxy_manager = ProxyManager()


# =========================================================
# AES-128-CBC Challenge Solver (InfinityFree / ByetHost)
# Pure Python, zero extra dependencies — preserved from v1
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
        w.append((key_bytes[4 * i] << 24) | (key_bytes[4 * i + 1] << 16)
                 | (key_bytes[4 * i + 2] << 8) | key_bytes[4 * i + 3])
    for i in range(4, 44):
        temp = w[i - 1]
        if i % 4 == 0:
            temp = _sub_word(_rot_word(temp)) ^ (_AES_RCON[i // 4] << 24)
        w.append(w[i - 4] ^ temp)
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
# Number Extraction Engine (modular, method-tracked)
# =========================================================
# Patterns are kept separate from normalization. Each pattern is tagged with
# the extraction "method" the spec wants recorded per number.

_WA_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("WA_ME_URL",        re.compile(r'wa\.me/(?:p/|qr/)?\+?(\d{10,15})', re.IGNORECASE)),
    ("WHATSAPP_API_URL", re.compile(
        r'(?:api|web)\.whatsapp\.com/send/?\??.*?(?:phone|number)=\+?(\d{10,15})',
        re.IGNORECASE)),
    ("WHATSAPP_SCHEME",  re.compile(
        r'(?:whatsapp|intent)://send\?.*?(?:phone|number)=\+?(\d{10,15})',
        re.IGNORECASE)),
    ("TEL_LINK",         re.compile(r'href=["\']tel:\+?(\d{10,15})["\']', re.IGNORECASE)),
    ("QUERY_PARAMETER",  re.compile(
        r'(?:[?&])(?:phone|mobile|number|wa_number|whatsapp|send_to|to)=\+?(\d{10,15})',
        re.IGNORECASE)),
    ("DATA_ATTRIBUTE",   re.compile(
        r'data-(?:phone|whatsapp|number|mobile)=["\']\+?(\d{10,15})["\']',
        re.IGNORECASE)),
    ("HTML_HREF",        re.compile(
        r'href=["\'](?:https?://[^"\']*?|whatsapp://send\?phone=)\+?(\d{10,15})["\']',
        re.IGNORECASE)),
    ("JSON_FIELD",       re.compile(
        r'["\'](?:whatsapp|phone_number|mobile_number|wa_number|phone|mobile|recipient|send_to|number)["\']\s*:\s*["\']?\+?(\d{10,15})["\']?',
        re.IGNORECASE)),
    ("PAGE_TEXT",        re.compile(
        r'(?<![\w\d])(\+?\d{10,15})(?![\w\d])')),
]

# Pre-compiled list of "shape" hints that a numeric token is an ID rather than
# a phone — used to filter out random long IDs.
_ID_HINTS = re.compile(
    r'(?:id|uid|userid|post|message|thread|chat|item|product|order|invoice|'
    r'receipt|session|token|csrf|nonce|hash|signature|ts|timestamp|date)',
    re.IGNORECASE)


def clean_phone_number(raw: str, source_text: str = "") -> Optional[str]:
    """Normalize a raw numeric capture. Returns E.164-ish digits or None."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    # Strip leading "00" international prefix
    if digits.startswith("00") and len(digits) > 12:
        digits = digits[2:]
    # 10–15 digits is the E.164 range. Anything else is suspect.
    if not (10 <= len(digits) <= 15):
        return None
    # Reject obvious sequential/repeated junk (e.g. 0000000000, 1234567890)
    if len(set(digits)) == 1:
        return None
    # If the surrounding text strongly suggests an ID field, drop it.
    if source_text and _ID_HINTS.search(source_text[:80]):
        return None
    return digits


def _scan(text: str) -> list[tuple[str, str]]:
    """Return [(normalized, method), ...] for a single text sample."""
    out: list[tuple[str, str]] = []
    if not text:
        return out
    # Run several decodings — many pages double-encode phone query params.
    samples = [
        text,
        urllib.parse.unquote(text),
        urllib.parse.unquote_plus(text),
        html.unescape(text),
    ]
    for method, pattern in _WA_PATTERNS:
        for sample in samples:
            for m in pattern.finditer(sample):
                raw = m.group(1)
                ctx = sample[max(0, m.start() - 40): m.end() + 10]
                cleaned = clean_phone_number(raw, ctx)
                if cleaned:
                    out.append((cleaned, method))
    return out


def extract_numbers_from_text(text: str) -> list[tuple[str, str]]:
    """Public entry: returns list of (normalized, method) tuples (may dup)."""
    return _scan(text)


def extract_from_url_chain(urls: list[str]) -> list[tuple[str, str]]:
    """Scan a redirect URL chain. URLs are themselves sources for phones."""
    out: list[tuple[str, str]] = []
    for u in urls:
        out.extend(_scan(u))
        # Also decode nested query params like ?u=https%3A%2F%2Fwa.me%2F91...
        try:
            qs = urllib.parse.urlparse(u).query
            if qs:
                for _, v in urllib.parse.parse_qs(qs).items():
                    for vv in v:
                        out.extend(_scan(vv))
        except Exception:
            pass
    return out


def dedupe_with_method(
    found: list[tuple[str, str]]
) -> tuple[list[tuple[str, str]], int]:
    """Return (unique ordered list of (num, first_method), duplicate_count)."""
    seen: dict[str, str] = {}
    dup = 0
    for num, method in found:
        if num in seen:
            dup += 1
        else:
            seen[num] = method
    return list(seen.items()), dup


# =========================================================
# URL Validator & Redirect-Aware Fetcher
# =========================================================
def validate_url(url: str) -> tuple[bool, str]:
    if len(url) > MAX_URL_LENGTH:
        return False, f"URL too long (max {MAX_URL_LENGTH} chars)"
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False, "Malformed URL"
    if parsed.scheme not in ("http", "https"):
        return False, "URL must start with http:// or https://"
    if not parsed.hostname:
        return False, "Missing hostname"
    return True, ""


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Records redirect targets; lets non-HTTP schemes (whatsapp://) be
    captured without following them."""

    def __init__(self):
        super().__init__()
        self.collected_redirect_targets: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.collected_redirect_targets.append(newurl)
        try:
            scheme = urllib.parse.urlparse(newurl).scheme.lower()
        except Exception:
            scheme = ""
        if scheme not in ("http", "https"):
            return None  # record but don't follow
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class ExtractionSession:
    """
    Per-request session. HTTP/HTTPS proxies use urllib; SOCKS5 uses
    requests+PySocks. Cookies are isolated per instance.
    """

    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    _DEFAULT_HEADERS = [
        ("User-Agent", _UA),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ("Accept-Language", "en-US,en;q=0.9"),
        ("Cache-Control", "no-cache"),
        ("Pragma", "no-cache"),
        ("Upgrade-Insecure-Requests", "1"),
    ]

    def __init__(self, proxy_endpoint: Optional[str] = None,
                 timeout: float = REQUEST_TIMEOUT):
        self.proxy_endpoint = proxy_endpoint
        self.timeout = timeout
        self.cached_test_cookie: Optional[str] = None

        parsed_proxy, _ = (None, "")
        if proxy_endpoint:
            parsed_proxy, _ = parse_proxy(proxy_endpoint)

        self._use_requests = (
            parsed_proxy is not None and parsed_proxy.scheme.startswith("socks5")
        )

        if self._use_requests:
            self._requests_session = requests.Session()
            pd = parsed_proxy.to_requests_proxies()
            if pd:
                self._requests_session.proxies = pd
            self._requests_session.headers.update({"User-Agent": self._UA})
            self._urllib_opener = None
            self._redirect_handler = None
            self._cj = None
        else:
            self._requests_session = None
            self._cj = http.cookiejar.CookieJar()
            self._redirect_handler = SafeRedirectHandler()
            handlers = [
                urllib.request.HTTPCookieProcessor(self._cj),
                self._redirect_handler,
            ]
            if parsed_proxy and not self._use_requests:
                handlers.append(urllib.request.ProxyHandler({
                    "http": proxy_endpoint,
                    "https": proxy_endpoint,
                }))
            self._urllib_opener = urllib.request.build_opener(*handlers)
            self._urllib_opener.addheaders = list(self._DEFAULT_HEADERS)

    def close(self) -> None:
        if self._requests_session:
            try:
                self._requests_session.close()
            except Exception:
                pass

    def fetch(self, url: str) -> tuple[str, str, list[str]]:
        """Returns (final_url, body, all_visited_urls)."""
        if self._use_requests:
            return self._fetch_requests(url)
        return self._fetch_urllib(url)

    def _fetch_requests(self, url: str) -> tuple[str, str, list[str]]:
        visited: list[str] = [url]
        sess = self._requests_session
        try:
            resp = sess.get(url, timeout=self.timeout, allow_redirects=True, stream=True)
            for r in resp.history:
                visited.append(r.url)
            visited.append(resp.url)
            body_bytes = b""
            for chunk in resp.iter_content(chunk_size=65536):
                body_bytes += chunk
                if len(body_bytes) > MAX_RESPONSE_SIZE:
                    break
            return resp.url, body_bytes.decode("utf-8", errors="ignore"), visited
        except requests.exceptions.RequestException as exc:
            _, txt = _classify_requests_error(exc)
            raise OSError(txt) from exc

    def _fetch_urllib(self, url: str) -> tuple[str, str, list[str]]:
        if self._redirect_handler:
            self._redirect_handler.collected_redirect_targets.clear()

        domain = urllib.parse.urlparse(url).hostname
        if self.cached_test_cookie and domain and self._cj is not None:
            self._cj.set_cookie(http.cookiejar.Cookie(
                0, "__test", self.cached_test_cookie, None, False,
                domain, True, False, "/", True, False, None, None, None,
                {"HttpOnly": None}, rfc2109=False,
            ))

        visited: list[str] = [url]
        current_url = url
        body = ""

        try:
            resp = self._urllib_opener.open(urllib.request.Request(url), timeout=self.timeout)
            current_url = resp.geturl()
            visited.append(current_url)
            body = resp.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as e:
            current_url = e.geturl() or url
            visited.append(current_url)
            try:
                body = (e.read(MAX_RESPONSE_SIZE) if hasattr(e, "read") else b"").decode(
                    "utf-8", errors="ignore")
            except Exception:
                body = ""
        except Exception:
            if self._redirect_handler:
                visited.extend(self._redirect_handler.collected_redirect_targets)
            raise

        if self._redirect_handler:
            visited.extend(self._redirect_handler.collected_redirect_targets)

        # InfinityFree / ByetHost slowAES challenge (preserved from v1)
        if "slowAES" in body or ("toNumbers(" in body and "__test=" in body):
            matches = re.findall(r'toNumbers\("([a-f0-9]+)"\)', body)
            if len(matches) >= 3:
                a_key, b_iv, c_cipher = matches[0], matches[1], matches[2]
                self.cached_test_cookie = decrypt_byet_challenge(c_cipher, a_key, b_iv)
                if domain and self._cj is not None:
                    self._cj.set_cookie(http.cookiejar.Cookie(
                        0, "__test", self.cached_test_cookie, None, False,
                        domain, True, False, "/", True, False, None, None, None,
                        {"HttpOnly": None}, rfc2109=False,
                    ))
                loc_match = re.search(r'location\.href\s*=\s*["\'](.*?)["\']', body)
                next_dest = loc_match.group(1) if loc_match else (
                    url + ("&i=1" if "?" in url else "?i=1"))
                next_url = urllib.parse.urljoin(current_url, next_dest)
                visited.append(next_url)
                try:
                    resp2 = self._urllib_opener.open(
                        urllib.request.Request(next_url), timeout=self.timeout)
                    current_url = resp2.geturl()
                    visited.append(current_url)
                    body = resp2.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
                except Exception:
                    pass

        # Meta-refresh + JS redirects (up to MAX_REDIRECTS hops total)
        for _ in range(min(MAX_REDIRECTS, 4)):
            meta = re.search(
                r'<meta[^>]*?http-equiv\s*=\s*["\']?refresh["\']?[^>]*?'
                r'content\s*=\s*["\']?[^"\'>]*?url\s*=\s*([^\s"\'\';>]+)',
                body, re.IGNORECASE)
            if meta:
                dest_url = urllib.parse.urljoin(current_url, meta.group(1).strip())
                visited.append(dest_url)
                if dest_url.lower().startswith(("http://", "https://")):
                    try:
                        r = self._urllib_opener.open(
                            urllib.request.Request(dest_url), timeout=self.timeout)
                        current_url = r.geturl()
                        visited.append(current_url)
                        body = r.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
                        continue
                    except Exception:
                        pass
                break

            js = re.search(
                r'(?:window\.|document\.|top\.)?location(?:\.href|\.replace|\.assign)?\s*'
                r'(?:=|\()\s*["\'](https?://[^"\']+|whatsapp://[^"\']+|wa\.me/[^"\']+)["\']',
                body, re.IGNORECASE)
            if js:
                dest_url = js.group(1).strip()
                visited.append(dest_url)
                if dest_url.lower().startswith(("http://", "https://")):
                    try:
                        r = self._urllib_opener.open(
                            urllib.request.Request(dest_url), timeout=self.timeout)
                        current_url = r.geturl()
                        visited.append(current_url)
                        body = r.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
                        continue
                    except Exception:
                        pass
                break
            break

        return current_url, body, visited


def add_cache_buster(url: str, cycle: int) -> str:
    """Cache-busting that preserves signed URLs — skipped when the URL has
    obvious signature/token params."""
    if any(k in url.lower() for k in ("signature=", "sig=", "token=", "expires=", "policy=")):
        return url
    ts = int(time.time() * 1000)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}_cb={ts}_{cycle}"


# =========================================================
# Live Progress Updater (independent of the network worker)
# =========================================================
class JobContext:
    """Shared, thread-safe state for one extraction job."""
    def __init__(self, user_id: int, chat_id: int, message_id: int,
                 total_visits: int, mode: str, url: str):
        self.user_id = user_id
        self.chat_id = chat_id
        self.message_id = message_id
        self.total_visits = total_visits
        self.mode = mode  # NORMAL / ROTATING
        self.url = url
        self.cancel = threading.Event()

        self.visit = 0
        self.success = 0
        self.failed = 0
        self.unique = 0
        self.duplicates = 0
        self.stage = "Initialising…"
        self.last_proxy_display = "—"
        self.last_proxy_protocol = ""
        self.last_exit_ip = ""
        self.last_latency_ms = 0.0
        self.last_error = ""
        self.start_time = time.time()
        self._lock = threading.Lock()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "visit": self.visit, "success": self.success, "failed": self.failed,
                "unique": self.unique, "duplicates": self.duplicates,
                "stage": self.stage,
                "last_proxy_display": self.last_proxy_display,
                "last_proxy_protocol": self.last_proxy_protocol,
                "last_exit_ip": self.last_exit_ip,
                "last_latency_ms": self.last_latency_ms,
                "elapsed": int(time.time() - self.start_time),
            }


def _progress_bar(pct: float, width: int = 10) -> str:
    done = int(pct * width)
    return "█" * done + "░" * (width - done)


def _render_progress(ctx: JobContext) -> str:
    snap = ctx.snapshot()
    pct = snap["visit"] / ctx.total_visits if ctx.total_visits else 0
    bar = _progress_bar(pct)
    mode_disp = "🌐 IP Rotation" if ctx.mode == "ROTATING" else "🟢 Direct"
    proxy_block = ""
    if ctx.mode == "ROTATING":
        proxy_block = (
            f"\n🌐 <b>Proxy:</b> <code>{html.escape(snap['last_proxy_display'])}</code>"
            f" {'(' + snap['last_proxy_protocol'] + ')' if snap['last_proxy_protocol'] else ''}"
            f"\n📡 <b>Exit IP:</b> <code>{html.escape(snap['last_exit_ip'] or '—')}</code>"
            f"\n⚡ <b>Latency:</b> <code>{snap['last_latency_ms']:.0f} ms</code>"
        )
    if snap["last_error"] and snap["failed"] > 0:
        proxy_block += f"\n⚠️ <i>{html.escape(snap['last_error'][:80])}</i>"
    return (
        f"⏳ <b>EXTRACTION IN PROGRESS</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Mode:</b> {mode_disp}\n\n"
        f"<b>Progress:</b> <code>[{bar}]</code> {int(pct * 100)}%\n\n"
        f"🔄 <b>Visits:</b> <code>{snap['visit']}/{ctx.total_visits}</code>\n"
        f"✅ <b>Successful:</b> <code>{snap['success']}</code>\n"
        f"❌ <b>Failed:</b> <code>{snap['failed']}</code>\n"
        f"📱 <b>Unique Numbers:</b> <code>{snap['unique']}</code>\n"
        f"♻️ <b>Duplicates:</b> <code>{snap['duplicates']}</code>"
        f"{proxy_block}\n"
        f"⏱ <b>Elapsed:</b> <code>{snap['elapsed']}s</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔄 <i>{html.escape(snap['stage'])}</i>"
    )


def progress_updater(ctx: JobContext) -> None:
    """Background thread: edits Telegram at a safe interval, independent of
    how fast/slow the network worker is. Never throws out of the job."""
    interval = get_setting_float("progress_interval", PROGRESS_INTERVAL)
    interval = max(0.7, min(interval, 1.6))
    last_text = ""
    while not ctx.cancel.is_set():
        try:
            text = _render_progress(ctx)
            if text != last_text:
                last_text = text
                try:
                    bot.edit_message_text(
                        chat_id=ctx.chat_id,
                        message_id=ctx.message_id,
                        text=text,
                    )
                except ApiTelegramException as e:
                    # "message is not modified" — fine, just skip
                    if "not modified" not in str(e).lower():
                        logger.debug("progress edit: %s", e)
                except Exception as e:
                    logger.debug("progress edit: %s", e)
        except Exception:
            pass
        time.sleep(interval)


# =========================================================
# Channel auto-post
# =========================================================
def _mask_id(uid: int) -> str:
    s = str(uid)
    if len(s) <= 4:
        return "*" * len(s)
    return "*" * (len(s) - 4) + s[-4:]


def _try_resolve_channel(username: str) -> Optional[int]:
    """Resolve a @channel username to a chat id. None on failure."""
    if not username:
        return None
    username = username.lstrip("@")
    try:
        # send a no-op get_chat via the bot; if it lacks permission this throws.
        chat = bot.get_chat(f"@{username}")
        return chat.id
    except Exception as e:
        logger.info("channel resolve %s: %s", username, e)
        return None


def post_extraction_to_channel(job_id: int, ctx_snapshot: dict,
                               numbers: list[str], username: str) -> str:
    """
    Publish a completed extraction to the configured channel. Returns a status
    string: 'POSTED', 'NO_CHANNEL', 'NO_PERMISSION', 'DISABLED', 'ERROR:...'.
    Never raises into the caller; never blocks extraction completion.
    """
    if not get_setting_bool("channel_enabled", False):
        return "DISABLED"
    channel = get_setting("channel_username", DEFAULT_CHANNEL)
    if not channel:
        return "NO_CHANNEL"
    bot_name = get_setting("bot_name", DEFAULT_BOT_NAME)

    include_user = get_setting_bool("channel_post_summary", True)
    include_numbers = get_setting_bool("channel_post_numbers", False)
    attach_txt = get_setting_bool("channel_attach_txt", True)

    mode_disp = "🌐 IP Rotation" if ctx_snapshot.get("mode") == "ROTATING" else "🟢 Direct"

    lines = [
        f"📡 <b>EXTRACTION COMPLETED</b>",
        f"━━━━━━━━━━━━━━━━━━",
    ]
    if include_user:
        u_show = f"@{username}" if username else "(unknown)"
        lines.append(f"👤 <b>User:</b> {html.escape(u_show)}")
        lines.append(f"🆔 <b>User ID:</b> <code>{_mask_id(ctx_snapshot.get('user_id', 0))}</code>")
    source = ctx_snapshot.get("url", "")
    src_disp = urllib.parse.urlparse(source).netloc or source[:60]
    lines.append(f"🔗 <b>Source:</b> <code>{html.escape(src_disp)}</code>")
    lines.append(f"⚙️ <b>Method:</b> {mode_disp}")
    lines.append(f"🔄 <b>Visits:</b> {ctx_snapshot.get('total_visits', 0)}")
    lines.append(f"✅ <b>Successful:</b> {ctx_snapshot.get('success', 0)}")
    lines.append(f"❌ <b>Failed:</b> {ctx_snapshot.get('failed', 0)}")
    lines.append(f"📱 <b>Unique Numbers:</b> {ctx_snapshot.get('unique', 0)}")
    lines.append(f"♻️ <b>Duplicates:</b> {ctx_snapshot.get('duplicates', 0)}")
    lines.append("━━━━━━━━━━━━━━━━━━")
    if include_numbers and numbers:
        shown = numbers[:30]
        lines.append("📞 <b>Numbers:</b>")
        for n in shown:
            lines.append(f"<code>+{n}</code>")
        if len(numbers) > 30:
            lines.append(f"<i>… and {len(numbers) - 30} more</i>")
        lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append(f"🆔 <b>Job ID:</b> #{job_id}")
    lines.append(f"🕒 {datetime.now().strftime('%d %b %Y, %H:%M')}")
    lines.append(f"🤖 {html.escape(bot_name)}")
    text = "\n".join(lines)

    try:
        sent = bot.send_message(channel, text, disable_web_page_preview=True)
        # Optionally attach the .txt
        if attach_txt and numbers:
            try:
                file_data = (
                    f"{bot_name} - Extraction Results\n"
                    f"{'=' * 45}\n"
                    f"Job ID: #{job_id}\n"
                    f"Source: {source}\n"
                    f"Mode: {mode_disp}\n"
                    f"Visits: {ctx_snapshot.get('total_visits', 0)}\n"
                    f"Successful: {ctx_snapshot.get('success', 0)}\n"
                    f"Failed: {ctx_snapshot.get('failed', 0)}\n"
                    f"Unique: {ctx_snapshot.get('unique', 0)}\n"
                    f"{'=' * 45}\n\n"
                    + "\n".join(f"+{n}" for n in numbers)
                    + "\n"
                )
                bio = io.BytesIO(file_data.encode("utf-8"))
                bio.name = f"job_{job_id}_numbers.txt"
                bot.send_document(channel, bio)
            except Exception as e:
                logger.info("channel txt upload: %s", e)
        return "POSTED"
    except ApiTelegramException as e:
        msg = str(e)
        if "CHAT_ADMIN_REQUIRED" in msg or "not enough rights" in msg.lower():
            return "NO_PERMISSION"
        if "chat not found" in msg.lower():
            return "NO_CHANNEL"
        return f"ERROR:{msg[:60]}"
    except Exception as e:
        return f"ERROR:{str(e)[:60]}"


def _run_proxy_tests_in_bg(chat_id: int, status_msg_id: int,
                           endpoints_raw: list[str],
                           cancel_event: threading.Event,
                           detailed: bool = False) -> None:
    """Bounded concurrent proxy testing with live progress + summary cards."""
    total = len(endpoints_raw)
    results: list[tuple[str, ProxyTestResult]] = []
    tested = working = dead = 0
    last_update = 0.0
    workers = max(2, min(get_setting_int("proxy_test_concurrency", 8), 16))

    def _test_one(raw: str) -> tuple[str, ProxyTestResult]:
        parsed, err = parse_proxy(raw)
        if not parsed:
            return raw, ProxyTestResult(
                ProxyManager.sanitize_display(raw), "UNKNOWN", False, None, None,
                f"Parse error: {err}", "INVALID")
        return raw, test_proxy(parsed, quick=not detailed)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_test_one, raw): raw for raw in endpoints_raw}
            for fut in concurrent.futures.as_completed(futures):
                if cancel_event.is_set():
                    for f in futures:
                        f.cancel()
                    break
                raw, result = fut.result()
                results.append((raw, result))
                tested += 1
                if result.working:
                    working += 1
                    row = db_get_proxy_by_endpoint(raw)
                    if row:
                        db_update_proxy_success(row["id"], result.latency_ms or 0,
                                                 result.observed_ip or "")
                else:
                    dead += 1
                    row = db_get_proxy_by_endpoint(raw)
                    if row:
                        db_update_proxy_failure(row["id"], result.error_reason or "Test failed",
                                                 result.status_label)
                now = time.time()
                if (now - last_update > 1.5) or tested == total:
                    last_update = now
                    pct = int((tested / total) * 100) if total else 100
                    bar = _progress_bar(tested / total if total else 1)
                    txt = (
                        f"🧪 <b>Testing proxies...</b>\n\n"
                        f"<code>[{bar}]</code> {pct}%\n\n"
                        f"Tested: {tested}/{total}\n"
                        f"✅ Working: {working}\n"
                        f"❌ Failed: {dead}"
                    )
                    try:
                        bot.edit_message_text(chat_id=chat_id,
                                               message_id=status_msg_id, text=txt)
                    except Exception:
                        pass
    except Exception as e:
        logger.warning("proxy test bg error: %s", e)

    fast = sum(1 for _, r in results if r.working and (r.latency_ms or 9999) < 1500)
    slow = working - fast
    summary = (
        f"🧪 <b>PROXY TEST RESULTS</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"Total: {total}\n"
        f"✅ Working: {working}\n"
        f"⚡ Fast: {fast}\n"
        f"🐢 Slow: {slow}\n"
        f"❌ Dead: {dead}\n\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=status_msg_id, text=summary)
    except Exception:
        try:
            bot.send_message(chat_id, summary)
        except Exception:
            pass

    CARDS_PER_MSG = 4
    card_lines: list[str] = []
    for idx, (_, r) in enumerate(results, 1):
        card_lines.append(r.to_telegram_card(index=idx))
        if len(card_lines) == CARDS_PER_MSG or idx == len(results):
            block = "\n\n━━━━━━━━━━━━━━━━━━\n\n".join(card_lines)
            try:
                bot.send_message(chat_id, block)
            except Exception:
                pass
            card_lines = []
            time.sleep(0.4)


# =========================================================
# Extraction Worker (uses JobContext + independent progress thread)
# =========================================================
_CYCLE_COUNT_MAP = {
    "🧪 Test — 1 Visit": 1,
    "🚀 20 Visits": 20,
    "⚡ 50 Visits": 50,
    "💎 100 Visits": 100,
}


def extraction_worker(ctx: JobContext, username: str, job_id: int) -> None:
    """Run the full extraction pipeline for one job. Records per-number
    method, per-visit proxy attempts, and never silently falls back to direct."""
    logger.info("JOB_START job=%d user=%d mode=%s visits=%d url=%s",
                job_id, ctx.user_id, ctx.mode, ctx.total_visits, _scrub(ctx.url))

    found_pairs: list[tuple[str, str]] = []
    found_set: set[str] = set()
    total_seen = 0
    failed_in_row = 0
    excluded: set[str] = set()  # proxies that failed this job

    # Start the independent progress updater
    updater = threading.Thread(target=progress_updater, args=(ctx,), daemon=True)
    updater.start()

    shared_session: Optional[ExtractionSession] = None
    if ctx.mode == "NORMAL":
        shared_session = ExtractionSession(proxy_endpoint=None)

    try:
        for cycle in range(1, ctx.total_visits + 1):
            if ctx.cancel.is_set():
                logger.info("JOB_CANCELLED job=%d at cycle %d", job_id, cycle)
                break

            ctx.visit = cycle
            ctx.stage = "Preparing request…"

            target = add_cache_buster(ctx.url, cycle)
            session: Optional[ExtractionSession] = shared_session
            endpoint_raw: Optional[str] = None

            # ── IP Rotation: select a healthy proxy, retry with another on failure ──
            if ctx.mode == "ROTATING":
                ctx.stage = "Selecting proxy…"
                endpoint_raw = proxy_manager.get_next_endpoint(exclude=excluded)
                if endpoint_raw is None:
                    # Honest failure: no proxy → stop, do NOT fall back to direct.
                    logger.warning("JOB_STOPPED job=%d no proxy available at cycle %d",
                                   job_id, cycle)
                    with ctx._lock:
                        ctx.stage = "❌ No verified proxy available"
                        ctx.last_error = "All proxies in cooldown or none configured"
                    break
                session = ExtractionSession(proxy_endpoint=endpoint_raw)
                parsed, _ = parse_proxy(endpoint_raw)
                with ctx._lock:
                    ctx.last_proxy_display = ProxyManager.sanitize_display(endpoint_raw)
                    ctx.last_proxy_protocol = parsed.protocol_label if parsed else ""

            ctx.stage = "Connecting…" if ctx.mode == "NORMAL" else "Fetching via proxy…"
            cycle_start = time.perf_counter()

            visit_failed = False
            try:
                final_url, body, visited_urls = session.fetch(target)
                latency_ms = (time.perf_counter() - cycle_start) * 1000

                # Extract from URL chain + body
                ctx.stage = "Scanning response…"
                raw_pairs = extract_from_url_chain(visited_urls)
                raw_pairs.extend(extract_numbers_from_text(body))
                unique_this, dup_this = dedupe_with_method(raw_pairs)
                total_seen += len(raw_pairs)

                new_in_this = 0
                for num, method in unique_this:
                    if num not in found_set:
                        found_set.add(num)
                        found_pairs.append((num, method))
                        new_in_this += 1
                        # Persist per-number record
                        exit_ip = ctx.last_exit_ip if ctx.mode == "ROTATING" else ""
                        try:
                            insert_extracted_number(job_id, ctx.user_id, num,
                                                     final_url or ctx.url, method,
                                                     cycle, exit_ip)
                        except Exception:
                            pass

                with ctx._lock:
                    ctx.success += 1
                    ctx.failed_in_row = 0 if hasattr(ctx, "failed_in_row") else 0
                    ctx.unique += new_in_this
                    ctx.duplicates += dup_this
                    ctx.last_latency_ms = latency_ms
                    ctx.last_error = ""
                    if new_in_this:
                        ctx.stage = f"Found {new_in_this} new number(s)"
                    else:
                        ctx.stage = "Visit complete"

                update_job_counts(job_id, success=1, unique=new_in_this, dup=dup_this)

                # ── Determine exit IP for rotation jobs ──
                if ctx.mode == "ROTATING" and endpoint_raw:
                    # Extract observed IP from the visited chain / body if available.
                    observed = ""
                    for u in visited_urls:
                        m = re.search(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b", u)
                        if m:
                            observed = m.group(1)
                            break
                    if not observed:
                        observed = _extract_ip_from_text(body[:4000]) or ""
                    if observed:
                        with ctx._lock:
                            ctx.last_exit_ip = observed
                    proxy_manager.mark_success(endpoint_raw, latency_ms, observed)
                    try:
                        parsed, _ = parse_proxy(endpoint_raw)
                        insert_proxy_attempt(job_id, cycle,
                                              ProxyManager.sanitize_display(endpoint_raw),
                                              parsed.protocol_label if parsed else "",
                                              "SUCCESS", latency_ms, observed, "")
                    except Exception:
                        pass

            except Exception as exc:
                visit_failed = True
                err_str = str(exc)[:120]
                logger.warning("JOB_CYCLE_FAIL job=%d cycle=%d proxy=%s err=%s",
                               job_id, cycle, _scrub(endpoint_raw or "direct"), err_str)
                with ctx._lock:
                    ctx.failed += 1
                    ctx.last_error = err_str
                    ctx.stage = "Retrying with another proxy…" if ctx.mode == "ROTATING" else "Visit failed"
                update_job_counts(job_id, failed=1)
                if ctx.mode == "ROTATING" and endpoint_raw:
                    excluded.add(endpoint_raw)
                    proxy_manager.mark_failed(endpoint_raw, err_str, "DEAD")
                    try:
                        parsed, _ = parse_proxy(endpoint_raw)
                        insert_proxy_attempt(job_id, cycle,
                                              ProxyManager.sanitize_display(endpoint_raw),
                                              parsed.protocol_label if parsed else "",
                                              "FAILED", 0, "", err_str)
                    except Exception:
                        pass
            finally:
                if ctx.mode == "ROTATING" and session:
                    try:
                        session.close()
                    except Exception:
                        pass

            time.sleep(0.05)

    finally:
        if shared_session:
            try:
                shared_session.close()
            except Exception:
                pass
        with _state_lock:
            active_jobs.pop(ctx.user_id, None)

    # ── Finalize ──
    duration_ms = int((time.time() - ctx.start_time) * 1000)
    unique_count = len(found_pairs)
    duplicate_count = max(total_seen - unique_count, 0)
    cancelled = ctx.cancel.is_set()

    if not cancelled and ctx.mode == "ROTATING" and ctx.failed > 0 and ctx.success == 0:
        status = "FAILED"
    elif cancelled:
        status = "CANCELLED"
    else:
        status = "COMPLETED"

    finish_job(job_id, status, duration_ms)
    update_user_stats(ctx.user_id, unique_count, success=(status == "COMPLETED"))
    save_history(ctx.user_id, ctx.url, ctx.mode, ctx.total_visits,
                 unique_count, duplicate_count)

    logger.info("JOB_COMPLETE job=%d status=%s unique=%d ok=%d fail=%d",
                job_id, status, unique_count, ctx.success, ctx.failed)

    # Force a final progress render
    with ctx._lock:
        ctx.stage = "Completed" if status == "COMPLETED" else (
            "Cancelled" if status == "CANCELLED" else "Failed — no working proxy")
    try:
        bot.edit_message_text(chat_id=ctx.chat_id, message_id=ctx.message_id,
                               text=_render_progress(ctx))
    except Exception:
        pass

    _send_final_result(ctx, job_id, status, found_pairs, duplicate_count)
    _maybe_channel_post(ctx, job_id, found_pairs, username)


def _send_final_result(ctx: JobContext, job_id: int, status: str,
                       found_pairs: list[tuple[str, str]], duplicates: int) -> None:
    """Send the user-facing completion message + optional .txt file."""
    sorted_nums = sorted({n for n, _ in found_pairs})
    unique_count = len(sorted_nums)
    mode_disp = "🌐 IP Rotation" if ctx.mode == "ROTATING" else "🟢 Direct"
    note = ""
    if status == "CANCELLED":
        note = " <i>(cancelled early)</i>"
    elif status == "FAILED":
        note = " <i>(no verified proxy was available — IP Rotation stopped)</i>"

    if unique_count > 0:
        numbers_plain = "\n".join(f"+{n}" for n in sorted_nums)
        # Show numbers inline, paginated to avoid huge messages
        CHUNK = 3200
        lines = [f"+{n}" for n in sorted_nums]
        chunks: list[str] = []
        cur = ""
        for ln in lines:
            if len(cur) + len(ln) + 1 > CHUNK:
                chunks.append(cur.strip()); cur = ln + "\n"
            else:
                cur += ln + "\n"
        if cur.strip():
            chunks.append(cur.strip())

        header = (
            f"✅ <b>EXTRACTION COMPLETED</b>{note}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>Job:</b> #{job_id}\n"
            f"🔗 <b>Source:</b> <code>{html.escape(ctx.url[:80])}</code>\n"
            f"⚙️ <b>Mode:</b> {mode_disp}\n\n"
            f"🔄 <b>Visits:</b> {ctx.visit}/{ctx.total_visits}\n"
            f"✅ <b>Successful:</b> {ctx.success}\n"
            f"❌ <b>Failed:</b> {ctx.failed}\n\n"
            f"📱 <b>Unique Numbers:</b> {unique_count}\n"
            f"♻️ <b>Duplicates:</b> {duplicates}\n"
            f"⏱ <b>Duration:</b> {int((time.time() - ctx.start_time))}s\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📱 <b>NUMBERS</b>\n<code>{html.escape(chunks[0])}</code>"
        )
        try:
            bot.send_message(ctx.chat_id, header,
                              reply_markup=build_copy_markup(numbers_plain))
        except Exception:
            bot.send_message(ctx.chat_id, header)
        for i, extra in enumerate(chunks[1:], start=2):
            try:
                bot.send_message(ctx.chat_id,
                                  f"📱 <b>Numbers (Part {i})</b>\n<code>{html.escape(extra)}</code>")
            except Exception:
                pass

        # Always attach a .txt for convenience
        try:
            bot_name = get_setting("bot_name", DEFAULT_BOT_NAME)
            file_data = (
                f"{bot_name} - Extraction Results\n"
                f"{'=' * 45}\n"
                f"Job ID: #{job_id}\n"
                f"Source URL: {ctx.url}\n"
                f"Mode: {mode_disp}\n"
                f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Total Visits: {ctx.total_visits}\n"
                f"Successful Visits: {ctx.success}\n"
                f"Failed Visits: {ctx.failed}\n"
                f"Unique Numbers: {unique_count}\n"
                f"Duplicates Filtered: {duplicates}\n"
                f"{'=' * 45}\n\n"
                f"Numbers\n--------\n{numbers_plain}\n"
            )
            bio = io.BytesIO(file_data.encode("utf-8"))
            bio.name = f"job_{job_id}_numbers.txt"
            bot.send_document(ctx.chat_id, bio,
                              caption=f"📁 <b>Job #{job_id}</b> • <code>{unique_count}</code> numbers",
                              reply_markup=main_keyboard())
        except Exception as e:
            logger.warning("file upload: %s", e)
            bot.send_message(ctx.chat_id,
                              f"⚠️ File upload error: {html.escape(str(e)[:80])}",
                              reply_markup=main_keyboard())
    else:
        bot.send_message(
            ctx.chat_id,
            (
                f"⚠️ <b>No numbers found</b>{note}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🆔 <b>Job:</b> #{job_id}\n"
                f"🔗 <b>Source:</b> <code>{html.escape(ctx.url[:80])}</code>\n"
                f"⚙️ <b>Mode:</b> {mode_disp}\n"
                f"🔄 <b>Visits Completed:</b> {ctx.success}/{ctx.total_visits}\n"
                f"❌ <b>Failed:</b> {ctx.failed}\n\n"
                f"<i>The target may not expose trackable phone patterns, "
                f"or the request could not be completed.</i>"
            ),
            reply_markup=main_keyboard(),
        )


def _maybe_channel_post(ctx: JobContext, job_id: int,
                        found_pairs: list[tuple[str, str]], username: str) -> None:
    try:
        snap = {
            "user_id": ctx.user_id, "url": ctx.url, "mode": ctx.mode,
            "total_visits": ctx.total_visits, "success": ctx.success,
            "failed": ctx.failed, "unique": ctx.unique, "duplicates": ctx.duplicates,
        }
        nums = sorted({n for n, _ in found_pairs})
        result = post_extraction_to_channel(job_id, snap, nums, username)
        if result not in ("POSTED", "DISABLED"):
            logger.info("JOB_CHANNEL_POST job=%d status=%s", job_id, result)
    except Exception as e:
        logger.warning("channel post error: %s", e)


# =========================================================
# Keyboards
# =========================================================
def main_keyboard(user_id: int = 0) -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🔗 Send New Link"),
        types.KeyboardButton("📊 My Stats"),
        types.KeyboardButton("📋 My History"),
        types.KeyboardButton("❓ Help"),
        types.KeyboardButton("📞 Support"),
    )
    if user_id and is_admin(user_id):
        markup.add(types.KeyboardButton("🔐 Admin Panel"))
    return markup


def mode_selection_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🟢 Direct Connection"),
        types.KeyboardButton("🌐 IP Rotation"),
        types.KeyboardButton("🔙 Back"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def extraction_cycles_keyboard() -> types.ReplyKeyboardMarkup:
    max_v = get_setting_int("max_visits", MAX_VISITS_PER_JOB)
    rows = [
        ("🧪 Test — 1 Visit", "🚀 20 Visits"),
        ("⚡ 50 Visits", "💎 100 Visits"),
        ("✍️ Custom", "🔙 Back"),
        ("❌ Cancel",),
    ]
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(*[types.KeyboardButton(b) for row in rows for b in row])
    return markup


def admin_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    maint_label = "🔴 Maintenance: ON" if maintenance_on() else "🟢 Maintenance: OFF"
    markup.add(
        types.KeyboardButton("📊 Dashboard"),
        types.KeyboardButton("👥 Users"),
        types.KeyboardButton("🔎 Extraction Logs"),
        types.KeyboardButton("🌐 Proxy Manager"),
        types.KeyboardButton("📢 Broadcast"),
        types.KeyboardButton("📡 Channel Settings"),
        types.KeyboardButton("⚙️ Bot Settings"),
        types.KeyboardButton("👮 Admin Management"),
        types.KeyboardButton("📜 Audit Log"),
        types.KeyboardButton(maint_label),
        types.KeyboardButton("🔙 Main Menu"),
    )
    return markup


def proxy_manager_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("➕ Add Single Proxy"),
        types.KeyboardButton("📦 Bulk Add Proxies"),
        types.KeyboardButton("🧪 Test All Proxies"),
        types.KeyboardButton("🔄 Retest Failed"),
        types.KeyboardButton("📋 List All Proxies"),
        types.KeyboardButton("📊 Proxy Statistics"),
        types.KeyboardButton("🗑️ Delete Proxy"),
        types.KeyboardButton("🗑️ Clear Dead Proxies"),
        types.KeyboardButton("🧪 System Diagnostics"),
        types.KeyboardButton("🔙 Admin Panel"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def cancel_only_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("❌ Cancel"))
    return markup


def back_cancel_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🔙 Back"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def build_copy_markup(numbers_text: str) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    try:
        markup.add(types.InlineKeyboardButton(
            text="📋 Copy All Numbers",
            copy_text=types.CopyTextButton(text=numbers_text),
        ))
    except Exception:
        markup.add(types.InlineKeyboardButton(
            text="📋 Copy All Numbers",
            switch_inline_query=numbers_text[:250],
        ))
    return markup


def proxy_status_emoji(row: dict) -> str:
    status = row.get("health_status", "UNTESTED")
    return {
        "WORKING": "🟢", "SLOW": "🟡", "AUTH_FAILED": "🟠",
        "TCP_FAILED": "🔴", "DEAD": "🔴", "INVALID": "⚫",
        "COOLDOWN": "🔵",
    }.get(status, "⚪")


# =========================================================
# Command Handlers
# =========================================================
@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message) -> None:
    user = message.from_user
    register_user(user.id, user.username, user.first_name)

    # Approval flow: brand-new user when approval mode is on → mark PENDING
    if approval_required() and not is_admin(user.id):
        st = user_status(user.id)
        if st == "NEW":
            set_user_status(user.id, "PENDING")
            try:
                # Notify admins of the pending request
                for admin_id in ADMIN_IDS:
                    markup = types.InlineKeyboardMarkup(row_width=2)
                    markup.add(
                        types.InlineKeyboardButton("✅ Approve",
                            callback_data=f"approve_{user.id}"),
                        types.InlineKeyboardButton("❌ Reject",
                            callback_data=f"reject_{user.id}"),
                    )
                    bot.send_message(admin_id,
                        f"👤 <b>New Access Request</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"Name: {html.escape(user.first_name or '—')}\n"
                        f"Username: @{html.escape(user.username or '—')}\n"
                        f"ID: <code>{user.id}</code>",
                        reply_markup=markup)
            except Exception:
                pass
            bot.send_message(message.chat.id,
                "🔒 <b>ACCESS PENDING</b>\n\n"
                "Your access request has been submitted.\n"
                "Please wait for administrator approval.")
            return
        if st == "PENDING":
            bot.send_message(message.chat.id,
                "🔒 <b>ACCESS PENDING</b>\n\n"
                "Your request is awaiting administrator approval.")
            return
        if st == "BLOCKED":
            bot.send_message(message.chat.id,
                "🚫 <b>Access Revoked</b>\n\nContact the administrator.")
            return

    welcome = get_setting("welcome_text", "Welcome to the URL Extraction Center.")
    bot_name = get_setting("bot_name", DEFAULT_BOT_NAME)
    bot.send_message(
        message.chat.id,
        (
            f"👋 <b>{html.escape(welcome)}</b>\n\n"
            f"🤖 <b>{html.escape(bot_name)}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<b>How it works:</b>\n"
            f"1️⃣ Tap <b>🔗 Send New Link</b>\n"
            f"2️⃣ Paste any valid HTTP/HTTPS link\n"
            f"3️⃣ Choose a connection mode\n"
            f"4️⃣ Pick a visit count\n"
            f"5️⃣ Receive unique numbers with Copy + .txt download\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Select an option below:</b>"
        ),
        reply_markup=main_keyboard(user.id),
    )


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "❌ <b>Access Denied.</b>")
        return
    bot.send_message(
        message.chat.id,
        "🔐 <b>ADMIN CONTROL CENTER</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Select an administrative function:",
        reply_markup=admin_keyboard(),
    )


# =========================================================
# Inline callback handlers (approve/reject, proxy delete)
# =========================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("approve_"))
def cb_approve(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    target = int(call.data.split("_", 1)[1])
    approve_user(target, call.from_user.id)
    bot.answer_callback_query(call.id, "✅ User approved.")
    try:
        bot.edit_message_text(call.message.chat.id, call.message.message_id,
            f"✅ <b>User {target} approved</b> by <code>{call.from_user.id}</code>.")
    except Exception:
        pass
    try:
        bot.send_message(target, "✅ <b>Your access has been approved.</b>\nYou can now use the bot.")
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("reject_"))
def cb_reject(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    target = int(call.data.split("_", 1)[1])
    block_user(target, "Rejected by admin", call.from_user.id)
    bot.answer_callback_query(call.id, "❌ User rejected.")
    try:
        bot.edit_message_text(call.message.chat.id, call.message.message_id,
            f"❌ <b>User {target} rejected</b> by <code>{call.from_user.id}</code>.")
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("del_proxy_"))
def cb_del_proxy(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    pid = call.data.replace("del_proxy_", "")
    if pid.isdigit():
        db_delete_proxy(int(pid))
        bot.answer_callback_query(call.id, "✅ Proxy deleted.")
        try:
            bot.edit_message_text(call.message.chat.id, call.message.message_id,
                f"🗑️ <i>Proxy #{pid} deleted.</i>")
        except Exception:
            pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("job_"))
def cb_job_detail(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    jid = int(call.data.split("_", 1)[1])
    job = get_job(jid)
    if not job:
        bot.answer_callback_query(call.id, "Job not found.")
        return
    nums = get_job_numbers(jid, limit=50)
    attempts = get_job_attempts(jid, limit=50)
    mode_disp = "🌐 IP Rotation" if job["mode"] == "ROTATING" else "🟢 Direct"
    lines = [
        f"🔍 <b>JOB #{jid} DETAILS</b>",
        f"━━━━━━━━━━━━━━━━━━",
        f"👤 <b>User:</b> @{html.escape(job.get('username') or '—')}",
        f"🆔 <b>User ID:</b> <code>{job['user_id']}</code>",
        f"🔗 <b>URL:</b> <code>{html.escape(job['source_url'][:80])}</code>",
        f"⚙️ <b>Mode:</b> {mode_disp}",
        f"🔄 <b>Visits:</b> {job['successful_visits']}/{job['requested_visits']} (❌ {job['failed_visits']})",
        f"📱 <b>Unique:</b> {job['unique_numbers']} | ♻️ Duplicates: {job['duplicate_numbers']}",
        f"⏱ <b>Duration:</b> {(job['duration_ms'] or 0)/1000:.1f}s",
        f"🕒 <b>Status:</b> {job['status']}",
    ]
    if nums:
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("📱 <b>Numbers</b> (first 50):")
        for n in nums[:50]:
            lines.append(f"<code>+{html.escape(n['number'])}</code> · <i>{html.escape(n['extraction_method'])}</i> · v{n['visit_number']}")
    if attempts:
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("🌐 <b>Proxy Attempts</b> (first 30):")
        for a in attempts[:30]:
            lines.append(f"#{a['visit_number']} {html.escape(a['status'])} · <code>{html.escape(a['proxy_endpoint_safe'])}</code> · {a['latency_ms'] or 0:.0f}ms")
    try:
        bot.send_message(call.message.chat.id, "\n".join(lines))
    except Exception:
        bot.send_message(call.message.chat.id, "Job details too long to display inline.")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("user_"))
def cb_user_detail(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    uid = int(call.data.split("_", 1)[1])
    stats = get_user_stats(uid)
    if not stats:
        bot.answer_callback_query(call.id, "User not found.")
        return
    history = get_user_history(uid, limit=5)
    lines = [
        f"👤 <b>USER PROFILE</b>",
        f"━━━━━━━━━━━━━━━━━━",
        f"Name: {html.escape(stats.get('first_name') or '—')}",
        f"Username: @{html.escape(stats.get('username') or '—')}",
        f"Telegram ID: <code>{uid}</code>",
        f"Status: {stats.get('status', 'APPROVED')}",
        f"",
        f"📊 <b>Statistics</b>",
        f"Total Extractions: {stats.get('total_extractions', 0)}",
        f"Successful: {stats.get('successful_extractions', 0)}",
        f"Failed: {stats.get('failed_extractions', 0)}",
        f"Total Numbers Found: {stats.get('total_numbers_found', 0)}",
        f"",
        f"🕒 Joined: {str(stats.get('joined_at') or '—')[:19]}",
        f"🕒 Last Active: {str(stats.get('last_active') or '—')[:19]}",
    ]
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🚫 Block", callback_data=f"block_{uid}"),
        types.InlineKeyboardButton("✅ Unblock", callback_data=f"unblock_{uid}"),
    )
    try:
        bot.send_message(call.message.chat.id, "\n".join(lines), reply_markup=markup)
    except Exception:
        pass
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith(("block_", "unblock_")))
def cb_block_unblock(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    action, uid = call.data.split("_", 1)
    uid = int(uid)
    if action == "block":
        block_user(uid, "Blocked by admin", call.from_user.id)
        bot.answer_callback_query(call.id, "🚫 User blocked.")
    else:
        unblock_user(uid, call.from_user.id)
        bot.answer_callback_query(call.id, "✅ User unblocked.")
    try:
        bot.edit_message_text(call.message.chat.id, call.message.message_id,
            f"{action.title()}ed user {uid}.")
    except Exception:
        pass


# =========================================================
# Main Message Router
# =========================================================
@bot.message_handler(func=lambda m: True)
def handle_all_messages(message: types.Message) -> None:
    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()
    register_user(user.id, user.username, user.first_name)

    # Per-user lock so a single user cannot race two flows at once.
    lk = _user_lock(user.id)
    if not lk.acquire(blocking=False):
        bot.send_message(chat_id, "⏳ <i>Finishing your previous action…</i> Try again in a moment.")
        return
    try:
        _route_message(user, chat_id, text)
    finally:
        lk.release()


def _route_message(user, chat_id: int, text: str) -> None:
    with _state_lock:
        state = dict(user_states.get(user.id, {}))
    is_admin_user = is_admin(user.id)

    # ── Maintenance check (admins bypass) ──
    if maintenance_on() and not is_admin_user:
        bot.send_message(chat_id, "🛠 <b>BOT UNDER MAINTENANCE</b>\n\nPlease try again later.")
        return

    # ── Global Cancel ──
    if text == "❌ Cancel":
        ctx = active_jobs.get(user.id)
        if ctx:
            ctx.cancel.set()
            bot.send_message(chat_id, "🛑 <b>Cancellation requested.</b> Stopping after current step…",
                              reply_markup=main_keyboard(user.id))
        else:
            with _state_lock:
                user_states[user.id] = {}
            bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard(user.id))
        return

    if text == "🔙 Main Menu":
        with _state_lock:
            user_states[user.id] = {}
        bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard(user.id))
        return

    # ── Back navigation ──
    if text == "🔙 Back":
        step = state.get("step")
        if step == "AWAITING_MODE":
            with _state_lock:
                user_states[user.id] = {"step": "AWAITING_URL"}
            bot.send_message(chat_id, "🔗 <b>Submit Target URL</b>\n\nSend a valid HTTP/HTTPS link:",
                              reply_markup=cancel_only_keyboard())
            return
        if step == "AWAITING_CYCLES":
            with _state_lock:
                user_states[user.id] = {"step": "AWAITING_MODE", "url": state.get("url")}
            bot.send_message(chat_id, "Choose Extraction Mode:",
                              reply_markup=mode_selection_keyboard())
            return
        if is_admin_user and state.get("step", "").startswith("ADMIN_"):
            with _state_lock:
                user_states[user.id] = {}
            bot.send_message(chat_id, "🔐 <b>Admin Control Center</b>", reply_markup=admin_keyboard())
            return
        with _state_lock:
            user_states[user.id] = {}
        bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard(user.id))
        return

    if is_admin_user and text == "🔙 Admin Panel":
        with _state_lock:
            user_states[user.id] = {}
        bot.send_message(chat_id, "🔐 <b>Admin Control Center</b>", reply_markup=admin_keyboard())
        return

    # ── Admin broadcast input ──
    if is_admin_user and state.get("awaiting_broadcast"):
        with _state_lock:
            user_states[user.id] = {}
        all_users = get_all_user_ids()
        sent = failed = 0
        status_msg = bot.send_message(chat_id, f"🚀 <b>Broadcasting to {len(all_users)} users...</b>")
        for uid in all_users:
            try:
                bot.send_message(uid, text)
                sent += 1
                time.sleep(0.05)
            except Exception:
                failed += 1
        try:
            bot.edit_message_text(chat_id=chat_id, message_id=status_msg.message_id,
                text=(f"✅ <b>Broadcast Completed</b>\n"
                      f"🎉 Sent: <code>{sent}</code>\n"
                      f"❌ Failed: <code>{failed}</code>"))
        except Exception:
            pass
        audit_log(user.id, "BROADCAST", "", f"sent={sent} failed={failed}")
        bot.send_message(chat_id, "🔐 <b>Admin Control Center</b>", reply_markup=admin_keyboard())
        return

    # ── Admin: settings input handlers ──
    if is_admin_user and state.get("step") == "ADMIN_SET_SETTING":
        with _state_lock:
            user_states[user.id] = {}
        key = state.get("setting_key")
        if key:
            set_setting(key, text)
            audit_log(user.id, "SET_SETTING", key, text[:200])
            bot.send_message(chat_id, f"✅ <b>{html.escape(key)}</b> set to: <code>{html.escape(text[:100])}</code>",
                              reply_markup=admin_keyboard())
        return

    if is_admin_user and state.get("step") == "ADMIN_ADD_PROXY":
        _handle_add_proxy(user.id, chat_id, text)
        return

    if is_admin_user and state.get("step") == "ADMIN_BULK_ADD_PROXIES":
        _handle_bulk_add(user.id, chat_id, text)
        return

    if is_admin_user and state.get("step") == "ADMIN_DELETE_PROXY":
        with _state_lock:
            user_states[user.id] = {}
        if text.isdigit():
            ok = db_delete_proxy(int(text))
            bot.send_message(chat_id,
                f"🗑️ Proxy #{html.escape(text)} deleted." if ok else f"⚠️ Proxy #{html.escape(text)} not found.",
                reply_markup=proxy_manager_keyboard())
        else:
            bot.send_message(chat_id, "⚠️ Send a numeric proxy ID.",
                              reply_markup=proxy_manager_keyboard())
        return

    if is_admin_user and state.get("step") == "ADMIN_ADD_ADMIN":
        with _state_lock:
            user_states[user.id] = {}
        try:
            target = int(text.strip())
        except Exception:
            bot.send_message(chat_id, "⚠️ Send a numeric Telegram ID.", reply_markup=admin_keyboard())
            return
        add_admin(target, "ADMIN", user.id)
        bot.send_message(chat_id, f"✅ Admin added: <code>{target}</code>", reply_markup=admin_keyboard())
        return

    if is_admin_user and state.get("step") == "ADMIN_REMOVE_ADMIN":
        with _state_lock:
            user_states[user.id] = {}
        try:
            target = int(text.strip())
        except Exception:
            bot.send_message(chat_id, "⚠️ Send a numeric Telegram ID.", reply_markup=admin_keyboard())
            return
        ok = remove_admin(target, user.id)
        bot.send_message(chat_id,
            f"✅ Admin removed: <code>{target}</code>" if ok else f"⚠️ Admin {target} not found.",
            reply_markup=admin_keyboard())
        return

    if is_admin_user and state.get("step") == "ADMIN_CUSTOM_VISITS":
        with _state_lock:
            user_states[user.id] = {}
        try:
            count = int(text.strip())
        except Exception:
            bot.send_message(chat_id, "⚠️ Send a number.", reply_markup=main_keyboard(user.id))
            return
        max_v = get_setting_int("max_visits", MAX_VISITS_PER_JOB)
        if count < 1:
            bot.send_message(chat_id, "⚠️ Must be at least 1.", reply_markup=main_keyboard(user.id))
            return
        if count > max_v:
            bot.send_message(chat_id, f"⚠️ Max is {max_v}.", reply_markup=main_keyboard(user.id))
            return
        # Trigger extraction with this custom count
        target_url = state.get("url", "")
        mode = state.get("mode", "NORMAL")
        _start_extraction(user, chat_id, target_url, mode, count)
        return

    # ── Admin menu buttons ──
    if is_admin_user:
        if text == "📊 Dashboard":
            s = get_admin_stats()
            ps = db_proxy_stats()
            avg_lat = ps.get("avg_latency")
            bot.send_message(chat_id,
                f"📊 <b>BOT DASHBOARD</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👥 <b>Users:</b> {s.get('users', 0)}\n"
                f"🟢 <b>Active Today:</b> {s.get('active_today', 0)}\n\n"
                f"🔄 <b>Total Jobs:</b> {s.get('jobs_total', 0)}\n"
                f"✅ <b>Successful:</b> {s.get('jobs_success', 0)}\n"
                f"❌ <b>Failed:</b> {s.get('jobs_failed', 0)}\n"
                f"⚡ <b>Running:</b> {s.get('jobs_running', 0)}\n\n"
                f"📱 <b>Total Numbers:</b> {s.get('numbers', 0)}\n"
                f"📅 <b>Today:</b> {s.get('numbers_today', 0)}\n"
                f"📆 <b>This Week:</b> {s.get('numbers_week', 0)}\n\n"
                f"🌐 <b>Proxies:</b> 🟢 {ps.get('working', 0)} · 🟡 {ps.get('slow', 0)} · 🔴 {ps.get('dead', 0)} · ⚪ {ps.get('untested', 0)}\n"
                f"⏱ <b>Avg Job Time:</b> {(s.get('avg_duration') or 0)/1000:.1f}s\n"
                f"━━━━━━━━━━━━━━━━━━",
                reply_markup=admin_keyboard())
            return

        if text == "👥 Users":
            _show_users_list(chat_id, page=0)
            return

        if text == "🔎 Extraction Logs":
            _show_jobs_list(chat_id, page=0)
            return

        if text == "🌐 Proxy Manager":
            total_eps = proxy_manager.get_endpoint_count()
            env_count = proxy_manager.env_count()
            db_count = len(db_get_all_proxies())
            ps = db_proxy_stats()
            bot.send_message(chat_id,
                f"🌐 <b>PROXY MANAGER</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📡 <b>Total Active:</b> <code>{total_eps}</code>\n"
                f"⚙️ <b>From Env:</b> <code>{env_count}</code>\n"
                f"💾 <b>From DB:</b> <code>{db_count}</code>\n"
                f"🟢 <b>Working:</b> <code>{ps.get('working', 0)}</code>\n"
                f"🟡 <b>Slow:</b> <code>{ps.get('slow', 0)}</code>\n"
                f"🔴 <b>Dead:</b> <code>{ps.get('dead', 0)}</code>\n"
                f"⚪ <b>Untested:</b> <code>{ps.get('untested', 0)}</code>\n"
                f"━━━━━━━━━━━━━━━━━━",
                reply_markup=proxy_manager_keyboard())
            return

        if text == "📢 Broadcast":
            with _state_lock:
                user_states[user.id] = {"awaiting_broadcast": True}
            bot.send_message(chat_id,
                "📢 <b>Broadcast Console</b>\n\nType the message to send to all users:",
                reply_markup=cancel_only_keyboard())
            return

        if text == "📡 Channel Settings":
            ch = get_setting("channel_username", DEFAULT_CHANNEL)
            enabled = "✅ ON" if get_setting_bool("channel_enabled", False) else "❌ OFF"
            post_nums = "✅ ON" if get_setting_bool("channel_post_numbers", False) else "❌ OFF"
            attach = "✅ ON" if get_setting_bool("channel_attach_txt", True) else "❌ OFF"
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("Toggle Channel Posting", callback_data="set_channel_enabled"),
                types.InlineKeyboardButton("Toggle Publish Numbers", callback_data="set_channel_post_numbers"),
                types.InlineKeyboardButton("Toggle Attach TXT", callback_data="set_channel_attach"),
                types.InlineKeyboardButton("🧪 Test Channel", callback_data="test_channel"),
            )
            bot.send_message(chat_id,
                f"📡 <b>CHANNEL SETTINGS</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📢 Channel: <code>{html.escape(ch)}</code>\n"
                f"Auto Post: {enabled}\n"
                f"Publish Numbers: {post_nums}\n"
                f"Attach TXT: {attach}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"<i>Use /admin and tap ⚙️ Bot Settings to change the channel username.</i>",
                reply_markup=markup)
            return

        if text == "⚙️ Bot Settings":
            _show_bot_settings(chat_id, user.id)
            return

        if text == "👮 Admin Management":
            conn = get_conn()
            try:
                rows = conn.execute("SELECT user_id, username, role FROM admins WHERE is_active=1").fetchall()
            finally:
                conn.close()
            lines = ["👮 <b>ADMIN MANAGEMENT</b>", "━━━━━━━━━━━━━━━━━━"]
            for r in rows:
                lines.append(f"• <code>{r['user_id']}</code> · {html.escape(r['role'] or 'ADMIN')} · @{html.escape(r['username'] or '—')}")
            lines.append("━━━━━━━━━━━━━━━━━━")
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("➕ Add Admin", callback_data="prompt_add_admin"),
                types.InlineKeyboardButton("➖ Remove Admin", callback_data="prompt_remove_admin"),
            )
            bot.send_message(chat_id, "\n".join(lines), reply_markup=markup)
            return

        if text == "📜 Audit Log":
            conn = get_conn()
            try:
                rows = conn.execute(
                    "SELECT * FROM admin_audit_log ORDER BY id DESC LIMIT 20"
                ).fetchall()
            finally:
                conn.close()
            if not rows:
                bot.send_message(chat_id, "📜 <i>No admin actions logged yet.</i>", reply_markup=admin_keyboard())
                return
            lines = ["📜 <b>RECENT ADMIN ACTIONS</b>", "━━━━━━━━━━━━━━━━━━"]
            for r in rows:
                lines.append(f"<code>{str(r['created_at'])[:19]}</code> · {html.escape(r['action'])} · {html.escape(r['target'] or '—')}")
            lines.append("━━━━━━━━━━━━━━━━━━")
            bot.send_message(chat_id, "\n".join(lines), reply_markup=admin_keyboard())
            return

        if text in ("🟢 Maintenance: OFF", "🔴 Maintenance: ON"):
            new_val = "0" if maintenance_on() else "1"
            set_setting("maintenance_mode", new_val)
            audit_log(user.id, "MAINTENANCE", new_val, "")
            status = "🔴 <b>ON</b> (admins only)" if new_val == "1" else "🟢 <b>OFF</b>"
            bot.send_message(chat_id, f"🔧 <b>Maintenance Mode:</b> {status}", reply_markup=admin_keyboard())
            return

        # Proxy manager sub-buttons
        if text == "➕ Add Single Proxy":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_ADD_PROXY"}
            bot.send_message(chat_id,
                "➕ <b>Add Single Proxy</b>\n\nSend in format:\n"
                "• <code>http://ip:port</code>\n"
                "• <code>http://user:pass@ip:port</code>\n"
                "• <code>socks5://ip:port</code>\n"
                "• <code>socks5://user:pass@ip:port</code>\n\n"
                "<i>Credentials are never displayed.</i>",
                reply_markup=back_cancel_keyboard())
            return

        if text == "📦 Bulk Add Proxies":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_BULK_ADD_PROXIES"}
            bot.send_message(chat_id,
                "📦 <b>Bulk Add Proxies</b>\n\nPaste one proxy per line:",
                reply_markup=back_cancel_keyboard())
            return

        if text == "🧪 Test All Proxies":
            all_eps = proxy_manager.get_all_raw()
            if not all_eps:
                bot.send_message(chat_id, "⚠️ No proxies configured.", reply_markup=proxy_manager_keyboard())
                return
            status_msg = bot.send_message(chat_id, f"🧪 <b>Testing {len(all_eps)} proxies…</b>")
            cancel_ev = threading.Event()
            threading.Thread(target=_run_proxy_tests_in_bg,
                              args=(chat_id, status_msg.message_id, all_eps, cancel_ev, False),
                              daemon=True).start()
            return

        if text == "🔄 Retest Failed":
            _trigger_retest_failed(chat_id)
            return

        if text == "📋 List All Proxies":
            _list_proxies(chat_id)
            return

        if text == "📊 Proxy Statistics":
            ps = db_proxy_stats()
            avg_lat = ps.get("avg_latency")
            avg_lat_str = f"{avg_lat:.0f} ms" if avg_lat else "N/A"
            bot.send_message(chat_id,
                f"📊 <b>Proxy Statistics</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📡 Total: <code>{ps.get('total', 0)}</code>\n"
                f"🟢 Working: <code>{ps.get('working', 0)}</code>\n"
                f"🟡 Slow: <code>{ps.get('slow', 0)}</code>\n"
                f"🔴 Dead: <code>{ps.get('dead', 0)}</code>\n"
                f"⚪ Untested: <code>{ps.get('untested', 0)}</code>\n"
                f"⚡ Avg Latency: <code>{avg_lat_str}</code>\n"
                f"━━━━━━━━━━━━━━━━━━",
                reply_markup=proxy_manager_keyboard())
            return

        if text == "🗑️ Delete Proxy":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_DELETE_PROXY"}
            bot.send_message(chat_id, "🗑️ Send the numeric <b>proxy ID</b> to delete:",
                              reply_markup=back_cancel_keyboard())
            return

        if text == "🗑️ Clear Dead Proxies":
            deleted = db_clear_dead_proxies()
            bot.send_message(chat_id, f"🗑️ Cleared {deleted} dead proxy entries.",
                              reply_markup=proxy_manager_keyboard())
            return

        if text == "🧪 System Diagnostics":
            _run_diagnostics(chat_id)
            return

    # ── Standard user menu ──
    if text == "🔗 Send New Link":
        with _state_lock:
            if user.id in active_jobs:
                bot.send_message(chat_id, "⚠️ <b>A job is already running!</b> Tap ❌ Cancel first.",
                                  reply_markup=cancel_only_keyboard())
                return
            user_states[user.id] = {"step": "AWAITING_URL"}
        allowed, reason = user_can_extract(user.id)
        if not allowed:
            with _state_lock:
                user_states[user.id] = {}
            bot.send_message(chat_id, reason, reply_markup=main_keyboard(user.id))
            return
        bot.send_message(chat_id,
            "🔗 <b>Submit Target URL</b>\n\nSend a valid HTTP/HTTPS link:\n\n"
            "<i>Example:</i> <code>https://example.com/redirect</code>",
            reply_markup=cancel_only_keyboard())
        return

    if text == "📊 My Stats":
        s = get_user_stats(user.id)
        if not s:
            bot.send_message(chat_id, "📊 No stats yet. Run an extraction first!", reply_markup=main_keyboard(user.id))
            return
        bot.send_message(chat_id,
            f"📊 <b>MY STATS</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔄 <b>Total Extractions:</b> {s.get('total_extractions', 0)}\n"
            f"✅ <b>Successful:</b> {s.get('successful_extractions', 0)}\n"
            f"❌ <b>Failed:</b> {s.get('failed_extractions', 0)}\n"
            f"📱 <b>Unique Numbers:</b> {s.get('total_numbers_found', 0)}\n"
            f"━━━━━━━━━━━━━━━━━━",
            reply_markup=main_keyboard(user.id))
        return

    if text == "📋 My History":
        history = get_user_history(user.id, limit=10)
        if not history:
            bot.send_message(chat_id, "📋 <b>No extraction history yet.</b>", reply_markup=main_keyboard(user.id))
            return
        msg = "📋 <b>MY HISTORY</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        for i, h in enumerate(history, 1):
            url_disp = html.escape(h["url"][:40] + ("…" if len(h["url"]) > 40 else ""))
            mode_lbl = "🌐 IP" if h.get("mode") == "ROTATING" else "🟢 Norm"
            msg += (f"<b>#{i}</b> [{mode_lbl}] <code>{str(h.get('completed_at') or '')[:16]}</code>\n"
                    f"🔗 <code>{url_disp}</code>\n"
                    f"🔄 {h['cycles']} visits · 📱 {h['unique_numbers']} numbers\n\n")
        bot.send_message(chat_id, msg, reply_markup=main_keyboard(user.id))
        return

    if text == "❓ Help":
        socks_status = "✅ Available" if _SOCKS5_AVAILABLE else "❌ Not installed"
        bot.send_message(chat_id,
            "❓ <b>Help</b>\n━━━━━━━━━━━━━━━━━━\n"
            "<b>Modes:</b>\n"
            "• 🟢 <b>Direct Connection</b> — server IP\n"
            "• 🌐 <b>IP Rotation</b> — through configured proxies\n\n"
            f"<b>SOCKS5:</b> {socks_status}\n\n"
            "<b>Privacy:</b> Only public/authorized URLs are fetched. "
            "No login/CAPTCHA bypass. Numbers come from publicly exposed content.",
            reply_markup=main_keyboard(user.id))
        return

    if text == "📞 Support":
        support = get_setting("support_username", DEFAULT_SUPPORT_USERNAME)
        support_disp = f"@{html.escape(support)}" if support else "the administrator"
        bot.send_message(chat_id,
            f"📞 <b>Support</b>\n━━━━━━━━━━━━━━━━━━\nContact: {support_disp}",
            reply_markup=main_keyboard(user.id))
        return

    # ── Extraction flow: URL submission ──
    if state.get("step") == "AWAITING_URL" or text.startswith(("http://", "https://")):
        valid, err = validate_url(text)
        if not valid:
            bot.send_message(chat_id,
                f"⚠️ <b>Invalid URL</b>\n\n{html.escape(err)}\n\nPlease send a valid URL.",
                reply_markup=cancel_only_keyboard())
            return
        with _state_lock:
            user_states[user.id] = {"step": "AWAITING_MODE", "url": text}
        bot.send_message(chat_id,
            f"🔗 <b>Link Received</b>\n<code>{html.escape(text[:80])}</code>\n\n"
            f"<b>Choose Extraction Mode:</b>\n"
            f"🟢 <b>Direct Connection</b>\n🌐 <b>IP Rotation</b>",
            reply_markup=mode_selection_keyboard())
        return

    # ── Mode selection ──
    if state.get("step") == "AWAITING_MODE":
        target_url = state.get("url", "")
        if text == "🟢 Direct Connection":
            with _state_lock:
                user_states[user.id] = {"step": "AWAITING_CYCLES", "url": target_url, "mode": "NORMAL"}
            bot.send_message(chat_id,
                f"🟢 <b>Direct Connection</b>\n🔗 <code>{html.escape(target_url[:60])}</code>\n\nSelect visits:",
                reply_markup=extraction_cycles_keyboard())
            return
        if text == "🌐 IP Rotation":
            if not proxy_manager.has_endpoints():
                bot.send_message(chat_id,
                    "⚠️ <b>IP Rotation Unavailable</b>\n\nNo proxy endpoints configured.\n"
                    "Admins can add proxies via /admin → 🌐 Proxy Manager.\n\n"
                    "<i>Use 🟢 Direct Connection instead.</i>",
                    reply_markup=mode_selection_keyboard())
                return
            with _state_lock:
                user_states[user.id] = {"step": "AWAITING_CYCLES", "url": target_url, "mode": "ROTATING"}
            bot.send_message(chat_id,
                f"🌐 <b>IP Rotation</b>\n🔗 <code>{html.escape(target_url[:60])}</code>\n"
                f"📡 Proxies: <code>{proxy_manager.get_endpoint_count()}</code>\n\nSelect visits:",
                reply_markup=extraction_cycles_keyboard())
            return

    # ── Visit count selection ──
    if state.get("step") == "AWAITING_CYCLES":
        target_url = state.get("url", "")
        mode = state.get("mode", "NORMAL")
        if text == "✍️ Custom":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_CUSTOM_VISITS" if is_admin_user else "CUSTOM_VISITS",
                                          "url": target_url, "mode": mode}
            max_v = get_setting_int("max_visits", MAX_VISITS_PER_JOB)
            bot.send_message(chat_id, f"✍️ Send a visit count (1–{max_v}):",
                              reply_markup=back_cancel_keyboard())
            return
        if state.get("step") == "CUSTOM_VISITS":
            try:
                count = int(text.strip())
            except Exception:
                bot.send_message(chat_id, "⚠️ Send a number.", reply_markup=back_cancel_keyboard())
                return
            max_v = get_setting_int("max_visits", MAX_VISITS_PER_JOB)
            if count < 1 or count > max_v:
                bot.send_message(chat_id, f"⚠️ Must be 1–{max_v}.", reply_markup=back_cancel_keyboard())
                return
            with _state_lock:
                user_states[user.id] = {}
            _start_extraction(user, chat_id, target_url, mode, count)
            return
        count = _CYCLE_COUNT_MAP.get(text)
        if count is not None:
            with _state_lock:
                user_states[user.id] = {}
            _start_extraction(user, chat_id, target_url, mode, count)
            return

    # ── Fallback ──
    bot.send_message(chat_id,
        "❓ Please select an option from the menu or tap <b>🔗 Send New Link</b>.",
        reply_markup=main_keyboard(user.id))


def _start_extraction(user, chat_id: int, url: str, mode: str, count: int) -> None:
    """Create a job, a JobContext, and launch the worker + progress thread."""
    with _state_lock:
        if user.id in active_jobs:
            bot.send_message(chat_id, "⚠️ A job is already running. Cancel it first.",
                              reply_markup=cancel_only_keyboard())
            return
    if not is_admin(user.id):
        allowed, reason = user_can_extract(user.id)
        if not allowed:
            bot.send_message(chat_id, reason, reply_markup=main_keyboard(user.id))
            return

    if mode == "ROTATING" and not proxy_manager.has_endpoints():
        bot.send_message(chat_id,
            "❌ <b>IP Rotation stopped</b>\n\nNo verified proxy is currently available.",
            reply_markup=main_keyboard(user.id))
        return

    mode_disp = "🌐 IP Rotation" if mode == "ROTATING" else "🟢 Direct Connection"
    start_msg = bot.send_message(chat_id,
        f"⏳ <b>EXTRACTION IN PROGRESS</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Mode:</b> {mode_disp}\n"
        f"<b>Visits:</b> 0/{count}\n"
        f"<b>Successful:</b> 0\n"
        f"<b>Failed:</b> 0\n"
        f"<b>Unique Numbers:</b> 0\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<i>Initialising…</i>",
        reply_markup=cancel_only_keyboard())

    job_id = create_job(user.id, user.username or "", url, mode, count)
    ctx = JobContext(user.id, chat_id, start_msg.message_id, count, mode, url)
    with _state_lock:
        active_jobs[user.id] = ctx
    threading.Thread(target=extraction_worker, args=(ctx, user.username or "", job_id),
                     daemon=True).start()


# =========================================================
# Admin helpers (UI / settings / diagnostics)
# =========================================================
def _handle_add_proxy(user_id: int, chat_id: int, text: str) -> None:
    with _state_lock:
        user_states[user_id] = {}
    endpoint = text.strip()
    parsed, err = parse_proxy(endpoint)
    if not parsed:
        bot.send_message(chat_id,
            f"❌ <b>Invalid Proxy</b>\n\nReason: {html.escape(err)}",
            reply_markup=proxy_manager_keyboard())
        return
    if parsed.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
        bot.send_message(chat_id,
            "⚠️ <b>SOCKS5 Not Available</b>\nInstall <code>requests[socks]</code> first.",
            reply_markup=proxy_manager_keyboard())
        return
    wait_msg = bot.send_message(chat_id, "⏳ <i>Testing proxy…</i>")
    result = test_proxy(parsed, quick=True)
    try:
        bot.delete_message(chat_id, wait_msg.message_id)
    except Exception:
        pass
    added = db_add_proxy(endpoint, user_id)
    if added:
        row = db_get_proxy_by_endpoint(endpoint)
        if row:
            if result.working:
                db_update_proxy_success(row["id"], result.latency_ms or 0, result.observed_ip or "")
            else:
                db_update_proxy_failure(row["id"], result.error_reason or "Initial test failed",
                                         result.status_label)
        audit_log(user_id, "ADD_PROXY", parsed.display, result.status_label)
    bot.send_message(chat_id,
        f"{'✅ Saved to database.' if added else '⚠️ Already exists.'}\n\n{result.to_telegram_card()}",
        reply_markup=proxy_manager_keyboard())


def _handle_bulk_add(user_id: int, chat_id: int, text: str) -> None:
    with _state_lock:
        user_states[user_id] = {}
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    added = skipped = invalid = 0
    for ln in lines:
        parsed, _ = parse_proxy(ln)
        if not parsed:
            invalid += 1
            continue
        if parsed.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
            invalid += 1
            continue
        if db_add_proxy(ln, user_id):
            added += 1
        else:
            skipped += 1
    audit_log(user_id, "BULK_ADD_PROXY", "", f"added={added} skipped={skipped} invalid={invalid}")
    bot.send_message(chat_id,
        f"📦 <b>Bulk Import</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"➕ Added: <code>{added}</code>\n"
        f"🔁 Already existed: <code>{skipped}</code>\n"
        f"⚠️ Invalid: <code>{invalid}</code>\n"
        f"📡 Total Active: <code>{proxy_manager.get_endpoint_count()}</code>",
        reply_markup=proxy_manager_keyboard())


def _list_proxies(chat_id: int) -> None:
    db_proxies = db_get_all_proxies(active_only=False)
    env_proxies = proxy_manager._env_proxies
    if not db_proxies and not env_proxies:
        bot.send_message(chat_id, "📋 <b>No proxies configured.</b>", reply_markup=proxy_manager_keyboard())
        return
    if env_proxies:
        lines = ["⚙️ <b>Env Proxies (read-only):</b>"]
        for ep in env_proxies:
            lines.append(f"• <code>{html.escape(ProxyManager.sanitize_display(ep))}</code>")
        bot.send_message(chat_id, "\n".join(lines))
    if db_proxies:
        for item in db_proxies:
            emoji = proxy_status_emoji(item)
            lat = f"{(item['average_latency'] or 0):.0f} ms" if item.get("average_latency") else "N/A"
            ip = item.get("last_observed_ip") or "—"
            card = (
                f"{emoji} <b>#{item['id']}</b> <code>{html.escape(ProxyManager.sanitize_display(item['endpoint']))}</code>\n"
                f"   🌐 {item.get('health_status', 'UNTESTED')} · ⚡ {lat} · 🌍 {html.escape(ip)}\n"
                f"   ✅ {item.get('success_count', 0)} · ❌ {item.get('failure_count', 0)}"
            )
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(f"🗑️ Delete #{item['id']}",
                callback_data=f"del_proxy_{item['id']}"))
            try:
                bot.send_message(chat_id, card, reply_markup=markup)
            except Exception:
                pass
            time.sleep(0.25)


def _show_users_list(chat_id: int, page: int) -> None:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT user_id, first_name, username, status, total_extractions, "
            "total_numbers_found, joined_at FROM users ORDER BY joined_at DESC LIMIT 10 OFFSET ?",
            (page * 10,)
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        bot.send_message(chat_id, "👥 <i>No users found.</i>", reply_markup=admin_keyboard())
        return
    msg = f"👥 <b>USERS</b> (page {page + 1})\n━━━━━━━━━━━━━━━━━━\n\n"
    for r in rows:
        name = html.escape(r["first_name"] or "User")
        uname = html.escape(r["username"] or "—")
        msg += (
            f"<b>{name}</b> · @{uname} · <code>{r['user_id']}</code>\n"
            f"   {r.get('status', 'APPROVED')} · 🔄 {r['total_extractions']} · 📱 {r['total_numbers_found']}\n\n"
        )
    markup = types.InlineKeyboardMarkup(row_width=2)
    for r in rows[:8]:
        markup.add(types.InlineKeyboardButton(f"👁 {r['user_id']}", callback_data=f"user_{r['user_id']}"))
    bot.send_message(chat_id, msg, reply_markup=markup)
    bot.send_message(chat_id, "🔐 <b>Admin Control Center</b>", reply_markup=admin_keyboard())


def _show_jobs_list(chat_id: int, page: int) -> None:
    jobs = list_jobs(limit=10, offset=page * 10)
    if not jobs:
        bot.send_message(chat_id, "🔎 <i>No extraction jobs found.</i>", reply_markup=admin_keyboard())
        return
    msg = f"🔎 <b>EXTRACTION LOGS</b> (page {page + 1})\n━━━━━━━━━━━━━━━━━━\n\n"
    markup = types.InlineKeyboardMarkup(row_width=1)
    for j in jobs:
        mode_lbl = "🌐 IP" if j["mode"] == "ROTATING" else "🟢 Norm"
        status_icon = "✅" if j["status"] == "COMPLETED" else ("❌" if j["status"] == "FAILED" else "🔄")
        url_disp = html.escape((j["source_url"] or "")[:35])
        msg += (
            f"{status_icon} <b>#{j['job_id']}</b> · @{html.escape(j.get('username') or '—')}\n"
            f"   {mode_lbl} · {j['successful_visits']}/{j['requested_visits']} · 📱 {j['unique_numbers']}\n\n"
        )
        markup.add(types.InlineKeyboardButton(f"#{j['job_id']} details", callback_data=f"job_{j['job_id']}"))
    bot.send_message(chat_id, msg, reply_markup=markup)


def _show_bot_settings(chat_id: int, user_id: int) -> None:
    settings = [
        ("channel_username", "Channel Username"),
        ("bot_name", "Bot Name"),
        ("admin_display_username", "Admin Display Username"),
        ("support_username", "Support Username"),
        ("welcome_text", "Welcome Text"),
        ("max_visits", "Max Visits/Job"),
    ]
    lines = ["⚙️ <b>BOT SETTINGS</b>", "━━━━━━━━━━━━━━━━━━"]
    for key, label in settings:
        val = get_setting(key, "")
        lines.append(f"<b>{label}:</b> <code>{html.escape(val[:50])}</code>")
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("<i>Reply with:</i> <code>key=value</code> to change. e.g. <code>bot_name=My Bot</code>")
    with _state_lock:
        user_states[user_id] = {"step": "ADMIN_SET_SETTING"}
    bot.send_message(chat_id, "\n".join(lines), reply_markup=back_cancel_keyboard())


def _run_diagnostics(chat_id: int) -> None:
    """Run a startup self-check; report per-component status."""
    results: list[tuple[str, bool, str]] = []

    # Telegram API
    try:
        bot.get_me()
        results.append(("Telegram API", True, ""))
    except Exception as e:
        results.append(("Telegram API", False, str(e)[:60]))

    # Database
    try:
        with _db_lock:
            conn = get_conn()
            conn.execute("SELECT 1").fetchone()
            conn.close()
        results.append(("Database", True, ""))
    except Exception as e:
        results.append(("Database", False, str(e)[:60]))

    # SOCKS5
    results.append(("SOCKS5 Support", _SOCKS5_AVAILABLE,
                     "" if _SOCKS5_AVAILABLE else "pip install requests[socks]"))

    # Direct HTTP
    try:
        r = requests.get("https://www.example.com", timeout=8)
        results.append(("Direct HTTP", r.status_code < 500, f"HTTP {r.status_code}"))
    except Exception as e:
        results.append(("Direct HTTP", False, str(e)[:60]))

    # Proxy parser
    try:
        p, err = parse_proxy("http://1.2.3.4:8080")
        results.append(("Proxy Parser", p is not None, err))
    except Exception as e:
        results.append(("Proxy Parser", False, str(e)[:60]))

    # Proxy pool
    results.append(("Proxy Pool", proxy_manager.has_endpoints(),
                     f"{proxy_manager.get_endpoint_count()} endpoints" if proxy_manager.has_endpoints() else "no proxies"))

    # Channel
    if get_setting_bool("channel_enabled", False):
        ch = get_setting("channel_username", DEFAULT_CHANNEL)
        try:
            bot.get_chat(ch)
            results.append(("Channel", True, ch))
        except Exception as e:
            results.append(("Channel", False, str(e)[:60]))
    else:
        results.append(("Channel", False, "disabled"))

    lines = ["🩺 <b>SYSTEM DIAGNOSTICS</b>", "━━━━━━━━━━━━━━━━━━"]
    for name, ok, detail in results:
        icon = "✅" if ok else "❌"
        extra = f" — {html.escape(detail)}" if detail else ""
        lines.append(f"{icon} {name}{extra}")
    lines.append("━━━━━━━━━━━━━━━━━━")
    bot.send_message(chat_id, "\n".join(lines), reply_markup=proxy_manager_keyboard())


@bot.callback_query_handler(func=lambda c: c.data == "test_channel")
def cb_test_channel(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    ch = get_setting("channel_username", DEFAULT_CHANNEL)
    try:
        bot.send_message(ch, "🧪 <b>Channel test</b>\nThis confirms the bot can post here.")
        bot.answer_callback_query(call.id, "✅ Channel OK")
    except ApiTelegramException as e:
        bot.answer_callback_query(call.id, f"❌ Failed: {str(e)[:60]}", show_alert=True)
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {str(e)[:60]}", show_alert=True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("set_channel_"))
def cb_toggle_channel(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    key = {"set_channel_enabled": "channel_enabled",
            "set_channel_post_numbers": "channel_post_numbers",
            "set_channel_attach": "channel_attach_txt"}[call.data]
    new = "0" if get_setting_bool(key) else "1"
    set_setting(key, new)
    audit_log(call.from_user.id, "SET_SETTING", key, new)
    bot.answer_callback_query(call.id, f"Set to {new}")
    try:
        bot.edit_message_text(call.message.chat.id, call.message.message_id,
            f"✅ <b>{key}</b> = <code>{new}</code>")
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data in ("prompt_add_admin", "prompt_remove_admin"))
def cb_prompt_admin(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    step = "ADMIN_ADD_ADMIN" if call.data == "prompt_add_admin" else "ADMIN_REMOVE_ADMIN"
    with _state_lock:
        user_states[call.from_user.id] = {"step": step}
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
        f"{'➕ Add' if step == 'ADMIN_ADD_ADMIN' else '➖ Remove'} admin — send the numeric Telegram ID:",
        reply_markup=back_cancel_keyboard())


def _trigger_retest_failed(chat_id: int) -> None:
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT endpoint FROM admin_proxies
               WHERE is_active = 1
                 AND (failure_count > 0 OR last_tested IS NULL
                      OR health_status IN ('DEAD','TCP_FAILED','AUTH_FAILED','UNTESTED'))
               ORDER BY last_tested ASC NULLS FIRST"""
        ).fetchall()
    finally:
        conn.close()
    endpoints = [r["endpoint"] for r in rows]
    if not endpoints:
        bot.send_message(chat_id, "✅ No failed/untested proxies found.", reply_markup=proxy_manager_keyboard())
        return
    status_msg = bot.send_message(chat_id, f"🔄 <b>Retesting {len(endpoints)} proxies…</b>")
    cancel_ev = threading.Event()
    threading.Thread(target=_run_proxy_tests_in_bg,
                      args=(chat_id, status_msg.message_id, endpoints, cancel_ev, False),
                      daemon=True).start()


# =========================================================
# Background: periodic proxy re-test
# =========================================================
def _background_retester() -> None:
    """Lightweight background loop that re-tests unhealthy proxies at the
    configured retest interval. Compatible with Render threading."""
    interval = max(60, get_setting_int("retest_interval", 300))
    while True:
        time.sleep(interval)
        if not get_setting_bool("auto_retest", True):
            continue
        try:
            conn = get_conn()
            try:
                rows = conn.execute(
                    """SELECT endpoint FROM admin_proxies
                       WHERE is_active = 1
                         AND (health_status IN ('DEAD','TCP_FAILED','AUTH_FAILED','UNTESTED')
                              OR cooldown_until IS NOT NULL)
                       LIMIT 30"""
                ).fetchall()
            finally:
                conn.close()
            endpoints = [r["endpoint"] for r in rows]
            if not endpoints:
                continue
            workers = max(2, get_setting_int("proxy_test_concurrency", 8))
            def _test_one(raw):
                parsed, _ = parse_proxy(raw)
                if not parsed:
                    return
                r = test_proxy(parsed, quick=True)
                row = db_get_proxy_by_endpoint(raw)
                if row:
                    if r.working:
                        db_update_proxy_success(row["id"], r.latency_ms or 0, r.observed_ip or "")
                    else:
                        db_update_proxy_failure(row["id"], r.error_reason or "Retest", r.status_label)
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(_test_one, endpoints))
            logger.info("Background retest of %d proxies complete.", len(endpoints))
        except Exception as e:
            logger.warning("background retest error: %s", e)


# =========================================================
# Entry Point
# =========================================================
def _startup_self_check() -> None:
    """Best-effort startup report in logs."""
    logger.info("=" * 60)
    logger.info("URL Fetcher Bot v2 — Starting")
    logger.info("Admins: %s", ADMIN_IDS or "(none)")
    logger.info("DB: %s", DB_FILE)
    logger.info("Proxy endpoints: %d", proxy_manager.get_endpoint_count())
    logger.info("SOCKS5 support: %s", "available" if _SOCKS5_AVAILABLE else "not installed")
    logger.info("Max concurrency: %d", MAX_CONCURRENCY)
    logger.info("Channel: %s (enabled=%s)", get_setting("channel_username", DEFAULT_CHANNEL),
                get_setting_bool("channel_enabled", False))
    logger.info("=" * 60)


if __name__ == "__main__":
    init_db()
    _startup_self_check()

    # Start background proxy re-tester
    threading.Thread(target=_background_retester, daemon=True).start()

    try:
        bot.remove_webhook()
        time.sleep(1)
        logger.info("Webhook cleared.")
    except Exception as exc:
        logger.warning("Webhook clear notice: %s", exc)

    logger.info("Polling started.")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as exc:
            logger.warning("Polling interrupted: %s", exc)
            time.sleep(4)
            logger.info("Reconnecting…")
