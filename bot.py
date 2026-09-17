#!/usr/bin/env python3
"""
URL Fetcher & Rotating Link Engine  —  Production Telegram Bot
--------------------------------------------------------------
Upgraded build. Render / VPS compatible, single-file deployment.

Highlights
  * IP-rotation that actually rotates: verified-proxy selection, per-visit
    retry on another proxy, exponential cooldown, and NO silent direct fallback.
  * Trustworthy proxy health: transport / exit-IP / target are classified
    separately. A blocked IP-check service never marks a good proxy dead.
  * Independent progress monitor thread so the status message keeps ticking
    even while a network request is blocking.
  * Full audit trail: job_ids, per-number provenance (method / visit / proxy /
    exit IP), proxy attempts, channel-post status.
  * Premium admin control center, approvals, configurable channel auto-post,
    persistent settings, and safe schema migration (existing data preserved).

Configuration is environment-driven. See README block at the bottom of this file.
"""

import concurrent.futures
import html
import http.cookiejar
import io
import json
import logging
import os
import queue
import random
import re
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# ── Third-party ──────────────────────────────────────────────────────────────
import requests
import requests.exceptions
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

try:
    # PySocks must be installed for SOCKS5 to work.
    import socks  # noqa: F401
    _SOCKS5_AVAILABLE = True
except ImportError:
    _SOCKS5_AVAILABLE = False


# =========================================================
# Logging (credential-safe)
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bot")


def _safe_log(msg: str) -> str:
    """Strip anything that looks like a credential from log strings."""
    try:
        return re.sub(r"(://)[^@/\s]+@", r"\1****:****@", str(msg))
    except Exception:
        return "<?>"


def _log_event(event: str, **fields: Any) -> None:
    """Structured, single-line, credential-safe log event."""
    if not fields:
        logger.info("%s", event)
        return
    payload = " ".join(f"{k}={_safe_log(v)}" for k, v in fields.items())
    logger.info("%s %s", event, payload)


# =========================================================
# Configuration (environment-driven)
# =========================================================

def _env_str(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)).strip())
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)).strip())
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "y")


BOT_TOKEN = _env_str("BOT_TOKEN")
if not BOT_TOKEN:
    logger.critical("8553353076:AAFgLdPCaSL_TfZds10qQS1_Hr5iGnn0e5M")
    sys.exit(1)

ADMIN_IDS: list[int] = [
    int(x.strip())
    for x in _env_str("ADMIN_IDS").split("8753914631")
    if x.strip().lstrip("-").isdigit()
]
if not ADMIN_IDS:
    logger.warning("ADMIN_IDS is empty — no admin will be able to authenticate.")

DB_FILE = _env_str("DATABASE_PATH", "bot_database.db")

REQUEST_TIMEOUT = _env_float("REQUEST_TIMEOUT", 12.0)
CONNECT_TIMEOUT = _env_float("CONNECT_TIMEOUT", 6.0)
READ_TIMEOUT = _env_float("READ_TIMEOUT", 8.0)

MAX_CONCURRENCY = max(1, _env_int("MAX_CONCURRENCY", 4))
MAX_RESPONSE_SIZE = _env_int("MAX_RESPONSE_SIZE", 5 * 1024 * 1024)  # 5 MB
MAX_URL_LENGTH = _env_int("MAX_URL_LENGTH", 2048)
MAX_REDIRECTS = _env_int("MAX_REDIRECTS", 10)
MAX_VISITS_PER_JOB = _env_int("MAX_VISITS_PER_JOB", 200)
DEFAULT_VISITS = _env_int("DEFAULT_VISITS", 20)

PROGRESS_INTERVAL = max(0.8, _env_float("PROGRESS_INTERVAL", 1.1))

PROXY_COOLDOWN_BASE = _env_float("PROXY_COOLDOWN_BASE", 30.0)
PROXY_COOLDOWN_MAX = _env_float("PROXY_COOLDOWN_MAX", 600.0)
PROXY_HEALTH_TIMEOUT = _env_float("PROXY_HEALTH_TIMEOUT", 6.0)
PROXY_QUARANTINE = _env_float("PROXY_QUARANTINE", 120.0)
RETEST_INTERVAL = _env_float("RETEST_INTERVAL", 600.0)

VISIT_OPTIONS: dict[str, int] = {
    "🧪 Test — 1 Visit": 1,
    "🚀 20 Visits": 20,
    "⚡ 50 Visits": 50,
    "💎 100 Visits": 100,
}

RAW_PROXY_ENV = _env_str("PROXY_ENDPOINTS") or _env_str("ROTATING_PROXIES")

# Channel defaults (can be overridden at runtime from the admin panel).
DEFAULT_CHANNEL_USERNAME = _env_str("CHANNEL_USERNAME")
DEFAULT_AUTO_CHANNEL_POST = _env_bool("AUTO_CHANNEL_POST", False)
DEFAULT_POST_NUMBERS = _env_bool("POST_NUMBERS", False)
DEFAULT_APPROVAL_MODE = _env_bool("APPROVAL_MODE", False)

ADMIN_DISPLAY_USERNAME = _env_str("ADMIN_USERNAME")
SUPPORT_USERNAME = _env_str("SUPPORT_USERNAME")
BOT_NAME = _env_str("BOT_NAME", "URL Extraction Center")

# ── Bot instance ─────────────────────────────────────────────
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ── Global runtime state ─────────────────────────────────────
user_states: dict[int, dict] = {}
_state_lock = threading.RLock()



# =========================================================
# Database layer  (schema v2 + safe migration)
# =========================================================
_db_lock = threading.RLock()
_write_gate = threading.Semaphore(1)  # serialize writers without holding a net call


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ── Schema ----------------------------------------------------------

_SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS users (
    user_id               INTEGER PRIMARY KEY,
    username              TEXT,
    first_name            TEXT,
    status                TEXT DEFAULT 'APPROVED',
    is_blocked            INTEGER DEFAULT 0,
    total_extractions     INTEGER DEFAULT 0,
    successful_extractions INTEGER DEFAULT 0,
    failed_extractions    INTEGER DEFAULT 0,
    total_numbers_found   INTEGER DEFAULT 0,
    joined_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_extraction_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS extraction_jobs (
    job_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id           INTEGER,
    username          TEXT,
    source_url        TEXT,
    mode              TEXT,
    requested_visits  INTEGER,
    completed_visits  INTEGER DEFAULT 0,
    successful_visits INTEGER DEFAULT 0,
    failed_visits     INTEGER DEFAULT 0,
    unique_numbers    INTEGER DEFAULT 0,
    duplicate_numbers INTEGER DEFAULT 0,
    duration_ms       INTEGER DEFAULT 0,
    status            TEXT DEFAULT 'RUNNING',
    channel_post_status TEXT,
    channel_message_id  INTEGER,
    channel_post_error  TEXT,
    started_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS extracted_numbers (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id            INTEGER,
    user_id           INTEGER,
    number            TEXT,
    source_url        TEXT,
    extraction_method TEXT,
    source_type       TEXT,
    visit_number      INTEGER,
    proxy_display     TEXT,
    observed_ip       TEXT,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_attempts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id         INTEGER,
    visit_number   INTEGER,
    attempt_number INTEGER,
    proxy_id       INTEGER,
    proxy_display  TEXT,
    status         TEXT,
    latency_ms     REAL,
    observed_ip    TEXT,
    error          TEXT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS admin_proxies (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint              TEXT UNIQUE NOT NULL,
    scheme                TEXT,
    host                  TEXT,
    port                  INTEGER,
    added_by              INTEGER,
    is_active             INTEGER DEFAULT 1,
    health_status         TEXT DEFAULT 'UNTESTED',
    health_score          REAL DEFAULT 0,
    success_count         INTEGER DEFAULT 0,
    failure_count         INTEGER DEFAULT 0,
    consecutive_failures  INTEGER DEFAULT 0,
    consecutive_successes INTEGER DEFAULT 0,
    average_latency       REAL DEFAULT 0,
    last_observed_ip      TEXT,
    last_error            TEXT,
    last_tested           TIMESTAMP,
    last_success          TIMESTAMP,
    last_failure          TIMESTAMP,
    cooldown_until        TIMESTAMP,
    target_compat         TEXT,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bot_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS admins (
    user_id    INTEGER PRIMARY KEY,
    username   TEXT,
    role       TEXT DEFAULT 'ADMIN',
    added_by   INTEGER,
    is_active  INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS admin_audit_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id   INTEGER,
    action     TEXT,
    target     TEXT,
    details    TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobs_user      ON extraction_jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_jobs_started   ON extraction_jobs(started_at);
CREATE INDEX IF NOT EXISTS idx_jobs_status    ON extraction_jobs(status);
CREATE INDEX IF NOT EXISTS idx_numbers_job    ON extracted_numbers(job_id);
CREATE INDEX IF NOT EXISTS idx_numbers_user   ON extracted_numbers(user_id);
CREATE INDEX IF NOT EXISTS idx_numbers_number ON extracted_numbers(number);
CREATE INDEX IF NOT EXISTS idx_attempts_job   ON job_attempts(job_id);
CREATE INDEX IF NOT EXISTS idx_proxies_active ON admin_proxies(is_active);
CREATE INDEX IF NOT EXISTS idx_proxies_health ON admin_proxies(health_status);
"""


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r["name"] for r in rows}
    except sqlite3.Error:
        return set()


def _migrate_add_columns(conn: sqlite3.Connection) -> None:
    """Add new columns to pre-existing tables without destroying data."""
    migrations: list[tuple[str, str, str]] = [
        ("users", "status", "TEXT DEFAULT 'APPROVED'"),
        ("users", "is_blocked", "INTEGER DEFAULT 0"),
        ("users", "successful_extractions", "INTEGER DEFAULT 0"),
        ("users", "failed_extractions", "INTEGER DEFAULT 0"),
        ("users", "last_extraction_at", "TIMESTAMP"),
        ("admin_proxies", "scheme", "TEXT"),
        ("admin_proxies", "host", "TEXT"),
        ("admin_proxies", "port", "INTEGER"),
        ("admin_proxies", "health_status", "TEXT DEFAULT 'UNTESTED'"),
        ("admin_proxies", "health_score", "REAL DEFAULT 0"),
        ("admin_proxies", "consecutive_failures", "INTEGER DEFAULT 0"),
        ("admin_proxies", "consecutive_successes", "INTEGER DEFAULT 0"),
        ("admin_proxies", "target_compat", "TEXT"),
        ("admin_proxies", "updated_at", "TIMESTAMP"),
    ]
    for table, column, decl in migrations:
        cols = _existing_columns(conn, table)
        if not cols:
            continue  # table doesn't exist yet; CREATE will build it fresh
        if column not in cols:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
                logger.info("Migration: added %s.%s", table, column)
            except sqlite3.Error as exc:
                logger.warning("Migration skip %s.%s: %s", table, column, exc)


def init_db() -> None:
    with _db_lock:
        conn = get_conn()
        try:
            _migrate_add_columns_flags = _existing_columns(conn, "users")
            conn.executescript(_SCHEMA_V2)
            conn.commit()
            _migrate_add_columns(conn)
            conn.commit()
            # Seed bootstrap admins if the admins table is empty.
            existing = conn.execute("SELECT COUNT(*) AS c FROM admins").fetchone()["c"]
            if existing == 0 and ADMIN_IDS:
                for idx, aid in enumerate(ADMIN_IDS):
                    conn.execute(
                        """INSERT OR IGNORE INTO admins (user_id, username, role, added_by)
                           VALUES (?, ?, ?, ?)""",
                        (aid, None, "OWNER" if idx == 0 else "ADMIN", aid),
                    )
                conn.commit()
        finally:
            conn.close()
    _load_settings_cache()


# ── generic helpers -------------------------------------------------

def _exec(sql: str, params: tuple = ()) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(sql, params)
            conn.commit()
        except sqlite3.Error as exc:
            logger.warning("DB exec error: %s | sql=%s", exc, sql[:80])
        finally:
            conn.close()


def _query(sql: str, params: tuple = ()) -> list[dict]:
    with _db_lock:
        conn = get_conn()
        try:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()


def _query_one(sql: str, params: tuple = ()) -> Optional[dict]:
    rows = _query(sql, params)
    return rows[0] if rows else None


# ── Settings (env -> DB defaults precedence, DB wins at runtime) -----

_settings_cache: dict[str, str] = {}
_settings_lock = threading.RLock()


def _default_settings() -> dict[str, str]:
    return {
        "maintenance_mode": "0",
        "approval_mode": "1" if DEFAULT_APPROVAL_MODE else "0",
        "channel_logging_enabled": "1" if DEFAULT_AUTO_CHANNEL_POST else "0",
        "channel_username": DEFAULT_CHANNEL_USERNAME,
        "channel_post_numbers": "1" if DEFAULT_POST_NUMBERS else "0",
        "channel_attach_txt": "0",
        "channel_include_username": "1",
        "channel_include_userid": "0",
        "admin_display_username": ADMIN_DISPLAY_USERNAME,
        "support_username": SUPPORT_USERNAME,
        "bot_name": BOT_NAME,
        "max_visits": str(MAX_VISITS_PER_JOB),
        "auto_proxy_retest": "1",
        "proxy_enabled": "1",
    }


def _load_settings_cache() -> None:
    defaults = _default_settings()
    rows = _query("SELECT key, value FROM bot_settings")
    db_map = {r["key"]: r["value"] for r in rows}
    with _settings_lock:
        _settings_cache.clear()
        _settings_cache.update(defaults)
        _settings_cache.update(db_map)


def get_setting(key: str, default: str = "") -> str:
    with _settings_lock:
        return _settings_cache.get(key, default)


def get_setting_int(key: str, default: int = 0) -> int:
    try:
        return int(get_setting(key, str(default)))
    except (TypeError, ValueError):
        return default


def get_setting_bool(key: str, default: bool = False) -> bool:
    return get_setting(key, "1" if default else "0").lower() in ("1", "true", "yes", "on")


def set_setting(key: str, value: Any) -> None:
    val = str(value)
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO bot_settings (key, value, updated_at)
                   VALUES (?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                                                  updated_at=CURRENT_TIMESTAMP""",
                (key, val),
            )
            conn.commit()
        finally:
            conn.close()
    with _settings_lock:
        _settings_cache[key] = val


# ── User helpers ----------------------------------------------------

def register_user(user_id: int, username: Optional[str] = None,
                  first_name: Optional[str] = None) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            approval_on = get_setting_bool("approval_mode", False)
            default_status = "PENDING" if approval_on else "APPROVED"
            conn.execute(
                """INSERT OR IGNORE INTO users (user_id, username, first_name, status)
                   VALUES (?, ?, ?, ?)""",
                (user_id, username, first_name, default_status),
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


def get_user(user_id: int) -> Optional[dict]:
    return _query_one("SELECT * FROM users WHERE user_id = ?", (user_id,))


def is_user_approved(user_id: int) -> bool:
    row = get_user(user_id)
    if row is None:
        return True  # will be created on next register
    if row.get("is_blocked"):
        return False
    return (row.get("status") or "APPROVED") == "APPROVED"


def is_user_blocked(user_id: int) -> bool:
    row = get_user(user_id)
    return bool(row and row.get("is_blocked"))


def set_user_status(user_id: int, status: str) -> None:
    _exec("UPDATE users SET status = ? WHERE user_id = ?", (status, user_id))


def set_user_blocked(user_id: int, blocked: bool) -> None:
    _exec("UPDATE users SET is_blocked = ? WHERE user_id = ?", (1 if blocked else 0, user_id))


def get_pending_users(limit: int = 30) -> list[dict]:
    return _query(
        "SELECT * FROM users WHERE status = 'PENDING' ORDER BY joined_at DESC LIMIT ?",
        (limit,),
    )


def update_user_job_result(user_id: int, unique_count: int, success: bool) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE users
                   SET total_extractions      = total_extractions + 1,
                       successful_extractions = successful_extractions + ?,
                       failed_extractions     = failed_extractions + ?,
                       total_numbers_found    = total_numbers_found + ?,
                       last_active            = CURRENT_TIMESTAMP,
                       last_extraction_at     = CURRENT_TIMESTAMP
                   WHERE user_id = ?""",
                (1 if success else 0, 0 if success else 1, unique_count, user_id),
            )
            conn.commit()
        finally:
            conn.close()


def get_all_user_ids(only_approved: bool = False) -> list[int]:
    if only_approved:
        rows = _query(
            "SELECT user_id FROM users WHERE is_blocked = 0 AND status = 'APPROVED'"
        )
    else:
        rows = _query("SELECT user_id FROM users")
    return [r["user_id"] for r in rows]


def search_users(term: str, limit: int = 20) -> list[dict]:
    term = (term or "").strip()
    if not term:
        return []
    like = f"%{term}%"
    if term.lstrip("-").isdigit():
        rows = _query(
            """SELECT * FROM users WHERE CAST(user_id AS TEXT) = ?
               OR CAST(user_id AS TEXT) LIKE ? LIMIT ?""",
            (term, like, limit),
        )
    else:
        rows = _query(
            """SELECT * FROM users
               WHERE username LIKE ? OR first_name LIKE ? LIMIT ?""",
            (like, like, limit),
        )
    return rows


# ── Job / number / attempt helpers ----------------------------------

def create_job(user_id: int, username: Optional[str], url: str, mode: str,
               requested_visits: int) -> int:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """INSERT INTO extraction_jobs
                       (user_id, username, source_url, mode, requested_visits, status)
                   VALUES (?, ?, ?, ?, ?, 'RUNNING')""",
                (user_id, username, url, mode, requested_visits),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()


def finalize_job(job_id: int, *, completed: int, successful: int, failed: int,
                 unique_numbers: int, duplicate_numbers: int, duration_ms: int,
                 status: str) -> None:
    _exec(
        """UPDATE extraction_jobs
           SET completed_visits = ?, successful_visits = ?, failed_visits = ?,
               unique_numbers = ?, duplicate_numbers = ?, duration_ms = ?,
               status = ?, completed_at = CURRENT_TIMESTAMP
           WHERE job_id = ?""",
        (completed, successful, failed, unique_numbers, duplicate_numbers,
         duration_ms, status, job_id),
    )


def get_job(job_id: int) -> Optional[dict]:
    return _query_one("SELECT * FROM extraction_jobs WHERE job_id = ?", (job_id,))


def save_extracted_number(job_id: int, user_id: int, number: str, source_url: str,
                          method: str, source_type: str, visit_number: int,
                          proxy_display: Optional[str], observed_ip: Optional[str]) -> None:
    _exec(
        """INSERT INTO extracted_numbers
               (job_id, user_id, number, source_url, extraction_method,
                source_type, visit_number, proxy_display, observed_ip)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (job_id, user_id, number, source_url, method, source_type,
         visit_number, proxy_display, observed_ip),
    )


def save_job_attempt(job_id: int, visit_number: int, attempt_number: int,
                     proxy_id: Optional[int], proxy_display: Optional[str],
                     status: str, latency_ms: Optional[float],
                     observed_ip: Optional[str], error: Optional[str]) -> None:
    _exec(
        """INSERT INTO job_attempts
               (job_id, visit_number, attempt_number, proxy_id, proxy_display,
                status, latency_ms, observed_ip, error)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (job_id, visit_number, attempt_number, proxy_id, proxy_display,
         status, latency_ms, observed_ip, error),
    )


def get_job_numbers(job_id: int, limit: int = 200) -> list[dict]:
    return _query(
        "SELECT * FROM extracted_numbers WHERE job_id = ? ORDER BY id ASC LIMIT ?",
        (job_id, limit),
    )


def get_job_attempts(job_id: int, limit: int = 200) -> list[dict]:
    return _query(
        "SELECT * FROM job_attempts WHERE job_id = ? ORDER BY id ASC LIMIT ?",
        (job_id, limit),
    )


def get_jobs_filtered(days: Optional[int] = None, user_id: Optional[int] = None,
                      status: Optional[str] = None, limit: int = 20,
                      offset: int = 0) -> list[dict]:
    where, params = [], []
    if days is not None:
        where.append("started_at >= datetime('now', ?)")
        params.append(f"-{days} days")
    if user_id is not None:
        where.append("user_id = ?")
        params.append(user_id)
    if status:
        where.append("status = ?")
        params.append(status)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    params.extend([limit, offset])
    return _query(
        f"""SELECT * FROM extraction_jobs {clause}
            ORDER BY job_id DESC LIMIT ? OFFSET ?""",
        tuple(params),
    )


def count_jobs_filtered(days: Optional[int] = None, user_id: Optional[int] = None,
                        status: Optional[str] = None) -> int:
    where, params = [], []
    if days is not None:
        where.append("started_at >= datetime('now', ?)")
        params.append(f"-{days} days")
    if user_id is not None:
        where.append("user_id = ?")
        params.append(user_id)
    if status:
        where.append("status = ?")
        params.append(status)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    row = _query_one(f"SELECT COUNT(*) AS c FROM extraction_jobs {clause}", tuple(params))
    return int(row["c"]) if row else 0


def get_user_jobs(user_id: int, limit: int = 10) -> list[dict]:
    return _query(
        "SELECT * FROM extraction_jobs WHERE user_id = ? ORDER BY job_id DESC LIMIT ?",
        (user_id, limit),
    )


def get_user_numbers(user_id: int, limit: int = 300) -> list[dict]:
    return _query(
        "SELECT * FROM extracted_numbers WHERE user_id = ? ORDER BY id DESC LIMIT ?",
        (user_id, limit),
    )


def search_number(number: str) -> dict:
    like = f"%{number}%"
    rows = _query(
        "SELECT * FROM extracted_numbers WHERE number LIKE ? ORDER BY id DESC LIMIT 100",
        (like,),
    )
    if not rows:
        return {}
    users = sorted({r["user_id"] for r in rows})
    jobs = sorted({r["job_id"] for r in rows})
    methods = sorted({(r["extraction_method"] or "unknown") for r in rows})
    return {
        "number": number,
        "count": len(rows),
        "users": users,
        "jobs": jobs,
        "methods": methods,
        "first_seen": min((r["created_at"] or "") for r in rows),
        "last_seen": max((r["created_at"] or "") for r in rows),
    }


# ── Admin / audit ---------------------------------------------------

def get_admins() -> list[dict]:
    return _query("SELECT * FROM admins WHERE is_active = 1 ORDER BY created_at ASC")


def is_admin(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    row = _query_one(
        "SELECT user_id FROM admins WHERE user_id = ? AND is_active = 1", (user_id,)
    )
    return row is not None


def is_owner(user_id: int) -> bool:
    first_owner = _query_one(
        "SELECT user_id FROM admins WHERE role = 'OWNER' AND is_active = 1 ORDER BY created_at ASC"
    )
    if first_owner:
        return first_owner["user_id"] == user_id
    return bool(ADMIN_IDS) and user_id == ADMIN_IDS[0]


def add_admin(user_id: int, role: str, added_by: int, username: Optional[str] = None) -> None:
    _exec(
        """INSERT INTO admins (user_id, username, role, added_by, is_active)
           VALUES (?, ?, ?, ?, 1)
           ON CONFLICT(user_id) DO UPDATE SET role=excluded.role, is_active=1""",
        (user_id, username, role, added_by),
    )


def remove_admin(user_id: int) -> None:
    _exec("UPDATE admins SET is_active = 0 WHERE user_id = ?", (user_id,))


def audit(admin_id: int, action: str, target: str = "", details: str = "") -> None:
    _exec(
        """INSERT INTO admin_audit_log (admin_id, action, target, details)
           VALUES (?, ?, ?, ?)""",
        (admin_id, action, target[:200], details[:500]),
    )



# =========================================================
# Proxy parsing & validation
# =========================================================
SUPPORTED_SCHEMES = ("http", "https", "socks5", "socks5h")


class ParsedProxy:
    """A validated, parsed proxy endpoint."""
    __slots__ = ("raw", "scheme", "host", "port", "username", "password")

    def __init__(self, raw: str, scheme: str, host: str, port: int,
                 username: Optional[str], password: Optional[str]):
        self.raw = raw
        self.scheme = scheme
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    @property
    def display(self) -> str:
        """Credential-free display string."""
        return f"{self.scheme}://{self.host}:{self.port}"

    @property
    def protocol_label(self) -> str:
        if self.scheme.startswith("socks5"):
            return "SOCKS5"
        return self.scheme.upper()

    def to_requests_proxies(self) -> Optional[dict]:
        """Build a proxies dict for requests. None if SOCKS5 required but missing."""
        if self.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
            return None
        if self.username and self.password:
            u = urllib.parse.quote(self.username, safe="")
            p = urllib.parse.quote(self.password, safe="")
            url = f"{self.scheme}://{u}:{p}@{self.host}:{self.port}"
        else:
            url = f"{self.scheme}://{self.host}:{self.port}"
        return {"http": url, "https": url}

    def to_url(self) -> str:
        """Full URL with credentials (used only for urllib plumbing, never logged)."""
        if self.username and self.password:
            u = urllib.parse.quote(self.username, safe="")
            p = urllib.parse.quote(self.password, safe="")
            return f"{self.scheme}://{u}:{p}@{self.host}:{self.port}"
        return f"{self.scheme}://{self.host}:{self.port}"


def parse_proxy(raw: str) -> tuple[Optional[ParsedProxy], str]:
    """
    Parse & validate a proxy string.
    Supports: scheme://[user:pass@]host:port, and bare host:port / host:port:user:pass.
    Returns (ParsedProxy, "") or (None, reason).
    """
    raw = (raw or "").strip()
    if not raw:
        return None, "Empty proxy string"

    # Bare host:port:user:pass  or  host:port
    if "://" not in raw:
        parts = raw.split(":")
        if len(parts) == 2:
            host, port_s = parts
            if _valid_host(host) and port_s.isdigit():
                raw = f"http://{host}:{port_s}"
        elif len(parts) == 4:
            host, port_s, user, pw = parts
            if _valid_host(host) and port_s.isdigit() and user and pw:
                raw = f"http://{urllib.parse.quote(user, safe='')}:{urllib.parse.quote(pw, safe='')}@{host}:{port_s}"

    try:
        parsed = urllib.parse.urlsplit(raw)
    except Exception:
        return None, "Malformed URL"

    scheme = (parsed.scheme or "").lower()
    if scheme not in SUPPORTED_SCHEMES:
        supported = ", ".join(SUPPORTED_SCHEMES)
        return None, f"Unsupported scheme '{scheme or '?'}'. Supported: {supported}"

    if scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
        return None, "SOCKS5 requires 'requests[socks]' / PySocks — not installed"

    host = parsed.hostname
    if not host:
        return None, "Missing hostname"

    try:
        port = parsed.port
    except ValueError:
        return None, "Invalid port"
    if port is None:
        return None, "Missing port number"
    if not (1 <= port <= 65535):
        return None, f"Invalid port {port} (must be 1–65535)"

    username = password = None
    if parsed.username is not None:
        try:
            username = urllib.parse.unquote(parsed.username)
        except Exception:
            return None, "Could not decode username"
    if parsed.password is not None:
        try:
            password = urllib.parse.unquote(parsed.password)
        except Exception:
            return None, "Could not decode password"

    if (username is None) != (password is None):
        return None, "Both username and password must be provided together"

    return ParsedProxy(raw=raw, scheme=scheme, host=host, port=port,
                       username=username, password=password), ""


def _valid_host(host: str) -> bool:
    if not host:
        return False
    # IPv4 / IPv6 / hostname
    if re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", host):
        return all(0 <= int(o) <= 255 for o in host.split("."))
    if re.fullmatch(r"[A-Za-z0-9._-]+", host):
        return True
    return False


def sanitize_display(endpoint: str) -> str:
    """Credential-free display for any endpoint string."""
    parsed, _ = parse_proxy(endpoint)
    if parsed:
        return parsed.display
    try:
        p = urllib.parse.urlsplit(endpoint if "://" in endpoint else f"http://{endpoint}")
        netloc = f"{p.hostname}:{p.port}" if p.port else (p.hostname or "proxy")
        scheme = p.scheme or "http"
        return f"{scheme}://{netloc}"
    except Exception:
        return "proxy-endpoint"


# =========================================================
# Proxy health tester  (transport / exit-IP / target separated)
# =========================================================
# Multiple independent IP-check endpoints. Success of ANY one is enough for
# exit-IP verification; a single blocked/down endpoint NEVER marks a proxy dead.

_IP_CHECK_URLS = [
    "https://api.ipify.org?format=json",
    "https://httpbin.org/ip",
    "http://ip-api.com/json?fields=query",
    "https://ifconfig.me/all.json",
]

# Lightweight transport targets (a plain 2xx/3xx through the proxy is enough).
_TRANSPORT_URLS = [
    "https://httpbin.org/get",
    "https://api.ipify.org?format=json",
    "http://example.com/",
]

_IPV4_RE = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b")


class ProxyHealth:
    """Result of a proxy health test."""
    __slots__ = ("display", "protocol", "status", "latency_ms", "exit_ip",
                 "error", "transport_ok", "ip_verified", "tested_at")

    def __init__(self, display: str, protocol: str, status: str,
                 latency_ms: Optional[float], exit_ip: Optional[str],
                 error: Optional[str], transport_ok: bool, ip_verified: bool):
        self.display = display
        self.protocol = protocol
        self.status = status           # WORKING/SLOW/CONNECTED/AUTH_FAILED/TCP_FAILED/INVALID/UNTESTED/TARGET_FAILED
        self.latency_ms = latency_ms
        self.exit_ip = exit_ip
        self.error = error
        self.transport_ok = transport_ok
        self.ip_verified = ip_verified
        self.tested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    @property
    def emoji(self) -> str:
        return _STATUS_EMOJI.get(self.status, "❔")

    def to_card(self, index: Optional[int] = None) -> str:
        head = f"<b>#{index}</b> " if index is not None else ""
        lines = [f"{head}{self.emoji} <b>{html.escape(self.status)}</b>"]
        lines.append(f"🌐 {html.escape(self.protocol)} · <code>{html.escape(self.display)}</code>")
        if self.latency_ms is not None:
            lines.append(f"⚡ <code>{self.latency_ms:.0f} ms</code>")
        if self.exit_ip:
            lines.append(f"🌍 Exit IP: <code>{html.escape(self.exit_ip)}</code>")
        elif self.transport_ok:
            lines.append("⚠️ Exit IP unverified (IP service unreachable)")
        if self.error:
            lines.append(f"❗ {html.escape(self.error)}")
        return "\n".join(lines)


_STATUS_EMOJI = {
    "WORKING": "🟢", "SLOW": "🟡", "CONNECTED": "🔵", "TARGET_FAILED": "🟠",
    "AUTH_FAILED": "🟣", "TCP_FAILED": "🔴", "INVALID": "⚫",
    "UNTESTED": "⚪", "UNREACHABLE": "🔴", "UNSUPPORTED": "⚫",
}


def _tcp_check(host: str, port: int, timeout: float = 4.0) -> tuple[bool, str]:
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True, ""
    except socket.timeout:
        return False, "TCP timeout"
    except socket.gaierror as exc:
        return False, f"DNS failure: {exc.args[-1]}"
    except ConnectionRefusedError:
        return False, "Connection refused"
    except OSError as exc:
        return False, str(exc)[:80]


def _classify_requests_error(exc: Exception) -> tuple[str, str]:
    """Returns (status_category, human_reason)."""
    msg = str(exc)
    low = msg.lower()
    if "socks" in low and not _SOCKS5_AVAILABLE:
        return "UNSUPPORTED", "SOCKS5 support not installed"
    if "407" in msg or "proxy authentication" in low or "auth" in low and "proxy" in low:
        return "AUTH_FAILED", "Proxy authentication failed (HTTP 407)"
    if "timed out" in low or "timeout" in low:
        return "TCP_FAILED", "Connection timeout"
    if "refused" in low:
        return "TCP_FAILED", "Connection refused"
    if "ssl" in low or "certificate" in low:
        return "TARGET_FAILED", "TLS/SSL failure"
    if "name or service not known" in low or "nodename" in low or "getaddrinfo" in low:
        return "TCP_FAILED", "DNS failure"
    return "UNREACHABLE", msg[:110]


def _extract_ip_from_response(resp: requests.Response) -> Optional[str]:
    try:
        data = resp.json()
        if isinstance(data, dict):
            for key in ("ip", "query", "origin", "IPv4"):
                val = data.get(key)
                if val:
                    m = _IPV4_RE.search(str(val))
                    if m:
                        return m.group(1)
    except Exception:
        pass
    m = _IPV4_RE.search(resp.text or "")
    return m.group(1) if m else None


def make_requests_session(parsed: ParsedProxy, timeout: float) -> Optional[requests.Session]:
    proxies = parsed.to_requests_proxies()
    if proxies is None:
        return None
    session = requests.Session()
    session.proxies = proxies
    session.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "*/*",
    })
    return session


def test_proxy(parsed: ParsedProxy, quick: bool = True,
               target_url: Optional[str] = None,
               cancel_event: Optional[threading.Event] = None) -> ProxyHealth:
    """
    Multi-stage proxy health test.

      Stage 1  TCP reachability of the proxy endpoint.
      Stage 2  Real HTTP(S) request THROUGH the proxy (transport).
      Stage 3  Exit-IP verification against multiple independent endpoints.
      Stage 4  Optional target compatibility against a real URL.

    A proxy is WORKING if transport succeeds even when every IP-check
    endpoint is blocked. It is never marked DEAD for one failed IP service.
    """
    timeout = PROXY_HEALTH_TIMEOUT if quick else max(PROXY_HEALTH_TIMEOUT, 12.0)
    display = parsed.display
    proto = parsed.protocol_label

    def _cancelled() -> bool:
        return cancel_event is not None and cancel_event.is_set()

    # Stage 1 — TCP
    tcp_ok, tcp_err = _tcp_check(parsed.host, parsed.port, timeout=min(timeout, 4.0))
    if not tcp_ok:
        return ProxyHealth(display, proto, "TCP_FAILED", None, None,
                           f"TCP connect failed: {tcp_err}", False, False)
    if _cancelled():
        return ProxyHealth(display, proto, "UNTESTED", None, None, "Cancelled", False, False)

    session = make_requests_session(parsed, timeout)
    if session is None:
        reason = ("SOCKS5 requires 'requests[socks]' — not installed"
                  if parsed.scheme.startswith("socks5") else "Proxy configuration error")
        return ProxyHealth(display, proto, "UNSUPPORTED", None, None, reason, False, False)

    transport_ok = False
    latency_ms: Optional[float] = None
    exit_ip: Optional[str] = None
    last_error: Optional[str] = None
    last_status = "UNREACHABLE"

    try:
        # Stage 2 — transport (any 2xx/3xx is enough)
        for t_url in _TRANSPORT_URLS:
            if _cancelled():
                break
            try:
                t0 = time.perf_counter()
                r = session.get(t_url, timeout=(min(timeout, 5.0), timeout), allow_redirects=True)
                latency_ms = (time.perf_counter() - t0) * 1000
                if r.status_code < 400:
                    transport_ok = True
                    break
                last_status = "TARGET_FAILED"
                last_error = f"HTTP {r.status_code} from transport target"
            except requests.exceptions.RequestException as exc:
                last_status, last_error = _classify_requests_error(exc)
            except Exception as exc:
                last_status, last_error = "UNREACHABLE", str(exc)[:80]

        # Stage 3 — exit IP (independent of transport success; any one is enough)
        if not _cancelled():
            for ip_url in _IP_CHECK_URLS:
                try:
                    r = session.get(ip_url, timeout=(min(timeout, 5.0), timeout))
                    if r.status_code == 200:
                        ip = _extract_ip_from_response(r)
                        if ip:
                            exit_ip = ip
                            break
                except Exception:
                    continue

        # Stage 4 — optional target compatibility
        target_compat = None
        if target_url and not _cancelled():
            try:
                r = session.get(target_url, timeout=(min(timeout, 5.0), timeout),
                                allow_redirects=True)
                target_compat = "OK" if r.status_code < 400 else f"HTTP {r.status_code}"
            except Exception as exc:
                target_compat = f"FAILED: {str(exc)[:60]}"

        if not transport_ok:
            if last_status in ("AUTH_FAILED", "TCP_FAILED", "UNSUPPORTED"):
                status = last_status
            else:
                status = "TARGET_FAILED" if exit_ip else "UNREACHABLE"
            return ProxyHealth(display, proto, status, latency_ms, exit_ip,
                               last_error or "Transport request failed", False,
                               bool(exit_ip))

        # Transport worked → the proxy is usable.
        if latency_ms is not None and latency_ms >= 1500:
            status = "SLOW"
        elif exit_ip:
            status = "WORKING"
        else:
            status = "CONNECTED"   # transport OK, exit IP unverified — still usable
        return ProxyHealth(display, proto, status, latency_ms, exit_ip,
                           None if exit_ip else "Exit IP unverified (IP services unreachable)",
                           True, bool(exit_ip))
    finally:
        try:
            session.close()
        except Exception:
            pass


def classify_to_db_status(status: str) -> str:
    """Map a health status to the stored canonical category."""
    if status in ("WORKING", "CONNECTED"):
        return "WORKING"
    if status == "SLOW":
        return "SLOW"
    if status in ("AUTH_FAILED", "TARGET_FAILED", "TCP_FAILED", "UNREACHABLE",
                  "INVALID", "UNSUPPORTED"):
        return "UNHEALTHY"
    return "UNTESTED"



# =========================================================
# Proxy pool  (thread-safe rotation + health bookkeeping)
# =========================================================
from dataclasses import dataclass, field


@dataclass
class PoolEntry:
    endpoint: str
    parsed: Optional[ParsedProxy]
    proxy_id: Optional[int] = None
    scheme: str = "http"
    host: str = ""
    port: int = 0
    display: str = ""
    from_env: bool = False
    # runtime health
    cooldown_until: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    success_count: int = 0
    failure_count: int = 0
    avg_latency: float = 0.0
    last_exit_ip: str = ""
    health_status: str = "UNTESTED"
    last_used: float = 0.0
    verified: bool = False

    def available(self, now: float) -> bool:
        return self.parsed is not None and now >= self.cooldown_until


class ProxyPool:
    """
    Manages all proxy endpoints (env + DB).
    Rotation: prefer verified/fast, skip cooled-down & unhealthy, avoid immediate reuse.
    Never returns a proxy as usable unless it truly is; caller must handle None.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, PoolEntry] = {}
        self._rr_index = 0
        self._last_selected: Optional[str] = None
        self._load()

    # ── loading ─────────────────────────────────────
    def _load(self) -> None:
        with self._lock:
            self._entries.clear()
            # environment
            for raw in RAW_PROXY_ENV.split(","):
                raw = raw.strip()
                if not raw:
                    continue
                parsed, err = parse_proxy(raw)
                if not parsed:
                    logger.warning("Ignoring invalid env proxy: %s", err)
                    continue
                self._entries[raw] = PoolEntry(
                    endpoint=raw, parsed=parsed, scheme=parsed.scheme,
                    host=parsed.host, port=parsed.port, display=parsed.display,
                    from_env=True,
                )
            # database
            for row in _query("SELECT * FROM admin_proxies WHERE is_active = 1"):
                ep = row["endpoint"]
                parsed, _ = parse_proxy(ep)
                cooldown_until = 0.0
                if row.get("cooldown_until"):
                    cooldown_until = _parse_ts(row["cooldown_until"])
                self._entries[ep] = PoolEntry(
                    endpoint=ep, parsed=parsed, proxy_id=row["id"],
                    scheme=(row.get("scheme") or (parsed.scheme if parsed else "http")),
                    host=row.get("host") or (parsed.host if parsed else ""),
                    port=row.get("port") or (parsed.port if parsed else 0),
                    display=sanitize_display(ep),
                    from_env=False,
                    cooldown_until=cooldown_until,
                    consecutive_failures=row.get("consecutive_failures") or 0,
                    consecutive_successes=row.get("consecutive_successes") or 0,
                    success_count=row.get("success_count") or 0,
                    failure_count=row.get("failure_count") or 0,
                    avg_latency=row.get("average_latency") or 0.0,
                    last_exit_ip=row.get("last_observed_ip") or "",
                    health_status=row.get("health_status") or "UNTESTED",
                    verified=(row.get("success_count") or 0) > 0,
                )

    def reload(self) -> None:
        self._load()

    # ── counts ──────────────────────────────────────
    def all_endpoints(self) -> list[str]:
        with self._lock:
            return list(self._entries.keys())

    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def env_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries.values() if e.from_env)

    def db_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries.values() if not e.from_env)

    def has_endpoints(self) -> bool:
        with self._lock:
            return any(e.parsed is not None for e in self._entries.values())

    def available_count(self) -> int:
        now = time.time()
        with self._lock:
            return sum(1 for e in self._entries.values() if e.available(now))

    def healthy_count(self) -> int:
        now = time.time()
        with self._lock:
            return sum(1 for e in self._entries.values()
                       if e.available(now) and e.health_status in ("WORKING", "SLOW", "CONNECTED"))

    # ── selection ───────────────────────────────────
    def select(self, exclude: Optional[set] = None) -> Optional[PoolEntry]:
        """
        Choose the best available proxy.
        Priority: verified fast > verified normal > previously successful >
        untested > cooled-down-but-backoff-expired. Avoids immediate reuse.
        Returns None when nothing is available (caller must NOT fall back to direct).
        """
        exclude = exclude or set()
        now = time.time()
        with self._lock:
            cands = [e for e in self._entries.values()
                     if e.available(now) and e.parsed is not None and e.endpoint not in exclude]
            if not cands:
                # allow excluding less strictly? No — respect exclude but retry w/o last pick
                cands = [e for e in self._entries.values()
                         if e.available(now) and e.parsed is not None]

            if not cands:
                return None

            def rank(e: PoolEntry) -> tuple:
                status_rank = {"WORKING": 0, "CONNECTED": 1, "SLOW": 2,
                               "UNTESTED": 3, "TARGET_FAILED": 4}.get(e.health_status, 5)
                verified_rank = 0 if e.verified else 1
                latency_rank = e.avg_latency if e.avg_latency > 0 else 99999
                reuse_penalty = 1 if e.endpoint == self._last_selected else 0
                return (status_rank, verified_rank, reuse_penalty, latency_rank)

            cands.sort(key=rank)
            chosen = cands[0]
            self._last_selected = chosen.endpoint
            chosen.last_used = now
            return chosen

    def get(self, endpoint: str) -> Optional[PoolEntry]:
        with self._lock:
            return self._entries.get(endpoint)

    # ── feedback ────────────────────────────────────
    def mark_success(self, endpoint: str, latency_ms: float,
                     observed_ip: Optional[str], status: str = "WORKING") -> None:
        with self._lock:
            e = self._entries.get(endpoint)
            if not e:
                return
            e.consecutive_failures = 0
            e.consecutive_successes += 1
            e.success_count += 1
            e.cooldown_until = 0.0
            if latency_ms > 0:
                n = e.success_count
                e.avg_latency = ((e.avg_latency * (n - 1)) + latency_ms) / n if n > 1 else latency_ms
            if observed_ip:
                e.last_exit_ip = observed_ip
            e.health_status = status if status in ("WORKING", "SLOW", "CONNECTED") else "WORKING"
            e.verified = True
            pid = e.proxy_id
        if pid is not None:
            _db_proxy_success(pid, latency_ms, observed_ip or "", classify_to_db_status(status))

    def mark_failure(self, endpoint: str, error: str, status: str = "UNHEALTHY") -> None:
        """Failure with exponential backoff — never permanently kills a proxy."""
        with self._lock:
            e = self._entries.get(endpoint)
            if not e:
                return
            e.consecutive_failures += 1
            e.consecutive_successes = 0
            e.failure_count += 1
            e.health_status = status if status in _STATUS_EMOJI else "UNHEALTHY"
            # backoff: base * 2^(n-1), capped
            cd = min(PROXY_COOLDOWN_BASE * (2 ** (e.consecutive_failures - 1)), PROXY_COOLDOWN_MAX)
            e.cooldown_until = time.time() + cd
            pid = e.proxy_id
            fails = e.consecutive_failures
        if pid is not None:
            _db_proxy_failure(pid, error[:200], e.health_status if e else "UNHEALTHY", fails)

    def clear_cooldowns(self) -> None:
        with self._lock:
            for e in self._entries.values():
                e.cooldown_until = 0.0
                e.consecutive_failures = 0


def _parse_ts(value: str) -> float:
    """Parse a SQLite CURRENT_TIMESTAMP ('YYYY-MM-DD HH:MM:SS', UTC) to epoch."""
    try:
        dt = datetime.strptime(value.split(".")[0], "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return 0.0


proxy_pool = ProxyPool()


# ── proxy DB helpers ------------------------------------------------

def _db_proxy_success(proxy_id: int, latency_ms: float, observed_ip: str, status: str) -> None:
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
            new_avg = ((old_avg * old_cnt) + latency_ms) / new_cnt if latency_ms else old_avg
            conn.execute(
                """UPDATE admin_proxies
                   SET success_count = success_count + 1,
                       consecutive_successes = consecutive_successes + 1,
                       consecutive_failures = 0,
                       last_success = CURRENT_TIMESTAMP,
                       last_tested = CURRENT_TIMESTAMP,
                       average_latency = ?,
                       last_observed_ip = COALESCE(NULLIF(?, ''), last_observed_ip),
                       cooldown_until = NULL,
                       last_error = NULL,
                       health_status = ?,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (new_avg, observed_ip, status, proxy_id),
            )
            conn.commit()
        finally:
            conn.close()


def _db_proxy_failure(proxy_id: int, error: str, status: str, consecutive: int) -> None:
    cd = min(PROXY_COOLDOWN_BASE * (2 ** max(consecutive - 1, 0)), PROXY_COOLDOWN_MAX)
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE admin_proxies
                   SET failure_count = failure_count + 1,
                       consecutive_failures = consecutive_failures + 1,
                       consecutive_successes = 0,
                       last_failure = CURRENT_TIMESTAMP,
                       last_tested = CURRENT_TIMESTAMP,
                       last_error = ?,
                       health_status = ?,
                       cooldown_until = datetime('now', ?),
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (error[:200], status, f"+{int(cd)} seconds", proxy_id),
            )
            conn.commit()
        finally:
            conn.close()


def db_add_proxy(endpoint: str, added_by: int,
                 parsed: Optional[ParsedProxy] = None) -> bool:
    if parsed is None:
        parsed, _ = parse_proxy(endpoint)
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """INSERT OR IGNORE INTO admin_proxies
                       (endpoint, scheme, host, port, added_by, is_active)
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (endpoint,
                 parsed.scheme if parsed else None,
                 parsed.host if parsed else None,
                 parsed.port if parsed else None,
                 added_by),
            )
            conn.commit()
            return cur.rowcount > 0
        except sqlite3.Error as exc:
            logger.warning("db_add_proxy error: %s", exc)
            return False
        finally:
            conn.close()


def db_get_all_proxies(active_only: bool = True) -> list[dict]:
    if active_only:
        return _query("SELECT * FROM admin_proxies WHERE is_active = 1 ORDER BY id ASC")
    return _query("SELECT * FROM admin_proxies ORDER BY id ASC")


def db_get_proxy_by_endpoint(endpoint: str) -> Optional[dict]:
    return _query_one("SELECT * FROM admin_proxies WHERE endpoint = ?", (endpoint,))


def db_get_proxy(proxy_id: int) -> Optional[dict]:
    return _query_one("SELECT * FROM admin_proxies WHERE id = ?", (proxy_id,))


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
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """DELETE FROM admin_proxies
                   WHERE consecutive_failures >= 3
                     AND (success_count = 0
                          OR failure_count > success_count * 3)"""
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()


def db_proxy_stats() -> dict:
    row = _query_one(
        """SELECT
             COUNT(*) AS total,
             SUM(CASE WHEN health_status = 'UNTESTED' THEN 1 ELSE 0 END) AS untested,
             SUM(CASE WHEN health_status = 'WORKING' THEN 1 ELSE 0 END) AS working,
             SUM(CASE WHEN health_status = 'SLOW' THEN 1 ELSE 0 END) AS slow,
             SUM(CASE WHEN health_status = 'UNHEALTHY' THEN 1 ELSE 0 END) AS dead,
             AVG(CASE WHEN success_count > 0 THEN average_latency END) AS avg_latency,
             MAX(last_tested) AS last_test_time
           FROM admin_proxies WHERE is_active = 1"""
    )
    return row or {}


def update_proxy_target_compat(proxy_id: int, compat: str) -> None:
    _exec("UPDATE admin_proxies SET target_compat = ? WHERE id = ?", (compat[:80], proxy_id))



# =========================================================
# Number extraction pipeline
# =========================================================
# Each pattern carries a method tag so provenance is preserved end-to-end.

_PHONE_MIN, _PHONE_MAX = 10, 15

# Ordered so that higher-signal methods win when a number is found by several.
_METHOD_PATTERNS: list[tuple[str, str, re.Pattern]] = [
    ("wa_me", "whatsapp_url",
     re.compile(r"wa\.me/(?:p/|qr/)?\+?(\d{10,15})", re.IGNORECASE)),
    ("whatsapp_api", "whatsapp_url",
     re.compile(r"(?:api|web)\.whatsapp\.com/(?:send|message)/?\??[^\"'<>]*?"
                r"(?:phone|number)=\+?(\d{10,15})", re.IGNORECASE)),
    ("whatsapp_scheme", "whatsapp_url",
     re.compile(r"whatsapp://send\?[^\"'<>]*?(?:phone|number)=\+?(\d{10,15})", re.IGNORECASE)),
    ("tel_link", "tel_link",
     re.compile(r"tel:\+?(\d{7,15})", re.IGNORECASE)),
    ("query_parameter", "query_param",
     re.compile(r"[?&](?:phone|mobile|mobile_number|contact|"
                r"wa_number|whatsapp|send_to|recipient|number|to|mob|cell)=\+?(\d{10,15})",
                re.IGNORECASE)),
    ("json_field", "json",
     re.compile(
         r"[\"'](?:phone|phone_number|mobile|mobile_number|whatsapp|wa_number|"
         r"contact|recipient|number|telephone|cell|tel)[\"']\s*:\s*[\"']\+?(\d{10,15})[\"']",
         re.IGNORECASE)),
    ("data_attribute", "html_attr",
     re.compile(r"data-(?:phone|whatsapp|number|mobile|tel)=[\"']\+?(\d{10,15})[\"']",
                re.IGNORECASE)),
    ("html_href", "html_attr",
     re.compile(r"href=[\"'](?:tel:|whatsapp://send\?phone=)\+?(\d{10,15})[\"']",
                re.IGNORECASE)),
    ("intent", "intent",
     re.compile(r"intent://[^\"'<>]*?(?:phone|number)=\+?(\d{10,15})", re.IGNORECASE)),
]

# Looser "page_text" fallback, applied last and only to a bounded window.
_PAGE_TEXT_RE = re.compile(
    r"(?:(?:\+|00)\d[\d\s().-]{7,20}\d|\b\d{10,13}\b)"
)

COUNTRY_MIN_LEN = {   # a few sanity hints; generic rule below still applies
    "91": 12, "1": 11, "44": 12, "971": 12, "92": 12, "880": 13, "62": 12,
}


def normalize_number(raw: str) -> Optional[str]:
    """Return a consistent digits-only representation, or None if implausible."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    # strip international prefixes
    if digits.startswith("00"):
        digits = digits[2:]
    if not (_PHONE_MIN <= len(digits) <= _PHONE_MAX):
        return None
    # Reject obvious non-phones: repeated single digit, all zeros, year-like.
    if len(set(digits)) <= 2:
        return None
    if digits.startswith("0"):
        return None
    return digits


def format_number(digits: str) -> str:
    return f"+{digits}"


def extract_numbers_detailed(text: str) -> dict[str, tuple[str, str]]:
    """
    Scan one text blob and return {digits: (method, source_type)}.
    Higher-signal methods override lower ones when a number is seen twice.
    """
    found: dict[str, tuple[str, str]] = {}
    if not text:
        return found

    samples = [text]
    try:
        samples.append(urllib.parse.unquote(text))
        samples.append(urllib.parse.unquote_plus(text))
        samples.append(html.unescape(text))
    except Exception:
        pass

    for sample in samples:
        for method, source_type, pattern in _METHOD_PATTERNS:
            for match in pattern.finditer(sample):
                digits = normalize_number(match.group(1))
                if digits and digits not in found:
                    found[digits] = (method, source_type)

    # Fallback: generic page-text scan (bounded to avoid huge CPU cost)
    if len(text) < 400_000:
        for m in _PAGE_TEXT_RE.finditer(text):
            digits = normalize_number(m.group(0))
            if digits and digits not in found:
                found[digits] = ("page_text", "page_text")

    return found


def extract_from_url(url: str) -> dict[str, tuple[str, str]]:
    return extract_numbers_detailed(url)


def extract_from_html(body: str) -> dict[str, tuple[str, str]]:
    return extract_numbers_detailed(body)


# =========================================================
# URL validation & fetch engine
# =========================================================

def validate_url(url: str) -> tuple[bool, str]:
    url = (url or "").strip()
    if not url:
        return False, "Empty URL"
    if len(url) > MAX_URL_LENGTH:
        return False, f"URL too long (max {MAX_URL_LENGTH} chars)"
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False, "Malformed URL"
    if parsed.scheme not in ("http", "https"):
        return False, "URL must start with http:// or https://"
    if not parsed.hostname:
        return False, "Missing hostname in URL"
    if "." not in parsed.hostname and parsed.hostname != "localhost":
        return False, "Hostname looks invalid"
    return True, ""


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
    return ((_AES_SBOX[(w >> 24) & 0xFF] << 24) | (_AES_SBOX[(w >> 16) & 0xFF] << 16)
            | (_AES_SBOX[(w >> 8) & 0xFF] << 8) | _AES_SBOX[w & 0xFF])


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
    return bytes([dec[i] ^ b[i] for i in range(16)]).hex()


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        super().__init__()
        self.collected_redirect_targets: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.collected_redirect_targets.append(newurl)
        parsed = urllib.parse.urlparse(newurl)
        if parsed.scheme.lower() not in ("http", "https"):
            return None
        if len(self.collected_redirect_targets) > MAX_REDIRECTS:
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# =========================================================
# Fetch session  (urllib for http/https, requests for socks5)
# =========================================================
class ExtractionSession:
    """Per-request session. Isolated cookie jar. Never leaks across proxies."""

    _UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
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
        self._proxy_verified = False

        parsed_proxy = None
        if proxy_endpoint:
            parsed_proxy, _ = parse_proxy(proxy_endpoint)

        self._use_requests = parsed_proxy is not None and parsed_proxy.scheme.startswith("socks5")

        if self._use_requests:
            self._requests_session = make_requests_session(parsed_proxy, timeout)
            self._urllib_opener = None
            self._redirect_handler = None
            self._cj = None
        else:
            self._requests_session = None
            self._cj = http.cookiejar.CookieJar()
            self._redirect_handler = SafeRedirectHandler()
            handlers: list[urllib.request.BaseHandler] = [
                urllib.request.HTTPCookieProcessor(self._cj),
                self._redirect_handler,
            ]
            if parsed_proxy is not None:
                p_url = parsed_proxy.to_url()
                handlers.append(urllib.request.ProxyHandler({"http": p_url, "https": p_url}))
            self._urllib_opener = urllib.request.build_opener(*handlers)
            self._urllib_opener.addheaders = list(self._DEFAULT_HEADERS)

    def close(self) -> None:
        if self._requests_session:
            try:
                self._requests_session.close()
            except Exception:
                pass

    def fetch(self, url: str) -> tuple[str, str, list[str]]:
        """Returns (final_url, body, visited_urls)."""
        if self._use_requests:
            return self._fetch_requests(url)
        return self._fetch_urllib(url)

    def verify_exit_ip(self) -> Optional[str]:
        """Best-effort exit-IP verification through this session."""
        try:
            if self._use_requests and self._requests_session:
                for ip_url in _IP_CHECK_URLS[:2]:
                    try:
                        r = self._requests_session.get(
                            ip_url, timeout=(min(self.timeout, 5.0), self.timeout)
                        )
                        if r.status_code == 200:
                            ip = _extract_ip_from_response(r)
                            if ip:
                                return ip
                    except Exception:
                        continue
            else:
                for ip_url in _IP_CHECK_URLS[:2]:
                    try:
                        r = self._urllib_opener.open(ip_url, timeout=min(self.timeout, 5.0))
                        body = r.read(4096).decode("utf-8", "ignore")
                        m = _IPV4_RE.search(body)
                        if m:
                            return m.group(1)
                    except Exception:
                        continue
        except Exception:
            pass
        return None

    def _fetch_requests(self, url: str) -> tuple[str, str, list[str]]:
        visited = [url]
        sess = self._requests_session
        try:
            resp = sess.get(url, timeout=(min(self.timeout, 6.0), self.timeout),
                            allow_redirects=True, stream=True)
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
            _, reason = _classify_requests_error(exc)
            raise OSError(reason) from exc

    def _fetch_urllib(self, url: str) -> tuple[str, str, list[str]]:
        if self._redirect_handler:
            self._redirect_handler.collected_redirect_targets.clear()

        domain = urllib.parse.urlparse(url).hostname
        if self.cached_test_cookie and domain and self._cj is not None:
            self._set_cookie(domain, self.cached_test_cookie)

        visited = [url]
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
                body = e.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
            except Exception:
                body = ""
        except urllib.error.URLError as e:
            if self._redirect_handler:
                visited.extend(self._redirect_handler.collected_redirect_targets)
            raise OSError(_url_error_reason(e)) from e

        if self._redirect_handler:
            visited.extend(self._redirect_handler.collected_redirect_targets)

        # InfinityFree / ByetHost slowAES challenge
        if "slowAES" in body or ("toNumbers(" in body and "__test=" in body):
            matches = re.findall(r'toNumbers\("([a-f0-9]+)"\)', body)
            if len(matches) >= 3:
                self.cached_test_cookie = decrypt_byet_challenge(matches[2], matches[0], matches[1])
                if domain and self._cj is not None:
                    self._set_cookie(domain, self.cached_test_cookie)
                loc = re.search(r'location\.href\s*=\s*["\'](.*?)["\']', body)
                nxt = loc.group(1) if loc else (url + ("&i=1" if "?" in url else "?i=1"))
                next_url = urllib.parse.urljoin(current_url, nxt)
                visited.append(next_url)
                try:
                    r2 = self._urllib_opener.open(urllib.request.Request(next_url), timeout=self.timeout)
                    current_url = r2.geturl()
                    visited.append(current_url)
                    body = r2.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
                except Exception:
                    pass

        # meta-refresh / JS redirects (bounded)
        for _ in range(min(2, MAX_REDIRECTS)):
            meta = re.search(
                r'<meta[^>]*?http-equiv\s*=\s*["\']?refresh["\']?[^>]*?'
                r'content\s*=\s*["\']?[^"\'>]*?url\s*=\s*([^\s"\';>]+)',
                body, re.IGNORECASE)
            if meta:
                dest = urllib.parse.urljoin(current_url, meta.group(1).strip())
                visited.append(dest)
                if dest.lower().startswith(("http://", "https://")):
                    try:
                        r = self._urllib_opener.open(urllib.request.Request(dest), timeout=self.timeout)
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
                dest = js.group(1).strip()
                visited.append(dest)
                if dest.lower().startswith(("http://", "https://")):
                    try:
                        r = self._urllib_opener.open(urllib.request.Request(dest), timeout=self.timeout)
                        current_url = r.geturl()
                        visited.append(current_url)
                        body = r.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
                        continue
                    except Exception:
                        pass
                break
            break

        return current_url, body, visited

    def _set_cookie(self, domain: str, value: str) -> None:
        try:
            self._cj.set_cookie(http.cookiejar.Cookie(
                version=0, name="__test", value=value, port=None, port_specified=False,
                domain=domain, domain_specified=True, domain_initial_dot=False,
                path="/", path_specified=True, secure=False, expires=None,
                discard=True, comment=None, comment_url=None,
                rest={"HttpOnly": None}, rfc2109=False))
        except Exception:
            pass


def _url_error_reason(e: urllib.error.URLError) -> str:
    reason = getattr(e, "reason", e)
    text = str(reason).lower()
    if "timed out" in text or "timeout" in text:
        return "Connection timeout"
    if "refused" in text:
        return "Connection refused"
    if "name or service not known" in text or "nodename" in text or "getaddrinfo" in text:
        return "DNS failure"
    if "certificate" in text or "ssl" in text:
        return "TLS/SSL failure"
    return f"Connection error: {str(reason)[:80]}"


def add_cache_buster(url: str, cycle: int) -> str:
    """
    Cache-bust only when it is safe — never break signed URLs / tokens.
    """
    low = url.lower()
    sensitive_keys = ("sig=", "signature=", "token=", "expires=", "x-amz-", "auth=")
    if any(k in low for k in sensitive_keys):
        return url
    sep = "&" if "?" in url else "?"
    ts = int(time.time() * 1000)
    return f"{url}{sep}_cb={ts}_{cycle}_{random.randint(100, 999)}"



# =========================================================
# Job runtime  (state + independent progress monitor)
# =========================================================
class JobState:
    """Shared, lock-guarded state for one extraction job."""

    def __init__(self, job_id: int, user_id: int, chat_id: int,
                 message_id: int, url: str, mode: str, total_visits: int):
        self.job_id = job_id
        self.user_id = user_id
        self.chat_id = chat_id
        self.message_id = message_id
        self.url = url
        self.mode = mode
        self.total_visits = total_visits

        self.lock = threading.RLock()
        self.cancel = threading.Event()
        self.finished = threading.Event()
        self.started_at = time.time()

        self.stage = "Initialising…"
        self.completed_visits = 0
        self.successful = 0
        self.failed = 0
        self.unique_count = 0
        self.duplicate_count = 0
        self.current_proxy = "—"
        self.current_proxy_status = "—"
        self.current_exit_ip = "—"
        self.current_latency = 0.0
        self.retries = 0
        self.fatal_reason: Optional[str] = None

    def snapshot(self) -> dict:
        with self.lock:
            elapsed = max(int(time.time() - self.started_at), 0)
            speed = (self.completed_visits / elapsed) if elapsed > 0 else 0.0
            done = self.completed_visits
            total = self.total_visits or 1
            if speed > 0 and done < total:
                eta = int((total - done) / speed)
            else:
                eta = 0
            return {
                "job_id": self.job_id,
                "stage": self.stage,
                "completed": done,
                "total": self.total_visits,
                "successful": self.successful,
                "failed": self.failed,
                "unique": self.unique_count,
                "duplicates": self.duplicate_count,
                "proxy": self.current_proxy,
                "proxy_status": self.current_proxy_status,
                "exit_ip": self.current_exit_ip,
                "latency": self.current_latency,
                "retries": self.retries,
                "elapsed": elapsed,
                "speed": speed,
                "eta": eta,
                "mode": self.mode,
                "finished": self.finished.is_set(),
                "fatal_reason": self.fatal_reason,
            }

    def set_stage(self, stage: str) -> None:
        with self.lock:
            self.stage = stage


def render_progress(snap: dict) -> str:
    total = snap["total"] or 1
    done = snap["completed"]
    pct = min(int((done / total) * 100), 100)
    ticks = int((done / total) * 10)
    bar = "█" * ticks + "░" * (10 - ticks)
    mode_lbl = "🌐 IP Rotation" if snap["mode"] == "ROTATING" else "🟢 Direct"
    mm, ss = divmod(snap["elapsed"], 60)
    lines = [
        "⏳ <b>EXTRACTION IN PROGRESS</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🆔 Job: <b>#{snap['job_id']:06d}</b>",
        f"⚙️ Mode: {mode_lbl}",
        f"⚡ Stage: <i>{html.escape(snap['stage'])}</i>",
        "",
        f"Progress: <code>[{bar}]</code> {pct}%",
        f"🔄 Visits: <code>{done}/{snap['total']}</code>",
        f"✅ Successful: <code>{snap['successful']}</code>",
        f"❌ Failed: <code>{snap['failed']}</code>",
        f"📱 Unique Numbers: <code>{snap['unique']}</code>",
        f"♻️ Duplicates: <code>{snap['duplicates']}</code>",
    ]
    if snap["mode"] == "ROTATING":
        lines += [
            "",
            f"🌐 Proxy: <code>{html.escape(snap['proxy'])}</code>",
            f"📡 Status: {html.escape(snap['proxy_status'])}",
            f"🌍 Exit IP: <code>{html.escape(str(snap['exit_ip']))}</code>",
            f"⚡ Latency: <code>{snap['latency']:.0f} ms</code>"
            if snap["latency"] else "⚡ Latency: <code>—</code>",
        ]
    lines += [
        "",
        f"⏱ Elapsed: <code>{mm:02d}:{ss:02d}</code>",
        f"⏳ ETA: <code>{snap['eta']}s</code>",
        "━━━━━━━━━━━━━━━━━━━━",
        "<i>Tap ❌ Cancel to stop</i>",
    ]
    return "\n".join(lines)


def progress_monitor(state: JobState) -> None:
    """
    Independent updater thread: edits the progress message on a fixed interval
    regardless of blocking network calls in the worker.
    """
    last_text = ""
    while not state.finished.is_set():
        if state.cancel.is_set() and not state.finished.is_set():
            # keep showing until worker finalizes
            pass
        snap = state.snapshot()
        text = render_progress(snap)
        if text != last_text:
            try:
                bot.edit_message_text(chat_id=state.chat_id, message_id=state.message_id,
                                      text=text)
                last_text = text
            except ApiTelegramException as exc:
                desc = str(getattr(exc, "description", exc))
                if "message is not modified" in desc:
                    last_text = text
                elif "message to edit not found" in desc or "message can't be edited" in desc:
                    # Re-post a replacement status message
                    try:
                        m = bot.send_message(state.chat_id, text)
                        with state.lock:
                            state.message_id = m.message_id
                    except Exception:
                        pass
            except Exception:
                pass
        # ~PROGRESS_INTERVAL, checking finished frequently for snappy shutdown
        deadline = time.time() + PROGRESS_INTERVAL
        while time.time() < deadline and not state.finished.is_set():
            time.sleep(0.1)


# =========================================================
# Extraction worker  (rotation-aware, per-visit retry)
# =========================================================
def _attempt_fetch(url: str, mode: str, state: JobState
                   ) -> tuple[Optional[tuple[str, str, list[str]]], Optional[PoolEntry],
                              Optional[str], Optional[str], Optional[float], str]:
    """
    Perform one successful fetch for a single visit, rotating proxies on failure.
    Returns (fetch_result, entry, proxy_display, exit_ip, latency_ms, error).
    In ROTATING mode, tries up to N distinct proxies before giving up on this visit.
    Never falls back to a direct connection.
    """
    MAX_PROXY_TRIES = 4
    tried: set[str] = set()

    if mode == "NORMAL":
        state.set_stage("Connecting…")
        session = ExtractionSession(proxy_endpoint=None)
        try:
            t0 = time.perf_counter()
            state.set_stage("Fetching URL…")
            res = session.fetch(url)
            latency = (time.perf_counter() - t0) * 1000
            return res, None, None, None, latency, ""
        except Exception as exc:
            return None, None, None, None, None, str(exc)[:120]
        finally:
            session.close()

    # ROTATING
    last_err = "No proxy available"
    for attempt in range(1, MAX_PROXY_TRIES + 1):
        if state.cancel.is_set():
            return None, None, None, None, None, "Cancelled"
        entry = proxy_pool.select(exclude=tried)
        if entry is None:
            state.set_stage("Waiting for proxy…")
            last_err = "No verified proxy available"
            break
        tried.add(entry.endpoint)
        state.set_stage("Selecting proxy…")
        with state.lock:
            state.current_proxy = entry.display
            state.current_proxy_status = "🟢 Working" if entry.verified else "⚪ Untested"
            state.current_exit_ip = entry.last_exit_ip or "—"
            state.current_latency = entry.avg_latency
            state.retries += (1 if attempt > 1 else 0)

        session = ExtractionSession(proxy_endpoint=entry.endpoint)
        try:
            state.set_stage("Fetching through proxy…")
            t0 = time.perf_counter()
            res = session.fetch(url)
            latency = (time.perf_counter() - t0) * 1000

            # Verify the request actually used the proxy (best-effort exit IP)
            state.set_stage("Verifying exit IP…")
            exit_ip = session.verify_exit_ip()

            proxy_pool.mark_success(
                entry.endpoint, latency, exit_ip,
                "SLOW" if latency >= 1500 else "WORKING",
            )
            with state.lock:
                state.current_proxy_status = "🟢 Working"
                state.current_exit_ip = exit_ip or entry.last_exit_ip or "—"
                state.current_latency = latency
            save_job_attempt(state.job_id, state.completed_visits + 1, attempt,
                             entry.proxy_id, entry.display, "SUCCESS", latency,
                             exit_ip, None)
            return res, entry, entry.display, exit_ip, latency, ""
        except Exception as exc:
            err = str(exc)[:120]
            last_err = err
            status = "UNHEALTHY"
            if "auth" in err.lower() or "407" in err:
                status = "AUTH_FAILED"
            elif "tcp" in err.lower() or "refused" in err.lower() or "dns" in err.lower():
                status = "TCP_FAILED"
            proxy_pool.mark_failure(entry.endpoint, err, status)
            with state.lock:
                state.current_proxy_status = "🔴 Failed"
            save_job_attempt(state.job_id, state.completed_visits + 1, attempt,
                             entry.proxy_id, entry.display, "FAILED", None, None, err)
            state.set_stage("Retrying with another proxy…")
            continue
        finally:
            session.close()

    return None, None, None, None, None, last_err


def extraction_worker(state: JobState) -> None:
    """Runs the full job: visits → attempts → numbers → DB → channel."""
    _log_event("JOB_START", job=state.job_id, user=state.user_id,
               mode=state.mode, visits=state.total_visits)
    monitor = threading.Thread(target=progress_monitor, args=(state,), daemon=True)
    monitor.start()

    found: dict[str, tuple[str, str]] = {}      # digits -> (method, source_type)
    seen_sources: dict[str, str] = {}
    total_detections = 0
    start = time.time()

    try:
        for cycle in range(1, state.total_visits + 1):
            if state.cancel.is_set():
                state.set_stage("Cancelling…")
                break

            target_url = add_cache_buster(state.url, cycle)
            state.set_stage("Preparing visit…")

            result, entry, proxy_disp, exit_ip, latency, err = _attempt_fetch(
                target_url, state.mode, state
            )

            with state.lock:
                state.completed_visits = cycle

            if result is None:
                with state.lock:
                    state.failed += 1
                if state.mode == "ROTATING" and entry is None and err == "No verified proxy available":
                    # Distinguish "pool exhausted" from a normal failure.
                    if proxy_pool.available_count() == 0:
                        state.fatal_reason = ("❌ IP Rotation stopped — no verified proxy is "
                                              "currently available.")
                        _log_event("PROXY_POOL_EXHAUSTED", job=state.job_id)
                        break
                continue

            final_url, body, visited = result
            state.set_stage("Parsing page…")

            visit_numbers: dict[str, tuple[str, str]] = {}
            for v_url in visited:
                for digits, meta in extract_from_url(v_url).items():
                    visit_numbers.setdefault(digits, meta)
                    seen_sources.setdefault(digits, v_url)
            for digits, meta in extract_from_html(body).items():
                visit_numbers.setdefault(digits, meta)

            total_detections += len(visit_numbers)
            new_here = 0
            for digits, (method, source_type) in visit_numbers.items():
                if digits in found:
                    continue
                found[digits] = (method, source_type)
                src = seen_sources.get(digits, final_url)
                save_extracted_number(state.job_id, state.user_id, digits, src,
                                      method, source_type, cycle, proxy_disp, exit_ip)
                new_here += 1

            with state.lock:
                state.successful += 1
                state.unique_count = len(found)
                state.duplicate_count = max(total_detections - len(found), 0)
            if new_here:
                state.set_stage(f"Found {new_here} new number(s)")

            # small courtesy pause keeps target load sane and yields to cancel
            for _ in range(2):
                if state.cancel.is_set():
                    break
                time.sleep(0.05)

    finally:
        duration_ms = int((time.time() - start) * 1000)
        monitor.join(timeout=2.0)
        state.set_stage("Completed")
        state.finished.set()

        unique_count = len(found)
        duplicate_count = max(total_detections - unique_count, 0)
        cancelled = state.cancel.is_set()
        if state.fatal_reason:
            status = "FAILED"
        elif cancelled:
            status = "CANCELLED"
        elif state.failed > 0 and state.successful == 0:
            status = "FAILED"
        else:
            status = "COMPLETED"

        finalize_job(state.job_id, completed=state.completed_visits,
                     successful=state.successful, failed=state.failed,
                     unique_numbers=unique_count, duplicate_numbers=duplicate_count,
                     duration_ms=duration_ms, status=status)
        update_user_job_result(state.user_id, unique_count,
                               success=status in ("COMPLETED",))
        _log_event("JOB_COMPLETE", job=state.job_id, unique=unique_count,
                   ok=state.successful, err=state.failed, status=status)

        _deliver_results(state, found, unique_count, duplicate_count,
                         duration_ms, status, cancelled)
        _maybe_post_to_channel(state, found, unique_count, duplicate_count,
                              duration_ms, status)
        with _state_lock:
            active_jobs.pop(state.user_id, None)



# =========================================================
# Result delivery
# =========================================================
def _chunk_text(lines: list[str], limit: int = 3200) -> list[str]:
    chunks: list[str] = []
    curr = ""
    for line in lines:
        if len(curr) + len(line) + 1 > limit:
            if curr:
                chunks.append(curr.strip())
            curr = line + "\n"
        else:
            curr += line + "\n"
    if curr.strip():
        chunks.append(curr.strip())
    return chunks or [""]


def build_copy_markup(numbers_text: str) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    try:
        markup.add(types.InlineKeyboardButton(
            text="📋 Copy All Numbers",
            copy_text=types.CopyTextButton(text=numbers_text[:3900]),
        ))
    except Exception:
        try:
            markup.add(types.InlineKeyboardButton(
                text="📋 Copy All Numbers",
                switch_inline_query=numbers_text[:250],
            ))
        except Exception:
            pass
    return markup


def _deliver_results(state: JobState, found: dict[str, tuple[str, str]],
                     unique_count: int, duplicate_count: int,
                     duration_ms: int, status: str, cancelled: bool) -> None:
    mode_lbl = "🌐 IP Rotation" if state.mode == "ROTATING" else "🟢 Direct"
    dur_s = duration_ms / 1000.0
    numbers = sorted(found.keys())
    status_line = {
        "COMPLETED": "✅ <b>EXTRACTION COMPLETED</b>",
        "CANCELLED": "🛑 <b>EXTRACTION CANCELLED</b>",
        "FAILED": "❌ <b>EXTRACTION FAILED</b>",
    }.get(status, "✅ <b>EXTRACTION DONE</b>")

    header_lines = [
        status_line,
        "━━━━━━━━━━━━━━━━━━━━",
        f"🆔 Job: <b>#{state.job_id:06d}</b>",
        f"🔗 Source: <code>{html.escape(state.url[:70])}</code>",
        f"⚙️ Mode: {mode_lbl}",
        "",
        f"🔄 Visits: <code>{state.completed_visits}/{state.total_visits}</code>",
        f"✅ Successful: <code>{state.successful}</code>",
        f"❌ Failed: <code>{state.failed}</code>",
        f"📱 Unique Numbers: <code>{unique_count}</code>",
        f"♻️ Duplicates: <code>{duplicate_count}</code>",
        f"⏱ Duration: <code>{dur_s:.1f}s</code>",
    ]
    if state.fatal_reason:
        header_lines += ["", f"❗ {html.escape(state.fatal_reason)}"]
    if cancelled:
        header_lines += ["", "<i>(Stopped early by user)</i>"]

    reply_kb = main_keyboard()

    if unique_count == 0:
        try:
            bot.send_message(state.chat_id, "\n".join(header_lines), reply_markup=reply_kb)
        except Exception:
            pass
        return

    numbers_plain = "\n".join(format_number(n) for n in numbers)
    chunks = _chunk_text([format_number(n) for n in numbers], limit=3200)
    header_lines += ["━━━━━━━━━━━━━━━━━━━━", "📱 <b>NUMBERS</b>", ""]

    try:
        bot.send_message(
            state.chat_id,
            "\n".join(header_lines) + f"<code>{html.escape(chunks[0])}</code>",
            reply_markup=build_copy_markup(numbers_plain),
        )
    except Exception:
        try:
            bot.send_message(state.chat_id, "\n".join(header_lines) + chunks[0], reply_markup=reply_kb)
        except Exception:
            pass

    for idx, chunk in enumerate(chunks[1:], start=2):
        try:
            bot.send_message(state.chat_id,
                             f"📱 <b>Numbers (Part {idx})</b>\n\n<code>{html.escape(chunk)}</code>")
        except Exception:
            pass

    # TXT file
    try:
        file_text = _build_result_file(state, found, unique_count, duplicate_count,
                                       duration_ms, status)
        stream = io.BytesIO(file_text.encode("utf-8"))
        stream.name = f"job_{state.job_id:06d}_numbers.txt"
        bot.send_document(
            state.chat_id, stream,
            caption=(f"📁 <b>Result File</b>\n"
                     f"🆔 Job #{state.job_id:06d} · 📱 <code>{unique_count}</code> numbers"),
            reply_markup=reply_kb,
        )
    except Exception as exc:
        logger.warning("File upload failed for job %s: %s", state.job_id, exc)


def _build_result_file(state: JobState, found: dict[str, tuple[str, str]],
                       unique_count: int, duplicate_count: int,
                       duration_ms: int, status: str) -> str:
    mode_lbl = "IP Rotation" if state.mode == "ROTATING" else "Direct Connection"
    methods: dict[str, int] = {}
    for _d, (method, _st) in found.items():
        methods[method] = methods.get(method, 0) + 1
    lines = [
        "URL Extraction Center — Results",
        "=" * 40,
        f"Job ID: #{state.job_id:06d}",
        f"User ID: {state.user_id}",
        f"Source URL: {state.url}",
        f"Mode: {mode_lbl}",
        f"Status: {status}",
        f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        f"Visits Requested: {state.total_visits}",
        f"Visits Completed: {state.completed_visits}",
        f"Successful: {state.successful}",
        f"Failed: {state.failed}",
        f"Unique Numbers: {unique_count}",
        f"Duplicates Filtered: {duplicate_count}",
        f"Duration: {duration_ms / 1000.0:.1f}s",
        "",
        "Extraction Methods",
        "-" * 40,
    ]
    for method, cnt in sorted(methods.items(), key=lambda x: -x[1]):
        lines.append(f"  {method}: {cnt}")
    lines += ["", "Numbers", "-" * 40]
    for n in sorted(found.keys()):
        lines.append(format_number(n))
    return "\n".join(lines) + "\n"


# =========================================================
# Channel auto-post  (with limited retry + DB status)
# =========================================================
def _maybe_post_to_channel(state: JobState, found: dict[str, tuple[str, str]],
                           unique_count: int, duplicate_count: int,
                           duration_ms: int, status: str) -> None:
    if not get_setting_bool("channel_logging_enabled", False):
        return
    channel = get_setting("channel_username", "").strip()
    if not channel:
        return
    if not channel.startswith("@"):
        channel = "@" + channel

    job = get_job(state.job_id) or {}
    lines = [
        "🚀 <b>EXTRACTION COMPLETED</b>",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if get_setting_bool("channel_include_username", True) and job.get("username"):
        lines.append(f"👤 User: @{html.escape(str(job['username']))}")
    if get_setting_bool("channel_include_userid", False):
        lines.append(f"🆔 User ID: <code>{state.user_id}</code>")
    lines += [
        f"🔗 Source: <code>{html.escape(state.url[:70])}</code>",
        f"⚙️ Method: {'🌐 IP Rotation' if state.mode == 'ROTATING' else '🟢 Direct'}",
        "",
        f"🔄 Visits: <code>{state.completed_visits}/{state.total_visits}</code>",
        f"✅ Successful: <code>{state.successful}</code>",
        f"❌ Failed: <code>{state.failed}</code>",
        f"📱 Unique Numbers: <code>{unique_count}</code>",
        f"♻️ Duplicates: <code>{duplicate_count}</code>",
        f"⏱ Duration: <code>{duration_ms / 1000.0:.1f}s</code>",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🆔 Job: <b>#{state.job_id:06d}</b>",
        f"🕒 {datetime.now(timezone.utc).strftime('%d %b %Y · %H:%M UTC')}",
    ]
    post_numbers = get_setting_bool("channel_post_numbers", False)
    if post_numbers and unique_count:
        nums = sorted(found.keys())
        shown = nums[:60]
        lines += ["", "📞 <b>Numbers:</b>"]
        lines += [format_number(n) for n in shown]
        if len(nums) > len(shown):
            lines.append(f"<i>…and {len(nums) - len(shown)} more (see file)</i>")

    text = "\n".join(lines)

    msg_id = None
    last_err = None
    for attempt in range(1, 4):
        try:
            sent = bot.send_message(channel, text)
            msg_id = sent.message_id
            last_err = None
            break
        except Exception as exc:
            last_err = str(exc)[:160]
            time.sleep(1.5 * attempt)

    # Optionally attach the (already-generated) numbers file content
    if msg_id and get_setting_bool("channel_attach_txt", False) and unique_count:
        try:
            stream = io.BytesIO(_build_result_file(state, found, unique_count,
                                                   duplicate_count, duration_ms, status).encode("utf-8"))
            stream.name = f"job_{state.job_id:06d}_numbers.txt"
            bot.send_document(channel, stream)
        except Exception as exc:
            last_err = (last_err or "") + f" | file: {str(exc)[:80]}"

    _exec(
        """UPDATE extraction_jobs
           SET channel_post_status = ?, channel_message_id = ?, channel_post_error = ?
           WHERE job_id = ?""",
        ("SUCCESS" if msg_id else "FAILED", msg_id, last_err, state.job_id),
    )
    if msg_id:
        _log_event("CHANNEL_POST_SUCCESS", job=state.job_id, channel=channel)
    else:
        _log_event("CHANNEL_POST_FAILURE", job=state.job_id, error=last_err)


def test_channel(channel_username: str) -> tuple[bool, str]:
    channel = channel_username.strip()
    if not channel.startswith("@"):
        channel = "@" + channel
    try:
        me = bot.get_me()
        member = bot.get_chat_member(channel, me.id)
        status_ok = getattr(member, "status", "") in ("administrator", "creator")
        probe = bot.send_message(channel, "✅ <i>Channel connection verified.</i>")
        try:
            bot.delete_message(channel, probe.message_id)
        except Exception:
            pass
        if status_ok:
            return True, "Channel connection verified"
        return True, "Bot can post (not an admin — limited permissions)"
    except Exception as exc:
        return False, str(exc)[:160]


# =========================================================
# Keyboards
# =========================================================
def main_keyboard(user_id: Optional[int] = None) -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        types.KeyboardButton("🔗 New Extraction"),
        types.KeyboardButton("📊 My Stats"),
        types.KeyboardButton("📋 My History"),
        types.KeyboardButton("❓ Help"),
        types.KeyboardButton("📞 Support"),
    ]
    if user_id is not None and is_admin(user_id):
        buttons.append(types.KeyboardButton("🔐 Admin Panel"))
    markup.add(*buttons)
    return markup


def mode_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🟢 Direct Connection"),
        types.KeyboardButton("🌐 IP Rotation"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def visits_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🧪 Test — 1 Visit"),
        types.KeyboardButton("🚀 20 Visits"),
        types.KeyboardButton("⚡ 50 Visits"),
        types.KeyboardButton("💎 100 Visits"),
        types.KeyboardButton("🔙 Back"),
        types.KeyboardButton("❌ Cancel"),
    )
    return markup


def cancel_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("❌ Cancel"))
    return markup


def back_cancel_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("🔙 Back"), types.KeyboardButton("❌ Cancel"))
    return markup


def admin_keyboard() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2).add(
        types.KeyboardButton("📊 Dashboard"),
        types.KeyboardButton("👥 Users"),
        types.KeyboardButton("📱 Extraction Logs"),
        types.KeyboardButton("🔎 Search"),
        types.KeyboardButton("🌐 Proxy Center"),
        types.KeyboardButton("📢 Broadcast"),
        types.KeyboardButton("📡 Channel"),
        types.KeyboardButton("⚙️ Settings"),
        types.KeyboardButton("👮 Admins"),
        types.KeyboardButton("📤 Export"),
        types.KeyboardButton("🩺 Diagnostics"),
        types.KeyboardButton("🔙 Main Menu"),
    )


def admin_back_keyboard() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("🔙 Admin Panel"), types.KeyboardButton("❌ Cancel"))
    return markup


def proxy_center_keyboard() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2).add(
        types.KeyboardButton("📊 Proxy Dashboard"),
        types.KeyboardButton("➕ Add Proxies"),
        types.KeyboardButton("📋 Proxy List"),
        types.KeyboardButton("🧪 Health Check"),
        types.KeyboardButton("🔄 Retest Unhealthy"),
        types.KeyboardButton("🗑️ Cleanup"),
        types.KeyboardButton("🔙 Admin Panel"),
        types.KeyboardButton("❌ Cancel"),
    )


def proxy_status_emoji(row: dict) -> str:
    hs = row.get("health_status")
    if hs:
        return _STATUS_EMOJI.get(hs, "⚪")
    if row.get("last_tested") is None:
        return "⚪"
    if row.get("success_count", 0) == 0:
        return "🔴"
    if (row.get("average_latency") or 0) >= 1500:
        return "🟡"
    return "🟢"



# =========================================================
# User flow handlers
# =========================================================
active_jobs: dict[int, JobState] = {}


def _approval_gate(message: types.Message) -> bool:
    """Return True if the user may proceed; else send the pending/blocked notice."""
    uid = message.from_user.id
    if is_admin(uid):
        return True
    row = get_user(uid)
    if row and row.get("is_blocked"):
        bot.send_message(message.chat.id,
                         "🚫 <b>Access Blocked</b>\n\nYour access has been revoked by an admin.")
        return False
    if row and (row.get("status") or "APPROVED") == "PENDING":
        bot.send_message(
            message.chat.id,
            "🔒 <b>ACCESS PENDING</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
            "Your access request has been submitted.\n"
            "Please wait for administrator approval.",
        )
        return False
    return True


@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message) -> None:
    user = message.from_user
    register_user(user.id, user.username, user.first_name)
    bot_name = get_setting("bot_name", BOT_NAME)
    bot.send_message(
        message.chat.id,
        (f"🤖 <b>{html.escape(bot_name)}</b>\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"Fetch any HTTP/HTTPS link, follow its redirects, and extract publicly "
         f"exposed phone numbers.\n\n"
         f"<b>How to use:</b>\n"
         f"1️⃣ Tap <b>🔗 New Extraction</b>\n"
         f"2️⃣ Send a valid link\n"
         f"3️⃣ Choose mode (Direct / IP Rotation)\n"
         f"4️⃣ Choose visit count\n"
         f"5️⃣ Receive numbers + a .txt file\n\n"
         f"━━━━━━━━━━━━━━━━━━━━\n👇 Select an option below:"),
        reply_markup=main_keyboard(user.id),
    )


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "❌ <b>Access Denied.</b>")
        return
    bot.send_message(message.chat.id, "🔐 <b>ADMIN CONTROL CENTER</b>\n"
                     "━━━━━━━━━━━━━━━━━━━━\nSelect a function:",
                     reply_markup=admin_keyboard())


@bot.message_handler(commands=["cancel"])
def cmd_cancel(message: types.Message) -> None:
    _handle_cancel(message.chat.id, message.from_user.id)


def _handle_cancel(chat_id: int, user_id: int) -> None:
    with _state_lock:
        job = active_jobs.get(user_id)
        user_states[user_id] = {}
    if job and not job.finished.is_set():
        job.cancel.set()
        job.set_stage("Cancelling…")
        bot.send_message(chat_id, "🛑 <b>Cancellation requested.</b> Stopping after the "
                         "current step…")
    else:
        bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard(user_id))


@bot.message_handler(commands=["stats"])
def cmd_stats(message: types.Message) -> None:
    _send_user_stats(message.chat.id, message.from_user.id)


@bot.message_handler(commands=["history"])
def cmd_history(message: types.Message) -> None:
    _send_user_history(message.chat.id, message.from_user.id, 0)


def _send_user_stats(chat_id: int, user_id: int) -> None:
    row = get_user(user_id)
    if not row:
        bot.send_message(chat_id, "📊 No statistics yet. Run an extraction first!")
        return
    jobs = get_user_jobs(user_id, limit=1000)
    ip_jobs = sum(1 for j in jobs if j.get("mode") == "ROTATING")
    direct_jobs = len(jobs) - ip_jobs
    bot.send_message(
        chat_id,
        (f"📊 <b>MY STATISTICS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
         f"👤 Name: {html.escape(row.get('first_name') or 'User')}\n"
         f"🆔 ID: <code>{user_id}</code>\n\n"
         f"🔄 Total Jobs: <code>{row.get('total_extractions', 0)}</code>\n"
         f"✅ Successful: <code>{row.get('successful_extractions', 0)}</code>\n"
         f"❌ Failed: <code>{row.get('failed_extractions', 0)}</code>\n"
         f"📱 Unique Numbers: <code>{row.get('total_numbers_found', 0)}</code>\n\n"
         f"🌐 IP Jobs: <code>{ip_jobs}</code> · 🟢 Direct: <code>{direct_jobs}</code>\n"
         f"📅 Joined: <code>{str(row.get('joined_at', 'N/A'))[:10]}</code>\n"
         f"🕒 Last Active: <code>{str(row.get('last_active', 'N/A'))[:16]}</code>\n"
         f"━━━━━━━━━━━━━━━━━━━━"),
        reply_markup=main_keyboard(user_id),
    )


def _send_user_history(chat_id: int, user_id: int, offset: int) -> None:
    jobs = get_user_jobs(user_id, limit=10)
    if not jobs:
        bot.send_message(chat_id, "📋 <b>No extraction history yet.</b>",
                         reply_markup=main_keyboard(user_id))
        return
    lines = ["📋 <b>MY RECENT EXTRACTIONS</b>", "━━━━━━━━━━━━━━━━━━━━", ""]
    for j in jobs:
        mode_lbl = "🌐 IP" if j.get("mode") == "ROTATING" else "🟢 Direct"
        lines.append(
            f"🆔 <b>#{j['job_id']:06d}</b> · {mode_lbl} · {j.get('status', '')}\n"
            f"🔗 <code>{html.escape((j['source_url'] or '')[:40])}</code>\n"
            f"🔄 {j.get('completed_visits', 0)}/{j.get('requested_visits', 0)} · "
            f"📱 {j.get('unique_numbers', 0)} numbers · 🕒 {str(j.get('started_at', ''))[:16]}\n"
        )
    bot.send_message(chat_id, "\n".join(lines), reply_markup=main_keyboard(user_id))


# ── Extraction flow state machine ───────────────────────────────

def _start_new_extraction(chat_id: int, user_id: int) -> None:
    with _state_lock:
        if user_id in active_jobs and not active_jobs[user_id].finished.is_set():
            bot.send_message(chat_id, "⚠️ <b>A job is already running!</b> "
                             "Tap ❌ Cancel to stop it first.", reply_markup=cancel_keyboard())
            return
        user_states[user_id] = {"step": "AWAITING_URL"}
    bot.send_message(chat_id,
                     "🔗 <b>SUBMIT TARGET URL</b>\n\nSend a valid HTTP/HTTPS link:\n\n"
                     "<i>Example:</i> <code>https://example.com/redirect</code>",
                     reply_markup=cancel_keyboard())


@bot.message_handler(commands=["help"])
def cmd_help(message: types.Message) -> None:
    _send_help(message.chat.id, message.from_user.id)


def _send_help(chat_id: int, user_id: int) -> None:
    socks = "✅ Available" if _SOCKS5_AVAILABLE else "❌ Not installed"
    bot.send_message(
        chat_id,
        (f"❓ <b>HELP</b>\n━━━━━━━━━━━━━━━━━━━━\n"
         f"<b>What this bot does:</b>\n"
         f"Fetches HTTP/HTTPS URLs, follows redirects (301/302/303/307/308, "
         f"meta-refresh, JS redirects, WhatsApp/tel links), and extracts publicly "
         f"exposed phone numbers.\n\n"
         f"<b>Modes:</b>\n"
         f"• 🟢 Direct — server connection.\n"
         f"• 🌐 IP Rotation — rotates through verified proxies.\n\n"
         f"<b>Proxy support:</b> HTTP / HTTPS / SOCKS5 ({socks})\n\n"
         f"<b>Tips:</b>\n"
         f"• Duplicates are removed automatically.\n"
         f"• Results arrive as a copyable block + a .txt file.\n"
         f"• Cancel any time with ❌ Cancel."),
        reply_markup=main_keyboard(user_id),
    )


def _send_support(chat_id: int, user_id: int) -> None:
    support = get_setting("support_username", "") or get_setting("admin_display_username", "")
    line = f"Contact: @{html.escape(support)}" if support else "Contact the bot administrator."
    bot.send_message(chat_id,
                     f"📞 <b>SUPPORT</b>\n━━━━━━━━━━━━━━━━━━━━\n{line}",
                     reply_markup=main_keyboard(user_id))


# ── Broadcast send (approval-gated, rate-limited) ────────────────
def _run_broadcast(chat_id: int, admin_id: int, text: str, audience: str) -> None:
    if audience == "active":
        uids = [r["user_id"] for r in _query(
            "SELECT user_id FROM users WHERE last_active >= datetime('now','-7 days')")]
    elif audience == "approved":
        uids = get_all_user_ids(only_approved=True)
    else:
        uids = get_all_user_ids()

    status_msg = bot.send_message(chat_id, f"🚀 <b>Broadcasting to {len(uids)} users…</b>")
    sent = failed = blocked = 0
    text_html = text
    for i, uid in enumerate(uids, 1):
        try:
            bot.send_message(uid, text_html)
            sent += 1
        except ApiTelegramException as exc:
            desc = str(getattr(exc, "description", "")).lower()
            if "blocked" in desc or "deactivated" in desc or "chat not found" in desc:
                blocked += 1
            else:
                failed += 1
        except Exception:
            failed += 1
        time.sleep(0.05)
        if i % 25 == 0:
            try:
                bot.edit_message_text(
                    chat_id=chat_id, message_id=status_msg.message_id,
                    text=(f"📢 <b>Broadcasting…</b>\n"
                          f"Sent: <code>{sent}</code> · Failed: <code>{failed}</code> · "
                          f"Blocked: <code>{blocked}</code>\n"
                          f"Remaining: <code>{len(uids) - i}</code>"))
            except Exception:
                pass
    try:
        bot.edit_message_text(
            chat_id=chat_id, message_id=status_msg.message_id,
            text=(f"✅ <b>BROADCAST COMPLETE</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                  f"📤 Sent: <code>{sent}</code>\n❌ Failed: <code>{failed}</code>\n"
                  f"🚫 Blocked: <code>{blocked}</code>"))
    except Exception:
        pass
    bot.send_message(chat_id, "Admin Console:", reply_markup=admin_keyboard())
    audit(admin_id, "broadcast", target=audience, details=f"sent={sent} failed={failed}")



# =========================================================
# Admin panel handlers
# =========================================================
PAGE_SIZE = 8


def _admin_dashboard_text() -> str:
    users = _query_one("""SELECT COUNT(*) AS total,
                                 SUM(CASE WHEN last_active >= datetime('now','-1 day') THEN 1 ELSE 0 END) AS active
                          FROM users""") or {}
    jobs = _query_one("""SELECT COUNT(*) AS total,
                                SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS ok,
                                SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS bad,
                                SUM(unique_numbers) AS nums,
                                AVG(duration_ms) AS avg_ms
                         FROM extraction_jobs""") or {}
    today = _query_one("""SELECT COUNT(*) AS jobs,
                                 COALESCE(SUM(unique_numbers),0) AS nums
                          FROM extraction_jobs WHERE started_at >= datetime('now','-1 day')""") or {}
    pstats = db_proxy_stats()
    running = sum(1 for j in active_jobs.values() if not j.finished.is_set())
    avg_s = (jobs.get("avg_ms") or 0) / 1000.0
    return (
        f"📊 <b>BOT DASHBOARD</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Users: <code>{users.get('total', 0)}</code>\n"
        f"🟢 Active Today: <code>{users.get('active', 0)}</code>\n\n"
        f"🔄 Total Jobs: <code>{jobs.get('total', 0)}</code>\n"
        f"✅ Successful: <code>{jobs.get('ok', 0)}</code>\n"
        f"❌ Failed: <code>{jobs.get('bad', 0)}</code>\n\n"
        f"📱 Numbers Found: <code>{jobs.get('nums', 0) or 0}</code>\n"
        f"📅 Today: <code>{today.get('nums', 0)}</code> "
        f"(<code>{today.get('jobs', 0)}</code> jobs)\n\n"
        f"⚡ Running Jobs: <code>{running}</code>\n\n"
        f"🌐 Proxies:\n"
        f"  Total: <code>{pstats.get('total', 0) or proxy_pool.env_count()}</code>\n"
        f"  🟢 Working: <code>{pstats.get('working', 0) or 0}</code>\n"
        f"  🟡 Slow: <code>{pstats.get('slow', 0) or 0}</code>\n"
        f"  🔴 Unhealthy: <code>{pstats.get('dead', 0) or 0}</code>\n"
        f"  ⚪ Untested: <code>{pstats.get('untested', 0) or 0}</code>\n"
        f"  ⚡ Avg Latency: <code>{(pstats.get('avg_latency') or 0):.0f} ms</code>\n\n"
        f"⏱ Avg Job Time: <code>{avg_s:.1f}s</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


def _proxy_dashboard_text() -> str:
    pstats = db_proxy_stats()
    return (
        f"🌐 <b>PROXY MANAGER</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Total Endpoints: <code>{proxy_pool.count()}</code> "
        f"(env <code>{proxy_pool.env_count()}</code> / db <code>{proxy_pool.db_count()}</code>)\n"
        f"⚡ Available Now: <code>{proxy_pool.available_count()}</code>\n"
        f"✅ Verified Healthy: <code>{proxy_pool.healthy_count()}</code>\n\n"
        f"🟢 Working: <code>{pstats.get('working', 0) or 0}</code>\n"
        f"🟡 Slow: <code>{pstats.get('slow', 0) or 0}</code>\n"
        f"🔴 Unhealthy: <code>{pstats.get('dead', 0) or 0}</code>\n"
        f"⚪ Untested: <code>{pstats.get('untested', 0) or 0}</code>\n"
        f"⚡ Avg Latency: <code>{(pstats.get('avg_latency') or 0):.0f} ms</code>\n"
        f"🕒 Last Test: <code>{str(pstats.get('last_test_time') or 'Never')[:19]}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


def _list_proxies(chat_id: int, page: int = 0) -> None:
    rows = db_get_all_proxies(active_only=False)
    env_eps = [e for e in proxy_pool.all_endpoints()
               if proxy_pool.get(e) and proxy_pool.get(e).from_env]
    if not rows and not env_eps:
        bot.send_message(chat_id, "📋 <b>No proxies configured yet.</b>",
                         reply_markup=proxy_center_keyboard())
        return

    if env_eps:
        lines = ["⚙️ <b>Environment Proxies (read-only):</b>"]
        for ep in env_eps:
            lines.append(f"  • <code>{html.escape(sanitize_display(ep))}</code>")
        bot.send_message(chat_id, "\n".join(lines))

    if not rows:
        bot.send_message(chat_id, "Proxy Center:", reply_markup=proxy_center_keyboard())
        return

    total_pages = (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    chunk = rows[start:start + PAGE_SIZE]

    header = (f"📋 <b>PROXY LIST</b> — page {page + 1}/{total_pages}\n"
              f"━━━━━━━━━━━━━━━━━━━━\n")
    body = []
    for r in chunk:
        emoji = proxy_status_emoji(r)
        lat = f"{r['average_latency']:.0f} ms" if r.get("average_latency") else "N/A"
        ip = r.get("last_observed_ip") or "—"
        body.append(
            f"{emoji} <b>#{r['id']}</b> {(r.get('scheme') or '').upper()} "
            f"<code>{html.escape(sanitize_display(r['endpoint']))}</code>\n"
            f"   ⚡ {lat} · 🌍 <code>{html.escape(ip)}</code>\n"
            f"   ✅ {r.get('success_count', 0)} · ❌ {r.get('failure_count', 0)} · "
            f"{html.escape((r.get('health_status') or 'UNTESTED'))}"
        )
    text = header + "\n\n".join(body)
    markup = types.InlineKeyboardMarkup(row_width=3)
    nav = []
    for r in chunk:
        nav.append(types.InlineKeyboardButton(f"🗑️ #{r['id']}", callback_data=f"delp:{r['id']}"))
    markup.add(*nav)
    page_btns = []
    if page > 0:
        page_btns.append(types.InlineKeyboardButton("◀️ Prev", callback_data=f"plist:{page-1}"))
    if page < total_pages - 1:
        page_btns.append(types.InlineKeyboardButton("Next ▶️", callback_data=f"plist:{page+1}"))
    if page_btns:
        markup.add(*page_btns)
    bot.send_message(chat_id, text, reply_markup=markup)


def _run_health_check(chat_id: int, endpoints: Optional[list[str]] = None,
                      admin_id: Optional[int] = None) -> None:
    eps = endpoints if endpoints is not None else proxy_pool.all_endpoints()
    if not eps:
        bot.send_message(chat_id, "⚠️ No proxies configured.", reply_markup=proxy_center_keyboard())
        return

    status = bot.send_message(chat_id, f"🧪 <b>Checking {len(eps)} proxies…</b>")
    cancel = threading.Event()
    lock = threading.Lock()
    counters = {"tested": 0, "working": 0, "slow": 0, "failed": 0}
    last_update = [0.0]

    def _one(raw: str) -> tuple[str, Optional[ProxyHealth]]:
        parsed, err = parse_proxy(raw)
        if not parsed:
            return raw, ProxyHealth(sanitize_display(raw), "?", "INVALID", None, None, err, False, False)
        return raw, test_proxy(parsed, quick=True, cancel_event=cancel)

    def _update(force: bool = False) -> None:
        now = time.time()
        if not force and now - last_update[0] < 1.2:
            return
        last_update[0] = now
        d = counters["tested"]
        pct = int((d / len(eps)) * 100) if eps else 0
        ticks = int((d / len(eps)) * 10) if eps else 0
        bar = "█" * ticks + "░" * (10 - ticks)
        try:
            bot.edit_message_text(
                chat_id=chat_id, message_id=status.message_id,
                text=(f"🧪 <b>PROXY HEALTH CHECK</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                      f"Progress: <code>[{bar}]</code> {pct}%\n\n"
                      f"Checked: <code>{d}/{len(eps)}</code>\n"
                      f"✅ Working: <code>{counters['working']}</code>\n"
                      f"🟡 Slow: <code>{counters['slow']}</code>\n"
                      f"❌ Failed: <code>{counters['failed']}</code>\n"
                      f"━━━━━━━━━━━━━━━━━━━━"))
        except Exception:
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as pool:
        futures = [pool.submit(_one, raw) for raw in eps]
        for fut in concurrent.futures.as_completed(futures):
            raw, health = fut.result()
            with lock:
                counters["tested"] += 1
                entry = proxy_pool.get(raw)
                if health.status in ("WORKING", "CONNECTED"):
                    counters["working"] += 1
                    proxy_pool.mark_success(raw, health.latency_ms or 0, health.exit_ip,
                                            health.status)
                elif health.status == "SLOW":
                    counters["slow"] += 1
                    proxy_pool.mark_success(raw, health.latency_ms or 0, health.exit_ip, "SLOW")
                else:
                    counters["failed"] += 1
                    proxy_pool.mark_failure(raw, health.error or "health check failed",
                                            classify_to_db_status(health.status))
                _update()

    _update(force=True)
    summary = (f"✅ <b>HEALTH CHECK COMPLETE</b>\n━━━━━━━━━━━━━━━━━━━━\n"
               f"Checked: <code>{counters['tested']}</code>\n"
               f"🟢 Working: <code>{counters['working']}</code>\n"
               f"🟡 Slow: <code>{counters['slow']}</code>\n"
               f"🔴 Failed: <code>{counters['failed']}</code>")
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=status.message_id, text=summary)
    except Exception:
        bot.send_message(chat_id, summary)
    bot.send_message(chat_id, "Proxy Center:", reply_markup=proxy_center_keyboard())
    if admin_id:
        audit(admin_id, "proxy_health_check", details=f"checked={len(eps)}")


def _show_users(chat_id: int, page: int = 0) -> None:
    total = (_query_one("SELECT COUNT(*) AS c FROM users") or {}).get("c", 0)
    rows = _query("SELECT * FROM users ORDER BY last_active DESC LIMIT ? OFFSET ?",
                  (PAGE_SIZE, page * PAGE_SIZE))
    if not rows:
        bot.send_message(chat_id, "👥 No users yet.", reply_markup=admin_keyboard())
        return
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    lines = [f"👥 <b>USERS</b> — page {page + 1}/{total_pages} (total {total})",
             "━━━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        lines.append(
            f"👤 {html.escape(r.get('first_name') or 'User')} "
            f"(@{html.escape(r.get('username') or 'none')})\n"
            f"   🆔 <code>{r['user_id']}</code> · {r.get('status', 'APPROVED')}"
            f"{' 🚫' if r.get('is_blocked') else ''}\n"
            f"   🔄 {r.get('total_extractions', 0)} · 📱 {r.get('total_numbers_found', 0)}"
        )
    markup = types.InlineKeyboardMarkup(row_width=3)
    for r in rows:
        markup.add(types.InlineKeyboardButton(f"👤 {r['user_id']}",
                                              callback_data=f"user:{r['user_id']}"))
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("◀️", callback_data=f"ulist:{page-1}"))
    if (page + 1) * PAGE_SIZE < total:
        nav.append(types.InlineKeyboardButton("▶️", callback_data=f"ulist:{page+1}"))
    if nav:
        markup.add(*nav)
    bot.send_message(chat_id, "\n".join(lines), reply_markup=markup)


def _user_profile_text(user_id: int) -> str:
    row = get_user(user_id)
    if not row:
        return "❓ User not found."
    return (
        f"👤 <b>USER PROFILE</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"Name: {html.escape(row.get('first_name') or 'User')}\n"
        f"Username: @{html.escape(row.get('username') or 'none')}\n"
        f"Telegram ID: <code>{user_id}</code>\n"
        f"Status: {row.get('status', 'APPROVED')}{' 🚫 BLOCKED' if row.get('is_blocked') else ''}\n\n"
        f"🔄 Total Jobs: <code>{row.get('total_extractions', 0)}</code>\n"
        f"✅ Successful: <code>{row.get('successful_extractions', 0)}</code>\n"
        f"❌ Failed: <code>{row.get('failed_extractions', 0)}</code>\n"
        f"📱 Total Numbers: <code>{row.get('total_numbers_found', 0)}</code>\n\n"
        f"📅 Joined: <code>{str(row.get('joined_at', ''))[:16]}</code>\n"
        f"🕒 Last Active: <code>{str(row.get('last_active', ''))[:16]}</code>\n"
        f"🕒 Last Extraction: <code>{str(row.get('last_extraction_at') or '—')[:16]}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


def _user_profile_markup(user_id: int, blocked: bool) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📋 History", callback_data=f"uhist:{user_id}"),
        types.InlineKeyboardButton("📱 Numbers", callback_data=f"unums:{user_id}"),
    )
    if blocked:
        markup.add(types.InlineKeyboardButton("✅ Unblock", callback_data=f"unblock:{user_id}"))
    else:
        markup.add(types.InlineKeyboardButton("🚫 Block", callback_data=f"block:{user_id}"))
    return markup


def _show_extraction_logs(chat_id: int, days: Optional[int] = None,
                          status: Optional[str] = None, page: int = 0) -> None:
    total = count_jobs_filtered(days=days, status=status)
    rows = get_jobs_filtered(days=days, status=status, limit=PAGE_SIZE, offset=page * PAGE_SIZE)
    filter_lbl = "All Time" if days is None else f"Last {days}d"
    if not rows:
        bot.send_message(chat_id, f"📱 No jobs found ({filter_lbl}).", reply_markup=admin_keyboard())
        return
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    lines = [f"📱 <b>EXTRACTION LOGS</b> — {filter_lbl} · page {page + 1}/{total_pages}",
             "━━━━━━━━━━━━━━━━━━━━"]
    for j in rows:
        mode_lbl = "🌐 IP" if j.get("mode") == "ROTATING" else "🟢 Direct"
        lines.append(
            f"🆔 <b>#{j['job_id']:06d}</b> · @{html.escape(j.get('username') or 'user')} · {mode_lbl}\n"
            f"   📱 {j.get('unique_numbers', 0)} nums · ✅ {j.get('successful_visits', 0)}/{j.get('requested_visits', 0)} · "
            f"🕒 {str(j.get('started_at', ''))[:16]} · {j.get('status', '')}"
        )
    markup = types.InlineKeyboardMarkup(row_width=4)
    for j in rows:
        markup.add(types.InlineKeyboardButton(f"#{j['job_id']:06d}", callback_data=f"job:{j['job_id']}"))
    filt = types.InlineKeyboardMarkup(row_width=5)
    filt.add(
        types.InlineKeyboardButton("Today", callback_data="logs:1:0"),
        types.InlineKeyboardButton("7d", callback_data="logs:7:0"),
        types.InlineKeyboardButton("30d", callback_data="logs:30:0"),
        types.InlineKeyboardButton("All", callback_data="logs:-1:0"),
    )
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("◀️", callback_data=f"logspage:{days if days is not None else -1}:{page-1}"))
    if (page + 1) * PAGE_SIZE < total:
        nav.append(types.InlineKeyboardButton("▶️", callback_data=f"logspage:{days if days is not None else -1}:{page+1}"))
    if nav:
        filt.add(*nav)
    bot.send_message(chat_id, "\n".join(lines), reply_markup=filt)


def _job_detail_text(job_id: int) -> str:
    j = get_job(job_id)
    if not j:
        return "❓ Job not found."
    mode_lbl = "🌐 IP Rotation" if j.get("mode") == "ROTATING" else "🟢 Direct"
    nums = get_job_numbers(job_id, limit=100)
    methods: dict[str, int] = {}
    for n in get_job_numbers(job_id, limit=5000):
        methods[n["extraction_method"]] = methods.get(n["extraction_method"], 0) + 1
    attempts = get_job_attempts(job_id, limit=5000)
    proxy_ok = sum(1 for a in attempts if a["status"] == "SUCCESS")
    proxy_fail = sum(1 for a in attempts if a["status"] == "FAILED")
    lines = [
        f"🆔 <b>JOB #{job_id:06d}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"👤 User: @{html.escape(j.get('username') or 'user')}",
        f"🆔 ID: <code>{j.get('user_id', '')}</code>",
        f"🔗 <code>{html.escape((j.get('source_url') or '')[:70])}</code>",
        f"⚙️ Mode: {mode_lbl} · {j.get('status', '')}",
        "",
        f"🔄 Visits: <code>{j.get('completed_visits', 0)}/{j.get('requested_visits', 0)}</code>",
        f"✅ Successful: <code>{j.get('successful_visits', 0)}</code>",
        f"❌ Failed: <code>{j.get('failed_visits', 0)}</code>",
        f"📱 Unique Numbers: <code>{j.get('unique_numbers', 0)}</code>",
        f"♻️ Duplicates: <code>{j.get('duplicate_numbers', 0)}</code>",
        f"⏱ Duration: <code>{(j.get('duration_ms') or 0) / 1000.0:.1f}s</code>",
    ]
    if methods:
        lines += ["", "🔎 <b>Methods:</b>"]
        for m, c in sorted(methods.items(), key=lambda x: -x[1]):
            lines.append(f"   • {html.escape(m or 'unknown')}: {c}")
    if attempts:
        lines += ["", f"🌐 Proxy attempts: ✅ {proxy_ok} · ❌ {proxy_fail}"]
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📱 <b>Numbers ({len(nums)}):</b>")
    for n in nums[:50]:
        lines.append(f"<code>{format_number(n['number'])}</code> · {html.escape(n['extraction_method'] or '')}")
    if len(nums) > 50:
        lines.append(f"<i>…and {len(nums) - 50} more</i>")
    return "\n".join(lines)



# =========================================================
# Admin settings / admins / channel
# =========================================================
SETTING_TOGGLES: list[tuple[str, str, str]] = [
    ("maintenance_mode", "🛠 Maintenance Mode", "🔴 ON / 🟢 OFF"),
    ("approval_mode", "🔒 Approval Required", "New users need approval"),
    ("channel_logging_enabled", "📡 Channel Auto-Post", "Post results automatically"),
    ("channel_post_numbers", "📞 Publish Numbers", "Include numbers in channel"),
    ("channel_attach_txt", "📁 Attach TXT to Channel", "Send result file too"),
    ("channel_include_username", "👤 Include Username", "Show @username in post"),
    ("channel_include_userid", "🆔 Include User ID", "Show Telegram ID in post"),
    ("auto_proxy_retest", "♻️ Auto Proxy Retest", "Background health checks"),
]


def _settings_text() -> str:
    lines = ["⚙️ <b>BOT SETTINGS</b>", "━━━━━━━━━━━━━━━━━━━━"]
    for key, label, _desc in SETTING_TOGGLES:
        state = "🔴 ON" if get_setting_bool(key, False) else "🟢 OFF"
        lines.append(f"{label}: <b>{state}</b>")
    lines += [
        "",
        f"📡 Channel: <code>{html.escape(get_setting('channel_username', '') or '—')}</code>",
        f"👤 Admin Display: <code>{html.escape(get_setting('admin_display_username', '') or '—')}</code>",
        f"📞 Support: <code>{html.escape(get_setting('support_username', '') or '—')}</code>",
        f"🤖 Bot Name: <code>{html.escape(get_setting('bot_name', BOT_NAME))}</code>",
        f"✍️ Max Visits: <code>{get_setting_int('max_visits', MAX_VISITS_PER_JOB)}</code>",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)


def _settings_markup() -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=2)
    for key, label, _desc in SETTING_TOGGLES:
        state = "🔴" if get_setting_bool(key, False) else "🟢"
        markup.add(types.InlineKeyboardButton(f"{state} {label}", callback_data=f"tog:{key}"))
    markup.add(
        types.InlineKeyboardButton("📡 Set Channel", callback_data="setch"),
        types.InlineKeyboardButton("👤 Set Admin Username", callback_data="setadminuser"),
    )
    markup.add(
        types.InlineKeyboardButton("📞 Set Support Username", callback_data="setsupport"),
        types.InlineKeyboardButton("🤖 Set Bot Name", callback_data="setbotname"),
    )
    markup.add(types.InlineKeyboardButton("🔤 Set Max Visits", callback_data="setmaxvisits"))
    return markup


def _admins_text() -> str:
    rows = get_admins()
    lines = ["👮 <b>ADMIN MANAGEMENT</b>", "━━━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        lines.append(f"• <code>{r['user_id']}</code> — {r.get('role', 'ADMIN')}"
                     f" (@{html.escape(r.get('username') or 'none')})")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("<i>Authorization is numeric-ID based. Username is display only.</i>")
    return "\n".join(lines)


# =========================================================
# Callback router
# =========================================================
def _guard_admin(call: types.CallbackQuery) -> bool:
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return False
    return True


@bot.callback_query_handler(func=lambda c: c.data.startswith("delp:"))
def cb_delete_proxy(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    pid = int(call.data.split(":")[1])
    db_delete_proxy(pid)
    proxy_pool.reload()
    bot.answer_callback_query(call.id, f"🗑️ Proxy #{pid} deleted.")
    audit(call.from_user.id, "delete_proxy", target=str(pid))


@bot.callback_query_handler(func=lambda c: c.data.startswith("plist:"))
def cb_proxy_page(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    bot.answer_callback_query(call.id)
    page = int(call.data.split(":")[1])
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    _list_proxies(call.message.chat.id, page)


@bot.callback_query_handler(func=lambda c: c.data.startswith("ulist:"))
def cb_user_page(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    bot.answer_callback_query(call.id)
    page = int(call.data.split(":")[1])
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    _show_users(call.message.chat.id, page)


@bot.callback_query_handler(func=lambda c: c.data.startswith("user:"))
def cb_user_detail(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    bot.answer_callback_query(call.id)
    uid = int(call.data.split(":")[1])
    row = get_user(uid) or {}
    bot.send_message(call.message.chat.id, _user_profile_text(uid),
                     reply_markup=_user_profile_markup(uid, bool(row.get("is_blocked"))))


@bot.callback_query_handler(func=lambda c: c.data.startswith("uhist:"))
def cb_user_history(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    bot.answer_callback_query(call.id)
    uid = int(call.data.split(":")[1])
    jobs = get_user_jobs(uid, limit=15)
    if not jobs:
        bot.send_message(call.message.chat.id, "📋 No jobs for this user.")
        return
    lines = [f"📋 <b>HISTORY</b> — <code>{uid}</code>", "━━━━━━━━━━━━━━━━━━━━"]
    for j in jobs:
        lines.append(f"🆔 #{j['job_id']:06d} · 📱 {j.get('unique_numbers', 0)} · "
                     f"{str(j.get('started_at', ''))[:16]} · {j.get('status', '')}")
    bot.send_message(call.message.chat.id, "\n".join(lines))


@bot.callback_query_handler(func=lambda c: c.data.startswith("unums:"))
def cb_user_numbers(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    bot.answer_callback_query(call.id)
    uid = int(call.data.split(":")[1])
    nums = get_user_numbers(uid, limit=200)
    if not nums:
        bot.send_message(call.message.chat.id, "📱 No numbers for this user.")
        return
    lines = [f"📱 <b>NUMBERS EXTRACTED</b> — <code>{uid}</code>", "━━━━━━━━━━━━━━━━━━━━"]
    for n in nums[:100]:
        lines.append(f"<code>{format_number(n['number'])}</code> · "
                     f"job #{n['job_id']:06d} · {html.escape(n['extraction_method'] or '')}")
    if len(nums) > 100:
        lines.append(f"<i>…and {len(nums) - 100} more</i>")
    bot.send_message(call.message.chat.id, "\n".join(lines))


@bot.callback_query_handler(func=lambda c: c.data.startswith("block:") or c.data.startswith("unblock:"))
def cb_block_user(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    action, uid_s = call.data.split(":")
    uid = int(uid_s)
    blocked = action == "block"
    set_user_blocked(uid, blocked)
    bot.answer_callback_query(call.id, "🚫 Blocked." if blocked else "✅ Unblocked.")
    audit(call.from_user.id, "block_user" if blocked else "unblock_user", target=str(uid))
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                      reply_markup=_user_profile_markup(uid, blocked))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("job:"))
def cb_job_detail(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    bot.answer_callback_query(call.id)
    jid = int(call.data.split(":")[1])
    bot.send_message(call.message.chat.id, _job_detail_text(jid))


@bot.callback_query_handler(func=lambda c: c.data.startswith("logs:") or c.data.startswith("logspage:"))
def cb_logs(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    bot.answer_callback_query(call.id)
    parts = call.data.split(":")
    days = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0
    _show_extraction_logs(call.message.chat.id,
                          days=None if days < 0 else days, page=page)


@bot.callback_query_handler(func=lambda c: c.data.startswith("tog:"))
def cb_toggle_setting(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    key = call.data.split(":", 1)[1]
    cur = get_setting_bool(key, False)
    set_setting(key, "0" if cur else "1")
    bot.answer_callback_query(call.id, f"{key} → {'OFF' if cur else 'ON'}")
    audit(call.from_user.id, "toggle_setting", target=key, details=f"->{'OFF' if cur else 'ON'}")
    try:
        bot.edit_message_text(_settings_text(), call.message.chat.id, call.message.message_id,
                              reply_markup=_settings_markup())
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data in
                            ("setch", "setadminuser", "setsupport", "setbotname", "setmaxvisits"))
def cb_setting_input(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    mapping = {
        "setch": ("channel_username", "📡 Send the channel username (e.g. @MyChannel):"),
        "setadminuser": ("admin_display_username", "👤 Send the admin display username:"),
        "setsupport": ("support_username", "📞 Send the support username:"),
        "setbotname": ("bot_name", "🤖 Send the bot display name:"),
        "setmaxvisits": ("max_visits", "🔤 Send the max visits per job (number):"),
    }
    key, prompt = mapping[call.data]
    with _state_lock:
        user_states[call.from_user.id] = {"step": "ADMIN_SETTING_INPUT", "key": key}
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, prompt, reply_markup=admin_back_keyboard())


@bot.callback_query_handler(func=lambda c: c.data.startswith("testch:"))
def cb_test_channel(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    bot.answer_callback_query(call.id, "Testing…")
    ch = call.data.split(":", 1)[1]
    ok, msg = test_channel(ch if ch != "-" else get_setting("channel_username", ""))
    bot.send_message(call.message.chat.id, ("✅ " if ok else "❌ ") + html.escape(msg))


@bot.callback_query_handler(func=lambda c: c.data.startswith("appr:") or c.data.startswith("rej:"))
def cb_approve_user(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    action, uid_s = call.data.split(":")
    uid = int(uid_s)
    if action == "appr":
        set_user_status(uid, "APPROVED")
        bot.answer_callback_query(call.id, "✅ Approved.")
        audit(call.from_user.id, "approve_user", target=str(uid))
        try:
            bot.send_message(uid, "✅ <b>ACCESS APPROVED</b>\n\nYou can now use the bot. "
                             "Tap /start to begin.")
        except Exception:
            pass
        try:
            bot.edit_message_text(call.message.chat.id, call.message.message_id,
                                  text=call.message.text + "\n\n✅ <i>Approved</i>")
        except Exception:
            pass
    else:
        set_user_status(uid, "BLOCKED")
        set_user_blocked(uid, True)
        bot.answer_callback_query(call.id, "❌ Rejected.")
        audit(call.from_user.id, "reject_user", target=str(uid))
        try:
            bot.edit_message_text(call.message.chat.id, call.message.message_id,
                                  text=call.message.text + "\n\n❌ <i>Rejected</i>")
        except Exception:
            pass



# =========================================================
# Main message router
# =========================================================
@bot.message_handler(func=lambda m: True, content_types=["text"])
def handle_all(message: types.Message) -> None:
    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()
    register_user(user.id, user.username, user.first_name)

    with _state_lock:
        state = dict(user_states.get(user.id, {}))

    # ── Global controls ──────────────────────────────
    if text == "❌ Cancel":
        _handle_cancel(chat_id, user.id)
        return

    if text == "🔙 Main Menu":
        with _state_lock:
            user_states[user.id] = {}
        bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard(user.id))
        return

    # ── Maintenance gate ─────────────────────────────
    if get_setting_bool("maintenance_mode", False) and not is_admin(user.id):
        bot.send_message(chat_id, "🛠 <b>BOT UNDER MAINTENANCE</b>\n\nPlease try again later.")
        return

    # ── Admin-only inbound states ────────────────────
    if is_admin(user.id):
        if _handle_admin_state(message, state, text):
            return

    # ── Approval / block gate ────────────────────────
    if not _approval_gate(message):
        return

    # ── User menu ────────────────────────────────────
    if text in ("🔗 New Extraction", "🔗 Send New Link"):
        _start_new_extraction(chat_id, user.id)
        return

    if text == "📊 My Stats":
        _send_user_stats(chat_id, user.id)
        return

    if text == "📋 My History":
        _send_user_history(chat_id, user.id, 0)
        return

    if text == "❓ Help":
        _send_help(chat_id, user.id)
        return

    if text == "📞 Support":
        _send_support(chat_id, user.id)
        return

    if text == "🔐 Admin Panel" and is_admin(user.id):
        bot.send_message(chat_id, "🔐 <b>ADMIN CONTROL CENTER</b>\n━━━━━━━━━━━━━━━━━━━━",
                         reply_markup=admin_keyboard())
        return

    # ── Extraction flow ──────────────────────────────
    if state.get("step") == "AWAITING_URL" or text.startswith(("http://", "https://")):
        if _handle_url_input(message, state, text):
            return

    if state.get("step") == "AWAITING_MODE":
        if _handle_mode_input(message, state, text):
            return

    if state.get("step") == "AWAITING_VISITS":
        if _handle_visits_input(message, state, text):
            return

    bot.send_message(chat_id, "❓ Please select an option from the menu or tap "
                     "<b>🔗 New Extraction</b>.", reply_markup=main_keyboard(user.id))


def _handle_url_input(message: types.Message, state: dict, text: str) -> bool:
    if state.get("step") != "AWAITING_URL" and not text.startswith(("http://", "https://")):
        return False
    valid, err = validate_url(text)
    if not valid:
        bot.send_message(message.chat.id,
                         f"⚠️ <b>Invalid URL</b>\n\n{html.escape(err)}\n\nPlease send a valid link.",
                         reply_markup=cancel_keyboard())
        return True
    with _state_lock:
        user_states[message.from_user.id] = {"step": "AWAITING_MODE", "url": text}
    bot.send_message(
        message.chat.id,
        (f"🔗 <b>Link received</b>\n<code>{html.escape(text[:80])}</code>\n\n"
         f"⚙️ <b>SELECT CONNECTION MODE</b>\n━━━━━━━━━━━━━━━━━━━━\n"
         f"🟢 <b>Direct Connection</b> — your server's connection.\n"
         f"🌐 <b>IP Rotation</b> — rotates through verified proxies."),
        reply_markup=mode_keyboard(),
    )
    return True


def _handle_mode_input(message: types.Message, state: dict, text: str) -> bool:
    url = state.get("url", "")
    if text == "🟢 Direct Connection":
        with _state_lock:
            user_states[message.from_user.id] = {"step": "AWAITING_VISITS", "url": url, "mode": "NORMAL"}
        bot.send_message(message.chat.id,
                         "🟢 <b>Direct Connection</b>\n\nSelect how many visits to run:",
                         reply_markup=visits_keyboard())
        return True
    if text == "🌐 IP Rotation":
        if not get_setting_bool("proxy_enabled", True) or not proxy_pool.has_endpoints():
            bot.send_message(message.chat.id,
                             "⚠️ <b>IP Rotation Unavailable</b>\n\n"
                             "No proxy endpoints are configured.\n"
                             "Admins can add proxies via <b>Admin → 🌐 Proxy Center</b>.\n\n"
                             "<i>You can still use 🟢 Direct Connection.</i>",
                             reply_markup=mode_keyboard())
            return True
        healthy = proxy_pool.healthy_count()
        avail = proxy_pool.available_count()
        if avail == 0:
            bot.send_message(message.chat.id,
                             "⚠️ <b>IP Rotation Currently Unavailable</b>\n\n"
                             "No verified working proxy is available right now.\n"
                             "Please try again later.",
                             reply_markup=mode_keyboard())
            return True
        note = ""
        if healthy == 0:
            note = ("\n\n<i>⚠️ Proxies are configured but not yet verified. "
                    "They'll be tested on the fly.</i>")
        with _state_lock:
            user_states[message.from_user.id] = {"step": "AWAITING_VISITS", "url": url, "mode": "ROTATING"}
        bot.send_message(message.chat.id,
                         f"🌐 <b>IP Rotation</b>\n\n"
                         f"📡 Available proxies: <code>{avail}</code>\n"
                         f"✅ Verified healthy: <code>{healthy}</code>{note}\n\n"
                         f"Select how many visits to run:",
                         reply_markup=visits_keyboard())
        return True
    if text == "🔙 Back":
        with _state_lock:
            user_states[message.from_user.id] = {"step": "AWAITING_URL"}
        _start_new_extraction(message.chat.id, message.from_user.id)
        return True
    return False


def _handle_visits_input(message: types.Message, state: dict, text: str) -> bool:
    if text == "🔙 Back":
        with _state_lock:
            user_states[message.from_user.id] = {"step": "AWAITING_MODE", "url": state.get("url", "")}
        bot.send_message(message.chat.id, "⚙️ <b>SELECT CONNECTION MODE</b>", reply_markup=mode_keyboard())
        return True
    count = VISIT_OPTIONS.get(text)
    if count is None:
        return False
    max_visits = get_setting_int("max_visits", MAX_VISITS_PER_JOB)
    if count > max_visits:
        bot.send_message(message.chat.id, f"⚠️ Max visits allowed is <code>{max_visits}</code>.")
        return True

    url = state.get("url", "")
    mode = state.get("mode", "NORMAL")
    user_id = message.from_user.id

    with _state_lock:
        if user_id in active_jobs and not active_jobs[user_id].finished.is_set():
            bot.send_message(message.chat.id, "⚠️ <b>A job is already running!</b>", reply_markup=cancel_keyboard())
            return True
        user_states[user_id] = {}

    job_id = create_job(user_id, message.from_user.username, url, mode, count)
    start_msg = bot.send_message(
        message.chat.id,
        render_progress({
            "job_id": job_id, "stage": "Initialising…", "completed": 0, "total": count,
            "successful": 0, "failed": 0, "unique": 0, "duplicates": 0, "proxy": "—",
            "proxy_status": "—", "exit_ip": "—", "latency": 0.0, "retries": 0,
            "elapsed": 0, "speed": 0.0, "eta": 0, "mode": mode, "finished": False,
            "fatal_reason": None,
        }),
        reply_markup=cancel_keyboard(),
    )
    job_state = JobState(job_id, user_id, message.chat.id, start_msg.message_id, url, mode, count)
    with _state_lock:
        active_jobs[user_id] = job_state
    threading.Thread(target=extraction_worker, args=(job_state,), daemon=True).start()
    return True


# ── Admin inbound-state processing ───────────────────────────────

INPUT_PROMPTS = {
    "ADMIN_ADD_PROXY": "➕ Send one proxy to add (e.g. <code>http://user:pass@ip:port</code>):",
    "ADMIN_BULK_ADD": "📦 Paste proxies, one per line:",
    "ADMIN_SETTING_INPUT": None,
    "ADMIN_ADD_ADMIN": "👮 Send the numeric Telegram user ID to promote to admin:",
    "ADMIN_REMOVE_ADMIN": "👮 Send the numeric Telegram user ID to demote:",
    "ADMIN_BROADCAST": "📢 Type the message to broadcast:",
    "ADMIN_SEARCH": "🔎 Send a Telegram ID, username, or name to search:",
    "ADMIN_NUMBER_SEARCH": "📱 Send a number (digits) to look up:",
}


def _handle_admin_state(message: types.Message, state: dict, text: str) -> bool:
    step = state.get("step")
    user_id = message.from_user.id
    chat_id = message.chat.id

    if step == "ADMIN_ADD_PROXY":
        with _state_lock:
            user_states[user_id] = {}
        parsed, err = parse_proxy(text)
        if not parsed:
            bot.send_message(chat_id, f"❌ <b>Invalid proxy:</b> {html.escape(err)}",
                             reply_markup=proxy_center_keyboard())
            return True
        added = db_add_proxy(text, user_id, parsed)
        proxy_pool.reload()
        bot.send_message(chat_id, "✅ Proxy added." if added else "⚠️ Proxy already exists.",
                         reply_markup=proxy_center_keyboard())
        audit(user_id, "add_proxy", target=sanitize_display(text))
        return True

    if step == "ADMIN_BULK_ADD":
        with _state_lock:
            user_states[user_id] = {}
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        added = dup = invalid = 0
        for ln in lines:
            parsed, _ = parse_proxy(ln)
            if not parsed:
                invalid += 1
                continue
            if db_add_proxy(ln, user_id, parsed):
                added += 1
            else:
                dup += 1
        proxy_pool.reload()
        bot.send_message(chat_id,
                         f"📦 <b>Bulk import complete</b>\n"
                         f"➕ Added: <code>{added}</code>\n"
                         f"🔁 Duplicate: <code>{dup}</code>\n"
                         f"⚠️ Invalid: <code>{invalid}</code>\n"
                         f"📡 Total proxies: <code>{proxy_pool.count()}</code>",
                         reply_markup=proxy_center_keyboard())
        audit(user_id, "bulk_add_proxy", details=f"added={added} invalid={invalid}")
        return True

    if step == "ADMIN_SETTING_INPUT":
        key = state.get("key")
        with _state_lock:
            user_states[user_id] = {}
        value = text.strip()
        if key == "max_visits" and not value.isdigit():
            bot.send_message(chat_id, "⚠️ Please send a number.", reply_markup=admin_back_keyboard())
            return True
        set_setting(key, value)
        bot.send_message(chat_id, f"✅ <b>{html.escape(key)}</b> updated.", reply_markup=admin_keyboard())
        audit(user_id, "set_setting", target=key, details=value[:80])
        return True

    if step == "ADMIN_ADD_ADMIN":
        with _state_lock:
            user_states[user_id] = {}
        if not is_owner(user_id):
            bot.send_message(chat_id, "❌ Only the OWNER can manage admins.", reply_markup=admin_keyboard())
            return True
        if text.isdigit():
            add_admin(int(text), "ADMIN", user_id)
            bot.send_message(chat_id, f"✅ Added admin <code>{text}</code>.", reply_markup=admin_keyboard())
            audit(user_id, "add_admin", target=text)
        else:
            bot.send_message(chat_id, "⚠️ Send a numeric ID.", reply_markup=admin_back_keyboard())
        return True

    if step == "ADMIN_REMOVE_ADMIN":
        with _state_lock:
            user_states[user_id] = {}
        if not is_owner(user_id):
            bot.send_message(chat_id, "❌ Only the OWNER can manage admins.", reply_markup=admin_keyboard())
            return True
        if text.isdigit():
            remove_admin(int(text))
            bot.send_message(chat_id, f"✅ Removed admin <code>{text}</code>.", reply_markup=admin_keyboard())
            audit(user_id, "remove_admin", target=text)
        else:
            bot.send_message(chat_id, "⚠️ Send a numeric ID.", reply_markup=admin_back_keyboard())
        return True

    if step == "ADMIN_BROADCAST":
        with _state_lock:
            user_states[user_id] = {}
        threading.Thread(target=_run_broadcast,
                         args=(chat_id, user_id, text, "all"), daemon=True).start()
        return True

    if step == "ADMIN_SEARCH":
        with _state_lock:
            user_states[user_id] = {}
        results = search_users(text)
        if not results:
            bot.send_message(chat_id, "🔎 No users matched.", reply_markup=admin_keyboard())
            return True
        markup = types.InlineKeyboardMarkup(row_width=3)
        for r in results:
            markup.add(types.InlineKeyboardButton(f"👤 {r['user_id']}", callback_data=f"user:{r['user_id']}"))
        bot.send_message(chat_id, f"🔎 <b>{len(results)} match(es):</b>", reply_markup=markup)
        return True

    if step == "ADMIN_NUMBER_SEARCH":
        with _state_lock:
            user_states[user_id] = {}
        info = search_number(re.sub(r"\D", "", text))
        if not info:
            bot.send_message(chat_id, "📱 No record for that number.", reply_markup=admin_keyboard())
            return True
        bot.send_message(chat_id,
                         f"📱 <b>NUMBER {format_number(info['number'])}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                         f"Times Found: <code>{info['count']}</code>\n"
                         f"Users: <code>{len(info['users'])}</code>\n"
                         f"Jobs: <code>{len(info['jobs'])}</code>\n"
                         f"Methods: {html.escape(', '.join(info['methods']))}\n"
                         f"First Seen: <code>{str(info['first_seen'])[:16]}</code>\n"
                         f"Last Seen: <code>{str(info['last_seen'])[:16]}</code>",
                         reply_markup=admin_keyboard())
        return True

    # ── Admin menu buttons (only when no text state pending) ──
    if step:
        return False

    if not is_admin(user_id):
        return False

    if text == "📊 Dashboard":
        bot.send_message(chat_id, _admin_dashboard_text(), reply_markup=admin_keyboard())
        return True
    if text == "👥 Users":
        _show_users(chat_id, 0)
        return True
    if text == "📱 Extraction Logs":
        _show_extraction_logs(chat_id, days=None, page=0)
        return True
    if text == "🔎 Search":
        with _state_lock:
            user_states[user_id] = {"step": "ADMIN_SEARCH"}
        bot.send_message(chat_id, INPUT_PROMPTS["ADMIN_SEARCH"], reply_markup=admin_back_keyboard())
        return True
    if text == "🌐 Proxy Center":
        bot.send_message(chat_id, _proxy_dashboard_text(), reply_markup=proxy_center_keyboard())
        return True
    if text == "📢 Broadcast":
        with _state_lock:
            user_states[user_id] = {"step": "ADMIN_BROADCAST"}
        bot.send_message(chat_id, INPUT_PROMPTS["ADMIN_BROADCAST"], reply_markup=cancel_keyboard())
        return True
    if text == "📡 Channel":
        ch = get_setting("channel_username", "") or "-"
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("🧪 Test Channel", callback_data=f"testch:{ch}"),
                   types.InlineKeyboardButton("📡 Set Channel Username", callback_data="setch"))
        bot.send_message(chat_id,
                         f"📡 <b>CHANNEL SETTINGS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                         f"Channel: <code>{html.escape(ch)}</code>\n"
                         f"Auto-Post: <b>{'🔴 ON' if get_setting_bool('channel_logging_enabled') else '🟢 OFF'}</b>\n"
                         f"Publish Numbers: <b>{'🔴 ON' if get_setting_bool('channel_post_numbers') else '🟢 OFF'}</b>\n"
                         f"Attach TXT: <b>{'🔴 ON' if get_setting_bool('channel_attach_txt') else '🟢 OFF'}</b>",
                         reply_markup=markup)
        return True
    if text == "⚙️ Settings":
        bot.send_message(chat_id, _settings_text(), reply_markup=_settings_markup())
        return True
    if text == "👮 Admins":
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(types.InlineKeyboardButton("➕ Add Admin", callback_data="adminadd"),
                   types.InlineKeyboardButton("➖ Remove Admin", callback_data="adminrm"))
        if not is_owner(user_id):
            markup = None
        bot.send_message(chat_id, _admins_text(), reply_markup=markup)
        return True
    if text == "📤 Export":
        _send_export(chat_id, user_id)
        return True
    if text == "🩺 Diagnostics":
        threading.Thread(target=_run_diagnostics, args=(chat_id,), daemon=True).start()
        return True
    if text == "🔙 Admin Panel":
        bot.send_message(chat_id, "🔐 <b>ADMIN CONTROL CENTER</b>", reply_markup=admin_keyboard())
        return True

    # Proxy center sub-buttons
    if text == "📊 Proxy Dashboard":
        bot.send_message(chat_id, _proxy_dashboard_text(), reply_markup=proxy_center_keyboard())
        return True
    if text == "➕ Add Proxies":
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(types.InlineKeyboardButton("➕ Single", callback_data="padd1"),
                   types.InlineKeyboardButton("📦 Bulk", callback_data="paddbulk"))
        bot.send_message(chat_id, "➕ <b>Add Proxies</b> — choose a mode:", reply_markup=markup)
        return True
    if text == "📋 Proxy List":
        _list_proxies(chat_id, 0)
        return True
    if text == "🧪 Health Check":
        threading.Thread(target=_run_health_check, args=(chat_id, None, user_id), daemon=True).start()
        return True
    if text == "🔄 Retest Unhealthy":
        eps = [r["endpoint"] for r in db_get_all_proxies()
               if r.get("health_status") in ("UNHEALTHY", "UNTESTED", None)
               or (r.get("consecutive_failures") or 0) > 0]
        if not eps:
            bot.send_message(chat_id, "✅ No unhealthy/untested proxies.", reply_markup=proxy_center_keyboard())
            return True
        threading.Thread(target=_run_health_check, args=(chat_id, eps, user_id), daemon=True).start()
        return True
    if text == "🗑️ Cleanup":
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(types.InlineKeyboardButton("🗑️ Clear Unhealthy", callback_data="pclear_dead"),
                   types.InlineKeyboardButton("🗑️ Clear All (DB)", callback_data="pclear_all"))
        bot.send_message(chat_id, "🗑️ <b>Cleanup</b> — choose an action:", reply_markup=markup)
        return True

    return False


@bot.callback_query_handler(func=lambda c: c.data in ("adminadd", "adminrm", "padd1", "paddbulk",
                                                      "pclear_dead", "pclear_all"))
def cb_misc_admin(call: types.CallbackQuery):
    if not _guard_admin(call):
        return
    data = call.data
    bot.answer_callback_query(call.id)
    if data == "adminadd":
        if not is_owner(call.from_user.id):
            bot.send_message(call.message.chat.id, "❌ Owner only.")
            return
        with _state_lock:
            user_states[call.from_user.id] = {"step": "ADMIN_ADD_ADMIN"}
        bot.send_message(call.message.chat.id, INPUT_PROMPTS["ADMIN_ADD_ADMIN"],
                         reply_markup=admin_back_keyboard())
    elif data == "adminrm":
        if not is_owner(call.from_user.id):
            bot.send_message(call.message.chat.id, "❌ Owner only.")
            return
        with _state_lock:
            user_states[call.from_user.id] = {"step": "ADMIN_REMOVE_ADMIN"}
        bot.send_message(call.message.chat.id, INPUT_PROMPTS["ADMIN_REMOVE_ADMIN"],
                         reply_markup=admin_back_keyboard())
    elif data == "padd1":
        with _state_lock:
            user_states[call.from_user.id] = {"step": "ADMIN_ADD_PROXY"}
        bot.send_message(call.message.chat.id, INPUT_PROMPTS["ADMIN_ADD_PROXY"],
                         reply_markup=admin_back_keyboard())
    elif data == "paddbulk":
        with _state_lock:
            user_states[call.from_user.id] = {"step": "ADMIN_BULK_ADD"}
        bot.send_message(call.message.chat.id, INPUT_PROMPTS["ADMIN_BULK_ADD"],
                         reply_markup=admin_back_keyboard())
    elif data == "pclear_dead":
        n = db_clear_dead_proxies()
        proxy_pool.reload()
        bot.send_message(call.message.chat.id, f"🗑️ Cleared <code>{n}</code> unhealthy proxies.",
                         reply_markup=proxy_center_keyboard())
        audit(call.from_user.id, "clear_dead_proxies", details=str(n))
    elif data == "pclear_all":
        n = db_clear_all_proxies()
        proxy_pool.reload()
        bot.send_message(call.message.chat.id,
                         f"🗑️ Deleted <code>{n}</code> DB proxies. Env proxies remain.",
                         reply_markup=proxy_center_keyboard())
        audit(call.from_user.id, "clear_all_proxies", details=str(n))


def _send_export(chat_id: int, admin_id: int) -> None:
    import csv
    try:
        users = _query("SELECT * FROM users")
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["user_id", "username", "first_name", "status", "is_blocked",
                    "total_extractions", "total_numbers_found", "joined_at", "last_active"])
        for u in users:
            w.writerow([u.get("user_id"), u.get("username"), u.get("first_name"),
                        u.get("status"), u.get("is_blocked"), u.get("total_extractions"),
                        u.get("total_numbers_found"), u.get("joined_at"), u.get("last_active")])
        s = io.BytesIO(buf.getvalue().encode("utf-8"))
        s.name = "users_export.csv"
        bot.send_document(chat_id, s, caption="📤 <b>Users export</b>")

        jobs = get_jobs_filtered(limit=5000)
        buf2 = io.StringIO()
        w2 = csv.writer(buf2)
        w2.writerow(["job_id", "user_id", "username", "source_url", "mode",
                     "requested_visits", "successful_visits", "failed_visits",
                     "unique_numbers", "duplicate_numbers", "status", "started_at"])
        for j in jobs:
            w2.writerow([j.get("job_id"), j.get("user_id"), j.get("username"),
                         j.get("source_url"), j.get("mode"), j.get("requested_visits"),
                         j.get("successful_visits"), j.get("failed_visits"),
                         j.get("unique_numbers"), j.get("duplicate_numbers"),
                         j.get("status"), j.get("started_at")])
        s2 = io.BytesIO(buf2.getvalue().encode("utf-8"))
        s2.name = "jobs_export.csv"
        bot.send_document(chat_id, s2, caption="📤 <b>Jobs export</b>",
                          reply_markup=admin_keyboard())
        audit(admin_id, "export_data")
    except Exception as exc:
        bot.send_message(chat_id, f"⚠️ Export failed: {html.escape(str(exc)[:80])}",
                         reply_markup=admin_keyboard())


def _run_diagnostics(chat_id: int) -> None:
    lines = ["🩺 <b>SYSTEM DIAGNOSTICS</b>", "━━━━━━━━━━━━━━━━━━━━"]

    def check(label: str, ok: bool, detail: str = "") -> None:
        lines.append(f"{'✅' if ok else '❌'} {label}" + (f" — {html.escape(detail)}" if detail else ""))

    # Telegram
    try:
        me = bot.get_me()
        check("Telegram API", True, me.username or "")
    except Exception as exc:
        check("Telegram API", False, str(exc)[:60])
    # Database
    try:
        _query_one("SELECT 1 AS x")
        check("Database", True)
    except Exception as exc:
        check("Database", False, str(exc)[:60])
    # Direct HTTP
    try:
        r = requests.get("https://httpbin.org/get", timeout=(5, 8))
        check("Direct HTTP", r.status_code < 400, f"HTTP {r.status_code}")
    except Exception as exc:
        check("Direct HTTP", False, str(exc)[:60])
    # Proxy parser
    p, _ = parse_proxy("http://1.2.3.4:8080")
    check("Proxy parser", p is not None)
    # SOCKS5
    check("SOCKS5 support", _SOCKS5_AVAILABLE)
    # IP verification
    try:
        r = requests.get("https://api.ipify.org?format=json", timeout=(5, 8))
        check("IP verification", "ip" in r.text)
    except Exception as exc:
        check("IP verification", False, str(exc)[:60])
    # Proxy pool
    check("Proxy pool", proxy_pool.count() > 0, f"{proxy_pool.count()} endpoints")
    # Channel permission
    ch = get_setting("channel_username", "")
    if ch:
        ok, msg = test_channel(ch)
        check("Channel posting", ok, msg[:60])
    else:
        check("Channel posting", False, "not configured")
    # Extraction engine
    try:
        found = extract_from_html('href="https://wa.me/919876543210"')
        check("Extraction engine", "919876543210" in found)
    except Exception as exc:
        check("Extraction engine", False, str(exc)[:60])

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    bot.send_message(chat_id, "\n".join(lines), reply_markup=admin_keyboard())


# =========================================================
# Startup
# =========================================================
def _startup_selfcheck() -> None:
    logger.info("=" * 60)
    logger.info("%s — Starting", BOT_NAME)
    logger.info("Admins: %s", ADMIN_IDS)
    logger.info("DB: %s", DB_FILE)
    logger.info("Proxy endpoints: %d", proxy_pool.count())
    logger.info("SOCKS5 support: %s", "yes" if _SOCKS5_AVAILABLE else "NO")
    logger.info("Max concurrency: %d", MAX_CONCURRENCY)
    logger.info("=" * 60)


def _retest_scheduler() -> None:
    """Background: periodically re-verify unhealthy/untested proxies."""
    while True:
        time.sleep(max(60.0, RETEST_INTERVAL))
        try:
            if not get_setting_bool("auto_proxy_retest", True):
                continue
            rows = db_get_all_proxies()
            due = [r["endpoint"] for r in rows
                   if r.get("health_status") in ("UNHEALTHY", "UNTESTED", None)]
            for ep in due[:20]:
                parsed, _ = parse_proxy(ep)
                if not parsed:
                    continue
                health = test_proxy(parsed, quick=True)
                if health.status in ("WORKING", "CONNECTED", "SLOW"):
                    proxy_pool.mark_success(ep, health.latency_ms or 0, health.exit_ip, health.status)
                else:
                    proxy_pool.mark_failure(ep, health.error or "auto-retest", classify_to_db_status(health.status))
            _log_event("AUTO_RETEST", checked=len(due[:20]))
        except Exception as exc:
            logger.debug("retest scheduler error: %s", exc)


if __name__ == "__main__":
    init_db()
    _startup_selfcheck()

    try:
        bot.remove_webhook()
        time.sleep(0.5)
    except Exception as exc:
        logger.warning("Webhook clear notice: %s", exc)

    threading.Thread(target=_retest_scheduler, daemon=True).start()

    logger.info("Polling started.")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as exc:
            logger.warning("Polling interrupted: %s", exc)
            time.sleep(4)
            logger.info("Reconnecting…")

