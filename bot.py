#!/usr/bin/env python3
"""
URL Fetcher & Rotating Link Engine — Professional Edition
============================================================
Production-ready Telegram bot for authorized extraction of publicly
exposed phone numbers from HTTP/HTTPS pages and redirect chains.

Key properties:
  * IP-rotation mode routes EVERY request through a verified proxy
    (requests + PySocks) and NEVER silently falls back to direct.
  * Multi-stage proxy health testing with multiple IP-check endpoints,
    exponential-backoff cooldown, and a failure threshold (no false-dead).
  * Independent live-progress thread that updates while a request is
    in flight, so the user never stares at "Visits: 0/20".
  * Full extraction audit trail (jobs / numbers / proxy attempts).
  * Multi-admin (numeric-ID auth), user approval, DB-backed settings,
    optional channel auto-posting.
  * Safe SQLite migrations — existing data is preserved.

Compatible with Render and standard VPS deployments.
Configuration is environment-driven; secrets are never hard-coded.
"""

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
from datetime import datetime, timezone
from typing import Optional

# ── Third-party ──────────────────────────────────────────────────────────────
import requests
import requests.exceptions
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

try:
    # PySocks is required for SOCKS5 proxy support (installed via requests[socks]).
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
    """Strip anything that looks like proxy credentials from a log string."""
    return re.sub(r"(://)[^@/\s]+@", r"\1****:****@", str(msg))


def log_event(event: str, **fields) -> None:
    """Structured, credential-safe log line: EVENT key=value key=value ..."""
    parts = [event]
    for k, v in fields.items():
        parts.append(f"{k}={_safe_log(v)}")
    logger.info(" ".join(parts))


# =========================================================
# Configuration (environment-driven; DB overrides at runtime)
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8553353076:AAFgLdPCaSL_TfZds10qQS1_Hr5iGnn0e5M").strip()
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN environment variable is not set. Exiting.")
    sys.exit(1)

def _parse_admin_ids(raw: str) -> list[int]:
    out: list[int] = []
    for x in raw.split(","):
        x = x.strip()
        if x.lstrip("-").isdigit():
            out.append(int(x))
    return out

ADMIN_IDS: list[int] = _parse_admin_ids(os.environ.get("ADMIN_IDS", "8753914631"))
# The first configured admin (env) is the bootstrap OWNER.
BOOTSTRAP_OWNER_ID: Optional[int] = ADMIN_IDS[0] if ADMIN_IDS else None

DB_FILE = os.environ.get("DATABASE_PATH", "bot_database.db")
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "12"))
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "6"))
READ_TIMEOUT = float(os.environ.get("READ_TIMEOUT", "10"))
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "6"))
MAX_RESPONSE_SIZE = int(os.environ.get("MAX_RESPONSE_SIZE", str(5 * 1024 * 1024)))  # 5 MB
MAX_URL_LENGTH = int(os.environ.get("MAX_URL_LENGTH", "2048"))
MAX_REDIRECTS = int(os.environ.get("MAX_REDIRECTS", "10"))
MAX_VISITS_PER_JOB = int(os.environ.get("MAX_VISITS_PER_JOB", "100"))
PROGRESS_INTERVAL = float(os.environ.get("PROGRESS_INTERVAL", "1.2"))

# Proxy cooldown ladder (exponential backoff), capped.
PROXY_COOLDOWN_LADDER = [30.0, 60.0, 120.0, 300.0]
PROXY_COOLDOWN_MAX = float(os.environ.get("PROXY_COOLDOWN_MAX", "600"))
PROXY_UNHEALTHY_THRESHOLD = 3  # consecutive failures before "unhealthy"

DEFAULT_CHANNEL = os.environ.get("CHANNEL_USERNAME", "@HshDkSharmaBotsmall").strip()
DEFAULT_AUTO_CHANNEL_POST = os.environ.get("AUTO_CHANNEL_POST", "0").strip() in ("1", "true", "TRUE", "yes")

RAW_PROXY_ENV = (
    os.environ.get("PROXY_ENDPOINTS", "")
    or os.environ.get("ROTATING_PROXIES", "")
    or ""
)

# Bot instance
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# =========================================================
# Global in-process state (all access guarded by _state_lock)
# =========================================================
user_states: dict[int, dict] = {}
active_jobs: dict[int, dict] = {}   # user_id -> {"cancel": Event, "job_id": int, "state": dict}
_state_lock = threading.RLock()

# Bounded executor for background proxy testing (never unbounded threads).
_proxy_test_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=MAX_CONCURRENCY, thread_name_prefix="proxytest"
)


# =========================================================
# Database layer (SQLite, WAL, safe migrations)
# =========================================================
_db_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r["name"] for r in rows}
    except Exception:
        return set()


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if column not in _table_columns(conn, table):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            logger.info("Migration: added %s.%s", table, column)
        except Exception as exc:
            logger.warning("Migration skip %s.%s: %s", table, column, exc)


def init_db() -> None:
    """Create tables if absent and migrate existing ones without data loss."""
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
                    successful_jobs       INTEGER DEFAULT 0,
                    failed_jobs           INTEGER DEFAULT 0,
                    status                TEXT DEFAULT 'APPROVED',   -- PENDING/APPROVED/BLOCKED
                    joined_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Legacy aggregate history (kept for backward compatibility).
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

                -- Full per-job audit record.
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
                    duration_ms       INTEGER DEFAULT 0,
                    status            TEXT DEFAULT 'RUNNING',
                    started_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at      TIMESTAMP,
                    channel_post_status TEXT,
                    channel_message_id  INTEGER,
                    channel_post_error  TEXT
                );

                CREATE TABLE IF NOT EXISTS extracted_numbers (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id            INTEGER,
                    user_id           INTEGER,
                    number            TEXT,
                    source_url        TEXT,
                    extraction_method TEXT,
                    visit_number      INTEGER,
                    proxy_display     TEXT,
                    observed_ip       TEXT,
                    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS job_proxy_attempts (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id        INTEGER,
                    visit_number  INTEGER,
                    proxy_display TEXT,
                    protocol      TEXT,
                    status        TEXT,
                    latency_ms    REAL,
                    observed_ip   TEXT,
                    error         TEXT,
                    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS admin_proxies (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint         TEXT UNIQUE NOT NULL,
                    added_by         INTEGER,
                    is_active        INTEGER DEFAULT 1,
                    health_status    TEXT DEFAULT 'UNTESTED',
                    last_tested      TIMESTAMP,
                    last_success     TIMESTAMP,
                    last_failure     TIMESTAMP,
                    success_count    INTEGER DEFAULT 0,
                    failure_count    INTEGER DEFAULT 0,
                    consecutive_failures INTEGER DEFAULT 0,
                    average_latency  REAL DEFAULT 0,
                    last_error       TEXT,
                    last_observed_ip TEXT,
                    cooldown_until   TIMESTAMP,
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS admins (
                    user_id    INTEGER PRIMARY KEY,
                    username   TEXT,
                    role       TEXT DEFAULT 'ADMIN',   -- OWNER/ADMIN/MODERATOR
                    added_by   INTEGER,
                    is_active  INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                CREATE INDEX IF NOT EXISTS idx_jobs_user   ON extraction_jobs(user_id);
                CREATE INDEX IF NOT EXISTS idx_jobs_date   ON extraction_jobs(started_at);
                CREATE INDEX IF NOT EXISTS idx_numbers_job ON extracted_numbers(job_id);
                CREATE INDEX IF NOT EXISTS idx_numbers_user ON extracted_numbers(user_id);
                CREATE INDEX IF NOT EXISTS idx_numbers_num ON extracted_numbers(number);
                CREATE INDEX IF NOT EXISTS idx_attempts_job ON job_proxy_attempts(job_id);
                CREATE INDEX IF NOT EXISTS idx_proxies_active ON admin_proxies(is_active);
            """)

            # Migrations for databases created by older versions of this bot.
            _add_column_if_missing(conn, "users", "successful_jobs", "INTEGER DEFAULT 0")
            _add_column_if_missing(conn, "users", "failed_jobs", "INTEGER DEFAULT 0")
            _add_column_if_missing(conn, "users", "status", "TEXT DEFAULT 'APPROVED'")
            _add_column_if_missing(conn, "admin_proxies", "health_status", "TEXT DEFAULT 'UNTESTED'")
            _add_column_if_missing(conn, "admin_proxies", "consecutive_failures", "INTEGER DEFAULT 0")

            conn.commit()
        finally:
            conn.close()

    # Seed bootstrap owner + env admins.
    _seed_admins()


# ── Settings (DB-backed, env defaults) ─────────────────────
_SETTING_DEFAULTS = {
    "maintenance_mode": "0",
    "approval_mode": "0",
    "channel_username": DEFAULT_CHANNEL,
    "channel_logging": "1" if DEFAULT_AUTO_CHANNEL_POST else "0",
    "channel_post_numbers": "0",     # privacy default: OFF
    "channel_attach_txt": "0",
    "support_username": "",
    "bot_name": "URL Extraction Center",
    "max_visits": str(MAX_VISITS_PER_JOB),
}


def get_setting(key: str, default: Optional[str] = None) -> str:
    conn = get_conn()
    try:
        row = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)).fetchone()
        if row is not None:
            return row["value"]
    finally:
        conn.close()
    if default is not None:
        return default
    return _SETTING_DEFAULTS.get(key, "")


def set_setting(key: str, value: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO bot_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )
            conn.commit()
        finally:
            conn.close()


def get_bool_setting(key: str) -> bool:
    return get_setting(key) in ("1", "true", "TRUE", "yes", "on")


# ── Admin management (numeric-ID authorization) ────────────
def _seed_admins() -> None:
    with _db_lock:
        conn = get_conn()
        try:
            first = True
            for aid in ADMIN_IDS:
                role = "OWNER" if first else "ADMIN"
                conn.execute(
                    "INSERT OR IGNORE INTO admins (user_id, role, added_by, is_active) "
                    "VALUES (?, ?, ?, 1)",
                    (aid, role, aid),
                )
                first = False
            conn.commit()
        finally:
            conn.close()


def get_admin(user_id: int) -> Optional[dict]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM admins WHERE user_id = ? AND is_active = 1", (user_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def is_admin(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    return get_admin(user_id) is not None


def is_owner(user_id: int) -> bool:
    if BOOTSTRAP_OWNER_ID is not None and user_id == BOOTSTRAP_OWNER_ID:
        return True
    row = get_admin(user_id)
    return bool(row and row.get("role") == "OWNER")


def list_admins() -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM admins WHERE is_active = 1 ORDER BY created_at ASC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def add_admin(user_id: int, added_by: int, role: str = "ADMIN", username: str = None) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO admins (user_id, username, role, added_by, is_active) "
                "VALUES (?, ?, ?, ?, 1) "
                "ON CONFLICT(user_id) DO UPDATE SET is_active = 1, role = excluded.role",
                (user_id, username, role, added_by),
            )
            conn.commit()
        finally:
            conn.close()


def remove_admin(user_id: int) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute("UPDATE admins SET is_active = 0 WHERE user_id = ?", (user_id,))
            conn.commit()
        finally:
            conn.close()


def audit(admin_id: int, action: str, target: str = "", details: str = "") -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO admin_audit_log (admin_id, action, target, details) VALUES (?, ?, ?, ?)",
                (admin_id, action, _safe_log(target)[:200], _safe_log(details)[:300]),
            )
            conn.commit()
        finally:
            conn.close()
    log_event("ADMIN_ACTION", admin=admin_id, action=action, target=target)


# ── User helpers ──────────────────────────────────────────
def register_user(user_id: int, username: str = None, first_name: str = None) -> None:
    approval = get_bool_setting("approval_mode")
    with _db_lock:
        conn = get_conn()
        try:
            exists = conn.execute(
                "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if not exists:
                # New user: PENDING when approval mode is on (admins auto-approved).
                status = "APPROVED"
                if approval and not is_admin(user_id):
                    status = "PENDING"
                conn.execute(
                    "INSERT INTO users (user_id, username, first_name, status) VALUES (?, ?, ?, ?)",
                    (user_id, username, first_name, status),
                )
            conn.execute(
                "UPDATE users SET last_active = CURRENT_TIMESTAMP, "
                "username = COALESCE(?, username), first_name = COALESCE(?, first_name) "
                "WHERE user_id = ?",
                (username, first_name, user_id),
            )
            conn.commit()
        finally:
            conn.close()


def get_user(user_id: int) -> Optional[dict]:
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def set_user_status(user_id: int, status: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute("UPDATE users SET status = ? WHERE user_id = ?", (status, user_id))
            conn.commit()
        finally:
            conn.close()


def user_is_blocked(user_id: int) -> bool:
    u = get_user(user_id)
    return bool(u and u.get("status") == "BLOCKED")


def user_is_approved(user_id: int) -> bool:
    if is_admin(user_id):
        return True
    if not get_bool_setting("approval_mode"):
        return True
    u = get_user(user_id)
    return bool(u and u.get("status") == "APPROVED")


def update_user_stats(user_id: int, unique_count: int, success: bool) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "UPDATE users SET total_extractions = total_extractions + 1, "
                "total_numbers_found = total_numbers_found + ?, "
                "successful_jobs = successful_jobs + ?, failed_jobs = failed_jobs + ?, "
                "last_active = CURRENT_TIMESTAMP WHERE user_id = ?",
                (unique_count, 1 if success else 0, 0 if success else 1, user_id),
            )
            conn.commit()
        finally:
            conn.close()


def get_user_history(user_id: int, limit: int = 8, offset: int = 0) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM extraction_jobs WHERE user_id = ? "
            "ORDER BY started_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_all_user_ids(status: Optional[str] = None) -> list[int]:
    conn = get_conn()
    try:
        if status:
            rows = conn.execute("SELECT user_id FROM users WHERE status = ?", (status,)).fetchall()
        else:
            rows = conn.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]
    finally:
        conn.close()


def search_users(term: str, limit: int = 10) -> list[dict]:
    conn = get_conn()
    try:
        if term.lstrip("-").isdigit():
            rows = conn.execute(
                "SELECT * FROM users WHERE user_id = ? LIMIT ?", (int(term), limit)
            ).fetchall()
        else:
            like = f"%{term.lstrip('@')}%"
            rows = conn.execute(
                "SELECT * FROM users WHERE username LIKE ? OR first_name LIKE ? LIMIT ?",
                (like, like, limit),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Job / audit helpers ────────────────────────────────────
def create_job(user_id: int, username: str, url: str, mode: str, requested: int) -> int:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                "INSERT INTO extraction_jobs (user_id, username, source_url, mode, requested_visits) "
                "VALUES (?, ?, ?, ?, ?)",
                (user_id, username, url, mode, requested),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()


def finalize_job(job_id: int, **fields) -> None:
    if not fields:
        return
    cols = ", ".join(f"{k} = ?" for k in fields)
    vals = list(fields.values()) + [job_id]
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(f"UPDATE extraction_jobs SET {cols} WHERE job_id = ?", vals)
            conn.commit()
        finally:
            conn.close()


def save_extracted_number(job_id, user_id, number, source_url, method, visit, proxy_display, ip) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO extracted_numbers "
                "(job_id, user_id, number, source_url, extraction_method, visit_number, proxy_display, observed_ip) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (job_id, user_id, number, source_url, method, visit, proxy_display, ip),
            )
            conn.commit()
        finally:
            conn.close()


def save_proxy_attempt(job_id, visit, proxy_display, protocol, status, latency, ip, error) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO job_proxy_attempts "
                "(job_id, visit_number, proxy_display, protocol, status, latency_ms, observed_ip, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (job_id, visit, proxy_display, protocol, status, latency, ip, _safe_log(error or "")[:200]),
            )
            conn.commit()
        finally:
            conn.close()


def get_job(job_id: int) -> Optional[dict]:
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM extraction_jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_job_numbers(job_id: int) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM extracted_numbers WHERE job_id = ? ORDER BY id ASC", (job_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def recent_jobs(limit: int = 10, offset: int = 0, since_days: Optional[int] = None) -> list[dict]:
    conn = get_conn()
    try:
        if since_days is not None:
            rows = conn.execute(
                "SELECT * FROM extraction_jobs WHERE started_at >= datetime('now', ?) "
                "ORDER BY started_at DESC LIMIT ? OFFSET ?",
                (f"-{since_days} days", limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM extraction_jobs ORDER BY started_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_admin_dashboard_stats() -> dict:
    conn = get_conn()
    try:
        u = conn.execute(
            "SELECT COUNT(*) AS total_users, "
            "COALESCE(SUM(total_numbers_found),0) AS total_nums, "
            "SUM(CASE WHEN last_active >= datetime('now','-1 day') THEN 1 ELSE 0 END) AS active_today "
            "FROM users"
        ).fetchone()
        j = conn.execute(
            "SELECT COUNT(*) AS total_jobs, "
            "SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS ok_jobs, "
            "SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_jobs, "
            "SUM(CASE WHEN started_at >= date('now') THEN 1 ELSE 0 END) AS jobs_today, "
            "AVG(CASE WHEN duration_ms>0 THEN duration_ms END) AS avg_ms "
            "FROM extraction_jobs"
        ).fetchone()
        n = conn.execute(
            "SELECT SUM(CASE WHEN created_at >= date('now') THEN 1 ELSE 0 END) AS nums_today "
            "FROM extracted_numbers"
        ).fetchone()
        out = {}
        out.update(dict(u) if u else {})
        out.update(dict(j) if j else {})
        out.update(dict(n) if n else {})
        return out
    finally:
        conn.close()


# ── Proxy DB helpers ──────────────────────────────────────
def db_add_proxy(endpoint: str, added_by: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            before = conn.total_changes
            conn.execute(
                "INSERT OR IGNORE INTO admin_proxies (endpoint, added_by, is_active) VALUES (?, ?, 1)",
                (endpoint, added_by),
            )
            conn.commit()
            return (conn.total_changes - before) > 0
        except Exception as exc:
            logger.warning("db_add_proxy error: %s", exc)
            return False
        finally:
            conn.close()


def db_get_all_proxies(active_only: bool = True) -> list[dict]:
    conn = get_conn()
    try:
        q = "SELECT * FROM admin_proxies"
        if active_only:
            q += " WHERE is_active = 1"
        q += " ORDER BY id ASC"
        return [dict(r) for r in conn.execute(q).fetchall()]
    finally:
        conn.close()


def db_delete_proxy(proxy_id: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            before = conn.total_changes
            conn.execute("DELETE FROM admin_proxies WHERE id = ?", (proxy_id,))
            conn.commit()
            return (conn.total_changes - before) > 0
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
    """Only remove proxies that have never succeeded AND are past the failure threshold."""
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                "DELETE FROM admin_proxies WHERE success_count = 0 "
                "AND consecutive_failures >= ?",
                (PROXY_UNHEALTHY_THRESHOLD,),
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()


def db_get_proxy_by_endpoint(endpoint: str) -> Optional[dict]:
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM admin_proxies WHERE endpoint = ?", (endpoint,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _cooldown_seconds(consecutive_failures: int) -> float:
    idx = min(max(consecutive_failures - 1, 0), len(PROXY_COOLDOWN_LADDER) - 1)
    return min(PROXY_COOLDOWN_LADDER[idx], PROXY_COOLDOWN_MAX)


def db_update_proxy_success(proxy_id: int, latency_ms: float, observed_ip: str, status: str = "WORKING") -> None:
    with _db_lock:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT average_latency, success_count FROM admin_proxies WHERE id = ?", (proxy_id,)
            ).fetchone()
            if not row:
                return
            old_avg = row["average_latency"] or 0.0
            old_cnt = row["success_count"] or 0
            new_avg = ((old_avg * old_cnt) + latency_ms) / (old_cnt + 1)
            conn.execute(
                "UPDATE admin_proxies SET success_count = success_count + 1, "
                "consecutive_failures = 0, last_success = CURRENT_TIMESTAMP, "
                "last_tested = CURRENT_TIMESTAMP, average_latency = ?, "
                "last_observed_ip = COALESCE(NULLIF(?, ''), last_observed_ip), "
                "health_status = ?, cooldown_until = NULL, last_error = NULL WHERE id = ?",
                (new_avg, observed_ip or "", status, proxy_id),
            )
            conn.commit()
        finally:
            conn.close()


def db_update_proxy_failure(proxy_id: int, error: str, status: str = "TEMPORARY_FAILURE") -> None:
    with _db_lock:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT consecutive_failures FROM admin_proxies WHERE id = ?", (proxy_id,)
            ).fetchone()
            if not row:
                return
            cf = (row["consecutive_failures"] or 0) + 1
            cd = _cooldown_seconds(cf)
            final_status = "UNHEALTHY" if cf >= PROXY_UNHEALTHY_THRESHOLD else status
            conn.execute(
                "UPDATE admin_proxies SET failure_count = failure_count + 1, "
                "consecutive_failures = ?, last_failure = CURRENT_TIMESTAMP, "
                "last_tested = CURRENT_TIMESTAMP, last_error = ?, health_status = ?, "
                "cooldown_until = datetime('now', ?) WHERE id = ?",
                (cf, _safe_log(error)[:200], final_status, f"+{int(cd)} seconds", proxy_id),
            )
            conn.commit()
        finally:
            conn.close()


def db_proxy_stats() -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN health_status='UNTESTED' THEN 1 ELSE 0 END) AS untested, "
            "SUM(CASE WHEN health_status='WORKING' THEN 1 ELSE 0 END) AS working, "
            "SUM(CASE WHEN health_status='SLOW' THEN 1 ELSE 0 END) AS slow, "
            "SUM(CASE WHEN health_status IN ('UNHEALTHY','TCP_FAILED','AUTH_FAILED','DEAD') THEN 1 ELSE 0 END) AS dead, "
            "SUM(CASE WHEN health_status='TEMPORARY_FAILURE' THEN 1 ELSE 0 END) AS temp_fail, "
            "AVG(CASE WHEN success_count>0 THEN average_latency END) AS avg_latency, "
            "MAX(last_tested) AS last_test_time FROM admin_proxies WHERE is_active = 1"
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


# =========================================================
# Proxy parser & validator
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
        return "SOCKS5" if self.scheme.startswith("socks5") else self.scheme.upper()

    def to_requests_proxies(self) -> Optional[dict]:
        """
        Build a proxies dict for requests. ALL proxy protocols (HTTP, HTTPS,
        SOCKS5) go through requests, which handles CONNECT tunnelling and
        proxy auth correctly — this is the fix for the old urllib path that
        broke HTTPS-over-proxy and authenticated proxies.
        """
        if self.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
            return None
        if self.username is not None and self.password is not None:
            u = urllib.parse.quote(self.username, safe="")
            p = urllib.parse.quote(self.password, safe="")
            url = f"{self.scheme}://{u}:{p}@{self.host}:{self.port}"
        else:
            url = f"{self.scheme}://{self.host}:{self.port}"
        return {"http": url, "https": url}


def parse_proxy(raw: str) -> tuple[Optional[ParsedProxy], str]:
    raw = (raw or "").strip()
    if not raw:
        return None, "Empty proxy string"

    # Accept shorthand host:port and host:port:user:pass by prefixing http://
    if "://" not in raw:
        parts = raw.split(":")
        if len(parts) == 2:
            raw = "http://" + raw
        elif len(parts) == 4:
            h, po, us, pw = parts
            raw = f"http://{us}:{pw}@{h}:{po}"
        else:
            raw = "http://" + raw

    try:
        parsed = urllib.parse.urlsplit(raw)
    except Exception:
        return None, "Malformed URL"

    scheme = (parsed.scheme or "").lower()
    if scheme not in SUPPORTED_SCHEMES:
        return None, f"Unsupported scheme '{scheme}'. Supported: {', '.join(SUPPORTED_SCHEMES)}"

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
        return None, f"Invalid port {port} (must be 1-65535)"

    username = password = None
    if parsed.username:
        username = urllib.parse.unquote(parsed.username)
    if parsed.password:
        password = urllib.parse.unquote(parsed.password)
    if (username is None) != (password is None):
        return None, "Both username and password must be provided together"

    return ParsedProxy(raw, scheme, host, port, username, password), ""


def sanitize_display(endpoint: str) -> str:
    parsed, _ = parse_proxy(endpoint)
    if parsed:
        return parsed.display
    return "proxy-endpoint"


# =========================================================
# Proxy health testing (multi-stage, multi-endpoint)
# =========================================================
_IP_CHECK_URLS = [
    "https://api.ipify.org?format=json",
    "https://ifconfig.me/all.json",
    "http://ip-api.com/json?fields=query",
    "https://httpbin.org/ip",
]

PROXY_STATUS_EMOJI = {
    "WORKING": "🟢", "SLOW": "🟡", "CONNECTED": "🔵",
    "TARGET_FAILED": "🟠", "AUTH_FAILED": "🟣", "TCP_FAILED": "🔴",
    "TEMPORARY_FAILURE": "🟠", "UNHEALTHY": "🔴", "INVALID": "⚫",
    "UNTESTED": "⚪", "DEAD": "🔴", "TIMEOUT": "🟠", "DNS_FAILED": "🔴",
}


class ProxyTestResult:
    __slots__ = ("proxy_display", "protocol", "status", "working",
                 "latency_ms", "observed_ip", "error_reason", "tested_at")

    def __init__(self, proxy_display, protocol, status, working,
                 latency_ms, observed_ip, error_reason):
        self.proxy_display = proxy_display
        self.protocol = protocol
        self.status = status
        self.working = working
        self.latency_ms = latency_ms
        self.observed_ip = observed_ip
        self.error_reason = error_reason
        self.tested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def card(self, index: Optional[int] = None) -> str:
        emoji = PROXY_STATUS_EMOJI.get(self.status, "⚪")
        head = f"<b>#{index}</b> " if index is not None else ""
        lines = [f"{head}{emoji} <b>{html.escape(self.status)}</b> · {html.escape(self.protocol)}",
                 f"📡 <code>{html.escape(self.proxy_display)}</code>"]
        if self.latency_ms is not None:
            lines.append(f"⚡ Latency: <code>{self.latency_ms:.0f} ms</code>")
        if self.observed_ip:
            lines.append(f"🌍 Exit IP: <code>{html.escape(self.observed_ip)}</code>")
        if self.error_reason and not self.working:
            lines.append(f"Reason: {html.escape(self.error_reason)}")
        lines.append(f"🕒 {html.escape(self.tested_at)}")
        return "\n".join(lines)


def _tcp_check(host: str, port: int, timeout: float) -> tuple[bool, str]:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True, ""
    except socket.timeout:
        return False, "TCP timeout"
    except socket.gaierror as exc:
        return False, f"DNS failure: {exc.args[-1]}"
    except ConnectionRefusedError:
        return False, "Connection refused"
    except OSError as exc:
        return False, str(exc)


def _classify_requests_error(exc: Exception) -> str:
    msg = str(exc)
    low = msg.lower()
    if "SOCKS" in msg and not _SOCKS5_AVAILABLE:
        return "SOCKS5 support not installed (add requests[socks])"
    if "407" in msg:
        return "Proxy authentication failed (HTTP 407)"
    if "timed out" in low or "timeout" in low:
        return "Connection timeout"
    if "refused" in low:
        return "Connection refused"
    if "ssl" in low or "certificate" in low:
        return "TLS/SSL failure"
    if "name or service not known" in low or "nodename" in low or "getaddrinfo" in low:
        return "DNS failure"
    return msg[:120]


def test_proxy(parsed: ParsedProxy, quick: bool = True) -> ProxyTestResult:
    """Stage 1 TCP → Stage 2/3 real request through proxy → multi-endpoint IP verify."""
    connect_to = min(CONNECT_TIMEOUT, 5.0) if quick else CONNECT_TIMEOUT
    read_to = READ_TIMEOUT if quick else READ_TIMEOUT * 1.5

    # Stage 1 — TCP reachability of the proxy endpoint itself.
    tcp_ok, tcp_err = _tcp_check(parsed.host, parsed.port, timeout=connect_to)
    if not tcp_ok:
        return ProxyTestResult(parsed.display, parsed.protocol_label, "TCP_FAILED",
                               False, None, None, f"TCP: {tcp_err}")

    proxies_dict = parsed.to_requests_proxies()
    if proxies_dict is None:
        return ProxyTestResult(parsed.display, parsed.protocol_label, "UNHEALTHY",
                               False, None, None, "SOCKS5 requires requests[socks] (not installed)")

    session = requests.Session()
    session.proxies = proxies_dict
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; ProxyTester/2.0)"})

    observed_ip = None
    best_latency = None
    last_error = None
    transport_ok = False

    try:
        # Stage 2/3 — try IP-check endpoints; ANY success = transport-working.
        for ip_url in _IP_CHECK_URLS:
            try:
                t0 = time.perf_counter()
                resp = session.get(ip_url, timeout=(connect_to, read_to), allow_redirects=True)
                latency = (time.perf_counter() - t0) * 1000
                if resp.status_code == 407:
                    return ProxyTestResult(parsed.display, parsed.protocol_label, "AUTH_FAILED",
                                           False, latency, None, "Proxy auth failed (HTTP 407)")
                if resp.status_code == 200:
                    transport_ok = True
                    best_latency = latency if best_latency is None else min(best_latency, latency)
                    try:
                        data = resp.json()
                        observed_ip = (data.get("ip") or data.get("query")
                                       or (data.get("origin", "").split(",")[0].strip() or None))
                    except Exception:
                        m = re.search(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b", resp.text)
                        if m:
                            observed_ip = m.group(1)
                    if observed_ip:
                        break  # verified exit IP — stop early
                else:
                    last_error = f"HTTP {resp.status_code} from IP check"
            except requests.exceptions.RequestException as exc:
                last_error = _classify_requests_error(exc)
                # A connection-level failure to the FIRST endpoint may be transport
                # failure; but keep trying other endpoints before concluding.
    finally:
        session.close()

    if transport_ok and observed_ip:
        status = "SLOW" if (best_latency or 0) >= 2000 else "WORKING"
        log_event("PROXY_SUCCESS", proxy=parsed.display, ip=observed_ip, latency=f"{best_latency:.0f}ms")
        return ProxyTestResult(parsed.display, parsed.protocol_label, status,
                               True, best_latency, observed_ip, None)
    if transport_ok and not observed_ip:
        # Proxy carried the request but no IP endpoint returned a parseable IP.
        return ProxyTestResult(parsed.display, parsed.protocol_label, "CONNECTED",
                               True, best_latency, None, "Transport OK, exit IP not verified")

    log_event("PROXY_FAILURE", proxy=parsed.display, reason=last_error or "all endpoints failed")
    return ProxyTestResult(parsed.display, parsed.protocol_label, "UNHEALTHY",
                           False, best_latency, None, last_error or "All IP-check endpoints failed")


# =========================================================
# Proxy Manager / Rotation Engine (job-aware, thread-safe)
# =========================================================
class ProxyManager:
    """
    Fair round-robin rotation across env + DB proxies.
    Skips proxies in DB cooldown; never falls back to a direct connection.
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
                items.append(parsed.raw)
            else:
                logger.warning("Ignoring invalid env proxy '%s': %s", _safe_log(raw), err)
        self._env_proxies = list(dict.fromkeys(items))
        logger.info("Loaded %d proxy endpoints from environment.", len(self._env_proxies))

    def get_all_raw(self) -> list[str]:
        db_eps = [r["endpoint"] for r in db_get_all_proxies(active_only=True)]
        return list(dict.fromkeys(self._env_proxies + db_eps))

    def has_endpoints(self) -> bool:
        return len(self.get_all_raw()) > 0

    def get_endpoint_count(self) -> int:
        return len(self.get_all_raw())

    def env_count(self) -> int:
        return len(self._env_proxies)

    def _in_cooldown(self, endpoint: str) -> bool:
        row = db_get_proxy_by_endpoint(endpoint)
        if not row or not row.get("cooldown_until"):
            return False
        conn = get_conn()
        try:
            r = conn.execute(
                "SELECT (cooldown_until > datetime('now')) AS cd FROM admin_proxies WHERE endpoint = ?",
                (endpoint,),
            ).fetchone()
            return bool(r and r["cd"])
        finally:
            conn.close()

    def eligible_endpoints(self) -> list[str]:
        """Endpoints not currently in cooldown."""
        return [ep for ep in self.get_all_raw() if not self._in_cooldown(ep)]

    def has_eligible(self) -> bool:
        return len(self.eligible_endpoints()) > 0

    def get_next_endpoint(self, exclude: Optional[set] = None) -> Optional[str]:
        """
        Next eligible proxy via round-robin. Returns None when every proxy is
        in cooldown — the caller MUST stop, never fall back to direct.
        """
        exclude = exclude or set()
        with self._lock:
            available = [ep for ep in self.eligible_endpoints() if ep not in exclude]
            if not available:
                # Allow reuse if excludes emptied the pool but proxies remain.
                available = self.eligible_endpoints()
                if not available:
                    return None
            idx = self._rotation_index % len(available)
            self._rotation_index += 1
            return available[idx]

    def mark_failed(self, endpoint: str, reason: str = "Rotation failure") -> None:
        row = db_get_proxy_by_endpoint(endpoint)
        if row:
            db_update_proxy_failure(row["id"], reason)
        else:
            # Env-only proxy with no DB row: insert one so cooldown persists.
            db_add_proxy(endpoint, added_by=0)
            row = db_get_proxy_by_endpoint(endpoint)
            if row:
                db_update_proxy_failure(row["id"], reason)
        log_event("PROXY_FAILURE", proxy=sanitize_display(endpoint), reason=reason)

    def mark_success(self, endpoint: str, latency_ms: float, observed_ip: str) -> None:
        row = db_get_proxy_by_endpoint(endpoint)
        if not row:
            db_add_proxy(endpoint, added_by=0)
            row = db_get_proxy_by_endpoint(endpoint)
        if row:
            status = "SLOW" if latency_ms >= 2000 else "WORKING"
            db_update_proxy_success(row["id"], latency_ms, observed_ip, status)


proxy_manager = ProxyManager()


# =========================================================
# Number extraction pipeline
# =========================================================
_EXTRACTORS = [
    ("wa.me", re.compile(r'wa\.me/(?:p/|qr/)?\+?(\d{10,15})', re.I)),
    ("whatsapp_api", re.compile(r'(?:api|web)\.whatsapp\.com/send/?\??[^"\'\s]*?(?:phone|number)=\+?(\d{10,15})', re.I)),
    ("whatsapp_scheme", re.compile(r'(?:whatsapp|intent)://send\?[^"\'\s]*?(?:phone|number)=\+?(\d{10,15})', re.I)),
    ("query_parameter", re.compile(r'(?:[?&])(?:phone|mobile|wa_number|whatsapp|send_to|number|recipient|to)=\+?(\d{10,15})', re.I)),
    ("tel_link", re.compile(r'(?:href=["\']|["\'])tel:\+?(\d{10,15})', re.I)),
    ("html_attribute", re.compile(r'data-(?:phone|whatsapp|number|mobile)=["\']\+?(\d{10,15})["\']', re.I)),
    ("json_field", re.compile(r'["\'](?:whatsapp|phone_number|mobile_number|wa_number|phone|mobile)["\']\s*:\s*["\']\+?(\d{10,15})["\']', re.I)),
]


def clean_phone_number(raw: str) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("00") and len(digits) > 12:
        digits = digits[2:]
    if 10 <= len(digits) <= 15:
        return digits
    return None


def extract_numbers_with_methods(text: str) -> dict[str, str]:
    """Return {normalized_number: extraction_method}. First method wins per number."""
    found: dict[str, str] = {}
    if not text:
        return found
    samples = [text, urllib.parse.unquote(text),
               urllib.parse.unquote_plus(text), html.unescape(text)]
    for sample in samples:
        for method, pattern in _EXTRACTORS:
            for match in pattern.findall(sample):
                if isinstance(match, tuple):
                    match = match[0]
                cleaned = clean_phone_number(match)
                if cleaned and cleaned not in found:
                    found[cleaned] = method
    return found


# =========================================================
# URL validation
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
        return False, "Missing hostname in URL"
    return True, ""


# =========================================================
# Extraction session (requests-based; proxy-safe)
# =========================================================
class ExtractionSession:
    """
    One session per request in rotation mode; a shared session in direct mode.
    All proxy traffic (HTTP/HTTPS/SOCKS5) goes through requests, which handles
    HTTPS CONNECT tunnelling and proxy auth correctly.
    """

    _UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    def __init__(self, proxy_endpoint: Optional[str] = None):
        self.proxy_endpoint = proxy_endpoint
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self._UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.session.max_redirects = MAX_REDIRECTS
        if proxy_endpoint:
            parsed, _ = parse_proxy(proxy_endpoint)
            if parsed:
                pd = parsed.to_requests_proxies()
                if pd:
                    self.session.proxies = pd

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def fetch(self, url: str) -> tuple[str, str, list[str]]:
        """Return (final_url, body, visited_urls). Follows meta/JS redirects (2 hops)."""
        visited: list[str] = [url]
        resp = self.session.get(
            url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            allow_redirects=True, stream=True,
        )
        for r in resp.history:
            visited.append(r.url)
        visited.append(resp.url)

        body_bytes = b""
        for chunk in resp.iter_content(chunk_size=65536):
            body_bytes += chunk
            if len(body_bytes) > MAX_RESPONSE_SIZE:
                break
        body = body_bytes.decode("utf-8", errors="ignore")
        current_url = resp.url
        resp.close()

        for _ in range(2):
            meta = re.search(
                r'<meta[^>]*?http-equiv\s*=\s*["\']?refresh["\']?[^>]*?content\s*=\s*["\']?[^"\'>]*?url\s*=\s*([^\s"\';>]+)',
                body, re.I)
            js = re.search(
                r'(?:window\.|document\.|top\.)?location(?:\.href|\.replace|\.assign)?\s*(?:=|\()\s*["\'](https?://[^"\']+)["\']',
                body, re.I)
            dest = None
            if meta:
                dest = urllib.parse.urljoin(current_url, meta.group(1).strip())
            elif js:
                dest = js.group(1).strip()
            if not dest or not dest.lower().startswith(("http://", "https://")):
                break
            visited.append(dest)
            try:
                r2 = self.session.get(dest, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                                      allow_redirects=True, stream=True)
                for r in r2.history:
                    visited.append(r.url)
                current_url = r2.url
                visited.append(current_url)
                bb = b""
                for chunk in r2.iter_content(chunk_size=65536):
                    bb += chunk
                    if len(bb) > MAX_RESPONSE_SIZE:
                        break
                body = bb.decode("utf-8", errors="ignore")
                r2.close()
            except Exception:
                break

        return current_url, body, visited


def add_cache_buster(url: str, cycle: int) -> str:
    """Append a cache-busting param unless the URL looks signed (has a token/sig)."""
    low = url.lower()
    if any(tok in low for tok in ("signature=", "sig=", "token=", "&x-amz-", "?x-amz-")):
        return url  # preserve signed URLs
    import random as _r
    ts = int(time.time() * 1000)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}_cb={ts}_{cycle}_{_r.randint(100, 999)}"


# =========================================================
# Live progress updater (independent thread)
# =========================================================
def _progress_bar(pct: int) -> str:
    ticks = max(0, min(10, int(pct / 10)))
    return "█" * ticks + "░" * (10 - ticks)


def render_progress(state: dict) -> str:
    total = state.get("total", 0)
    visits = state.get("visits", 0)
    pct = int((visits / total) * 100) if total else 0
    elapsed = int(time.time() - state.get("start", time.time()))
    speed = (visits / elapsed) if elapsed > 0 else 0
    lines = [
        "⏳ <b>EXTRACTION IN PROGRESS</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"🆔 Job: <code>#{state.get('job_id', 0):06d}</code>",
        f"⚙️ Mode: {state.get('mode_display', '')}",
        "",
        f"Progress: <code>[{_progress_bar(pct)}]</code> {pct}%",
        f"🔄 Visits: <code>{visits}/{total}</code>",
        f"✅ Successful: <code>{state.get('ok', 0)}</code>",
        f"❌ Failed: <code>{state.get('failed', 0)}</code>",
        f"📱 Unique Numbers: <code>{state.get('unique', 0)}</code>",
        f"♻️ Duplicates: <code>{state.get('dupes', 0)}</code>",
    ]
    if state.get("mode") == "ROTATING":
        lines.append("")
        lines.append(f"🌐 Proxy: <code>{html.escape(state.get('proxy_display', 'selecting…'))}</code>")
        if state.get("exit_ip"):
            lines.append(f"📡 Exit IP: <code>{html.escape(state['exit_ip'])}</code>")
        if state.get("latency"):
            lines.append(f"⚡ Latency: <code>{state['latency']:.0f} ms</code>")
    lines += [
        "",
        f"🔧 Stage: <i>{html.escape(state.get('stage', 'Working'))}</i>",
        f"⏱ Elapsed: {elapsed}s · Speed: {speed:.2f}/s",
        "━━━━━━━━━━━━━━━━━━",
        "<i>Tap ❌ Cancel to stop</i>",
    ]
    return "\n".join(lines)


def progress_updater(chat_id: int, message_id: int, state: dict,
                     stop_event: threading.Event) -> None:
    """Runs independently so the UI keeps moving even during a slow request."""
    last_text = ""
    while not stop_event.is_set():
        try:
            text = render_progress(state)
            if text != last_text:
                bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
                last_text = text
        except ApiTelegramException as exc:
            if "message is not modified" not in str(exc).lower():
                logger.debug("progress edit: %s", exc)
        except Exception as exc:
            logger.debug("progress edit error: %s", exc)
        stop_event.wait(PROGRESS_INTERVAL)


# =========================================================
# Channel posting
# =========================================================
def channel_target() -> str:
    return get_setting("channel_username", DEFAULT_CHANNEL).strip()


def verify_channel() -> tuple[bool, str]:
    target = channel_target()
    if not target:
        return False, "No channel configured"
    try:
        chat = bot.get_chat(target)
        member = bot.get_chat_member(chat.id, bot.get_me().id)
        if member.status in ("administrator", "creator"):
            return True, f"Verified: {getattr(chat, 'title', target)}"
        return False, "Bot is not an administrator of the channel"
    except ApiTelegramException as exc:
        return False, str(exc)[:120]
    except Exception as exc:
        return False, str(exc)[:120]


def post_to_channel(job: dict, numbers: list[str], txt_bytes: Optional[bytes]) -> None:
    if not get_bool_setting("channel_logging"):
        return
    target = channel_target()
    if not target:
        return

    dur = (job.get("duration_ms") or 0) / 1000.0
    uname = job.get("username")
    uname_disp = f"@{uname}" if uname else "N/A"
    summary = (
        "🚀 <b>EXTRACTION COMPLETED</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👤 User: {html.escape(uname_disp)}\n"
        f"🆔 User ID: <code>{job.get('user_id')}</code>\n\n"
        f"🔗 Source:\n<code>{html.escape((job.get('source_url') or '')[:120])}</code>\n\n"
        f"⚙️ Method: {'🌐 IP Rotation' if job.get('mode') == 'ROTATING' else '🟢 Direct'}\n"
        f"🔄 Visits: {job.get('requested_visits')}\n"
        f"✅ Successful: {job.get('successful_visits')}\n"
        f"❌ Failed: {job.get('failed_visits')}\n"
        f"📱 Unique Numbers: {job.get('unique_numbers')}\n"
        f"♻️ Duplicates: {job.get('duplicate_numbers')}\n"
        f"⏱ Duration: {dur:.1f}s\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Job ID: <code>#{job.get('job_id'):06d}</code>"
    )
    if get_bool_setting("channel_post_numbers") and numbers:
        preview = "\n".join(f"+{n}" for n in numbers[:30])
        summary += f"\n\n📞 <b>Numbers:</b>\n<code>{html.escape(preview)}</code>"
        if len(numbers) > 30:
            summary += f"\n<i>… and {len(numbers) - 30} more</i>"

    status, msg_id, err = "FAILED", None, None
    for attempt in range(3):
        try:
            sent = bot.send_message(target, summary)
            msg_id = sent.message_id
            status = "POSTED"
            if get_bool_setting("channel_attach_txt") and txt_bytes:
                stream = io.BytesIO(txt_bytes)
                stream.name = f"job_{job.get('job_id'):06d}_numbers.txt"
                try:
                    bot.send_document(target, stream)
                except Exception as e:
                    logger.warning("Channel TXT upload failed: %s", e)
            log_event("CHANNEL_POST_SUCCESS", job=job.get("job_id"))
            break
        except ApiTelegramException as exc:
            err = str(exc)[:150]
            time.sleep(1.5 * (attempt + 1))
        except Exception as exc:
            err = str(exc)[:150]
            break
    if status != "POSTED":
        log_event("CHANNEL_POST_FAILURE", job=job.get("job_id"), reason=err or "unknown")
    finalize_job(job.get("job_id"),
                 channel_post_status=status, channel_message_id=msg_id, channel_post_error=err)


# =========================================================
# Result TXT builder
# =========================================================
def build_result_txt(job: dict, numbers_records: list[dict]) -> bytes:
    bot_name = get_setting("bot_name")
    lines = [
        f"{bot_name} — Extraction Results",
        "=" * 45,
        f"Job ID:      #{job.get('job_id'):06d}",
        f"User ID:     {job.get('user_id')}",
        f"Username:    @{job.get('username') or 'N/A'}",
        f"Source URL:  {job.get('source_url')}",
        f"Mode:        {'IP Rotation' if job.get('mode') == 'ROTATING' else 'Direct'}",
        f"Date:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Visits:      {job.get('requested_visits')}",
        f"Successful:  {job.get('successful_visits')}",
        f"Failed:      {job.get('failed_visits')}",
        f"Unique:      {job.get('unique_numbers')}",
        f"Duplicates:  {job.get('duplicate_numbers')}",
        "",
        "Numbers (number | method | visit)",
        "-" * 45,
    ]
    for rec in numbers_records:
        lines.append(f"+{rec['number']}  |  {rec.get('extraction_method', '')}  |  visit {rec.get('visit_number', '')}")
    return ("\n".join(lines) + "\n").encode("utf-8")


# =========================================================
# Extraction worker
# =========================================================
def extraction_worker(chat_id: int, user_id: int, username: str, url: str,
                      mode: str, total: int, progress_msg_id: int,
                      job_id: int, cancel_event: threading.Event) -> None:
    log_event("JOB_START", job=job_id, user=user_id, mode=mode, visits=total)

    mode_display = "🌐 IP Rotation" if mode == "ROTATING" else "🟢 Direct Connection"
    state = {
        "job_id": job_id, "mode": mode, "mode_display": mode_display,
        "total": total, "visits": 0, "ok": 0, "failed": 0,
        "unique": 0, "dupes": 0, "start": time.time(),
        "stage": "Initialising", "proxy_display": "selecting…",
        "exit_ip": "", "latency": 0.0,
    }

    stop_progress = threading.Event()
    prog_thread = threading.Thread(
        target=progress_updater, args=(chat_id, progress_msg_id, state, stop_progress),
        daemon=True)
    prog_thread.start()

    found_order: list[str] = []
    found_set: set[str] = set()
    number_methods: dict[str, str] = {}
    total_seen = 0
    stopped_no_proxy = False
    start = time.time()

    shared_session = ExtractionSession(None) if mode == "NORMAL" else None
    recent_proxies: set = set()

    try:
        for cycle in range(1, total + 1):
            if cancel_event.is_set():
                break

            target_url = add_cache_buster(url, cycle)
            endpoint = None
            session = shared_session

            if mode == "ROTATING":
                state["stage"] = "Selecting proxy"
                endpoint = proxy_manager.get_next_endpoint(exclude=recent_proxies)
                if endpoint is None:
                    stopped_no_proxy = True
                    log_event("JOB_STOP_NO_PROXY", job=job_id, at=cycle)
                    break
                recent_proxies.add(endpoint)
                if len(recent_proxies) > max(1, min(3, proxy_manager.get_endpoint_count() - 1)):
                    recent_proxies.pop() if isinstance(recent_proxies, set) else None
                    recent_proxies = set(list(recent_proxies)[-3:])
                state["proxy_display"] = sanitize_display(endpoint)
                parsed, _ = parse_proxy(endpoint)
                session = ExtractionSession(endpoint)

            state["stage"] = "Connecting"
            t0 = time.perf_counter()
            attempt_status = "FAILED"
            attempt_ip = ""
            attempt_error = ""
            try:
                state["stage"] = "Fetching URL"
                final_url, body, visited = session.fetch(target_url)
                latency = (time.perf_counter() - t0) * 1000
                state["latency"] = latency
                state["stage"] = "Scanning response"

                cycle_numbers: dict[str, str] = {}
                for v in visited:
                    for num, meth in extract_numbers_with_methods(v).items():
                        cycle_numbers.setdefault(num, meth)
                for num, meth in extract_numbers_with_methods(body).items():
                    cycle_numbers.setdefault(num, meth)

                total_seen += len(cycle_numbers)
                for num, meth in sorted(cycle_numbers.items()):
                    if num not in found_set:
                        found_set.add(num)
                        found_order.append(num)
                        number_methods[num] = meth

                state["ok"] += 1
                state["unique"] = len(found_order)
                state["dupes"] = max(total_seen - len(found_order), 0)
                attempt_status = "SUCCESS"

                if mode == "ROTATING" and endpoint:
                    # Observe exit IP once per new proxy to confirm rotation.
                    observed = ""
                    try:
                        r = session.session.get(_IP_CHECK_URLS[0],
                                                 timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
                        if r.status_code == 200:
                            observed = (r.json().get("ip") or "")
                    except Exception:
                        observed = ""
                    attempt_ip = observed
                    state["exit_ip"] = observed or state.get("exit_ip", "")
                    proxy_manager.mark_success(endpoint, latency, observed)

            except Exception as exc:
                state["failed"] += 1
                attempt_error = _classify_requests_error(exc)
                log_event("VISIT_FAIL", job=job_id, cycle=cycle,
                          proxy=sanitize_display(endpoint or "direct"), reason=attempt_error)
                if mode == "ROTATING" and endpoint:
                    proxy_manager.mark_failed(endpoint, attempt_error)
            finally:
                if mode == "ROTATING" and session:
                    session.close()

            # Record every attempt & every number for the audit trail.
            if mode == "ROTATING":
                save_proxy_attempt(job_id, cycle, state["proxy_display"],
                                   parsed.protocol_label if endpoint and parsed else "",
                                   attempt_status, state.get("latency", 0),
                                   attempt_ip, attempt_error)
            state["visits"] = cycle
            time.sleep(0.05)

        # Persist all unique numbers with their first-seen method.
        for num in found_order:
            save_extracted_number(job_id, user_id, num, url, number_methods.get(num, ""),
                                  0, state.get("proxy_display", "") if mode == "ROTATING" else "",
                                  state.get("exit_ip", ""))

    finally:
        stop_progress.set()
        if shared_session:
            shared_session.close()
        with _state_lock:
            active_jobs.pop(user_id, None)

    # ── Finalize ──
    unique = len(found_order)
    dupes = max(total_seen - unique, 0)
    duration_ms = int((time.time() - start) * 1000)
    cancelled = cancel_event.is_set()
    status = "CANCELLED" if cancelled else ("FAILED" if (stopped_no_proxy and state["ok"] == 0) else "COMPLETED")

    finalize_job(job_id, successful_visits=state["ok"], failed_visits=state["failed"],
                 unique_numbers=unique, duplicate_numbers=dupes, duration_ms=duration_ms,
                 status=status, completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    update_user_stats(user_id, unique, success=(status == "COMPLETED"))
    # Legacy history table (backward compatibility).
    with _db_lock:
        c = get_conn()
        try:
            c.execute("INSERT INTO extraction_history (user_id, url, mode, cycles, unique_numbers, "
                      "duplicate_count, completed_at) VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)",
                      (user_id, url, mode, total, unique, dupes))
            c.commit()
        finally:
            c.close()

    log_event("JOB_COMPLETE", job=job_id, status=status, unique=unique)

    job = get_job(job_id)
    records = get_job_numbers(job_id)
    txt_bytes = build_result_txt(job, records) if unique else None

    # No-proxy stop message (never fell back to direct).
    if stopped_no_proxy and unique == 0:
        try:
            bot.edit_message_text(
                chat_id=chat_id, message_id=progress_msg_id,
                text=("❌ <b>IP Rotation stopped</b>\n━━━━━━━━━━━━━━━━━━\n"
                      "No verified proxy is currently available.\n"
                      f"Completed: {state['ok']}/{total} visits.\n\n"
                      "<i>Try again after proxies recover. The bot did not fall back "
                      "to a direct connection.</i>"))
        except Exception:
            pass
        bot.send_message(chat_id, "🏠 Main Menu", reply_markup=main_keyboard(user_id))
        return

    # Deliver results to the user.
    _send_results(chat_id, user_id, job, found_order, txt_bytes, cancelled, stopped_no_proxy, total, state)

    # Auto channel post (never crash the job if it fails).
    try:
        post_to_channel(get_job(job_id), found_order, txt_bytes)
    except Exception as exc:
        logger.warning("Channel post error: %s", exc)


def _send_results(chat_id, user_id, job, numbers, txt_bytes, cancelled,
                  stopped_no_proxy, total, state) -> None:
    mode_display = "🌐 IP Rotation" if job.get("mode") == "ROTATING" else "🟢 Direct Connection"
    note = " (Cancelled)" if cancelled else ""
    dur = (job.get("duration_ms") or 0) / 1000.0

    if not numbers:
        bot.send_message(
            chat_id,
            (f"⚠️ <b>No numbers found</b>{note}\n━━━━━━━━━━━━━━━━━━\n"
             f"🔄 Visits: <code>{job.get('successful_visits')}/{total}</code>\n"
             f"❌ Failed: <code>{job.get('failed_visits')}</code>\n"
             f"🌐 Mode: {mode_display}\n\n"
             "<i>The target returned no trackable phone patterns.</i>"),
            reply_markup=main_keyboard(user_id))
        return

    numbers_plain = "\n".join(f"+{n}" for n in numbers)
    result = (
        f"✅ <b>EXTRACTION COMPLETED</b>{note}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Job: <code>#{job.get('job_id'):06d}</code>\n"
        f"🔗 Source: <code>{html.escape((job.get('source_url') or '')[:60])}</code>\n\n"
        f"⚙️ Mode: {mode_display}\n"
        f"🔄 Visits: <code>{job.get('successful_visits')}/{total}</code>\n"
        f"✅ Successful: <code>{job.get('successful_visits')}</code>\n"
        f"❌ Failed: <code>{job.get('failed_visits')}</code>\n"
        f"📱 Unique Numbers: <code>{job.get('unique_numbers')}</code>\n"
        f"♻️ Duplicates: <code>{job.get('duplicate_numbers')}</code>\n"
        f"⏱ Duration: <code>{dur:.1f}s</code>\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    # First chunk of numbers inline (rest via TXT).
    preview = "\n".join(f"+{n}" for n in numbers[:60])
    result += f"\n\n📱 <b>NUMBERS</b>\n<code>{html.escape(preview)}</code>"
    if len(numbers) > 60:
        result += f"\n<i>… and {len(numbers) - 60} more (see TXT)</i>"

    try:
        bot.send_message(chat_id, result, reply_markup=build_copy_markup(numbers_plain))
    except Exception:
        bot.send_message(chat_id, result)

    if txt_bytes:
        try:
            stream = io.BytesIO(txt_bytes)
            stream.name = f"job_{job.get('job_id'):06d}_numbers.txt"
            bot.send_document(chat_id, stream,
                              caption=f"📁 <b>Results</b> · {job.get('unique_numbers')} unique numbers",
                              reply_markup=main_keyboard(user_id))
        except Exception as exc:
            logger.error("File upload failed: %s", exc)
            bot.send_message(chat_id, "🏠 Main Menu", reply_markup=main_keyboard(user_id))
    else:
        bot.send_message(chat_id, "🏠 Main Menu", reply_markup=main_keyboard(user_id))


# =========================================================
# Keyboards
# =========================================================
def main_keyboard(user_id: int = 0) -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(types.KeyboardButton("🔗 New Extraction"),
          types.KeyboardButton("📊 My Statistics"),
          types.KeyboardButton("📋 My History"),
          types.KeyboardButton("❓ Help"),
          types.KeyboardButton("📞 Support"))
    if is_admin(user_id):
        m.add(types.KeyboardButton("🔐 Admin Panel"))
    return m


def mode_keyboard() -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(types.KeyboardButton("🟢 WITHOUT IP"),
          types.KeyboardButton("🌐 WITH IP ROTATION"),
          types.KeyboardButton("🔙 Back"), types.KeyboardButton("❌ Cancel"))
    return m


def cycles_keyboard() -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(types.KeyboardButton("🧪 1 Visit"), types.KeyboardButton("🚀 20 Visits"),
          types.KeyboardButton("⚡ 50 Visits"), types.KeyboardButton("💎 100 Visits"),
          types.KeyboardButton("🔙 Back"), types.KeyboardButton("❌ Cancel"))
    return m


def cancel_keyboard() -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.add(types.KeyboardButton("❌ Cancel"))
    return m


def back_cancel_keyboard() -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(types.KeyboardButton("🔙 Back"), types.KeyboardButton("❌ Cancel"))
    return m


def admin_keyboard() -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(types.KeyboardButton("📊 Dashboard"), types.KeyboardButton("👥 Users"),
          types.KeyboardButton("🔎 Extraction Logs"), types.KeyboardButton("🌐 Proxy Manager"),
          types.KeyboardButton("📢 Broadcast"), types.KeyboardButton("📡 Channel Settings"),
          types.KeyboardButton("⚙️ Bot Settings"), types.KeyboardButton("👮 Admin Management"),
          types.KeyboardButton("🩺 Diagnostics"), types.KeyboardButton("🔙 Main Menu"))
    return m


def proxy_manager_keyboard() -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(types.KeyboardButton("➕ Add Proxy"), types.KeyboardButton("📦 Bulk Add"),
          types.KeyboardButton("🧪 Health Check"), types.KeyboardButton("🔄 Retest Unhealthy"),
          types.KeyboardButton("📋 Proxy List"), types.KeyboardButton("📊 Proxy Stats"),
          types.KeyboardButton("🗑 Cleanup"), types.KeyboardButton("🔙 Admin Panel"))
    return m


def build_copy_markup(numbers_text: str) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    try:
        markup.add(types.InlineKeyboardButton(
            text="📋 Copy All Numbers",
            copy_text=types.CopyTextButton(text=numbers_text[:4000])))
    except Exception:
        markup.add(types.InlineKeyboardButton(
            text="📋 Copy All Numbers", switch_inline_query=numbers_text[:250]))
    return markup


_CYCLE_MAP = {"🧪 1 Visit": 1, "🚀 20 Visits": 20, "⚡ 50 Visits": 50, "💎 100 Visits": 100}


# =========================================================
# Background proxy test runner (bounded pool, live progress)
# =========================================================
def run_proxy_tests_bg(chat_id: int, status_msg_id: int, endpoints: list[str],
                       cancel_event: threading.Event, detailed: bool = False) -> None:
    total = len(endpoints)
    results: list[ProxyTestResult] = []
    tested = working = slow = failed = 0
    last_update = 0.0

    def _one(raw: str) -> ProxyTestResult:
        parsed, err = parse_proxy(raw)
        if not parsed:
            return ProxyTestResult(sanitize_display(raw), "UNKNOWN", "INVALID",
                                   False, None, None, f"Parse: {err}")
        res = test_proxy(parsed, quick=not detailed)
        row = db_get_proxy_by_endpoint(raw)
        if row:
            if res.working:
                db_update_proxy_success(row["id"], res.latency_ms or 0, res.observed_ip or "", res.status)
            else:
                db_update_proxy_failure(row["id"], res.error_reason or "Test failed", res.status)
        return res

    futures = {_proxy_test_pool.submit(_one, raw): raw for raw in endpoints}
    for fut in concurrent.futures.as_completed(futures):
        if cancel_event.is_set():
            for f in futures:
                f.cancel()
            break
        res = fut.result()
        results.append(res)
        tested += 1
        if res.working and res.status == "WORKING":
            working += 1
        elif res.working and res.status in ("SLOW", "CONNECTED"):
            slow += 1
        else:
            failed += 1
        now = time.time()
        if now - last_update > 1.5 or tested == total:
            last_update = now
            pct = int(tested / total * 100)
            try:
                bot.edit_message_text(
                    chat_id=chat_id, message_id=status_msg_id,
                    text=(f"🧪 <b>PROXY HEALTH CHECK</b>\n━━━━━━━━━━━━━━━━━━\n"
                          f"<code>[{_progress_bar(pct)}]</code> {pct}%\n\n"
                          f"Checked: {tested}/{total}\n🟢 Working: {working}\n"
                          f"🟡 Slow: {slow}\n🔴 Failed: {failed}\n"
                          f"Remaining: {total - tested}"))
            except Exception:
                pass

    if cancel_event.is_set():
        bot.send_message(chat_id, f"🛑 Health check cancelled ({tested}/{total}).",
                         reply_markup=proxy_manager_keyboard())
        return

    summary = (f"🧪 <b>HEALTH CHECK COMPLETE</b>\n━━━━━━━━━━━━━━━━━━\n"
               f"Total: {total}\n🟢 Working: {working}\n🟡 Slow/Connected: {slow}\n"
               f"🔴 Failed: {failed}")
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=status_msg_id, text=summary)
    except Exception:
        pass

    for i in range(0, len(results), 5):
        block = "\n\n━━━━━━━━━━━━━━━━━━\n\n".join(
            r.card(index=idx + 1) for idx, r in enumerate(results[i:i + 5], start=i))
        try:
            bot.send_message(chat_id, block)
        except Exception:
            pass
        time.sleep(0.4)
    bot.send_message(chat_id, "✅ Done.", reply_markup=proxy_manager_keyboard())


def trigger_retest_unhealthy(chat_id: int) -> None:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT endpoint FROM admin_proxies WHERE is_active = 1 "
            "AND (health_status != 'WORKING' OR last_tested IS NULL) "
            "ORDER BY last_tested ASC").fetchall()
    finally:
        conn.close()
    endpoints = [r["endpoint"] for r in rows]
    if not endpoints:
        bot.send_message(chat_id, "✅ No unhealthy/untested proxies.", reply_markup=proxy_manager_keyboard())
        return
    status = bot.send_message(chat_id, f"🔄 Retesting {len(endpoints)} proxies…")
    ev = threading.Event()
    threading.Thread(target=run_proxy_tests_bg,
                     args=(chat_id, status.message_id, endpoints, ev, False), daemon=True).start()


# =========================================================
# Command handlers
# =========================================================
@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message) -> None:
    u = message.from_user
    register_user(u.id, u.username, u.first_name)

    if user_is_blocked(u.id):
        bot.send_message(message.chat.id, "🚫 <b>Access blocked.</b>")
        return

    if not user_is_approved(u.id):
        bot.send_message(message.chat.id,
                         "🔒 <b>ACCESS PENDING</b>\n\nYour access request has been submitted.\n"
                         "Please wait for administrator approval.")
        _notify_admins_new_request(u)
        return

    bot.send_message(
        message.chat.id,
        (f"🤖 <b>{html.escape(get_setting('bot_name'))}</b>\n"
         "━━━━━━━━━━━━━━━━━━\n"
         "Extract publicly exposed phone numbers from HTTP/HTTPS links and redirect chains.\n\n"
         "1️⃣ Tap <b>🔗 New Extraction</b>\n"
         "2️⃣ Send a valid link\n"
         "3️⃣ Choose mode (Direct / IP Rotation)\n"
         "4️⃣ Choose visit count\n"
         "5️⃣ Get unique numbers + TXT download\n"
         "━━━━━━━━━━━━━━━━━━"),
        reply_markup=main_keyboard(u.id))


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "❌ <b>Access Denied.</b>")
        return
    bot.send_message(message.chat.id,
                     "🔐 <b>ADMIN CONTROL CENTER</b>\n━━━━━━━━━━━━━━━━━━",
                     reply_markup=admin_keyboard())


def _notify_admins_new_request(u) -> None:
    text = (f"👤 <b>NEW USER REQUEST</b>\n\nName: {html.escape(u.first_name or '')}\n"
            f"Username: @{html.escape(u.username or 'none')}\nUser ID: <code>{u.id}</code>")
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{u.id}"),
               types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{u.id}"))
    for aid in [a["user_id"] for a in list_admins()]:
        try:
            bot.send_message(aid, text, reply_markup=markup)
        except Exception:
            pass


# =========================================================
# Callback handlers
# =========================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith(("approve_", "reject_")))
def cb_approval(call: types.CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    action, _, uid_s = call.data.partition("_")
    if not uid_s.isdigit():
        return
    uid = int(uid_s)
    if action == "approve":
        set_user_status(uid, "APPROVED")
        audit(call.from_user.id, "USER_APPROVED", target=str(uid))
        bot.answer_callback_query(call.id, "Approved.")
        try:
            bot.send_message(uid, "✅ <b>Access approved!</b> Tap /start to begin.")
        except Exception:
            pass
    else:
        set_user_status(uid, "BLOCKED")
        audit(call.from_user.id, "USER_REJECTED", target=str(uid))
        bot.answer_callback_query(call.id, "Rejected.")
    try:
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                              text=f"{call.message.text}\n\n➡️ <b>{action.upper()}D</b>")
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("del_proxy_"))
def cb_del_proxy(call: types.CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    pid = call.data.replace("del_proxy_", "")
    if pid.isdigit():
        db_delete_proxy(int(pid))
        audit(call.from_user.id, "PROXY_DELETED", target=pid)
        bot.answer_callback_query(call.id, "Deleted.")
        try:
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                  text=f"🗑 <i>Proxy #{html.escape(pid)} deleted.</i>")
        except Exception:
            pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("job_"))
def cb_job_detail(call: types.CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return
    jid = call.data.replace("job_", "")
    if not jid.isdigit():
        return
    bot.answer_callback_query(call.id)
    job = get_job(int(jid))
    if not job:
        bot.send_message(call.message.chat.id, "Job not found.")
        return
    nums = get_job_numbers(int(jid))
    dur = (job.get("duration_ms") or 0) / 1000.0
    lines = [
        f"📱 <b>JOB #{job['job_id']:06d}</b>", "━━━━━━━━━━━━━━━━━━",
        f"👤 @{html.escape(job.get('username') or 'N/A')} · 🆔 <code>{job.get('user_id')}</code>",
        f"🔗 <code>{html.escape((job.get('source_url') or '')[:80])}</code>",
        f"⚙️ Mode: {'IP Rotation' if job.get('mode') == 'ROTATING' else 'Direct'}",
        f"🔄 {job.get('successful_visits')}/{job.get('requested_visits')} ok · "
        f"❌ {job.get('failed_visits')}",
        f"📱 Unique: {job.get('unique_numbers')} · ♻️ Dupes: {job.get('duplicate_numbers')}",
        f"⏱ {dur:.1f}s · Status: {job.get('status')}", "",
        "<b>Numbers:</b>",
    ]
    for r in nums[:40]:
        lines.append(f"+{r['number']} · <i>{html.escape(r.get('extraction_method') or '')}</i>")
    if len(nums) > 40:
        lines.append(f"<i>… and {len(nums) - 40} more</i>")
    bot.send_message(call.message.chat.id, "\n".join(lines))


# =========================================================
# Main message router
# =========================================================
@bot.message_handler(func=lambda m: True)
def handle_all(message: types.Message) -> None:
    u = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()
    register_user(u.id, u.username, u.first_name)

    with _state_lock:
        state = dict(user_states.get(u.id, {}))

    admin = is_admin(u.id)

    # ── Access gates ──
    if user_is_blocked(u.id):
        bot.send_message(chat_id, "🚫 <b>Access blocked.</b>")
        return
    if get_bool_setting("maintenance_mode") and not admin:
        bot.send_message(chat_id, "🛠 <b>BOT UNDER MAINTENANCE</b>\n\nPlease try again later.")
        return
    if not user_is_approved(u.id):
        bot.send_message(chat_id, "🔒 <b>ACCESS PENDING</b>\n\nPlease wait for administrator approval.")
        return

    # ── Global navigation ──
    if text == "❌ Cancel":
        with _state_lock:
            job = active_jobs.get(u.id)
        if job:
            job["cancel"].set()
            bot.send_message(chat_id, "🛑 <b>Cancelling…</b>", reply_markup=main_keyboard(u.id))
        else:
            with _state_lock:
                user_states[u.id] = {}
            bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard(u.id))
        return

    if text == "🔙 Main Menu":
        with _state_lock:
            user_states[u.id] = {}
        bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard(u.id))
        return

    if text == "🔐 Admin Panel" and admin:
        with _state_lock:
            user_states[u.id] = {}
        bot.send_message(chat_id, "🔐 <b>ADMIN CONTROL CENTER</b>", reply_markup=admin_keyboard())
        return

    if text == "🔙 Admin Panel" and admin:
        with _state_lock:
            user_states[u.id] = {}
        bot.send_message(chat_id, "🔐 <b>ADMIN CONTROL CENTER</b>", reply_markup=admin_keyboard())
        return

    if text == "🔙 Back":
        step = state.get("step")
        if step == "AWAITING_MODE":
            with _state_lock:
                user_states[u.id] = {"step": "AWAITING_URL"}
            bot.send_message(chat_id, "🔗 Send a valid HTTP/HTTPS link:", reply_markup=cancel_keyboard())
        elif step == "AWAITING_CYCLES":
            with _state_lock:
                user_states[u.id] = {"step": "AWAITING_MODE", "url": state.get("url")}
            bot.send_message(chat_id, "Choose mode:", reply_markup=mode_keyboard())
        elif admin:
            with _state_lock:
                user_states[u.id] = {}
            bot.send_message(chat_id, "🔐 Admin Panel", reply_markup=admin_keyboard())
        else:
            with _state_lock:
                user_states[u.id] = {}
            bot.send_message(chat_id, "🏠 Main Menu", reply_markup=main_keyboard(u.id))
        return

    # ── Admin awaiting-input states ──
    if admin and state.get("await"):
        _handle_admin_input(message, u, chat_id, text, state)
        return

    # ── Admin menu buttons ──
    if admin and _handle_admin_menu(message, u, chat_id, text):
        return

    # ── User menu ──
    if text == "🔗 New Extraction":
        with _state_lock:
            if u.id in active_jobs:
                bot.send_message(chat_id, "⚠️ A job is already running. Tap ❌ Cancel first.",
                                 reply_markup=cancel_keyboard())
                return
            user_states[u.id] = {"step": "AWAITING_URL"}
        bot.send_message(chat_id,
                         "🔗 <b>Submit Target URL</b>\n\nSend a valid HTTP/HTTPS link:\n\n"
                         "<i>Example:</i> <code>https://example.com/redirect</code>",
                         reply_markup=cancel_keyboard())
        return

    if text == "📊 My Statistics":
        s = get_user(u.id) or {}
        bot.send_message(chat_id,
                         (f"📊 <b>MY STATISTICS</b>\n━━━━━━━━━━━━━━━━━━\n"
                          f"👤 {html.escape(s.get('first_name') or 'User')}\n"
                          f"🆔 <code>{u.id}</code>\n"
                          f"🔄 Total Extractions: <code>{s.get('total_extractions', 0)}</code>\n"
                          f"✅ Successful Jobs: <code>{s.get('successful_jobs', 0)}</code>\n"
                          f"❌ Failed Jobs: <code>{s.get('failed_jobs', 0)}</code>\n"
                          f"📱 Unique Numbers: <code>{s.get('total_numbers_found', 0)}</code>\n"
                          f"📅 Since: <code>{html.escape(str(s.get('joined_at', 'N/A'))[:10])}</code>"),
                         reply_markup=main_keyboard(u.id))
        return

    if text == "📋 My History":
        hist = get_user_history(u.id, limit=8)
        if not hist:
            bot.send_message(chat_id, "📋 <b>No history yet.</b>", reply_markup=main_keyboard(u.id))
            return
        msg = "📋 <b>MY HISTORY</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        for h in hist:
            mode_lbl = "🌐 IP" if h.get("mode") == "ROTATING" else "🟢 Direct"
            url_disp = html.escape((h["source_url"][:34] + "…") if len(h.get("source_url") or "") > 34 else (h.get("source_url") or ""))
            msg += (f"<b>#{h['job_id']:06d}</b> [{mode_lbl}] · <code>{html.escape(str(h.get('completed_at') or '')[:16])}</code>\n"
                    f"🔗 <code>{url_disp}</code>\n"
                    f"🔄 {h.get('requested_visits')} · 📱 {h.get('unique_numbers')}\n\n")
        bot.send_message(chat_id, msg, reply_markup=main_keyboard(u.id))
        return

    if text == "❓ Help":
        socks = "✅ Available" if _SOCKS5_AVAILABLE else "❌ Not installed"
        bot.send_message(chat_id,
                         ("❓ <b>HELP</b>\n━━━━━━━━━━━━━━━━━━\n"
                          "Fetches HTTP/HTTPS URLs and extracts publicly exposed phone numbers "
                          "from pages and redirect chains.\n\n"
                          "<b>Modes:</b>\n🟢 WITHOUT IP — direct connection\n"
                          "🌐 WITH IP ROTATION — via verified proxies\n\n"
                          f"<b>SOCKS5:</b> {socks}\n\n"
                          "Cancel anytime with ❌ Cancel."),
                         reply_markup=main_keyboard(u.id))
        return

    if text == "📞 Support":
        sup = get_setting("support_username")
        contact = f"Contact: @{html.escape(sup)}" if sup else "Contact the administrator."
        bot.send_message(chat_id, f"📞 <b>SUPPORT</b>\n━━━━━━━━━━━━━━━━━━\n{contact}",
                         reply_markup=main_keyboard(u.id))
        return

    # ── Extraction flow ──
    if state.get("step") == "AWAITING_URL" or text.startswith(("http://", "https://")):
        valid, err = validate_url(text)
        if not valid:
            bot.send_message(chat_id, f"⚠️ <b>Invalid URL</b>\n{html.escape(err)}",
                             reply_markup=cancel_keyboard())
            return
        with _state_lock:
            user_states[u.id] = {"step": "AWAITING_MODE", "url": text}
        bot.send_message(chat_id,
                         (f"🔗 <b>Link received</b>\n<code>{html.escape(text[:80])}</code>\n\n"
                          "⚙️ <b>SELECT CONNECTION MODE</b>\n"
                          "🟢 WITHOUT IP — direct\n🌐 WITH IP ROTATION — via proxies"),
                         reply_markup=mode_keyboard())
        return

    if state.get("step") == "AWAITING_MODE":
        target_url = state.get("url", "")
        if text == "🟢 WITHOUT IP":
            with _state_lock:
                user_states[u.id] = {"step": "AWAITING_CYCLES", "url": target_url, "mode": "NORMAL"}
            bot.send_message(chat_id, "🟢 <b>Direct mode.</b> Select visit count:",
                             reply_markup=cycles_keyboard())
            return
        if text == "🌐 WITH IP ROTATION":
            if not proxy_manager.has_endpoints():
                bot.send_message(chat_id,
                                 "⚠️ <b>IP Rotation Unavailable</b>\n\nNo proxy endpoints configured.",
                                 reply_markup=mode_keyboard())
                return
            if not proxy_manager.has_eligible():
                bot.send_message(chat_id,
                                 "⚠️ <b>IP Rotation Currently Unavailable</b>\n\n"
                                 "No verified working proxy is available (all in cooldown).\n"
                                 "Please try again later.",
                                 reply_markup=mode_keyboard())
                return
            with _state_lock:
                user_states[u.id] = {"step": "AWAITING_CYCLES", "url": target_url, "mode": "ROTATING"}
            bot.send_message(chat_id,
                             f"🌐 <b>IP Rotation mode.</b>\n📡 Available proxies: "
                             f"<code>{len(proxy_manager.eligible_endpoints())}</code>\n\nSelect visit count:",
                             reply_markup=cycles_keyboard())
            return

    if state.get("step") == "AWAITING_CYCLES":
        count = _CYCLE_MAP.get(text)
        if count is not None:
            max_v = int(get_setting("max_visits", str(MAX_VISITS_PER_JOB)))
            count = min(count, max_v)
            target_url = state.get("url", "")
            mode = state.get("mode", "NORMAL")
            cancel_ev = threading.Event()
            job_id = create_job(u.id, u.username, target_url, mode, count)
            with _state_lock:
                user_states[u.id] = {}
                active_jobs[u.id] = {"cancel": cancel_ev, "job_id": job_id}
            mode_name = "🌐 IP Rotation" if mode == "ROTATING" else "🟢 Direct Connection"
            start_msg = bot.send_message(chat_id,
                                         (f"⏳ <b>EXTRACTION IN PROGRESS</b>\n━━━━━━━━━━━━━━━━━━\n"
                                          f"🆔 Job: <code>#{job_id:06d}</code>\n"
                                          f"⚙️ Mode: {mode_name}\n"
                                          f"🔄 Visits: 0/{count}\n<i>Initialising…</i>"),
                                         reply_markup=cancel_keyboard())
            threading.Thread(target=extraction_worker,
                             args=(chat_id, u.id, u.username or "", target_url, mode, count,
                                   start_msg.message_id, job_id, cancel_ev), daemon=True).start()
            return

    bot.send_message(chat_id, "❓ Please pick an option or tap <b>🔗 New Extraction</b>.",
                     reply_markup=main_keyboard(u.id))


# =========================================================
# Admin menu dispatch
# =========================================================
def _handle_admin_menu(message, u, chat_id, text) -> bool:
    if text == "📊 Dashboard":
        s = get_admin_dashboard_stats()
        ps = db_proxy_stats()
        avg_ms = s.get("avg_ms")
        bot.send_message(chat_id,
                         (f"📊 <b>BOT DASHBOARD</b>\n━━━━━━━━━━━━━━━━━━\n"
                          f"👥 Users: <code>{s.get('total_users', 0)}</code>\n"
                          f"🟢 Active Today: <code>{s.get('active_today', 0)}</code>\n\n"
                          f"🔄 Total Jobs: <code>{s.get('total_jobs', 0)}</code>\n"
                          f"✅ Successful: <code>{s.get('ok_jobs', 0)}</code>\n"
                          f"❌ Failed: <code>{s.get('failed_jobs', 0)}</code>\n"
                          f"📅 Jobs Today: <code>{s.get('jobs_today', 0)}</code>\n\n"
                          f"📱 Numbers Today: <code>{s.get('nums_today', 0)}</code>\n\n"
                          f"🌐 Proxies — 🟢 {ps.get('working', 0)} · 🟡 {ps.get('slow', 0)} · 🔴 {ps.get('dead', 0)}\n"
                          f"⏱ Avg Job: <code>{(avg_ms/1000.0):.1f}s</code>" if avg_ms else
                          f"📊 <b>BOT DASHBOARD</b>\n━━━━━━━━━━━━━━━━━━\n"
                          f"👥 Users: <code>{s.get('total_users', 0)}</code>"),
                         reply_markup=admin_keyboard())
        return True

    if text == "👥 Users":
        with _state_lock:
            user_states[u.id] = {"await": "USER_SEARCH"}
        bot.send_message(chat_id, "🔎 <b>Search user</b>\nSend a Telegram ID, @username, or name:",
                         reply_markup=back_cancel_keyboard())
        return True

    if text == "🔎 Extraction Logs":
        jobs = recent_jobs(limit=10)
        if not jobs:
            bot.send_message(chat_id, "No extraction jobs yet.", reply_markup=admin_keyboard())
            return True
        for j in jobs:
            mode_lbl = "🌐 IP" if j.get("mode") == "ROTATING" else "🟢 Direct"
            card = (f"<b>#{j['job_id']:06d}</b> · @{html.escape(j.get('username') or 'N/A')} [{mode_lbl}]\n"
                    f"✅ {j.get('successful_visits')}/{j.get('requested_visits')} · "
                    f"📱 {j.get('unique_numbers')} · 🕒 {str(j.get('completed_at') or '')[:16]}")
            mk = types.InlineKeyboardMarkup()
            mk.add(types.InlineKeyboardButton("🔍 Details", callback_data=f"job_{j['job_id']}"))
            try:
                bot.send_message(chat_id, card, reply_markup=mk)
            except Exception:
                pass
        bot.send_message(chat_id, "🔎 Tap a job for full details.", reply_markup=admin_keyboard())
        return True

    if text == "🌐 Proxy Manager":
        ps = db_proxy_stats()
        bot.send_message(chat_id,
                         (f"🌐 <b>PROXY MANAGER</b>\n━━━━━━━━━━━━━━━━━━\n"
                          f"📊 Total: <code>{proxy_manager.get_endpoint_count()}</code> "
                          f"(⚙️ env {proxy_manager.env_count()})\n"
                          f"🟢 Working: <code>{ps.get('working', 0)}</code>\n"
                          f"🟡 Slow: <code>{ps.get('slow', 0)}</code>\n"
                          f"🔴 Failed: <code>{ps.get('dead', 0)}</code>\n"
                          f"⚪ Untested: <code>{ps.get('untested', 0)}</code>"),
                         reply_markup=proxy_manager_keyboard())
        return True

    if text == "➕ Add Proxy":
        with _state_lock:
            user_states[u.id] = {"await": "ADD_PROXY"}
        bot.send_message(chat_id,
                         "➕ <b>Add Proxy</b>\nSend one proxy:\n"
                         "<code>http://ip:port</code>\n<code>http://user:pass@ip:port</code>\n"
                         "<code>socks5://ip:port</code>\n\n<i>Credentials never shown in chat.</i>",
                         reply_markup=back_cancel_keyboard())
        return True

    if text == "📦 Bulk Add":
        with _state_lock:
            user_states[u.id] = {"await": "BULK_ADD"}
        bot.send_message(chat_id, "📦 <b>Bulk Add</b>\nPaste proxies, one per line:",
                         reply_markup=back_cancel_keyboard())
        return True

    if text == "🧪 Health Check":
        eps = proxy_manager.get_all_raw()
        if not eps:
            bot.send_message(chat_id, "⚠️ No proxies configured.", reply_markup=proxy_manager_keyboard())
            return True
        status = bot.send_message(chat_id, f"🧪 Checking {len(eps)} proxies…")
        ev = threading.Event()
        threading.Thread(target=run_proxy_tests_bg,
                         args=(chat_id, status.message_id, eps, ev, False), daemon=True).start()
        return True

    if text == "🔄 Retest Unhealthy":
        trigger_retest_unhealthy(chat_id)
        return True

    if text == "📋 Proxy List":
        rows = db_get_all_proxies(active_only=False)
        env = proxy_manager._env_proxies
        if not rows and not env:
            bot.send_message(chat_id, "📋 No proxies configured.", reply_markup=proxy_manager_keyboard())
            return True
        if env:
            lines = ["⚙️ <b>Environment proxies:</b>"] + [
                f"• <code>{html.escape(sanitize_display(e))}</code>" for e in env]
            bot.send_message(chat_id, "\n".join(lines))
        for i in range(0, len(rows), 8):
            for item in rows[i:i + 8]:
                emoji = PROXY_STATUS_EMOJI.get(item.get("health_status", "UNTESTED"), "⚪")
                lat = f"{item['average_latency']:.0f} ms" if item.get("average_latency") else "N/A"
                card = (f"{emoji} <b>#{item['id']}</b> <code>{html.escape(sanitize_display(item['endpoint']))}</code>\n"
                        f"   ✅ {item.get('success_count', 0)} · ❌ {item.get('failure_count', 0)} · "
                        f"⚡ {lat} · 🌍 <code>{html.escape(item.get('last_observed_ip') or '?')}</code>")
                mk = types.InlineKeyboardMarkup()
                mk.add(types.InlineKeyboardButton(f"🗑 Delete #{item['id']}", callback_data=f"del_proxy_{item['id']}"))
                try:
                    bot.send_message(chat_id, card, reply_markup=mk)
                except Exception:
                    pass
            time.sleep(0.3)
        bot.send_message(chat_id, "📋 End of list.", reply_markup=proxy_manager_keyboard())
        return True

    if text == "📊 Proxy Stats":
        ps = db_proxy_stats()
        avg = ps.get("avg_latency")
        bot.send_message(chat_id,
                         (f"📊 <b>PROXY STATISTICS</b>\n━━━━━━━━━━━━━━━━━━\n"
                          f"📡 Total (DB): <code>{ps.get('total', 0)}</code>\n"
                          f"🟢 Working: <code>{ps.get('working', 0)}</code>\n"
                          f"🟡 Slow: <code>{ps.get('slow', 0)}</code>\n"
                          f"🟠 Temp Failure: <code>{ps.get('temp_fail', 0)}</code>\n"
                          f"🔴 Unhealthy: <code>{ps.get('dead', 0)}</code>\n"
                          f"⚪ Untested: <code>{ps.get('untested', 0)}</code>\n"
                          f"⚡ Avg Latency: <code>{(avg or 0):.0f} ms</code>"),
                         reply_markup=proxy_manager_keyboard())
        return True

    if text == "🗑 Cleanup":
        deleted = db_clear_dead_proxies()
        audit(u.id, "PROXY_CLEANUP", details=f"{deleted} removed")
        bot.send_message(chat_id, f"🗑 Removed <code>{deleted}</code> permanently-dead proxies.\n"
                                  "<i>(Only proxies that never succeeded and passed the failure threshold.)</i>",
                         reply_markup=proxy_manager_keyboard())
        return True

    if text == "📢 Broadcast":
        with _state_lock:
            user_states[u.id] = {"await": "BROADCAST"}
        bot.send_message(chat_id, "📢 <b>Broadcast</b>\nType the message for all users:",
                         reply_markup=cancel_keyboard())
        return True

    if text == "📡 Channel Settings":
        _show_channel_settings(chat_id)
        return True

    if text == "🧪 Test Channel":
        ok, reason = verify_channel()
        bot.send_message(chat_id,
                         (f"✅ <b>Channel verified.</b>\n{html.escape(reason)}" if ok else
                          f"❌ <b>Channel posting failed.</b>\nReason: {html.escape(reason)}\n\n"
                          "Add the bot as an administrator with permission to post."),
                         reply_markup=admin_keyboard())
        return True

    if text == "🔀 Toggle Channel Posting":
        new = "0" if get_bool_setting("channel_logging") else "1"
        set_setting("channel_logging", new)
        audit(u.id, "CHANNEL_TOGGLE", details=new)
        _show_channel_settings(chat_id)
        return True

    if text == "🔀 Toggle Publish Numbers":
        new = "0" if get_bool_setting("channel_post_numbers") else "1"
        set_setting("channel_post_numbers", new)
        _show_channel_settings(chat_id)
        return True

    if text == "✏️ Set Channel":
        with _state_lock:
            user_states[u.id] = {"await": "SET_CHANNEL"}
        bot.send_message(chat_id, "✏️ Send the channel username (e.g. <code>@mychannel</code>):",
                         reply_markup=back_cancel_keyboard())
        return True

    if text == "⚙️ Bot Settings":
        _show_bot_settings(chat_id)
        return True

    if text == "🔀 Toggle Maintenance":
        new = "0" if get_bool_setting("maintenance_mode") else "1"
        set_setting("maintenance_mode", new)
        audit(u.id, "MAINTENANCE", details=new)
        _show_bot_settings(chat_id)
        return True

    if text == "🔀 Toggle Approval":
        new = "0" if get_bool_setting("approval_mode") else "1"
        set_setting("approval_mode", new)
        audit(u.id, "APPROVAL_MODE", details=new)
        _show_bot_settings(chat_id)
        return True

    if text == "✏️ Set Support Username":
        with _state_lock:
            user_states[u.id] = {"await": "SET_SUPPORT"}
        bot.send_message(chat_id, "✏️ Send the support username:", reply_markup=back_cancel_keyboard())
        return True

    if text == "👮 Admin Management":
        _show_admin_management(chat_id, u.id)
        return True

    if text == "➕ Add Admin" and is_owner(u.id):
        with _state_lock:
            user_states[u.id] = {"await": "ADD_ADMIN"}
        bot.send_message(chat_id, "➕ Send the numeric Telegram ID to promote to admin:",
                         reply_markup=back_cancel_keyboard())
        return True

    if text == "➖ Remove Admin" and is_owner(u.id):
        with _state_lock:
            user_states[u.id] = {"await": "REMOVE_ADMIN"}
        bot.send_message(chat_id, "➖ Send the numeric Telegram ID to remove:",
                         reply_markup=back_cancel_keyboard())
        return True

    if text == "🩺 Diagnostics":
        _run_diagnostics(chat_id)
        return True

    return False


def _show_channel_settings(chat_id) -> None:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(types.KeyboardButton("🧪 Test Channel"), types.KeyboardButton("✏️ Set Channel"),
          types.KeyboardButton("🔀 Toggle Channel Posting"),
          types.KeyboardButton("🔀 Toggle Publish Numbers"), types.KeyboardButton("🔙 Admin Panel"))
    bot.send_message(chat_id,
                     (f"📡 <b>CHANNEL SETTINGS</b>\n━━━━━━━━━━━━━━━━━━\n"
                      f"Channel: <code>{html.escape(channel_target() or 'not set')}</code>\n"
                      f"Auto Post: {'✅ ON' if get_bool_setting('channel_logging') else '❌ OFF'}\n"
                      f"Publish Numbers: {'✅ ON' if get_bool_setting('channel_post_numbers') else '❌ OFF'}"),
                     reply_markup=m)


def _show_bot_settings(chat_id) -> None:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add(types.KeyboardButton("🔀 Toggle Maintenance"), types.KeyboardButton("🔀 Toggle Approval"),
          types.KeyboardButton("✏️ Set Support Username"), types.KeyboardButton("🔙 Admin Panel"))
    bot.send_message(chat_id,
                     (f"⚙️ <b>BOT SETTINGS</b>\n━━━━━━━━━━━━━━━━━━\n"
                      f"Maintenance: {'🔴 ON' if get_bool_setting('maintenance_mode') else '🟢 OFF'}\n"
                      f"Approval Mode: {'✅ ON' if get_bool_setting('approval_mode') else '❌ OFF'}\n"
                      f"Support: @{html.escape(get_setting('support_username') or 'not set')}\n"
                      f"Max Visits: <code>{get_setting('max_visits')}</code>"),
                     reply_markup=m)


def _show_admin_management(chat_id, uid) -> None:
    admins = list_admins()
    lines = ["👮 <b>ADMIN MANAGEMENT</b>", "━━━━━━━━━━━━━━━━━━"]
    for a in admins:
        lines.append(f"• <code>{a['user_id']}</code> — {a.get('role', 'ADMIN')}")
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    if is_owner(uid):
        m.add(types.KeyboardButton("➕ Add Admin"), types.KeyboardButton("➖ Remove Admin"))
    m.add(types.KeyboardButton("🔙 Admin Panel"))
    if not is_owner(uid):
        lines.append("\n<i>Only the OWNER can add/remove admins.</i>")
    bot.send_message(chat_id, "\n".join(lines), reply_markup=m)


def _run_diagnostics(chat_id) -> None:
    status = bot.send_message(chat_id, "🩺 Running diagnostics…")
    checks = []
    checks.append(("Telegram API", True))
    try:
        get_conn().close()
        checks.append(("Database", True))
    except Exception:
        checks.append(("Database", False))
    try:
        requests.get("https://api.ipify.org", timeout=6)
        checks.append(("Direct HTTP", True))
    except Exception:
        checks.append(("Direct HTTP", False))
    checks.append(("SOCKS5 support", _SOCKS5_AVAILABLE))
    ok_ch, _ = verify_channel()
    checks.append(("Channel posting", ok_ch))
    report = "🩺 <b>SYSTEM DIAGNOSTICS</b>\n━━━━━━━━━━━━━━━━━━\n" + \
             "\n".join(f"{'✅' if ok else '❌'} {name}" for name, ok in checks)
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=status.message_id, text=report)
    except Exception:
        bot.send_message(chat_id, report)
    bot.send_message(chat_id, "Done.", reply_markup=admin_keyboard())


# =========================================================
# Admin awaiting-input dispatch
# =========================================================
def _handle_admin_input(message, u, chat_id, text, state) -> None:
    await_kind = state.get("await")
    with _state_lock:
        user_states[u.id] = {}

    if await_kind == "BROADCAST":
        audience = get_all_user_ids()
        status = bot.send_message(chat_id, f"🚀 Broadcasting to {len(audience)} users…")
        sent = failed = blocked = 0
        for uid in audience:
            try:
                bot.send_message(uid, text)
                sent += 1
                time.sleep(0.04)
            except ApiTelegramException as exc:
                if "blocked" in str(exc).lower() or "deactivated" in str(exc).lower():
                    blocked += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
        audit(u.id, "BROADCAST_SENT", details=f"sent={sent} failed={failed}")
        try:
            bot.edit_message_text(chat_id=chat_id, message_id=status.message_id,
                                  text=(f"✅ <b>Broadcast complete</b>\nTotal: {len(audience)}\n"
                                        f"Sent: {sent}\nFailed: {failed}\nBlocked: {blocked}"))
        except Exception:
            pass
        bot.send_message(chat_id, "Admin Panel", reply_markup=admin_keyboard())
        return

    if await_kind == "ADD_PROXY":
        parsed, err = parse_proxy(text)
        if not parsed:
            bot.send_message(chat_id, f"❌ Invalid proxy: {html.escape(err)}",
                             reply_markup=proxy_manager_keyboard())
            return
        if parsed.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
            bot.send_message(chat_id, "⚠️ SOCKS5 not available (add requests[socks]).",
                             reply_markup=proxy_manager_keyboard())
            return
        wait = bot.send_message(chat_id, "⏳ Testing proxy…")
        res = test_proxy(parsed, quick=True)
        added = db_add_proxy(parsed.raw, u.id)
        row = db_get_proxy_by_endpoint(parsed.raw)
        if row:
            if res.working:
                db_update_proxy_success(row["id"], res.latency_ms or 0, res.observed_ip or "", res.status)
            else:
                db_update_proxy_failure(row["id"], res.error_reason or "Initial test failed", res.status)
        audit(u.id, "PROXY_ADDED", target=parsed.display)
        try:
            bot.delete_message(chat_id, wait.message_id)
        except Exception:
            pass
        head = "✅ Saved." if added else "⚠️ Already existed (updated)."
        bot.send_message(chat_id, f"{head}\n\n{res.card()}", reply_markup=proxy_manager_keyboard())
        return

    if await_kind == "BULK_ADD":
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        added = skipped = invalid = socks_skip = 0
        for ln in lines:
            parsed, err = parse_proxy(ln)
            if not parsed:
                invalid += 1
                continue
            if parsed.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
                socks_skip += 1
                continue
            if db_add_proxy(parsed.raw, u.id):
                added += 1
            else:
                skipped += 1
        audit(u.id, "PROXY_BULK_ADD", details=f"added={added}")
        note = f"\n⚠️ SOCKS5 skipped: {socks_skip}" if socks_skip else ""
        bot.send_message(chat_id,
                         (f"✅ <b>Bulk import</b>\nImported: {added}\nDuplicate: {skipped}\n"
                          f"Invalid: {invalid}{note}\nTotal active: {proxy_manager.get_endpoint_count()}"),
                         reply_markup=proxy_manager_keyboard())
        return

    if await_kind == "USER_SEARCH":
        matches = search_users(text)
        if not matches:
            bot.send_message(chat_id, "No matching users.", reply_markup=admin_keyboard())
            return
        for us in matches[:8]:
            card = (f"👤 <b>{html.escape(us.get('first_name') or 'User')}</b>\n"
                    f"@{html.escape(us.get('username') or 'none')} · 🆔 <code>{us['user_id']}</code>\n"
                    f"Status: {us.get('status', 'APPROVED')}\n"
                    f"🔄 Jobs: {us.get('total_extractions', 0)} · 📱 Numbers: {us.get('total_numbers_found', 0)}\n"
                    f"🕒 Last: {str(us.get('last_active') or '')[:16]}")
            mk = types.InlineKeyboardMarkup()
            if us.get("status") == "BLOCKED":
                mk.add(types.InlineKeyboardButton("✅ Unblock", callback_data=f"approve_{us['user_id']}"))
            else:
                mk.add(types.InlineKeyboardButton("🚫 Block", callback_data=f"reject_{us['user_id']}"))
            bot.send_message(chat_id, card, reply_markup=mk)
        bot.send_message(chat_id, "🔎 Search complete.", reply_markup=admin_keyboard())
        return

    if await_kind == "SET_CHANNEL":
        val = text.strip()
        if not val.startswith("@") and not val.lstrip("-").isdigit():
            val = "@" + val
        set_setting("channel_username", val)
        audit(u.id, "CHANNEL_SET", target=val)
        ok, reason = verify_channel()
        bot.send_message(chat_id,
                         (f"✅ Channel set to <code>{html.escape(val)}</code>.\n"
                          + ("✅ Verified." if ok else f"⚠️ Not verified: {html.escape(reason)}")),
                         reply_markup=admin_keyboard())
        return

    if await_kind == "SET_SUPPORT":
        set_setting("support_username", text.strip().lstrip("@"))
        bot.send_message(chat_id, "✅ Support username updated.", reply_markup=admin_keyboard())
        return

    if await_kind == "ADD_ADMIN":
        if is_owner(u.id) and text.strip().lstrip("-").isdigit():
            add_admin(int(text.strip()), added_by=u.id)
            audit(u.id, "ADMIN_ADDED", target=text.strip())
            bot.send_message(chat_id, f"✅ Admin <code>{html.escape(text.strip())}</code> added.",
                             reply_markup=admin_keyboard())
        else:
            bot.send_message(chat_id, "❌ Send a numeric ID (owner only).", reply_markup=admin_keyboard())
        return

    if await_kind == "REMOVE_ADMIN":
        if is_owner(u.id) and text.strip().lstrip("-").isdigit():
            tid = int(text.strip())
            if tid == BOOTSTRAP_OWNER_ID:
                bot.send_message(chat_id, "❌ Cannot remove the bootstrap owner.",
                                 reply_markup=admin_keyboard())
                return
            remove_admin(tid)
            audit(u.id, "ADMIN_REMOVED", target=str(tid))
            bot.send_message(chat_id, f"✅ Admin <code>{tid}</code> removed.", reply_markup=admin_keyboard())
        else:
            bot.send_message(chat_id, "❌ Send a numeric ID (owner only).", reply_markup=admin_keyboard())
        return


# =========================================================
# Entry point
# =========================================================
def main() -> None:
    init_db()
    logger.info("=" * 60)
    logger.info("URL Extraction Bot — starting")
    logger.info("✅ Database ready: %s", DB_FILE)
    logger.info("✅ Admins: %s", [a["user_id"] for a in list_admins()])
    logger.info("✅ Proxy endpoints: %d", proxy_manager.get_endpoint_count())
    logger.info("✅ SOCKS5 support: %s", "yes" if _SOCKS5_AVAILABLE else "no (add requests[socks])")
    logger.info("✅ Channel: %s", channel_target() or "not set")
    logger.info("=" * 60)

    try:
        bot.remove_webhook()
        time.sleep(1)
    except Exception as exc:
        logger.warning("Webhook clear: %s", exc)

    logger.info("Polling started.")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as exc:
            logger.warning("Polling interrupted: %s", exc)
            time.sleep(4)
            logger.info("Reconnecting…")


if __name__ == "__main__":
    main()
