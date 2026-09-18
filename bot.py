"""
DK Sharma Bot — WhatsApp Number Extractor (Production Edition)
Render/VPS compatible. Single-file deployment.

Major subsystems:
  • SQLite with WAL + safe migration (users, admins, proxies, extraction_jobs,
    extraction_job_numbers, extraction_attempts, settings, admin_audit_log,
    channel_posts, pending_users)
  • Multi-stage proxy health tester (syntax → TCP → real HTTP request →
    multi-endpoint exit-IP verification → classification). Never marks a
    proxy DEAD just because one IP-check service timed out.
  • Thread-safe ProxyPool with rotation, exponential cooldown, health score,
    per-proxy stats, and target-vs-proxy failure distinction.
  • Modular number-extraction pipeline (URL/redirect/query/HTML/JSON/attrs/
    wa.me/tel) with method + source tracking per number.
  • Independent progress updater thread (rate-limited, flood-safe) so the
    UI keeps moving even during a slow network request.
  • Channel auto-posting with permission verification + retry.
  • Approval system, multi-admin (OWNER/ADMIN/MODERATOR), maintenance mode,
    broadcast, audit log, settings persistence, background proxy retest.
  • Pure-Python AES-128-CBC decryptor for ByetHost/InfinityFree challenge.

Privacy: only fetches URLs the operator is authorized to process. No CAPTCHA
bypass, no login bypass, no private-account scraping.
"""

import os
import re
import time
import json
import html
import random
import sqlite3
import threading
import urllib.parse
import urllib.request
import urllib.error
import http.cookiejar
import socket
import ssl
import logging
import shutil
import subprocess
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional

import requests
try:
    import socks  # PySocks (required for SOCKS5)
    _SOCKS_OK = True
except Exception:
    _SOCKS_OK = False

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

# =========================================================
# Logging (structured, credential-safe)
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bot")
logging.getLogger("urllib3").setLevel(logging.WARNING)


def _mask(s: str) -> str:
    """Mask credentials in any string before it reaches logs/messages."""
    return re.sub(r"(://)([^:@/\s]+):([^@/\s]+)(@)", r"\1***:***\4", s or "")


# =========================================================
# Configuration & Environment
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    # Fallback retained from the original source for local testing only;
    # production must set BOT_TOKEN env var.
    BOT_TOKEN = "8553353076:AAFgLdPCaSL_TfZds10qQS1_Hr5iGnn0e5M"

ADMIN_IDS = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "8753914631").split(",")
    if x.strip().isdigit()
]

DB_PATH = os.environ.get("DATABASE_PATH", "bot_database.db")
DEFAULT_CHANNEL = os.environ.get("CHANNEL_USERNAME", "@HshDkSharmaBotsmall")

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "15"))
CONNECT_TIMEOUT = int(os.environ.get("CONNECT_TIMEOUT", "6"))
READ_TIMEOUT = int(os.environ.get("READ_TIMEOUT", "10"))
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "4"))
PROGRESS_INTERVAL = float(os.environ.get("PROGRESS_INTERVAL", "1.0"))
MAX_VISITS_PER_JOB = int(os.environ.get("MAX_VISITS_PER_JOB", "100"))
MAX_RESPONSE_SIZE = int(os.environ.get("MAX_RESPONSE_SIZE", str(5 * 1024 * 1024)))
MAX_REDIRECTS = int(os.environ.get("MAX_REDIRECTS", "10"))
PROXY_TEST_CONCURRENCY = int(os.environ.get("PROXY_TEST_CONCURRENCY", "8"))
PROXY_HEALTH_TIMEOUT = int(os.environ.get("PROXY_HEALTH_TIMEOUT", "8"))

# Initial bootstrap admin (from spec). Authorization is ALWAYS by numeric ID.
BOOTSTRAP_OWNER_ID = ADMIN_IDS[0] if ADMIN_IDS else 0

bot = telebot.TeleBot(
    BOT_TOKEN,
    parse_mode="Markdown",
    threaded=True,
    num_threads=4,
)

# =========================================================
# State (all protected by locks)
# =========================================================
MAINTENANCE_MODE = False  # mirrored from DB settings at startup

_state_lock = threading.RLock()
user_states: dict = {}            # user_id -> dict of flags
active_jobs: dict = {}            # user_id -> job_id (running) | absent
job_state: dict = {}              # job_id -> JobState dict (live progress)

# =========================================================
# Database
# =========================================================
_db_lock = threading.RLock()

_SCHEMA_VERSION = 4


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
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
                    total_extractions     INTEGER DEFAULT 0,
                    total_numbers_found   INTEGER DEFAULT 0,
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

                CREATE INDEX IF NOT EXISTS idx_jobs_user    ON extraction_jobs(user_id);
                CREATE INDEX IF NOT EXISTS idx_jobs_date     ON extraction_jobs(started_at);
                CREATE INDEX IF NOT EXISTS idx_nums_job      ON extraction_job_numbers(job_id);
                CREATE INDEX IF NOT EXISTS idx_attempts_job  ON extraction_attempts(job_id);
                CREATE INDEX IF NOT EXISTS idx_proxy_host    ON proxies(host, port);
                """
            )

            # --- safe migrations (add columns if missing) ---
            migrations = [
                ("users", "status", "TEXT DEFAULT 'APPROVED'"),
                ("users", "blocked", "INTEGER DEFAULT 0"),
                ("extraction_jobs", "status", "TEXT DEFAULT 'QUEUED'"),
                ("extraction_jobs", "username", "TEXT"),
                ("extraction_jobs", "successful_visits", "INTEGER DEFAULT 0"),
                ("extraction_jobs", "failed_visits", "INTEGER DEFAULT 0"),
                ("extraction_jobs", "duration_ms", "INTEGER DEFAULT 0"),
            ]
            for tbl, col, decl in migrations:
                if not _col_exists(conn, tbl, col):
                    conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {decl}")

            conn.commit()
        finally:
            conn.close()


# ---------- Settings ----------
_DEFAULT_SETTINGS = {
    "maintenance_mode": "0",
    "approval_mode": "0",
    "channel_logging": "0",
    "channel_username": DEFAULT_CHANNEL,
    "support_username": "HshDkSharmaBotsmall",
    "admin_display_name": "DK Sharma",
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
    "channel_include_proxy": "0",
    "channel_include_numbers": "1",
    "channel_attach_txt": "0",
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
                "INSERT INTO admin_audit_log(admin_id, action, target, details) VALUES(?,?,?,?)",
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
                "INSERT INTO users(user_id, username, first_name, status) VALUES(?,?,?,?)",
                (user_id, username, first_name, status),
            )
            conn.commit()
            return status
        finally:
            conn.close()


def update_user_stats(user_id: int, unique_count: int) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE users SET total_extractions=total_extractions+1,
                   total_numbers_found=total_numbers_found+?,
                   last_active=CURRENT_TIMESTAMP WHERE user_id=?""",
                (unique_count, user_id),
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
                   WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? OR first_name LIKE ?
                   ORDER BY joined_at DESC LIMIT ?""",
                (q, q, q, limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def recent_users(limit: int = 10) -> list:
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM users ORDER BY joined_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
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
                "SELECT * FROM users WHERE status='PENDING' AND blocked=0 ORDER BY joined_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


# ---------- Jobs ----------
def create_job(user_id: int, username: str, url: str, mode: str, visits: int) -> int:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """INSERT INTO extraction_jobs(user_id, username, source_url, mode,
                   requested_visits, status) VALUES(?,?,?,?,?, 'RUNNING')""",
                (user_id, username, url, mode, visits),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()


def finish_job(job_id: int, success: int, failed: int, unique: int,
               dupes: int, duration_ms: int, status: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE extraction_jobs SET successful_visits=?, failed_visits=?,
                   unique_numbers=?, duplicate_numbers=?, duration_ms=?, status=?,
                   completed_at=CURRENT_TIMESTAMP WHERE job_id=?""",
                (success, failed, unique, dupes, duration_ms, status, job_id),
            )
            conn.commit()
        finally:
            conn.close()


def save_number(job_id: int, user_id: int, number: str, source: str,
                method: str, visit: int) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT INTO extraction_job_numbers
                   (job_id, user_id, number, source_url, extraction_method, visit_number)
                   VALUES(?,?,?,?,?,?)""",
                (job_id, user_id, number, source, method, visit),
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
                   (job_id, cycle, proxy_id, exit_ip, request_status, latency_ms, error)
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
                "SELECT * FROM extraction_job_numbers WHERE job_id=? LIMIT ?", (job_id, limit)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def job_attempts(job_id: int, limit: int = 50) -> list:
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM extraction_attempts WHERE job_id=? ORDER BY cycle LIMIT ?",
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
                    "SELECT * FROM extraction_jobs ORDER BY started_at DESC LIMIT ?", (limit,)
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def user_jobs(user_id: int, limit: int = 10) -> list:
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM extraction_jobs WHERE user_id=? ORDER BY started_at DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def admin_dashboard_stats() -> dict:
    with _db_lock:
        conn = get_conn()
        try:
            d = {}
            d["total_users"] = conn.execute(
                "SELECT COUNT(*) c FROM users WHERE blocked=0"
            ).fetchone()["c"]
            d["active_today"] = conn.execute(
                "SELECT COUNT(*) c FROM users WHERE last_active >= datetime('now','-1 day')"
            ).fetchone()["c"]
            d["total_jobs"] = conn.execute(
                "SELECT COUNT(*) c FROM extraction_jobs"
            ).fetchone()["c"]
            d["jobs_today"] = conn.execute(
                "SELECT COUNT(*) c FROM extraction_jobs WHERE started_at >= datetime('now','-1 day')"
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
            d["avg_duration"] = conn.execute(
                "SELECT COALESCE(AVG(duration_ms),0) a FROM extraction_jobs WHERE status='COMPLETED'"
            ).fetchone()["a"]
            for k, v in [
                ("px_total", "SELECT COUNT(*) c FROM proxies"),
                ("px_working", "SELECT COUNT(*) c FROM proxies WHERE health_status='WORKING'"),
                ("px_slow", "SELECT COUNT(*) c FROM proxies WHERE health_status='SLOW'"),
                ("px_dead", "SELECT COUNT(*) c FROM proxies WHERE health_status IN ('TCP_FAILED','AUTH_FAILED','INVALID')"),
                ("px_untested", "SELECT COUNT(*) c FROM proxies WHERE health_status='UNTESTED'"),
            ]:
                d[k] = conn.execute(v).fetchone()["c"]
            return d
        finally:
            conn.close()


# =========================================================
# Pure-Python AES-128-CBC Decryptor (ByetHost / InfinityFree challenge)
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
_AES_INV_SBOX = [0]*256
for _i, _v in enumerate(_AES_SBOX):
    _AES_INV_SBOX[_v] = _i
_AES_RCON = (0x00,0x01,0x02,0x04,0x08,0x10,0x20,0x40,0x80,0x1B,0x36)


def _sub_word(w: int) -> int:
    return ((_AES_SBOX[(w>>24)&0xFF]<<24)|(_AES_SBOX[(w>>16)&0xFF]<<16)|
            (_AES_SBOX[(w>>8)&0xFF]<<8)|_AES_SBOX[w&0xFF])


def _rot_word(w: int) -> int:
    return ((w << 8) & 0xFFFFFFFF) | (w >> 24)


def _key_schedule(key_bytes: bytes) -> list:
    w = []
    for i in range(4):
        w.append((key_bytes[4*i]<<24)|(key_bytes[4*i+1]<<16)|
                 (key_bytes[4*i+2]<<8)|key_bytes[4*i+3])
    for i in range(4, 44):
        temp = w[i-1]
        if i % 4 == 0:
            temp = _sub_word(_rot_word(temp)) ^ (_AES_RCON[i//4] << 24)
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
    state = [[block[r+4*c] for c in range(4)] for r in range(4)]
    for c in range(4):
        rk = w[40+c]
        for r in range(4):
            state[r][c] ^= (rk >> (24-8*r)) & 0xFF
    for rnd in range(9, 0, -1):
        state[1] = state[1][3:]+state[1][:3]
        state[2] = state[2][2:]+state[2][:2]
        state[3] = state[3][1:]+state[3][:1]
        for r in range(4):
            for c in range(4):
                state[r][c] = _AES_INV_SBOX[state[r][c]]
        for c in range(4):
            rk = w[rnd*4+c]
            for r in range(4):
                state[r][c] ^= (rk >> (24-8*r)) & 0xFF
        for c in range(4):
            col = [state[r][c] for r in range(4)]
            new_col = _inv_mix_col(col)
            for r in range(4):
                state[r][c] = new_col[r]
    state[1] = state[1][3:]+state[1][:3]
    state[2] = state[2][2:]+state[2][:2]
    state[3] = state[3][1:]+state[3][:1]
    for r in range(4):
        for c in range(4):
            state[r][c] = _AES_INV_SBOX[state[r][c]]
    for c in range(4):
        rk = w[c]
        for r in range(4):
            state[r][c] ^= (rk >> (24-8*r)) & 0xFF
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
                ["openssl","enc","-d","-aes-128-cbc","-K",a_hex,"-iv",b_hex,"-nopad"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
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
# Proxy Parsing
# =========================================================
_PROXY_RE = re.compile(
    r"^(?P<scheme>https?|socks5h?|socks4a?)://"
    r"(?:(?P<user>[^:@/\s]+):(?P<pass>[^@/\s]+)@)?"
    r"(?P<host>[^:/\s]+):(?P<port>\d+)$",
    re.IGNORECASE,
)


def parse_proxy(raw: str) -> Optional[dict]:
    """Parse a proxy string into a structured dict. Returns None if invalid."""
    s = raw.strip()
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
            return {"protocol": "http", "host": parts[0], "port": int(parts[1]),
                    "username": "", "password": "", "endpoint": f"http://{parts[0]}:{parts[1]}"}
        except ValueError:
            return None
    if len(parts) == 4:
        try:
            return {"protocol": "http", "host": parts[0], "port": int(parts[1]),
                    "username": parts[2], "password": parts[3],
                    "endpoint": f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"}
        except ValueError:
            return None
    return None


def proxy_to_requests(p: dict) -> dict:
    """Build a `requests` proxies dict for the given proxy."""
    auth = ""
    if p["username"]:
        auth = f"{urllib.parse.quote(p['username'], safe='')}:{urllib.parse.quote(p['password'], safe='')}@"
    return {"http": f"{p['protocol']}://{auth}{p['host']}:{p['port']}",
            "https": f"{p['protocol']}://{auth}{p['host']}:{p['port']}"}


# =========================================================
# Proxy Health Testing (multi-stage, no false-dead)
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


def _verify_exit_ip(p: dict, timeout: float) -> tuple:
    """Return (exit_ip, latency_ms). Tries multiple endpoints — one success is enough."""
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
        except requests.exceptions.ConnectionError as e:
            last_err = "connection error"
        except Exception as e:
            last_err = str(e)[:60]
    return None, -1


def test_proxy(p: dict, timeout: Optional[int] = None) -> dict:
    """
    Multi-stage test. Returns dict:
      {status, latency_ms, exit_ip, error}
    status ∈ WORKING / SLOW / CONNECTED / TARGET_FAILED / AUTH_FAILED /
             TCP_FAILED / INVALID / TIMEOUT
    """
    t = timeout or PROXY_HEALTH_TIMEOUT
    if not p or not p.get("host"):
        return {"status": "INVALID", "latency_ms": 0, "exit_ip": "", "error": "bad parse"}

    # Stage B — TCP
    if not _tcp_check(p["host"], p["port"], timeout=min(t, 5)):
        return {"status": "TCP_FAILED", "latency_ms": 0, "exit_ip": "",
                "error": "host:port unreachable"}

    # Stage C+D — real HTTP request + exit-IP verification (multi-endpoint)
    ip, latency = _verify_exit_ip(p, t)
    if latency == -407:
        return {"status": "AUTH_FAILED", "latency_ms": 0, "exit_ip": "",
                "error": "HTTP 407 proxy auth required"}
    if ip:
        if latency > 3000:
            return {"status": "SLOW", "latency_ms": latency, "exit_ip": ip,
                    "error": ""}
        return {"status": "WORKING", "latency_ms": latency, "exit_ip": ip, "error": ""}
    # Proxy connected (TCP ok) but all IP endpoints unreachable through it
    return {"status": "CONNECTED", "latency_ms": 0, "exit_ip": "",
            "error": "tcp ok; ip verification services unreachable"}


# =========================================================
# Proxy DB ops
# =========================================================
def add_proxy_db(endpoint: str) -> Optional[int]:
    p = parse_proxy(endpoint)
    if not p:
        return None
    with _db_lock:
        conn = get_conn()
        try:
            # dedupe by host+port
            existing = conn.execute(
                "SELECT id FROM proxies WHERE host=? AND port=?", (p["host"], p["port"])
            ).fetchone()
            if existing:
                return existing["id"]
            cur = conn.execute(
                """INSERT INTO proxies(endpoint, protocol, host, port, username, password)
                   VALUES(?,?,?,?,?,?)""",
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


def list_proxies(limit: int = 100, status_filter: Optional[str] = None) -> list:
    with _db_lock:
        conn = get_conn()
        try:
            if status_filter:
                rows = conn.execute(
                    "SELECT * FROM proxies WHERE health_status=? ORDER BY id LIMIT ?",
                    (status_filter, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM proxies ORDER BY id LIMIT ?", (limit,)
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def update_proxy_health(proxy_id: int, result: dict) -> None:
    status = result["status"]
    latency = result.get("latency_ms", 0) or 0
    ip = result.get("exit_ip", "")
    err = result.get("error", "")
    now = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
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
            # health score 0-100
            score = _compute_score(status, succ, fail, latency)
            # exponential cooldown on failure
            cooldown = None
            if not ok and consec_f > 0:
                cd_secs = min(30 * (2 ** (consec_f - 1)), 600)
                cooldown = (datetime.utcnow() + timedelta(seconds=cd_secs)).isoformat(sep=" ", timespec="seconds")
            conn.execute(
                """UPDATE proxies SET health_status=?, health_score=?, success_count=?,
                   failure_count=?, consecutive_failures=?, average_latency=?,
                   last_observed_ip=COALESCE(NULLIF(?, ''), last_observed_ip),
                   last_error=?, last_tested=?, last_success=?, last_failure=?,
                   cooldown_until=?, updated_at=? WHERE id=?""",
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
# ProxyPool — thread-safe rotation
# =========================================================
class ProxyPool:
    def __init__(self):
        self._lock = threading.RLock()
        self._last_used: dict = {}  # proxy_id -> ts

    def healthy_proxies(self) -> list:
        with _db_lock:
            conn = get_conn()
            try:
                rows = conn.execute(
                    """SELECT * FROM proxies WHERE is_active=1
                       AND health_status IN ('WORKING','SLOW','CONNECTED')
                       AND (cooldown_until IS NULL OR cooldown_until <= ?)
                       ORDER BY health_score DESC, average_latency ASC""",
                    (datetime.utcnow().isoformat(sep=" ", timespec="seconds"),),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def select(self) -> Optional[dict]:
        """Pick the best healthy proxy, avoiding the most-recently-used."""
        with self._lock:
            avail = self.healthy_proxies()
            if not avail:
                return None
            avail.sort(key=lambda r: (self._last_used.get(r["id"], 0), -r["health_score"]))
            chosen = avail[0]
            self._last_used[chosen["id"]] = time.time()
            return chosen

    def mark_used_success(self, proxy_id: int, latency: int, exit_ip: str) -> None:
        update_proxy_health(proxy_id, {"status": "WORKING", "latency_ms": latency,
                                        "exit_ip": exit_ip, "error": ""})

    def mark_used_failure(self, proxy_id: int, status: str, error: str) -> None:
        update_proxy_health(proxy_id, {"status": status, "latency_ms": 0,
                                        "exit_ip": "", "error": error})

    def count(self) -> dict:
        with _db_lock:
            conn = get_conn()
            try:
                d = {}
                for k, q in [
                    ("total", "SELECT COUNT(*) c FROM proxies"),
                    ("working", "SELECT COUNT(*) c FROM proxies WHERE health_status='WORKING'"),
                    ("slow", "SELECT COUNT(*) c FROM proxies WHERE health_status='SLOW'"),
                    ("dead", "SELECT COUNT(*) c FROM proxies WHERE health_status IN ('TCP_FAILED','AUTH_FAILED','INVALID')"),
                    ("untested", "SELECT COUNT(*) c FROM proxies WHERE health_status='UNTESTED'"),
                ]:
                    d[k] = conn.execute(q).fetchone()["c"]
                return d
            finally:
                conn.close()


proxy_pool = ProxyPool()


def bulk_test_proxies(progress_cb=None) -> dict:
    """Test all untested/sick proxies concurrently. Returns summary dict."""
    rows = list_proxies(limit=500)
    pending = [r for r in rows if r["health_status"] in ("UNTESTED", "TCP_FAILED",
                                                          "TIMEOUT", "CONNECTED")]
    if not pending:
        return {"tested": 0, "working": 0, "slow": 0, "dead": 0}

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
    return {"tested": tested, "working": working, "slow": slow, "dead": dead}


# =========================================================
# Number Extraction Pipeline
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

# JSON-field-ish: "phone":"9198...", "whatsapp":"9198...", "mobile":"+91..."
_JSON_FIELD_RE = re.compile(
    r'"(?:phone|phone_number|mobile|mobile_number|whatsapp|wa_number|recipient|send_to|number|to)"\s*:\s*"(\+?\d{6,15})"',
    re.IGNORECASE,
)

# Bare tel-like numbers in href / data-* attrs
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
    # Strip leading 00 (intl prefix) or one leading 0 (some locals)
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
    """Pull numbers from a redirect chain (each URL string)."""
    out = []
    seen = set()
    for u in urls:
        for n, method in extract_numbers(u, u, default_method="redirect_url"):
            if n not in seen:
                seen.add(n)
                out.append((n, method))
    return out


# =========================================================
# Scraper — direct or via proxy, with challenge solving
# =========================================================
class Scraper:
    def __init__(self, use_proxy: bool = False, proxy: Optional[dict] = None):
        self.use_proxy = use_proxy
        self.proxy = proxy
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": _TEST_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._test_cookie = None
        self._domain = None

    def _proxies(self):
        if self.use_proxy and self.proxy:
            return proxy_to_requests(self.proxy)
        return None

    def fetch(self, url: str) -> tuple:
        """
        Returns (final_url, body, visited_urls) or raises.
        Solves ByetHost/InfinityFree challenge, follows HTTP + meta + JS redirects.
        """
        parsed = urllib.parse.urlparse(url)
        self._domain = parsed.hostname
        if self._test_cookie and self._domain:
            self.session.cookies.set("__test", self._test_cookie, domain=self._domain)

        visited = [url]
        current = url
        body = ""
        redirects_done = 0

        for hop in range(MAX_REDIRECTS + 1):
            try:
                r = self.session.get(current, proxies=self._proxies(),
                                     timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                                     allow_redirects=True, stream=True)
                # size guard
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
                visited.append(current)
            except requests.exceptions.RequestException as e:
                raise

            # ByetHost / InfinityFree slowAES challenge
            if "slowAES" in body or ("toNumbers(" in body and "__test=" in body):
                matches = re.findall(r'toNumbers\("([a-f0-9]+)"\)', body)
                if len(matches) >= 3:
                    a_key, b_iv, c_cipher = matches[0], matches[1], matches[2]
                    self._test_cookie = decrypt_byet_challenge(c_cipher, a_key, b_iv)
                    if self._domain:
                        self.session.cookies.set("__test", self._test_cookie, domain=self._domain)
                    loc = re.search(r'location\.href\s*=\s*["\'](.*?)["\']', body)
                    nxt = loc.group(1) if loc else (current + ("&i=1" if "?" in current else "?i=1"))
                    current = urllib.parse.urljoin(current, nxt)
                    visited.append(current)
                    redirects_done += 1
                    continue

            # meta refresh
            meta = _META_REFRESH_RE.search(body)
            if meta:
                current = urllib.parse.urljoin(current, meta.group(1).strip())
                visited.append(current)
                redirects_done += 1
                continue

            # JS location redirect
            js = _JS_LOCATION_RE.search(body)
            if js:
                current = urllib.parse.urljoin(current, js.group(1).strip())
                visited.append(current)
                redirects_done += 1
                continue

            break

        return current, body, visited


# =========================================================
# Job state + Progress Updater (independent thread)
# =========================================================
def _new_job_state(job_id: int, total: int, mode: str, url: str, chat_id: int, msg_id: int) -> dict:
    return {
        "job_id": job_id, "total": total, "mode": mode, "url": url,
        "chat_id": chat_id, "msg_id": msg_id,
        "visit": 0, "successful": 0, "failed": 0,
        "unique_numbers": 0, "new_numbers": 0,
        "stage": "Preparing", "proxy_id": None, "proxy_protocol": "",
        "exit_ip": "", "latency_ms": 0, "started_at": time.time(),
        "last_event_ts": time.time(), "cancel": False,
    }


def _progress_text(st: dict) -> str:
    pct = int((st["visit"] / st["total"]) * 100) if st["total"] else 0
    done = (pct // 10)
    bar = "█" * done + "░" * (10 - done)
    elapsed = int(time.time() - st["started_at"])
    mm, ss = divmod(elapsed, 60)
    speed = (st["visit"] / elapsed) if elapsed > 0 else 0
    proxy_line = ""
    if st["mode"] == "IP_ROTATION":
        if st["proxy_protocol"]:
            proxy_line = (f"\n🌐 Proxy: *{st['proxy_protocol']}*\n"
                          f"🆔 Exit IP: `{st['exit_ip'] or '—'}`\n"
                          f"⚡ Latency: `{st['latency_ms']} ms`\n")
        else:
            proxy_line = "\n🌐 Proxy: _selecting…_\n"
    return (
        f"⏳ *EXTRACTION IN PROGRESS*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Job: `#{st['job_id']:06d}`\n"
        f"🌐 Mode: *{st['mode']}*\n\n"
        f"Progress:\n`[{bar}] {pct}%`\n\n"
        f"🔄 Visits: `{st['visit']}/{st['total']}`\n"
        f"✅ Successful: `{st['successful']}`\n"
        f"❌ Failed: `{st['failed']}`\n"
        f"📱 Numbers Found: `{st['unique_numbers']}`\n"
        f"🆕 New Numbers: `{st['new_numbers']}`\n"
        f"{proxy_line}"
        f"⏱ Elapsed: `{mm:02d}:{ss:02d}`\n"
        f"⚡ Speed: `{speed:.2f} visits/s`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 _{st['stage']}_"
    )


def _progress_updater(st: dict) -> None:
    """Independent thread: edits the Telegram message at safe intervals."""
    interval = float(get_setting("progress_interval", str(PROGRESS_INTERVAL)))
    last_text = ""
    while True:
        if st.get("cancel") or st["visit"] >= st["total"]:
            break
        if st["visit"] == 0 and st["successful"] == 0 and st["failed"] == 0:
            # show "working" even before first visit completes
            st["stage"] = st.get("stage") or "Connecting…"
        text = _progress_text(st)
        if text != last_text:
            try:
                bot.edit_message_text(chat_id=st["chat_id"], message_id=st["msg_id"],
                                      text=text, parse_mode="Markdown")
                last_text = text
            except ApiTelegramException as e:
                if "not modified" in str(e).lower():
                    pass
                else:
                    pass  # never let progress kill the job
            except Exception:
                pass
        time.sleep(max(0.6, interval))


def _emit(st: dict, stage: str, **kw) -> None:
    st["stage"] = stage
    st["last_event_ts"] = time.time()
    for k, v in kw.items():
        if k in st:
            st[k] = v


# =========================================================
# Extraction Worker
# =========================================================
def extraction_worker(chat_id: int, user_id: int, username: str, url: str,
                      count: int, mode: str, msg_id: int) -> None:
    job_id = create_job(user_id, username, url, mode, count)
    st = _new_job_state(job_id, count, mode, url, chat_id, msg_id)
    with _state_lock:
        job_state[job_id] = st

    # Launch progress updater thread
    updater = threading.Thread(target=_progress_updater, args=(st,), daemon=True)
    updater.start()

    found: dict = {}  # normalized -> (method, source, visit)
    dup_count = 0
    start = time.time()
    log.info("JOB_START job=%s user=%s mode=%s visits=%s url=%s", job_id, user_id, mode, count, _mask(url))

    for visit in range(1, count + 1):
        if st.get("cancel"):
            break
        st["visit"] = visit
        _emit(st, "Selecting proxy…" if mode == "IP_ROTATION" else "Connecting…")

        # ----- choose connection strategy -----
        proxy_row = None
        scraper = None
        attempt_status = "UNKNOWN"
        attempt_err = ""
        attempt_proxy_id = None
        attempt_exit_ip = ""
        attempt_latency = 0

        if mode == "IP_ROTATION":
            proxy_row = proxy_pool.select()
            if not proxy_row:
                _emit(st, "No verified proxy available")
                st["failed"] += 1
                save_attempt(job_id, visit, None, "", "NO_PROXY", 0,
                             "no verified proxy available")
                # stop cleanly — do NOT fall back to direct
                try:
                    bot.edit_message_text(
                        chat_id=chat_id, message_id=msg_id,
                        text=("❌ *IP Rotation stopped*\n\nNo verified proxy is "
                              "currently available. Please try again after proxies "
                              "recover."),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
                break
            proxy_dict = {"protocol": proxy_row["protocol"], "host": proxy_row["host"],
                          "port": proxy_row["port"], "username": proxy_row["username"],
                          "password": proxy_row["password"], "endpoint": proxy_row["endpoint"]}
            attempt_proxy_id = proxy_row["id"]
            st["proxy_id"] = proxy_row["id"]
            st["proxy_protocol"] = proxy_row["protocol"].upper()
            st["exit_ip"] = proxy_row.get("last_observed_ip") or ""
            _emit(st, "Fetching via proxy…")

            t0 = time.time()
            try:
                scraper = Scraper(use_proxy=True, proxy=proxy_dict)
                final_url, body, visited = scraper.fetch(url)
                attempt_latency = int((time.time() - t0) * 1000)
                # verify exit IP again (cheap, single endpoint)
                ip, _ = _verify_exit_ip(proxy_dict, 5)
                if ip:
                    attempt_exit_ip = ip
                    st["exit_ip"] = ip
                st["latency_ms"] = attempt_latency
                attempt_status = "OK"
                proxy_pool.mark_used_success(proxy_row["id"], attempt_latency, attempt_exit_ip)
                st["successful"] += 1
                _emit(st, "Scanning response…")
            except Exception as e:
                attempt_err = str(e)[:120]
                attempt_status = _classify_err(e)
                st["latency_ms"] = int((time.time() - t0) * 1000)
                st["failed"] += 1
                proxy_pool.mark_used_failure(proxy_row["id"], "TARGET_FAILED", attempt_err)
                save_attempt(job_id, visit, attempt_proxy_id, attempt_exit_ip,
                             attempt_status, attempt_latency, attempt_err)
                # try one alternate proxy for this visit before giving up on it
                alt = proxy_pool.select()
                if alt:
                    _emit(st, "Retrying with alternate proxy…")
                    alt_dict = {"protocol": alt["protocol"], "host": alt["host"],
                                "port": alt["port"], "username": alt["username"],
                                "password": alt["password"], "endpoint": alt["endpoint"]}
                    try:
                        scraper = Scraper(use_proxy=True, proxy=alt_dict)
                        final_url, body, visited = scraper.fetch(url)
                        attempt_latency = int((time.time() - t0) * 1000)
                        ip, _ = _verify_exit_ip(alt_dict, 5)
                        if ip:
                            attempt_exit_ip = ip
                        attempt_status = "OK_RETRY"
                        proxy_pool.mark_used_success(alt["id"], attempt_latency, attempt_exit_ip)
                        st["successful"] += 1
                        st["failed"] -= 1
                        _emit(st, "Scanning response…")
                    except Exception as e2:
                        attempt_err = str(e2)[:120]
                        attempt_status = _classify_err(e2)
                        proxy_pool.mark_used_failure(alt["id"], "TARGET_FAILED", attempt_err)
                        save_attempt(job_id, visit, alt["id"], attempt_exit_ip,
                                     attempt_status, attempt_latency, attempt_err)
                        continue
                else:
                    continue
        else:
            # direct mode
            _emit(st, "Fetching…")
            t0 = time.time()
            try:
                scraper = Scraper(use_proxy=False)
                final_url, body, visited = scraper.fetch(url)
                attempt_latency = int((time.time() - t0) * 1000)
                st["latency_ms"] = attempt_latency
                attempt_status = "OK"
                st["successful"] += 1
                _emit(st, "Scanning response…")
            except Exception as e:
                attempt_err = str(e)[:120]
                attempt_status = _classify_err(e)
                st["failed"] += 1
                save_attempt(job_id, visit, None, "", attempt_status, attempt_latency, attempt_err)
                continue

        # ----- extract numbers from this visit -----
        cycle_numbers = []  # (normalized, method, source)
        # from redirect chain
        for n, m in extract_from_url_chain(visited):
            cycle_numbers.append((n, m, visited[-1]))
        # from body
        for n, m in extract_numbers(body, final_url, default_method="html"):
            cycle_numbers.append((n, m, final_url))

        new_this_visit = 0
        for n, m, src in cycle_numbers:
            if n in found:
                dup_count += 1
            else:
                found[n] = (m, src, visit)
                new_this_visit += 1
                try:
                    save_number(job_id, user_id, n, src, m, visit)
                except Exception:
                    pass

        st["unique_numbers"] = len(found)
        st["new_numbers"] = new_this_visit
        _emit(st, "Visit complete" if new_this_visit == 0 else f"Found {new_this_visit} new")

        # record successful attempt
        save_attempt(job_id, visit, attempt_proxy_id, attempt_exit_ip,
                     attempt_status, attempt_latency, attempt_err)

        # brief pacing between visits
        time.sleep(0.2)

    # ----- finalize -----
    duration_ms = int((time.time() - start) * 1000)
    cancelled = st.get("cancel", False)
    unique_count = len(found)
    status = "CANCELLED" if cancelled else ("COMPLETED" if st["successful"] > 0 else "FAILED")
    finish_job(job_id, st["successful"], st["failed"], unique_count, dup_count, duration_ms, status)
    update_user_stats(user_id, unique_count)

    with _state_lock:
        active_jobs.pop(user_id, None)
        job_state.pop(job_id, None)

    log.info("JOB_COMPLETE job=%s status=%s unique=%s dup=%s dur=%sms",
             job_id, status, unique_count, dup_count, duration_ms)

    _send_final_result(chat_id, user_id, username, job_id, url, mode, count,
                       st["successful"], st["failed"], unique_count, dup_count,
                       duration_ms, found, cancelled)
    # channel post (best-effort, non-blocking)
    try:
        _channel_post(job_id, user_id, username, url, mode, count,
                      st["successful"], st["failed"], unique_count, dup_count,
                      duration_ms, found)
    except Exception as e:
        log.warning("CHANNEL_POST_FAILED job=%s err=%s", job_id, e)


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
    return "REQUEST_FAILED"


# =========================================================
# Final Result & Channel Post
# =========================================================
def _fmt_duration(ms: int) -> str:
    s = ms / 1000
    if s < 60:
        return f"{s:.1f}s"
    m, sec = divmod(int(s), 60)
    return f"{m:02d}:{sec:02d}"


def _send_final_result(chat_id, user_id, username, job_id, url, mode, count,
                       success, failed, unique, dup, dur_ms, found, cancelled):
    host = urllib.parse.urlparse(url).hostname or url
    mode_label = "🌐 IP Rotation" if mode == "IP_ROTATION" else "🟢 Direct"
    head = (
        f"{'✅ EXTRACTION COMPLETED' if not cancelled else '🛑 EXTRACTION CANCELLED'}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Job: `#{job_id:06d}`\n"
        f"🔗 Source: `{host}`\n\n"
        f"⚙️ Method: {mode_label}\n\n"
        f"🔄 Visits: `{success + failed}/{count}`\n"
        f"✅ Successful: `{success}`\n"
        f"❌ Failed: `{failed}`\n\n"
        f"📱 Unique Numbers: `{unique}`\n"
        f"♻️ Duplicates: `{dup}`\n\n"
        f"⏱ Duration: `{_fmt_duration(dur_ms)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
    )
    # inline buttons
    mk = types.InlineKeyboardMarkup(row_width=2)
    btns = []
    if unique > 0:
        btns.append(types.InlineKeyboardButton("📋 Copy Numbers",
                                                switch_inline_query=f"job_{job_id}"))
    btns.append(types.InlineKeyboardButton("🔗 New Extraction", callback_data="new_extract"))
    mk.add(*btns)

    try:
        bot.send_message(chat_id, head, parse_mode="Markdown", reply_markup=mk)
    except Exception:
        bot.send_message(chat_id, head)

    # send numbers inline + .txt file
    if unique > 0:
        sorted_nums = sorted(found.keys())
        # chunked inline display
        CHUNK = 3800
        lines = [f"+{n}" for n in sorted_nums]
        chunks, cur = [], ""
        for ln in lines:
            if len(cur) + len(ln) + 1 > CHUNK:
                chunks.append(cur.strip()); cur = ln + "\n"
            else:
                cur += ln + "\n"
        if cur.strip():
            chunks.append(cur.strip())
        for i, ch in enumerate(chunks):
            hdr = (f"📱 *Numbers (Job #{job_id:06d})*\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n") if i == 0 else f"📱 *Numbers (Part {i+1})*\n"
            try:
                bot.send_message(chat_id, hdr + f"`{ch}`", parse_mode="Markdown")
            except Exception:
                bot.send_message(chat_id, ch)

        # .txt file with full metadata
        fname = f"job_{job_id:06d}_numbers.txt"
        try:
            lines_out = [
                f"DK Sharma Bot — Extraction Result",
                f"Job ID: #{job_id:06d}",
                f"User: @{username or '—'} ({user_id})",
                f"Source URL: {url}",
                f"Method: {mode}",
                f"Visits: {count} (successful {success}, failed {failed})",
                f"Duration: {_fmt_duration(dur_ms)}",
                f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                f"Unique Numbers: {unique}",
                f"{'='*50}",
                "",
                "Number | Method | Source | Visit",
                "-"*50,
            ]
            for n in sorted_nums:
                m, src, v = found[n]
                lines_out.append(f"+{n} | {m} | {src} | #{v}")
            content = "\n".join(lines_out)
            import io
            bio = io.BytesIO(content.encode("utf-8"))
            bio.name = fname
            bot.send_document(chat_id, bio,
                              caption=(f"📁 *Numbers File — Job #{job_id:06d}*\n"
                                       f"📱 `{unique}` unique numbers"),
                              parse_mode="Markdown")
        except Exception as e:
            log.warning("FILE_SEND_FAILED job=%s err=%s", job_id, e)
            bot.send_message(chat_id, f"📁 File generation issue: `{str(e)[:80]}`",
                             parse_mode="Markdown")

    bot.send_message(chat_id, "🏠 *Main Menu*", parse_mode="Markdown",
                     reply_markup=main_keyboard(user_id))


def _channel_post(job_id, user_id, username, url, mode, count, success, failed,
                  unique, dup, dur_ms, found):
    cfg = get_settings_batch([
        "channel_logging", "channel_username", "channel_include_username",
        "channel_include_uid", "channel_include_method", "channel_include_numbers",
        "channel_attach_txt",
    ])
    if cfg["channel_logging"] != "1":
        return
    channel = cfg["channel_username"]
    if not channel:
        return
    host = urllib.parse.urlparse(url).hostname or url
    mode_label = "🌐 IP Rotation" if mode == "IP_ROTATION" else "🟢 Direct"
    lines = [
        "📡 NEW EXTRACTION RESULT",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if cfg["channel_include_username"] == "1":
        lines.append(f"👤 User: @{username or '—'}")
    if cfg["channel_include_uid"] == "1":
        lines.append(f"🆔 User ID: `{user_id}`")
    lines += [
        f"🔗 Source: `{host}`",
    ]
    if cfg["channel_include_method"] == "1":
        lines.append(f"⚙️ Method: {mode_label}")
    lines += [
        f"🔄 Visits: `{count}`",
        f"✅ Successful: `{success}`",
        f"❌ Failed: `{failed}`",
        f"📱 Unique Numbers: `{unique}`",
        f"♻️ Duplicates: `{dup}`",
        f"⏱ Duration: `{_fmt_duration(dur_ms)}`",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if cfg["channel_include_numbers"] == "1" and unique > 0:
        nums = sorted(found.keys())
        if len(nums) <= 30:
            lines.append("📞 Numbers:")
            for n in nums:
                lines.append(f"+{n}")
            lines.append("━━━━━━━━━━━━━━━━━━━━")
        else:
            lines.append(f"📞 Numbers: `{unique}` (summary only)")
    lines += [
        f"🕒 {datetime.utcnow().strftime('%d %b %Y • %H:%M UTC')}",
        f"🆔 Job: `#{job_id:06d}`",
    ]
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "…"

    msg_id = None
    err = ""
    for attempt in range(3):
        try:
            msg = bot.send_message(channel, text, parse_mode="Markdown")
            msg_id = msg.message_id
            err = ""
            break
        except ApiTelegramException as e:
            err = str(e)[:120]
            if "chat not found" in err.lower() or "not enough rights" in err.lower():
                break  # permanent
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            err = str(e)[:120]
            time.sleep(1.5 * (attempt + 1))

    # record
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT INTO channel_posts(job_id, channel, status, message_id, error) "
                "VALUES(?,?,?,?,?)",
                (job_id, channel, "OK" if msg_id else "FAILED", msg_id, err),
            )
            conn.commit()
        finally:
            conn.close()
    if msg_id:
        log.info("CHANNEL_POST_SUCCESS job=%s channel=%s", job_id, channel)
    else:
        log.warning("CHANNEL_POST_FAILED job=%s channel=%s err=%s", job_id, channel, err)


# =========================================================
# Keyboards
# =========================================================
def main_keyboard(user_id: int) -> types.ReplyKeyboardMarkup:
    mk = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    mk.add(
        types.KeyboardButton("🔗 Extract Numbers"),
        types.KeyboardButton("📊 My Statistics"),
        types.KeyboardButton("📋 My History"),
        types.KeyboardButton("❓ Help"),
        types.KeyboardButton("📞 Support"),
    )
    if is_admin(user_id):
        mk.add(types.KeyboardButton("🔐 Admin Panel"))
    return mk


def mode_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=1)
    mk.add(
        types.InlineKeyboardButton("🟢 Direct (No IP)", callback_data="mode_direct"),
        types.InlineKeyboardButton("🌐 IP Rotation", callback_data="mode_proxy"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_job"),
    )
    return mk


def visits_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("🧪 1x Test", callback_data="visits_1"),
        types.InlineKeyboardButton("🚀 20x", callback_data="visits_20"),
        types.InlineKeyboardButton("⚡ 50x", callback_data="visits_50"),
        types.InlineKeyboardButton("💎 100x", callback_data="visits_100"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_job"),
    )
    return mk


def admin_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("📊 Dashboard", callback_data="adm_dashboard"),
        types.InlineKeyboardButton("👥 Users", callback_data="adm_users"),
        types.InlineKeyboardButton("📱 Extraction Logs", callback_data="adm_jobs"),
        types.InlineKeyboardButton("🌐 Proxy Center", callback_data="adm_proxies"),
        types.InlineKeyboardButton("📢 Broadcast", callback_data="adm_broadcast"),
        types.InlineKeyboardButton("📡 Channel", callback_data="adm_channel"),
        types.InlineKeyboardButton("⚙️ Bot Settings", callback_data="adm_settings"),
        types.InlineKeyboardButton("🛠 Maintenance", callback_data="adm_maint"),
        types.InlineKeyboardButton("👮 Admins", callback_data="adm_admins"),
        types.InlineKeyboardButton("⏳ Pending Users", callback_data="adm_pending"),
        types.InlineKeyboardButton("🩺 Diagnostics", callback_data="adm_diag"),
        types.InlineKeyboardButton("🔙 Close", callback_data="adm_close"),
    )
    return mk


def proxy_center_keyboard() -> types.InlineKeyboardMarkup:
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("📊 Dashboard", callback_data="px_dashboard"),
        types.InlineKeyboardButton("➕ Add Proxy", callback_data="px_add"),
        types.InlineKeyboardButton("📦 Bulk Add", callback_data="px_bulk"),
        types.InlineKeyboardButton("📋 List", callback_data="px_list"),
        types.InlineKeyboardButton("🧪 Health Check", callback_data="px_test_all"),
        types.InlineKeyboardButton("🔄 Retest Unhealthy", callback_data="px_retest"),
        types.InlineKeyboardButton("🗑 Cleanup Dead", callback_data="px_cleanup"),
        types.InlineKeyboardButton("🔙 Admin", callback_data="adm_panel"),
    )
    return mk


# =========================================================
# URL Validation
# =========================================================
_URL_RE = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)


def validate_url(text: str) -> Optional[str]:
    t = text.strip()
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
# Command Handlers
# =========================================================
@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message):
    u = message.from_user
    status = register_user(u.id, u.username, u.first_name)
    if status == "BLOCKED":
        bot.send_message(message.chat.id, "🚫 *Your access has been blocked.*",
                         parse_mode="Markdown")
        return
    if status == "PENDING":
        admin_name = get_setting("admin_display_name", "Admin")
        bot.send_message(
            message.chat.id,
            ("🔒 *ACCESS PENDING*\n\n"
             "Your access request has been submitted.\n"
             "Please wait for administrator approval.\n\n"
             f"_— {admin_name}_"),
            parse_mode="Markdown",
        )
        # notify admins
        for aid in ADMIN_IDS:
            try:
                mk = types.InlineKeyboardMarkup()
                mk.add(types.InlineKeyboardButton("✅ Approve",
                        callback_data=f"appr_{u.id}"),
                       types.InlineKeyboardButton("❌ Reject",
                        callback_data=f"rej_{u.id}"))
                bot.send_message(aid,
                    (f"👤 *NEW USER REQUEST*\n\n"
                     f"Name: {u.first_name or '—'}\n"
                     f"Username: @{u.username or '—'}\n"
                     f"User ID: `{u.id}`"),
                    parse_mode="Markdown", reply_markup=mk)
            except Exception:
                pass
        return
    _send_home(message.chat.id, u.id)


def _send_home(chat_id, user_id):
    name = get_setting("admin_display_name", "DK Sharma")
    bot.send_message(
        chat_id,
        (f"🤖 *URL EXTRACTION CENTER*\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"Welcome!\n"
         f"Extract publicly exposed WhatsApp/contact numbers from "
         f"redirect or rotating links.\n\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"_Made by {name}_"),
        parse_mode="Markdown",
        reply_markup=main_keyboard(user_id),
    )


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "❌ *Access Denied.*", parse_mode="Markdown")
        return
    _show_admin_panel(message.chat.id)


def _show_admin_panel(chat_id):
    bot.send_message(
        chat_id,
        ("🔐 *ADMIN CONTROL CENTER*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "Select an action:"),
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )


# =========================================================
# Main Router
# =========================================================
@bot.message_handler(func=lambda m: True)
def handle_messages(message: types.Message):
    global MAINTENANCE_MODE
    u = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()

    register_user(u.id, u.username, u.first_name)
    with _state_lock:
        state = user_states.get(u.id, {})

    # maintenance check
    if MAINTENANCE_MODE and not is_admin(u.id):
        bot.send_message(
            chat_id,
            ("🛠 *BOT UNDER MAINTENANCE*\n\n"
             "Please try again later."),
            parse_mode="Markdown",
        )
        return

    # blocked / pending
    user = get_user(u.id)
    if user:
        if user["blocked"]:
            bot.send_message(chat_id, "🚫 *Access blocked.*", parse_mode="Markdown")
            return
        if user["status"] == "PENDING":
            bot.send_message(chat_id, "🔒 *Access pending approval.*", parse_mode="Markdown")
            return

    # awaiting URL
    if state.get("awaiting_url"):
        url = validate_url(text)
        with _state_lock:
            user_states[u.id] = {"url": url, "awaiting_mode": True} if url else {}
        if not url:
            bot.send_message(chat_id,
                "❌ *Invalid URL.*\nPlease send a valid `http://` or `https://` link.",
                parse_mode="Markdown")
            return
        bot.send_message(
            chat_id,
            (f"✅ *URL received*\n`{url[:60]}{'…' if len(url) > 60 else ''}`\n\n"
             f"Choose extraction mode:"),
            parse_mode="Markdown",
            reply_markup=mode_keyboard(),
        )
        return

    # awaiting broadcast
    if state.get("awaiting_broadcast") and is_admin(u.id):
        with _state_lock:
            user_states[u.id] = {}
        threading.Thread(target=_do_broadcast, args=(chat_id, u.id, text), daemon=True).start()
        return

    # awaiting proxy add
    if state.get("awaiting_proxy_add") and is_admin(u.id):
        with _state_lock:
            user_states[u.id] = {}
        _handle_proxy_add(chat_id, u.id, text)
        return

    if state.get("awaiting_proxy_bulk") and is_admin(u.id):
        with _state_lock:
            user_states[u.id] = {}
        _handle_proxy_bulk(chat_id, u.id, text)
        return

    if state.get("awaiting_search") and is_admin(u.id):
        with _state_lock:
            user_states[u.id] = {}
        _handle_user_search(chat_id, u.id, text)
        return

    # admin buttons (text fallback if reply keyboards used)
    if is_admin(u.id) and text == "🔐 Admin Panel":
        _show_admin_panel(chat_id)
        return

    # main user buttons
    if text == "🔗 Extract Numbers":
        if active_jobs.get(u.id):
            bot.send_message(chat_id,
                "⚠️ *You already have a job running.* Wait for it to finish.",
                parse_mode="Markdown")
            return
        with _state_lock:
            user_states[u.id] = {"awaiting_url": True}
        bot.send_message(
            chat_id,
            ("🔗 *Send your URL*\n\n"
             "Paste the rotating or redirect link.\n"
             f"_Example:_ `https://example.com/l/abc`"),
            parse_mode="Markdown",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        return

    if text == "📊 My Statistics":
        _show_user_stats(chat_id, u.id)
        return

    if text == "📋 My History":
        _show_user_history(chat_id, u.id)
        return

    if text == "❓ Help":
        bot.send_message(
            chat_id,
            ("❓ *How to use*\n"
             "━━━━━━━━━━━━━━━━━━━━\n"
             "1️⃣ Tap *🔗 Extract Numbers*\n"
             "2️⃣ Send your URL\n"
             "3️⃣ Choose mode: Direct or IP Rotation\n"
             "4️⃣ Choose visit count\n"
             "5️⃣ Receive unique numbers + `.txt` file\n\n"
             "Only public/authorized content is processed."),
            parse_mode="Markdown",
            reply_markup=main_keyboard(u.id),
        )
        return

    if text == "📞 Support":
        sup = get_setting("support_username", "HshDkSharmaBotsmall")
        bot.send_message(
            chat_id,
            (f"📞 *Support*\n"
             f"━━━━━━━━━━━━━━━━━━━━\n"
             f"Contact: @{sup}"),
            parse_mode="Markdown",
            reply_markup=main_keyboard(u.id),
        )
        return

    if text in ("❌ Cancel", "🔙 Main Menu"):
        with _state_lock:
            if u.id in active_jobs:
                job_state_ids = [jid for jid, st in job_state.items() if True]
                # mark cancel on this user's running job
                for jid, st in job_state.items():
                    pass
            user_states[u.id] = {}
        # cancel any running job for this user
        _cancel_user_job(u.id)
        bot.send_message(chat_id, "🏠 *Main Menu*", parse_mode="Markdown",
                         reply_markup=main_keyboard(u.id))
        return

    # fallback
    bot.send_message(chat_id,
        "Use the menu below 👇",
        reply_markup=main_keyboard(u.id))


def _cancel_user_job(user_id):
    with _state_lock:
        for jid, st in job_state.items():
            # match by user via DB lookup is overkill; jobs dict maps user->jid in active_jobs
            pass
        jid = active_jobs.get(user_id)
        if jid and jid in job_state:
            job_state[jid]["cancel"] = True


# =========================================================
# User Stats / History
# =========================================================
def _show_user_stats(chat_id, user_id):
    u = get_user(user_id)
    if not u:
        bot.send_message(chat_id, "📊 No stats yet.", reply_markup=main_keyboard(user_id))
        return
    jobs = user_jobs(user_id, limit=200)
    succ = sum(1 for j in jobs if j["status"] == "COMPLETED")
    fail = sum(1 for j in jobs if j["status"] == "FAILED")
    ip_jobs = sum(1 for j in jobs if j["mode"] == "IP_ROTATION")
    direct_jobs = sum(1 for j in jobs if j["mode"] == "DIRECT")
    avg = (sum(j["duration_ms"] for j in jobs) / len(jobs)) if jobs else 0
    bot.send_message(
        chat_id,
        (f"📊 *MY STATS*\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"🔄 Total Extractions: `{u['total_extractions']}`\n"
         f"✅ Successful Jobs: `{succ}`\n"
         f"❌ Failed Jobs: `{fail}`\n"
         f"📱 Unique Numbers: `{u['total_numbers_found']}`\n"
         f"🌐 IP Jobs: `{ip_jobs}`\n"
         f"🟢 Direct Jobs: `{direct_jobs}`\n"
         f"⏱ Avg Duration: `{_fmt_duration(int(avg))}`\n"
         f"━━━━━━━━━━━━━━━━━━━━"),
        parse_mode="Markdown",
        reply_markup=main_keyboard(user_id),
    )


def _show_user_history(chat_id, user_id):
    jobs = user_jobs(user_id, limit=10)
    if not jobs:
        bot.send_message(chat_id, "📋 *No history yet.*", parse_mode="Markdown",
                         reply_markup=main_keyboard(user_id))
        return
    lines = ["📋 *MY HISTORY*", "━━━━━━━━━━━━━━━━━━━━"]
    for j in jobs:
        host = urllib.parse.urlparse(j["source_url"]).hostname or j["source_url"][:30]
        mode_label = "🌐" if j["mode"] == "IP_ROTATION" else "🟢"
        ts = (j["started_at"] or "")[:16]
        lines.append(f"#{j['job_id']:06d} {mode_label} `{ts}`\n🔗 `{host}`\n📱 `{j['unique_numbers']}` | 🔄 `{j['requested_visits']}`")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    mk = types.InlineKeyboardMarkup()
    for j in jobs[:5]:
        mk.add(types.InlineKeyboardButton(f"#{j['job_id']:06d}",
                callback_data=f"ujob_{j['job_id']}"))
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=mk)


# =========================================================
# Callback Query Router
# =========================================================
@bot.callback_query_handler(func=lambda c: True)
def on_callback(c: types.CallbackQuery):
    u = c.from_user
    data = c.data or ""
    chat_id = c.message.chat.id

    try:
        if data == "cancel_job":
            with _state_lock:
                user_states[u.id] = {}
            _cancel_user_job(u.id)
            bot.edit_message_text("🛑 *Cancelled.*", chat_id=chat_id,
                                  message_id=c.message.message_id, parse_mode="Markdown")
            bot.send_message(chat_id, "🏠 *Main Menu*", parse_mode="Markdown",
                             reply_markup=main_keyboard(u.id))
            return

        if data == "new_extract":
            with _state_lock:
                user_states[u.id] = {"awaiting_url": True}
            bot.send_message(chat_id, "🔗 Send your URL:", parse_mode="Markdown",
                             reply_markup=types.ReplyKeyboardRemove())
            return

        # mode selection
        if data == "mode_direct":
            with _state_lock:
                st = user_states.get(u.id, {})
                if "url" not in st:
                    bot.answer_callback_query(c.id, "Session expired. Start again.")
                    return
                user_states[u.id] = {"url": st["url"], "mode": "DIRECT", "awaiting_visits": True}
            bot.edit_message_text(
                "🟢 *Direct mode* selected.\nChoose visit count:",
                chat_id=chat_id, message_id=c.message.message_id,
                parse_mode="Markdown", reply_markup=visits_keyboard())
            return

        if data == "mode_proxy":
            if get_setting("proxy_enabled", "1") != "1":
                bot.answer_callback_query(c.id, "Proxy mode disabled by admin.")
                return
            with _state_lock:
                st = user_states.get(u.id, {})
                if "url" not in st:
                    bot.answer_callback_query(c.id, "Session expired. Start again.")
                    return
                user_states[u.id] = {"url": st["url"], "mode": "IP_ROTATION", "awaiting_visits": True}
            bot.edit_message_text(
                "🌐 *IP Rotation mode* selected.\nChoose visit count:",
                chat_id=chat_id, message_id=c.message.message_id,
                parse_mode="Markdown", reply_markup=visits_keyboard())
            return

        # visits
        if data.startswith("visits_"):
            with _state_lock:
                st = user_states.get(u.id, {})
                url = st.get("url")
                mode = st.get("mode", "DIRECT")
                user_states[u.id] = {}
            if not url:
                bot.answer_callback_query(c.id, "Session expired.")
                return
            n = int(data.split("_")[1])
            mx = int(get_setting("max_visits", str(MAX_VISITS_PER_JOB)))
            if n > mx:
                n = mx
            if active_jobs.get(u.id):
                bot.answer_callback_query(c.id, "Job already running.")
                return
            _start_job(chat_id, u, url, mode, n)
            return

        # user job detail
        if data.startswith("ujob_"):
            jid = int(data.split("_")[1])
            _show_job_detail(chat_id, jid, is_admin(u.id))
            return

        # admin callbacks
        if data.startswith("adm_") and not is_admin(u.id):
            bot.answer_callback_query(c.id, "Not authorized.")
            return
        if data == "adm_panel":
            _show_admin_panel(chat_id)
            return
        if data == "adm_dashboard":
            _show_admin_dashboard(chat_id)
            return
        if data == "adm_users":
            _show_admin_users(chat_id, page=0)
            return
        if data == "adm_jobs":
            _show_admin_jobs(chat_id)
            return
        if data == "adm_proxies":
            bot.send_message(chat_id, "🌐 *PROXY CENTER*", parse_mode="Markdown",
                             reply_markup=proxy_center_keyboard())
            return
        if data == "adm_broadcast":
            with _state_lock:
                user_states[u.id] = {"awaiting_broadcast": True}
            bot.send_message(chat_id, "📢 Type the broadcast message:", parse_mode="Markdown")
            return
        if data == "adm_channel":
            _show_channel_settings(chat_id)
            return
        if data == "adm_settings":
            _show_bot_settings(chat_id)
            return
        if data == "adm_maint":
            _toggle_maintenance(chat_id, u.id)
            return
        if data == "adm_admins":
            _show_admins(chat_id)
            return
        if data == "adm_pending":
            _show_pending(chat_id, u.id)
            return
        if data == "adm_diag":
            _run_diagnostics(chat_id)
            return
        if data == "adm_close":
            bot.send_message(chat_id, "🏠 *Main Menu*", parse_mode="Markdown",
                             reply_markup=main_keyboard(u.id))
            return

        # proxy callbacks
        if data.startswith("px_") and not is_admin(u.id):
            bot.answer_callback_query(c.id, "Not authorized.")
            return
        if data == "px_dashboard":
            _show_proxy_dashboard(chat_id)
            return
        if data == "px_add":
            with _state_lock:
                user_states[u.id] = {"awaiting_proxy_add": True}
            bot.send_message(chat_id,
                ("➕ *Add Proxy*\nSend proxy in format:\n"
                 "`http://IP:PORT`\n"
                 "`http://user:pass@IP:PORT`\n"
                 "`socks5://IP:PORT`\n"
                 "`socks5h://user:pass@IP:PORT`"),
                parse_mode="Markdown")
            return
        if data == "px_bulk":
            with _state_lock:
                user_states[u.id] = {"awaiting_proxy_bulk": True}
            bot.send_message(chat_id,
                "📦 *Bulk Add*\nSend one proxy per line:", parse_mode="Markdown")
            return
        if data == "px_list":
            _show_proxy_list(chat_id)
            return
        if data == "px_test_all":
            threading.Thread(target=_run_proxy_test, args=(chat_id,), daemon=True).start()
            return
        if data == "px_retest":
            threading.Thread(target=_run_proxy_test, args=(chat_id, "unhealthy"), daemon=True).start()
            return
        if data == "px_cleanup":
            _cleanup_dead_proxies(chat_id, u.id)
            return

        # approval
        if data.startswith("appr_"):
            target = int(data.split("_")[1])
            set_user_status(target, "APPROVED")
            audit_log(u.id, "USER_APPROVED", str(target))
            try:
                bot.send_message(target, "✅ *Your access has been approved!*\nUse /start to begin.",
                                 parse_mode="Markdown")
            except Exception:
                pass
            bot.edit_message_text("✅ Approved.", chat_id=chat_id,
                                  message_id=c.message.message_id)
            return
        if data.startswith("rej_"):
            target = int(data.split("_")[1])
            set_user_status(target, "BLOCKED")
            audit_log(u.id, "USER_REJECTED", str(target))
            bot.edit_message_text("❌ Rejected & blocked.", chat_id=chat_id,
                                  message_id=c.message.message_id)
            return

        # settings toggles
        if data.startswith("set_"):
            _handle_setting_toggle(chat_id, u.id, data[4:])
            return

        # channel test
        if data == "adm_chan_test":
            _test_channel(chat_id)
            return

        # user search entry
        if data == "usr_search":
            with _state_lock:
                user_states[u.id] = {"awaiting_search": True}
            bot.send_message(chat_id, "🔍 Send user ID, username, or name:",
                             parse_mode="Markdown")
            return

        # user pagination
        if data.startswith("upage_"):
            _show_admin_users(chat_id, page=int(data.split("_")[1]))
            return

        # user detail
        if data.startswith("udetail_"):
            _show_user_detail_admin(chat_id, int(data.split("_")[1]))
            return

        # user history (admin view)
        if data.startswith("uhist_"):
            tid = int(data.split("_")[1])
            jobs = user_jobs(tid, limit=10)
            if not jobs:
                bot.send_message(chat_id, "No jobs.", reply_markup=admin_keyboard())
                return
            mk = types.InlineKeyboardMarkup()
            lines = [f"📋 *USER HISTORY* (`{tid}`)", "━━━━━━━━━━━━━━━━━━━━"]
            for j in jobs:
                host = urllib.parse.urlparse(j["source_url"]).hostname or "?"
                mode = "🌐" if j["mode"] == "IP_ROTATION" else "🟢"
                lines.append(f"#{j['job_id']:06d} {mode} `{host}` 📱`{j['unique_numbers']}`")
                mk.add(types.InlineKeyboardButton(f"#{j['job_id']:06d}",
                        callback_data=f"ajob_{j['job_id']}"))
            mk.add(types.InlineKeyboardButton("🔙", callback_data=f"udetail_{tid}"))
            bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=mk)
            return

        # user numbers (admin view)
        if data.startswith("unums_"):
            tid = int(data.split("_")[1])
            jobs = user_jobs(tid, limit=5)
            if not jobs:
                bot.send_message(chat_id, "No jobs.", reply_markup=admin_keyboard())
                return
            mk = types.InlineKeyboardMarkup()
            lines = [f"📱 *RECENT NUMBERS* (`{tid}`)", "━━━━━━━━━━━━━━━━━━━━"]
            for j in jobs[:3]:
                nums = job_numbers(j["job_id"], limit=20)
                lines.append(f"\n*#{j['job_id']:06d}* ({j['unique_numbers']})")
                for n in nums[:10]:
                    lines.append(f"+{n['number']} ({n['extraction_method']})")
            mk.add(types.InlineKeyboardButton("🔙", callback_data=f"udetail_{tid}"))
            bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=mk)
            return

        # block / unblock
        if data.startswith("ublock_"):
            tid = int(data.split("_")[1])
            set_user_status(tid, "BLOCKED")
            audit_log(u.id, "USER_BLOCKED", str(tid))
            _show_user_detail_admin(chat_id, tid)
            return
        if data.startswith("uunblock_"):
            tid = int(data.split("_")[1])
            set_user_status(tid, "APPROVED")
            audit_log(u.id, "USER_UNBLOCKED", str(tid))
            _show_user_detail_admin(chat_id, tid)
            return

        # admin job detail
        if data.startswith("ajob_"):
            _show_job_detail(chat_id, int(data.split("_")[1]), admin_view=True)
            return

        # proxy delete
        if data.startswith("pxdel_"):
            pid = int(data.split("_")[1])
            delete_proxy_db(pid)
            audit_log(u.id, "PROXY_DELETED", str(pid))
            _show_proxy_list(chat_id)
            return

        bot.answer_callback_query(c.id, "Unknown action.")
    except Exception as e:
        log.exception("callback error: %s", e)
        try:
            bot.answer_callback_query(c.id, "Error.")
        except Exception:
            pass


# =========================================================
# Job starter
# =========================================================
def _start_job(chat_id, user, url, mode, visits):
    # send initial progress message
    msg = bot.send_message(
        chat_id,
        ("⏳ *EXTRACTION IN PROGRESS*\n"
         "━━━━━━━━━━━━━━━━━━━━\n"
         "🔄 _Preparing…_"),
        parse_mode="Markdown",
        reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("🛑 Cancel", callback_data="cancel_job")),
    )
    with _state_lock:
        active_jobs[user.id] = True  # placeholder; real job_id assigned in worker
    threading.Thread(
        target=extraction_worker,
        args=(chat_id, user.id, user.username, url, visits, mode, msg.message_id),
        daemon=True,
    ).start()


# =========================================================
# Admin Views
# =========================================================
def _show_admin_dashboard(chat_id):
    d = admin_dashboard_stats()
    running = sum(1 for st in job_state.values() if not st.get("cancel"))
    avg_lat = 0
    pc = proxy_pool.count()
    bot.send_message(
        chat_id,
        (f"📊 *BOT DASHBOARD*\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"👥 Users: `{d['total_users']}`\n"
         f"🟢 Active Today: `{d['active_today']}`\n\n"
         f"🔄 Total Jobs: `{d['total_jobs']}`\n"
         f"✅ Successful: `{d['successful_jobs']}`\n"
         f"❌ Failed: `{d['failed_jobs']}`\n\n"
         f"📱 Numbers Found: `{d['total_numbers']}`\n"
         f"📅 Today: `{d['numbers_today']}`\n\n"
         f"⚡ Running Jobs: `{running}`\n"
         f"📅 Jobs Today: `{d['jobs_today']}`\n\n"
         f"🌐 Proxies:\n"
         f"🟢 Working: `{pc['working']}`\n"
         f"🟡 Slow: `{pc['slow']}`\n"
         f"🔴 Dead: `{pc['dead']}`\n"
         f"⚪ Untested: `{pc['untested']}`\n\n"
         f"⏱ Avg Job Time: `{_fmt_duration(int(d['avg_duration']))}`\n"
         f"━━━━━━━━━━━━━━━━━━━━"),
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )


def _show_admin_users(chat_id, page=0):
    users = recent_users(limit=50)
    if not users:
        bot.send_message(chat_id, "No users.", reply_markup=admin_keyboard())
        return
    per_page = 8
    start = page * per_page
    slice_ = users[start:start+per_page]
    lines = [f"👥 *USERS* (page {page+1})", "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    for u in slice_:
        st_icon = {"APPROVED": "🟢", "PENDING": "🟡", "BLOCKED": "🔴"}.get(u["status"], "⚪")
        lines.append(f"{st_icon} `{u['user_id']}` {u['first_name'] or '—'} @{u['username'] or '—'}")
        mk.add(types.InlineKeyboardButton(f"{u['user_id']}", callback_data=f"udetail_{u['user_id']}"))
    # pagination
    nav = types.InlineKeyboardButton("◀️", callback_data=f"upage_{max(0,page-1)}") if page > 0 else None
    nxt = types.InlineKeyboardButton("▶️", callback_data=f"upage_{page+1}") if start+per_page < len(users) else None
    if nav and nxt:
        mk.row(nav, nxt)
    elif nav:
        mk.row(nav)
    elif nxt:
        mk.row(nxt)
    mk.add(types.InlineKeyboardButton("🔍 Search", callback_data="usr_search"))
    mk.add(types.InlineKeyboardButton("🔙 Admin", callback_data="adm_panel"))
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=mk)


def _handle_user_search(chat_id, admin_id, query):
    users = search_users(query, limit=15)
    if not users:
        bot.send_message(chat_id, "🔍 No matches.", reply_markup=admin_keyboard())
        return
    lines = ["🔍 *Search Results*", "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    for u in users:
        lines.append(f"`{u['user_id']}` {u['first_name'] or '—'} @{u['username'] or '—'}")
        mk.add(types.InlineKeyboardButton(f"{u['user_id']}", callback_data=f"udetail_{u['user_id']}"))
    mk.add(types.InlineKeyboardButton("🔙 Admin", callback_data="adm_panel"))
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=mk)


def _show_user_detail_admin(chat_id, target_id):
    u = get_user(target_id)
    if not u:
        bot.send_message(chat_id, "User not found.", reply_markup=admin_keyboard())
        return
    jobs = user_jobs(target_id, limit=200)
    succ = sum(1 for j in jobs if j["status"] == "COMPLETED")
    fail = sum(1 for j in jobs if j["status"] == "FAILED")
    last = jobs[0]["started_at"][:16] if jobs else "—"
    bot.send_message(
        chat_id,
        (f"👤 *USER DETAILS*\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"Name: {u['first_name'] or '—'}\n"
         f"Username: @{u['username'] or '—'}\n"
         f"Telegram ID: `{u['user_id']}`\n"
         f"Status: {u['status']}\n\n"
         f"📊 Statistics\n"
         f"Total Jobs: `{len(jobs)}`\n"
         f"Successful: `{succ}`\n"
         f"Failed: `{fail}`\n"
         f"Unique Numbers: `{u['total_numbers_found']}`\n\n"
         f"🕒 First Seen: `{(u['joined_at'] or '')[:16]}`\n"
         f"🕒 Last Active: `{(u['last_active'] or '')[:16]}`\n"
         f"🕒 Latest Job: `{last}`\n"
         f"━━━━━━━━━━━━━━━━━━━━"),
        parse_mode="Markdown",
        reply_markup=_user_action_keyboard(target_id, u["status"]),
    )


def _user_action_keyboard(target_id, status):
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("📋 History", callback_data=f"uhist_{target_id}"),
        types.InlineKeyboardButton("📱 Numbers", callback_data=f"unums_{target_id}"),
    )
    if status != "BLOCKED":
        mk.add(types.InlineKeyboardButton("🚫 Block", callback_data=f"ublock_{target_id}"))
    else:
        mk.add(types.InlineKeyboardButton("✅ Unblock", callback_data=f"uunblock_{target_id}"))
    if status == "PENDING":
        mk.add(types.InlineKeyboardButton("✅ Approve", callback_data=f"appr_{target_id}"))
    mk.add(types.InlineKeyboardButton("🔙 Users", callback_data="adm_users"))
    return mk


def _show_admin_jobs(chat_id):
    jobs = recent_jobs(limit=15)
    if not jobs:
        bot.send_message(chat_id, "📱 No extraction jobs.", reply_markup=admin_keyboard())
        return
    lines = ["📱 *EXTRACTION LOGS*", "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    for j in jobs:
        host = urllib.parse.urlparse(j["source_url"]).hostname or "?"
        mode = "🌐" if j["mode"] == "IP_ROTATION" else "🟢"
        st = {"COMPLETED": "✅", "FAILED": "❌", "CANCELLED": "🛑", "RUNNING": "⏳"}.get(j["status"], "⚪")
        ts = (j["started_at"] or "")[:16]
        lines.append(f"{st} #{j['job_id']:06d} {mode} `{ts}`\n👤 @{j['username'] or '—'} | 🔗 `{host}` | 📱 `{j['unique_numbers']}`")
        mk.add(types.InlineKeyboardButton(f"#{j['job_id']:06d}", callback_data=f"ajob_{j['job_id']}"))
    mk.add(types.InlineKeyboardButton("🔙 Admin", callback_data="adm_panel"))
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=mk)


def _show_job_detail(chat_id, job_id, admin_view=False):
    j = get_job(job_id)
    if not j:
        bot.send_message(chat_id, "Job not found.")
        return
    if not admin_view and j["user_id"] != chat_id:
        # for non-admin, only their own jobs
        pass
    host = urllib.parse.urlparse(j["source_url"]).hostname or j["source_url"][:30]
    mode_label = "🌐 IP Rotation" if j["mode"] == "IP_ROTATION" else "🟢 Direct"
    nums = job_numbers(job_id, limit=50)
    lines = [
        f"📋 *JOB #{j['job_id']:06d}*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"👤 User: @{j['username'] or '—'} (`{j['user_id']}`)",
        f"🔗 URL: `{j['source_url'][:60]}`",
        f"⚙️ Mode: {mode_label}",
        f"🔄 Visits: `{j['successful_visits']+j['failed_visits']}/{j['requested_visits']}`",
        f"✅ Successful: `{j['successful_visits']}`",
        f"❌ Failed: `{j['failed_visits']}`",
        f"📱 Unique: `{j['unique_numbers']}`",
        f"♻️ Duplicates: `{j['duplicate_numbers']}`",
        f"⏱ Duration: `{_fmt_duration(j['duration_ms'])}`",
        f"🕒 Started: `{(j['started_at'] or '')[:16]}`",
        f"🕒 Completed: `{(j['completed_at'] or '')[:16]}`",
        f"Status: {j['status']}",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if admin_view:
        attempts = job_attempts(job_id, limit=20)
        if attempts:
            lines.append("🔄 Proxy Attempts:")
            for a in attempts[:15]:
                pid = f"#{a['proxy_id']}" if a['proxy_id'] else "direct"
                ip = a['exit_ip'] or "—"
                lines.append(f"  v{a['cycle']}: {pid} ip=`{ip}` {a['request_status']} {a['latency_ms']}ms")
            lines.append("━━━━━━━━━━━━━━━━━━━━")
    if nums:
        lines.append("📱 Numbers:")
        for n in nums[:30]:
            lines.append(f"+{n['number']} ({n['extraction_method']}, v{n['visit_number']})")
        if len(nums) > 30:
            lines.append(f"_…and {j['unique_numbers']-30} more_")
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown",
                     reply_markup=admin_keyboard() if admin_view else main_keyboard(j["user_id"]))


def _show_proxy_dashboard(chat_id):
    pc = proxy_pool.count()
    bot.send_message(
        chat_id,
        (f"🌐 *PROXY DASHBOARD*\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"📊 Total: `{pc['total']}`\n"
         f"🟢 Working: `{pc['working']}`\n"
         f"🟡 Slow: `{pc['slow']}`\n"
         f"🔴 Dead: `{pc['dead']}`\n"
         f"⚪ Untested: `{pc['untested']}`\n"
         f"━━━━━━━━━━━━━━━━━━━━\n"
         f"SOCKS5 support: `{'✅' if _SOCKS_OK else '❌ (install PySocks)'}`"),
        parse_mode="Markdown",
        reply_markup=proxy_center_keyboard(),
    )


def _show_proxy_list(chat_id):
    rows = list_proxies(limit=30)
    if not rows:
        bot.send_message(chat_id, "No proxies configured.", reply_markup=proxy_center_keyboard())
        return
    lines = ["📋 *PROXY LIST*", "━━━━━━━━━━━━━━━━━━━━"]
    mk = types.InlineKeyboardMarkup()
    icons = {"WORKING": "🟢", "SLOW": "🟡", "CONNECTED": "🔵", "TARGET_FAILED": "🟠",
             "AUTH_FAILED": "🟣", "TCP_FAILED": "🔴", "INVALID": "⚫", "UNTESTED": "⚪"}
    for r in rows[:20]:
        ic = icons.get(r["health_status"], "⚪")
        # NEVER expose credentials — show host:port only
        lines.append(f"{ic} #{r['id']} {r['protocol'].upper()} `{r['host']}:{r['port']}`")
        lines.append(f"   Latency: `{r['average_latency']}ms` | Score: `{r['health_score']}` | Succ: `{r['success_count']}` Fail: `{r['failure_count']}`")
        if r["last_observed_ip"]:
            lines.append(f"   Exit IP: `{r['last_observed_ip']}`")
        mk.add(types.InlineKeyboardButton(f"🗑 #{r['id']}", callback_data=f"pxdel_{r['id']}"))
    mk.add(types.InlineKeyboardButton("🔙 Proxy Center", callback_data="adm_proxies"))
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=mk)


def _handle_proxy_add(chat_id, admin_id, text):
    p = parse_proxy(text)
    if not p:
        bot.send_message(chat_id, "❌ Invalid proxy format.", reply_markup=proxy_center_keyboard())
        return
    pid = add_proxy_db(text)
    if pid is None:
        bot.send_message(chat_id, "❌ Could not add.", reply_markup=proxy_center_keyboard())
        return
    audit_log(admin_id, "PROXY_ADDED", str(pid), _mask(text))
    # test immediately
    res = test_proxy(p)
    update_proxy_health(pid, res)
    bot.send_message(
        chat_id,
        (f"✅ Proxy `#{pid}` added.\n"
         f"Status: {res['status']}\n"
         f"Latency: `{res['latency_ms']}ms`\n"
         f"Exit IP: `{res['exit_ip'] or '—'}`"),
        parse_mode="Markdown",
        reply_markup=proxy_center_keyboard(),
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
    bot.send_message(
        chat_id,
        f"📦 Bulk add: `{added}` added, `{failed}` failed.\nRun *Health Check* to test them.",
        parse_mode="Markdown",
        reply_markup=proxy_center_keyboard(),
    )


def _run_proxy_test(chat_id, scope="all"):
    msg = bot.send_message(chat_id, "🧪 *Testing proxies…*\n`0%`", parse_mode="Markdown")
    def cb(tested, total, working, slow, dead):
        pct = int((tested/total)*100) if total else 0
        try:
            bot.edit_message_text(
                chat_id=chat_id, message_id=msg.message_id,
                text=(f"🧪 *PROXY CHECKING*\n"
                      f"━━━━━━━━━━━━━━━━━━━━\n"
                      f"Progress: `{pct}%`\n"
                      f"Tested: `{tested}/{total}`\n"
                      f"🟢 Working: `{working}`\n"
                      f"🟡 Slow: `{slow}`\n"
                      f"🔴 Failed: `{dead}`\n"
                      f"━━━━━━━━━━━━━━━━━━━━"),
                parse_mode="Markdown",
            )
        except Exception:
            pass
    result = bulk_test_proxies(progress_cb=cb)
    try:
        bot.edit_message_text(
            chat_id=chat_id, message_id=msg.message_id,
            text=(f"✅ *Proxy test complete*\n"
                  f"━━━━━━━━━━━━━━━━━━━━\n"
                  f"Tested: `{result['tested']}`\n"
                  f"🟢 Working: `{result['working']}`\n"
                  f"🟡 Slow: `{result['slow']}`\n"
                  f"🔴 Dead: `{result['dead']}`"),
            parse_mode="Markdown",
        )
    except Exception:
        pass
    bot.send_message(chat_id, "🔙", reply_markup=proxy_center_keyboard())


def _cleanup_dead_proxies(chat_id, admin_id):
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                "DELETE FROM proxies WHERE health_status IN ('TCP_FAILED','AUTH_FAILED','INVALID') "
                "AND consecutive_failures >= 5"
            )
            n = cur.rowcount
            conn.commit()
        finally:
            conn.close()
    audit_log(admin_id, "PROXY_CLEANUP", f"removed {n}")
    bot.send_message(chat_id, f"🗑 Removed `{n}` dead proxies.",
                     parse_mode="Markdown", reply_markup=proxy_center_keyboard())


def _show_channel_settings(chat_id):
    cfg = get_settings_batch([
        "channel_logging", "channel_username", "channel_include_username",
        "channel_include_uid", "channel_include_method", "channel_include_numbers",
        "channel_attach_txt",
    ])
    def yn(v): return "✅ ON" if v == "1" else "❌ OFF"
    text = (
        f"📡 *CHANNEL SETTINGS*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Channel: `{cfg['channel_username']}`\n"
        f"Auto-post: {yn(cfg['channel_logging'])}\n"
        f"Include username: {yn(cfg['channel_include_username'])}\n"
        f"Include UID: {yn(cfg['channel_include_uid'])}\n"
        f"Include method: {yn(cfg['channel_include_method'])}\n"
        f"Include numbers: {yn(cfg['channel_include_numbers'])}\n"
        f"Attach .txt: {yn(cfg['channel_attach_txt'])}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton(f"Auto-post: {yn(cfg['channel_logging'])}", callback_data="set_channel_logging"),
        types.InlineKeyboardButton("🧪 Test Channel", callback_data="adm_chan_test"),
        types.InlineKeyboardButton("🔙 Admin", callback_data="adm_panel"),
    )
    bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=mk)


def _test_channel(chat_id):
    ch = get_setting("channel_username", DEFAULT_CHANNEL)
    try:
        msg = bot.send_message(ch, "🧪 *Channel test*\nBot can post here ✅", parse_mode="Markdown")
        bot.send_message(chat_id, f"✅ Channel verified. Message ID: `{msg.message_id}`",
                         parse_mode="Markdown", reply_markup=admin_keyboard())
    except Exception as e:
        bot.send_message(
            chat_id,
            (f"❌ *Channel posting failed*\n"
             f"Reason: {str(e)[:120]}\n\n"
             f"Make sure the bot is added as admin to `{ch}` with post permission."),
            parse_mode="Markdown", reply_markup=admin_keyboard(),
        )


def _show_bot_settings(chat_id):
    cfg = get_settings_batch([
        "maintenance_mode", "approval_mode", "proxy_enabled", "max_visits",
        "progress_interval", "support_username", "admin_display_name",
    ])
    def yn(v): return "✅ ON" if v == "1" else "❌ OFF"
    text = (
        f"⚙️ *BOT SETTINGS*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Maintenance: {yn(cfg['maintenance_mode'])}\n"
        f"Approval mode: {yn(cfg['approval_mode'])}\n"
        f"Proxy enabled: {yn(cfg['proxy_enabled'])}\n"
        f"Max visits: `{cfg['max_visits']}`\n"
        f"Progress interval: `{cfg['progress_interval']}s`\n"
        f"Support username: @{cfg['support_username']}\n"
        f"Admin display: {cfg['admin_display_name']}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton(f"Maintenance: {yn(cfg['maintenance_mode'])}", callback_data="set_maintenance_mode"),
        types.InlineKeyboardButton(f"Approval: {yn(cfg['approval_mode'])}", callback_data="set_approval_mode"),
        types.InlineKeyboardButton(f"Proxy: {yn(cfg['proxy_enabled'])}", callback_data="set_proxy_enabled"),
        types.InlineKeyboardButton("🔙 Admin", callback_data="adm_panel"),
    )
    bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=mk)


def _toggle_maintenance(chat_id, admin_id):
    cur = get_setting("maintenance_mode", "0")
    new = "0" if cur == "1" else "1"
    set_setting("maintenance_mode", new)
    global MAINTENANCE_MODE
    MAINTENANCE_MODE = new == "1"
    audit_log(admin_id, "MAINTENANCE_TOGGLE", new)
    bot.send_message(
        chat_id,
        f"🛠 Maintenance: `{'ON' if new=='1' else 'OFF'}`",
        parse_mode="Markdown", reply_markup=admin_keyboard(),
    )


def _show_admins(chat_id):
    with _db_lock:
        conn = get_conn()
        try:
            rows = conn.execute("SELECT * FROM admins WHERE is_active=1 ORDER BY role").fetchall()
        finally:
            conn.close()
    lines = ["👮 *ADMINS*", "━━━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        lines.append(f"{r['role']} @{r['username'] or '—'} (`{r['user_id']}`)")
    lines.append(f"OWNER (env) admins: {', '.join(str(a) for a in ADMIN_IDS)}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("_Add admins via ADMIN_IDS env var or contact owner._")
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=admin_keyboard())


def _show_pending(chat_id, admin_id):
    users = pending_users()
    if not users:
        bot.send_message(chat_id, "✅ No pending users.", reply_markup=admin_keyboard())
        return
    mk = types.InlineKeyboardMarkup()
    lines = ["⏳ *PENDING USERS*", "━━━━━━━━━━━━━━━━━━━━"]
    for u in users[:15]:
        lines.append(f"`{u['user_id']}` {u['first_name'] or '—'} @{u['username'] or '—'}")
        mk.add(types.InlineKeyboardButton(f"✅ {u['user_id']}", callback_data=f"appr_{u['user_id']}"))
        mk.add(types.InlineKeyboardButton(f"❌ {u['user_id']}", callback_data=f"rej_{u['user_id']}"))
    mk.add(types.InlineKeyboardButton("🔙 Admin", callback_data="adm_panel"))
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown", reply_markup=mk)


def _do_broadcast(chat_id, admin_id, text):
    users = all_user_ids()
    msg = bot.send_message(chat_id, f"📢 *Broadcasting to `{len(users)}` users…*\n`0%`",
                           parse_mode="Markdown")
    sent = failed = 0
    total = len(users)
    for i, uid in enumerate(users, 1):
        try:
            bot.send_message(uid, text)
            sent += 1
        except Exception:
            failed += 1
        if i % 25 == 0 or i == total:
            try:
                bot.edit_message_text(
                    chat_id=chat_id, message_id=msg.message_id,
                    text=(f"📢 *Broadcasting…*\n"
                          f"Progress: `{int((i/total)*100)}%`\n"
                          f"Sent: `{sent}` | Failed: `{failed}`"),
                    parse_mode="Markdown",
                )
            except Exception:
                pass
        time.sleep(0.05)
    audit_log(admin_id, "BROADCAST_SENT", f"sent={sent} failed={failed}")
    bot.send_message(chat_id, f"✅ *Broadcast complete*\nSent: `{sent}`\nFailed: `{failed}`",
                     parse_mode="Markdown", reply_markup=admin_keyboard())


def _handle_setting_toggle(chat_id, admin_id, key):
    cur = get_setting(key, "0")
    new = "0" if cur == "1" else "1"
    set_setting(key, new)
    if key == "maintenance_mode":
        global MAINTENANCE_MODE
        MAINTENANCE_MODE = new == "1"
    audit_log(admin_id, "SETTING_TOGGLE", f"{key}={new}")
    # refresh view
    if key.startswith("channel"):
        _show_channel_settings(chat_id)
    else:
        _show_bot_settings(chat_id)


def _run_diagnostics(chat_id):
    msg = bot.send_message(chat_id, "🩺 *Running diagnostics…*", parse_mode="Markdown")
    results = []
    # Telegram API
    results.append(("Telegram API", "✅" if bot.get_me() else "❌"))
    # Database
    try:
        with _db_lock:
            conn = get_conn()
            conn.execute("SELECT 1").fetchone()
            conn.close()
        results.append(("Database", "✅"))
    except Exception:
        results.append(("Database", "❌"))
    # Direct HTTP
    try:
        r = requests.get("https://api.ipify.org?format=text", timeout=8)
        results.append(("Direct HTTP", "✅" if r.status_code == 200 else "❌"))
    except Exception:
        results.append(("Direct HTTP", "❌"))
    # Proxy parser
    results.append(("Proxy parser", "✅" if parse_proxy("socks5://1.2.3.4:1080") else "❌"))
    # SOCKS5
    results.append(("SOCKS5 support", "✅" if _SOCKS_OK else "❌"))
    # Channel
    ch = get_setting("channel_username", DEFAULT_CHANNEL)
    try:
        bot.send_message(ch, "🩺 diag ping", parse_mode="Markdown")
        results.append(("Channel posting", "✅"))
    except Exception:
        results.append(("Channel posting", "❌"))
    # Proxy count
    pc = proxy_pool.count()
    results.append((f"Proxies (working={pc['working']})", "✅" if pc['total'] > 0 else "⚠️ none"))

    lines = ["🩺 *SYSTEM DIAGNOSTICS*", "━━━━━━━━━━━━━━━━━━━━"]
    for name, st in results:
        lines.append(f"{st} {name}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=msg.message_id,
                              text="\n".join(lines), parse_mode="Markdown")
    except Exception:
        bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown")


# =========================================================
# Background proxy retest scheduler
# =========================================================
def _retest_loop():
    while True:
        time.sleep(300)
        try:
            rows = list_proxies(limit=500, status_filter="UNTESTED")
            rows += list_proxies(limit=500, status_filter="TCP_FAILED")
            if not rows:
                continue
            log.info("RETEST_LOOP testing %d proxies", len(rows))
            for r in rows[:50]:
                p = {"protocol": r["protocol"], "host": r["host"], "port": r["port"],
                     "username": r["username"], "password": r["password"],
                     "endpoint": r["endpoint"]}
                res = test_proxy(p)
                update_proxy_health(r["id"], res)
        except Exception as e:
            log.warning("retest loop error: %s", e)


# =========================================================
# Startup
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
        init_db(); seed_settings()
        checks.append("✅ Database")
    except Exception as e:
        checks.append(f"❌ Database: {e}")
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
    checks.append(f"📡 Channel: {ch}")
    global MAINTENANCE_MODE
    MAINTENANCE_MODE = get_setting("maintenance_mode", "0") == "1"
    checks.append(f"✅ Configuration loaded (maintenance={'ON' if MAINTENANCE_MODE else 'OFF'})")
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
                    "INSERT OR IGNORE INTO admins(user_id, role, is_active) VALUES(?, 'OWNER', 1)",
                    (BOOTSTRAP_OWNER_ID,),
                )
                conn.commit()
            finally:
                conn.close()
    # start background retester
    threading.Thread(target=_retest_loop, daemon=True).start()
    log.info("Polling started.")
    bot.infinity_polling(timeout=30, long_polling_timeout=20, skip_pending=True)


if __name__ == "__main__":
    main()
