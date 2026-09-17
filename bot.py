#!/usr/bin/env python3
"""
URL Fetcher & Rotating Link Engine
Production-ready Telegram bot with robust proxy management.
Compatible with Render and standard VPS deployments.
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
    # PySocks must be installed for SOCKS5 to work
    import socks  # noqa: F401
    _SOCKS5_AVAILABLE = True
except ImportError:
    _SOCKS5_AVAILABLE = False

# =========================================================
# Logging Setup
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bot")


def _safe_log(msg: str) -> str:
    """Strip anything that looks like a credential from log strings."""
    return re.sub(r"(://)[^@/\s]+@", r"\1****:****@", msg)


# =========================================================
# Configuration (Environment-Driven)
# =========================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8553353076:AAHOv1uzboKpXBJsAfEgDqwe39gSBDyQl1U").strip()
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN environment variable is not set. Exiting.")
    sys.exit(1)

ADMIN_IDS: list[int] = [
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "8753914631").split(",")
    if x.strip().isdigit()
]

DB_FILE = os.environ.get("DATABASE_PATH", "bot_database.db")
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "10"))
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "4"))
MAX_RESPONSE_SIZE = int(os.environ.get("MAX_RESPONSE_SIZE", str(5 * 1024 * 1024)))  # 5 MB
MAX_URL_LENGTH = int(os.environ.get("MAX_URL_LENGTH", "2048"))
PROXY_COOLDOWN = float(os.environ.get("PROXY_COOLDOWN", "120"))

RAW_PROXY_ENV = (
    os.environ.get("PROXY_ENDPOINTS", "")
    or os.environ.get("ROTATING_PROXIES", "")
    or ""
)

# Bot instance
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# Global state
MAINTENANCE_MODE = False
user_states: dict[int, dict] = {}
active_jobs: dict[int, threading.Event] = {}   # user_id -> cancel event
_state_lock = threading.Lock()

# =========================================================
# Database
# =========================================================
_db_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
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
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint         TEXT UNIQUE NOT NULL,
                    added_by         INTEGER,
                    is_active        INTEGER DEFAULT 1,
                    last_tested      TIMESTAMP,
                    last_success     TIMESTAMP,
                    last_failure     TIMESTAMP,
                    success_count    INTEGER DEFAULT 0,
                    failure_count    INTEGER DEFAULT 0,
                    average_latency  REAL DEFAULT 0,
                    last_error       TEXT,
                    last_observed_ip TEXT,
                    cooldown_until   TIMESTAMP,
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_history_user ON extraction_history(user_id);
                CREATE INDEX IF NOT EXISTS idx_history_date ON extraction_history(started_at);
                CREATE INDEX IF NOT EXISTS idx_proxies_active ON admin_proxies(is_active);
            """)
            conn.commit()
        finally:
            conn.close()


# ── User helpers ──────────────────────────────────────────
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


def get_user_stats(user_id: int) -> Optional[dict]:
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
            """SELECT COUNT(*)                               AS total_users,
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


# ── Proxy DB helpers ──────────────────────────────────────
def db_add_proxy(endpoint: str, added_by: int) -> bool:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """INSERT OR IGNORE INTO admin_proxies (endpoint, added_by, is_active)
                   VALUES (?, ?, 1)""",
                (endpoint, added_by),
            )
            conn.commit()
            return conn.execute(
                "SELECT changes() AS c"
            ).fetchone()["c"] > 0 or True
        except Exception as exc:
            logger.warning("db_add_proxy error: %s", exc)
            return False
        finally:
            conn.close()


def db_get_all_proxies(active_only: bool = True) -> list[dict]:
    conn = get_conn()
    try:
        if active_only:
            rows = conn.execute(
                "SELECT * FROM admin_proxies WHERE is_active = 1 ORDER BY id ASC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM admin_proxies ORDER BY id ASC"
            ).fetchall()
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


def db_clear_dead_proxies() -> int:
    with _db_lock:
        conn = get_conn()
        try:
            cur = conn.execute(
                """DELETE FROM admin_proxies
                   WHERE failure_count > 0
                     AND (success_count = 0
                          OR (failure_count > success_count * 3))"""
            )
            deleted = cur.rowcount
            conn.commit()
            return deleted
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
            conn.execute(
                """UPDATE admin_proxies
                   SET success_count    = success_count + 1,
                       last_success     = CURRENT_TIMESTAMP,
                       last_tested      = CURRENT_TIMESTAMP,
                       average_latency  = ?,
                       last_observed_ip = ?,
                       cooldown_until   = NULL,
                       last_error       = NULL
                   WHERE id = ?""",
                (new_avg, observed_ip, proxy_id),
            )
            conn.commit()
        finally:
            conn.close()


def db_update_proxy_failure(proxy_id: int, error: str) -> None:
    with _db_lock:
        conn = get_conn()
        try:
            conn.execute(
                """UPDATE admin_proxies
                   SET failure_count  = failure_count + 1,
                       last_failure   = CURRENT_TIMESTAMP,
                       last_tested    = CURRENT_TIMESTAMP,
                       last_error     = ?,
                       cooldown_until = datetime('now', '+2 minutes')
                   WHERE id = ?""",
                (error[:200], proxy_id),
            )
            conn.commit()
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


def db_proxy_stats() -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            """SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN last_tested IS NULL THEN 1 ELSE 0 END) AS untested,
                SUM(CASE WHEN success_count > 0 AND failure_count = 0 THEN 1
                         WHEN success_count > 0 AND average_latency < 1500 THEN 1
                         ELSE 0 END) AS working,
                SUM(CASE WHEN success_count > 0 AND average_latency >= 1500 THEN 1 ELSE 0 END) AS slow,
                SUM(CASE WHEN failure_count > 0 AND success_count = 0 THEN 1 ELSE 0 END) AS dead,
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
    """Holds a validated, parsed proxy endpoint."""
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
        """
        Build a proxies dict for the requests library.
        For SOCKS5, requires requests[socks] / PySocks installed.
        Returns None if SOCKS5 is required but unavailable.
        """
        if self.scheme.startswith("socks5"):
            if not _SOCKS5_AVAILABLE:
                return None
            if self.username and self.password:
                u = urllib.parse.quote(self.username, safe="")
                p = urllib.parse.quote(self.password, safe="")
                url = f"{self.scheme}://{u}:{p}@{self.host}:{self.port}"
            else:
                url = f"{self.scheme}://{self.host}:{self.port}"
        else:
            if self.username and self.password:
                u = urllib.parse.quote(self.username, safe="")
                p = urllib.parse.quote(self.password, safe="")
                url = f"{self.scheme}://{u}:{p}@{self.host}:{self.port}"
            else:
                url = f"{self.scheme}://{self.host}:{self.port}"
        return {"http": url, "https": url}


def parse_proxy(raw: str) -> tuple[Optional[ParsedProxy], str]:
    """
    Parse and validate a proxy string.
    Returns (ParsedProxy, "") on success or (None, "error reason") on failure.
    """
    raw = raw.strip()
    if not raw:
        return None, "Empty proxy string"

    try:
        parsed = urllib.parse.urlsplit(raw)
    except Exception:
        return None, "Malformed URL"

    scheme = (parsed.scheme or "").lower()
    if scheme not in SUPPORTED_SCHEMES:
        supported = ", ".join(SUPPORTED_SCHEMES)
        return None, f"Unsupported scheme '{scheme}'. Supported: {supported}"

    host = parsed.hostname
    if not host:
        return None, "Missing hostname"

    port = parsed.port
    if port is None:
        return None, "Missing port number"
    if not (1 <= port <= 65535):
        return None, f"Invalid port {port} (must be 1–65535)"

    # Safely decode credentials
    username: Optional[str] = None
    password: Optional[str] = None
    if parsed.username:
        try:
            username = urllib.parse.unquote(parsed.username)
        except Exception:
            return None, "Could not decode username"
    if parsed.password:
        try:
            password = urllib.parse.unquote(parsed.password)
        except Exception:
            return None, "Could not decode password"

    if (username is None) != (password is None):
        return None, "Both username and password must be provided together"

    return ParsedProxy(raw=raw, scheme=scheme, host=host, port=port,
                       username=username, password=password), ""


# =========================================================
# Proxy Test Result
# =========================================================
class ProxyTestResult:
    __slots__ = (
        "proxy_display", "scheme_label", "working", "latency_ms",
        "observed_ip", "error_reason", "tested_at",
    )

    def __init__(
        self,
        proxy_display: str,
        scheme_label: str,
        working: bool,
        latency_ms: Optional[float],
        observed_ip: Optional[str],
        error_reason: Optional[str],
    ):
        self.proxy_display = proxy_display
        self.scheme_label = scheme_label
        self.working = working
        self.latency_ms = latency_ms
        self.observed_ip = observed_ip
        self.error_reason = error_reason
        self.tested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def to_telegram_card(self, index: Optional[int] = None) -> str:
        prefix = f"<b>#{index}</b>\n" if index is not None else ""
        lines = [prefix]
        lines.append(f"🌐 {html.escape(self.scheme_label)}")
        lines.append(f"📡 <code>{html.escape(self.proxy_display)}</code>")

        if self.working:
            lines.append("✅ <b>WORKING</b>")
            if self.latency_ms is not None:
                speed = "⚡ Fast" if self.latency_ms < 1500 else "🐢 Slow"
                lines.append(f"{speed}: <code>{self.latency_ms:.0f} ms</code>")
            if self.observed_ip:
                lines.append(f"🌍 Exit IP: <code>{html.escape(self.observed_ip)}</code>")
            else:
                lines.append("⚠️ IP Verification Failed")
        else:
            lines.append("❌ <b>DEAD</b>")
            if self.error_reason:
                lines.append(f"Reason: {html.escape(self.error_reason)}")

        lines.append(f"🕒 {html.escape(self.tested_at)}")
        return "\n".join(line for line in lines if line)


# =========================================================
# Core Proxy Tester
# =========================================================
_IP_CHECK_URLS = [
    "https://api.ipify.org?format=json",
    "https://httpbin.org/ip",
    "http://ip-api.com/json?fields=query",
]

_TEST_TARGET = "https://httpbin.org/get"
_TCP_TEST_HOST = "httpbin.org"
_TCP_TEST_PORT = 443


def _tcp_check(host: str, port: int, timeout: float = 4.0) -> tuple[bool, str]:
    """Stage 1: raw TCP connectivity (no proxy, just checks DNS + connect)."""
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
        return False, str(exc)


def _classify_requests_error(exc: Exception) -> str:
    """Convert a requests exception to a human-readable reason."""
    msg = str(exc)
    if "SOCKS" in msg and not _SOCKS5_AVAILABLE:
        return "SOCKS5 support not installed (pip install requests[socks])"
    if "timed out" in msg.lower() or "timeout" in msg.lower():
        return "Connection timeout"
    if "refused" in msg.lower():
        return "Connection refused"
    if "407" in msg:
        return "Authentication failed (HTTP 407)"
    if "ssl" in msg.lower() or "certificate" in msg.lower():
        return "TLS/SSL failure"
    if "name or service not known" in msg.lower() or "nodename" in msg.lower():
        return "DNS failure"
    return msg[:120]


def test_proxy(parsed: ParsedProxy, quick: bool = True) -> ProxyTestResult:
    """
    Multi-stage proxy test:
      Stage 1 – TCP connectivity to proxy host
      Stage 2 – HTTP request through proxy
      Stage 3 – Public IP verification through proxy
    """
    timeout = 6.0 if quick else 12.0

    # Stage 1: TCP to the proxy itself
    tcp_ok, tcp_err = _tcp_check(parsed.host, parsed.port, timeout=min(timeout, 4.0))
    if not tcp_ok:
        return ProxyTestResult(
            proxy_display=parsed.display,
            scheme_label=parsed.protocol_label,
            working=False,
            latency_ms=None,
            observed_ip=None,
            error_reason=f"TCP connect to proxy failed: {tcp_err}",
        )

    # Stage 2+3: HTTP request through proxy, public IP check
    proxies_dict = parsed.to_requests_proxies()
    if proxies_dict is None:
        return ProxyTestResult(
            proxy_display=parsed.display,
            scheme_label=parsed.protocol_label,
            working=False,
            latency_ms=None,
            observed_ip=None,
            error_reason="SOCKS5 requires 'requests[socks]' — not installed",
        )

    session = requests.Session()
    session.proxies = proxies_dict
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; ProxyTester/1.0)"
    })

    observed_ip: Optional[str] = None
    latency_ms: Optional[float] = None
    error_reason: Optional[str] = None

    for ip_url in _IP_CHECK_URLS:
        try:
            t0 = time.perf_counter()
            resp = session.get(ip_url, timeout=timeout, allow_redirects=True)
            latency_ms = (time.perf_counter() - t0) * 1000

            if resp.status_code == 200:
                try:
                    data = resp.json()
                    observed_ip = (
                        data.get("ip")
                        or data.get("origin", "").split(",")[0].strip()
                        or data.get("query")
                    )
                except Exception:
                    text = resp.text.strip()
                    ip_match = re.search(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b", text)
                    if ip_match:
                        observed_ip = ip_match.group(1)

                logger.info(
                    "Proxy test OK: %s → exit IP %s @ %.0f ms",
                    _safe_log(parsed.display), observed_ip, latency_ms or 0,
                )
                session.close()
                return ProxyTestResult(
                    proxy_display=parsed.display,
                    scheme_label=parsed.protocol_label,
                    working=True,
                    latency_ms=latency_ms,
                    observed_ip=observed_ip,
                    error_reason=None,
                )
            else:
                error_reason = f"HTTP {resp.status_code} from {ip_url}"
        except requests.exceptions.RequestException as exc:
            error_reason = _classify_requests_error(exc)
            logger.debug("Proxy test fail (%s): %s", _safe_log(parsed.display), error_reason)
        except Exception as exc:
            error_reason = f"Unexpected error: {str(exc)[:80]}"

    session.close()
    return ProxyTestResult(
        proxy_display=parsed.display,
        scheme_label=parsed.protocol_label,
        working=False,
        latency_ms=latency_ms,
        observed_ip=None,
        error_reason=error_reason or "All IP-check targets failed",
    )


# =========================================================
# Proxy Manager / Rotation Engine
# =========================================================
class ProxyManager:
    """
    Manages proxy endpoints from environment variables and the SQLite database.
    Implements round-robin rotation with per-proxy cooldown on failure.
    Never silently falls back to a direct connection.
    """

    def __init__(self):
        self._env_proxies: list[str] = []
        self._rotation_index = 0
        self._cooldowns: dict[str, float] = {}   # endpoint -> expires timestamp
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
                logger.warning("Ignoring invalid env proxy '%s': %s", _safe_log(raw), err)
        self._env_proxies = list(dict.fromkeys(items))
        logger.info("Loaded %d proxy endpoints from environment.", len(self._env_proxies))

    def get_all_raw(self) -> list[str]:
        """All raw endpoint strings (env + db), deduplicated."""
        with self._lock:
            db_rows = db_get_all_proxies(active_only=True)
            db_eps = [r["endpoint"] for r in db_rows]
            combined = list(dict.fromkeys(self._env_proxies + db_eps))
            return combined

    def has_endpoints(self) -> bool:
        return len(self.get_all_raw()) > 0

    def get_endpoint_count(self) -> int:
        return len(self.get_all_raw())

    def env_count(self) -> int:
        return len(self._env_proxies)

    def get_next_endpoint(self) -> Optional[str]:
        """
        Returns the next available (non-cooled-down) proxy using round-robin.
        Returns None only when all proxies are in cooldown; the caller must
        NOT fall back to a direct connection silently.
        """
        with self._lock:
            all_eps = self.get_all_raw()
            if not all_eps:
                return None

            now = time.time()
            # Expire cooldowns
            self._cooldowns = {
                ep: exp for ep, exp in self._cooldowns.items() if exp > now
            }

            available = [ep for ep in all_eps if ep not in self._cooldowns]
            if not available:
                logger.warning("All proxies in cooldown; none available.")
                return None

            idx = self._rotation_index % len(available)
            self._rotation_index += 1
            chosen = available[idx]
            return chosen

    def mark_failed(self, endpoint: str) -> None:
        with self._lock:
            self._cooldowns[endpoint] = time.time() + PROXY_COOLDOWN
            logger.info("Proxy cooled down: %s", _safe_log(endpoint))
            # Also update DB if this proxy exists there
        row = db_get_proxy_by_endpoint(endpoint)
        if row:
            db_update_proxy_failure(row["id"], "Rotation failure")

    def mark_success(self, endpoint: str, latency_ms: float, observed_ip: str) -> None:
        with self._lock:
            self._cooldowns.pop(endpoint, None)
        row = db_get_proxy_by_endpoint(endpoint)
        if row:
            db_update_proxy_success(row["id"], latency_ms, observed_ip)

    @staticmethod
    def sanitize_display(endpoint: str) -> str:
        """Remove credentials for safe display."""
        parsed, err = parse_proxy(endpoint)
        if parsed:
            return parsed.display
        # Fallback strip
        try:
            p = urllib.parse.urlsplit(endpoint)
            netloc = f"{p.hostname}:{p.port}" if p.port else (p.hostname or "?")
            return f"{p.scheme}://{netloc}"
        except Exception:
            return "proxy-endpoint"


proxy_manager = ProxyManager()


# =========================================================
# AES-128-CBC Challenge Solver (InfinityFree / ByetHost)
# Pure Python, zero dependencies
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
# Number Extraction
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


def clean_phone_number(raw: str) -> Optional[str]:
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
# URL Validator
# =========================================================
def validate_url(url: str) -> tuple[bool, str]:
    """Returns (valid, error_reason)."""
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
# Redirect Handler & Extraction Session
# =========================================================
class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        super().__init__()
        self.collected_redirect_targets: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.collected_redirect_targets.append(newurl)
        parsed = urllib.parse.urlparse(newurl)
        if parsed.scheme.lower() not in ("http", "https"):
            # Non-HTTP scheme (whatsapp://, intent://, etc.) — record but don't follow
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class ExtractionSession:
    """
    Per-request session for fetching URLs.
    HTTP/HTTPS proxies use urllib (no external dep).
    SOCKS5 proxies use requests+PySocks.
    Cookie jars are isolated per instance — no cross-proxy leakage.
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

    def __init__(
        self,
        proxy_endpoint: Optional[str] = None,
        timeout: float = REQUEST_TIMEOUT,
    ):
        self.proxy_endpoint = proxy_endpoint
        self.timeout = timeout
        self.cached_test_cookie: Optional[str] = None

        parsed_proxy, parse_err = None, ""
        if proxy_endpoint:
            parsed_proxy, parse_err = parse_proxy(proxy_endpoint)

        self._use_requests = (
            parsed_proxy is not None
            and parsed_proxy.scheme.startswith("socks5")
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

            handlers: list[urllib.request.BaseHandler] = [
                urllib.request.HTTPCookieProcessor(self._cj),
                self._redirect_handler,
            ]
            if parsed_proxy and not self._use_requests:
                proxy_dict = {
                    "http": proxy_endpoint,
                    "https": proxy_endpoint,
                }
                handlers.append(urllib.request.ProxyHandler(proxy_dict))

            self._urllib_opener = urllib.request.build_opener(*handlers)
            self._urllib_opener.addheaders = list(self._DEFAULT_HEADERS)

    def close(self) -> None:
        if self._requests_session:
            try:
                self._requests_session.close()
            except Exception:
                pass

    def fetch(self, url: str) -> tuple[str, str, list[str]]:
        """
        Returns (final_url, body, all_visited_urls).
        Handles InfinityFree challenge, meta-refresh, JS redirects.
        Non-HTTP redirect targets (whatsapp://) are recorded but not followed.
        Limits response body to MAX_RESPONSE_SIZE.
        """
        if self._use_requests:
            return self._fetch_requests(url)
        return self._fetch_urllib(url)

    def _fetch_requests(self, url: str) -> tuple[str, str, list[str]]:
        visited: list[str] = [url]
        sess = self._requests_session
        try:
            resp = sess.get(url, timeout=self.timeout, allow_redirects=True, stream=True)
            # Track redirect chain
            for r in resp.history:
                visited.append(r.url)
            visited.append(resp.url)

            body_bytes = b""
            for chunk in resp.iter_content(chunk_size=65536):
                body_bytes += chunk
                if len(body_bytes) > MAX_RESPONSE_SIZE:
                    break
            body = body_bytes.decode("utf-8", errors="ignore")
            return resp.url, body, visited
        except requests.exceptions.RequestException as exc:
            raise OSError(_classify_requests_error(exc)) from exc

    def _fetch_urllib(self, url: str) -> tuple[str, str, list[str]]:
        if self._redirect_handler:
            self._redirect_handler.collected_redirect_targets.clear()

        domain = urllib.parse.urlparse(url).hostname
        if self.cached_test_cookie and domain and self._cj is not None:
            c_obj = http.cookiejar.Cookie(
                version=0, name="__test", value=self.cached_test_cookie,
                port=None, port_specified=False,
                domain=domain, domain_specified=True, domain_initial_dot=False,
                path="/", path_specified=True, secure=False, expires=None,
                discard=True, comment=None, comment_url=None,
                rest={"HttpOnly": None}, rfc2109=False,
            )
            self._cj.set_cookie(c_obj)

        visited: list[str] = [url]
        current_url = url
        body = ""

        try:
            req = urllib.request.Request(url)
            resp = self._urllib_opener.open(req, timeout=self.timeout)
            current_url = resp.geturl()
            visited.append(current_url)
            raw = resp.read(MAX_RESPONSE_SIZE)
            body = raw.decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as e:
            current_url = e.geturl() or url
            visited.append(current_url)
            try:
                raw = e.read(MAX_RESPONSE_SIZE) if hasattr(e, "read") else b""
                body = raw.decode("utf-8", errors="ignore")
            except Exception:
                body = ""
        except Exception:
            if self._redirect_handler:
                visited.extend(self._redirect_handler.collected_redirect_targets)
            raise

        if self._redirect_handler:
            visited.extend(self._redirect_handler.collected_redirect_targets)

        # InfinityFree / ByetHost slowAES challenge
        if "slowAES" in body or ("toNumbers(" in body and "__test=" in body):
            matches = re.findall(r'toNumbers\("([a-f0-9]+)"\)', body)
            if len(matches) >= 3:
                a_key, b_iv, c_cipher = matches[0], matches[1], matches[2]
                self.cached_test_cookie = decrypt_byet_challenge(c_cipher, a_key, b_iv)
                if domain and self._cj is not None:
                    c_obj = http.cookiejar.Cookie(
                        version=0, name="__test", value=self.cached_test_cookie,
                        port=None, port_specified=False,
                        domain=domain, domain_specified=True, domain_initial_dot=False,
                        path="/", path_specified=True, secure=False, expires=None,
                        discard=True, comment=None, comment_url=None,
                        rest={"HttpOnly": None}, rfc2109=False,
                    )
                    self._cj.set_cookie(c_obj)

                loc_match = re.search(r'location\.href\s*=\s*["\'](.*?)["\']', body)
                next_dest = loc_match.group(1) if loc_match else (
                    url + ("&i=1" if "?" in url else "?i=1")
                )
                next_url = urllib.parse.urljoin(current_url, next_dest)
                visited.append(next_url)
                try:
                    resp2 = self._urllib_opener.open(
                        urllib.request.Request(next_url), timeout=self.timeout
                    )
                    current_url = resp2.geturl()
                    visited.append(current_url)
                    body = resp2.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
                except Exception:
                    pass

        # Meta-refresh and JS redirect follow-through (up to 2 hops)
        for _ in range(2):
            meta_refresh = re.search(
                r'<meta[^>]*?http-equiv\s*=\s*["\']?refresh["\']?[^>]*?'
                r'content\s*=\s*["\']?[^"\'>]*?url\s*=\s*([^\s"\'\';>]+)',
                body, re.IGNORECASE,
            )
            if meta_refresh:
                dest = meta_refresh.group(1).strip()
                dest_url = urllib.parse.urljoin(current_url, dest)
                visited.append(dest_url)
                if dest_url.lower().startswith(("http://", "https://")):
                    try:
                        r = self._urllib_opener.open(
                            urllib.request.Request(dest_url), timeout=self.timeout
                        )
                        current_url = r.geturl()
                        visited.append(current_url)
                        body = r.read(MAX_RESPONSE_SIZE).decode("utf-8", errors="ignore")
                        continue
                    except Exception:
                        pass
                break

            js_match = re.search(
                r'(?:window\.|document\.|top\.)?location(?:\.href|\.replace|\.assign)?\s*'
                r'(?:=|\()\s*["\'](https?://[^"\']+|whatsapp://[^"\']+|wa\.me/[^"\']+)["\']',
                body, re.IGNORECASE,
            )
            if js_match:
                dest_url = js_match.group(1).strip()
                visited.append(dest_url)
                if dest_url.lower().startswith(("http://", "https://")):
                    try:
                        r = self._urllib_opener.open(
                            urllib.request.Request(dest_url), timeout=self.timeout
                        )
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
    import random as _r
    ts = int(time.time() * 1000)
    salt = _r.randint(100, 999)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}_cb={ts}_{cycle}_{salt}"


# =========================================================
# Keyboards
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
        types.KeyboardButton("🔙 Back"),
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
        types.KeyboardButton("🔙 Back"),
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
        types.KeyboardButton("🧪 Test Single Proxy"),
        types.KeyboardButton("📦 Test Multiple Proxies"),
        types.KeyboardButton("🧪 Test All Proxies"),
        types.KeyboardButton("📋 List All Proxies"),
        types.KeyboardButton("📊 Proxy Statistics"),
        types.KeyboardButton("🔄 Retest Failed"),
        types.KeyboardButton("🗑️ Delete Proxy"),
        types.KeyboardButton("🗑️ Clear Dead Proxies"),
        types.KeyboardButton("🗑️ Clear All Proxies"),
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


def proxy_status_emoji(row: dict) -> str:
    if row.get("last_tested") is None:
        return "⚪"
    if row.get("success_count", 0) == 0:
        return "🔴"
    avg_lat = row.get("average_latency") or 0
    if avg_lat >= 1500:
        return "🟡"
    return "🟢"


# =========================================================
# Extraction Worker
# =========================================================
_CYCLE_COUNT_MAP = {
    "🧪 Test — 1 Visit": 1,
    "🚀 20 Visits": 20,
    "⚡ 50 Visits": 50,
    "💎 100 Visits": 100,
}


def extraction_worker(
    chat_id: int,
    user_id: int,
    url: str,
    mode: str,
    total_cycles: int,
    progress_message_id: int,
    cancel_event: threading.Event,
) -> None:
    logger.info("Job started: user=%d url=%s mode=%s cycles=%d", user_id, url, mode, total_cycles)

    found_numbers: list[str] = []
    found_set: set[str] = set()
    total_numbers_seen = 0
    total_ok = 0
    errors = 0
    last_ui_update = 0.0
    start_time = time.time()

    mode_display = "🌐 IP Rotation" if mode == "ROTATING" else "🟢 Normal Connection"

    # Shared session for NORMAL mode (reuse for performance)
    shared_session: Optional[ExtractionSession] = None
    if mode == "NORMAL":
        shared_session = ExtractionSession(proxy_endpoint=None)

    try:
        for cycle in range(1, total_cycles + 1):
            if cancel_event.is_set():
                logger.info("Job cancelled: user=%d at cycle %d", user_id, cycle)
                break

            target_url = add_cache_buster(url, cycle)
            session: Optional[ExtractionSession] = shared_session
            endpoint_raw: Optional[str] = None

            if mode == "ROTATING":
                endpoint_raw = proxy_manager.get_next_endpoint()
                if endpoint_raw is None:
                    # No proxy available — abort honestly; do not fall back to direct.
                    logger.warning("No proxy available at cycle %d for user %d", cycle, user_id)
                    try:
                        bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=progress_message_id,
                            text=(
                                "❌ <b>Extraction Stopped</b>\n\n"
                                "No working proxy is available right now.\n"
                                "All proxies may be in cooldown.\n\n"
                                f"Completed: {cycle - 1}/{total_cycles} visits.\n"
                                f"Unique Numbers found so far: {len(found_numbers)}"
                            ),
                        )
                    except Exception:
                        pass
                    break
                session = ExtractionSession(proxy_endpoint=endpoint_raw)

            cycle_start = time.perf_counter()
            try:
                final_url, body, visited_urls = session.fetch(target_url)
                latency_ms = (time.perf_counter() - cycle_start) * 1000
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

                if mode == "ROTATING" and endpoint_raw:
                    proxy_manager.mark_success(endpoint_raw, latency_ms, "")

            except Exception as exc:
                errors += 1
                err_str = str(exc)[:120]
                logger.warning(
                    "Cycle %d error (user=%d proxy=%s): %s",
                    cycle, user_id, _safe_log(endpoint_raw or "direct"), err_str,
                )
                if mode == "ROTATING" and endpoint_raw:
                    proxy_manager.mark_failed(endpoint_raw)
            finally:
                if mode == "ROTATING" and session:
                    session.close()

            # Throttled progress update (~1.8 s)
            now = time.time()
            if (now - last_ui_update > 1.8) or (cycle == total_cycles):
                last_ui_update = now
                elapsed = max(int(now - start_time), 1)
                pct = int((cycle / total_cycles) * 100)
                done_ticks = int((cycle / total_cycles) * 10)
                bar = "█" * done_ticks + "░" * (10 - done_ticks)

                cancel_hint = ""
                if mode == "ROTATING":
                    ep_disp = ProxyManager.sanitize_display(endpoint_raw) if endpoint_raw else "None"
                    cancel_hint = f"\n📡 Last proxy: <code>{html.escape(ep_disp)}</code>"

                progress_text = (
                    f"⏳ <b>EXTRACTION IN PROGRESS</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"<b>Mode:</b> {mode_display}\n"
                    f"<b>Visits:</b> {cycle}/{total_cycles} ({pct}%)\n"
                    f"<b>Progress:</b> <code>[{bar}]</code>\n"
                    f"<b>Successful:</b> <code>{total_ok}</code>\n"
                    f"<b>Failed:</b> <code>{errors}</code>\n"
                    f"<b>Unique Numbers:</b> <code>{len(found_numbers)}</code>\n"
                    f"<b>Elapsed:</b> {elapsed}s"
                    f"{cancel_hint}\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"<i>Tap ❌ Cancel to stop</i>"
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

    finally:
        if shared_session:
            shared_session.close()
        with _state_lock:
            active_jobs.pop(user_id, None)

    logger.info(
        "Job completed: user=%d unique=%d ok=%d err=%d",
        user_id, len(found_numbers), total_ok, errors,
    )

    unique_count = len(found_numbers)
    duplicate_count = max(total_numbers_seen - unique_count, 0)
    sorted_numbers = sorted(found_numbers)

    update_user_stats(user_id, unique_count)
    save_history(user_id, url, mode, total_cycles, unique_count, duplicate_count)

    cancelled_note = " (Cancelled early)" if cancel_event.is_set() else ""

    if unique_count > 0:
        numbers_plain_text = "\n".join(f"+{num}" for num in sorted_numbers)

        CHUNK_LIMIT = 3200
        lines = [f"+{num}" for num in sorted_numbers]
        chunks: list[str] = []
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
            f"📱 <b>EXTRACTED NUMBERS</b>{html.escape(cancelled_note)}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<code>{html.escape(chunks[0])}</code>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>Unique Numbers:</b> <code>{unique_count}</code>\n"
            f"🔄 <b>Visits:</b> <code>{total_cycles}</code>\n"
            f"✅ <b>Successful:</b> <code>{total_ok}</code>\n"
            f"❌ <b>Failed:</b> <code>{errors}</code>\n"
            f"🌐 <b>Mode:</b> {mode_display}"
        )
        try:
            bot.send_message(chat_id, header_text, reply_markup=copy_markup)
        except Exception:
            bot.send_message(chat_id, f"Extracted Numbers:\n{chunks[0]}")

        for extra_idx, extra_chunk in enumerate(chunks[1:], start=2):
            extra_text = (
                f"📱 <b>Numbers (Part {extra_idx})</b>\n\n"
                f"<code>{html.escape(extra_chunk)}</code>"
            )
            try:
                bot.send_message(chat_id, extra_text)
            except Exception:
                bot.send_message(chat_id, extra_chunk)

        try:
            file_data = (
                "URL Extractor — Final Results\n"
                f"Source URL: {url}\n"
                f"Mode: {mode_display}\n"
                f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Total Visits Requested: {total_cycles}\n"
                f"Successful Visits: {total_ok}\n"
                f"Failed Visits: {errors}\n"
                f"Unique Numbers: {unique_count}\n"
                f"Duplicates Filtered: {duplicate_count}\n"
                + ("=" * 45) + "\n\n"
                + numbers_plain_text + "\n"
            )
            file_stream = io.BytesIO(file_data.encode("utf-8"))
            file_stream.name = f"numbers_{int(time.time())}.txt"
            bot.send_document(
                chat_id,
                file_stream,
                caption=(
                    f"📁 <b>Extraction File Ready</b>\n"
                    f"📱 <code>Unique Numbers: {unique_count}</code>\n"
                    f"<i>Tap above to save or download</i>"
                ),
                reply_markup=main_keyboard(),
            )
        except Exception as e:
            logger.error("File upload failed: %s", e)
            bot.send_message(
                chat_id,
                f"⚠️ File upload error: {html.escape(str(e)[:80])}",
                reply_markup=main_keyboard(),
            )
    else:
        bot.send_message(
            chat_id,
            (
                f"⚠️ <b>No numbers found</b>{html.escape(cancelled_note)}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔄 <b>Visits Completed:</b> <code>{total_ok}/{total_cycles}</code>\n"
                f"❌ <b>Errors:</b> <code>{errors}</code>\n"
                f"🌐 <b>Mode:</b> {mode_display}\n\n"
                "<i>The target may not have returned any trackable phone patterns.</i>"
            ),
            reply_markup=main_keyboard(),
        )


# =========================================================
# Background Proxy Test Worker (for bulk/all tests)
# =========================================================
def _run_proxy_tests_in_bg(
    chat_id: int,
    status_msg_id: int,
    endpoints_raw: list[str],
    cancel_event: threading.Event,
    detailed: bool = False,
) -> None:
    """
    Tests a list of raw proxy strings in a bounded thread pool.
    Sends progress updates, then a full summary.
    """
    total = len(endpoints_raw)
    results: list[tuple[str, ProxyTestResult]] = []
    tested = 0
    working = 0
    dead = 0
    last_update = 0.0

    def _test_one(raw: str) -> tuple[str, ProxyTestResult]:
        parsed, err = parse_proxy(raw)
        if not parsed:
            return raw, ProxyTestResult(
                proxy_display=ProxyManager.sanitize_display(raw),
                scheme_label="UNKNOWN",
                working=False,
                latency_ms=None,
                observed_ip=None,
                error_reason=f"Parse error: {err}",
            )
        return raw, test_proxy(parsed, quick=not detailed)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as pool:
        futures = {pool.submit(_test_one, raw): raw for raw in endpoints_raw}
        for future in concurrent.futures.as_completed(futures):
            if cancel_event.is_set():
                # Cancel remaining futures
                for f in futures:
                    f.cancel()
                break

            raw, result = future.result()
            results.append((raw, result))
            tested += 1
            if result.working:
                working += 1
                # Update DB stats
                db_row = db_get_proxy_by_endpoint(raw)
                if db_row:
                    db_update_proxy_success(
                        db_row["id"],
                        result.latency_ms or 0,
                        result.observed_ip or "",
                    )
            else:
                dead += 1
                db_row = db_get_proxy_by_endpoint(raw)
                if db_row:
                    db_update_proxy_failure(db_row["id"], result.error_reason or "Test failed")

            now = time.time()
            if (now - last_update > 2.0) or tested == total:
                last_update = now
                pct = int((tested / total) * 100)
                done_ticks = int((tested / total) * 10)
                bar = "█" * done_ticks + "░" * (10 - done_ticks)
                progress_text = (
                    f"🧪 <b>Testing proxies...</b>\n\n"
                    f"Progress:\n<code>[{bar}]</code> {pct}%\n\n"
                    f"Tested: {tested}/{total}\n"
                    f"✅ Working: {working}\n"
                    f"❌ Dead: {dead}"
                )
                try:
                    bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_msg_id,
                        text=progress_text,
                    )
                except Exception:
                    pass

    if cancel_event.is_set():
        try:
            bot.send_message(
                chat_id,
                f"🛑 <b>Test cancelled.</b>\nTested {tested}/{total} proxies.",
                reply_markup=proxy_manager_keyboard(),
            )
        except Exception:
            pass
        return

    # Build summary
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

    # Send summary first
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=status_msg_id, text=summary)
    except Exception:
        try:
            bot.send_message(chat_id, summary)
        except Exception:
            pass

    # Send individual cards in batches
    CARDS_PER_MSG = 5
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
            time.sleep(0.5)

    # Inline buttons for follow-up actions
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🔄 Retest Failed", callback_data="retest_failed"),
        types.InlineKeyboardButton("🔙 Proxy Manager", callback_data="proxy_manager"),
    )
    try:
        bot.send_message(
            chat_id,
            "✅ <b>Test complete.</b>",
            reply_markup=proxy_manager_keyboard(),
        )
    except Exception:
        pass


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
            f"👋 <b>Welcome!</b>\n\n"
            f"🤖 <b>URL Fetcher & Link Engine</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<b>How it works:</b>\n"
            f"1️⃣ Tap <b>🔗 Send New Link</b>\n"
            f"2️⃣ Paste any valid HTTP/HTTPS link\n"
            f"3️⃣ Choose extraction mode:\n"
            f"    • 🟢 <b>WITHOUT IP:</b> Direct server connection\n"
            f"    • 🌐 <b>WITH IP ROTATION:</b> Via configured proxy endpoints\n"
            f"4️⃣ Select visit count (1x / 20x / 50x / 100x)\n"
            f"5️⃣ Receive all unique numbers with Copy + .txt download!\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Select an option below:</b>"
        ),
        reply_markup=main_keyboard(),
    )


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "❌ <b>Access Denied.</b>")
        return
    bot.send_message(
        message.chat.id,
        "🔐 <b>Admin Control Console</b>\n━━━━━━━━━━━━━━━━━━\nSelect an administrative function:",
        reply_markup=admin_keyboard(),
    )


# =========================================================
# Inline Callback Handlers
# =========================================================
@bot.callback_query_handler(func=lambda call: call.data.startswith("del_proxy_"))
def handle_proxy_delete_callback(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "Access denied.")
        return
    proxy_id_str = call.data.replace("del_proxy_", "")
    if proxy_id_str.isdigit():
        db_delete_proxy(int(proxy_id_str))
        bot.answer_callback_query(call.id, "✅ Proxy deleted.")
        try:
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=f"🗑️ <i>Proxy #{proxy_id_str} deleted.</i>",
            )
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "proxy_manager")
def handle_proxy_manager_callback(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "Access denied.")
        return
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        "🌐 <b>Proxy Manager</b>",
        reply_markup=proxy_manager_keyboard(),
    )


@bot.callback_query_handler(func=lambda call: call.data == "retest_failed")
def handle_retest_failed_callback(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "Access denied.")
        return
    bot.answer_callback_query(call.id, "Starting retest of failed proxies...")
    _trigger_retest_failed(call.message.chat.id)


# =========================================================
# Main Message Router
# =========================================================
@bot.message_handler(func=lambda m: True)
def handle_all_messages(message: types.Message) -> None:
    global MAINTENANCE_MODE

    user = message.from_user
    chat_id = message.chat.id
    text = (message.text or "").strip()

    register_user(user.id, user.username, user.first_name)

    with _state_lock:
        state = dict(user_states.get(user.id, {}))

    # ── Maintenance mode check ────────────────────────────
    if MAINTENANCE_MODE and user.id not in ADMIN_IDS:
        bot.send_message(
            chat_id,
            "🔧 <b>Maintenance Mode Active</b>\n\nPlease check back shortly!",
        )
        return

    # ── Global Cancel / Main Menu ─────────────────────────
    if text == "❌ Cancel":
        with _state_lock:
            cancel_ev = active_jobs.get(user.id)
        if cancel_ev:
            cancel_ev.set()
            bot.send_message(chat_id, "🛑 <b>Cancellation requested.</b> Stopping after current step…", reply_markup=main_keyboard())
        else:
            with _state_lock:
                user_states[user.id] = {}
            bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard())
        return

    if text == "🔙 Main Menu":
        with _state_lock:
            user_states[user.id] = {}
        bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard())
        return

    # ── Back navigation ───────────────────────────────────
    if text == "🔙 Back":
        step = state.get("step")
        if step == "AWAITING_MODE":
            with _state_lock:
                user_states[user.id] = {"step": "AWAITING_URL"}
            bot.send_message(
                chat_id,
                "🔗 <b>Submit Target URL</b>\n\nPlease send a valid HTTP/HTTPS link:",
                reply_markup=cancel_only_keyboard(),
            )
        elif step == "AWAITING_CYCLES":
            with _state_lock:
                user_states[user.id] = {"step": "AWAITING_MODE", "url": state.get("url")}
            bot.send_message(
                chat_id,
                "Choose Extraction Mode:",
                reply_markup=mode_selection_keyboard(),
            )
        elif user.id in ADMIN_IDS and step in (
            "ADMIN_ADD_PROXY", "ADMIN_BULK_ADD_PROXIES",
            "ADMIN_TEST_SINGLE", "ADMIN_TEST_BULK",
            "ADMIN_DELETE_PROXY",
        ):
            with _state_lock:
                user_states[user.id] = {}
            bot.send_message(chat_id, "🌐 <b>Proxy Manager</b>", reply_markup=proxy_manager_keyboard())
        elif user.id in ADMIN_IDS:
            with _state_lock:
                user_states[user.id] = {}
            bot.send_message(chat_id, "🔐 <b>Admin Console</b>", reply_markup=admin_keyboard())
        else:
            with _state_lock:
                user_states[user.id] = {}
            bot.send_message(chat_id, "🏠 <b>Main Menu</b>", reply_markup=main_keyboard())
        return

    # ── Admin-only: Back to Admin Panel ──────────────────
    if user.id in ADMIN_IDS and text == "🔙 Admin Panel":
        with _state_lock:
            user_states[user.id] = {}
        bot.send_message(chat_id, "🔐 <b>Admin Control Console</b>", reply_markup=admin_keyboard())
        return

    # ── Admin: Broadcast input ────────────────────────────
    if user.id in ADMIN_IDS and state.get("awaiting_broadcast"):
        with _state_lock:
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
                    f"🎉 Sent: <code>{sent}</code>\n"
                    f"❌ Failed: <code>{failed}</code>"
                ),
            )
        except Exception:
            pass
        bot.send_message(chat_id, "Admin Console:", reply_markup=admin_keyboard())
        return

    # ── Admin: Add Single Proxy input ─────────────────────
    if user.id in ADMIN_IDS and state.get("step") == "ADMIN_ADD_PROXY":
        with _state_lock:
            user_states[user.id] = {}
        endpoint = text.strip()
        parsed, err = parse_proxy(endpoint)
        if not parsed:
            bot.send_message(
                chat_id,
                (
                    f"❌ <b>Invalid Proxy</b>\n\n"
                    f"Reason: {html.escape(err)}\n\n"
                    "<b>Supported formats:</b>\n"
                    "• <code>http://ip:port</code>\n"
                    "• <code>http://user:pass@ip:port</code>\n"
                    "• <code>https://ip:port</code>\n"
                    "• <code>socks5://ip:port</code>\n"
                    "• <code>socks5://user:pass@ip:port</code>"
                ),
                reply_markup=proxy_manager_keyboard(),
            )
            return

        if parsed.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
            bot.send_message(
                chat_id,
                (
                    "⚠️ <b>SOCKS5 Not Available</b>\n\n"
                    "PySocks is not installed.\n"
                    "Add <code>requests[socks]</code> to requirements.txt and redeploy."
                ),
                reply_markup=proxy_manager_keyboard(),
            )
            return

        test_msg = bot.send_message(
            chat_id, "⏳ <i>Testing proxy connectivity...</i>"
        )
        result = test_proxy(parsed, quick=True)

        try:
            bot.delete_message(chat_id, test_msg.message_id)
        except Exception:
            pass

        added = db_add_proxy(endpoint, user.id)
        if added:
            db_row = db_get_proxy_by_endpoint(endpoint)
            if db_row:
                if result.working:
                    db_update_proxy_success(
                        db_row["id"],
                        result.latency_ms or 0,
                        result.observed_ip or "",
                    )
                else:
                    db_update_proxy_failure(db_row["id"], result.error_reason or "Initial test failed")

        card = result.to_telegram_card()
        status_line = "✅ <b>Proxy saved to database.</b>" if added else "⚠️ Proxy may already exist."
        bot.send_message(
            chat_id,
            f"{status_line}\n\n{card}",
            reply_markup=proxy_manager_keyboard(),
        )
        return

    # ── Admin: Bulk Add Proxies input ─────────────────────
    if user.id in ADMIN_IDS and state.get("step") == "ADMIN_BULK_ADD_PROXIES":
        with _state_lock:
            user_states[user.id] = {}
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        added_count = 0
        skipped_count = 0
        invalid_count = 0
        socks_unavailable = 0

        status_msg = bot.send_message(chat_id, f"⏳ <i>Processing {len(lines)} entries...</i>")

        for line in lines:
            parsed, err = parse_proxy(line)
            if not parsed:
                invalid_count += 1
                continue
            if parsed.scheme.startswith("socks5") and not _SOCKS5_AVAILABLE:
                socks_unavailable += 1
                continue
            if db_add_proxy(line, user.id):
                added_count += 1
            else:
                skipped_count += 1

        socks_note = ""
        if socks_unavailable:
            socks_note = f"\n⚠️ SOCKS5 skipped (not installed): <code>{socks_unavailable}</code>"

        try:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg.message_id,
                text=(
                    f"✅ <b>Bulk Import Finished</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"➕ <b>Added:</b> <code>{added_count}</code>\n"
                    f"🔁 <b>Already existed:</b> <code>{skipped_count}</code>\n"
                    f"⚠️ <b>Invalid:</b> <code>{invalid_count}</code>"
                    f"{socks_note}\n"
                    f"📡 <b>Total Active Proxies:</b> <code>{proxy_manager.get_endpoint_count()}</code>"
                ),
            )
        except Exception:
            pass
        bot.send_message(chat_id, "Proxy Manager:", reply_markup=proxy_manager_keyboard())
        return

    # ── Admin: Test Single Proxy input ───────────────────
    if user.id in ADMIN_IDS and state.get("step") == "ADMIN_TEST_SINGLE":
        with _state_lock:
            user_states[user.id] = {}
        endpoint = text.strip()
        parsed, err = parse_proxy(endpoint)
        if not parsed:
            bot.send_message(
                chat_id,
                f"❌ <b>Invalid proxy:</b> {html.escape(err)}",
                reply_markup=proxy_manager_keyboard(),
            )
            return

        wait_msg = bot.send_message(chat_id, "🔍 <i>Running detailed proxy test…</i>")
        result = test_proxy(parsed, quick=False)
        try:
            bot.delete_message(chat_id, wait_msg.message_id)
        except Exception:
            pass

        card = result.to_telegram_card()

        # Inline buttons
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔄 Retest", callback_data=f"retest_single_{endpoint[:80]}"),
            types.InlineKeyboardButton("🔙 Proxy Manager", callback_data="proxy_manager"),
        )
        # Save option if not already in DB
        db_row = db_get_proxy_by_endpoint(endpoint)
        if not db_row:
            markup.add(
                types.InlineKeyboardButton("💾 Save Proxy", callback_data=f"save_proxy_{endpoint[:80]}")
            )

        bot.send_message(
            chat_id,
            f"🔍 <b>Single Proxy Test Result</b>\n\n{card}",
            reply_markup=proxy_manager_keyboard(),
        )
        return

    # ── Admin: Test Multiple Proxies input ───────────────
    if user.id in ADMIN_IDS and state.get("step") == "ADMIN_TEST_BULK":
        with _state_lock:
            user_states[user.id] = {}
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            bot.send_message(chat_id, "⚠️ No proxies found in input.", reply_markup=proxy_manager_keyboard())
            return

        cancel_ev = threading.Event()
        status_msg = bot.send_message(
            chat_id,
            f"🧪 <b>Testing {len(lines)} proxies...</b>",
        )

        def _bg():
            _run_proxy_tests_in_bg(chat_id, status_msg.message_id, lines, cancel_ev, detailed=True)

        t = threading.Thread(target=_bg, daemon=True)
        t.start()
        return

    # ── Admin: Delete Proxy by ID input ──────────────────
    if user.id in ADMIN_IDS and state.get("step") == "ADMIN_DELETE_PROXY":
        with _state_lock:
            user_states[user.id] = {}
        if text.isdigit():
            if db_delete_proxy(int(text)):
                bot.send_message(
                    chat_id,
                    f"🗑️ <b>Proxy #{html.escape(text)} deleted.</b>",
                    reply_markup=proxy_manager_keyboard(),
                )
            else:
                bot.send_message(
                    chat_id,
                    f"⚠️ Proxy #{html.escape(text)} not found.",
                    reply_markup=proxy_manager_keyboard(),
                )
        else:
            bot.send_message(
                chat_id, "⚠️ Please send a numeric proxy ID.", reply_markup=proxy_manager_keyboard()
            )
        return

    # ── Admin Menu Buttons ────────────────────────────────
    if user.id in ADMIN_IDS:
        if text == "🌐 Proxy Manager":
            total_eps = proxy_manager.get_endpoint_count()
            env_count = proxy_manager.env_count()
            db_count = len(db_get_all_proxies())
            bot.send_message(
                chat_id,
                (
                    f"🌐 <b>Proxy Manager</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"📡 <b>Total Active Endpoints:</b> <code>{total_eps}</code>\n"
                    f"⚙️ <b>From Environment:</b> <code>{env_count}</code>\n"
                    f"💾 <b>From Database:</b> <code>{db_count}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"Choose an option:"
                ),
                reply_markup=proxy_manager_keyboard(),
            )
            return

        if text == "➕ Add Single Proxy":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_ADD_PROXY"}
            bot.send_message(
                chat_id,
                (
                    "➕ <b>Add Single Proxy</b>\n\n"
                    "Send the proxy in one of these formats:\n"
                    "• <code>http://ip:port</code>\n"
                    "• <code>http://username:password@ip:port</code>\n"
                    "• <code>https://ip:port</code>\n"
                    "• <code>socks5://ip:port</code>\n"
                    "• <code>socks5://username:password@ip:port</code>\n\n"
                    "<i>Credentials are stored securely and never shown in chat.</i>"
                ),
                reply_markup=back_cancel_keyboard(),
            )
            return

        if text == "📦 Bulk Add Proxies":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_BULK_ADD_PROXIES"}
            bot.send_message(
                chat_id,
                (
                    "📦 <b>Bulk Add Proxies</b>\n\n"
                    "Paste your proxies below (one per line):\n\n"
                    "<code>http://user:pass@1.2.3.4:8080\n"
                    "http://5.6.7.8:8080\n"
                    "socks5://9.10.11.12:1080</code>"
                ),
                reply_markup=back_cancel_keyboard(),
            )
            return

        if text == "🧪 Test Single Proxy":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_TEST_SINGLE"}
            bot.send_message(
                chat_id,
                (
                    "🧪 <b>Test Single Proxy</b>\n\n"
                    "Send one proxy to test (any supported format):\n"
                    "• <code>http://ip:port</code>\n"
                    "• <code>socks5://ip:port</code>\n"
                    "• etc."
                ),
                reply_markup=back_cancel_keyboard(),
            )
            return

        if text == "📦 Test Multiple Proxies":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_TEST_BULK"}
            bot.send_message(
                chat_id,
                (
                    "📦 <b>Test Multiple Proxies</b>\n\n"
                    "Paste proxies to test (one per line):\n\n"
                    "<code>http://1.2.3.4:8080\n"
                    "socks5://5.6.7.8:1080</code>"
                ),
                reply_markup=back_cancel_keyboard(),
            )
            return

        if text == "🧪 Test All Proxies":
            all_eps = proxy_manager.get_all_raw()
            if not all_eps:
                bot.send_message(chat_id, "⚠️ No proxies configured.", reply_markup=proxy_manager_keyboard())
                return
            status_msg = bot.send_message(
                chat_id, f"🧪 <b>Testing {len(all_eps)} proxies…</b>"
            )
            cancel_ev = threading.Event()

            def _bg_all():
                _run_proxy_tests_in_bg(
                    chat_id, status_msg.message_id, all_eps, cancel_ev, detailed=False
                )

            threading.Thread(target=_bg_all, daemon=True).start()
            return

        if text == "📋 List All Proxies":
            db_proxies = db_get_all_proxies(active_only=False)
            env_proxies = proxy_manager._env_proxies

            if not db_proxies and not env_proxies:
                bot.send_message(
                    chat_id,
                    "📋 <b>No proxies configured yet.</b>",
                    reply_markup=proxy_manager_keyboard(),
                )
                return

            if env_proxies:
                lines = ["⚙️ <b>Environment Proxies (Read-Only):</b>"]
                for ep in env_proxies:
                    lines.append(f"• <code>{html.escape(ProxyManager.sanitize_display(ep))}</code>")
                bot.send_message(
                    chat_id, "\n".join(lines), reply_markup=proxy_manager_keyboard()
                )

            if db_proxies:
                bot.send_message(chat_id, "💾 <b>Database Proxies:</b>")
                # Send in pages of 8 to avoid huge messages
                page_size = 8
                for page_start in range(0, len(db_proxies), page_size):
                    page = db_proxies[page_start: page_start + page_size]
                    for item in page:
                        sanitized = ProxyManager.sanitize_display(item["endpoint"])
                        emoji = proxy_status_emoji(item)
                        lat_str = f"{item['average_latency']:.0f} ms" if item.get("average_latency") else "N/A"
                        ip_str = item.get("last_observed_ip") or "Unknown"
                        card = (
                            f"{emoji} <b>#{item['id']}</b> | <code>{html.escape(sanitized)}</code>\n"
                            f"   Successes: {item.get('success_count', 0)} | "
                            f"Failures: {item.get('failure_count', 0)}\n"
                            f"   Latency: {lat_str} | Exit IP: <code>{html.escape(ip_str)}</code>"
                        )
                        del_markup = types.InlineKeyboardMarkup()
                        del_markup.add(
                            types.InlineKeyboardButton(
                                f"🗑️ Delete #{item['id']}",
                                callback_data=f"del_proxy_{item['id']}",
                            )
                        )
                        try:
                            bot.send_message(chat_id, card, reply_markup=del_markup)
                        except Exception:
                            pass
                    time.sleep(0.3)
            return

        if text == "📊 Proxy Statistics":
            stats = db_proxy_stats()
            env_count = proxy_manager.env_count()
            avg_lat = stats.get("avg_latency")
            avg_lat_str = f"{avg_lat:.0f} ms" if avg_lat else "N/A"
            last_test = str(stats.get("last_test_time") or "Never")[:19]
            bot.send_message(
                chat_id,
                (
                    f"📊 <b>Proxy Statistics</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"📡 <b>Total (DB):</b> <code>{stats.get('total', 0)}</code>\n"
                    f"⚙️ <b>From Env:</b> <code>{env_count}</code>\n"
                    f"🟢 <b>Working:</b> <code>{stats.get('working', 0)}</code>\n"
                    f"🟡 <b>Slow:</b> <code>{stats.get('slow', 0)}</code>\n"
                    f"🔴 <b>Dead:</b> <code>{stats.get('dead', 0)}</code>\n"
                    f"⚪ <b>Untested:</b> <code>{stats.get('untested', 0)}</code>\n"
                    f"⚡ <b>Avg Latency:</b> <code>{avg_lat_str}</code>\n"
                    f"🕒 <b>Last Tested:</b> <code>{html.escape(last_test)}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━"
                ),
                reply_markup=proxy_manager_keyboard(),
            )
            return

        if text == "🔄 Retest Failed":
            _trigger_retest_failed(chat_id)
            return

        if text == "🗑️ Delete Proxy":
            with _state_lock:
                user_states[user.id] = {"step": "ADMIN_DELETE_PROXY"}
            bot.send_message(
                chat_id,
                "🗑️ <b>Delete Proxy</b>\n\nSend the numeric <b>proxy ID</b> to delete:",
                reply_markup=back_cancel_keyboard(),
            )
            return

        if text == "🗑️ Clear Dead Proxies":
            deleted = db_clear_dead_proxies()
            bot.send_message(
                chat_id,
                f"🗑️ <b>Cleared {deleted} dead proxy entries.</b>",
                reply_markup=proxy_manager_keyboard(),
            )
            return

        if text == "🗑️ Clear All Proxies":
            deleted = db_clear_all_proxies()
            bot.send_message(
                chat_id,
                f"🗑️ <b>Deleted {deleted} database proxies.</b>\nEnvironment proxies (if any) remain active.",
                reply_markup=proxy_manager_keyboard(),
            )
            return

        if text == "📈 Bot Stats":
            stats = get_admin_stats()
            proxy_count = proxy_manager.get_endpoint_count()
            proxy_stats = db_proxy_stats()
            bot.send_message(
                chat_id,
                (
                    f"📈 <b>Bot System Statistics</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"👥 <b>Total Users:</b> <code>{stats.get('total_users', 0)}</code>\n"
                    f"🔄 <b>Total Extractions:</b> <code>{stats.get('total_ex', 0)}</code>\n"
                    f"📱 <b>Total Numbers Found:</b> <code>{stats.get('total_nums', 0)}</code>\n"
                    f"🌐 <b>Active Proxies:</b> <code>{proxy_count}</code>\n"
                    f"✅ <b>Working Proxies:</b> <code>{proxy_stats.get('working', 0)}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━"
                ),
                reply_markup=admin_keyboard(),
            )
            return

        if text == "📢 Broadcast":
            with _state_lock:
                user_states[user.id] = {"awaiting_broadcast": True}
            bot.send_message(
                chat_id,
                "📢 <b>Broadcast Console</b>\n\nType the message to send to all users:\n\n"
                "<i>Tap ❌ Cancel to abort.</i>",
                reply_markup=cancel_only_keyboard(),
            )
            return

        if text == "👥 Recent Users":
            conn = get_conn()
            try:
                rows = conn.execute(
                    """SELECT user_id, first_name, username, total_numbers_found, joined_at
                       FROM users ORDER BY joined_at DESC LIMIT 10"""
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
            bot.send_message(
                chat_id, f"🔧 <b>Maintenance Mode:</b> {status}", reply_markup=admin_keyboard()
            )
            return

    # ── Standard User Menu ────────────────────────────────
    if text == "🔗 Send New Link":
        with _state_lock:
            if user.id in active_jobs:
                bot.send_message(
                    chat_id,
                    "⚠️ <b>A job is already running!</b>\nTap ❌ Cancel to stop it first.",
                    reply_markup=cancel_only_keyboard(),
                )
                return
            user_states[user.id] = {"step": "AWAITING_URL"}
        bot.send_message(
            chat_id,
            (
                "🔗 <b>Submit Target URL</b>\n\n"
                "Send a valid HTTP/HTTPS link:\n\n"
                "<i>Example:</i> <code>https://example.com/redirect</code>"
            ),
            reply_markup=cancel_only_keyboard(),
        )
        return

    if text == "📊 My Stats":
        stats = get_user_stats(user.id)
        if not stats:
            bot.send_message(chat_id, "📊 No stats yet. Run an extraction first!", reply_markup=main_keyboard())
            return
        bot.send_message(
            chat_id,
            (
                f"📊 <b>Your Statistics</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>Name:</b> {html.escape(stats.get('first_name') or 'User')}\n"
                f"🆔 <b>User ID:</b> <code>{user.id}</code>\n"
                f"🔄 <b>Total Extractions:</b> <code>{stats.get('total_extractions', 0)}</code>\n"
                f"📱 <b>Total Numbers Found:</b> <code>{stats.get('total_numbers_found', 0)}</code>\n"
                f"📅 <b>Member Since:</b> <code>{html.escape(str(stats.get('joined_at', 'N/A'))[:10])}</code>\n"
                f"━━━━━━━━━━━━━━━━━━"
            ),
            reply_markup=main_keyboard(),
        )
        return

    if text == "📋 My History":
        history = get_user_history(user.id, limit=8)
        if not history:
            bot.send_message(
                chat_id, "📋 <b>No extraction history yet.</b>", reply_markup=main_keyboard()
            )
            return
        msg = "📋 <b>Your Recent Extractions</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        for i, h in enumerate(history, 1):
            url_disp = html.escape((h["url"][:30] + "…") if len(h["url"]) > 30 else h["url"])
            completed = html.escape(str(h.get("completed_at") or "")[:16])
            mode_lbl = "🌐 IP" if h.get("mode") == "ROTATING" else "🟢 Norm"
            msg += (
                f"<b>#{i}</b> | <code>{completed}</code> [{mode_lbl}]\n"
                f"🔗 <code>{url_disp}</code>\n"
                f"🔄 Visits: <code>{h['cycles']}</code> | "
                f"📱 Numbers: <code>{h['unique_numbers']}</code>\n\n"
            )
        bot.send_message(chat_id, msg, reply_markup=main_keyboard())
        return

    if text == "❓ Help":
        socks_status = "✅ Available" if _SOCKS5_AVAILABLE else "❌ Not installed (add requests[socks])"
        bot.send_message(
            chat_id,
            (
                "❓ <b>Help & Overview</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "<b>What this bot does:</b>\n"
                "Fetches HTTP/HTTPS URLs and extracts phone numbers from redirect chains.\n\n"
                "<b>Two Extraction Modes:</b>\n"
                "• 🟢 <b>WITHOUT IP:</b> Direct server connection.\n"
                "• 🌐 <b>WITH IP ROTATION:</b> Rotates through configured proxy endpoints.\n\n"
                "<b>Proxy Support:</b>\n"
                "• HTTP / HTTPS / SOCKS5\n"
                f"• SOCKS5: {socks_status}\n\n"
                "<b>Features:</b>\n"
                "• All numbers collected silently during extraction.\n"
                "• Single final result with 📋 Copy + .txt download.\n"
                "• Cancel at any time with ❌ Cancel."
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
                "For assistance, contact the admin."
            ),
            reply_markup=main_keyboard(),
        )
        return

    # ── Flow: AWAITING_URL ────────────────────────────────
    if state.get("step") == "AWAITING_URL" or text.startswith(("http://", "https://")):
        valid, err = validate_url(text)
        if not valid:
            bot.send_message(
                chat_id,
                f"⚠️ <b>Invalid URL</b>\n\n{html.escape(err)}\n\nPlease send a valid URL.",
                reply_markup=cancel_only_keyboard(),
            )
            return

        with _state_lock:
            user_states[user.id] = {"step": "AWAITING_MODE", "url": text}

        bot.send_message(
            chat_id,
            (
                f"🔗 <b>Link Received</b>\n\n"
                f"<code>{html.escape(text[:80])}{'…' if len(text) > 80 else ''}</code>\n\n"
                f"<b>Choose Extraction Mode:</b>\n\n"
                f"🟢 <b>WITHOUT IP</b> — Direct connection.\n\n"
                f"🌐 <b>WITH IP ROTATION</b> — Rotates through configured proxy endpoints."
            ),
            reply_markup=mode_selection_keyboard(),
        )
        return

    # ── Flow: AWAITING_MODE ───────────────────────────────
    if state.get("step") == "AWAITING_MODE":
        target_url = state.get("url", "")

        if text == "🟢 WITHOUT IP":
            with _state_lock:
                user_states[user.id] = {
                    "step": "AWAITING_CYCLES",
                    "url": target_url,
                    "mode": "NORMAL",
                }
            bot.send_message(
                chat_id,
                (
                    f"🟢 <b>Normal Connection Mode</b>\n\n"
                    f"🔗 <code>{html.escape(target_url[:60])}</code>\n\n"
                    f"Select how many visits to run:"
                ),
                reply_markup=extraction_cycles_keyboard(),
            )
            return

        if text == "🌐 WITH IP ROTATION":
            if not proxy_manager.has_endpoints():
                bot.send_message(
                    chat_id,
                    (
                        "⚠️ <b>IP Rotation Unavailable</b>\n\n"
                        "No proxy endpoints are configured.\n"
                        "Admins can add proxies via <b>/admin → 🌐 Proxy Manager</b>.\n\n"
                        "<i>You can still use 🟢 WITHOUT IP mode.</i>"
                    ),
                    reply_markup=mode_selection_keyboard(),
                )
                return

            ep_count = proxy_manager.get_endpoint_count()
            with _state_lock:
                user_states[user.id] = {
                    "step": "AWAITING_CYCLES",
                    "url": target_url,
                    "mode": "ROTATING",
                }
            bot.send_message(
                chat_id,
                (
                    f"🌐 <b>IP Rotation Mode</b>\n\n"
                    f"🔗 <code>{html.escape(target_url[:60])}</code>\n"
                    f"📡 Available Proxies: <code>{ep_count}</code>\n\n"
                    f"Select how many visits to run:"
                ),
                reply_markup=extraction_cycles_keyboard(),
            )
            return

    # ── Flow: AWAITING_CYCLES ─────────────────────────────
    if state.get("step") == "AWAITING_CYCLES":
        count = _CYCLE_COUNT_MAP.get(text)
        if count is not None:
            target_url = state.get("url", "")
            mode = state.get("mode", "NORMAL")

            cancel_ev = threading.Event()
            with _state_lock:
                user_states[user.id] = {}
                active_jobs[user.id] = cancel_ev

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
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"<i>Initialising…</i>"
                ),
                reply_markup=cancel_only_keyboard(),
            )

            threading.Thread(
                target=extraction_worker,
                args=(chat_id, user.id, target_url, mode, count, start_msg.message_id, cancel_ev),
                daemon=True,
            ).start()
            return

    # ── Fallback ──────────────────────────────────────────
    bot.send_message(
        chat_id,
        "❓ Please select an option from the menu or tap <b>🔗 Send New Link</b>.",
        reply_markup=main_keyboard(),
    )


# =========================================================
# Helper: Retest Failed Proxies
# =========================================================
def _trigger_retest_failed(chat_id: int) -> None:
    """Find proxies with failures and retest them in the background."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT endpoint FROM admin_proxies
               WHERE is_active = 1
                 AND (failure_count > 0 OR last_tested IS NULL)
               ORDER BY last_tested ASC NULLS FIRST"""
        ).fetchall()
    finally:
        conn.close()

    endpoints = [r["endpoint"] for r in rows]
    if not endpoints:
        bot.send_message(chat_id, "✅ No failed/untested proxies found.", reply_markup=proxy_manager_keyboard())
        return

    status_msg = bot.send_message(
        chat_id,
        f"🔄 <b>Retesting {len(endpoints)} failed/untested proxies…</b>",
    )
    cancel_ev = threading.Event()

    def _bg():
        _run_proxy_tests_in_bg(chat_id, status_msg.message_id, endpoints, cancel_ev, detailed=False)

    threading.Thread(target=_bg, daemon=True).start()


# =========================================================
# Entry Point
# =========================================================
if __name__ == "__main__":
    init_db()

    socks_status = "✅ Available" if _SOCKS5_AVAILABLE else "❌ Not installed"
    logger.info("=" * 60)
    logger.info("URL Fetcher Bot — Starting")
    logger.info("Admins: %s", ADMIN_IDS)
    logger.info("DB: %s", DB_FILE)
    logger.info("Proxy endpoints: %d", proxy_manager.get_endpoint_count())
    logger.info("SOCKS5 support: %s", socks_status)
    logger.info("Max concurrency: %d", MAX_CONCURRENCY)
    logger.info("=" * 60)

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
