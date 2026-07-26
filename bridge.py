#!/usr/bin/env python3
"""
MeshCore ⇄ Telegram Bridge v4
Dwukierunkowa komunikacja + Web UI (FastAPI).

Usage:
  pip install meshcore httpx pyyaml fastapi uvicorn
  python3 bridge.py
"""

import asyncio, json, logging, os, sys, time, hashlib, sqlite3, threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import yaml
import httpx
import html as _html

# ── config ───────────────────────────────────────────────────
CONFIG_PATH = Path(os.environ.get("MESHCORE_BRIDGE_CONFIG", "config.yaml"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
# ──────────────────────────────────────────────────────────────

logging.basicConfig(level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("bridge")

# ── Global state ──────────────────────────────────────────────
_contact_cache: dict[str, str] = {}       # pubkey_prefix → name
_seen_nodes: dict[str, dict] = {}          # prefix → info {"ts": str}
_MAX_CONTACTS = 500
_MAX_NODES = 200
_last_sender_name: str | None = None     # most recent sender
_last_sender_key: str | None = None      # their pubkey prefix
_outbound_msgs: dict[str, float] = {}  # msg_hash → timestamp
_http: httpx.AsyncClient | None = None
_mc_cmd_lock: asyncio.Lock | None = None
_tg_offset: int = 0
_OFFSET_FILE = Path(CONFIG_PATH.parent, ".tg_offset")
_MSG_FILE = Path(CONFIG_PATH.parent, ".msg_history.json")
_DB_FILE = Path(CONFIG_PATH.parent, "msg_history.db")  # SQLite full history
_LOG_FILE = Path(CONFIG_PATH.parent, ".bridge.log")
_MAX_PERSIST_LOG = 5 * 1024 * 1024  # 5 MB log rotation
_state_lock = threading.RLock()  # Shared by async tasks, startup/shutdown code, and the log worker.
_log_io_lock = threading.Lock()
_log_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bridge-log")

# Packet tracking
_packet_observers: dict[int, set] = {}  # packet_id -> set of observer keys that acked
_packet_ack_targets: dict[str, int] = {}  # ack key -> packet_id
_packet_recent_keys: dict[str, float] = {}  # fingerprint -> last seen monotonic ts
_MAX_PACKETS = 500
_PACKET_PRUNE_EVERY = 25  # prune in batches so packet inserts stay cheap under load
_packet_inserts_since_prune = 0
_send_datagram_fn = None  # set by main() for start_web() API endpoint
_send_dm_ack_fn = None  # set by main() for handle_tg_cmd /r

def _load_offset() -> int:
    try:
        if _OFFSET_FILE.exists():
            val = int(_OFFSET_FILE.read_text().strip())
            if val > 0:
                return val
    except Exception as e:
        log.warning(f"Blad odczytu offsetu: {e}")
    return 0

def _save_offset(val: int):
    try:
        _OFFSET_FILE.write_text(str(val))
    except Exception as e:
        log.warning(f"Blad zapisu offsetu: {e}")
_mc_ref = None  # MeshCore instance reference
_last_rx_ts: float = 0.0  # last time we received ANY event or successful ping
_self_info: dict = {}  # cached SELF_INFO
_device_info: dict = {}  # cached device query
_device_info_ts: float = 0.0  # last refresh timestamp
_device_contact_count: int = 0  # cached count from device contacts
_msg_history: list[dict] = []  # structured message history for chat UI
MAX_MSG_HISTORY = 100
_rate_limits: dict[str, list[float]] = {}  # ip → list of request timestamps
_log_buffer = deque(maxlen=MAX_LOG)  # rolling log buffer for web UI
MAX_LOG = 200
RATE_LIMIT_WINDOW = 10  # 10 s keeps bursts small without penalizing normal UI polling.
RATE_SEND_MAX = 10      # 10 writes / 10 s is enough for chat use and blocks spam bursts.
RATE_GET_MAX = 60       # max GETs per window
_warn_last_ts: dict[str, float] = {}  # key -> last warning timestamp (for throttling)

WEB_PORT = int(os.environ.get("PORT", "8080"))

def _rate_check(request, limit: int) -> bool:
    """Simple sliding-window rate limiter per IP. Returns True if allowed."""
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW
    with _state_lock:
        timestamps = [t for t in _rate_limits.get(ip, []) if t > cutoff]
        if len(timestamps) >= limit:
            return False
        timestamps.append(now)
        _rate_limits[ip] = timestamps
        if len(_rate_limits) > 1000:
            # Evict the oldest inactive IP buckets instead of resetting everyone.
            oldest_ip = min(_rate_limits.items(), key=lambda item: item[1][-1] if item[1] else 0.0)[0]
            _rate_limits.pop(oldest_ip, None)
    return True


async def _mc_call(coro, timeout: float = 5):
    """Serialize MeshCore commands and apply a timeout.

    Companion Protocol documentation recommends one command at a time.
    """
    if _mc_cmd_lock is None:
        raise RuntimeError("MeshCore command lock is not initialized")
    async with _mc_cmd_lock:
        return await asyncio.wait_for(coro, timeout=timeout)

def esc(s: str) -> str:
    """Escape HTML entities in untrusted string."""
    return _html.escape(str(s), quote=True)


def _log_warn_throttled(key: str, msg: str, every_s: int = 60):
    """Log warning at most once per interval for a given key."""
    now = time.time()
    last = _warn_last_ts.get(key, 0.0)
    if now - last >= every_s:
        _warn_last_ts[key] = now
        log.warning(msg)

def _fmt_ts(ts) -> str | None:
    """Format Unix timestamp. Shows actual value + ⚠ if in the future."""
    if not ts:
        return None
    try:
        ts_f = float(ts)
        dt = datetime.fromtimestamp(ts_f)
        s = dt.strftime("%d.%m %H:%M")
        if ts_f > time.time() + 3600:
            s += " ⚠ (zegar w przyszlosci)"
        return s
    except Exception:
        return str(ts)

def _safe_json(obj):
    """Recursively convert bytes to hex strings for JSON safety."""
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_json(v) for v in obj]
    return obj

import math

def _haversine(lat1, lon1, lat2, lon2):
    """Distance in km between two lat/lon points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return round(R * c, 1)

def _persist_log_line(line: str):
    """Write a log line and rotate persisted file if needed."""
    try:
        with _log_io_lock:
            with open(_LOG_FILE, "a") as f:
                f.write(line + "\n")
            size = _LOG_FILE.stat().st_size
            if size > _MAX_PERSIST_LOG:
                # Keep last ~500 KB
                keep = _MAX_PERSIST_LOG // 10
                with open(_LOG_FILE, "rb") as f:
                    f.seek(max(0, size - keep), 0)
                    f.readline()  # skip partial line
                    tail = f.read()
                _LOG_FILE.write_bytes(tail)
    except Exception as e:
        print(f"[bridge] Log rotation failed: {e}", file=sys.stderr)


def _log(msg: str):
    log.info(msg)
    line = f"{datetime.now().strftime('%H:%M:%S')} {msg}"
    with _state_lock:
        _log_buffer.append(line)
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(_log_executor, _persist_log_line, line)
    except RuntimeError:
        # No running loop (e.g. startup/shutdown path): fallback to direct write.
        _persist_log_line(line)
    except Exception as e:
        print(f"[bridge] Log enqueue failed: {e}", file=sys.stderr)

def _save_msg_file_sync():
    try:
        _MSG_FILE.write_text(json.dumps(list(_msg_history), ensure_ascii=False))
    except Exception as e:
        log.warning(f"Nie mozna zapisac historii wiadomosci do {_MSG_FILE}: {e}")


async def _save_msg_file():
    await asyncio.to_thread(_save_msg_file_sync)

def _load_msg_file():
    try:
        if _MSG_FILE.exists():
            data = json.loads(_MSG_FILE.read_text())
            if isinstance(data, list):
                _msg_history.clear()
                _msg_history.extend(data[-MAX_MSG_HISTORY:])
            else:
                log.warning(f"Historia wiadomosci w {_MSG_FILE} ma nieoczekiwany format: {type(data).__name__}")
    except Exception as e:
        log.warning(f"Nie mozna wczytac historii wiadomosci z {_MSG_FILE}: {e}")


async def _delayed_reboot(delay_s: int = 3):
    """Reboot host after short delay, trying common Linux commands."""
    await asyncio.sleep(delay_s)
    commands = [
        ["sudo", "systemctl", "reboot"],
        ["systemctl", "reboot"],
        ["sudo", "reboot"],
        ["reboot"],
        ["sudo", "shutdown", "-r", "now"],
        ["shutdown", "-r", "now"],
    ]
    for cmd in commands:
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            rc = await asyncio.wait_for(proc.wait(), timeout=5)
            if rc == 0:
                _log(f"Host reboot command executed: {' '.join(cmd)}")
                return
        except FileNotFoundError:
            continue
        except Exception as e:
            log.warning(f"Reboot command failed ({' '.join(cmd)}): {e}")
    log.error("Unable to reboot host: no command succeeded")

# ── SQLite message history ────────────────────────────────────
def _init_db():
    """Initialize SQLite DB for full message history."""
    with sqlite3.connect(str(_DB_FILE)) as db:
        db.execute("CREATE TABLE IF NOT EXISTS messages ("
                   "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                   "ts TEXT NOT NULL,"       # ISO timestamp
                   "dir TEXT NOT NULL,"      # 'in' or 'out'
                   "ch TEXT NOT NULL,"       # 'CH0', 'DM', etc.
                   "sender TEXT NOT NULL,"
                   "text TEXT NOT NULL)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_messages_ch ON messages(ch)")
        db.execute("PRAGMA journal_mode=WAL")  # better concurrency
        db.execute("CREATE TABLE IF NOT EXISTS packets ("
                   "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                   "ts TEXT NOT NULL,"
                   "sender TEXT NOT NULL,"
                   "sender_key TEXT,"
                   "text TEXT,"
                   "ch TEXT NOT NULL,"
                   "path TEXT,"
                   "path_hops INTEGER DEFAULT 0,"
                   "snr REAL,"
                   "rssi REAL,"
                   "observers INTEGER DEFAULT 0,"
                   "observer_list TEXT,"
                   "raw_payload TEXT)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_packets_ts ON packets(ts)")
        db.commit()


def _db_call(label: str, fallback, action):
    """Run one SQLite operation with shared connection and error handling."""
    try:
        with sqlite3.connect(str(_DB_FILE)) as db:
            return action(db)
    except Exception as e:
        print(f"[bridge] {label} error: {e}", file=sys.stderr)
        return fallback

def _db_insert(direction: str, channel: str, sender: str, text: str):
    """Insert a message into SQLite history."""
    def _action(db):
            db.execute(
                "INSERT INTO messages (ts, dir, ch, sender, text) VALUES (?, ?, ?, ?, ?)",
                (datetime.now().isoformat(), direction, channel, sender, text))
            db.commit()
    _db_call("DB insert", None, _action)


async def _db_insert_async(direction: str, channel: str, sender: str, text: str):
    """Async wrapper for _db_insert offloaded to a worker thread."""
    await asyncio.to_thread(_db_insert, direction, channel, sender, text)

def _db_search(search: str = "", channel: str = "", limit: int = 100, offset: int = 0) -> list[dict]:
    """Search message history with optional filters. Returns list of dicts."""
    def _action(db):
            db.row_factory = sqlite3.Row
            query = "SELECT ts, dir, ch, sender, text FROM messages WHERE 1=1"
            params: list = []
            if search:
                query += " AND (text LIKE ? OR sender LIKE ?)"
                params.extend([f"%{search}%", f"%{search}%"])
            if channel:
                query += " AND ch = ?"
                params.append(channel)
            query += " ORDER BY id DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            rows = db.execute(query, params).fetchall()
            return [dict(r) for r in rows]
    return _db_call("DB search", [], _action)


async def _db_search_async(search: str = "", channel: str = "", limit: int = 100, offset: int = 0) -> list[dict]:
    """Async wrapper for _db_search offloaded to a worker thread."""
    return await asyncio.to_thread(_db_search, search, channel, limit, offset)

def _db_count(search: str = "", channel: str = "") -> int:
    """Count total messages matching filters."""
    def _action(db):
            query = "SELECT COUNT(*) FROM messages WHERE 1=1"
            params: list = []
            if search:
                query += " AND (text LIKE ? OR sender LIKE ?)"
                params.extend([f"%{search}%", f"%{search}%"])
            if channel:
                query += " AND ch = ?"
                params.append(channel)
            return db.execute(query, params).fetchone()[0]
    return _db_call("DB count", 0, _action)


async def _db_count_async(search: str = "", channel: str = "") -> int:
    """Async wrapper for _db_count offloaded to a worker thread."""
    return await asyncio.to_thread(_db_count, search, channel)

def _db_insert_packet(sender: str, sender_key: str, text: str, ch: str,
                      path: str, path_hops: int, snr, rssi, raw_payload: str):
    """Insert a packet trace into the packets table."""
    def _action(db):
        db.execute(
            "INSERT INTO packets (ts, sender, sender_key, text, ch, path, path_hops, snr, rssi, raw_payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (datetime.now().isoformat(), sender, sender_key, text[:200] if text else "",
             ch, path, path_hops, snr, rssi, raw_payload))
        pid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        global _packet_inserts_since_prune
        should_prune = False
        with _state_lock:
            _packet_inserts_since_prune += 1
            if _packet_inserts_since_prune >= _PACKET_PRUNE_EVERY:
                _packet_inserts_since_prune = 0
                should_prune = True
        if should_prune:
            # Use an ID cutoff instead of NOT IN over the full table, and do it only periodically.
            row = db.execute(
                "SELECT id FROM packets ORDER BY id DESC LIMIT 1 OFFSET ?",
                (_MAX_PACKETS - 1,),
            ).fetchone()
            if row:
                db.execute("DELETE FROM packets WHERE id < ?", (row[0],))
        db.commit()
        return pid
    return _db_call("Packet DB insert", None, _action)


async def _db_insert_packet_async(sender: str, sender_key: str, text: str, ch: str,
                                  path: str, path_hops: int, snr, rssi, raw_payload: str):
    """Async wrapper for _db_insert_packet offloaded to a worker thread."""
    return await asyncio.to_thread(
        _db_insert_packet, sender, sender_key, text, ch, path, path_hops, snr, rssi, raw_payload
    )

def _db_update_packet_observers(packet_id: int, count: int, obs_list: str):
    """Update observers column for a specific packet row."""
    def _action(db):
        db.execute(
            "UPDATE packets SET observers = ?, observer_list = ? WHERE id = ?",
            (count, obs_list, packet_id))
        db.commit()
    _db_call("Packet observer update", None, _action)


async def _db_update_packet_observers_async(packet_id: int, count: int, obs_list: str):
    """Async wrapper for _db_update_packet_observers offloaded to a worker thread."""
    await asyncio.to_thread(_db_update_packet_observers, packet_id, count, obs_list)

def _db_get_packets(limit: int = 100) -> list[dict]:
    """Get recent packets with observer data."""
    def _action(db):
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT id, ts, sender, sender_key, text, ch, path, path_hops, snr, rssi, observers, observer_list, raw_payload "
            "FROM packets ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        result = []
        seen = set()
        for r in rows:
            d = dict(r)
            try:
                d["raw_payload"] = json.loads(d["raw_payload"]) if d.get("raw_payload") else None
            except (json.JSONDecodeError, TypeError):
                d["raw_payload"] = d.get("raw_payload")  # keep as string if malformed
            ts_key = (d.get("ts") or "")[:16]
            dedup_key = (d.get("sender") or "", d.get("sender_key") or "", d.get("ch") or "", d.get("text") or "", ts_key)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            result.append(d)
        return result
    return _db_call("Packet DB query", [], _action)


async def _db_get_packets_async(limit: int = 100) -> list[dict]:
    """Async wrapper for _db_get_packets offloaded to a worker thread."""
    return await asyncio.to_thread(_db_get_packets, limit)

async def _push_msg(direction: str, channel: str, sender: str, text: str):
    """Push a structured message to the chat history with dedup."""
    # Dedup: skip "in" if same text+channel was just sent as "out"
    with _state_lock:
        if direction == "in":
            for i in range(len(_msg_history) - 1, max(len(_msg_history) - 20, -1), -1):
                m = _msg_history[i]
                if m["ch"] == channel and m["from"] == sender and m["text"] == text:
                    return  # duplicate (same sender+channel+text) — skip multi-repeater relay
        entry = {
            "ts": datetime.now().strftime("%H:%M:%S"),
            "dir": direction,
            "ch": channel,
            "from": sender,
            "text": text,
        }
        _msg_history.append(entry)
        if len(_msg_history) > MAX_MSG_HISTORY:
            _msg_history.pop(0)
    await _save_msg_file()
    await _db_insert_async(direction, channel, sender, text)


def load_config() -> dict:
    """Load config from YAML with cache (reloads on file change)."""
    now = time.time()
    if load_config._cache is not None and now - load_config._ts < 30:
        return load_config._cache
    try:
        mtime = os.path.getmtime(CONFIG_PATH)
        if load_config._cache is not None and mtime <= load_config._mtime:
            load_config._ts = now  # bump ts, keep cache
            return load_config._cache
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f) or {}
        load_config._cache = cfg
        load_config._ts = now
        load_config._mtime = mtime
        return cfg
    except Exception as e:
        log.warning(f"Config load error: {e}")
        return load_config._cache or {}
load_config._cache = None
load_config._ts = 0.0
load_config._mtime = 0.0

# Expected config keys with defaults (for validation)
_CONFIG_SCHEMA = {
    "telegram": {"bot_token": "", "chat_id": "", "allowed_users": []},
    "meshcore": {"connection": {"type": "tcp", "host": "localhost", "port": 5000, "auto_reconnect": True, "max_reconnect_attempts": 0}},
    "bridge": {"api_key": "", "auth": {"username": "", "password": ""}},
}

def _validate_config(cfg: dict):
    """Validate config keys, warn about missing/bad values on startup."""
    warnings = []

    # Check critical keys
    tg = cfg.get("telegram", {})
    if not tg.get("bot_token"):
        warnings.append("telegram.bot_token — puste, bot nie bedzie dzialal")
    if not tg.get("chat_id"):
        warnings.append("telegram.chat_id — puste, wiadomosci nie beda wysylane")

    # Check for unknown top-level keys
    known = set(_CONFIG_SCHEMA.keys())
    actual = set(cfg.keys())
    if actual - known:
        warnings.append(f"Nieznane sekcje configu: {', '.join(sorted(actual - known))}")

    # Warn about nested unknown keys
    for section, spec in _CONFIG_SCHEMA.items():
        if section in cfg and isinstance(cfg[section], dict):
            sec_known = set(spec.keys()) if isinstance(spec, dict) else set()
            sec_actual = set(cfg[section].keys())
            if sec_actual - sec_known:
                warnings.append(f"[{section}] nieznane klucze: {', '.join(sorted(sec_actual - sec_known))}")

    for w in warnings:
        _log(f"WARNING config: {w}")


# ── Telegram API ──────────────────────────────────────────────

async def tg_api(method: str, payload: dict = None) -> dict | None:
    cfg = load_config()
    token = cfg.get("telegram", {}).get("bot_token", "")
    if not token:
        return None
    url = f"https://api.telegram.org/bot{token}/{method}"
    MAX_TRIES = 3
    for attempt in range(MAX_TRIES):
        try:
            r = await _http.post(url, json=payload or {}, timeout=30)
            if r.status_code == 429:
                retry_after = 5  # Telegram default when field is missing
                try:
                    body = r.json()
                    retry_after = int(body.get("parameters", {}).get("retry_after", 5))
                except Exception:
                    pass
                if attempt < MAX_TRIES - 1:
                    _log_warn_throttled(
                        f"tg_api_429_{method}",
                        f"TG {method}: rate limited (429), retry in {retry_after}s (attempt {attempt + 1}/{MAX_TRIES})",
                        every_s=120,
                    )
                    await asyncio.sleep(retry_after)
                    continue
                _log_warn_throttled(
                    f"tg_api_429_exhausted_{method}",
                    f"TG {method}: rate limited (429), all {MAX_TRIES} attempts exhausted",
                    every_s=120,
                )
                return None
            if r.status_code != 200:
                body = (r.text or "").replace("\n", " ").strip()[:240]
                _log_warn_throttled(
                    f"tg_api_status_{method}_{r.status_code}",
                    f"TG {method}: HTTP {r.status_code}" + (f" body={body}" if body else ""),
                    every_s=60,
                )
                return None
            try:
                return r.json()
            except Exception as e:
                body = (r.text or "").replace("\n", " ").strip()[:240]
                _log_warn_throttled(
                    f"tg_api_json_{method}",
                    f"TG {method}: niepoprawny JSON ({e.__class__.__name__})" + (f" body={body}" if body else ""),
                    every_s=60,
                )
                return None
        except Exception as e:
            err = str(e).strip() or e.__class__.__name__
            # httpx often includes the full request URL (with bot token) in
            # network exception messages — redact it before writing to logs.
            if token:
                err = err.replace(token, "***")
            _log_warn_throttled(f"tg_api_exc_{method}", f"TG {method}: {err}", every_s=60)
            return None
    return None


def _check_tg_dedup(text: str, chat_id: str) -> bool:
    """Return True if *text* was already sent to *chat_id* within the dedup window.

    When False, the text+chat_id pair is atomically registered so the next
    call within the window is seen as a duplicate.  Callers should treat
    *both* return values as \"message is in Telegram\" — the distinction
    only matters internally to avoid re-sending within 5 min.
    """
    msg_hash = hashlib.sha256(f"{chat_id}:{text}".encode()).hexdigest()[:16]
    now = time.time()
    DEDUP_WINDOW = 300  # 5 minutes
    with _state_lock:
        stale = [k for k, t in list(_outbound_msgs.items()) if now - t > DEDUP_WINDOW]
        for k in stale:
            del _outbound_msgs[k]
        if len(_outbound_msgs) > 500:
            # Evict oldest half instead of clearing all — a blind .clear()
            # would reset dedup for every in-flight message during a burst.
            for k in list(_outbound_msgs.keys())[:250]:
                del _outbound_msgs[k]
        if msg_hash in _outbound_msgs:
            return True
        _outbound_msgs[msg_hash] = now
    return False


async def send_tg(text: str, chat_id: str = None) -> bool:
    """Send a plain-text message to Telegram.

    HTML metacharacters (<, >, &) are automatically escaped so that mesh
    messages containing those characters are never silently rejected by
    the Telegram API.  Callers that need formatting must use send_tg_html().

    Returns True when the message is known to be in Telegram — either sent
    by this call, or already sent within the 5-minute dedup window.
    Returns False only when no chat_id is configured or the API call failed.
    """
    if chat_id is None:
        chat_id = load_config().get("telegram", {}).get("chat_id", "")
    if not chat_id:
        return False
    if _check_tg_dedup(text, chat_id):
        return True   # already sent within dedup window — still a success
    safe = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    r = await tg_api("sendMessage", {
        "chat_id": chat_id, "text": safe,
        "disable_web_page_preview": True,
    })
    ok = r and r.get("ok")
    if not ok:
        log.warning(f"TG send fail: {r}")
    return bool(ok)


async def send_tg_html(html: str, chat_id: str = None) -> bool:
    """Send a pre-escaped HTML message to Telegram.

    The caller MUST escape every piece of untrusted content with esc()
    BEFORE interpolating it into the HTML string.  This function sends
    with ``parse_mode=HTML`` and trusts the caller.

    Returns True when the message is known to be in Telegram — either sent
    by this call, or already sent within the 5-minute dedup window.
    Returns False only when no chat_id is configured or the API call failed.
    """
    if chat_id is None:
        chat_id = load_config().get("telegram", {}).get("chat_id", "")
    if not chat_id:
        return False
    if _check_tg_dedup(html, chat_id):
        return True   # already sent within dedup window — still a success
    r = await tg_api("sendMessage", {
        "chat_id": chat_id, "text": html,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    })
    ok = r and r.get("ok")
    if not ok:
        log.warning(f"TG HTML send fail: {r}")
    return bool(ok)


def _payload_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]


def _extract_mesh_ids(payload: dict | None) -> list[str]:
    """Extract stable message identifiers from MeshCore payloads if present."""
    if not isinstance(payload, dict):
        return []
    keys = [
        "packet_id", "msg_id", "message_id", "id", "hash", "msg_hash",
        "packet_hash", "tx_id", "uid",
    ]
    out = []
    for k in keys:
        v = payload.get(k)
        if isinstance(v, (str, int)) and v != "":
            out.append(f"id:{k}:{v}")
    return out


def _build_ack_fingerprint(ch, sender_key, ts, text: str) -> str:
    """Build fallback ACK correlation fingerprint when no MeshCore id exists."""
    ch_s = str(ch if ch is not None else "")
    sender_s = str(sender_key if sender_key is not None else "")
    ts_s = str(ts if ts is not None else "")
    return f"fp:{ch_s}:{sender_s}:{ts_s}:{_payload_hash(text)}"


def _register_ack_target(packet_id: int, ch, sender_key, ts, text: str, response_payload: dict | None = None):
    """Register all known ACK lookup keys for an outbound packet."""
    with _state_lock:
        for k in _extract_mesh_ids(response_payload):
            _packet_ack_targets[k] = packet_id
        _packet_ack_targets[_build_ack_fingerprint(ch, sender_key, ts, text)] = packet_id


def _ack_lookup_keys(payload: dict) -> list[str]:
    """Generate candidate lookup keys from ACK payload."""
    keys = _extract_mesh_ids(payload)
    text = str(payload.get("text", "") or payload.get("msg", "") or payload.get("payload", ""))
    if text:
        ch = payload.get("ch", payload.get("channel_idx", payload.get("channel", "")))
        sender_key = payload.get("sender_key", payload.get("pubkey_prefix", payload.get("dst", payload.get("to", ""))))
        ts = payload.get("sender_timestamp", payload.get("timestamp", payload.get("ts", payload.get("time", None))))
        keys.append(_build_ack_fingerprint(ch, sender_key, ts, text))
    return keys


def _packet_fingerprint(sender: str, sender_key: str, text: str, ch: str, sender_timestamp=None) -> str:
    """Build a stable fingerprint for packet deduplication."""
    ts_part = ""
    try:
        if sender_timestamp is not None and str(sender_timestamp).strip() != "":
            ts_part = str(int(float(sender_timestamp)))
    except Exception:
        ts_part = ""
    if not ts_part:
        # No sender timestamp — fall back to the local epoch second so
        # that two packets traversing the same route at different times
        # cannot collide.  _payload_hash(path_str) was NOT a valid
        # substitute because path alone carries no temporal information.
        ts_part = str(int(time.time()))
    return f"{sender}|{sender_key}|{ch}|{ts_part}|{_payload_hash(text)}"


# ── MeshCore handlers ────────────────────────────────────────

async def _track_packet(sender: str, sender_key: str, text: str, ch: str, path_str: str, path_hops: int, snr, rssi, sender_timestamp=None, *, is_outbound: bool = False):
    """Record packet metadata to SQLite."""
    try:
        now = time.monotonic()
        fp = _packet_fingerprint(sender, sender_key, text, ch, sender_timestamp)
        dedup_window_s = 180
        with _state_lock:
            stale_keys = [k for k, t in list(_packet_recent_keys.items()) if now - t > dedup_window_s]
            for k in stale_keys:
                del _packet_recent_keys[k]
            if fp in _packet_recent_keys:
                return None
            _packet_recent_keys[fp] = now

        payload = json.dumps({"sender": sender, "ch": ch, "snr": snr, "rssi": rssi, "path": path_str})
        pid = await _db_insert_packet_async(sender, sender_key, text, ch, path_str, path_hops, snr, rssi, payload)
        # Observer setup + cleanup in one critical section (no await between them).
        if pid:
            with _state_lock:
                if is_outbound:
                    _packet_observers.setdefault(pid, set())
                if len(_packet_observers) > 200:
                    stale_ids = set(list(_packet_observers.keys())[:100])
                    for packet_id in stale_ids:
                        del _packet_observers[packet_id]
                    if stale_ids:
                        for k, v in list(_packet_ack_targets.items()):
                            if v in stale_ids:
                                del _packet_ack_targets[k]
                if len(_packet_ack_targets) > 500:
                    for k in list(_packet_ack_targets.keys())[:250]:
                        del _packet_ack_targets[k]
        return pid
    except Exception as e:
        print(f"[bridge] Packet track error: {e}", file=sys.stderr)
        return None

async def on_contact_message(mc, event):
    global _last_rx_ts, _last_sender_name, _last_sender_key
    _last_rx_ts = time.time()
    p = event.payload
    if not isinstance(p, dict):
        return
    text = p.get("text", "").strip()
    if not text:
        return
    pk = p.get("pubkey_prefix", "??????")
    ts = p.get("sender_timestamp", 0)
    snr = p.get("SNR", None)
    sender = await _resolve_name(mc, pk)
    _last_sender_name = sender
    _last_sender_key = pk
    t = _fmt_ts(ts) or datetime.now().strftime("%d.%m %H:%M")
    s = f" [{snr:.1f}dB]" if snr is not None else ""
    msg = f"📡 <b>MeshCore</b> {t}\n👤 {esc(sender)}{s}\n\n{esc(text)}\n\n\u2014\n💬 Odpisz: /r {esc(sender)} <tekst>"
    _log(f"<- od {sender}: {text[:60]}" + (f" [{snr:.1f}dB]" if snr is not None else ""))
    await _push_msg("in", "DM", sender, text)
    # Track packet
    path_str = p.get("path", "")
    path_hops = len(path_str) // 12 if path_str else 0  # 12 hex chars per hop (6-byte pubkey prefix)
    rssi = p.get("RSSI", None)
    await _track_packet(sender, pk, text, "DM", path_str, path_hops, snr, rssi, ts)
    await send_tg_html(msg)


async def on_channel_message(mc, event):
    global _last_rx_ts
    _last_rx_ts = time.time()
    p = event.payload
    if not isinstance(p, dict):
        return
    text = p.get("text", "").strip()
    if not text:
        return
    ch = p.get("channel_idx", "?")
    ts = p.get("sender_timestamp", 0)
    pk = p.get("pubkey_prefix") or p.get("pub_key", "")
    if not pk:
        # Channel messages don't carry sender pubkey — try path
        path = p.get("path", "")
        pk = path[:6] if path else ""
    if not pk:
        sender = "Nieznany"
        # Some devices prepend "Name: " to channel messages because the
        # protocol doesn't carry sender metadata.  Strip this prefix from
        # the displayed text so impersonation attempts ("Admin: ALERT")
        # aren't echoed verbatim — but NEVER use it as the sender identity.
        stripped = text
        if ":" in text:
            maybe_name = text.split(":")[0].strip()
            if maybe_name and len(maybe_name) < 20 and " " not in maybe_name and not maybe_name.startswith("http"):
                stripped = text.split(":", 1)[1].strip()
                if not stripped:
                    stripped = text  # don't blank the whole message
    else:
        sender = await _resolve_name(mc, pk)
        stripped = text
    t = _fmt_ts(ts) or datetime.now().strftime("%d.%m %H:%M")
    msg = f"📢 <b>Kanal {ch}</b> {t}\n👤 {esc(sender)}\n\n{esc(stripped)}"
    _log(f"<- kanal{ch} {sender}: {stripped[:60]}")
    await _push_msg("in", f"CH{ch}", sender, stripped)
    # Track packet
    path_str = p.get("path", "")
    path_hops = len(path_str) // 12 if path_str else 0  # 12 hex chars per hop (6-byte pubkey prefix)
    rssi = p.get("RSSI", None)
    ch_snr = p.get("SNR", None)
    ch_label = f"CH{ch}"
    await _track_packet(sender, pk, stripped, ch_label, path_str, path_hops, ch_snr, rssi, ts)
    await send_tg_html(msg)


async def on_ack(mc, event):
    """Track acknowledgements from repeaters for sent messages."""
    global _last_rx_ts
    _last_rx_ts = time.time()
    p = event.payload
    if isinstance(p, dict):
        key = p.get("from", "")[:8]
        text = p.get("text", "")
        if text and key:
            _log(f"ACK od {key}: {text[:40]}")
            # Update packet observer set by packet_id via id/fingerprint correlation.
            packet_id = None
            with _state_lock:
                for k in _ack_lookup_keys(p):
                    if k in _packet_ack_targets:
                        packet_id = _packet_ack_targets[k]
                        break
                if packet_id:
                    # Create observer set if _track_packet hasn't finished yet
                    # (covers the race window between DB insert and observer setup).
                    _packet_observers.setdefault(packet_id, set())
                    _packet_observers[packet_id].add(key)
                    observers = _packet_observers[packet_id]
                else:
                    observers = None
            if packet_id and observers is not None:
                await _db_update_packet_observers_async(packet_id, len(observers), ",".join(sorted(observers)))

async def on_self_info(mc, event):
    global _self_info, _last_rx_ts
    _last_rx_ts = time.time()
    p = event.payload
    if isinstance(p, dict):
        _self_info = p
        name = p.get("name", "?")
        freq = p.get("radio_freq", 0)
        sf = p.get("radio_sf", "?")
        s = f" {freq:.1f}MHz SF{sf}" if freq else ""
        _log(f"Polaczono z: {name}{s}")
        await send_tg_html(f"🟢 <b>MeshCore Bridge</b>\n📟 {name}{s}")


async def on_advert(mc, event):
    global _last_rx_ts
    _last_rx_ts = time.time()
    p = event.payload
    if isinstance(p, dict) and p.get("public_key"):
        prefix = p["public_key"][:12]
        ts = datetime.now().strftime("%d.%m %H:%M")
        now = time.time()
        NODE_MAX_AGE = 14 * 86400  # 14 dni
        with _state_lock:
            if prefix not in _seen_nodes:
                _seen_nodes[prefix] = {"ts": ts, "seen_at": now, "lat": p.get("adv_lat"), "lon": p.get("adv_lon")}
                new_node = True
                # Usuń nody starsze niż 14 dni
                cutoff = now - NODE_MAX_AGE
                stale = [k for k, v in _seen_nodes.items() if v.get("seen_at", 0) < cutoff]
                for k in stale:
                    del _seen_nodes[k]
                # Limit liczbowy jako fallback
                if len(_seen_nodes) > _MAX_NODES:
                    for k in list(_seen_nodes.keys())[:_MAX_NODES // 3]:
                        del _seen_nodes[k]
            else:
                _seen_nodes[prefix]["ts"] = ts
                _seen_nodes[prefix]["seen_at"] = now
                if p.get("adv_lat") is not None:
                    _seen_nodes[prefix]["lat"] = p.get("adv_lat")
                if p.get("adv_lon") is not None:
                    _seen_nodes[prefix]["lon"] = p.get("adv_lon")
                new_node = False
        if new_node:
            _log(f"Nowy node: {prefix[:8]}")


async def _resolve_name(mc, prefix: str) -> str:
    with _state_lock:
        cached = _contact_cache.get(prefix)
    if cached:
        return cached
    try:
        c = await mc.get_contact_by_key_prefix(prefix)
        name = (c.get("adv_name", "") or c.get("name", "") or prefix[:8]) if c else prefix[:8]
    except Exception as e:
        log.warning(f"Nie mozna rozpoznac kontaktu {prefix}: {e}")
        name = prefix[:8]
    with _state_lock:
        _contact_cache[prefix] = name
        if len(_contact_cache) > _MAX_CONTACTS:
            # keep newest half
            for k in list(_contact_cache.keys())[:_MAX_CONTACTS // 2]:
                del _contact_cache[k]
    return name


# ── Telegram polling ──────────────────────────────────────────

async def handle_tg_cmd(mc, text: str):
    text = text.strip()
    if text.startswith("/help") or text == "/start":
        await send_tg_html(
            "🤖 <b>MeshCore Bridge</b>\n\n"
            "/r &lt;tekst&gt; \u2014 odpowiedz ostatniemu\n"
            "/r &lt;nazwa&gt; &lt;tekst&gt; \u2014 do kontaktu\n"
            "/ch &lt;tekst&gt; — wyslij na kanal 0\n"
            "/ch &lt;nr&gt; &lt;tekst&gt; — na konkretny kanal\n"
            "/channel — to samo co /ch\n"
            "/contacts \u2014 kontakty + nody\n"
            "/status \u2014 status\n"
            "/help \u2014 pomoc")
        return
    if text == "/status":
        bat = "?"
        try:
            r = await _mc_call(mc.commands.get_bat(), timeout=5)
            if r.type.name != "ERROR":
                bat = r.payload.get("level", "?")
        except Exception:
            pass
        with _state_lock:
            contacts_count = len(_contact_cache)
            nodes_count = len(_seen_nodes)
        await send_tg_html(
            f"📊 <b>Status</b>\n"
            f"Polaczony: {'tak' if mc.is_connected else 'nie'}\n"
            f"Bateria: {bat}%\n"
            f"Kontakty: {contacts_count}\n"
            f"Nody: {nodes_count}")
        return
    if text == "/contacts":
        try:
            r = await _mc_call(mc.commands.get_contacts(), timeout=10)
            contacts = r.payload or {} if r.type.name != "ERROR" else {}
            lines = [f"<b>Kontakty ({len(contacts)})</b>"]
            for key, c in list(contacts.items())[:20]:
                n = c.get("adv_name", "") or c.get("name", "") or key[:8]
                lines.append(f"  \u2022 {n}")
            if _seen_nodes:
                lines.append(f"\n<b>Nody ({len(_seen_nodes)})</b>")
                for pfx in sorted(_seen_nodes)[:20]:
                    lines.append(f"  \u2022 {pfx[:8]}")
            await send_tg_html("\n".join(lines))
        except Exception as e:
            await send_tg(f"Blad: {e}")
        return
    if text == "/channel" or text.startswith("/channel "):
        parts = text.split(maxsplit=1)
        try:
            ch = int(parts[1]) if len(parts) > 1 else 0
        except ValueError:
            await send_tg("Uzycie: /channel [nr]")
            return
        try:
            r = await _mc_call(mc.commands.get_channel(ch), timeout=5)
            if r.type.name == "ERROR":
                await send_tg(f"Blad kanal{ch}: {r.payload.get('reason','?')}")
                return
            c = r.payload or {}
            name = c.get("name", "") or c.get("channel_name", "") or "?"
            secret = c.get("secret", "") or c.get("channel_secret", "")
            secret_state = "publiczny" if secret and set(str(secret)) <= {"0"} else "ustawiony"
            await send_tg_html(
                f"📶 <b>Kanal {ch}</b>\n"
                f"Nazwa: {esc(name)}\n"
                f"Sekret: {esc(secret_state)}\n"
                f"Surowe: <code>{esc(str(c))}</code>"
            )
        except Exception as e:
            await send_tg(f"Blad: {e}")
        return
    if text == "/ch" or text.startswith("/ch "):
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            await send_tg("Uzycie: /ch <tekst> lub /ch <nr> <tekst>")
            return
        if len(parts) == 2:
            ch, txt = 0, parts[1]
        else:
            try:
                ch = int(parts[1]); txt = parts[2]
            except ValueError:
                await send_tg("Nieprawidlowy numer kanalu. Uzycie: /ch <tekst> lub /ch <nr> <tekst>")
                return
        try:
            txt = txt[:200]
            send_ts = int(time.time())
            r = await _mc_call(mc.commands.send_chan_msg(ch, txt), timeout=5)
            if r.type.name == "ERROR":
                await send_tg(f"Blad kanal{ch}: {r.payload.get('reason','?')}")
            else:
                _log(f"-> kanal{ch}: {txt[:60]}")
                await _push_msg("out", f"CH{ch}", "TG", txt)
                pid = await _track_packet("TG", "", txt, f"CH{ch}", "", 0, None, None, send_ts, is_outbound=True)
                if pid:
                    _register_ack_target(pid, f"CH{ch}", "", send_ts, txt, getattr(r, "payload", None))
                await send_tg_html(f"📤 <b>Kanal {ch}</b>\n{esc(txt)}")
        except Exception as e:
            await send_tg(f"Blad: {e}")
        return
    if text == "/r" or text.startswith("/r "):
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            await send_tg("Uzycie: /r <tekst> lub /r <nazwa> <tekst>")
            return
        if len(parts) == 2:
            if not _last_sender_name:
                await send_tg("Brak ostatniego nadawcy")
                return
            target, key = _last_sender_name, _last_sender_key
            txt = parts[1]
        else:
            rest = parts[1:]
            key = None
            target = ""
            txt = " ".join(rest)
            for split_at in range(len(rest) - 1, 0, -1):
                candidate = " ".join(rest[:split_at])
                candidate_txt = " ".join(rest[split_at:])
                with _state_lock:
                    candidate_key = next((p for p, n in _contact_cache.items() if n.lower() == candidate.lower()), None)
                if not candidate_key:
                    try:
                        c = await mc.get_contact_by_name(candidate)
                        if c:
                            candidate_key = c.get("public_key", "")[:12]
                    except Exception as e:
                        log.warning(f"/r: nie znaleziono kontaktu '{candidate}': {e}")
                if candidate_key:
                    target = candidate
                    key = candidate_key
                    txt = candidate_txt
                    break
            if not key:
                await send_tg(f"Nie znaleziono kontaktu: {' '.join(rest[:-1])}")
                return
        if not key:
            await send_tg("Brak klucza odbiorcy")
            return
        if not _send_dm_ack_fn:
            await send_tg("Blad: bridge niegotowy (brak polaczenia?)")
            return
        try:
            send_ts = int(time.time())
            r = await _send_dm_ack_fn(key, txt[:200])
            if r.type.name == "ERROR":
                await send_tg(f"Blad: {r.payload.get('reason','?')}")
            else:
                _log(f"-> do {target}: {txt[:60]}")
                await _push_msg("out", "DM", target, txt)
                pid = await _track_packet("TG", key, txt, "DM", "", 0, None, None, send_ts, is_outbound=True)
                if pid:
                    _register_ack_target(pid, "DM", key, send_ts, txt, getattr(r, "payload", None))
                await send_tg_html(f"📤 <b>Do {esc(target)}</b>\n{esc(txt)}")
        except Exception as e:
            await send_tg(f"Blad: {e}")
        return
    await send_tg(f"Nieznane: {text}\n/help")


async def tg_poll_loop(mc):
    global _tg_offset
    _tg_offset = _load_offset()
    _log(f"Telegram polling: start (offset={_tg_offset})")
    while True:
        try:
            cfg = load_config()
            tg = cfg.get("telegram", {})
            chat_id = str(tg.get("chat_id", ""))
            allowed = tg.get("allowed_users", [])
            if not allowed:
                allowed = [int(chat_id)] if chat_id else []
            if not chat_id:
                await asyncio.sleep(5); continue
            r = await tg_api("getUpdates", {
                "offset": _tg_offset, "timeout": 5, "allowed_updates": ["message"]})
            if r and r.get("ok") and r.get("result"):
                for upd in r["result"]:
                    _tg_offset = upd["update_id"] + 1
                    _save_offset(_tg_offset)
                    msg = upd.get("message", {})
                    if str(msg.get("chat", {}).get("id", "")) == chat_id:
                        uid = msg.get("from", {}).get("id")
                        if uid not in allowed:
                            _log(f"TG odrzucone od uid={uid}: {msg.get('text','')[:40]}")
                            continue
                        text = msg.get("text", "").strip()
                        if text and (text.startswith("/") or text.startswith("!")):
                            _log(f"TG cmd: {text}")
                            await handle_tg_cmd(mc, text)
        except Exception as e:
            err = str(e).strip() or e.__class__.__name__
            _log_warn_throttled("tg_poll_loop", f"TG poll: {err}", every_s=60)
        await asyncio.sleep(2)


# ── Web UI (FastAPI) ──────────────────────────────────────────

_session_token: str = ""

LOGIN_FORM = r"""
<div id="login-overlay" class="login-overlay">
  <div class="login-card">
    <div class="login-mark">
      <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M12 2v4M12 18v4M4.9 4.9l2.8 2.8M16.3 16.3l2.8 2.8M2 12h4M18 12h4M4.9 19.1l2.8-2.8M16.3 7.7l2.8-2.8"/><circle cx="12" cy="12" r="3"/></svg>
    </div>
    <h1>MeshCore Bridge</h1>
    <p class="login-sub">Zaloguj się, aby zarządzać mostem</p>
    <div class="field"><label>Użytkownik</label><input id="login-user" type="text" autocomplete="username"></div>
    <div class="field"><label>Hasło</label><input id="login-pass" type="password" autocomplete="current-password" onkeydown="if(event.key==='Enter')doLogin()"></div>
    <button class="btn btn-primary btn-block" onclick="doLogin()">Zaloguj</button>
    <div id="login-err" class="login-err"></div>
  </div>
</div>
<script>
async function doLogin(){
  const u=document.getElementById('login-user').value,p=document.getElementById('login-pass').value;
  const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u,password:p})});
  const d=await r.json();
  if(d.ok){document.getElementById('login-overlay').style.display='none';startPolling()}
  else{const e=document.getElementById('login-err');e.textContent=d.error||'Blad logowania';e.style.display='block'}
}
function startPolling(){load();loadLog();loadDeviceCards();window._p1=setInterval(load,5000);window._p2=setInterval(loadLog,2000);loadContacts();}
function showLoginAgain(){document.getElementById('login-overlay').style.display='flex';clearInterval(window._p1);clearInterval(window._p2);}
// Auto-check if session is already active (fires after LOGIN_FORM is in DOM)
fetch('/api/ping').then(r=>r.json()).then(d=>{if(d.auth){document.getElementById('login-overlay').style.display='none';startPolling()}}).catch(()=>{});
</script>
"""
# ── No-auth mode: auto-start polling (only when login form NOT present) ──
NOAUTH_START = """
<script>
load();loadLog();loadDeviceCards();setInterval(load,5000);setInterval(loadLog,2000);loadContacts();
</script>
"""

WEB_HTML = r"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MeshCore Bridge</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<style>
:root{
  --bg:#0a0e13;
  --bg-1:#0f141b;
  --bg-2:#151b23;
  --bg-3:#1b222c;
  --border:#232b36;
  --border-soft:#1a212a;
  --text:#e6ecf3;
  --text-dim:#8993a3;
  --text-faint:#576172;
  --accent:#e2a34e;
  --accent-2:#d4913a;
  --accent-soft:rgba(226,163,78,.14);
  --good:#4fd193;
  --good-soft:rgba(79,209,147,.14);
  --bad:#f16565;
  --bad-soft:rgba(241,101,101,.14);
  --warn:#eecb56;
  --font-sans:'Inter',system-ui,-apple-system,sans-serif;
  --font-mono:'IBM Plex Mono',ui-monospace,'SFMono-Regular',monospace;
  --radius:10px;
  --radius-sm:7px;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{font-family:var(--font-sans);background:var(--bg);color:var(--text);font-size:14px;-webkit-font-smoothing:antialiased}
::selection{background:var(--accent-soft);color:var(--accent)}
::-webkit-scrollbar{width:8px;height:8px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px}
::-webkit-scrollbar-thumb:hover{background:var(--text-faint)}
a{color:inherit;text-decoration:none}
button{font-family:inherit;cursor:pointer}
input,select{font-family:inherit}
hr{border:none;border-top:1px solid var(--border-soft)}

.shell{display:flex;min-height:100vh}

/* sidebar */
.sidebar{width:212px;flex-shrink:0;background:var(--bg-1);border-right:1px solid var(--border-soft);display:flex;flex-direction:column;padding:18px 12px;position:sticky;top:0;height:100vh}
.brand{display:flex;align-items:center;gap:9px;padding:6px 8px 20px;color:var(--accent)}
.brand svg{flex-shrink:0}
.brand span{font-family:var(--font-mono);font-weight:600;font-size:13px;letter-spacing:.09em;color:var(--text)}
.nav{display:flex;flex-direction:column;gap:2px;flex:1}
.nav-item{display:flex;align-items:center;gap:10px;padding:9px 10px;border-radius:var(--radius-sm);color:var(--text-dim);font-size:13.5px;font-weight:500;border-left:2px solid transparent}
.nav-item svg{flex-shrink:0;opacity:.85}
.nav-item:hover{background:var(--bg-2);color:var(--text)}
.nav-item.active{background:var(--accent-soft);color:var(--accent);border-left-color:var(--accent)}
.sidebar-foot{display:flex;flex-direction:column;gap:10px;padding:10px 8px 4px;border-top:1px solid var(--border-soft);margin-top:8px}
.conn-chip{display:flex;align-items:center;gap:7px;font-size:12px;color:var(--text-dim)}
.dot{width:7px;height:7px;border-radius:50%;background:var(--text-faint);flex-shrink:0}
.dot.on{background:var(--good);box-shadow:0 0 0 3px var(--good-soft)}
.dot.off{background:var(--bad);box-shadow:0 0 0 3px var(--bad-soft)}
.clock{font-family:var(--font-mono);font-size:12px;color:var(--text-faint);letter-spacing:.03em}

/* main */
.main{flex:1;min-width:0;display:flex;flex-direction:column}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:20px 28px;border-bottom:1px solid var(--border-soft);position:sticky;top:0;background:rgba(10,14,19,.85);backdrop-filter:blur(6px);z-index:5}
.topbar h1{font-size:17px;font-weight:600;letter-spacing:-.01em}
#app{padding:24px 28px 60px}
.page{display:flex;flex-direction:column;gap:20px}

/* panel */
.panel{background:var(--bg-1);border:1px solid var(--border-soft);border-radius:var(--radius)}
.panel-head{display:flex;align-items:center;justify-content:space-between;padding:13px 16px;border-bottom:1px solid var(--border-soft)}
.panel-head h2{font-size:12.5px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--text-dim)}
.panel-body{padding:14px 16px}
.panel-actions{display:flex;gap:8px}

/* stat / cfg grids */
.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.stat-card{background:var(--bg-1);border:1px solid var(--border-soft);border-left:2px solid var(--border);border-radius:var(--radius-sm);padding:12px 14px}
.stat-card.k-good{border-left-color:var(--good)}
.stat-card.k-warn{border-left-color:var(--warn)}
.stat-card.k-bad{border-left-color:var(--bad)}
.stat-card.k-accent{border-left-color:var(--accent)}
.stat-value{font-family:var(--font-mono);font-size:19px;font-weight:600}
.stat-label{font-size:11.5px;color:var(--text-faint);margin-top:3px;text-transform:uppercase;letter-spacing:.05em}
.cfg-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));gap:14px}

/* buttons */
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 14px;border-radius:var(--radius-sm);border:1px solid var(--border);background:var(--bg-2);color:var(--text);font-size:13px;font-weight:500;transition:border-color .15s,background .15s}
.btn:hover{border-color:var(--text-faint);background:var(--bg-3)}
.btn-primary{background:var(--accent-soft);border-color:transparent;color:var(--accent)}
.btn-primary:hover{background:var(--accent);color:#1a1206}
.btn-danger{background:var(--bad-soft);border-color:transparent;color:var(--bad)}
.btn-danger:hover{background:var(--bad);color:#2a0a0a}
.btn-block{width:100%;justify-content:center}
.btn-sm{padding:6px 10px;font-size:12px}

/* inputs */
.field{margin-bottom:12px}
.field label{display:block;font-size:11.5px;color:var(--text-faint);margin-bottom:5px;text-transform:uppercase;letter-spacing:.05em}
.hint{font-size:11px;color:var(--text-faint);margin-top:4px;font-weight:400}
input[type=text],input[type=password],input[type=number],select,textarea{width:100%;padding:9px 11px;border-radius:var(--radius-sm);border:1px solid var(--border);background:var(--bg);color:var(--text);font-size:13px}
input:focus,select:focus,textarea:focus{outline:none;border-color:var(--accent)}
select{appearance:none;background-image:linear-gradient(45deg,transparent 50%,var(--text-faint) 50%),linear-gradient(135deg,var(--text-faint) 50%,transparent 50%);background-position:calc(100% - 16px) center,calc(100% - 11px) center;background-size:5px 5px,5px 5px;background-repeat:no-repeat}
.row-2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
label.check{display:flex;align-items:center;gap:8px;font-size:12.5px;color:var(--text-dim);margin-bottom:10px}
label.check span.note{color:var(--text-faint);font-weight:400}

/* table */
table{width:100%;border-collapse:collapse;font-size:12.5px}
th{text-align:left;padding:8px 10px;color:var(--text-faint);font-weight:500;text-transform:uppercase;font-size:10.5px;letter-spacing:.06em;border-bottom:1px solid var(--border-soft)}
td{padding:8px 10px;border-bottom:1px solid var(--border-soft)}
tr:last-child td{border-bottom:none}
.sortable{cursor:pointer;user-select:none}
.sortable:hover{color:var(--text)}
.mono{font-family:var(--font-mono)}

/* chips */
.chip-row{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}
.chip{padding:7px 13px;border-radius:99px;border:1px solid var(--border);background:var(--bg-2);color:var(--text-dim);font-size:12.5px;font-weight:600;font-family:var(--font-mono)}
.chip.active{background:var(--accent);border-color:var(--accent);color:#1a1206}

/* log */
.log-box{background:var(--bg);border:1px solid var(--border-soft);border-radius:var(--radius-sm);padding:10px 12px;height:280px;overflow:auto;font-family:var(--font-mono);font-size:11.5px;line-height:1.7;color:var(--text-dim);white-space:pre-wrap;word-break:break-word}
.log-box .t{color:var(--text-faint);margin-right:8px}

/* map */
#map{height:340px;border-radius:var(--radius-sm);border:1px solid var(--border-soft)}

/* contacts */
.contacts-scroll{max-height:320px;overflow:auto}
.search-bar{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
.search-bar input{flex:1;min-width:180px}

/* drawer */
.drawer{background:var(--bg-2);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-top:14px}
.drawer-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.drawer-head h3{font-size:14px;color:var(--accent)}
.drawer-close{background:none;border:none;color:var(--text-faint);font-size:20px;line-height:1}
.kv{width:100%;font-size:12.5px}
.kv td{padding:6px 4px;border-bottom:1px solid var(--border-soft);vertical-align:top}
.kv td:first-child{color:var(--text-faint);width:150px}
pre.raw{background:var(--bg);border:1px solid var(--border-soft);border-radius:var(--radius-sm);padding:10px;font-size:11px;max-height:200px;overflow:auto;color:var(--text-dim);margin-top:8px;font-family:var(--font-mono);white-space:pre-wrap;word-break:break-word}

/* chat */
.chat-wrap{display:flex;flex-direction:column;height:calc(100vh - 170px)}
.chat-msgs{flex:1;overflow-y:auto;background:var(--bg);border:1px solid var(--border-soft);border-radius:var(--radius);padding:14px;margin-bottom:12px;display:flex;flex-direction:column;gap:8px}
.bubble{max-width:72%;padding:9px 12px;border-radius:10px;font-size:13px;line-height:1.5;border:1px solid var(--border-soft)}
.bubble.out{align-self:flex-end;background:var(--accent-soft);border-color:transparent}
.bubble.in{align-self:flex-start;background:var(--bg-2)}
.bubble .meta{font-size:10.5px;color:var(--text-faint);margin-bottom:3px;font-family:var(--font-mono)}
.bubble.out .meta{color:var(--accent-2)}
.chat-input-row{display:flex;gap:8px}
.empty{color:var(--text-faint);text-align:center;padding:40px 10px;font-size:13px}

/* history */
.hist-list{display:flex;flex-direction:column;gap:6px}
.hist-row{padding:8px 10px;border:1px solid var(--border-soft);border-radius:var(--radius-sm);font-size:12.5px;background:var(--bg)}
.hist-row .t{font-family:var(--font-mono);color:var(--text-faint);margin-right:8px;font-size:11px}
.pager{display:flex;align-items:center;gap:10px;margin-top:12px;font-size:12.5px;color:var(--text-faint)}

/* login */
.login-overlay{position:fixed;inset:0;background:var(--bg);display:flex;align-items:center;justify-content:center;z-index:999}
.login-card{width:320px;background:var(--bg-1);border:1px solid var(--border-soft);border-radius:var(--radius);padding:28px}
.login-mark{color:var(--accent);margin-bottom:14px}
.login-card h1{font-size:16px;margin-bottom:4px}
.login-sub{font-size:12.5px;color:var(--text-faint);margin-bottom:18px}
.login-err{color:var(--bad);font-size:12px;margin-top:10px;display:none}

/* toast */
#toast-wrap{position:fixed;bottom:18px;right:18px;display:flex;flex-direction:column;gap:8px;z-index:1000}
.toast{background:var(--bg-2);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:var(--radius-sm);padding:10px 14px;font-size:12.5px;min-width:220px;box-shadow:0 6px 18px rgba(0,0,0,.35)}
.toast.bad{border-left-color:var(--bad)}
.toast.good{border-left-color:var(--good)}

@media(max-width:860px){
  .sidebar{position:fixed;left:0;bottom:0;top:auto;width:100%;height:auto;flex-direction:row;border-right:none;border-top:1px solid var(--border-soft);padding:8px 10px;z-index:50}
  .brand,.sidebar-foot{display:none}
  .nav{flex-direction:row;justify-content:space-around;flex:1}
  .nav-item{flex-direction:column;gap:2px;font-size:10.5px;border-left:none;border-top:2px solid transparent}
  .nav-item.active{border-top-color:var(--accent);border-left:none}
  .main{padding-bottom:64px}
  #app{padding:16px}
  .topbar{padding:14px 16px}
  .chat-wrap{height:calc(100vh - 220px)}
}
</style>
</head>
<body>
<div class="shell">
  <aside class="sidebar">
    <div class="brand">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M12 2v4M12 18v4M4.9 4.9l2.8 2.8M16.3 16.3l2.8 2.8M2 12h4M18 12h4M4.9 19.1l2.8-2.8M16.3 7.7l2.8-2.8"/><circle cx="12" cy="12" r="3"/></svg>
      <span>MESHBRIDGE</span>
    </div>
    <nav class="nav">
      <a href="/" class="nav-item active" data-page="dashboard">
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>
        Panel
      </a>
      <a href="/chat" class="nav-item" data-page="chat">
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M21 11.5a8.4 8.4 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.4 8.4 0 0 1-3.8-.9L3 21l1.9-5.7a8.4 8.4 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.4 8.4 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/></svg>
        Czat
      </a>
      <a href="/config" class="nav-item" data-page="config">
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M1 14h6M9 8h6M17 16h6"/></svg>
        Konfiguracja
      </a>
      <a href="/history" class="nav-item" data-page="history">
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>
        Historia
      </a>
      <a href="/packets" class="nav-item" data-page="packets">
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M4 6h16M4 12h16M4 18h16"/><circle cx="8" cy="6" r="1"/><circle cx="8" cy="12" r="1"/><circle cx="8" cy="18" r="1"/></svg>
        Pakiety
      </a>
    </nav>
    <div class="sidebar-foot">
      <div class="conn-chip"><span class="dot" id="conn-dot"></span><span id="conn-text">łączenie…</span></div>
      <div class="clock" id="clock">--:--:--</div>
    </div>
  </aside>

  <div class="main">
    <header class="topbar">
      <h1 id="page-title">Panel</h1>
    </header>

    <div id="app">
      <div id="page-dashboard" class="page">
        <div class="stat-grid" id="stats"></div>

        <div class="panel">
          <div class="panel-head"><h2>Host</h2></div>
          <div class="panel-body"><div class="stat-grid" id="sys-cards"></div></div>
        </div>

        <div class="panel">
          <div class="panel-head"><h2>Urządzenie</h2>
            <div class="panel-actions">
              <button class="btn btn-sm" onclick="fetch('/api/device/advert',{method:'POST'}).then(r=>r.json()).then(d=>toast(d.ok?'Advert wysłany':'Błąd wysyłki advertu',d.ok?'good':'bad'))">Wyślij advert</button>
              <button class="btn btn-sm" onclick="loadDeviceCards()">Odśwież</button>
            </div>
          </div>
          <div class="panel-body"><div class="stat-grid" id="device-cards"></div></div>
        </div>

        <div class="panel">
          <div class="panel-head"><h2>Mapa sieci</h2></div>
          <div class="panel-body"><div id="map"></div></div>
        </div>

        <div class="panel">
          <div class="panel-head"><h2>Nody</h2></div>
          <div class="panel-body"><table id="nodes"><tr><th>Node</th><th>Widziany</th><th>Odległość</th></tr></table></div>
        </div>

        <div class="panel">
          <div class="panel-head"><h2>Kontakty</h2></div>
          <div class="panel-body">
            <div class="contacts-scroll"><table id="contacts-table"></table></div>
            <div id="contact-detail" class="drawer" style="display:none">
              <div class="drawer-head">
                <h3 id="cd-name"></h3>
                <button class="drawer-close" onclick="closeDetail()">&times;</button>
              </div>
              <table class="kv">
                <tr><td>Klucz publiczny</td><td id="cd-key" class="mono" style="font-size:11px;word-break:break-all"></td></tr>
                <tr><td>Advert Type</td><td id="cd-type"></td></tr>
                <tr><td>Flagi</td><td id="cd-flags"></td></tr>
                <tr><td>Pozycja</td><td id="cd-pos"></td></tr>
                <tr><td>Odległość</td><td id="cd-dist"></td></tr>
                <tr><td>TX Power</td><td id="cd-txp"></td></tr>
                <tr><td>Odebrany</td><td id="cd-last"></td></tr>
                <tr><td>Ostatnia modyfikacja</td><td id="cd-mod"></td></tr>
                <tr><td>Ścieżka routingu</td><td id="cd-path"></td></tr>
              </table>
              <div style="display:flex;gap:10px;margin-top:10px">
                <button class="btn btn-sm" onclick="copyKey()">Kopiuj klucz</button>
                <button class="btn btn-sm" onclick="toggleRaw()">Raw data</button>
              </div>
              <pre id="cd-raw" class="raw" style="display:none"></pre>
            </div>
          </div>
        </div>

        <div class="panel">
          <div class="panel-head"><h2>Log zdarzeń</h2></div>
          <div class="panel-body"><div class="log-box" id="log"></div></div>
        </div>
      </div>

      <div id="page-chat" class="page" style="display:none">
        <div class="chat-wrap">
          <div class="chip-row" id="chat-chips">
            <button class="chip active" data-ch="0" onclick="setChan(0)">CH 0 · #public</button>
            <button class="chip" data-ch="1" onclick="setChan(1)">CH 1</button>
            <button class="chip" data-ch="2" onclick="setChan(2)">CH 2</button>
            <button class="chip" data-ch="3" onclick="setChan(3)">CH 3</button>
            <button class="chip" data-ch="4" onclick="setChan(4)">CH 4</button>
            <button class="chip" data-ch="5" onclick="setChan(5)">CH 5</button>
            <button class="chip" data-ch="6" onclick="setChan(6)">CH 6</button>
            <button class="chip" data-ch="7" onclick="setChan(7)">CH 7</button>
          </div>
          <select id="chat-chan" style="display:none">
            <option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option>
            <option value="4">4</option><option value="5">5</option><option value="6">6</option><option value="7">7</option>
          </select>
          <div class="chat-msgs" id="chat-msgs"><div class="empty">Wybierz kanał i czekaj na wiadomości…</div></div>
          <div class="chat-input-row">
            <input id="chat-input" type="text" placeholder="Napisz wiadomość…" onkeydown="if(event.key==='Enter')chatSend()">
            <button class="btn btn-primary" onclick="chatSend()">Wyślij</button>
          </div>
        </div>
      </div>

      <div id="page-history" class="page" style="display:none">
        <div class="panel">
          <div class="panel-body">
            <div class="search-bar">
              <input id="hist-search" type="text" placeholder="Szukaj w wiadomościach…" onkeydown="if(event.key==='Enter')histLoad(0)">
              <select id="hist-chan" style="max-width:170px" onchange="histLoad(0)">
                <option value="">Wszystkie kanały</option>
                <option value="CH0">Kanał 0</option>
                <option value="CH1">Kanał 1</option>
                <option value="DM">DM</option>
              </select>
              <button class="btn btn-primary" onclick="histLoad(0)">Szukaj</button>
            </div>
            <div class="hist-list" id="hist-results"></div>
            <div class="pager" id="hist-pager"></div>
          </div>
        </div>
      </div>

      <div id="page-packets" class="page" style="display:none">
        <div class="panel">
          <div class="panel-head"><h2>Ostatnie pakiety</h2>
            <div class="panel-actions">
              <button class="btn btn-sm" onclick="loadPackets()">Odśwież</button>
            </div>
          </div>
          <div class="panel-body">
            <div class="hist-list" id="packets-list"></div>
          </div>
        </div>
      </div>

      <div id="page-config" class="page" style="display:none">
        <div class="cfg-grid">
          <div class="panel">
            <div class="panel-head"><h2>Nazwa</h2></div>
            <div class="panel-body">
              <div class="field"><input id="cfg-name" placeholder="WWR01M"><div class="hint">Nazwa urządzenia widoczna w sieci MeshCore (max 32 znaki)</div></div>
              <button class="btn btn-primary" onclick="setCfg({name: document.getElementById('cfg-name').value})">Zapisz</button>
            </div>
          </div>
          <div class="panel">
            <div class="panel-head"><h2>TX Power</h2></div>
            <div class="panel-body">
              <div class="field"><input id="cfg-txp" type="number" value="20" min="2" max="22"><div class="hint">Moc nadajnika 2–22 dBm. Wyższa = dalszy zasięg, większe zużycie baterii</div></div>
              <button class="btn btn-primary" onclick="setCfg({tx_power: +document.getElementById('cfg-txp').value})">Ustaw</button>
            </div>
          </div>
          <div class="panel">
            <div class="panel-head"><h2>Współrzędne</h2></div>
            <div class="panel-body">
              <div class="row-2">
                <div class="field"><input id="cfg-lat" type="number" step="0.000001" placeholder="50.1197"></div>
                <div class="field"><input id="cfg-lon" type="number" step="0.000001" placeholder="20.2789"></div>
              </div>
              <div class="hint" style="margin-bottom:10px">Szerokość i długość geograficzna (GPS). Do obliczania odległości i mapy</div>
              <button class="btn btn-primary" onclick="setCfg({coords:[+document.getElementById('cfg-lat').value,+document.getElementById('cfg-lon').value]})">Zapisz</button>
            </div>
          </div>
          <div class="panel">
            <div class="panel-head"><h2>PIN urządzenia</h2></div>
            <div class="panel-body">
              <div class="field"><input id="cfg-pin" type="password" placeholder="••••••"><div class="hint">Kod PIN do parowania BLE. Chroni przed nieautoryzowanym dostępem przez Bluetooth</div></div>
              <button class="btn btn-primary" onclick="setCfg({devicepin: document.getElementById('cfg-pin').value})">Ustaw PIN</button>
            </div>
          </div>

          <div class="panel" style="grid-column:1/-1">
            <div class="panel-head"><h2>Radio</h2></div>
            <div class="panel-body">
              <div class="hint" style="margin-bottom:10px">Parametry radia LoRa. Wszystkie urządzenia w sieci muszą mieć te same ustawienia.</div>
              <div class="cfg-grid" style="grid-template-columns:repeat(auto-fit,minmax(120px,1fr));margin-bottom:12px">
                <div class="field"><label>Częst. (MHz)</label><input id="cfg-freq" type="number" step="0.001" value="869.618"></div>
                <div class="field"><label>BW (kHz)</label><input id="cfg-bw" type="number" step="0.1" value="62.5"></div>
                <div class="field"><label>SF</label><input id="cfg-sf" type="number" value="8" min="7" max="12"></div>
                <div class="field"><label>CR (5–8)</label><input id="cfg-cr" type="number" value="8" min="5" max="8"></div>
                <div class="field"><label>RX Dly</label><input id="cfg-rxdly" type="number" value="0"></div>
                <div class="field"><label>AF</label><input id="cfg-af" type="number" value="0"></div>
              </div>
              <div style="display:flex;gap:8px">
                <button class="btn btn-primary" onclick="setCfg({freq:+document.getElementById('cfg-freq').value,bw:+document.getElementById('cfg-bw').value,sf:+document.getElementById('cfg-sf').value,cr:+document.getElementById('cfg-cr').value})">Zapisz radio</button>
                <button class="btn" onclick="setCfg({rx_dly:+document.getElementById('cfg-rxdly').value,af:+document.getElementById('cfg-af').value})">Zapisz tuning</button>
              </div>
            </div>
          </div>

          <div class="panel">
            <div class="panel-head"><h2>Telemetria</h2></div>
            <div class="panel-body">
              <div class="hint" style="margin-bottom:10px">Częstotliwość wysyłania danych. 0 = OFF, 3 = najczęściej</div>
              <div class="field"><label>Mode Base</label><select id="cfg-tmb"><option value="0">0 – OFF</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
              <div class="field"><label>Mode Loc</label><select id="cfg-tml"><option value="0">0 – OFF</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
              <div class="field"><label>Mode Env</label><select id="cfg-tme"><option value="0">0 – OFF</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
              <button class="btn btn-primary" onclick="setCfg({telemetry_mode_base:+document.getElementById('cfg-tmb').value,telemetry_mode_loc:+document.getElementById('cfg-tml').value,telemetry_mode_env:+document.getElementById('cfg-tme').value})">Zapisz telemetrię</button>
              <hr style="margin:14px 0">
              <label class="check"><input id="cfg-mac" type="checkbox" onchange="setCfg({manual_add_contacts:this.checked})"> Manual add contacts</label>
              <div class="field"><label>Advert Loc Policy</label><select id="cfg-alp" onchange="setCfg({advert_loc_policy:+this.value})"><option value="0">0</option><option value="1">1</option><option value="2">2</option></select></div>
            </div>
          </div>

          <div class="panel">
            <div class="panel-head"><h2>Zaawansowane</h2></div>
            <div class="panel-body">
              <div class="hint" style="margin-bottom:10px">Opcje dla zaawansowanych. Zmieniaj tylko jeśli wiesz co robisz.</div>
              <div class="field"><label>Path Hash Mode</label>
                <select id="cfg-phm" onchange="setCfg({path_hash_mode:+this.value})">
                  <option value="0">0 — 1 bajt (254 ID, 64 hopy, legacy)</option>
                  <option value="1">1 — 2 bajty (65k ID, 32 hopy, zalecany)</option>
                  <option value="2">2 — 3 bajty (16M ID, 21 hopów, gęste sieci)</option>
                </select>
                <div class="hint">Zmiana wymaga wysłania Advert. <a href="https://nodakmesh.org/blog/meshcore-path-hash-explained" target="_blank" style="color:var(--accent)">Więcej info</a></div>
              </div>
              <label class="check"><input id="cfg-macks" type="checkbox"> Multi ACKs <span class="note">(wysyła 2 potwierdzenia zamiast 1)</span></label>
              <button class="btn btn-sm" onclick="setCfg({multi_acks:document.getElementById('cfg-macks').checked?1:0})">Ustaw</button>
              <div class="field" style="margin-top:12px"><label>Flood Scope</label><input id="cfg-fs" type="text" placeholder="#public"></div>
              <button class="btn btn-sm" onclick="setCfg({flood_scope:document.getElementById('cfg-fs').value})">Ustaw</button>
              <div class="field" style="margin-top:12px"><label>Custom Var (JSON)</label><input id="cfg-cv" type="text" placeholder='{"key":"x","value":"y"}'></div>
              <button class="btn btn-sm" onclick="try{setCfg({custom_var:JSON.parse(document.getElementById('cfg-cv').value)})}catch(e){toast('Nieprawidlowy JSON','bad')}">Ustaw</button>
            </div>
          </div>

          <div class="panel">
            <div class="panel-head"><h2>Akcje</h2></div>
            <div class="panel-body">
              <div class="hint" style="margin-bottom:10px">Operacje na urządzeniu</div>
              <div style="display:flex;gap:8px;flex-wrap:wrap">
                <button class="btn" onclick="fetch('/api/device/advert',{method:'POST'}).then(r=>r.json()).then(d=>toast(d.ok?'Advert wysłany':'Błąd',d.ok?'good':'bad'))">Wyślij Advert</button>
                <button class="btn btn-danger" onclick="if(confirm('Restartować Helteca?'))fetch('/api/device/reboot',{method:'POST'}).then(r=>r.json()).then(d=>toast(d.ok?'Restart…':'Błąd',d.ok?'good':'bad'))">Restart</button>
                <button class="btn" onclick="loadDeviceInfo()">Odśwież</button>
              </div>
            </div>
          </div>
        </div>

        <div class="panel">
          <div class="panel-head"><h2>Info z urządzenia</h2></div>
          <div class="panel-body"><div class="log-box" id="device-info" style="height:200px">Ładowanie…</div></div>
        </div>
        <div class="panel">
          <div class="panel-head"><h2>Statystyki</h2></div>
          <div class="panel-body"><div class="log-box" id="stats-display" style="height:200px">Ładowanie…</div></div>
        </div>
        <div class="panel">
          <div class="panel-head"><h2>Kanały</h2></div>
          <div class="panel-body"><div class="log-box" id="channels-display" style="height:200px">Ładowanie…</div></div>
        </div>
      </div>
    </div>
  </div>
</div>
<div id="toast-wrap"></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
function esc(s){return String(s).replace(/[&<>"']/g,function(m){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m];});}

function toast(msg,kind){
  const t=document.createElement('div');
  t.className='toast'+(kind?' '+kind:'');
  t.textContent=msg;
  document.getElementById('toast-wrap').appendChild(t);
  setTimeout(()=>{t.style.opacity='0';t.style.transition='opacity .3s';setTimeout(()=>t.remove(),300)},3500);
}

function tickClock(){document.getElementById('clock').textContent=new Date().toLocaleTimeString('pl-PL');}
setInterval(tickClock,1000);tickClock();

let _map=null,_markers=[];
function initMap(lat,lon){if(!_map){_map=L.map('map').setView([lat,lon],9);L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:18,attribution:'&copy; OpenStreetMap'}).addTo(_map);}
updateMarkers(lat,lon);}
function updateMarkers(myLat,myLon){_markers.forEach(m=>_map.removeLayer(m));_markers=[];
if(myLat!=null){_markers.push(L.marker([myLat,myLon],{icon:L.divIcon({html:'<div style="background:#e2a34e;width:12px;height:12px;border-radius:50%;border:2px solid #0a0e13"></div>',iconSize:[12,12],className:''})}).bindPopup('<b>WWR01M</b> (ja)').addTo(_map));}
fetch('/api/device/contacts').then(r=>r.json()).then(d=>{if(d.contacts)d.contacts.forEach(c=>{if(c.lat!=null&&c.lon!=null){const m=L.marker([c.lat,c.lon],{icon:L.divIcon({html:'<div style="background:#4fd193;width:10px;height:10px;border-radius:50%;border:1px solid #0a0e13"></div>',iconSize:[10,10],className:''})});m.bindPopup(`<b>${esc(c.name)}</b><br>${c.dist_km?c.dist_km+' km':'?'}<br>${c.last_seen||''}`);m.addTo(_map);_markers.push(m);}})});
fetch('/api/status').then(r=>r.json()).then(d=>{if(d.node_data)Object.entries(d.node_data).forEach(([k,v])=>{if(v.lat!=null&&v.lon!=null){const m=L.marker([v.lat,v.lon],{icon:L.divIcon({html:'<div style="background:#eecb56;width:8px;height:8px;border-radius:50%"></div>',iconSize:[8,8],className:''})});m.bindPopup(`<b>${esc(k.slice(0,8))}</b><br>${v.dist?v.dist+' km':'?'}<br>${v.ts||''}`);m.addTo(_map);_markers.push(m);}})})}

async function load(){
  const r=await fetch('/api/status');
  if(r.status===401){showLoginAgain();return}
  if(!r.ok)return;
  const d=await r.json();
  const dot=document.getElementById('conn-dot'),txt=document.getElementById('conn-text');
  dot.className='dot '+(d.connected?'on':'off');
  txt.textContent=d.connected?'Połączony':'Rozłączony';
  document.getElementById('stats').innerHTML=
    `<div class="stat-card ${d.connected?'k-good':'k-bad'}"><div class="stat-value">${d.connected?'Online':'Offline'}</div><div class="stat-label">Status łącza</div></div>`+
    `<div class="stat-card k-accent"><div class="stat-value">${esc(d.device_contacts||0)}</div><div class="stat-label">Kontakty (urządzenie)</div></div>`+
    `<div class="stat-card"><div class="stat-value">${esc(d.contacts)}</div><div class="stat-label">Kontakty (cache DM)</div></div>`+
    `<div class="stat-card"><div class="stat-value">${esc(d.nodes)}</div><div class="stat-label">Widziane nody</div></div>`;
  const n=document.getElementById('nodes');
  n.innerHTML='<tr><th>Node</th><th>Widziany</th><th>Odległość</th></tr>';
  d.node_list.forEach(p=>{const nd=d.node_data[p]||{};const dist=nd.dist?nd.dist+' km':'—';
    n.innerHTML+=`<tr><td class="mono">${esc(p.slice(0,8))}</td><td>${esc(nd.ts||'-')}</td><td>${dist}</td></tr>`});
}

async function loadLog(){
  const r=await fetch('/api/log');
  if(r.status===401){showLoginAgain();return}
  if(!r.ok)return;
  const d=await r.json();
  const box=document.getElementById('log');
  box.innerHTML=d.log.map(l=>{
    const t=l.slice(0,8),rest=esc(l.slice(8));
    let color='';
    if(/blad|error|warning/i.test(rest))color='color:var(--bad)';
    else if(rest.indexOf('->')!==-1)color='color:var(--accent)';
    else if(rest.indexOf('<-')!==-1)color='color:var(--good)';
    return `<div style="${color}"><span class="t">${esc(t)}</span>${rest}</div>`;
  }).join('');
  box.scrollTop=box.scrollHeight;
}

async function loadContacts(){
  const r=await fetch('/api/device/contacts');
  if(r.status===401){showLoginAgain();return}
  const d=await r.json();
  if(d.contacts){window._cd=d.contacts.slice(0,200);sortContacts('last_advert');renderContactsTable();}
}
let _sortCol='last_advert',_sortDir=-1;
function sortContacts(col){if(_sortCol===col)_sortDir*=-1;else{_sortCol=col;_sortDir=1;}
window._cd.sort((a,b)=>{
  let va=a[col],vb=b[col];
  if(col==='dist_km'){va=va||9999;vb=vb||9999;}
  if(col==='last_seen'){va=va||'';vb=vb||'';}
  if(col==='last_advert'){va=va||0;vb=vb||0;}
  if(va<vb)return -1*_sortDir;if(va>vb)return 1*_sortDir;return 0;});
renderContactsTable();}
function _relTime(ts){
  if(!ts)return '—';
  const now=Date.now()/1000;
  const d=now-ts;
  if(d<60)return 'przed chwilą';
  if(d<3600)return Math.round(d/60)+' min temu';
  if(d<86400)return Math.round(d/3600)+'h temu';
  if(d<604800)return Math.round(d/86400)+' dni temu';
  return Math.round(d/604800)+' tyg temu';
}
function renderContactsTable(){
const t=document.getElementById('contacts-table');
const arrow=c=>_sortCol===c?(_sortDir>0?' ▾':' ▴'):'';
let h='<tr><th class="sortable" onclick="sortContacts(\'name\')">Nazwa'+arrow('name')+'</th><th class="sortable" onclick="sortContacts(\'dist_km\')">Odległość'+arrow('dist_km')+'</th><th class="sortable" onclick="sortContacts(\'last_advert\')">Aktywność'+arrow('last_advert')+'</th><th></th></tr>';
window._cd.forEach(c=>{
  const dst=c.dist_km?c.dist_km+' km':'—';
  const rel=_relTime(c.last_advert);
  const fresh=c.last_advert&&(Date.now()/1000-c.last_advert<3600);
  const stale=c.last_advert&&(Date.now()/1000-c.last_advert>86400*7);
  h+='<tr><td>'+esc(c.name)+'</td><td>'+dst+'</td><td style="color:'+(fresh?'var(--good)':stale?'var(--text-faint)':'var(--text-dim)')+'">'+rel+'</td><td style="text-align:right"><button class="btn btn-sm" onclick="showContact(\''+c.key+'\')">Szczegóły</button></td></tr>';
});
t.innerHTML=h;}
const CONTACT_TYPES={0:'NONE',1:'Klient',2:'Repeater',3:'Room',4:'Sensor'};
function showContact(key){const c=window._cd.find(x=>x.key===key);if(!c)return;
document.getElementById('cd-name').textContent=c.name;
document.getElementById('cd-key').textContent=c.public_key||c.key||'?';
document.getElementById('cd-type').textContent=c.type!=null?(CONTACT_TYPES[c.type]||'Type '+c.type):'-';
document.getElementById('cd-flags').textContent=c.flag_desc||(c.flags!=null?'Flagi: '+c.flags:'-');
document.getElementById('cd-pos').textContent=c.lat!=null?c.lat+', '+c.lon:'-';
document.getElementById('cd-dist').textContent=c.dist_km?c.dist_km+' km ('+Math.round(c.dist_km*0.6214)+' mi)':'-';
document.getElementById('cd-txp').textContent=c.tx_power!=null?c.tx_power+' dBm':'-';
document.getElementById('cd-last').textContent=c.last_seen||'-';
document.getElementById('cd-mod').textContent=c.lastmod?'data modyfikacji: '+(new Date(c.lastmod*1e3).toLocaleString('pl-PL')):'-';
document.getElementById('cd-path').textContent=c.path_len!=null&&c.path_len>=0?'Hopy: '+(c.path_len+1)+', hash: '+(c.path_hash||'?'):'brak';
document.getElementById('cd-raw').textContent=c._raw?JSON.stringify(c._raw,null,2):'';
document.getElementById('contact-detail').style.display='block';
document.getElementById('contact-detail').scrollIntoView({behavior:'smooth',block:'nearest'});}
function closeDetail(){document.getElementById('contact-detail').style.display='none';}
function toggleRaw(){const e=document.getElementById('cd-raw');e.style.display=e.style.display==='none'?'block':'none';}
function copyKey(){const k=document.getElementById('cd-key').textContent;navigator.clipboard.writeText(k).then(()=>toast('Skopiowano klucz','good')).catch(()=>{prompt('Ręcznie skopiuj:',k);});}

function setChan(n){document.getElementById('chat-chan').value=n;
document.querySelectorAll('#chat-chips .chip').forEach(c=>c.classList.toggle('active',+c.dataset.ch===n));
chatRefresh();}
async function chatRefresh(){const r=await fetch('/api/messages');const d=await r.json();const ch=document.getElementById('chat-chan').value;const el=document.getElementById('chat-msgs');let h='';d.forEach(m=>{if(m.ch.replace('CH','')==ch||ch==='*'){const me=m.dir==='out';h+='<div class="bubble '+(me?'out':'in')+'"><div class="meta">'+(me?'JA':esc(m.from))+' · '+m.ts+' · '+m.ch+'</div><div>'+esc(m.text)+'</div></div>'}});el.innerHTML=h||'<div class="empty">Brak wiadomości w kanale '+ch+'</div>';el.scrollTop=el.scrollHeight;}
async function chatSend(){const inp=document.getElementById('chat-input');const t=inp.value.trim();if(!t)return;const ch=document.getElementById('chat-chan').value;const r=await fetch('/api/send',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channel:parseInt(ch),text:t})});const d=await r.json();if(d.ok){inp.value='';chatRefresh();if(d.acks!==undefined){toast('Odebrano przez '+d.acks+' repeater(ów)');setTimeout(chatRefresh,5000)}}else{toast('Błąd: '+(d.error||'?'),'bad')}}
setInterval(function(){if(document.getElementById('page-chat').style.display!=='none')chatRefresh()},3000);

async function loadDeviceCards(){const r=await fetch('/api/device/info');if(r.status===401){showLoginAgain();return};if(!r.ok)return;const d=await r.json();
const dev=d.device||{};const self=d.self||{};
if(self.adv_lat!=null&&self.adv_lon!=null){
  if(!_map)initMap(self.adv_lat,self.adv_lon);else updateMarkers(self.adv_lat,self.adv_lon);}
const cards=[
  {v:dev.model||'?',l:'Model'},{v:dev.ver||'?',l:'Firmware'},
  {v:self.adv_name||self.name||'?',l:'Nazwa'},
  {v:self.radio_freq!=null?self.radio_freq+' MHz':'?',l:'Częstotliwość'},
  {v:'SF'+(self.radio_sf||'?'),l:'SF'},
  {v:self.last_snr!=null?self.last_snr+' dB':'?',l:'Ostatni SNR'},
  {v:self.last_rssi!=null?self.last_rssi+' dBm':'?',l:'Ostatni RSSI'},
];
try{const sr=await fetch('/api/device/stats');const sd=await sr.json();
  cards.push({v:sd.bat&&sd.bat.level?sd.bat.level+' mV':'?',l:'Bateria'});}catch(e){cards.push({v:'?',l:'Bateria'});}
document.getElementById('device-cards').innerHTML=cards.map(c=>`<div class="stat-card"><div class="stat-value">${esc(c.v)}</div><div class="stat-label">${c.l}</div></div>`).join('');
const sr=await fetch('/api/system');const sys=await sr.json();
const sysCards=[
  {v:sys.hostname||'?',l:'Hostname'},{v:sys.ip||'?',l:'IP'},
  {v:sys.uptime||'?',l:'Uptime'},{v:sys.ram||'?',l:'RAM'},
  {v:sys.disk||'?',l:'Disk'},{v:sys.cpu_temp||'?',l:'CPU Temp'},
  {v:sys.arch||'?',l:'Architektura'},
];
document.getElementById('sys-cards').innerHTML=sysCards.map(c=>`<div class="stat-card"><div class="stat-value">${esc(c.v)}</div><div class="stat-label">${c.l}</div></div>`).join('')+
  '<div style="grid-column:1/-1;display:flex;gap:8px;margin-top:2px"><button class="btn btn-sm" onclick="loadDeviceCards()">Przeładuj metryki</button>'+
  '<button class="btn btn-sm btn-danger" onclick="rebootPi()">Reboot Pi</button></div>';}

async function rebootPi(){if(!confirm('Na pewno zrestartować Raspberry Pi?'))return;
const r=await fetch('/api/system/reboot-pi',{method:'POST',headers:{'Content-Type':'application/json'}});
const d=await r.json();toast(d.msg||d.error||'OK',d.error?'bad':'good');}
async function loadDeviceInfo(){const r=await fetch('/api/device/info');const d=await r.json();
document.getElementById('device-info').textContent=JSON.stringify(d,null,2);
if(d.self){const s=d.self;if(s.name)document.getElementById('cfg-name').value=s.name;
if(s.tx_power)document.getElementById('cfg-txp').value=s.tx_power;
if(s.radio_freq)document.getElementById('cfg-freq').value=s.radio_freq;
if(s.radio_bw)document.getElementById('cfg-bw').value=s.radio_bw;
if(s.radio_sf)document.getElementById('cfg-sf').value=s.radio_sf;
if(s.radio_cr)document.getElementById('cfg-cr').value=s.radio_cr;
if(s.multi_acks!==undefined)document.getElementById('cfg-macks').checked=!!s.multi_acks;
if(s.manual_add_contacts!==undefined)document.getElementById('cfg-mac').checked=!!s.manual_add_contacts;
if(s.advert_loc_policy!==undefined)document.getElementById('cfg-alp').value=s.advert_loc_policy;
if(s.path_hash_mode!==undefined)document.getElementById('cfg-phm').value=s.path_hash_mode;}}
async function setCfg(data){const r=await fetch('/api/device/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
const d=await r.json();
if(d.results){const failed=d.results.filter(x=>!x.ok);toast(failed.length?failed.length+' błędów zapisu':'Zapisano pomyślnie',failed.length?'bad':'good');}
else{toast(d.error||'Błąd zapisu','bad');}}
async function loadStats(){const r=await fetch('/api/device/stats');const d=await r.json();document.getElementById('stats-display').textContent=JSON.stringify(d,null,2);}
async function loadChannels(){const r=await fetch('/api/device/channels');const d=await r.json();document.getElementById('channels-display').textContent=JSON.stringify(d,null,2);}

let _histPage=0,_histTotal=0;
async function histLoad(page){
  _histPage=page||0;
  const q=document.getElementById('hist-search').value;
  const ch=document.getElementById('hist-chan').value;
  const r=await fetch('/api/messages/search?q='+encodeURIComponent(q)+'&ch='+encodeURIComponent(ch)+'&limit=50&offset='+(_histPage*50));
  if(r.status===401){showLoginAgain();return}
  const d=await r.json();
  _histTotal=d.total;
  histRender(d.messages);
}
function histRender(msgs){
  let h='';
  msgs.forEach(m=>{
    const ts=m.ts?m.ts.replace('T',' ').substring(0,19):'?';
    const me=m.dir==='out';
    h+='<div class="hist-row"><span class="t">'+esc(ts)+'</span><b style="color:'+(me?'var(--accent)':'var(--good)')+'">'+(me?'JA':esc(m.sender))+'</b> <span style="color:var(--text-faint)">['+esc(m.ch)+']</span> '+esc(m.text)+'</div>';
  });
  document.getElementById('hist-results').innerHTML=h||'<div class="empty">Brak wyników</div>';
  const totalPages=Math.ceil(_histTotal/50);
  let pager='Strona ' + (_histPage+1) + ' z ' + (totalPages||1) + ' (' + _histTotal + ' wiadomości) ';
  if(_histPage>0)pager+='<button class="btn btn-sm" onclick="histLoad('+(_histPage-1)+')">&lt; Wstecz</button> ';
  if((_histPage+1)*50<_histTotal)pager+='<button class="btn btn-sm" onclick="histLoad('+(_histPage+1)+')">Dalej &gt;</button>';
  document.getElementById('hist-pager').innerHTML=pager;
}

async function loadPackets(){
  const r=await fetch('/api/packets?limit=100');
  if(r.status===401){showLoginAgain();return}
  const d=await r.json();
  const el=document.getElementById('packets-list');
  if(!d.packets||!d.packets.length){el.innerHTML='<div class="empty">Brak zarejestrowanych pakietów</div>';return}
  let h='';
  d.packets.forEach(p=>{
    const ts=p.ts?p.ts.replace('T',' ').substring(0,19):'?';
    const hops=p.path_hops||0;
    const snr=p.snr!=null?p.snr.toFixed(1)+' dB':'—';
    const obs=p.observers||0;
    const obsList=p.observer_list?p.observer_list.split(',').join(', '):'';
    const isOut=p.sender==='JA';
    h+='<div class="hist-row">'+
      '<span class="t">'+esc(ts)+'</span>'+
      '<b style="color:'+(isOut?'var(--accent)':'var(--good)')+'">'+esc(p.sender)+'</b>'+
      ' <span style="color:var(--text-faint)">['+esc(p.ch)+']</span> '+
      '<span style="color:var(--text-dim)">'+esc((p.text||'').substring(0,80))+'</span>'+
      '<div style="margin-top:4px;font-size:11px;color:var(--text-faint)">'+
      (isOut?'<span style="color:var(--accent)">📤 Wysłane</span>':
        '<span style="color:var(--accent)">Hopy: '+hops+'</span> · SNR: '+snr)+
      (obs?' · <span style="color:var(--accent)">Obserwatorzy: '+obs+'</span> ('+esc(obsList)+')':'')+
      '</div></div>';
  });
  el.innerHTML=h;
}

const pageTitles={dashboard:'Panel',chat:'Czat',config:'Konfiguracja',history:'Historia',packets:'Pakiety'};
document.querySelectorAll('.nav-item').forEach(a=>{a.addEventListener('click',function(e){e.preventDefault();
document.querySelectorAll('.nav-item').forEach(x=>x.classList.remove('active'));this.classList.add('active');
document.querySelectorAll('#app>div').forEach(x=>x.style.display='none');
const page=document.getElementById('page-'+this.dataset.page);
if(page){page.style.display='flex'}
document.getElementById('page-title').textContent=pageTitles[this.dataset.page]||'';
if(this.dataset.page==='dashboard'){load();loadLog();loadDeviceCards();loadContacts();if(_map)setTimeout(()=>_map.invalidateSize(),100)};
if(this.dataset.page==='chat')chatRefresh();
if(this.dataset.page==='config'){loadDeviceInfo();loadStats();loadChannels()};
if(this.dataset.page==='history')histLoad(0);
if(this.dataset.page==='packets')loadPackets();
})})
</script>
</body>
</html>"""

LOG_HTML = """<!DOCTYPE html>
<html lang="pl">
<head><meta charset="utf-8"><meta http-equiv="refresh" content="5">
<title>Log — MeshCore Bridge</title>
<style>
body{background:#0a0e13;color:#c9d3df;font:12px/1.7 'IBM Plex Mono',ui-monospace,monospace;padding:16px}
.ts{color:#576172;margin-right:8px}
</style>
</head>
<body><pre>""" + '{% for line in log %}<span class="ts">{{ line[:8] }}</span>{{ line[8:] }}\n{% endfor %}</pre></body></html>'
def build_status(mc) -> dict:
    with _state_lock:
        node_list = sorted(_seen_nodes.keys())
        my_lat = _self_info.get("adv_lat")
        my_lon = _self_info.get("adv_lon")
        contacts_count = len(_contact_cache)
        nodes_count = len(_seen_nodes)
    nodes_with_dist = []
    for p in node_list:
        with _state_lock:
            nd = dict(_seen_nodes[p])
        if nd.get("lat") is not None and nd.get("lon") is not None and my_lat is not None and my_lon is not None:
            nd["dist"] = _haversine(my_lat, my_lon, nd["lat"], nd["lon"])
        else:
            nd["dist"] = None
        nodes_with_dist.append(nd)
    return {
        "connected": mc and mc.is_connected,
        "contacts": contacts_count,        # from DM events (cache)
        "device_contacts": _device_contact_count,  # from device CMD_GET_CONTACTS
        "nodes": nodes_count,
        "node_list": node_list,
        "node_data": {p: d for p, d in zip(node_list, nodes_with_dist)},
    }


# ── Main ──────────────────────────────────────────────────────

async def start_web():
    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse, JSONResponse
    from starlette.middleware.base import BaseHTTPMiddleware
    import uvicorn
    import secrets, base64, hmac

    # Generate session token
    global _session_token
    _session_token = secrets.token_urlsafe(32)

    class AuthMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            if request.url.path == "/login" or (request.url.path == "/" and request.method == "GET")\
               or request.url.path == "/api/ping":
                return await call_next(request)
            cfg = load_config()
            bridge_cfg = cfg.get("bridge", {})
            auth_cfg = cfg.get("bridge", {}).get("auth", {})
            api_key = str(bridge_cfg.get("api_key", "") or "")
            user = auth_cfg.get("username", "")
            pwd = auth_cfg.get("password", "")
            # Backward-compatibility: when username auth is disabled,
            # keep UI/API open even if api_key is present in config.
            if not user:
                return await call_next(request)
            session = request.cookies.get("bridge_session", "")
            if session and hmac.compare_digest(session, request.app.state.session_token):
                return await call_next(request)
            auth_header = request.headers.get("authorization", "")
            if auth_header.startswith("Basic "):
                try:
                    decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                    u, p = decoded.split(":", 1)
                    if hmac.compare_digest(u, user) and hmac.compare_digest(p, pwd):
                        return await call_next(request)
                except Exception:
                    pass
            if api_key:
                x_api_key = request.headers.get("x-api-key", "")
                if x_api_key and hmac.compare_digest(x_api_key, api_key):
                    return await call_next(request)
                if auth_header.startswith("Bearer "):
                    bearer = auth_header[7:].strip()
                    if bearer and hmac.compare_digest(bearer, api_key):
                        return await call_next(request)
            return JSONResponse({"error": "Unauthorized"}, status_code=401)

    app = FastAPI(title="MeshCore Bridge")
    app.state.send_datagram = _send_datagram_fn  # set by main() after init
    app.add_middleware(AuthMiddleware)
    # Store token in app state to avoid closure issues
    app.state.session_token = _session_token

    @app.post("/login")
    async def login(request: Request):
        cfg = load_config()
        auth_cfg = cfg.get("bridge", {}).get("auth", {})
        try:
            body = await request.json()
            u = body.get("username", "")
            p = body.get("password", "")
        except Exception:
            u = p = ""
        if not auth_cfg.get("username"):
            return JSONResponse({"ok": True})
        if hmac.compare_digest(u, auth_cfg.get("username", "")) and hmac.compare_digest(p, auth_cfg.get("password", "")):
            resp = JSONResponse({"ok": True})
            resp.set_cookie("bridge_session", request.app.state.session_token, httponly=True, max_age=86400)
            return resp
        return JSONResponse({"error": "Invalid credentials"}, status_code=403)

    @app.get("/")
    async def index(request: Request):
        html = WEB_HTML
        cfg = load_config()
        auth_cfg = cfg.get("bridge", {}).get("auth", {})
        if auth_cfg.get("username"):
            html = html.replace("</body>", LOGIN_FORM + "</body>")
        else:
            html = html.replace("</body>", NOAUTH_START + "</body>")
        return HTMLResponse(html)

    @app.get("/api/ping")
    async def api_ping(request: Request):
        """Check if session is active (exempt from auth)."""
        cfg = load_config()
        if not cfg.get("bridge", {}).get("auth", {}).get("username"):
            return JSONResponse({"auth": False})
        session = request.cookies.get("bridge_session", "")
        ok = bool(session and hmac.compare_digest(session, request.app.state.session_token))
        return JSONResponse({"auth": ok, "connected": _mc_ref and _mc_ref.is_connected if ok else None})

    @app.get("/api/status")
    async def api_status():
        return JSONResponse(build_status(_mc_ref))

    @app.get("/api/log")
    async def api_log():
        with _state_lock:
            log_lines = list(_log_buffer)
        return JSONResponse({"log": log_lines})

    @app.get("/log")
    async def log_page():
        with _state_lock:
            recent = list(_log_buffer)[-100:]
        lines = "".join(f'<span class="ts">{esc(l[:8])}</span>{esc(l[8:])}\n' for l in recent)
        return HTMLResponse(LOG_HTML.replace("{% for line in log %}", "").replace("{% endfor %}", "")
                           + "<pre>" + lines + "</pre></body></html>")

    @app.get("/api/device/info")
    async def api_device_info():
        global _device_info, _device_info_ts
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            result = {"self": _self_info}
            try:
                phm = await _mc_call(_mc_ref.commands.get_path_hash_mode(), timeout=3)
                result["self"]["path_hash_mode"] = phm
            except Exception:
                pass
            r = await _mc_call(_mc_ref.commands.send_device_query(), timeout=5)
            if r.type.name != "ERROR":
                _device_info = r.payload
                _device_info_ts = time.time()
                result["device"] = r.payload
            return JSONResponse(result)
        except asyncio.TimeoutError:
            return JSONResponse({"self": _self_info})
        except Exception as e:
            return JSONResponse({"error": str(e), "self": _self_info})

    @app.post("/api/device/advert")
    async def api_advert(request: Request):
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            r = await _mc_call(_mc_ref.commands.send_advert(flood=False), timeout=5)
            ok = r.type.name != "ERROR"
            _log(f"Advert {'wyslany' if ok else 'BLAD'}")
            return JSONResponse({"ok": ok})
        except Exception as e:
            return JSONResponse({"error": str(e)})

    @app.get("/api/device/contacts")
    async def api_device_contacts(request: Request):
        if not _rate_check(request, RATE_GET_MAX):
            return JSONResponse({"error": "Too many requests"}, status_code=429)
        global _device_contact_count
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            r = await _mc_call(_mc_ref.commands.get_contacts(), timeout=10)
            if r.type.name != "ERROR":
                contacts = r.payload or {}
                _device_contact_count = len(contacts)
                my_lat = _self_info.get("adv_lat")
                my_lon = _self_info.get("adv_lon")
                result = []
                for key, c in list(contacts.items()):
                    name = c.get("adv_name", "") or c.get("name", "") or key[:12]
                    lat = c.get("adv_lat")
                    lon = c.get("adv_lon")
                    dist = None
                    if lat is not None and lon is not None and my_lat is not None and my_lon is not None:
                        # Ignore coordinates near (0,0) — GPS not locked
                        if abs(lat) < 0.001 and abs(lon) < 0.001:
                            dist = None
                        else:
                            dist = _haversine(my_lat, my_lon, lat, lon)
                    last_ts = c.get("lastmod")  # our clock — when we received it
                    adv_ts = c.get("last_advert")  # remote clock — may drift (or uptime counter if no sync)
                    # Sanity: timestamps < 1e9 (before Sep 2001) are uptime counters
                    # from devices without clock sync, not real Unix time.
                    if adv_ts and adv_ts < 1000000000:
                        adv_ts = last_ts  # fall back to our local receipt time
                    last_str = _fmt_ts(last_ts)
                    if adv_ts:
                        adv_str = _fmt_ts(adv_ts)
                        if adv_str and adv_str != last_str:
                            last_str = f"{last_str} (reklama: {adv_str})" if last_str else adv_str
                    flags = c.get("flags", 0)
                    flag_desc = []
                    if flags & 1: flag_desc.append("Repeater")
                    if flags & 2: flag_desc.append("Ma pozycje")
                    if flags & 4: flag_desc.append("Ma telemetrie")
                    result.append({
                        "key": key[:12],
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "dist_km": dist,
                        "type": c.get("type"),
                        "flags": flags,
                        "flag_desc": ", ".join(flag_desc) if flag_desc else None,
                        "tx_power": c.get("tx_power"),
                        "last_seen": last_str,
                        "lastmod": c.get("lastmod"),
                        "last_advert": adv_ts,  # corrected — uptime counters fallen back to lastmod
                        "public_key": c.get("public_key") or None,
                        "path_len": c.get("out_path_len"),
                        "path_hash": c.get("out_path_hash_mode"),
                        "_raw": {k: _safe_json(v) for k, v in c.items() if k not in ("public_key",)},  # full raw data minus pubkey (shown separately)
                    })
                return JSONResponse({"count": len(result), "contacts": result})
            return JSONResponse({"count": 0, "contacts": []})
        except Exception as e:
            return JSONResponse({"error": str(e)})

    @app.get("/api/messages")
    async def api_messages(request: Request):
        if not _rate_check(request, RATE_GET_MAX):
            return JSONResponse({"error": "Too many requests"}, status_code=429)
        with _state_lock:
            messages = list(_msg_history)
        return JSONResponse(messages)

    @app.get("/api/messages/search")
    async def api_messages_search(request: Request, q: str = "", ch: str = "",
                                    limit: int = 100, offset: int = 0):
        """Full-text search in message history (SQLite)."""
        if not _rate_check(request, RATE_GET_MAX):
            return JSONResponse({"error": "Too many requests"}, status_code=429)
        limit = min(limit, 500)
        rows = await _db_search_async(search=q, channel=ch, limit=limit, offset=offset)
        total = await _db_count_async(search=q, channel=ch)
        return JSONResponse({"total": total, "limit": limit, "offset": offset, "messages": rows})

    @app.post("/api/send")
    async def api_send(request: Request):
        if not _rate_check(request, RATE_SEND_MAX):
            return JSONResponse({"error": "Too many requests"}, status_code=429)
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            body = await request.json()
            ch = int(body.get("channel", 0))
            max_ch = _device_info.get("max_channels", 40) or 40
            if not (0 <= ch < max_ch):
                return JSONResponse({"error": f"Nieprawidlowy kanal {ch} (zakres 0-{max_ch - 1})"}, status_code=400)
            text = str(body.get("text", "")).strip()
            if not text:
                return JSONResponse({"error": "Empty message"})
            if len(text) > 200:
                text = text[:200]
            send_ts = int(time.time())
            r = await _mc_call(_mc_ref.commands.send_chan_msg(ch, text), timeout=5)
            if r.type.name != "ERROR":
                await _push_msg("out", f"CH{ch}", "JA", text)
                pid = await _track_packet("JA", "", text, f"CH{ch}", "", 0, None, None, send_ts, is_outbound=True)
                if pid:
                    _register_ack_target(pid, f"CH{ch}", "", send_ts, text, getattr(r, "payload", None))
                _log(f"-> kanal{ch}: {text[:60]}")
                await send_tg_html(f"📤 <b>Kanal {ch}</b>\n{esc(text)}")
                with _state_lock:
                    ack_count = len(_packet_observers.get(pid, set())) if pid else 0
                return JSONResponse({"ok": True, "acks": ack_count})
            return JSONResponse({"error": r.payload.get("reason", "unknown") if r.payload else "unknown"})
        except Exception as e:
            return JSONResponse({"error": str(e)})

    @app.post("/api/send/datagram")
    async def api_send_datagram(request: Request):
        if not _rate_check(request, RATE_SEND_MAX):
            return JSONResponse({"error": "Too many requests"}, status_code=429)
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            body = await request.json()
            ch = int(body.get("channel", 0))
            data_type = int(body.get("data_type", 0))
            payload_hex = str(body.get("payload", ""))
            payload = bytes.fromhex(payload_hex)
            if not payload:
                return JSONResponse({"error": "Empty payload"})
            send_fn = getattr(request.app.state, "send_datagram", None)
            if not send_fn:
                return JSONResponse({"error": "Not ready"})
            r = await send_fn(ch, data_type, payload)
            if r.type.name != "ERROR":
                return JSONResponse({"ok": True})
            return JSONResponse({"error": r.payload.get("reason", "unknown") if r.payload else "unknown"})
        except ValueError:
            return JSONResponse({"error": "Invalid hex payload"})
        except Exception as e:
            return JSONResponse({"error": str(e)})

    @app.get("/api/packets")
    async def api_packets(request: Request, limit: int = 100):
        if not _rate_check(request, RATE_GET_MAX):
            return JSONResponse({"error": "Too many requests"}, status_code=429)
        rows = await _db_get_packets_async(min(limit, _MAX_PACKETS))
        return JSONResponse({"packets": rows, "total": len(rows)})

    @app.get("/api/system")
    async def api_system():
        try:
            import platform, psutil
            hostname = platform.node()
            ip = ""
            for iface, addrs in psutil.net_if_addrs().items():
                for a in addrs:
                    if a.family == 2 and not a.address.startswith("127."):  # AF_INET
                        ip = a.address
                        break
                if ip:
                    break
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage("/")
            uptime_s = time.time() - psutil.boot_time()
            uptime = f"{int(uptime_s // 86400)}d {int((uptime_s % 86400) // 3600)}h {int((uptime_s % 3600) // 60)}m"
            cpu_temp = None
            try:
                with open("/sys/class/thermal/thermal_zone0/temp") as f:
                    cpu_temp = round(int(f.read().strip()) / 1000, 1)
            except Exception:
                pass
            return JSONResponse({
                "hostname": hostname,
                "ip": ip,
                "uptime": uptime,
                "ram": f"{mem.used // 1048576} / {mem.total // 1048576} MB ({mem.percent}%)",
                "disk": f"{disk.used // 1073741824} / {disk.total // 1073741824} GB ({disk.percent}%)",
                "cpu_temp": f"{cpu_temp}°C" if cpu_temp else "N/A",
                "arch": platform.machine(),
            })
        except ImportError:
            return JSONResponse({"hostname": "unknown", "ip": "unknown", "error": "psutil not installed"})
        except Exception as e:
            return JSONResponse({"error": str(e)})

    @app.post("/api/system/reboot-pi")
    async def api_reboot_pi(request: Request):
        try:
            asyncio.create_task(_delayed_reboot())
            return JSONResponse({"ok": True, "msg": "Reboot za 3 sekundy..."})
        except Exception as e:
            return JSONResponse({"error": str(e)})

    @app.post("/api/device/reboot")
    async def api_reboot(request: Request):
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            await _mc_ref.commands.reboot()
            return JSONResponse({"ok": True})
        except Exception as e:
            return JSONResponse({"error": str(e)})

    @app.post("/api/device/config")
    async def api_device_config(request: Request):
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON body"})

        results = []

        async def _run(cmd_str, coro):
            try:
                r = await _mc_call(coro, timeout=5)
                ok = r.type.name != "ERROR"
                payload = r.payload if hasattr(r, "payload") else None
                results.append({"action": cmd_str, "ok": ok, "payload": payload})
            except asyncio.TimeoutError:
                results.append({"action": cmd_str, "ok": False, "error": "timeout"})
            except Exception as e:
                results.append({"action": cmd_str, "ok": False, "error": str(e)})

        # ── Device Configuration ───────────────────────────────
        if "name" in body and body["name"]:
            n = str(body["name"])[:32]
            await _run("set_name", _mc_ref.commands.set_name(n))
        if "tx_power" in body:
            v = int(body["tx_power"])
            if not (2 <= v <= 22):
                results.append({"action": "set_tx_power", "ok": False, "error": "zakres 2-22 dBm"})
            else:
                await _run("set_tx_power", _mc_ref.commands.set_tx_power(v))
        if "coords" in body and isinstance(body["coords"], (list, tuple)) and len(body["coords"]) == 2:
            lat, lon = float(body["coords"][0]), float(body["coords"][1])
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                results.append({"action": "set_coords", "ok": False, "error": "niepoprawne wspolrzedne"})
            else:
                await _run("set_coords", _mc_ref.commands.set_coords(lat, lon))
        if "devicepin" in body and body["devicepin"]:
            await _run("set_devicepin", _mc_ref.commands.set_devicepin(int(body["devicepin"])))
        if "custom_var" in body and isinstance(body["custom_var"], dict):
            cv = body["custom_var"]
            await _run("set_custom_var",
                       _mc_ref.commands.set_custom_var(str(cv.get("key", "")), str(cv.get("value", ""))))

        # ── Radio Configuration ────────────────────────────────
        if all(k in body for k in ["freq", "bw", "sf", "cr"]):
            freq, bw = float(body["freq"]), float(body["bw"])
            sf, cr = int(body["sf"]), int(body["cr"])
            if not (860 <= freq <= 880):
                results.append({"action": "set_radio", "ok": False, "error": "freq zakres 860-880 MHz"})
            elif not (7 <= sf <= 12):
                results.append({"action": "set_radio", "ok": False, "error": "SF zakres 7-12"})
            elif not (5 <= cr <= 8):
                results.append({"action": "set_radio", "ok": False, "error": "CR zakres 5-8"})
            else:
                await _run("set_radio", _mc_ref.commands.set_radio(freq, bw, sf, cr))
        if all(k in body for k in ["rx_dly", "af"]):
            await _run("set_tuning",
                       _mc_ref.commands.set_tuning(int(body["rx_dly"]), int(body["af"])))

        # ── Telemetry Configuration ────────────────────────────
        for tm_key, tm_field in [("telemetry_mode_base", "set_telemetry_mode_base"),
                                  ("telemetry_mode_loc", "set_telemetry_mode_loc"),
                                  ("telemetry_mode_env", "set_telemetry_mode_env")]:
            if tm_key in body:
                v = int(body[tm_key])
                if not (0 <= v <= 3):
                    results.append({"action": tm_field, "ok": False, "error": "zakres 0-3"})
                else:
                    await _run(tm_field, getattr(_mc_ref.commands, tm_field)(v))
        if "manual_add_contacts" in body:
            await _run("set_manual_add_contacts",
                       _mc_ref.commands.set_manual_add_contacts(bool(body["manual_add_contacts"])))
        if "advert_loc_policy" in body:
            await _run("set_advert_loc_policy",
                       _mc_ref.commands.set_advert_loc_policy(int(body["advert_loc_policy"])))

        # ── Advanced ───────────────────────────────────────────
        if "multi_acks" in body:
            await _run("set_multi_acks",
                       _mc_ref.commands.set_multi_acks(bool(body["multi_acks"])))
        if "flood_scope" in body:
            await _run("set_flood_scope", _mc_ref.commands.set_flood_scope(str(body["flood_scope"])))
        if "path_hash_mode" in body:
            await _run("set_path_hash_mode",
                       _mc_ref.commands.set_path_hash_mode(int(body["path_hash_mode"])))

        return JSONResponse({"results": results})

    @app.get("/api/device/stats")
    async def api_device_stats(request: Request):
        if not _rate_check(request, RATE_GET_MAX):
            return JSONResponse({"error": "Too many requests"}, status_code=429)
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        out = {}
        cmds = _mc_ref.commands
        try:
            r = await _mc_call(cmds.get_bat(), timeout=5)
            out["bat"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["bat"] = None
        try:
            r = await _mc_call(cmds.get_time(), timeout=5)
            out["time"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["time"] = None
        try:
            r = await _mc_call(cmds.get_stats_core(), timeout=5)
            out["stats_core"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["stats_core"] = None
        try:
            r = await _mc_call(cmds.get_stats_radio(), timeout=5)
            out["stats_radio"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["stats_radio"] = None
        try:
            r = await _mc_call(cmds.get_stats_packets(), timeout=5)
            out["stats_packets"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["stats_packets"] = None
        return JSONResponse(out)

    @app.get("/api/device/channels")
    async def api_device_channels(request: Request):
        if not _rate_check(request, RATE_GET_MAX):
            return JSONResponse({"error": "Too many requests"}, status_code=429)
        global _device_info, _device_info_ts
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        # Refresh cache if stale (>5 min)
        now = time.time()
        if not _device_info or now - _device_info_ts > 300:
            try:
                r = await _mc_call(_mc_ref.commands.send_device_query(), timeout=5)
                if r.type.name != "ERROR":
                    _device_info = r.payload
                    _device_info_ts = now
            except Exception:
                pass
        max_ch = _device_info.get("max_channels", 40) or 40
        channels = []
        for idx in range(max_ch):
            try:
                r = await _mc_call(_mc_ref.commands.get_channel(idx), timeout=5)
                ch = _safe_json(r.payload) if r.type.name != "ERROR" else None
                channels.append(ch)
            except Exception:
                channels.append(None)
        return JSONResponse({"channels": channels})

    config = uvicorn.Config(app, host="0.0.0.0", port=WEB_PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    global _http, _mc_ref, _mc_cmd_lock
    _log("MeshCore <=> Telegram Bridge v5")
    _init_db()  # SQLite full history
    _load_msg_file()
    # Migrate existing JSON history into DB (only if DB is empty)
    if _msg_history and _db_count() == 0:
        _log(f"Migruje {len(_msg_history)} wiadomosci z JSON do SQLite...")
        for m in _msg_history:
            _db_insert(m["dir"], m["ch"], m["from"], m["text"])
        _log("Migracja zakonczona")

    cfg = load_config()
    _validate_config(cfg)
    _http = httpx.AsyncClient(timeout=30)

    if not cfg.get("telegram", {}).get("bot_token"):
        _log("Telegram: nie skonfigurowany")

    import meshcore
    from meshcore.tcp_cx import TCPConnection
    mc_cfg = cfg.get("meshcore", {}).get("connection", {})
    conn = TCPConnection(mc_cfg.get("host", "localhost"), int(mc_cfg.get("port", 5000)))
    mc = meshcore.MeshCore(conn, debug=(LOG_LEVEL == "DEBUG"),
                            auto_reconnect=True, max_reconnect_attempts=0)
    _mc_ref = mc
    _mc_cmd_lock = asyncio.Lock()

    async def _send_dm_ack(dst_key: str, msg: str):
        """Send DM with need-ack flag (bit 0). ACK comes async via on_ack."""
        ts = int(time.time())
        dst_bytes = bytes.fromhex(dst_key)
        data = (b"\x02\x01"  # CMD_SEND_CONTACT_MSG, flags=0x01 (need_ack)
                + (0).to_bytes(1, "little")           # attempt
                + ts.to_bytes(4, "little")            # timestamp
                + dst_bytes                           # pubkey prefix (6 bytes)
                + msg.encode("utf-8"))
        return await _mc_call(mc.commands.send(data,
            [meshcore.EventType.MSG_SENT, meshcore.EventType.ERROR]), timeout=5)

    async def _send_channel_datagram(channel: int, data_type: int, payload: bytes):
        """Send binary datagram to channel (CMD_SEND_CHANNEL_DATA 0x3E)."""
        data = (b"\x3E"
                + channel.to_bytes(1, "little")
                + b"\xFF"                         # flood
                + data_type.to_bytes(2, "little")
                + payload)
        return await _mc_call(mc.commands.send(data,
            [meshcore.EventType.OK, meshcore.EventType.ERROR]), timeout=5)
    global _send_datagram_fn, _send_dm_ack_fn
    _send_datagram_fn = _send_channel_datagram
    _send_dm_ack_fn = _send_dm_ack

    mc.subscribe(meshcore.EventType.CONTACT_MSG_RECV,
                 lambda e: asyncio.create_task(on_contact_message(mc, e)))
    mc.subscribe(meshcore.EventType.CHANNEL_MSG_RECV,
                 lambda e: asyncio.create_task(on_channel_message(mc, e)))
    mc.subscribe(meshcore.EventType.SELF_INFO,
                 lambda e: asyncio.create_task(on_self_info(mc, e)))
    mc.subscribe(meshcore.EventType.ADVERTISEMENT,
                 lambda e: asyncio.create_task(on_advert(mc, e)))
    mc.subscribe(meshcore.EventType.ACK,
                 lambda e: asyncio.create_task(on_ack(mc, e)))
    # MESSAGES_WAITING: firmware push → wake poller immediately
    _msg_waiting = asyncio.Event()
    mc.subscribe(meshcore.EventType.MESSAGES_WAITING,
                 lambda e: _msg_waiting.set())

    # Start web UI immediately (independent of device connection)
    web_task = asyncio.create_task(start_web())
    _log(f"Web UI: http://0.0.0.0:{WEB_PORT}")

    res = None
    retries = 10
    while True:
        res = await mc.connect()
        if res is not None and res.type != meshcore.EventType.ERROR:
            break
        retries -= 1
        if retries > 0:
            _log(f"Retry polaczenia... ({retries} prob)")
            await asyncio.sleep(5)
        else:
            _log("10 nieudanych prob — czekam 30s i probuje dalej...")
            await asyncio.sleep(30)
            retries = 10

    # Companion Protocol bootstrap per docs: app start and device clock sync.
    try:
        await _mc_call(mc.commands.send_appstart(), timeout=5)
    except Exception as e:
        _log(f"APP_START failed: {e}")
    try:
        await _mc_call(mc.commands.set_time(int(time.time())), timeout=5)
    except Exception as e:
        _log(f"SET_TIME failed: {e}")

    # Start auto-fetch: initializes library's internal event reader
    # that dispatches CONTACT_MSG_RECV, CHANNEL_MSG_RECV, ADVERTISEMENT, ACK.

    # Monkey-patch: add CHANNEL_DATA_RECV (0x1B) support to reader
    from meshcore.packets import PacketType
    if not hasattr(PacketType, "CHANNEL_DATA_RECV"):
        PacketType.CHANNEL_DATA_RECV = 0x1B  # type: ignore[attr-defined]
    _orig_handle = mc._reader.handle_rx
    async def _patched_handle(data: bytearray):
        if data and data[0] == 0x1B and len(data) >= 9:
            import io as _io
            dbuf = _io.BytesIO(data[1:])
            snr_byte = dbuf.read(1)[0]
            snr = (snr_byte if snr_byte < 128 else snr_byte - 256) / 4.0
            dbuf.read(2)  # reserved
            ch = dbuf.read(1)[0]
            path_len = dbuf.read(1)[0]
            data_type = int.from_bytes(dbuf.read(2), "little")
            data_len = dbuf.read(1)[0]
            payload = dbuf.read(data_len)
            _log(f"<- datagram ch{ch} type={data_type} "
                 f"len={data_len} [{snr:.1f}dB] {payload.hex()[:40]}")
            return
        await _orig_handle(data)
    mc._reader.handle_rx = _patched_handle  # type: ignore[method-assign]

    await mc.start_auto_message_fetching()
    _log("Auto-fetch started, event reader active")

    # Enable decrypted channel logs → path, SNR, RSSI in channel messages
    mc.set_decrypt_channel_logs(True)
    async def _load_channels():
        import asyncio as _asyncio
        try:
            r = await _mc_call(mc.commands.send_device_query(), timeout=5)
            max_ch = r.payload.get("max_channels", 40) if r.payload else 40
        except Exception:
            max_ch = 40
        loaded = 0
        for idx in range(max_ch):
            try:
                await _mc_call(mc.commands.get_channel(idx), timeout=2)
                loaded += 1
            except Exception:
                pass
        _log(f"Decrypt channels: {loaded}/{max_ch} loaded")
    asyncio.create_task(_load_channels())

    # Poller: wakes on MESSAGES_WAITING event, falls back to 10s interval.
    global _last_rx_ts
    _last_rx_ts = time.time()  # initialize as alive after successful connect
    async def _keep_alive_poller():
        global _last_rx_ts
        while True:
            try:
                # Wait for firmware push or 10s fallback
                await asyncio.wait_for(_msg_waiting.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass  # no push, poll anyway
            _msg_waiting.clear()
            try:
                if mc.is_connected:
                    r = await _mc_call(mc.commands.get_msg(timeout=5), timeout=6)
                    if r is not None and r.type.name != "ERROR":
                        _last_rx_ts = time.time()
            except Exception as e:
                print(f"[bridge] poller error: {e}", file=sys.stderr)
    asyncio.create_task(_keep_alive_poller())

    # Connection watchdog: if no events for 120s, force reconnect.
    # After 3 consecutive failed pings → restart proxy (dead pyserial fd).
    async def _connection_watchdog():
        global _last_rx_ts
        TIMEOUT_S = 120
        MAX_FAILS = 5  # consecutive failed pings before proxy restart
        fails = 0
        while True:
            await asyncio.sleep(60)
            try:
                # Active ping — get_time with timeout detects dead TCP
                r = await _mc_call(mc.commands.get_time(), timeout=8)
                if r is not None and r.type.name != "ERROR":
                    _last_rx_ts = time.time()
                    fails = 0
                    continue
            except Exception as e:
                err = str(e).strip() or e.__class__.__name__
                _log(f"Watchdog: brak odpowiedzi na ping ({err})")

            fails += 1
            if time.time() - _last_rx_ts > TIMEOUT_S:
                if fails >= MAX_FAILS:
                    _log(f"Watchdog: {fails} nieudanych pingow — wymuszam reconnect")
                    fails = 0

                _log("Watchdog: polaczenie martwe, wymuszam reconnect")
                try:
                    await mc.disconnect()
                except Exception:
                    pass
                try:
                    await mc.connect()
                    _last_rx_ts = time.time()
                    fails = 0
                    _log("Watchdog: reconnect OK")
                except Exception as e:
                    _log(f"Watchdog: reconnect nieudany: {e}")
    asyncio.create_task(_connection_watchdog())
    _log("Nasluchiwanie...")

    # Cleanup: remove contacts inactive >100 weeks (clock drift considered)
    async def _clean_old_contacts():
        try:
            r = await _mc_call(mc.commands.get_contacts(), timeout=15)
            if not r or r.type.name == "ERROR":
                return
            clist = r.payload.get("contacts", [])
            now = int(time.time())
            removed = 0
            for c in clist:
                la = c.get("last_advert", 0)
                if la > 1000000000 and (now - la) / (7*24*3600) > 100:
                    pk = c.get("public_key", "")
                    if len(pk) == 64:
                        try:
                            await mc.commands.remove_contact(pk)
                            removed += 1
                        except Exception:
                            pass
            if removed:
                _log(f"Usunieto {removed} starych kontaktow (>100 tyg)")
        except Exception:
            pass
    asyncio.create_task(_clean_old_contacts())

    # Pre-populate device info cache
    global _device_info_ts, _device_contact_count
    try:
        r = await _mc_call(mc.commands.send_device_query(), timeout=5)
        if r.type.name != "ERROR":
            _device_info = r.payload
            _device_info_ts = time.time()
    except Exception:
        pass
    # Pre-populate device contact count
    try:
        r = await _mc_call(mc.commands.get_contacts(), timeout=10)
        if r.type.name != "ERROR" and r.payload:
            _device_contact_count = len(r.payload)
    except Exception:
        pass

    # Run Telegram polling concurrently
    poll_task = asyncio.create_task(tg_poll_loop(mc))

    tasks = [poll_task, web_task]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        # If any task finished (exception or normal exit), cancel the other
        for t in tasks:
            if not t.done():
                t.cancel()
        # Wait for all to finish cleanly
        await asyncio.gather(*tasks, return_exceptions=True)
    except (asyncio.CancelledError, KeyboardInterrupt):
        _log("Zatrzymywanie...")
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        maybe_stop = mc.stop_auto_message_fetching()
        if asyncio.iscoroutine(maybe_stop):
            await maybe_stop
        await mc.disconnect()
        await _http.aclose()
        _log_executor.shutdown(wait=False, cancel_futures=True)
        _log("Bridge zatrzymany")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stop")
    except Exception as e:
        log.exception(f"Blad: {e}")
        sys.exit(1)
