#!/usr/bin/env python3
"""
MeshCore ⇄ Telegram Bridge v4
Dwukierunkowa komunikacja + Web UI (FastAPI).

Usage:
  pip install meshcore httpx pyyaml fastapi uvicorn
  python3 bridge.py
"""

import asyncio, json, logging, os, sys, time, hashlib
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
_last_sender: dict[str, str] = {}        # name → pubkey (for /r name lookup)
_last_sender_name: str | None = None     # most recent sender
_last_sender_key: str | None = None      # their pubkey prefix
_outbound_msgs: dict[str, float] = {}  # msg_hash → timestamp
_msg_acks: dict[str, set] = {}          # msg_text → set of repeater keys that acked
_http: httpx.AsyncClient | None = None
_tg_offset: int = 0
_OFFSET_FILE = Path(CONFIG_PATH.parent, ".tg_offset")
_MSG_FILE = Path(CONFIG_PATH.parent, ".msg_history.json")
_LOG_FILE = Path(CONFIG_PATH.parent, ".bridge.log")
_MAX_PERSIST_LOG = 5 * 1024 * 1024  # 5 MB log rotation

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
_self_info: dict = {}  # cached SELF_INFO
_device_info: dict = {}  # cached device query
_device_info_ts: float = 0.0  # last refresh timestamp
_msg_history: list[dict] = []  # structured message history for chat UI
MAX_MSG_HISTORY = 100
_rate_limits: dict[str, list[float]] = {}  # ip → list of request timestamps
_log_buffer: list = []  # rolling log buffer for web UI
MAX_LOG = 200
RATE_LIMIT_WINDOW = 10  # seconds
RATE_SEND_MAX = 3       # max sends per window
RATE_GET_MAX = 30       # max GETs per window

WEB_PORT = int(os.environ.get("PORT", "8080"))

def _rate_check(request, limit: int) -> bool:
    """Simple sliding-window rate limiter per IP. Returns True if allowed."""
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW
    timestamps = [t for t in _rate_limits.get(ip, []) if t > cutoff]
    if len(timestamps) >= limit:
        return False
    timestamps.append(now)
    _rate_limits[ip] = timestamps
    if len(_rate_limits) > 1000:
        _rate_limits.clear()
    return True

def esc(s: str) -> str:
    """Escape HTML entities in untrusted string."""
    return _html.escape(str(s), quote=True)

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

def _log(msg: str):
    log.info(msg)
    line = f"{datetime.now().strftime('%H:%M:%S')} {msg}"
    _log_buffer.append(line)
    if len(_log_buffer) > MAX_LOG:
        _log_buffer.pop(0)
    # Persist to rotating log file
    try:
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

def _save_msg_file():
    try:
        _MSG_FILE.write_text(json.dumps(list(_msg_history), ensure_ascii=False))
    except Exception:
        pass

def _load_msg_file():
    try:
        if _MSG_FILE.exists():
            data = json.loads(_MSG_FILE.read_text())
            if isinstance(data, list):
                _msg_history.clear()
                _msg_history.extend(data[-MAX_MSG_HISTORY:])
    except Exception:
        pass

def _push_msg(direction: str, channel: str, sender: str, text: str):
    """Push a structured message to the chat history with dedup."""
    # Dedup: skip "in" if same text+channel was just sent as "out"
    if direction == "in":
        for i in range(len(_msg_history) - 1, max(len(_msg_history) - 20, -1), -1):
            m = _msg_history[i]
            if m["dir"] == "out" and m["ch"] == channel and m["text"] == text:
                return  # echo of our own message, skip
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
    _save_msg_file()


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
    try:
        r = await _http.post(url, json=payload or {}, timeout=30)
        return r.json() if r.status_code == 200 else None
    except Exception as e:
        log.debug(f"TG {method}: {e}")
        return None


async def send_tg(text: str, chat_id: str = None) -> bool:
    if chat_id is None:
        chat_id = load_config().get("telegram", {}).get("chat_id", "")
    if not chat_id:
        return False
    msg_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    now = time.time()
    DEDUP_WINDOW = 300  # 5 minutes

    # Clean expired entries
    stale = [k for k, t in list(_outbound_msgs.items()) if now - t > DEDUP_WINDOW]
    for k in stale:
        del _outbound_msgs[k]
    if len(_outbound_msgs) > 500:
        _outbound_msgs.clear()

    if msg_hash in _outbound_msgs:
        return True  # duplicate within window, silently skip
    _outbound_msgs[msg_hash] = now

    r = await tg_api("sendMessage", {
        "chat_id": chat_id, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    })
    ok = r and r.get("ok")
    if not ok:
        log.warning(f"TG send fail: {r}")
    return bool(ok)


# ── MeshCore handlers ────────────────────────────────────────

async def on_contact_message(mc, event):
    global _last_sender
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
    _last_sender[sender] = pk
    _last_sender_name = sender
    _last_sender_key = pk
    t = (datetime.fromtimestamp(ts).strftime("%d.%m %H:%M") if ts
         else datetime.now().strftime("%d.%m %H:%M"))
    s = f" [{snr:.1f}dB]" if snr is not None else ""
    msg = f"📡 <b>MeshCore</b> {t}\n👤 {esc(sender)}{s}\n\n{esc(text)}\n\n\u2014\n💬 Odpisz: /r {esc(sender)} <tekst>"
    _log(f"<- od {sender}: {text[:60]}")
    _push_msg("in", "DM", sender, text)
    await send_tg(msg)


async def on_channel_message(mc, event):
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
        # Try to extract sender from message text prefix (MeshCore convention: "Name: message")
        if ":" in text and len(text.split(":")[0]) < 20:
            prefix = text.split(":")[0].strip()
            if prefix and not prefix.startswith("http") and " " not in prefix:
                sender = prefix
    else:
        sender = await _resolve_name(mc, pk)
    t = (datetime.fromtimestamp(ts).strftime("%d.%m %H:%M") if ts
         else datetime.now().strftime("%d.%m %H:%M"))
    msg = f"📢 <b>Kanal {ch}</b> {t}\n👤 {esc(sender)}\n\n{esc(text)}"
    _log(f"<- kanal{ch} {sender}: {text[:60]}")
    _push_msg("in", f"CH{ch}", sender, text)
    await send_tg(msg)


async def on_ack(mc, event):
    """Track acknowledgements from repeaters for sent messages."""
    p = event.payload
    if isinstance(p, dict):
        key = p.get("from", "")[:8]
        text = p.get("text", "")
        if text and key:
            if text not in _msg_acks:
                _msg_acks[text] = set()
            _msg_acks[text].add(key)
            _log(f"ACK od {key}: {text[:40]}")
    # Cleanup old entries
    if len(_msg_acks) > 100:
        for k in list(_msg_acks.keys())[:50]:
            del _msg_acks[k]


async def on_self_info(mc, event):
    global _self_info
    p = event.payload
    if isinstance(p, dict):
        _self_info = p
        name = p.get("name", "?")
        freq = p.get("radio_freq", 0)
        sf = p.get("radio_sf", "?")
        s = f" {freq:.1f}MHz SF{sf}" if freq else ""
        _log(f"Polaczono z: {name}{s}")
        await send_tg(f"🟢 <b>MeshCore Bridge</b>\n📟 {name}{s}")


async def on_advert(mc, event):
    p = event.payload
    if isinstance(p, dict) and p.get("public_key"):
        prefix = p["public_key"][:12]
        ts = datetime.now().strftime("%H:%M")
        if prefix not in _seen_nodes:
            _seen_nodes[prefix] = {"ts": ts, "lat": p.get("adv_lat"), "lon": p.get("adv_lon")}
            _log(f"Nowy node: {prefix[:8]}")
            if len(_seen_nodes) > _MAX_NODES:
                for k in list(_seen_nodes.keys())[:_MAX_NODES // 3]:
                    del _seen_nodes[k]
        else:
            _seen_nodes[prefix]["ts"] = ts
            if p.get("adv_lat") is not None:
                _seen_nodes[prefix]["lat"] = p.get("adv_lat")
            if p.get("adv_lon") is not None:
                _seen_nodes[prefix]["lon"] = p.get("adv_lon")


async def _resolve_name(mc, prefix: str) -> str:
    if prefix in _contact_cache:
        return _contact_cache[prefix]
    try:
        c = await mc.get_contact_by_key_prefix(prefix)
        name = (c.get("adv_name", "") or c.get("name", "") or prefix[:8]) if c else prefix[:8]
    except Exception as e:
        log.warning(f"Nie mozna rozpoznac kontaktu {prefix}: {e}")
        name = prefix[:8]
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
        await send_tg(
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
            r = await mc.commands.get_bat()
            if r.type.name != "ERROR":
                bat = r.payload.get("level", "?")
        except Exception:
            pass
        await send_tg(
            f"📊 <b>Status</b>\n"
            f"Polaczony: {'tak' if mc.is_connected else 'nie'}\n"
            f"Bateria: {bat}%\n"
            f"Kontakty: {len(_contact_cache)}\n"
            f"Nody: {len(_seen_nodes)}")
        return
    if text == "/contacts":
        try:
            r = await mc.commands.get_contacts()
            contacts = r.payload or {} if r.type.name != "ERROR" else {}
            lines = [f"<b>Kontakty ({len(contacts)})</b>"]
            for key, c in list(contacts.items())[:20]:
                n = c.get("adv_name", "") or c.get("name", "") or key[:8]
                lines.append(f"  \u2022 {n}")
            if _seen_nodes:
                lines.append(f"\n<b>Nody ({len(_seen_nodes)})</b>")
                for pfx in sorted(_seen_nodes)[:20]:
                    lines.append(f"  \u2022 {pfx[:8]}")
            await send_tg("\n".join(lines))
        except Exception as e:
            await send_tg(f"Blad: {e}")
        return
    if text == "/ch" or text.startswith("/ch ") or text.startswith("/channel") or text.startswith("/channel "):
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
                ch, txt = 0, parts[1] + " " + parts[2]
        try:
            txt = txt[:200]
            r = await mc.commands.send_chan_msg(ch, txt)
            if r.type.name == "ERROR":
                await send_tg(f"Blad kanal{ch}: {r.payload.get('reason','?')}")
            else:
                _log(f"-> kanal{ch}: {txt[:60]}")
                _push_msg("out", f"CH{ch}", "TG", txt)
                await send_tg(f"📤 <b>Kanal {ch}</b>\n{esc(txt)}")
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
            target = parts[1]; txt = parts[2]
            key = next((p for p, n in _contact_cache.items() if n.lower() == target.lower()), None)
            if not key:
                try:
                    c = await mc.get_contact_by_name(target)
                    if c: key = c.get("public_key", "")[:12]
                except Exception as e:
                    log.warning(f"/r: nie znaleziono kontaktu '{target}': {e}")
            if not key:
                await send_tg(f"Nie znaleziono: {target}")
                return
            _last_sender[target] = key
        try:
            r = await mc.commands.send_msg(key, txt[:200])
            if r.type.name == "ERROR":
                await send_tg(f"Blad: {r.payload.get('reason','?')}")
            else:
                _log(f"-> do {target}: {txt[:60]}")
                _push_msg("out", "DM", target, txt)
                await send_tg(f"📤 <b>Do {esc(target)}</b>\n{esc(txt)}")
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
            log.debug(f"TG poll: {e}")
        await asyncio.sleep(2)


# ── Web UI (FastAPI) ──────────────────────────────────────────

_session_token: str = ""

LOGIN_FORM = r"""
<div id="login-overlay" style="position:fixed;top:0;left:0;width:100%;height:100%;background:#0a0e1a;display:flex;align-items:center;justify-content:center;z-index:9999">
  <div style="background:#121828;border:1px solid #2a3a4a;border-radius:12px;padding:30px;width:320px;text-align:center">
    <h2 style="margin-bottom:16px;color:#66b8ff">🔐 MeshCore Bridge</h2>
    <input id="login-user" type="text" placeholder="Uzytkownik" style="width:100%;padding:10px;margin-bottom:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;font-size:14px">
    <input id="login-pass" type="password" placeholder="Haslo" style="width:100%;padding:10px;margin-bottom:16px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;font-size:14px" onkeydown="if(event.key==='Enter')doLogin()">
    <button onclick="doLogin()" style="width:100%;padding:10px;background:#1e3a5f;border:none;border-radius:8px;color:#66b8ff;font-weight:600;cursor:pointer;font-size:14px">Zaloguj</button>
    <div id="login-err" style="color:#ff6666;font-size:12px;margin-top:10px;display:none"></div>
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
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MeshCore Bridge</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:#0a0e1a;color:#d0d8e8;padding:20px;max-width:1200px;margin:0 auto}
h1{color:#66b8ff;font-size:22px;margin-bottom:16px}
h2{color:#88c8ff;font-size:16px;margin:16px 0 8px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.card{background:#121828;border:1px solid #1e2a3a;border-radius:12px;padding:14px}
.card .val{font-size:20px;font-weight:600;color:#66b8ff}
.card .lbl{font-size:12px;color:#8899b0;margin-top:2px}
pre.log{background:#080c14;border:1px solid #1a2434;border-radius:8px;padding:10px;font-size:12px;height:300px;overflow:auto;font-family:'Consolas','Courier New',monospace;color:#aabbcc;line-height:1.5}
pre.log .ts{color:#556677}
table{width:100%;border-collapse:collapse;font-size:13px}
td,th{padding:6px 8px;text-align:left;border-bottom:1px solid #1a2434}
th{color:#8899b0;font-weight:500}
.status-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
.dot-green{background:#22c55e}
.dot-red{background:#ef4444}
.nav{display:flex;gap:8px;margin-bottom:20px}
.nav a{padding:6px 14px;border-radius:8px;text-decoration:none;color:#8899b0;font-size:14px;border:1px solid transparent}
.nav a.active,.nav a:hover{background:#1a2a3a;color:#66b8ff;border-color:#2a3a4a}
</style>
</head>
<body>
<div class="nav">
  <a href="/" class="active" data-page="dashboard">Dashboard</a>
  <a href="/chat" data-page="chat">Czat</a>
  <a href="/config" data-page="config">Konfiguracja</a>
</div>
<div id="app">
  <div id="page-dashboard">
    <h1>🔌 MeshCore Bridge</h1>
    <div class="grid" id="stats"></div>
    <h2>💻 Host</h2>
    <div class="grid" id="sys-cards"></div>
    <h2>📟 Urzadzenie</h2>
    <div class="grid" id="device-cards"></div>
    <div id="map" style="height:350px;border-radius:8px;border:1px solid #1a2434;margin-bottom:12px;background:#0a0e1a"></div>
    <h2>📡 Siec</h2>
    <table id="nodes"><tr><th>Node</th><th>Status</th></tr></table>
    <h2>👥 Kontakty</h2>
    <div id="contacts-wrap" style="max-height:300px;overflow-y:auto;background:#080c14;border:1px solid #1a2434;border-radius:8px;padding:8px;font-size:13px">
    <table id="contacts-table" style="width:100%"><tr><th onclick="sortContacts('name')" style="cursor:pointer">Nazwa ▾</th><th onclick="sortContacts('dist_km')" style="cursor:pointer">Odleglosc ▾</th><th onclick="sortContacts('last_seen')" style="cursor:pointer">Widziany ▾</th><th></th></tr></table></div>
    <div id="contact-detail" style="display:none;margin-top:8px;background:#121828;border:1px solid #2a3a4a;border-radius:12px;padding:14px">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <h3 id="cd-name" style="margin:0;color:#66b8ff;font-size:16px"></h3>
        <button onclick="closeDetail()" style="background:none;border:none;color:#8899b0;font-size:20px;cursor:pointer">&times;</button>
      </div>
      <table style="width:100%;margin-top:8px;font-size:13px">
        <tr><td style="color:#8899b0;width:120px">Klucz publiczny</td><td id="cd-key" style="font-family:monospace;font-size:11px;word-break:break-all"></td><td style="width:30px"><button onclick="copyKey()" style="background:none;border:none;color:#66b8ff;cursor:pointer;font-size:13px" title="Kopiuj">📋</button></td></tr>
        <tr><td style="color:#8899b0">Advert Type</td><td id="cd-type" colspan="2"></td></tr>
        <tr><td style="color:#8899b0">Flagi</td><td id="cd-flags" colspan="2"></td></tr>
        <tr><td style="color:#8899b0">Pozycja</td><td id="cd-pos" colspan="2"></td></tr>
        <tr><td style="color:#8899b0">Odleglosc</td><td id="cd-dist" colspan="2"></td></tr>
        <tr><td style="color:#8899b0">TX Power</td><td id="cd-txp" colspan="2"></td></tr>
        <tr><td style="color:#8899b0">Odebrany</td><td id="cd-last" colspan="2"></td></tr>
        <tr><td style="color:#8899b0">Ostatnia modyfikacja</td><td id="cd-mod" colspan="2"></td></tr>
        <tr><td style="color:#8899b0">Sciezka routingu</td><td id="cd-path" colspan="2"></td></tr>
        <tr><td style="color:#8899b0"><button onclick="toggleRaw()" style="background:none;border:none;color:#66b8ff;cursor:pointer;font-size:12px;padding:0">📄 Raw data</button></td><td colspan="2"></td></tr>
      </table>
      <pre id="cd-raw" style="display:none;margin-top:8px;background:#080c14;border:1px solid #1a2434;border-radius:8px;padding:10px;font-size:11px;max-height:200px;overflow:auto;color:#8899b0"></pre>
    </div>
    <h2>📜 Ostatnie wiadomosci</h2>
    <pre class="log" id="log"></pre>
  </div>
  <div id="page-chat" style="display:none">
    <div style="display:flex;gap:12px;height:calc(100vh - 80px)">
      <div style="flex:1;display:flex;flex-direction:column">
        <h1 style="margin-bottom:12px">💬 Czat MeshCore</h1>
        <select id="chat-chan" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin-bottom:8px">
          <option value="0">Kanal 0 (#public)</option>
          <option value="1">Kanal 1</option>
          <option value="2">Kanal 2</option>
          <option value="3">Kanal 3</option>
          <option value="4">Kanal 4</option>
          <option value="5">Kanal 5</option>
          <option value="6">Kanal 6</option>
          <option value="7">Kanal 7</option>
        </select>
        <div id="chat-msgs" style="flex:1;overflow-y:auto;background:#080c14;border:1px solid #1a2434;border-radius:8px;padding:10px;margin-bottom:8px;min-height:300px;font-size:13px">
          <div style="color:#8899b0;text-align:center;padding:20px">Wybierz kanał i czekaj na wiadomosci...</div>
        </div>
        <div style="display:flex;gap:8px">
          <input id="chat-input" type="text" placeholder="Napisz wiadomosc..." style="flex:1;padding:10px 14px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;font-size:14px" onkeydown="if(event.key==='Enter')chatSend()">
          <button onclick="chatSend()" style="padding:10px 20px;background:#1e3a5f;border:none;border-radius:8px;color:#66b8ff;font-weight:600;cursor:pointer;font-size:14px">Wyślij</button>
        </div>
      </div>
    </div>
  </div>
  <div id="page-config" style="display:none">
    <h1>⚙️ Konfiguracja urzadzenia</h1>
    <div class="grid">
      <div class="card">
        <h3>Nazwa</h3>
        <input id="cfg-name" placeholder="WWR01M" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:8px 0">
        <button onclick="setCfg({name: document.getElementById('cfg-name').value})">Zapisz</button>
      </div>
      <div class="card">
        <h3>TX Power (dBm)</h3>
        <input id="cfg-txp" type="number" value="20" min="2" max="22" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:8px 0">
        <button onclick="setCfg({tx_power: +document.getElementById('cfg-txp').value})">Ustaw</button>
      </div>
      <div class="card">
        <h3>Wspolrzedne</h3>
        <input id="cfg-lat" type="number" step="0.000001" placeholder="50.1197" style="width:48%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:4px 0">
        <input id="cfg-lon" type="number" step="0.000001" placeholder="20.2789" style="width:48%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:4px 0">
        <button onclick="setCfg({coords:[+document.getElementById('cfg-lat').value,+document.getElementById('cfg-lon').value]})">Zapisz</button>
      </div>
      <div class="card">
        <h3>PIN urzadzenia</h3>
        <input id="cfg-pin" type="password" placeholder="******" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:8px 0">
        <button onclick="setCfg({devicepin: document.getElementById('cfg-pin').value})">Ustaw PIN</button>
      </div>
      <div class="card" style="grid-column:1/-1">
        <h3>Radio</h3>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px">
          <div><label style="font-size:12px;color:#8899b0">Czest. (MHz)</label><input id="cfg-freq" type="number" step="0.001" value="869.618" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8"></div>
          <div><label style="font-size:12px;color:#8899b0">BW (kHz)</label><input id="cfg-bw" type="number" step="0.1" value="62.5" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8"></div>
          <div><label style="font-size:12px;color:#8899b0">SF</label><input id="cfg-sf" type="number" value="8" min="7" max="12" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8"></div>
          <div><label style="font-size:12px;color:#8899b0">CR (5-8)</label><input id="cfg-cr" type="number" value="8" min="5" max="8" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8"></div>
          <div><label style="font-size:12px;color:#8899b0">RX Dly</label><input id="cfg-rxdly" type="number" value="0" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8"></div>
          <div><label style="font-size:12px;color:#8899b0">AF</label><input id="cfg-af" type="number" value="0" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8"></div>
        </div>
        <button onclick="setCfg({freq:+document.getElementById('cfg-freq').value,bw:+document.getElementById('cfg-bw').value,sf:+document.getElementById('cfg-sf').value,cr:+document.getElementById('cfg-cr').value})" style="margin-top:8px">Zapisz radio</button>
        <button onclick="setCfg({rx_dly:+document.getElementById('cfg-rxdly').value,af:+document.getElementById('cfg-af').value})" style="margin-top:8px">Zapisz tuning</button>
      </div>
      <div class="card">
        <h3>Telemetria</h3>
        <label style="font-size:12px;color:#8899b0">Mode Base</label>
        <select id="cfg-tmb" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:4px 0"><option value="0">0 - OFF</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select>
        <label style="font-size:12px;color:#8899b0">Mode Loc</label>
        <select id="cfg-tml" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:4px 0"><option value="0">0 - OFF</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select>
        <label style="font-size:12px;color:#8899b0">Mode Env</label>
        <select id="cfg-tme" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:4px 0"><option value="0">0 - OFF</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select>
        <button onclick="setCfg({telemetry_mode_base:+document.getElementById('cfg-tmb').value,telemetry_mode_loc:+document.getElementById('cfg-tml').value,telemetry_mode_env:+document.getElementById('cfg-tme').value})" style="margin-top:4px">Zapisz telemetrie</button>
        <hr style="border-color:#1a2434;margin:12px 0">
        <label><input id="cfg-mac" type="checkbox" onchange="setCfg({manual_add_contacts:this.checked})"> Manual add contacts</label>
        <label style="font-size:12px;color:#8899b0;display:block;margin-top:8px">Advert Loc Policy</label>
        <select id="cfg-alp" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:4px 0" onchange="setCfg({advert_loc_policy:+this.value})"><option value="0">0</option><option value="1">1</option><option value="2">2</option></select>
      </div>
      <div class="card">
        <h3>Zaawansowane</h3>
        <label style="font-size:12px;color:#8899b0">Multi ACKs <span style="color:#556;font-weight:normal">(wysyla 2 potwierdzenia zamiast 1 — zwieksza szanse dotarcia ACK)</span></label>
        <input id="cfg-macks" type="checkbox" value="1" style="margin:4px 0;accent-color:#66b8ff">
        <button onclick="setCfg({multi_acks:document.getElementById('cfg-macks').checked?1:0})">Ustaw</button>
        <label style="font-size:12px;color:#8899b0;display:block;margin-top:8px">Flood Scope</label>
        <input id="cfg-fs" type="text" placeholder="#public" style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:4px 0">
        <button onclick="setCfg({flood_scope:document.getElementById('cfg-fs').value})">Ustaw</button>
        <label style="font-size:12px;color:#8899b0;display:block;margin-top:8px">Custom Var (JSON {"key":"...","value":"..."})</label>
        <input id="cfg-cv" type="text" placeholder='{"key":"x","value":"y"}' style="width:100%;padding:8px;border-radius:8px;border:1px solid #2a3a4a;background:#0a0e1a;color:#d0d8e8;margin:4px 0">
        <button onclick="try{setCfg({custom_var:JSON.parse(document.getElementById('cfg-cv').value)})}catch(e){alert('Invalid JSON')}">Ustaw</button>
      </div>
      <div class="card">
        <h3>Akcje</h3>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">
          <button onclick="fetch('/api/device/advert',{method:'POST'}).then(r=>r.json()).then(d=>alert(d.ok?'Advert wyslany':'Blad'))">Wyslij Advert</button>
          <button onclick="if(confirm('Restartowac Helteca?'))fetch('/api/device/reboot',{method:'POST'}).then(r=>r.json()).then(d=>alert(d.ok?'Restart...':'Blad'))" style="background:#ef4444">Restart</button>
          <button onclick="loadDeviceInfo()">Odswiez</button>
        </div>
      </div>
    </div>
    <h2>📋 Info z urzadzenia</h2>
    <pre class="log" id="device-info" style="height:200px">Ladowanie...</pre>
    <h2>📊 Statystyki</h2>
    <div id="stats-container"><pre class="log" id="stats-display" style="height:200px">Ladowanie...</pre></div>
    <h2>📡 Kanaty</h2>
    <div id="channels-container"><pre class="log" id="channels-display" style="height:200px">Ladowanie...</pre></div>
  </div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
let _map=null,_markers=[];
function initMap(lat,lon){if(!_map){_map=L.map('map').setView([lat,lon],9);L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:18,attribution:'&copy; OpenStreetMap'}).addTo(_map);}
updateMarkers(lat,lon);}
function updateMarkers(myLat,myLon){_markers.forEach(m=>_map.removeLayer(m));_markers=[];
if(myLat!=null){_markers.push(L.marker([myLat,myLon],{icon:L.divIcon({html:'<div style="background:#66b8ff;width:12px;height:12px;border-radius:50%;border:2px solid #fff"></div>',iconSize:[12,12],className:''})}).bindPopup('<b>WWR01M</b> (ja)').addTo(_map));}
fetch('/api/device/contacts').then(r=>r.json()).then(d=>{if(d.contacts)d.contacts.forEach(c=>{if(c.lat!=null&&c.lon!=null){const m=L.marker([c.lat,c.lon],{icon:L.divIcon({html:'<div style="background:#88cc66;width:10px;height:10px;border-radius:50%;border:1px solid #fff"></div>',iconSize:[10,10],className:''})});m.bindPopup(`<b>${esc(c.name)}</b><br>${c.dist_km?c.dist_km+' km':'?'}<br>${c.last_seen||''}`);m.addTo(_map);_markers.push(m);}})});
fetch('/api/status').then(r=>r.json()).then(d=>{if(d.node_data)Object.entries(d.node_data).forEach(([k,v])=>{if(v.lat!=null&&v.lon!=null){const m=L.marker([v.lat,v.lon],{icon:L.divIcon({html:'<div style="background:#ffaa44;width:8px;height:8px;border-radius:50%"></div>',iconSize:[8,8],className:''})});m.bindPopup(`<b>${esc(k.slice(0,8))}</b><br>${v.dist?v.dist+' km':'?'}<br>${v.ts||''}`);m.addTo(_map);_markers.push(m);}})})}
function esc(s){return String(s).replace(/[&<>"']/g,function(m){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m];});}
async function load(){const r=await fetch('/api/status');if(r.status===401){showLoginAgain();return};if(!r.ok)return;const d=await r.json();
document.getElementById('stats').innerHTML=
  `<div class="card"><div class="val"><span class="status-dot ${d.connected?'dot-green':'dot-red'}"></span>${d.connected?'Polaczony':'Rozlaczony'}</div><div class="lbl">Status</div></div>`+
  `<div class="card"><div class="val">${esc(d.contacts)}</div><div class="lbl">Kontakty</div></div>`+
  `<div class="card"><div class="val">${esc(d.nodes)}</div><div class="lbl">Widziane nody</div></div>`;
const n=document.getElementById('nodes');
n.innerHTML='<tr><th>Node</th><th>Widziany</th><th>Odleglosc</th></tr>';
d.node_list.forEach(p=>{const nd=d.node_data[p]||{};const dist=nd.dist?nd.dist+' km':'—';n.innerHTML+=`<tr><td>${esc(p.slice(0,8))}</td><td>${esc(nd.ts||'-')}</td><td>${dist}</td></tr>`})}
async function loadLog(){const r=await fetch('/api/log');if(r.status===401){showLoginAgain();return};if(!r.ok)return;const d=await r.json();
document.getElementById('log').textContent=d.log.join('\n');}
async function loadContacts(){const r=await fetch('/api/device/contacts');if(r.status===401){showLoginAgain();return};const d=await r.json();
if(d.contacts){window._cd=d.contacts.slice(0,200);renderContactsTable();
document.querySelector('#contacts-wrap').scrollTop=0;}}
let _sortCol='name',_sortDir=1;
function sortContacts(col){if(_sortCol===col)_sortDir*=-1;else{_sortCol=col;_sortDir=1;}
window._cd.sort((a,b)=>{
  let va=a[col],vb=b[col];
  if(col==='dist_km'){va=va||9999;vb=vb||9999;}
  if(col==='last_seen'){va=va||'';vb=vb||'';}
  if(va<vb)return -1*_sortDir;if(va>vb)return 1*_sortDir;return 0;});
renderContactsTable();}
function renderContactsTable(){
const t=document.getElementById('contacts-table');
t.innerHTML='<tr><th onclick="sortContacts(\'name\')" style="cursor:pointer">Nazwa '+(_sortCol=='name'?'▴':'▾')+'</th><th onclick="sortContacts(\'dist_km\')" style="cursor:pointer">Odleglosc '+(_sortCol=='dist_km'?'▴':'▾')+'</th><th onclick="sortContacts(\'last_seen\')" style="cursor:pointer">Widziany '+(_sortCol=='last_seen'?'▴':'▾')+'</th><th></th></tr>';
window._cd.forEach((c,i)=>{const dst=c.dist_km?c.dist_km+' km':'-';const seen=c.last_seen||'-';
t.innerHTML+='<tr><td>'+esc(c.name)+'</td><td>'+dst+'</td><td style="color:'+(c.last_seen&&c.last_seen.includes('⚠')?'#ff9944':'#8899b0')+'">'+seen+'</td><td style="text-align:right"><button onclick="showContact(\''+c.key+'\')" style="background:none;border:none;color:#8899b0;cursor:pointer;font-size:16px;padding:2px 6px">...</button></td></tr>';});}
// Device type mapping from meshcore v2.3.7 (meshcore_parser.py:13)
// CONTACT_TYPENAMES = ["NONE","CLI","REP","ROOM","SENS"]
// Test: python -c "from meshcore.meshcore_parser import CONTACT_TYPENAMES; print(CONTACT_TYPENAMES)"
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
document.getElementById('contact-detail').style.display='block';}
function closeDetail(){document.getElementById('contact-detail').style.display='none';}
function toggleRaw(){const e=document.getElementById('cd-raw');e.style.display=e.style.display==='none'?'block':'none';}
function copyKey(){const k=document.getElementById('cd-key').textContent;navigator.clipboard.writeText(k).then(()=>{const b=event.target;b.textContent='✅';setTimeout(()=>b.textContent='📋',1500);}).catch(()=>{prompt('Ręcznie skopiuj:',k);});}
async function chatRefresh(){const r=await fetch('/api/messages');const d=await r.json();const ch=document.getElementById('chat-chan').value;const el=document.getElementById('chat-msgs');let h='';d.forEach(m=>{if(m.ch.replace('CH','')==ch||ch==='*'){const me=m.dir==='out';h+='<div style="margin-bottom:8px;padding:8px;border-radius:8px;border:1px solid '+(me?'#1a3a2a':'#1a2a3a')+';background:'+(me?'#0a1a0e':'#0a1218')+'"><div style="font-size:11px;color:#8899b0;margin-bottom:3px">'+(me?'<b style="color:#66b8ff">JA</b>':'<b style="color:#88cc66">'+esc(m.from)+'</b>')+' <span style="color:#556">'+m.ts+'</span> '+m.ch+'</div><div style="line-height:1.5">'+esc(m.text)+'</div></div>'}});el.innerHTML=h||'<div style="color:#8899b0;text-align:center;padding:20px">Brak wiadomosci w kanale '+ch+'</div>';el.scrollTop=el.scrollHeight;}
async function chatSend(){const inp=document.getElementById('chat-input');const t=inp.value.trim();if(!t)return;const ch=document.getElementById('chat-chan').value;const r=await fetch('/api/send',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channel:parseInt(ch),text:t})});const d=await r.json();if(d.ok){inp.value='';chatRefresh();if(d.acks!==undefined){const el=document.getElementById('chat-msgs');el.innerHTML+='<div style=\"font-size:11px;color:#ffaa44;padding:2px 8px\">📡 Odebrano przez '+d.acks+' repeater(ow)</div>';el.scrollTop=el.scrollHeight;setTimeout(chatRefresh,5000)}}else{alert('Blad: '+(d.error||'?'))}}
setInterval(function(){if(document.getElementById('page-chat').style.display!=='none')chatRefresh()},3000);
async function loadDeviceCards(){const r=await fetch('/api/device/info');if(r.status===401){showLoginAgain();return};if(!r.ok)return;const d=await r.json();
const dev=d.device||{};const self=d.self||{};
if(self.adv_lat!=null&&self.adv_lon!=null){
  if(!_map)initMap(self.adv_lat,self.adv_lon);else updateMarkers(self.adv_lat,self.adv_lon);}
const cards=[
  {v:dev.model||'?',l:'Model'},{v:dev.ver||'?',l:'Firmware'},
  {v:self.adv_name||self.name||'?',l:'Nazwa'},
  {v:self.radio_freq!=null?self.radio_freq+' MHz':'?',l:'Czestotliwosc'},
  {v:'SF'+(self.radio_sf||'?'),l:'SF'},
  {v:self.last_snr!=null?self.last_snr+' dB':'?',l:'Ostatni SNR'},
  {v:self.last_rssi!=null?self.last_rssi+' dBm':'?',l:'Ostatni RSSI'},
];
// Fetch stats separately for battery
try{const sr=await fetch('/api/device/stats');const sd=await sr.json();
  cards.push({v:sd.bat&&sd.bat.level?sd.bat.level+' mV':'?',l:'Bateria'});}catch(e){cards.push({v:'?',l:'Bateria'});}
document.getElementById('device-cards').innerHTML=cards.map(c=>`<div class="card"><div class="val">${esc(c.v)}</div><div class="lbl">${c.l}</div></div>`).join('');
// System info
const sr=await fetch('/api/system');const sys=await sr.json();
const sysCards=[
  {v:sys.hostname||'?',l:'Hostname'},{v:sys.ip||'?',l:'IP'},
  {v:sys.uptime||'?',l:'Uptime'},{v:sys.ram||'?',l:'RAM'},
  {v:sys.disk||'?',l:'Disk'},{v:sys.cpu_temp||'?',l:'CPU Temp'},
  {v:sys.arch||'?',l:'Architektura'},
];
document.getElementById('sys-cards').innerHTML=sysCards.map(c=>`<div class="card"><div class="val">${esc(c.v)}</div><div class="lbl">${c.lbl||c.l}</div></div>`).join()+
  '<div style="margin-top:8px;display:flex;gap:8px"><button onclick="loadDeviceCards()" style="padding:6px 14px;background:#1e3a5f;border:none;border-radius:6px;color:#66b8ff;cursor:pointer;font-size:12px">Przeladuj metryki</button>'+
  '<button onclick="rebootPi()" style="padding:6px 14px;background:#5f1e1e;border:none;border-radius:6px;color:#ff6666;cursor:pointer;font-size:12px">Reboot Pi</button></div>';}

async function rebootPi(){if(!confirm('Na pewno zrestartowac Raspberry Pi?'))return;
const r=await fetch('/api/system/reboot-pi',{method:'POST',headers:{'Content-Type':'application/json'}});
const d=await r.json();alert(d.msg||d.error||'OK');}
async function loadDeviceInfo(){const r=await fetch('/api/device/info');const d=await r.json();
document.getElementById('device-info').textContent=JSON.stringify(d,null,2);
if(d.self){const s=d.self;if(s.name)document.getElementById('cfg-name').value=s.name;
if(s.tx_power)document.getElementById('cfg-txp').value=s.tx_power;
if(s.radio_freq)document.getElementById('cfg-freq').value=s.radio_freq;
if(s.radio_bw)document.getElementById('cfg-bw').value=s.radio_bw;
if(s.radio_sf)document.getElementById('cfg-sf').value=s.radio_sf;
if(s.radio_cr)document.getElementById('cfg-cr').value=s.radio_cr;}}
async function setCfg(data){const r=await fetch('/api/device/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});const d=await r.json();alert(JSON.stringify(d.results||d.error));}
async function loadStats(){const r=await fetch('/api/device/stats');const d=await r.json();document.getElementById('stats-display').textContent=JSON.stringify(d,null,2);}
async function loadChannels(){const r=await fetch('/api/device/channels');const d=await r.json();document.getElementById('channels-display').textContent=JSON.stringify(d,null,2);}
// SPA navigation
document.querySelectorAll('.nav a').forEach(a=>{a.addEventListener('click',function(e){e.preventDefault();
document.querySelectorAll('.nav a').forEach(x=>x.classList.remove('active'));this.classList.add('active');
document.querySelectorAll('#app>div').forEach(x=>x.style.display='none');
const page=document.getElementById('page-'+this.dataset.page);
if(page){page.style.display='block'}
if(this.dataset.page==='dashboard'){load();loadLog();loadDeviceCards();loadContacts();if(_map)setTimeout(()=>_map.invalidateSize(),100)};
if(this.dataset.page==='chat')chatRefresh();
if(this.dataset.page==='config'){loadDeviceInfo();loadStats();loadChannels()};
})})
</script>
</body>
</html>"""

LOG_HTML = """<!DOCTYPE html>
<html lang="pl">
<head><meta charset="utf-8"><meta http-equiv="refresh" content="5">
<title>Log - MeshCore Bridge</title>
<style>body{background:#080c14;color:#aabbcc;font:12px/1.6 'Consolas',monospace;padding:10px}.ts{color:#556677}</style>
</head>
<body><pre>""" + '{% for line in log %}<span class="ts">{{ line[:8] }}</span>{{ line[8:] }}\n{% endfor %}</pre></body></html>'


def build_status(mc) -> dict:
    node_list = sorted(_seen_nodes.keys())
    my_lat = _self_info.get("adv_lat")
    my_lon = _self_info.get("adv_lon")
    nodes_with_dist = []
    for p in node_list:
        nd = dict(_seen_nodes[p])
        if nd.get("lat") is not None and nd.get("lon") is not None and my_lat is not None and my_lon is not None:
            nd["dist"] = _haversine(my_lat, my_lon, nd["lat"], nd["lon"])
        else:
            nd["dist"] = None
        nodes_with_dist.append(nd)
    return {
        "connected": mc and mc.is_connected,
        "contacts": len(_contact_cache),
        "nodes": len(_seen_nodes),
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
            auth_cfg = cfg.get("bridge", {}).get("auth", {})
            user = auth_cfg.get("username", "")
            pwd = auth_cfg.get("password", "")
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
            return JSONResponse({"error": "Unauthorized"}, status_code=401)

    app = FastAPI(title="MeshCore Bridge")
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
        return JSONResponse({"log": list(_log_buffer)})

    @app.get("/log")
    async def log_page():
        lines = "".join(f'<span class="ts">{l[:8]}</span>{l[8:]}\n' for l in _log_buffer[-100:])
        return HTMLResponse(LOG_HTML.replace("{% for line in log %}", "").replace("{% endfor %}", "")
                           + "<pre>" + lines + "</pre></body></html>")

    @app.get("/api/device/info")
    async def api_device_info():
        global _device_info, _device_info_ts
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            result = {"self": _self_info}
            r = await asyncio.wait_for(_mc_ref.commands.send_device_query(), timeout=5)
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
            r = await _mc_ref.commands.send_advert(flood=False)
            return JSONResponse({"ok": r.type.name != "ERROR"})
        except Exception as e:
            return JSONResponse({"error": str(e)})

    @app.get("/api/device/contacts")
    async def api_device_contacts():
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        try:
            r = await asyncio.wait_for(_mc_ref.commands.get_contacts(), timeout=10)
            if r.type.name != "ERROR":
                contacts = r.payload or {}
                my_lat = _self_info.get("adv_lat")
                my_lon = _self_info.get("adv_lon")
                result = []
                for key, c in list(contacts.items()):
                    name = c.get("adv_name", "") or c.get("name", "") or key[:12]
                    lat = c.get("adv_lat")
                    lon = c.get("adv_lon")
                    dist = None
                    if lat is not None and lon is not None and my_lat is not None and my_lon is not None:
                        dist = _haversine(my_lat, my_lon, lat, lon)
                    last_ts = c.get("lastmod")  # our clock — when we received it
                    adv_ts = c.get("last_advert")  # remote clock — may drift
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
        return JSONResponse(list(_msg_history))

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
            r = await asyncio.wait_for(_mc_ref.commands.send_chan_msg(ch, text), timeout=5)
            if r.type.name != "ERROR":
                _push_msg("out", f"CH{ch}", "JA", text)
                _log(f"-> kanal{ch}: {text[:60]}")
                await send_tg(f"📤 <b>Kanal {ch}</b>\n{esc(text)}")
                return JSONResponse({"ok": True, "acks": len(_msg_acks.get(text, set()))})
            return JSONResponse({"error": r.payload.get("reason", "unknown") if r.payload else "unknown"})
        except Exception as e:
            return JSONResponse({"error": str(e)})

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
                r = await asyncio.wait_for(coro, timeout=5)
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
            await _run("set_devicepin", _mc_ref.commands.set_devicepin(str(body["devicepin"])))
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

        return JSONResponse({"results": results})

    @app.get("/api/device/stats")
    async def api_device_stats():
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        out = {}
        cmds = _mc_ref.commands
        try:
            r = await asyncio.wait_for(cmds.get_bat(), timeout=5)
            out["bat"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["bat"] = None
        try:
            r = await asyncio.wait_for(cmds.get_time(), timeout=5)
            out["time"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["time"] = None
        try:
            r = await asyncio.wait_for(cmds.get_stats_core(), timeout=5)
            out["stats_core"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["stats_core"] = None
        try:
            r = await asyncio.wait_for(cmds.get_stats_radio(), timeout=5)
            out["stats_radio"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["stats_radio"] = None
        try:
            r = await asyncio.wait_for(cmds.get_stats_packets(), timeout=5)
            out["stats_packets"] = _safe_json(r.payload if r.type.name != "ERROR" else None)
        except Exception:
            out["stats_packets"] = None
        return JSONResponse(out)

    @app.get("/api/device/channels")
    async def api_device_channels():
        global _device_info, _device_info_ts
        if not _mc_ref:
            return JSONResponse({"error": "Not connected"})
        # Refresh cache if stale (>5 min)
        now = time.time()
        if not _device_info or now - _device_info_ts > 300:
            try:
                r = await asyncio.wait_for(_mc_ref.commands.send_device_query(), timeout=5)
                if r.type.name != "ERROR":
                    _device_info = r.payload
                    _device_info_ts = now
            except Exception:
                pass
        max_ch = _device_info.get("max_channels", 8) or 8
        channels = []
        for idx in range(max_ch):
            try:
                r = await asyncio.wait_for(_mc_ref.commands.get_channel(idx), timeout=5)
                ch = _safe_json(r.payload) if r.type.name != "ERROR" else None
                channels.append(ch)
            except Exception:
                channels.append(None)
        return JSONResponse({"channels": channels})

    config = uvicorn.Config(app, host="0.0.0.0", port=WEB_PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    global _http, _mc_ref
    _log("MeshCore <=> Telegram Bridge v5")
    _load_msg_file()
    if _msg_history:
        _log(f"Loaded {len(_msg_history)} chat messages from disk")

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

    # Start web UI immediately (independent of device connection)
    web_task = asyncio.create_task(start_web())
    _log(f"Web UI: http://0.0.0.0:{WEB_PORT}")

    res = None
    retries = 10
    while retries > 0:
        res = await mc.connect()
        if res is not None and res.type != meshcore.EventType.ERROR:
            break
        retries -= 1
        _log(f"Retry polaczenia... ({retries} prob)")
        await asyncio.sleep(5)
    if res is None or res.type == meshcore.EventType.ERROR:
        _log("Blad: brak odpowiedzi z Helteca po 10 probach")
        sys.exit(1)

    # Manual poller: MeshOS 2.0 doesn't fire MESSAGES_WAITING events,
    # so auto-fetch never triggers. Poll directly every 10 seconds.
    async def _keep_alive_poller():
        while True:
            await asyncio.sleep(10)
            try:
                if mc.is_connected:
                    await mc.commands.get_msg()
            except Exception as e:
                print(f"[bridge] poller error: {e}", file=sys.stderr)
    asyncio.create_task(_keep_alive_poller())
    _log("Nasluchiwanie...")

    # Pre-populate device info cache
    global _device_info_ts
    try:
        r = await asyncio.wait_for(mc.commands.send_device_query(), timeout=5)
        if r.type.name != "ERROR":
            _device_info = r.payload
            _device_info_ts = time.time()
    except Exception:
        pass

    # Run Telegram polling concurrently
    poll_task = asyncio.create_task(tg_poll_loop(mc))

    try:
        tasks = [poll_task, web_task]
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
        mc.stop_auto_message_fetching()
        await mc.disconnect()
        await _http.aclose()
        _log("Bridge zatrzymany")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stop")
    except Exception as e:
        log.exception(f"Blad: {e}")
        sys.exit(1)
