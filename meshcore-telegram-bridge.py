#!/usr/bin/env python3
"""
MeshCore ⇄ Telegram Bridge
Dwukierunkowa komunikacja: MeshCore → Telegram i Telegram → MeshCore.

Wymagania:
  pip install meshcore meshcore-proxy

Użycie:
  export TELEGRAM_BOT_TOKEN="..."
  export TELEGRAM_CHAT_ID="..."
  python meshcore-telegram-bridge.py

Komendy Telegram:
  /r <tekst>           — odpowiedz ostatniemu nadawcy
  /r <nazwa> <tekst>   — odpowiedz konkretnemu kontaktowi
  /contacts            — lista kontaktów
  /status              — status bridge'a i urządzenia
  /help                — pomoc
"""

import asyncio
import json
import logging
import os
import sys
import urllib.request
import urllib.error
import time
from collections import deque
from datetime import datetime

import meshcore
from meshcore.tcp_cx import TCPConnection

# ── konfiguracja ──────────────────────────────────────────────
MESHCORE_HOST = os.getenv("MESHCORE_HOST", "localhost")
MESHCORE_PORT = int(os.getenv("MESHCORE_PORT", "5000"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_POLL_TIMEOUT = 30  # long-poll timeout (sek)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
# Rate-limiting dla operacji nadawania w sieć mesh (wiadomości/sec)
RATELIMIT_MAX_SENDS = int(os.getenv("RATELIMIT_MAX_SENDS", "5"))    # max wysyłek
RATELIMIT_WINDOW_S = int(os.getenv("RATELIMIT_WINDOW_S", "10"))    # w oknie (sek)
STATE_FILE = os.getenv("STATE_FILE", "meshcore-bridge-state.json")
SAVE_INTERVAL_S = 30  # co tyle sekund flush stanu na dysk (jeśli dirty)
# ───────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("meshcore-bridge")

# cache: pubkey_prefix → nazwa
_contact_cache: dict[str, str] = {}
# widziane nody (z reklam): pubkey → info
_seen_nodes: dict[str, dict] = {}
# ostatni nadawca: nazwa → pubkey_prefix
_last_sender: dict[str, str] = {}
_last_sender_name: str | None = None  # jawnie śledzony, nie zgadywany z dict-order
# offset do Telegram long-poll
_tg_offset = 0
# referencja do mc dla callbacków
_mc_ref = None
# pending tasks — trzymane, żeby GC ich nie sprzątnął w trakcie wykonywania
_pending_tasks: set[asyncio.Task] = set()


# kolejka timestampów wysyłek do rate-limitingu (sliding window)
_tx_timestamps: deque[float] = deque()


def _check_rate_limit() -> bool:
    """Sprawdź, czy nie przekroczono limitu wysyłek w okno.
    Zwraca True jeśli LIMIT PRZEKROCZONY (nie wysyłać)."""
    now = time.monotonic()
    cutoff = now - RATELIMIT_WINDOW_S
    # wyrzuć wpisy starsze niż okno
    while _tx_timestamps and _tx_timestamps[0] < cutoff:
        _tx_timestamps.popleft()
    if len(_tx_timestamps) >= RATELIMIT_MAX_SENDS:
        return True
    _tx_timestamps.append(now)
    return False


def _track_task(coro) -> asyncio.Task:
    """Utwórz Task i trzymaj referencję, żeby GC go nie zabił w locie."""
    task = asyncio.create_task(coro)
    _pending_tasks.add(task)
    task.add_done_callback(_pending_tasks.discard)
    return task


# ── Trwałość stanu (state persistence) ────────────────────────

_dirty = False
_save_lock = asyncio.Lock()


def _mark_dirty():
    """Oznacz stan jako wymagający zapisu."""
    global _dirty
    _dirty = True


def _load_state():
    """Wczytaj zapisany stan z pliku JSON (jeśli istnieje)."""
    global _contact_cache, _seen_nodes, _last_sender, _last_sender_name
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return
    if not isinstance(data, dict):
        return
    _contact_cache.update(data.get("contact_cache", {}))
    _seen_nodes.update(data.get("seen_nodes", {}))
    _last_sender.update(data.get("last_sender", {}))
    if data.get("last_sender_name"):
        _last_sender_name = data["last_sender_name"]
    log.info(f"Stan wczytany z {STATE_FILE}: "
             f"{len(_contact_cache)} kontaktów, {len(_seen_nodes)} nodów")


async def _save_state():
    """Zapisz bieżący stan do pliku JSON (atomic write)."""
    data = {
        "contact_cache": _contact_cache,
        "seen_nodes": _seen_nodes,
        "last_sender": _last_sender,
        "last_sender_name": _last_sender_name,
    }
    tmp_path = STATE_FILE + ".tmp"

    def _write():
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STATE_FILE)  # atomic na tym samym FS

    await asyncio.to_thread(_write)


async def _flush_state():
    """Zapisz stan na dysk, jeśli był modyfikowany."""
    global _dirty
    if not _dirty:
        return
    async with _save_lock:
        if not _dirty:  # double-check pod lockiem
            return
        await _save_state()
        _dirty = False
        log.debug(f"Stan zapisany do {STATE_FILE}")


# ───────────────────────────────────────────────────────────────


async def _tg_api(method: str, data: dict = None) -> dict | None:
    """Wywołaj Telegram Bot API (w osobnym wątku, nie blokuje event loop)."""
    if not TELEGRAM_BOT_TOKEN:
        return None

    def _blocking_call():
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "application/json"} if body else {},
                                     method="POST" if body else "GET")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except Exception as e:
            log.debug(f"TG API {method}: {e}")
            return None

    return await asyncio.to_thread(_blocking_call)


async def send_telegram(text: str) -> bool:
    """Wyślij wiadomość przez Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    result = await _tg_api("sendMessage", {
        "chat_id": TELEGRAM_CHAT_ID, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    })
    ok = result is not None and result.get("ok")
    if ok:
        log.debug(f"TG wysłano OK ({len(text)} znaków)")
    else:
        log.error(f"TG wysłanie NIEUDANE: {result}")
    return ok


async def resolve_contact_name(mc, pubkey_prefix: str) -> str:
    """Znajdź nazwę kontaktu po prefiksie klucza."""
    if pubkey_prefix in _contact_cache:
        return _contact_cache[pubkey_prefix]
    try:
        contact = await mc.get_contact_by_key_prefix(pubkey_prefix)
        if contact and isinstance(contact, dict):
            name = contact.get("adv_name", "") or contact.get("name", "") or pubkey_prefix[:8]
        else:
            name = pubkey_prefix[:8]
    except Exception:
        log.debug(f"Nie udało się rozwiązać kontaktu dla {pubkey_prefix[:8]}", exc_info=True)
        name = pubkey_prefix[:8]
    _contact_cache[pubkey_prefix] = name
    _mark_dirty()
    return name


async def on_contact_message(mc, event):
    """Nowa wiadomość MeshCore → przekaż na Telegram."""
    global _last_sender, _last_sender_name
    payload = event.payload
    if not isinstance(payload, dict):
        return
    text = payload.get("text", "").strip()
    if not text:
        return
    pubkey_prefix = payload.get("pubkey_prefix", "??????")
    timestamp = payload.get("sender_timestamp", 0)
    snr = payload.get("SNR", None)

    sender = await resolve_contact_name(mc, pubkey_prefix)
    _last_sender[sender] = pubkey_prefix  # zapamiętaj do reply
    _last_sender_name = sender
    _mark_dirty()

    time_str = (datetime.fromtimestamp(timestamp).strftime("%d.%m %H:%M")
                if timestamp else datetime.now().strftime("%d.%m %H:%M"))
    snr_str = f" [{snr:.1f}dB]" if snr is not None else ""
    msg = (
        f"📡 <b>MeshCore</b> {time_str}\n"
        f"👤 {sender}{snr_str}\n\n"
        f"{text}\n\n"
        f"—\n💬 Odpisz: /r {sender} <tekst>"
    )
    log.info(f"← od {sender}: {text[:60]}")
    await send_telegram(msg)


async def on_self_info(mc, event):
    payload = event.payload
    if isinstance(payload, dict):
        name = payload.get("name", "?")
        freq = payload.get("radio_freq", 0)
        sf = payload.get("radio_sf", "?")
        freq_str = f" {freq:.1f}MHz SF{sf}" if freq else ""
        log.info(f"Połączono z: {name}{freq_str}")
        await send_telegram(f"🟢 <b>MeshCore Bridge</b> uruchomiony\n📟 {name}{freq_str}")


async def on_channel_message(mc, event):
    """Wiadomość z kanału."""
    payload = event.payload
    if not isinstance(payload, dict):
        return
    text = payload.get("text", "").strip()
    if not text:
        return
    channel_idx = payload.get("channel_idx", "?")
    timestamp = payload.get("sender_timestamp", 0)
    pubkey_prefix = payload.get("pubkey_prefix", "??????")
    sender = await resolve_contact_name(mc, pubkey_prefix)
    time_str = (datetime.fromtimestamp(timestamp).strftime("%d.%m %H:%M")
                if timestamp else datetime.now().strftime("%d.%m %H:%M"))
    msg = (
        f"📢 <b>Kanał {channel_idx}</b> {time_str}\n"
        f"👤 {sender}\n\n"
        f"{text}"
    )
    log.info(f"← kanał {channel_idx} od {sender}: {text[:60]}")
    await send_telegram(msg)


async def on_advert(mc, event):
    """Nowa reklama w sieci — zapamiętaj noda."""
    global _seen_nodes
    payload = event.payload
    if isinstance(payload, dict):
        pubkey = payload.get("public_key", "")
        if pubkey:
            prefix = pubkey[:12]
            if prefix not in _seen_nodes:
                _seen_nodes[prefix] = {"pubkey": pubkey, "first_seen": datetime.now().strftime("%H:%M")}
                _mark_dirty()
                log.info(f"Nowy node w sieci: {prefix[:8]}")


# ── Telegram polling (nasłuchiwanie na komendy) ──────────────

async def handle_tg_command(mc, chat_id: int, text: str):
    """Przetwórz komendę z Telegram."""
    global _last_sender
    text = text.strip()

    if text.startswith("/help") or text == "/start":
        await send_telegram(
            "🤖 <b>MeshCore Bridge — komendy</b>\n\n"
            "/r &lt;tekst&gt; — odpowiedz ostatniemu nadawcy\n"
            "/r &lt;nazwa&gt; &lt;tekst&gt; — odpowiedz konkretnemu kontaktowi\n"
            "/ch &lt;tekst&gt; — wyślij na kanał 0 (#public)\n"
            "/ch &lt;nr&gt; &lt;tekst&gt; — wyślij na konkretny kanał\n"
            "/contacts — lista kontaktów i widzianych nodów\n"
            "/status — status bridge\n"
            "/help — ta pomoc"
        )
        return

    if text == "/contacts":
        try:
            result = await mc.commands.get_contacts()
            if result.type != meshcore.EventType.ERROR:
                contacts = result.payload or {}
                lines = [f"👥 <b>Kontakty ({len(contacts)})</b>"]
                for key, c in list(contacts.items())[:20]:
                    name = c.get("adv_name", "") or c.get("name", "") or key[:8]
                    lines.append(f"  • {name}")
            else:
                lines = ["👥 <b>Kontakty</b>\n  (brak dostępu)"]

            if _seen_nodes:
                lines.append(f"\n📡 <b>Widziane nody ({len(_seen_nodes)})</b>")
                for prefix, info in sorted(_seen_nodes.items(), key=lambda x: x[1].get("first_seen", "")):
                    lines.append(f"  • {prefix[:8]}  [{info.get('first_seen', '?')}]")
            await send_telegram("\n".join(lines))
        except Exception as e:
            await send_telegram(f"❌ Błąd: {e}")
        return

    if text.startswith("/ch") and not text.startswith("/channel"):
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            await send_telegram("❌ Użycie: /ch &lt;tekst&gt; lub /ch &lt;nr&gt; &lt;tekst&gt;\nDomyślny kanał: 0 (#public)")
            return
        if len(parts) == 2:
            # /ch <tekst> → kanał 0
            chan_idx, chan_text = 0, parts[1]
        else:
            # /ch <nr> <tekst> lub /ch <tekst> (gdy tekst ze spacjami)
            try:
                chan_idx = int(parts[1])
                chan_text = parts[2]
            except ValueError:
                # parts[1] to nie numer → całość to tekst na kanał 0
                chan_idx = 0
                chan_text = parts[1] + " " + parts[2]
        if _check_rate_limit():
            await send_telegram(f"⏳ Zbyt wiele wysyłek — poczekaj {RATELIMIT_WINDOW_S}s")
            return
        try:
            result = await mc.commands.send_chan_msg(chan_idx, chan_text)
            if result.type == meshcore.EventType.ERROR:
                reason = result.payload.get("reason", "nieznany błąd")
                await send_telegram(f"❌ Błąd wysyłania na kanał {chan_idx}: {reason}")
            else:
                log.info(f"→ kanał {chan_idx}: {chan_text[:60]}")
                await send_telegram(f"✅ Wysłano na kanał {chan_idx}")
        except Exception as e:
            await send_telegram(f"❌ Błąd: {e}")
        return

    if text == "/status":
        try:
            bat = await mc.commands.get_bat()
            bat_level = bat.payload.get("level", "?") if bat.type != meshcore.EventType.ERROR else "?"
            connected = mc.is_connected
            await send_telegram(
                f"📊 <b>Status bridge</b>\n"
                f"🔗 Połączony: {'✅' if connected else '❌'}\n"
                f"🔋 Bateria: {bat_level}%\n"
                f"👤 Kontakty w cache: {len(_contact_cache)}\n"
                f"📡 Widziane nody: {len(_seen_nodes)}"
            )
        except Exception as e:
            await send_telegram(f"❌ Błąd: {e}")
        return

    if text.startswith("/channel") or text == "/channels":
        parts = text.split()
        if len(parts) > 1 and parts[1].isdigit():
            idx = int(parts[1])
        else:
            idx = 0
        try:
            result = await mc.commands.get_channel(idx)
            if result.type == meshcore.EventType.ERROR:
                await send_telegram(f"❌ Kanał {idx}: brak dostępu lub nie istnieje")
            else:
                ch = result.payload
                name = ch.get("name", "?")
                has_secret = "🔑" if ch.get("secret") and any(b != 0 for b in ch["secret"]) else "🔓"
                await send_telegram(f"📢 <b>Kanał {idx}</b> {has_secret}\nNazwa: {name}\nSkrypt: {'tak' if has_secret == '🔑' else 'nie (tylko nasłuch)'}")
        except Exception as e:
            await send_telegram(f"❌ Błąd: {e}")
        return

    if text.startswith("/r"):
        words = text.split()[1:]  # wszystko po /r
        if not words:
            await send_telegram("❌ Użycie: /r &lt;tekst&gt; lub /r &lt;nazwa&gt; &lt;tekst&gt;")
            return

        if len(words) == 1:
            # /r <tekst> — reply do ostatniego nadawcy
            reply_text = words[0]
            if not _last_sender_name:
                await send_telegram("❌ Brak ostatniego nadawcy. Użyj: /r &lt;nazwa&gt; &lt;tekst&gt;")
                return
            target_name = _last_sender_name
            target_key = _last_sender[target_name]
        else:
            # /r <nazwa...> <tekst> — dopasuj wielowyrazową nazwę kontaktu
            target_name = None
            target_key = None
            reply_text = None
            for i in range(len(words) - 1, 0, -1):
                candidate_name = " ".join(words[:i])
                candidate_text = " ".join(words[i:])
                # szukaj w cache
                for prefix, name in _contact_cache.items():
                    if name.lower() == candidate_name.lower():
                        target_name = candidate_name
                        target_key = prefix
                        reply_text = candidate_text
                        break
                if target_key:
                    break
                # spróbuj przez API
                try:
                    contact = await mc.get_contact_by_name(candidate_name)
                    if contact and isinstance(contact, dict):
                        target_name = candidate_name
                        target_key = contact.get("public_key", "")[:12]
                        reply_text = candidate_text
                        break
                except Exception:
                    log.debug(f"Błąd API przy wyszukiwaniu kontaktu '{candidate_name}'", exc_info=True)
            if not target_key:
                await send_telegram(f"❌ Nie znaleziono kontaktu: {' '.join(words)}")
                return
            _last_sender[target_name] = target_key
            _mark_dirty()

        # wyślij wiadomość przez MeshCore
        if _check_rate_limit():
            await send_telegram(f"⏳ Zbyt wiele wysyłek — poczekaj {RATELIMIT_WINDOW_S}s")
            return
        try:
            result = await mc.commands.send_msg(target_key, reply_text)
            if result.type == meshcore.EventType.ERROR:
                reason = result.payload.get("reason", "nieznany błąd")
                await send_telegram(f"❌ Błąd wysyłania do {target_name}: {reason}")
            else:
                log.info(f"→ do {target_name}: {reply_text[:60]}")
                await send_telegram(f"✅ Wysłano do {target_name}")
        except Exception as e:
            await send_telegram(f"❌ Błąd wysyłania: {e}")
        return

    # Nieznana komenda
    await send_telegram(f"❌ Nieznana komenda: {text}\n/help — pomoc")


async def telegram_poll_loop(mc):
    """Pętla long-polling Telegram — odbiera komendy od użytkownika."""
    global _tg_offset
    log.info("Telegram polling: start")
    while True:
        try:
            params = {"timeout": TELEGRAM_POLL_TIMEOUT, "offset": _tg_offset, "allowed_updates": ["message"]}
            data = await _tg_api("getUpdates", params)
            if data and data.get("ok") and data.get("result"):
                for update in data["result"]:
                    _tg_offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    chat_id = msg.get("chat", {}).get("id")
                    if chat_id and str(chat_id) == str(TELEGRAM_CHAT_ID):
                        text = msg.get("text", "").strip()
                        log.info(f"TG odebrano: {text}")
                        if text and (text.startswith("/") or text.startswith("@")):
                            await handle_tg_command(mc, chat_id, text)
            else:
                log.debug(f"TG poll: {data}")
        except Exception as e:
            log.debug(f"TG poll: {e}")
        await asyncio.sleep(2)
    log.info("Telegram polling: stop")


# ── Główna pętla ──────────────────────────────────────────────

async def run():
    global _mc_ref
    log.info("MeshCore ⇄ Telegram Bridge v2.0")
    log.info(f"Proxy: {MESHCORE_HOST}:{MESHCORE_PORT}")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        log.info("Telegram: skonfigurowany")
    else:
        log.warning("Telegram: NIE skonfigurowany — tylko logowanie")
        log.warning("Ustaw TELEGRAM_BOT_TOKEN i TELEGRAM_CHAT_ID")

    _load_state()

    connection = TCPConnection(MESHCORE_HOST, MESHCORE_PORT)
    mc = meshcore.MeshCore(connection, debug=(LOG_LEVEL == "DEBUG"),
                            auto_reconnect=True, max_reconnect_attempts=0)
    _mc_ref = mc

    mc.subscribe(meshcore.EventType.CONTACT_MSG_RECV, lambda e: _track_task(on_contact_message(mc, e)))
    mc.subscribe(meshcore.EventType.SELF_INFO, lambda e: _track_task(on_self_info(mc, e)))
    mc.subscribe(meshcore.EventType.CHANNEL_MSG_RECV, lambda e: _track_task(on_channel_message(mc, e)))
    mc.subscribe(meshcore.EventType.ADVERTISEMENT, lambda e: _track_task(on_advert(mc, e)))

    res = await mc.connect()
    if res is None or res.type == meshcore.EventType.ERROR:
        log.error("Brak odpowiedzi z Helteca — sprawdź USB i tryb połączenia")
        sys.exit(1)

    await mc.start_auto_message_fetching()
    log.info("Nasłuchiwanie…")

    # Uruchom Telegram polling w tle
    poll_task = asyncio.create_task(telegram_poll_loop(mc))

    try:
        while True:
            await asyncio.sleep(SAVE_INTERVAL_S)
            await _flush_state()
            if not mc.is_connected:
                log.warning("Rozłączono — czekam na rekonnekt…")
    except asyncio.CancelledError:
        log.info("Zatrzymywanie…")
        poll_task.cancel()
        mc.stop_auto_message_fetching()
        await mc.disconnect()


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Zatrzymano przez użytkownika")
    except Exception as e:
        log.exception(f"Krytyczny błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
