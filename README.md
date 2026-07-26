# MeshCore ⇄ Telegram Bridge

Dwukierunkowy most między siecią [MeshCore](https://github.com/meshcore-dev/meshcore.js) (LoRa) a Telegramem. Odbieraj wiadomości z sieci mesh w Telegramie i wysyłaj odpowiedzi — wszystko przez jednego bota.

Dostępne są dwie wersje:

| Plik | Opis |
|---|---|
| `meshcore-telegram-bridge.py` | Lekki bridge (~500 linii). Konfiguracja przez zmienne środowiskowe. Dla prostych instalacji. |
| `bridge.py` | Rozbudowany bridge z web UI (FastAPI), mapą nodów, historią czatu, autoryzacją. Konfiguracja przez YAML. |

## Wymagania

- Python 3.10+
- [meshcore-proxy](https://github.com/meshcore-dev/meshcore-proxy) — łączy się z Heltec przez USB i wystawia TCP API
- [meshcore](https://pypi.org/project/meshcore/) ([źródła](https://github.com/meshcore-dev/meshcore_py)) — biblioteka Python do komunikacji z meshcore-proxy
- Bot Telegram — utwórz przez [@BotFather](https://t.me/BotFather)

```bash
pip install meshcore meshcore-proxy
```

## Szybki start (wersja lekka)

```bash
export TELEGRAM_BOT_TOKEN="123456:ABC..."
export TELEGRAM_CHAT_ID="123456789"
export MESHCORE_HOST="localhost"
export MESHCORE_PORT="5000"

python meshcore-telegram-bridge.py
```

## Szybki start (wersja rozbudowana)

```bash
# Skopiuj i wypełnij konfigurację
cp config.example.yaml config.yaml
# Edytuj config.yaml — ustaw bot_token, chat_id

# Uruchom (z web UI na porcie 8080)
python bridge.py
```

Web UI dostępne pod `http://<ip-raspberry>:8080` — czat, mapa nodów, panel konfiguracji, logi.

## Komendy Telegram

| Komenda | Działanie |
|---|---|
| `/r <tekst>` | Odpowiedz ostatniemu nadawcy |
| `/r <nazwa> <tekst>` | Odpowiedz konkretnemu kontaktowi po dokładnym dopasowaniu nazwy, także wielowyrazowej |
| `/ch <tekst>` | Wyślij na kanał 0 (#public) |
| `/ch <nr> <tekst>` | Wyślij na konkretny kanał |
| `/channel [nr]` | Pokaż info o kanale (czy zaszyfrowany, nazwa) |
| `/contacts` | Lista kontaktów i widzianych nodów |
| `/status` | Status bridge'a, bateria, połączenie |
| `/help` | Pomoc |

## Instalacja na Raspberry Pi

```bash
git clone https://github.com/<twoje-repo>/meshcore-bridge.git
cd meshcore-bridge
bash install-rpi.sh
```

Skrypt instaluje pakiety, tworzy pliki systemd i uruchamia serwisy:
- `meshcore-proxy.service` — proxy USB ↔ TCP
- `meshcore-telegram.service` — bridge MeshCore ↔ Telegram

### Konfiguracja systemd

Pliki `.service` w repozytorium to szablony — przed użyciem:
1. Podmień `TWÓJ_BOT_TOKEN` i `TWÓJ_CHAT_ID` na prawdziwe wartości
2. Dostosuj ścieżki (`$HOME/meshcore-bridge/`) do swojej instalacji
3. Upewnij się, że port szeregowy w `meshcore-proxy.service` jest poprawny (`/dev/ttyACM0` lub `/dev/ttyUSB0`)

```bash
sudo cp meshcore-proxy.service /etc/systemd/system/
sudo cp meshcore-telegram.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now meshcore-proxy
sudo systemctl enable --now meshcore-telegram
```

## Zmienne środowiskowe (wersja lekka)

| Zmienna | Domyślnie | Opis |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | (wymagane) | Token bota Telegram |
| `TELEGRAM_CHAT_ID` | (wymagane) | Chat ID dla wiadomości |
| `MESHCORE_HOST` | `localhost` | Adres meshcore-proxy |
| `MESHCORE_PORT` | `5000` | Port meshcore-proxy |
| `LOG_LEVEL` | `INFO` | Poziom logowania |
| `STATE_FILE` | `meshcore-bridge-state.json` | Ścieżka pliku stanu |
| `RATELIMIT_MAX_SENDS` | `5` | Max wysyłek w oknie |
| `RATELIMIT_WINDOW_S` | `10` | Okno rate-limit (sekundy) |

## Konfiguracja YAML (wersja rozbudowana)

Patrz [`config.example.yaml`](config.example.yaml) — zawiera wszystkie klucze z komentarzami.

## Bezpieczeństwo

- Bridge ma wbudowany rate-limiting: domyślnie max 5 wysyłek na 10 sekund. Nie da się zalać sieci mesh spamem z Telegrama.

## Architektura

```
Heltec V4 (LoRa)
    │ USB
    ▼
meshcore-proxy ──TCP──► meshcore-telegram-bridge.py ──HTTPS──► Telegram API
                              │
                              ├── _contact_cache (nazwy kontaktów)
                              ├── _seen_nodes (nody w zasięgu)
                              ├── _last_sender (do szybkiego reply)
                              ├── APP_START + SET_TIME bootstrap
                              ├── Rate limiter (sliding window)
                              └── Stan zapisywany na dysk (JSON, atomic write)
```

Stan bridge'a (kontakty, nody, ostatni nadawca) jest zapisywany na dysk co 30 sekund i odtwarzany po restarcie — brak utraty danych przy rebootcie.

## Pliki

```
meshcore-bridge/
├── meshcore-telegram-bridge.py   # Bridge v2 (lekki, env vars)
├── bridge.py                     # Bridge v4 (FastAPI + web UI)
├── tcp_cx.py                     # TCP connection layer (vendored)
├── config.example.yaml           # Szablon konfiguracji YAML
├── install-rpi.sh                # Instalator na Raspberry Pi
├── meshcore-proxy.service        # systemd: proxy USB→TCP
├── meshcore-telegram.service     # systemd: bridge v2
├── meshcore-bridge.service       # systemd: bridge v4
├── meshcore-bot.service          # systemd: alternatywny bot
├── tests/
│   └── test_contact_types.py     # Test zgodności typów kontaktów meshcore
├── .gitignore
└── README.md
```

## Licencja

MIT
