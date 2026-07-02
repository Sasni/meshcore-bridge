#!/usr/bin/env bash
# Instalator MeshCore → Telegram Bridge na Raspberry Pi
# Uruchom: bash install-rpi.sh
# Wymaga: python3, pip, sudo

set -euo pipefail

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BOLD}MeshCore → Telegram Bridge — Instalator na RPi${NC}"
echo ""

# 1. Zainstaluj pakiety Python
echo -e "${YELLOW}[1/4] Instalowanie pakietów Python...${NC}"
pip3 install --user meshcore meshcore-proxy 2>&1 | tail -1

# 2. Utwórz katalog
BRIDGE_DIR="$HOME/meshcore-bridge"
mkdir -p "$BRIDGE_DIR"
SCRIPT_SRC="$(dirname "$0")/meshcore-telegram-bridge.py"
if [ -f "$SCRIPT_SRC" ]; then
    cp "$SCRIPT_SRC" "$BRIDGE_DIR/"
    chmod +x "$BRIDGE_DIR/meshcore-telegram-bridge.py"
    echo -e "${GREEN}  Skrypt skopiowany${NC}"
else
    echo -e "${YELLOW}  Skopiuj meshcore-telegram-bridge.py do $BRIDGE_DIR/ ręcznie${NC}"
fi

# 3. Konfiguracja Telegram
if [ -z "${TELEGRAM_BOT_TOKEN:-}" ]; then
    read -rp "Podaj TELEGRAM_BOT_TOKEN: " TELEGRAM_BOT_TOKEN
fi
if [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
    read -rp "Podaj TELEGRAM_CHAT_ID: " TELEGRAM_CHAT_ID
fi

# 4. Systemd service — meshcore-proxy
echo -e "${YELLOW}[2/4] Tworzenie systemd dla meshcore-proxy...${NC}"
sudo tee /etc/systemd/system/meshcore-proxy.service > /dev/null <<'SVC'
[Unit]
Description=MeshCore USB Proxy (Heltec V4)
After=multi-user.target

[Service]
Type=simple
ExecStart=/home/$USER/.local/bin/meshcore-proxy --serial /dev/ttyACM0
ExecSearchPath=/home/$USER/.local/bin
Restart=always
RestartSec=5
User=$USER

[Install]
WantedBy=multi-user.target
SVC
echo -e "${GREEN}  meshcore-proxy.service utworzony${NC}"

# 5. Systemd service — telegram bridge
echo -e "${YELLOW}[3/4] Tworzenie systemd dla telegram bridge...${NC}"
sudo tee /etc/systemd/system/meshcore-telegram.service > /dev/null <<SVC
[Unit]
Description=MeshCore → Telegram Bridge
After=meshcore-proxy.service
Requires=meshcore-proxy.service

[Service]
Type=simple
ExecStart=$HOME/meshcore-bridge/meshcore-telegram-bridge.py
WorkingDirectory=$HOME/meshcore-bridge
Restart=always
RestartSec=5
User=$USER
Environment=TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN
Environment=TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID

[Install]
WantedBy=multi-user.target
SVC
echo -e "${GREEN}  meshcore-telegram.service utworzony${NC}"

# 6. Uruchom
echo -e "${YELLOW}[4/4] Uruchamianie serwisów...${NC}"
sudo systemctl daemon-reload
sudo systemctl enable meshcore-proxy
sudo systemctl enable meshcore-telegram
sudo systemctl restart meshcore-proxy
sleep 2
sudo systemctl restart meshcore-telegram

echo ""
echo -e "${GREEN}${BOLD}✅ Instalacja zakończona!${NC}"
echo -e "  Sprawdź status:"
echo -e "    sudo systemctl status meshcore-proxy"
echo -e "    sudo systemctl status meshcore-telegram"
echo -e "  Logi:"
echo -e "    sudo journalctl -u meshcore-proxy -f"
echo -e "    sudo journalctl -u meshcore-telegram -f"
