#!/usr/bin/env bash
# Actualiza Datalogger V2 ya instalado: git pull + reinstalar deps + restart systemd.
# Uso:   cd /opt/datalogger_v2 && sudo bash deploy/update.sh

set -euo pipefail

APP_DIR="/opt/datalogger_v2"
APP_USER="datalogger_v2"
SERVICE_FILE="datalogger_v2.service"

if [[ $EUID -ne 0 ]]; then
  echo "[!] Este script necesita sudo (restart del service)."
  exit 1
fi

cd "$APP_DIR"

echo "[*] Trayendo últimos cambios..."
sudo -u "$APP_USER" git pull --ff-only

echo "[*] Reinstalando paquete (en caso de cambios en pyproject.toml)..."
sudo -u "$APP_USER" ./.venv/bin/pip install -e . --quiet

echo "[*] Reiniciando servicio..."
systemctl restart "$SERVICE_FILE"
sleep 2
systemctl status "$SERVICE_FILE" --no-pager --lines=8 || true

echo "[+] Listo. Logs en vivo:   journalctl -u $SERVICE_FILE -f"
