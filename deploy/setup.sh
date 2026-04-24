#!/usr/bin/env bash
# Instalación inicial de Datalogger V2 en Linux (IOT2050 / Raspberry Pi).
# Se corre UNA vez por máquina. Para actualizaciones posteriores: deploy/update.sh
#
# Uso (desde la raíz del repo clonado):
#     sudo bash deploy/setup.sh
#
# Requiere: git + python3 + python3-venv instalados.

set -euo pipefail

APP_DIR="/opt/datalogger_v2"
APP_USER="datalogger_v2"
SERVICE_FILE="datalogger_v2.service"

if [[ $EUID -ne 0 ]]; then
  echo "[!] Este script necesita sudo (instala usuario de sistema + unit systemd)."
  exit 1
fi

echo "[*] Instalando Datalogger V2 en $APP_DIR"

# 0. Dependencias del SO (Debian/Ubuntu). Si falta algo, lo instalamos.
# En distros no basadas en apt (Fedora/RHEL/Alpine) adaptar a mano.
if ! command -v apt-get >/dev/null 2>&1; then
  echo "[!] apt-get no disponible. Este script asume Debian/Ubuntu (IOT2050, Raspberry Pi OS)."
  echo "    Instalá a mano: python3 (>=3.10), python3-venv, python3-pip, git."
else
  MISSING=()
  for pkg in python3 python3-venv python3-pip git; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
      MISSING+=("$pkg")
    fi
  done
  if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo "[*] Instalando paquetes faltantes: ${MISSING[*]}"
    apt-get update -qq
    apt-get install -y "${MISSING[@]}"
  else
    echo "[+] Dependencias del SO OK (python3, venv, pip, git)."
  fi
fi

# Verificación de versión de Python (necesitamos >=3.10 por zoneinfo, `|` typing, etc.).
PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_OK=$(python3 -c 'import sys; print(1 if sys.version_info >= (3, 10) else 0)')
if [[ "$PY_OK" != "1" ]]; then
  echo "[!] Python $PY_VER detectado; se necesita >= 3.10. Abortando."
  exit 1
fi
echo "[+] Python $PY_VER OK."

# 1. Crear usuario de sistema sin shell (el servicio corre como este user).
if ! id -u "$APP_USER" >/dev/null 2>&1; then
  useradd --system --home "$APP_DIR" --shell /bin/false "$APP_USER"
  echo "[+] Usuario $APP_USER creado."
fi

# 2. Copiar el repo a /opt (si el script no se está corriendo desde ahí).
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
if [[ "$SRC_DIR" != "$APP_DIR" ]]; then
  mkdir -p "$APP_DIR"
  cp -r "$SRC_DIR/." "$APP_DIR/"
fi
cd "$APP_DIR"

# 3. Venv + dependencias.
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
  echo "[+] venv creado."
fi
./.venv/bin/pip install --upgrade pip --quiet
./.venv/bin/pip install -e . --quiet
echo "[+] Dependencias instaladas."

# 4. Permisos + carpeta de datos.
mkdir -p data/logs
chown -R "$APP_USER:$APP_USER" "$APP_DIR"

# 5. Systemd unit.
install -m 644 "deploy/$SERVICE_FILE" "/etc/systemd/system/$SERVICE_FILE"
systemctl daemon-reload
systemctl enable "$SERVICE_FILE"
echo "[+] Servicio systemd instalado y habilitado."

# 6. Arrancar.
systemctl restart "$SERVICE_FILE"
sleep 2
systemctl status "$SERVICE_FILE" --no-pager --lines=5 || true

cat <<EOF

==========================================================
  Datalogger V2 instalado.

  Ver logs:       journalctl -u $SERVICE_FILE -f
  Reiniciar:      sudo systemctl restart $SERVICE_FILE
  Web UI:         http://\$(hostname -I | awk '{print \$1}'):8080
  Health:         curl -s http://localhost:8080/healthz

  Para actualizar en el futuro:
      cd $APP_DIR && sudo bash deploy/update.sh
==========================================================
EOF
