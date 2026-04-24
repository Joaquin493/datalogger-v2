# Datalogger V2

Registrador de eventos digitales para PLCs Schneider **Modicon M221** vía **Modbus TCP**, con interfaz web para consulta y análisis.

Diseñado para correr 24/7 en un Siemens SIMATIC IOT2050 (ARM / Linux) o Raspberry Pi, dentro de una LAN industrial.

---

## Qué hace

- Polea el PLC cada 20 ms (configurable), detecta cambios de estado en las 104 I/O digitales (56 `%I` + 48 `%Q`) y los registra en SQLite.
- FIFO de **1 000 000 eventos** (trigger de SQLite, elimina los más viejos al superar el cupo).
- **Web UI** (FastAPI + JS vanilla) accesible desde cualquier navegador en la LAN:
  - Panel de señales en vivo (flash al cambiar de estado).
  - Registro de eventos con filtros (tag, dirección, estado, fechas, búsqueda), paginación y sort.
  - Auto-refresh tipo `tail -f` con pausa inteligente.
  - Contadores por variable (total, ON, OFF, último evento).
  - Sistema (STARTUP / CONNECT / DISCONNECT / ERROR).
  - Export **XLSX** (hasta 50 k) y **CSV** (streaming, apto para el FIFO completo).
  - Presets de fecha (15 min / 1 h / Hoy / 24 h / 7 días), atajos de teclado, tema claro/oscuro.
- Autenticación por usuario + password (bcrypt) con sesión firmada.
- Rate-limit contra fuerza bruta en `/login`.
- Reconexión Modbus con backoff exponencial + `sys_event RECONCILED` para eventos detectados tras un corte.
- Autodetección de **bloques contiguos** de offsets — si tu mapa Modbus tiene huecos (`%Q0.0–0.15` + `%Q3.0–4.15`), lee por bloques separados automáticamente.
- Log rotativo a archivo (10 × 10 MB) + systemd journal.
- Endpoint `/healthz` sin auth para monitoreo externo.

## Stack

- **Backend**: Python 3.10+, FastAPI, uvicorn, pymodbus 3.x, SQLite (WAL).
- **Frontend**: HTML + JS vanilla + CSS (sin build step).
- **Runtime**: asyncio — el poller Modbus y el servidor web corren en el mismo event loop.

## Arquitectura

```
     ┌──────────────┐   Modbus TCP    ┌────────────────┐
     │  Modicon     │  ◄───────────►  │  poller.py     │
     │  M221 (PLC)  │                 │  (asyncio)     │
     └──────────────┘                 └────────┬───────┘
                                               │ diff → eventos
                                               ▼
                               ┌──────────────────────────┐
                               │  SQLite (WAL, FIFO 1M)   │
                               │  data/datalogger_v2.db   │
                               └─────────────┬────────────┘
                                             │ reads
                                             ▼
     ┌──────────────┐    HTTPS / LAN    ┌──────────────┐
     │  Operario    │  ◄────────────►  │  web.py      │
     │  (browser)   │                   │  (FastAPI)   │
     └──────────────┘                   └──────────────┘
```

La fuente de verdad del mapeo entre direcciones Schneider (`%I0.3`, `%Q2.5`) y offsets Modbus es un **xlsx** (`Programa_TTA_IRSA_convertido v3.xlsx`, hoja `Sheet2`) con las 104 señales, símbolos y descripciones.

## Quickstart local (con simulador, sin PLC)

```bash
# 1. venv + instalación
python -m venv .venv
source .venv/bin/activate          # Linux/Mac
# .venv\Scripts\activate             Windows PowerShell
pip install -e .

# 2. config (para dev: apunta al simulador en 127.0.0.1:5020)
cp config.example.yaml config.yaml
# editar web.session_secret con: python -c "import secrets; print(secrets.token_hex(32))"

# 3. en terminal A — simulador Modbus que imita al M221
python scripts/modbus_sim.py

# 4. en terminal B — Datalogger V2
python -m datalogger_v2 -c config.yaml
```

Abrí `http://127.0.0.1:8080` → login `admin` / `admin` → deberías ver el panel de señales moviéndose.

## Configuración (`config.yaml`)

```yaml
modbus:
  host: 192.168.1.10            # IP del PLC
  port: 502                     # 5020 para el simulador local (evita admin)
  unit_id: 1
  timeout_s: 2.0
  min_cycle_ms: 20              # piso entre ciclos del poller
  reconnect_backoff_s: [1, 2, 5, 10, 30]
  addressing_scheme: "sequential"   # o "module_stride:32" según cómo mapee el PLC

catalog:
  xlsx_path: "Programa_TTA_IRSA_convertido v3.xlsx"
  sheet: "Sheet2"

storage:
  db_path: "data/datalogger_v2.db"
  max_events: 1000000           # FIFO

web:
  host: 0.0.0.0                 # 127.0.0.1 si NO querés exponer en LAN
  port: 8080
  session_secret: "<32+ bytes aleatorios>"
  session_max_age_s: 28800

initial_users:
  - username: admin
    password: admin             # cambiar en el primer login
```

El secret también se puede pasar por entorno (recomendado en prod):

- `DATALOGGER_V2_SESSION_SECRET=<valor>`
- `DATALOGGER_V2_SESSION_SECRET_FILE=/ruta/al/archivo`  (permisos `600`)

## Deploy en Linux (IOT2050 / Raspberry Pi)

```bash
# instalar en /opt/datalogger_v2 con venv
sudo mkdir -p /opt/datalogger_v2
sudo chown $USER: /opt/datalogger_v2
git clone <repo> /opt/datalogger_v2    # o scp
cd /opt/datalogger_v2
python3 -m venv .venv
. .venv/bin/activate
pip install -e .

# config + secret externo
cp config.example.yaml config.yaml
python3 -c "import secrets; print(secrets.token_hex(32))" > .session_secret
chmod 600 .session_secret

# usuario del servicio
sudo useradd --system --home /opt/datalogger_v2 --shell /bin/false datalogger_v2
sudo chown -R datalogger_v2:datalogger_v2 /opt/datalogger_v2

# systemd
sudo cp deploy/datalogger_v2.service /etc/systemd/system/
# editar y agregar: Environment=DATALOGGER_V2_SESSION_SECRET_FILE=/opt/datalogger_v2/.session_secret
sudo systemctl daemon-reload
sudo systemctl enable --now datalogger_v2
sudo systemctl status datalogger_v2
```

Ver logs en vivo: `journalctl -u datalogger_v2 -f`

Healthcheck externo: `curl -s http://localhost:8080/healthz` → 200 si hay ciclo OK <30 s.

## API

Todas las rutas `/api/*` requieren sesión (cookie firmada).

| Método | Ruta | Qué hace |
|---|---|---|
| `GET` | `/healthz` | Liveness (sin auth). 200 = OK, 503 = degraded |
| `POST` | `/login` | Form `username`, `password` → sesión |
| `GET` | `/logout` | Cierra sesión |
| `GET` | `/api/me` | Usuario actual |
| `GET` | `/api/status` | Estado del enlace Modbus + total de eventos |
| `GET` | `/api/variables` | Snapshot actual de las 104 señales |
| `GET` | `/api/events` | Historial filtrado + paginado (`{items, total}`) |
| `GET` | `/api/stats` | Agregados por variable (total, ON, OFF, último) |
| `GET` | `/api/sysevents` | Últimos eventos de sistema |
| `GET` | `/api/export.xlsx` | Export filtrado (cap 50 k filas) |
| `GET` | `/api/export.csv` | Export en streaming (apto para 1 M filas) |

Filtros comunes de `/api/events` y `/api/export.*`:
`address`, `symbol`, `description`, `state` (`ON`/`OFF`/`0`/`1`), `ts_from`, `ts_to` (ISO 8601), `search`, `sort_by`, `order`, `limit`, `offset`.

## Estructura del proyecto

```
datalogger2/
├── src/datalogger_v2/
│   ├── main.py          # entrypoint asyncio (poller + uvicorn)
│   ├── config.py        # carga de YAML + overrides por env
│   ├── catalog.py       # lee xlsx Sheet2 → lista de Variable
│   ├── addressing.py    # parseo %Ix.y / %Qx.y + offsets Modbus
│   ├── poller.py        # loop Modbus, detección de cambios
│   ├── db.py            # SQLite (schema, FIFO, queries)
│   ├── state.py         # estado vivo compartido poller ↔ web
│   ├── auth.py          # bcrypt + sesión + rate-limit
│   ├── web.py           # FastAPI + rutas HTML/API
│   └── static/          # SPA (index.html, app.js, style.css)
├── scripts/
│   └── modbus_sim.py    # simulador M221 (scan cycle + categorías de señales)
├── deploy/
│   └── datalogger_v2.service
├── config.yaml          # config local (no versionar)
├── config.example.yaml  # plantilla
└── pyproject.toml
```

## Atajos de teclado (Web UI)

| Tecla | Acción |
|---|---|
| `1` – `4` | Cambiar tab (Señales / Eventos / Contadores / Sistema) |
| `/` | Enfocar el buscador del tab activo |
| `Esc` | Limpiar filtros / quitar foco |
| `T` | Alternar tema claro / oscuro |

## Troubleshooting

- **`only one usage of each socket address`**: ya hay un `datalogger_v2` corriendo. En Windows: `Get-NetTCPConnection -LocalPort 8080` + `Stop-Process -Id <PID> -Force`.
- **`Modbus caído (TimeoutError)`**: `ping` al PLC; chequear firewall puerto 502; verificar `unit_id` en config.
- **`Catálogo vacío`**: el xlsx no se encontró o la hoja no es `Sheet2`. Ajustar `catalog.xlsx_path` / `catalog.sheet` en el yaml.
- **`web.session_secret está vacío`**: completar en yaml o setear `DATALOGGER_V2_SESSION_SECRET(_FILE)`.
- **Acentos rotos en logs (Windows)**: el código ya reconfigura `stdout/stderr` a UTF-8; si aún así se ven mal, forzá `PYTHONIOENCODING=utf-8`.
- **Export XLSX se corta en 50 k**: es el cap para evitar OOM en IOT2050 (openpyxl carga todo a RAM). Para datasets grandes usar **CSV**.

## Licencia / autoría

Proyecto privado — registrador para instalación industrial específica. Ver commits para trazabilidad de cambios.
