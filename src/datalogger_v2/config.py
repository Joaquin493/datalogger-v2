"""Carga y valida `config.yaml` y expone dataclasses inmutables por sección.

La configuración del runtime se define en YAML (ej. `config.yaml` en la raíz
del proyecto). Este módulo:

  - Define los dataclasses que reflejan la estructura esperada.
  - Resuelve rutas relativas (xlsx, db) respecto al directorio del yaml.
  - Permite sobreescribir el `session_secret` con variables de entorno, para
    no versionar secretos en producción.
  - Valida que el secret no quede vacío (falla ruidosamente en el arranque).

El resto de la app recibe siempre una instancia `Config` ya validada.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

log = logging.getLogger("datalogger_v2.config")

# Variables de entorno que sobreescriben el yaml (para no versionar secretos).
ENV_SESSION_SECRET = "DATALOGGER_V2_SESSION_SECRET"
ENV_SESSION_SECRET_FILE = "DATALOGGER_V2_SESSION_SECRET_FILE"


@dataclass
class ModbusConfig:
    """Parámetros del cliente Modbus TCP (host del PLC, unidad, timings)."""
    host: str
    port: int = 502
    unit_id: int = 1
    timeout_s: float = 2.0
    min_cycle_ms: int = 20                  # piso por ciclo — evita martillear al PLC
    reconnect_backoff_s: list[float] = field(default_factory=lambda: [1, 2, 5, 10, 30])
    addressing_scheme: str = "sequential"   # "sequential" | "module_stride:N"


@dataclass
class CatalogConfig:
    """Ubicación del xlsx con el catálogo de variables del PLC."""
    xlsx_path: Path
    sheet: str = "Sheet2"


@dataclass
class StorageConfig:
    """Ruta a la DB SQLite y cupo máximo de eventos (FIFO)."""
    db_path: Path
    max_events: int = 1_000_000


@dataclass
class WebConfig:
    """Bind del servidor HTTP + secret para firmar la cookie de sesión."""
    host: str = "0.0.0.0"
    port: int = 8080
    session_secret: str = ""
    session_max_age_s: int = 28_800         # 8 horas
    # TZ de los timestamps en los exports xlsx/csv. La UI siempre muestra en
    # hora local del browser; esto es sólo para cuando bajás el archivo y lo
    # abrís en Excel. Valor vacío o "UTC" = dejar el ISO original.
    export_timezone: str = "America/Argentina/Buenos_Aires"


@dataclass
class InitialUser:
    """Usuario a sembrar en `users` la primera vez que arranca la app."""
    username: str
    password: str


@dataclass
class Config:
    """Contenedor raíz de toda la configuración ya resuelta."""
    modbus: ModbusConfig
    catalog: CatalogConfig
    storage: StorageConfig
    web: WebConfig
    initial_users: list[InitialUser] = field(default_factory=list)


def load_config(path: str | Path) -> Config:
    """Lee el YAML en `path`, resuelve rutas y overrides, y devuelve un `Config`.

    Overrides del `session_secret` (prioridad descendente):
      1. $DATALOGGER2_SESSION_SECRET (valor literal)
      2. $DATALOGGER2_SESSION_SECRET_FILE (ruta a un archivo con el secret)
      3. `web.session_secret` del yaml

    Lanza ValueError si el secret queda vacío tras aplicar los overrides.
    """
    path = Path(path)
    log.info("Cargando configuración desde %s", path)
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    base_dir = path.parent

    def _resolve(p: str) -> Path:
        """Convierte una ruta del yaml en absoluta, relativa al directorio del yaml."""
        candidate = Path(p)
        return candidate if candidate.is_absolute() else (base_dir / candidate).resolve()

    web_raw = dict(raw["web"])
    # Override de session_secret: env > archivo apuntado por env > yaml.
    # Útil para no versionar secretos en el yaml de producción.
    env_secret = os.environ.get(ENV_SESSION_SECRET)
    env_secret_file = os.environ.get(ENV_SESSION_SECRET_FILE)
    if env_secret:
        web_raw["session_secret"] = env_secret
        log.info("session_secret tomado de $%s", ENV_SESSION_SECRET)
    elif env_secret_file:
        secret_path = Path(env_secret_file)
        web_raw["session_secret"] = secret_path.read_text(encoding="utf-8").strip()
        log.info("session_secret leído de %s ($%s)", secret_path, ENV_SESSION_SECRET_FILE)

    if not web_raw.get("session_secret"):
        raise ValueError(
            "web.session_secret está vacío; seteá el yaml o "
            f"${ENV_SESSION_SECRET}/${ENV_SESSION_SECRET_FILE}"
        )

    cfg = Config(
        modbus=ModbusConfig(**raw["modbus"]),
        catalog=CatalogConfig(
            xlsx_path=_resolve(raw["catalog"]["xlsx_path"]),
            sheet=raw["catalog"].get("sheet", "Sheet2"),
        ),
        storage=StorageConfig(
            db_path=_resolve(raw["storage"]["db_path"]),
            max_events=int(raw["storage"].get("max_events", 1_000_000)),
        ),
        web=WebConfig(**web_raw),
        initial_users=[InitialUser(**u) for u in raw.get("initial_users", [])],
    )
    log.info(
        "Config OK — Modbus=%s:%d unit=%d scheme=%s, xlsx=%s sheet=%s, db=%s fifo=%d, web=%s:%d",
        cfg.modbus.host, cfg.modbus.port, cfg.modbus.unit_id, cfg.modbus.addressing_scheme,
        cfg.catalog.xlsx_path.name, cfg.catalog.sheet,
        cfg.storage.db_path, cfg.storage.max_events,
        cfg.web.host, cfg.web.port,
    )
    return cfg
