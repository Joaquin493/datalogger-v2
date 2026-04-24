"""Entrypoint: levanta poller Modbus + servidor web en el mismo event loop.

Orquestación mínima:
  1. Lee `config.yaml` (CLI `-c`).
  2. Configura logging (consola + archivo rotativo en `data/logs/`).
  3. Abre la DB, inicializa schema y FIFO, siembra usuarios iniciales.
  4. Carga el catálogo desde el xlsx y levanta el `LiveState`.
  5. Arranca en paralelo:
       - `ModbusPoller.run()`  (lee el PLC y escribe eventos)
       - `uvicorn.Server.serve()` (API + UI web)
  6. Maneja señales (SIGINT/SIGTERM) para apagar limpio y loguear SHUTDOWN.

Es el comando `datalogger_v2` instalado por `pyproject.toml`.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import os
import signal
import sys
from pathlib import Path

import uvicorn

from .auth import seed_users
from .catalog import load_catalog
from .config import load_config
from .db import Database
from .poller import ModbusPoller
from .state import LiveState
from .web import create_app


def _setup_logging(log_file: Path | None = None) -> None:
    """Configura el logger raíz: consola UTF-8 + archivo rotativo opcional.

    Respeta `$LOG_LEVEL` (DEBUG/INFO/WARNING/ERROR) y silencia el ruido de
    uvicorn.access y pymodbus.
    """
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    # Forzar UTF-8 en stdout/stderr para que acentos y Ñ no se rompan en consolas Windows (cp1252).
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8")
            except Exception:
                pass
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    root.addHandler(stream_handler)

    # Log a archivo rotativo: cubre post-mortems cuando el journal de systemd rota antes.
    # 10×10MB ≈ 100MB máx — seguro en los 16GB de eMMC del IOT2050.
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8"
        )
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
        logging.getLogger("datalogger_v2").info("Log rotativo en %s (10×10MB)", log_file)

    root.setLevel(level)
    # Silenciar el ruido del access log; dejar errores de uvicorn visibles.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("pymodbus").setLevel(logging.WARNING)


async def _amain(config_path: str) -> int:
    """Corutina principal: wire-up de todos los componentes y event loop.

    Devuelve un rc (0 = OK, 2 = catálogo vacío). El caller hace `sys.exit()`.
    """
    cfg = load_config(config_path)
    # Archivo rotativo junto a la DB (misma carpeta data/).
    _setup_logging(log_file=cfg.storage.db_path.parent / "logs" / "datalogger_v2.log")
    db = Database(cfg.storage.db_path, max_events=cfg.storage.max_events)
    db.init_schema()

    variables = load_catalog(
        cfg.catalog.xlsx_path,
        cfg.catalog.sheet,
        addressing_scheme=cfg.modbus.addressing_scheme,
    )
    if not variables:
        logging.error("Catálogo vacío; nada para loguear. Abortando.")
        return 2
    db.upsert_variables(variables)
    seed_users(db, cfg.initial_users)
    db.insert_sys_event("STARTUP", f"Datalogger V2 iniciado ({len(variables)} variables)")

    live = LiveState()
    live.init_snapshots(variables)

    poller = ModbusPoller(cfg.modbus, variables, db, live)

    app = create_app(cfg, db, live)
    uv_cfg = uvicorn.Config(
        app,
        host=cfg.web.host,
        port=cfg.web.port,
        log_level=os.environ.get("LOG_LEVEL", "info").lower(),
        access_log=False,
    )
    server = uvicorn.Server(uv_cfg)

    stop_event = asyncio.Event()

    def _graceful(*_):
        """Handler de SIGINT/SIGTERM: pide parada ordenada al poller y a uvicorn."""
        logging.info("Señal recibida — deteniendo.")
        poller.stop()
        server.should_exit = True
        stop_event.set()

    # En Windows no hay add_signal_handler; usamos signal.signal.
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, _graceful)
        except NotImplementedError:
            signal.signal(sig, _graceful)

    logging.info(
        "Datalogger V2 iniciado — %d variables (%d %%I, %d %%Q). Modbus %s:%d. Web %s:%d.",
        len(variables),
        sum(1 for v in variables if v.type == "INPUT"),
        sum(1 for v in variables if v.type == "OUTPUT"),
        cfg.modbus.host,
        cfg.modbus.port,
        cfg.web.host,
        cfg.web.port,
    )

    poller_task = asyncio.create_task(poller.run(), name="poller")
    server_task = asyncio.create_task(server.serve(), name="web")

    done, pending = await asyncio.wait(
        {poller_task, server_task}, return_when=asyncio.FIRST_COMPLETED
    )
    poller.stop()
    server.should_exit = True
    for t in pending:
        t.cancel()
    for t in pending:
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
    try:
        db.insert_sys_event("SHUTDOWN", "Datalogger V2 apagado limpiamente")
    except Exception:
        logging.exception("No pude registrar SHUTDOWN")
    db.close()
    return 0


def run() -> None:
    """Punto de entrada sync (instalado como comando `datalogger_v2`).

    Parsea `-c/--config`, configura logging mínimo y ejecuta `_amain` en asyncio.
    """
    parser = argparse.ArgumentParser(prog="datalogger_v2")
    parser.add_argument("-c", "--config", default="config.yaml", help="Ruta a config.yaml")
    args = parser.parse_args()
    # Logging inicial sólo a consola; _amain agrega el archivo rotativo cuando
    # tiene la ruta resuelta desde la config.
    _setup_logging()
    try:
        rc = asyncio.run(_amain(args.config))
    except KeyboardInterrupt:
        rc = 0
    sys.exit(rc)


if __name__ == "__main__":
    run()
