"""API FastAPI + servicio de la SPA estática.

Arma la app web que consume la UI del navegador:
  - Sirve `index.html` y `login.html` desde `static/` (una SPA vanilla JS).
  - Expone endpoints JSON bajo `/api/*` para dashboard, filtros, stats y export.
  - Usa sesión firmada (SessionMiddleware) con el secret de `config.web`.
  - Protege rutas internas con `Depends(require_user)`.
  - Aplica rate-limit en memoria al login contra fuerza bruta.

Rutas HTML:
  GET  /         -> si hay sesión, sirve index.html; si no, redirige a /login
  GET  /login    -> formulario de login
  POST /login    -> autentica y redirige a /
  GET  /logout   -> limpia sesión y redirige a /login

API (requiere sesión):
  GET  /api/me
  GET  /api/status
  GET  /api/variables
  GET  /api/events          -> {items, total}
  GET  /api/stats
  GET  /api/sysevents
  GET  /api/export.xlsx     -> cap 50 k filas (openpyxl es memory-bound)
  GET  /api/export.csv      -> stream paginado (apto para los 1 M del FIFO)

Sin auth:
  GET  /healthz             -> liveness para systemd/monitores externos
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import openpyxl
from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from .auth import authenticate, require_user
from .config import Config
from .db import Database
from .state import LiveState

log = logging.getLogger("datalogger_v2.web")

# Rate limit simple en memoria para /login: desalienta fuerza bruta sin deps.
# Clave = IP; valor = (intentos_fallidos, último_timestamp_monotonic).
_LOGIN_ATTEMPTS: dict[str, tuple[int, float]] = {}
_LOGIN_WINDOW_S = 60.0       # Ventana de agrupación
_LOGIN_MAX_SLEEP_S = 5.0     # Tope del castigo por intento fallido

_STATIC_DIR = Path(__file__).parent / "static"


def _login_penalty(ip: str) -> float:
    """Cuántos segundos esperar antes de procesar este login para `ip`.

    Exponencial: 0, 0.5, 1, 2, 4, 5, 5, … — crece rápido pero topa en 5 s.
    La ventana de 60 s resetea el contador si el atacante da un respiro.
    """
    now = time.monotonic()
    attempts, last = _LOGIN_ATTEMPTS.get(ip, (0, 0.0))
    if now - last > _LOGIN_WINDOW_S:
        attempts = 0
    if attempts == 0:
        return 0.0
    return min(_LOGIN_MAX_SLEEP_S, 0.25 * (2 ** (attempts - 1)))


def _login_record(ip: str, success: bool) -> None:
    """Actualiza el contador de intentos: reset en éxito, +1 en fallo."""
    if success:
        _LOGIN_ATTEMPTS.pop(ip, None)
        return
    attempts, _ = _LOGIN_ATTEMPTS.get(ip, (0, 0.0))
    _LOGIN_ATTEMPTS[ip] = (attempts + 1, time.monotonic())


def _state_from_param(val: Optional[str]) -> Optional[int]:
    """Normaliza el query param `state`: acepta '1'/'0'/'ON'/'OFF'/'true'/'false' o vacío."""
    if val is None or val == "":
        return None
    v = val.strip().lower()
    if v in ("1", "on", "true"):
        return 1
    if v in ("0", "off", "false"):
        return 0
    raise HTTPException(400, f"Valor de state inválido: {val!r}")


def create_app(cfg: Config, db: Database, live: LiveState) -> FastAPI:
    """Construye y devuelve la app FastAPI configurada.

    Registra:
      - Middleware de sesión firmada con `cfg.web.session_secret`.
      - Rutas HTML (login/logout/index) que leen archivos de `static/`.
      - Rutas JSON bajo `/api/*` protegidas con `Depends(require_user)`.
      - `/healthz` sin auth para monitoreo externo.
      - Exports xlsx (cap 50k filas) y CSV streaming (apto para el FIFO completo).
    """
    log.info("Creando app FastAPI — static=%s", _STATIC_DIR)
    app = FastAPI(title="Datalogger V2", version="0.1.0", docs_url=None, redoc_url=None)
    app.add_middleware(
        SessionMiddleware,
        secret_key=cfg.web.session_secret,
        max_age=cfg.web.session_max_age_s,
        same_site="lax",
        https_only=False,
    )

    # ---------- HTML pages ----------

    def _read_static(name: str) -> str:
        """Lee un archivo de /static como texto UTF-8 (index.html o login.html)."""
        return (_STATIC_DIR / name).read_text(encoding="utf-8")

    @app.get("/", response_class=HTMLResponse)
    async def root(request: Request):
        """Home: si hay sesión, sirve la SPA; si no, redirige a /login."""
        if not request.session.get("user"):
            return RedirectResponse("/login", status_code=303)
        return HTMLResponse(_read_static("index.html"))

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request, error: Optional[str] = None):
        """Formulario de login. Si ya hay sesión, redirige a /."""
        if request.session.get("user"):
            return RedirectResponse("/", status_code=303)
        html = _read_static("login.html")
        msg = ""
        if error:
            msg = f'<div class="err">{error}</div>'
        return HTMLResponse(html.replace("<!--ERROR-->", msg))

    @app.post("/login")
    async def login_submit(
        request: Request, username: str = Form(...), password: str = Form(...)
    ):
        """Procesa el form de login: aplica rate-limit, valida, setea sesión."""
        client = request.client.host if request.client else "?"
        penalty = _login_penalty(client)
        if penalty > 0:
            log.warning("Login rate-limit: esperando %.2fs para %s", penalty, client)
            await asyncio.sleep(penalty)
        if not authenticate(db, username, password):
            _login_record(client, success=False)
            log.warning("Login HTTP 303 (inválido) user=%s from=%s", username, client)
            return RedirectResponse(
                "/login?error=Usuario+o+contrase%C3%B1a+inv%C3%A1lidos",
                status_code=303,
            )
        _login_record(client, success=True)
        request.session["user"] = username
        log.info("Sesión iniciada user=%s from=%s", username, client)
        return RedirectResponse("/", status_code=303)

    @app.get("/logout")
    async def logout(request: Request):
        """Limpia la sesión y vuelve al /login."""
        user = request.session.get("user")
        request.session.clear()
        if user:
            log.info("Logout user=%s", user)
        return RedirectResponse("/login", status_code=303)

    # ---------- Health (sin auth, liveness externa) ----------

    @app.get("/healthz")
    async def healthz():
        """Liveness: 200 si el PLC responde y hubo ciclo <30 s; 503 si no.

        No requiere sesión — apto para systemd, Docker healthcheck o monitores.
        """
        link = live.link_status()
        now = datetime.now(timezone.utc)
        last_age_s: Optional[float] = None
        if link.last_ok_ts:
            try:
                last_age_s = (now - datetime.fromisoformat(link.last_ok_ts)).total_seconds()
            except Exception:
                last_age_s = None
        # Se considera "healthy" si hubo al menos un ciclo exitoso reciente.
        # El umbral de 30 s cubre holgado el min_cycle y reconexiones cortas.
        healthy = link.connected and last_age_s is not None and last_age_s < 30
        payload = {
            "status": "ok" if healthy else "degraded",
            "modbus_connected": link.connected,
            "last_cycle_age_s": last_age_s,
            "cycles_total": link.cycles_total,
        }
        return JSONResponse(payload, status_code=200 if healthy else 503)

    # ---------- API ----------

    @app.get("/api/me")
    async def me(request: Request):
        """Devuelve el usuario de la sesión actual (null si no hay login)."""
        return {"user": request.session.get("user")}

    @app.get("/api/status", dependencies=[Depends(require_user)])
    async def status():
        """Estado del enlace Modbus + total de eventos + cupo FIFO."""
        return {
            "link": asdict(live.link_status()),
            "events_total": db.count_events(),
            "max_events": cfg.storage.max_events,
        }

    @app.get("/api/variables", dependencies=[Depends(require_user)])
    async def variables():
        """Snapshot actual de todas las variables (lo que muestra el dashboard)."""
        return [asdict(s) for s in live.snapshot_list()]

    @app.get("/api/events", dependencies=[Depends(require_user)])
    async def events(
        address: Optional[str] = None,
        symbol: Optional[str] = None,
        description: Optional[str] = None,
        state: Optional[str] = None,
        ts_from: Optional[str] = None,
        ts_to: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = Query("id"),
        order: str = Query("desc"),
        limit: int = Query(50, ge=1, le=5000),
        offset: int = Query(0, ge=0),
    ):
        """Historial de eventos con filtros, orden y paginado.

        Devuelve `{items, total}` — `total` es el count aplicando los mismos
        filtros, lo necesita el paginador de la UI.
        """
        st = _state_from_param(state)
        kwargs = dict(
            address=address,
            symbol_like=symbol,
            description_like=description,
            state=st,
            ts_from=ts_from,
            ts_to=ts_to,
            search=search,
        )
        total = db.count_events_filtered(**kwargs)
        rows = db.query_events(sort_by=sort_by, order=order, limit=limit, offset=offset, **kwargs)
        return {"items": [dict(r) for r in rows], "total": total}

    @app.get("/api/stats", dependencies=[Depends(require_user)])
    async def stats(ts_from: Optional[str] = None, ts_to: Optional[str] = None):
        """Agregados por variable (total, ON, OFF, last_event) en la ventana opcional."""
        rows = db.stats_by_variable(ts_from=ts_from, ts_to=ts_to)
        return [dict(r) for r in rows]

    @app.get("/api/sysevents", dependencies=[Depends(require_user)])
    async def sysevents(limit: int = Query(500, ge=1, le=5000)):
        """Últimos eventos de sistema (STARTUP/CONNECT/DISCONNECT/RECONCILED/...)."""
        rows = db.list_sys_events(limit=limit)
        return [dict(r) for r in rows]

    @app.get("/api/export.xlsx", dependencies=[Depends(require_user)])
    async def export_xlsx(
        address: Optional[str] = None,
        symbol: Optional[str] = None,
        description: Optional[str] = None,
        state: Optional[str] = None,
        ts_from: Optional[str] = None,
        ts_to: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = Query("id"),
        order: str = Query("desc"),
        # Cap bajo a propósito: openpyxl acumula todo en RAM antes de guardar;
        # con 2 GB (IOT2050) ~100 k filas ya pesa. Para más, usar /api/export.csv.
        limit: int = Query(50_000, ge=1, le=100_000),
    ):
        """Exporta eventos filtrados como xlsx. Cap bajo; para datasets grandes, /api/export.csv."""
        st = _state_from_param(state)
        rows = db.query_events(
            address=address,
            symbol_like=symbol,
            description_like=description,
            state=st,
            ts_from=ts_from,
            ts_to=ts_to,
            search=search,
            sort_by=sort_by,
            order=order,
            limit=limit,
            offset=0,
        )
        log.info("Export xlsx — %d filas (limit=%d)", len(rows), limit)
        wb = openpyxl.Workbook(write_only=True)
        ws = wb.create_sheet("eventos")
        ws.append(["id", "timestamp", "address", "tag", "description", "state"])
        for r in rows:
            ws.append(
                [
                    r["id"], r["ts"], r["address"], r["symbol"], r["description"],
                    "ON" if r["state"] == 1 else "OFF",
                ]
            )
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="eventos.xlsx"'},
        )

    @app.get("/api/export.csv", dependencies=[Depends(require_user)])
    async def export_csv(
        address: Optional[str] = None,
        symbol: Optional[str] = None,
        description: Optional[str] = None,
        state: Optional[str] = None,
        ts_from: Optional[str] = None,
        ts_to: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = Query("id"),
        order: str = Query("desc"),
        limit: int = Query(1_000_000, ge=1, le=1_000_000),
    ):
        """CSV en streaming — paginado dentro de SQLite, no carga todo a RAM.

        Apto para exportar el FIFO completo (1 M filas) en el IOT2050 sin OOM.
        """
        st = _state_from_param(state)
        log.info("Export csv — inicio (limit=%d)", limit)

        def _gen():
            """Generador que arma el CSV por bloques de 1000 filas (evita buffer gigante)."""
            buf = io.StringIO()
            writer = csv.writer(buf, lineterminator="\n")
            writer.writerow(["id", "timestamp", "address", "tag", "description", "state"])
            yield buf.getvalue()
            buf.seek(0); buf.truncate()

            n = 0
            for r in db.iter_events(
                address=address,
                symbol_like=symbol,
                description_like=description,
                state=st,
                ts_from=ts_from,
                ts_to=ts_to,
                search=search,
                sort_by=sort_by,
                order=order,
                limit=limit,
                batch=2000,
            ):
                writer.writerow([
                    r["id"], r["ts"], r["address"], r["symbol"], r["description"],
                    "ON" if r["state"] == 1 else "OFF",
                ])
                n += 1
                if n % 1000 == 0:
                    yield buf.getvalue()
                    buf.seek(0); buf.truncate()
            if buf.tell():
                yield buf.getvalue()
            log.info("Export csv — %d filas enviadas", n)

        return StreamingResponse(
            _gen(),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="eventos.csv"'},
        )

    # ---------- estáticos (css/js/favicon) ----------

    _STATIC_DIR.mkdir(exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.exception_handler(HTTPException)
    async def _http_exc(request: Request, exc: HTTPException):
        """Normaliza errores: JSON para /api/*, texto plano para HTML. Loguea 4xx/5xx."""
        # Para rutas de API devuelve JSON; para HTML deja que FastAPI lo maneje por defecto.
        if exc.status_code >= 500:
            log.error("HTTP %d en %s: %s", exc.status_code, request.url.path, exc.detail)
        elif exc.status_code not in (401, 303):
            log.warning("HTTP %d en %s: %s", exc.status_code, request.url.path, exc.detail)
        if request.url.path.startswith("/api/"):
            return JSONResponse({"error": exc.detail}, status_code=exc.status_code)
        return Response(str(exc.detail), status_code=exc.status_code)

    return app
