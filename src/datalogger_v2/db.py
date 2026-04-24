"""SQLite: schema, FIFO de 1 M eventos vía trigger, y capa de acceso.

Modelo:
  - `variables`  : catálogo del PLC (address → symbol/description/type).
  - `events`     : log append-only de cambios de estado (ts, dirección, 0/1).
  - `sys_events` : STARTUP/SHUTDOWN/CONNECT/DISCONNECT/RECONCILED/ERROR.
  - `users`      : usuarios + hash bcrypt.

El FIFO de `events` se implementa con un trigger AFTER INSERT:
    DELETE FROM events WHERE id <= NEW.id - max_events
Con WHEN NEW.id > max_events para no hacer trabajo hasta que se alcance el tope.
`id` es INTEGER PRIMARY KEY AUTOINCREMENT, así que siempre crece y el delete
por rango es un simple recorrido del índice primario.

Notas de performance:
  - Conexión SQLite única reutilizada por toda la app (asyncio single-thread).
  - Cache en memoria del total de `events` para evitar full scan en /api/status.
  - WAL + synchronous=NORMAL: buenas latencias para el patrón escritura/lectura.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from .catalog import Variable

log = logging.getLogger("datalogger_v2.db")


SCHEMA = """
CREATE TABLE IF NOT EXISTS variables (
    address      TEXT PRIMARY KEY,
    type         TEXT NOT NULL CHECK (type IN ('INPUT','OUTPUT')),
    symbol       TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    address      TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    state        INTEGER NOT NULL CHECK (state IN (0,1))
);
CREATE INDEX IF NOT EXISTS idx_events_ts      ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_address ON events(address);
CREATE INDEX IF NOT EXISTS idx_events_state   ON events(state);

CREATE TABLE IF NOT EXISTS sys_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    type         TEXT NOT NULL,      -- STARTUP | SHUTDOWN | CONNECT | DISCONNECT | ERROR
    description  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_sys_events_ts ON sys_events(ts);

CREATE TABLE IF NOT EXISTS users (
    username       TEXT PRIMARY KEY,
    password_hash  TEXT NOT NULL,
    created_ts     TEXT NOT NULL
);
"""


def _fifo_trigger_sql(max_events: int) -> str:
    """Genera el DDL de los triggers FIFO según el cupo configurado."""
    max_events = int(max_events)
    return f"""
    DROP TRIGGER IF EXISTS events_fifo;
    CREATE TRIGGER events_fifo
    AFTER INSERT ON events
    WHEN NEW.id > {max_events}
    BEGIN
        DELETE FROM events WHERE id <= NEW.id - {max_events};
    END;

    DROP TRIGGER IF EXISTS sys_events_fifo;
    CREATE TRIGGER sys_events_fifo
    AFTER INSERT ON sys_events
    WHEN NEW.id > 10000
    BEGIN
        DELETE FROM sys_events WHERE id <= NEW.id - 10000;
    END;
    """


# Campos permitidos para ORDER BY en /api/events (whitelist contra SQL injection).
_EVENTS_SORTABLE = {
    "id": "id",
    "timestamp": "ts",
    "ts": "ts",
    "address": "address",
    "tag": "symbol",
    "symbol": "symbol",
    "state": "state",
}


def _now_iso() -> str:
    """Timestamp UTC con ms, formato ISO 8601 (mismo que usa el poller)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Database:
    """Wrapper de SQLite con conexión persistente y helpers tipados.

    Todos los métodos son síncronos: se llaman desde el event loop asyncio,
    pero las operaciones son lo suficientemente cortas como para no hacer
    falta un executor. SQLite está en modo WAL, así que lectores no bloquean
    al escritor.
    """

    def __init__(self, path: Path, max_events: int = 1_000_000) -> None:
        """Guarda la ruta y el cupo FIFO; no abre la conexión hasta que haga falta."""
        self.path = Path(path)
        self.max_events = max_events
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        # Protege el uso de la conexión única contra concurrencia inesperada
        # (ej. un endpoint eventualmente movido a threadpool).
        self._lock = threading.RLock()
        # Cache del COUNT(*) de events: evita full scan en cada /api/status.
        # Se siembra en init_schema y se mantiene en insert_events (FIFO
        # conserva el cupo, así que una vez saturado queda en max_events).
        self._event_count: int | None = None
        log.info("Database path=%s max_events=%d (FIFO)", self.path, self.max_events)

    def _open(self) -> sqlite3.Connection:
        """Abre una conexión con los PRAGMA de performance habituales (WAL, NORMAL)."""
        conn = sqlite3.connect(
            self.path,
            isolation_level=None,
            detect_types=0,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Context manager que devuelve la conexión persistente (lazy open)."""
        with self._lock:
            if self._conn is None:
                self._conn = self._open()
            yield self._conn

    def close(self) -> None:
        """Cierra la conexión (idempotente). Llamar desde el shutdown del main."""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None
                    log.info("Conexión SQLite cerrada")

    def init_schema(self) -> None:
        """Crea tablas/índices/triggers si faltan y siembra el cache de count."""
        with self.connect() as c:
            c.executescript(SCHEMA)
            c.executescript(_fifo_trigger_sql(self.max_events))
            self._event_count = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        log.info(
            "Schema y triggers FIFO inicializados (cap events=%d, sys_events=10000, count actual=%d)",
            self.max_events, self._event_count,
        )

    # ----- variables -----

    def upsert_variables(self, variables: Iterable[Variable]) -> None:
        """Inserta o actualiza el catálogo en la tabla `variables` en una transacción."""
        vars_list = list(variables)
        with self.connect() as c:
            c.execute("BEGIN;")
            try:
                c.executemany(
                    """
                    INSERT INTO variables(address, type, symbol, description)
                    VALUES(?,?,?,?)
                    ON CONFLICT(address) DO UPDATE SET
                        type=excluded.type,
                        symbol=excluded.symbol,
                        description=excluded.description
                    """,
                    [(v.address, v.type, v.symbol, v.description) for v in vars_list],
                )
                c.execute("COMMIT;")
            except Exception:
                c.execute("ROLLBACK;")
                log.exception("upsert_variables falló")
                raise
        log.info("Upsert de %d variables en tabla 'variables'", len(vars_list))

    def list_variables(self) -> list[sqlite3.Row]:
        """Lee el catálogo completo ordenado por dirección (para debug/API)."""
        with self.connect() as c:
            return c.execute(
                "SELECT address, type, symbol, description FROM variables ORDER BY address"
            ).fetchall()

    # ----- eventos I/O -----

    def insert_events(self, events: list[tuple[str, str, str, str, int]]) -> None:
        """Inserta un lote de eventos (ts, address, symbol, description, state).

        Usa una transacción para amortiguar el costo de fsync y mantiene
        sincronizado el cache `_event_count`.
        """
        if not events:
            return
        with self.connect() as c:
            c.execute("BEGIN;")
            try:
                c.executemany(
                    "INSERT INTO events(ts, address, symbol, description, state) "
                    "VALUES(?,?,?,?,?)",
                    events,
                )
                c.execute("COMMIT;")
            except Exception:
                c.execute("ROLLBACK;")
                log.exception("insert_events falló (batch=%d)", len(events))
                raise
        # Mantener el cache en línea con la realidad: el FIFO conserva el cupo
        # una vez saturado, así que simplemente topamos a max_events.
        if self._event_count is not None:
            self._event_count = min(self._event_count + len(events), self.max_events)
        log.debug("insert_events commit: %d filas (count=%s)", len(events), self._event_count)

    def _events_where(
        self,
        address: str | None,
        symbol_like: str | None,
        description_like: str | None,
        state: int | None,
        ts_from: str | None,
        ts_to: str | None,
        search: str | None,
    ) -> tuple[str, list[Any]]:
        """Construye el WHERE y los params según los filtros; todos son opcionales.

        Usa placeholders `?` para no concatenar strings — protección SQL injection.
        """
        parts: list[str] = ["1=1"]
        params: list[Any] = []
        if address:
            parts.append("address = ?")
            params.append(address)
        if symbol_like:
            parts.append("symbol LIKE ?")
            params.append(f"%{symbol_like}%")
        if description_like:
            parts.append("description LIKE ?")
            params.append(f"%{description_like}%")
        if state is not None:
            parts.append("state = ?")
            params.append(int(state))
        if ts_from:
            parts.append("ts >= ?")
            params.append(ts_from)
        if ts_to:
            parts.append("ts <= ?")
            params.append(ts_to)
        if search:
            parts.append("(symbol LIKE ? OR address LIKE ? OR description LIKE ?)")
            like = f"%{search}%"
            params.extend([like, like, like])
        return " AND ".join(parts), params

    def query_events(
        self,
        *,
        address: str | None = None,
        symbol_like: str | None = None,
        description_like: str | None = None,
        state: int | None = None,
        ts_from: str | None = None,
        ts_to: str | None = None,
        search: str | None = None,
        sort_by: str = "id",
        order: str = "desc",
        limit: int = 500,
        offset: int = 0,
    ) -> list[sqlite3.Row]:
        """Query principal de /api/events: filtros + orden + paginado (LIMIT/OFFSET)."""
        col = _EVENTS_SORTABLE.get(sort_by.lower(), "id")
        direction = "ASC" if order.lower() == "asc" else "DESC"
        where, params = self._events_where(
            address, symbol_like, description_like, state, ts_from, ts_to, search
        )
        sql = (
            f"SELECT id, ts, address, symbol, description, state "
            f"FROM events WHERE {where} ORDER BY {col} {direction} LIMIT ? OFFSET ?"
        )
        params.extend([int(limit), int(offset)])
        with self.connect() as c:
            return c.execute(sql, params).fetchall()

    def count_events_filtered(
        self,
        *,
        address: str | None = None,
        symbol_like: str | None = None,
        description_like: str | None = None,
        state: int | None = None,
        ts_from: str | None = None,
        ts_to: str | None = None,
        search: str | None = None,
    ) -> int:
        """COUNT(*) aplicando los mismos filtros que `query_events`.

        Se usa para mostrar el total en el paginador de la UI.
        """
        where, params = self._events_where(
            address, symbol_like, description_like, state, ts_from, ts_to, search
        )
        with self.connect() as c:
            return c.execute(f"SELECT COUNT(*) FROM events WHERE {where}", params).fetchone()[0]

    def count_events(self) -> int:
        """Total de eventos (cached). Evita full scan en /api/status cada 2 s."""
        if self._event_count is None:
            with self.connect() as c:
                self._event_count = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return self._event_count

    def iter_events(
        self,
        *,
        address: str | None = None,
        symbol_like: str | None = None,
        description_like: str | None = None,
        state: int | None = None,
        ts_from: str | None = None,
        ts_to: str | None = None,
        search: str | None = None,
        sort_by: str = "id",
        order: str = "desc",
        limit: int = 1_000_000,
        batch: int = 2000,
    ) -> Iterator[sqlite3.Row]:
        """Itera resultados paginando dentro de SQLite para no levantar todo a RAM.

        Usado por `/api/export.csv` (streaming). Cada yield devuelve una fila.
        """
        col = _EVENTS_SORTABLE.get(sort_by.lower(), "id")
        direction = "ASC" if order.lower() == "asc" else "DESC"
        where, params = self._events_where(
            address, symbol_like, description_like, state, ts_from, ts_to, search
        )
        fetched = 0
        offset = 0
        while fetched < limit:
            page = min(batch, limit - fetched)
            sql = (
                f"SELECT id, ts, address, symbol, description, state "
                f"FROM events WHERE {where} ORDER BY {col} {direction} "
                f"LIMIT ? OFFSET ?"
            )
            with self.connect() as c:
                rows = c.execute(sql, [*params, page, offset]).fetchall()
            if not rows:
                return
            for r in rows:
                yield r
            fetched += len(rows)
            offset += len(rows)
            if len(rows) < page:
                return

    def stats_by_variable(
        self, *, ts_from: str | None = None, ts_to: str | None = None
    ) -> list[sqlite3.Row]:
        """Devuelve por variable: total, total_on, total_off, last_event."""
        sql = [
            "SELECT v.address, v.symbol, v.description,",
            "       COUNT(e.id)                                    AS total,",
            "       COALESCE(SUM(CASE WHEN e.state=1 THEN 1 END),0) AS total_on,",
            "       COALESCE(SUM(CASE WHEN e.state=0 THEN 1 END),0) AS total_off,",
            "       MAX(e.ts)                                      AS last_event",
            "  FROM variables v",
            "  LEFT JOIN events e ON e.address = v.address",
        ]
        params: list[Any] = []
        conds: list[str] = []
        if ts_from:
            conds.append("e.ts >= ?")
            params.append(ts_from)
        if ts_to:
            conds.append("e.ts <= ?")
            params.append(ts_to)
        if conds:
            sql.append(" AND " + " AND ".join(conds))
        sql.append(" GROUP BY v.address, v.symbol, v.description")
        sql.append(" ORDER BY v.address")
        with self.connect() as c:
            return c.execute(" ".join(sql), params).fetchall()

    # ----- eventos de sistema -----

    def insert_sys_event(self, type_: str, description: str = "") -> None:
        """Registra un evento de sistema (STARTUP/CONNECT/DISCONNECT/RECONCILED/etc.)."""
        with self.connect() as c:
            c.execute(
                "INSERT INTO sys_events(ts, type, description) VALUES(?,?,?)",
                (_now_iso(), type_, description),
            )
        log.info("sys_event %s — %s", type_, description)

    def list_sys_events(self, limit: int = 500) -> list[sqlite3.Row]:
        """Últimos N eventos de sistema (más recientes primero)."""
        with self.connect() as c:
            return c.execute(
                "SELECT id, ts, type, description FROM sys_events "
                "ORDER BY id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()

    # ----- users -----

    def get_user(self, username: str) -> sqlite3.Row | None:
        """Devuelve la fila (username, password_hash) o None si no existe."""
        with self.connect() as c:
            return c.execute(
                "SELECT username, password_hash FROM users WHERE username = ?",
                (username,),
            ).fetchone()

    def create_user(self, username: str, password_hash: str) -> None:
        """Crea un usuario con su hash bcrypt (ya calculado por `auth.hash_password`)."""
        with self.connect() as c:
            c.execute(
                "INSERT INTO users(username, password_hash, created_ts) VALUES(?,?,?)",
                (username, password_hash, _now_iso()),
            )

    def users_exist(self) -> bool:
        """True si hay al menos un usuario cargado (gating para el seed inicial)."""
        with self.connect() as c:
            return c.execute("SELECT 1 FROM users LIMIT 1").fetchone() is not None
