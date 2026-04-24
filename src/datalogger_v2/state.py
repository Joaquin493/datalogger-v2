"""Estado vivo compartido entre el poller y la API web.

Mantiene en memoria:
  - Un snapshot por variable (estado actual 0/1 y timestamp del último cambio).
  - Un `LinkStatus` con el salud del vínculo Modbus (conectado, ciclos, latencia).

Es la fuente que alimenta la UI en tiempo real (/api/variables, /api/status)
sin golpear SQLite en cada refresh del dashboard.

Todo corre en el mismo event loop (asyncio, single-thread), así que no hace
falta lock: no hay await dentro de las secciones críticas → son atómicas.

Solo memoria — se reconstruye desde cero al iniciar. No persiste.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class VarSnapshot:
    """Estado actual de una variable (lo que muestra el dashboard en vivo)."""
    address: str
    symbol: str
    description: str
    type: str               # INPUT | OUTPUT
    state: int | None = None     # 0 | 1 | None (aún no leído)
    last_change_ts: str | None = None


@dataclass
class LinkStatus:
    """Salud del enlace Modbus reportada por el poller."""
    connected: bool = False
    last_ok_ts: str | None = None
    last_error: str | None = None
    cycles_total: int = 0
    last_cycle_ms: float | None = None


@dataclass
class LiveState:
    """Estado global del runtime: snapshots por variable + estado del enlace."""
    snapshots: dict[str, VarSnapshot] = field(default_factory=dict)
    link: LinkStatus = field(default_factory=LinkStatus)

    def init_snapshots(self, variables) -> None:
        """Inicializa un snapshot vacío por cada variable del catálogo."""
        self.snapshots = {
            v.address: VarSnapshot(
                address=v.address,
                symbol=v.symbol,
                description=v.description,
                type=v.type,
            )
            for v in variables
        }

    def apply_change(self, address: str, new_state: int, ts: str) -> None:
        """Registra una transición: actualiza estado y timestamp del último cambio."""
        s = self.snapshots.get(address)
        if s is not None:
            s.state = new_state
            s.last_change_ts = ts

    def update_state_no_event(self, address: str, new_state: int) -> None:
        """Primera lectura (sin registrar evento porque no hubo transición)."""
        s = self.snapshots.get(address)
        if s is not None:
            s.state = new_state

    def set_link_ok(self, cycle_ms: float) -> None:
        """Marca un ciclo exitoso y guarda la latencia del último read."""
        self.link.connected = True
        self.link.last_ok_ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        self.link.last_error = None
        self.link.cycles_total += 1
        self.link.last_cycle_ms = cycle_ms

    def set_link_error(self, err: str) -> None:
        """Marca el enlace como caído y guarda el último error (string)."""
        self.link.connected = False
        self.link.last_error = err

    def snapshot_list(self) -> list[VarSnapshot]:
        """Devuelve todos los snapshots (para /api/variables)."""
        return list(self.snapshots.values())

    def link_status(self) -> LinkStatus:
        """Devuelve una *copia* del LinkStatus — evita mutaciones desde la API."""
        return LinkStatus(
            connected=self.link.connected,
            last_ok_ts=self.link.last_ok_ts,
            last_error=self.link.last_error,
            cycles_total=self.link.cycles_total,
            last_cycle_ms=self.link.last_cycle_ms,
        )