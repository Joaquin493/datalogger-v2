"""Poller Modbus TCP: lee %I y %Q en cada ciclo y registra cambios de estado.

Estrategia:
  - N lecturas por ciclo: una por cada "bloque contiguo" del catálogo, dentro
    de cada kind (FC=2 para %I, FC=1 para %Q). Los bloques se detectan desde
    los offsets del catálogo: cuando hay un hueco > `_BLOCK_GAP_THRESHOLD`
    bits, se parte en un bloque nuevo. Así el poller no pide al PLC coils /
    inputs que no existen en el mapa (algunos PLCs responden con error).
  - Diff contra el estado previo en memoria; cada transición es un evento.
  - Reconexión con backoff exponencial tomado de config.modbus.reconnect_backoff_s.
  - Cada ciclo tiene un piso de min_cycle_ms ms para no saturar al PLC.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

from .catalog import Variable
from .config import ModbusConfig
from .db import Database
from .state import LiveState

log = logging.getLogger("datalogger_v2.poller")


def _now_iso() -> str:
    """Timestamp UTC con ms, formato ISO 8601 (el mismo que usa `db`)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# Umbral de gap para partir en bloques. Un hueco más chico que esto se lee
# "de corrido" porque un request más largo suele costar menos que dos requests
# cortos separados (ida-vuelta TCP + overhead de header ≈ 12 bytes/request).
_BLOCK_GAP_THRESHOLD = 8


def _blocks(offsets: list[int]) -> list[tuple[int, int]]:
    """Agrupa offsets en bloques contiguos (con tolerancia de gap pequeño).

    Devuelve una lista de `(start, count)` que juntas cubren todos los offsets.
    Cuando el gap entre dos offsets consecutivos supera `_BLOCK_GAP_THRESHOLD`,
    arranca un bloque nuevo en vez de extender el anterior. Evita pedir al PLC
    direcciones que no existen (fuente típica de ModbusException en el M221).

    Ejemplos:
        [0..55]              → [(0, 56)]
        [0..15, 48..79]      → [(0, 16), (48, 32)]
        [0..2, 5..7]  (gap=2) → [(0, 8)]     # gap chico, mejor un solo read
    """
    if not offsets:
        return []
    sorted_offs = sorted(set(offsets))
    blocks: list[tuple[int, int]] = []
    start = sorted_offs[0]
    prev = start
    for off in sorted_offs[1:]:
        if off - prev > _BLOCK_GAP_THRESHOLD:
            blocks.append((start, prev - start + 1))
            start = off
        prev = off
    blocks.append((start, prev - start + 1))
    return blocks


class ModbusPoller:
    """Loop que lee el PLC vía Modbus TCP y convierte cambios en eventos.

    Ciclo de vida:
      1. `run()` corre un loop de conectar → pollear → manejar errores → reconectar.
      2. `_loop(client)` hace lecturas mientras el socket esté sano.
      3. Cada lectura pasa por `_diff_and_build_events`: sólo las transiciones
         (0→1 o 1→0) producen filas en la tabla `events`.
      4. `stop()` señaliza al loop para que corte con limpieza.
    """

    def __init__(
        self,
        cfg: ModbusConfig,
        variables: list[Variable],
        db: Database,
        live: LiveState,
    ) -> None:
        """Prepara los maps offset→variable y los rangos de lectura por kind."""
        self.cfg = cfg
        self.variables = variables
        self.db = db
        self.live = live
        self._var_by_addr = {v.address: v for v in variables}

        # Mapa offset -> Variable, por kind.
        self._by_offset: dict[str, dict[int, Variable]] = {"I": {}, "Q": {}}
        for v in variables:
            self._by_offset[v.kind][v.offset] = v

        # Bloques de lectura por kind: lista de (start, count). Detectados
        # desde el catálogo (un bloque por zona contigua; huecos grandes parten
        # en bloques separados para no pedirle al PLC direcciones inexistentes).
        self._ranges: dict[str, list[tuple[int, int]]] = {}
        # Y precomputamos, por bloque, qué variables caen adentro —
        # evita filtrar en cada tick.
        self._vars_per_block: dict[str, list[list[tuple[int, Variable]]]] = {"I": [], "Q": []}
        for kind in ("I", "Q"):
            blocks = _blocks([v.offset for v in variables if v.kind == kind])
            if not blocks:
                continue
            self._ranges[kind] = blocks
            for start, count in blocks:
                in_block = [
                    (off, var) for off, var in self._by_offset[kind].items()
                    if start <= off < start + count
                ]
                self._vars_per_block[kind].append(in_block)
                log.info("Bloque de lectura %%%s: start=%d count=%d (FC=%d, %d vars)",
                         kind, start, count, 2 if kind == "I" else 1, len(in_block))

        self._prev_state: dict[str, int] = {}
        self._stopping = asyncio.Event()
        self._cycle_count = 0
        # Flag: el próximo ciclo es el primero post-reconexión. Si emite eventos,
        # registramos un sys_event RECONCILED para marcar cambios que ocurrieron
        # durante el corte (timestamp real es "ahora", no cuándo pasaron).
        self._post_reconnect = False

    def stop(self) -> None:
        """Pide al loop que termine en el próximo punto de control (cancelación limpia)."""
        log.info("Señal stop() recibida por el poller")
        self._stopping.set()

    async def run(self) -> None:
        """Loop principal: conecta, poll-ea, y reintenta con backoff si cae.

        Registra sys_events CONNECT/DISCONNECT en cada transición del enlace
        y se detiene cuando `stop()` fue llamado.
        """
        backoff = list(self.cfg.reconnect_backoff_s) or [1.0]
        bi = 0
        log.info("Poller iniciando — Modbus %s:%d unit=%d timeout=%.1fs min_cycle=%dms",
                 self.cfg.host, self.cfg.port, self.cfg.unit_id,
                 self.cfg.timeout_s, self.cfg.min_cycle_ms)
        while not self._stopping.is_set():
            client = AsyncModbusTcpClient(
                host=self.cfg.host,
                port=self.cfg.port,
                timeout=self.cfg.timeout_s,
            )
            try:
                log.debug("Intentando conectar a Modbus %s:%d...", self.cfg.host, self.cfg.port)
                ok = await client.connect()
                if not ok:
                    raise ConnectionError(f"No pude conectar a {self.cfg.host}:{self.cfg.port}")
                log.info("Conectado a Modbus %s:%s", self.cfg.host, self.cfg.port)
                self.db.insert_sys_event(
                    "CONNECT", f"Modbus TCP {self.cfg.host}:{self.cfg.port}"
                )
                # Si ya teníamos estado previo, el próximo ciclo podría disparar
                # eventos "recuperados" del corte.
                self._post_reconnect = bool(self._prev_state)
                bi = 0
                await self._loop(client)
            except asyncio.CancelledError:
                raise
            except (ModbusException, ConnectionError, OSError) as e:
                delay = backoff[min(bi, len(backoff) - 1)]
                bi += 1
                self.live.set_link_error(f"{type(e).__name__}: {e}")
                self.db.insert_sys_event(
                    "DISCONNECT", f"{type(e).__name__}: {e}. Reintento en {delay:.1f}s"
                )
                log.warning("Modbus caído (%s). Reintento en %.1fs", e, delay)
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                    return
                except asyncio.TimeoutError:
                    pass
            finally:
                try:
                    client.close()
                except Exception:
                    pass

    async def _loop(self, client: AsyncModbusTcpClient) -> None:
        """Ciclo interno: read → diff → persist → sleep (con piso min_cycle_ms).

        Cualquier excepción sale de acá y la maneja `run()` (reconecta).
        """
        floor = self.cfg.min_cycle_ms / 1000.0
        while not self._stopping.is_set():
            t0 = time.monotonic()
            bits = await self._read_all(client)

            ts = _now_iso()
            events = self._diff_and_build_events(bits, ts)
            if events:
                self.db.insert_events(events)
                for (_ts, addr, sym, desc, state) in events:
                    log.info("EVENT %s %s %s → %s%s",
                             addr, sym, "ON " if state == 1 else "OFF",
                             state, f" ({desc})" if desc else "")
                if self._post_reconnect:
                    self.db.insert_sys_event(
                        "RECONCILED",
                        f"{len(events)} evento(s) detectado(s) tras reconexión "
                        "(timestamp = ahora, no cuándo ocurrieron realmente)",
                    )
            # El flag solo aplica al primer ciclo post-reconexión.
            self._post_reconnect = False

            cycle_ms = (time.monotonic() - t0) * 1000.0
            self.live.set_link_ok(cycle_ms)
            self._cycle_count += 1
            if self._cycle_count % 1000 == 0:
                log.info("Ciclos=%d último=%.1fms eventos_totales=%d",
                         self._cycle_count, cycle_ms, self.db.count_events())
            else:
                log.debug("ciclo #%d %.1fms (%d eventos)", self._cycle_count, cycle_ms, len(events))

            elapsed = time.monotonic() - t0
            sleep_s = max(0.0, floor - elapsed)
            if sleep_s > 0:
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=sleep_s)
                    return
                except asyncio.TimeoutError:
                    pass

    async def _read_with_retry(self, fn, **kwargs):
        """Un reintento corto dentro de la misma conexión — evita reconectar
        por glitches puntuales (frecuente en redes industriales con ruido).
        """
        try:
            rr = await fn(**kwargs)
            if rr.isError():
                raise ModbusException(f"{fn.__name__} error: {rr}")
            return rr
        except (ModbusException, asyncio.TimeoutError, OSError) as e:
            log.debug("Lectura falló (%s: %s), reintentando en 50ms", type(e).__name__, e)
            await asyncio.sleep(0.05)
            rr = await fn(**kwargs)
            if rr.isError():
                raise ModbusException(f"{fn.__name__} error tras retry: {rr}")
            return rr

    async def _read_all(self, client: AsyncModbusTcpClient) -> dict[str, int]:
        """Hace N lecturas por ciclo — una por cada bloque del catálogo.

        Devuelve `{address: 0|1}` con el estado actual de cada variable.
        """
        out: dict[str, int] = {}
        unit = self.cfg.unit_id

        readers = (
            ("I", client.read_discrete_inputs),
            ("Q", client.read_coils),
        )
        for kind, fn in readers:
            for i, (start, count) in enumerate(self._ranges.get(kind, [])):
                rr = await self._read_with_retry(
                    fn, address=start, count=count, device_id=unit
                )
                for off, var in self._vars_per_block[kind][i]:
                    out[var.address] = int(bool(rr.bits[off - start]))
        return out

    def _diff_and_build_events(
        self, bits: dict[str, int], ts: str
    ) -> list[tuple[str, str, str, str, int]]:
        """Compara la lectura nueva contra `_prev_state` y arma la lista de eventos.

        - La primer lectura de cada variable inicializa el estado sin emitir evento
          (evita spam de eventos al arrancar).
        - Sólo las transiciones (prev != new) generan tuplas listas para insertar.
        - Actualiza `LiveState` con el cambio para que la UI lo refleje en vivo.
        """
        events: list[tuple[str, str, str, str, int]] = []
        for addr, new_state in bits.items():
            prev = self._prev_state.get(addr)
            if prev is None:
                self._prev_state[addr] = new_state
                self.live.update_state_no_event(addr, new_state)
                continue
            if prev != new_state:
                self._prev_state[addr] = new_state
                v = self._var_by_addr[addr]
                events.append((ts, addr, v.symbol, v.description, new_state))
                self.live.apply_change(addr, new_state, ts)
        return events
