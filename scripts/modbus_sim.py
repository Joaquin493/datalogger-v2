"""Simulador Modbus TCP que imita a un Schneider Modicon M221.

A diferencia del "flip aleatorio" original, este simulador:

  1. Lee el mismo xlsx que usa el datalogger y conoce las 104 señales reales
     (con símbolos y descripciones en castellano).
  2. Clasifica cada variable en una categoría de comportamiento según
     keywords del símbolo/descripción:

       - Presencias / alimentación / habilitaciones  → STEADY_ON (casi siempre ON)
       - Paradas de emergencia / setas               → NC_STEADY_ON (ON = circuito OK)
       - Pulsadores de marcha/paro/reset             → PULSE (pulso corto random)
       - Alarmas / fallas                            → RARE_ON (activaciones raras)
       - Finales de carrera / sensores cíclicos      → TOGGLE_MEDIUM
       - Salidas (%Q) lógicas                        → FOLLOW_INPUT (si hay match)
                                                     o TOGGLE_SLOW (fallback)

  3. Corre un scan cycle de 20 ms (igual que el tiempo de scan típico del
     M221) que avanza el estado interno y refleja los cambios en el datastore
     Modbus.
  4. Modela un "boot" del PLC: arranca todo en OFF; las presencias de tensión
     y enables suben tras ~1 s (como un M221 real al energizarse).
  5. Si hay acoples naturales (pulsador "MARCHA_X" ↔ salida con prefijo
     similar), los liga con un pequeño controlador de 2 estados.

Útil para dev local y demos sin PLC. Uso:

    python scripts/modbus_sim.py
    python scripts/modbus_sim.py --port 502 --xlsx "Programa_TTA_IRSA_convertido v3.xlsx"
    python scripts/modbus_sim.py --scan-ms 20 --seed 42
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
)
from pymodbus.server import StartAsyncTcpServer

# Reusamos el loader del datalogger (lee el xlsx y resuelve offsets).
from datalogger_v2.catalog import Variable, load_catalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s sim: %(message)s",
)
log = logging.getLogger("modbus_sim")


# Tablas Modbus del PLC simulado — 64 bits por tabla cubren holgado las 56 %I + 48 %Q.
N_DI = 64
N_CO = 64


# ======================================================================
# Comportamientos: cada señal tiene su "personalidad"
# ======================================================================


@dataclass
class Behavior:
    """Interfaz común: `initial()` + `tick(state, dt, rng)` → nuevo estado.

    El controlador llama `tick()` cada `dt` segundos y usa el valor devuelto.
    Mantiene todo el estado interno del comportamiento acá (timers, flags).
    """
    def initial(self) -> int:  # pragma: no cover - default trivial
        return 0

    def tick(self, state: int, dt: float, rng: random.Random) -> int:
        return state


@dataclass
class SteadyOn(Behavior):
    """Señal que normalmente está ON (presencia de tensión, listo, habilitado).

    Arranca en OFF y sube tras `boot_delay_s` para simular el energizado.
    Cada tanto puede tener un dropout brevísimo (cable flojo, ruido).
    """
    boot_delay_s: float = 1.0
    mean_dropout_gap_s: float = 3600.0    # un dropout cada ~1 h en promedio
    dropout_length_s: float = 0.3
    _t: float = 0.0
    _dropout_left: float = 0.0
    _next_dropout_in: float = field(default=-1.0)

    def initial(self) -> int:
        return 0  # parte en OFF; el boot lo levanta

    def tick(self, state: int, dt: float, rng: random.Random) -> int:
        self._t += dt
        # Fase de boot: OFF hasta cumplir el delay, ahí pasa a ON.
        if self._t < self.boot_delay_s:
            return 0
        # Dropout en curso: descontamos hasta volver a ON.
        if self._dropout_left > 0:
            self._dropout_left -= dt
            return 0 if self._dropout_left > 0 else 1
        # Siembra del próximo dropout si hace falta.
        if self._next_dropout_in < 0:
            self._next_dropout_in = rng.expovariate(1.0 / self.mean_dropout_gap_s)
        self._next_dropout_in -= dt
        if self._next_dropout_in <= 0:
            self._dropout_left = self.dropout_length_s
            self._next_dropout_in = -1.0
            return 0
        return 1


@dataclass
class NcSteadyOn(Behavior):
    """Contacto Normalmente Cerrado — ON = circuito OK.

    Típico de paradas de emergencia y setas: si el operario la pulsa, el
    contacto abre y la señal cae a 0. Muy raro de ver activado.
    """
    boot_delay_s: float = 1.0
    mean_press_gap_s: float = 1800.0      # presión cada ~30 min en promedio
    press_length_s: float = 2.0           # queda presionada 2 s
    _t: float = 0.0
    _pressed_left: float = 0.0
    _next_press_in: float = field(default=-1.0)

    def initial(self) -> int:
        return 0  # parte en OFF; el boot la energiza (contacto NC pero reportado como 1 cuando OK)

    def tick(self, state: int, dt: float, rng: random.Random) -> int:
        self._t += dt
        if self._t < self.boot_delay_s:
            return 0
        if self._pressed_left > 0:
            self._pressed_left -= dt
            return 0 if self._pressed_left > 0 else 1
        if self._next_press_in < 0:
            self._next_press_in = rng.expovariate(1.0 / self.mean_press_gap_s)
        self._next_press_in -= dt
        if self._next_press_in <= 0:
            self._pressed_left = self.press_length_s
            self._next_press_in = -1.0
            return 0
        return 1


@dataclass
class Pulse(Behavior):
    """Pulsador (marcha/paro/reset): normalmente OFF, pulsos cortos ocasionales."""
    mean_press_gap_s: float = 45.0        # apretado cada ~45 s en promedio
    press_length_s: float = 0.3           # pulso de 300 ms
    _pressed_left: float = 0.0
    _next_press_in: float = field(default=-1.0)

    def initial(self) -> int:
        return 0

    def tick(self, state: int, dt: float, rng: random.Random) -> int:
        if self._pressed_left > 0:
            self._pressed_left -= dt
            return 1 if self._pressed_left > 0 else 0
        if self._next_press_in < 0:
            self._next_press_in = rng.expovariate(1.0 / self.mean_press_gap_s)
        self._next_press_in -= dt
        if self._next_press_in <= 0:
            self._pressed_left = self.press_length_s
            self._next_press_in = -1.0
            return 1
        return 0


@dataclass
class RareOn(Behavior):
    """Alarma/falla: activaciones muy raras; cuando pasa, queda ON varios segundos."""
    mean_activation_gap_s: float = 900.0  # ~15 min
    activation_length_s: float = 10.0
    _active_left: float = 0.0
    _next_activation_in: float = field(default=-1.0)

    def initial(self) -> int:
        return 0

    def tick(self, state: int, dt: float, rng: random.Random) -> int:
        if self._active_left > 0:
            self._active_left -= dt
            return 1 if self._active_left > 0 else 0
        if self._next_activation_in < 0:
            self._next_activation_in = rng.expovariate(1.0 / self.mean_activation_gap_s)
        self._next_activation_in -= dt
        if self._next_activation_in <= 0:
            self._active_left = self.activation_length_s
            self._next_activation_in = -1.0
            return 1
        return 0


@dataclass
class Toggle(Behavior):
    """Conmutación cíclica: estado actual cambia cada `mean_period_s` en promedio.

    Con un poco de jitter para que no quede robotizado. Usado para finales de
    carrera, sensores de posición, etc.
    """
    mean_period_s: float = 15.0
    _next_toggle_in: float = field(default=-1.0)

    def initial(self) -> int:
        return 0

    def tick(self, state: int, dt: float, rng: random.Random) -> int:
        if self._next_toggle_in < 0:
            self._next_toggle_in = rng.uniform(0.5, 1.5) * self.mean_period_s
        self._next_toggle_in -= dt
        if self._next_toggle_in <= 0:
            self._next_toggle_in = rng.uniform(0.5, 1.5) * self.mean_period_s
            return 0 if state else 1
        return state


@dataclass
class Driven(Behavior):
    """Salida controlada por otro input — no decide sola.

    Le pasás una función `follow(now_state_of_inputs) -> int`. El controlador
    la llama en cada tick con el estado global.
    """
    follow: Callable[[dict[str, int]], int] = field(default=lambda _: 0)

    def initial(self) -> int:
        return 0

    def tick(self, state: int, dt: float, rng: random.Random) -> int:
        # El controlador global sobrescribe este estado usando follow().
        return state


# ======================================================================
# Clasificación por keywords
# ======================================================================


def _contains_any(haystack: str, needles: tuple[str, ...]) -> bool:
    return any(n in haystack for n in needles)


def classify(v: Variable, rng: random.Random) -> Behavior:
    """Devuelve la `Behavior` apropiada para la variable según su símbolo/descripción."""
    text = f"{v.symbol} {v.description}".upper()

    # Emergencias / setas / paros de seguridad: contacto NC, normalmente 1.
    if _contains_any(text, ("EMERG", "SETA", "PARO SEG", "SAFETY")):
        return NcSteadyOn(
            mean_press_gap_s=rng.uniform(1200.0, 3600.0),
            press_length_s=rng.uniform(1.0, 3.0),
        )

    # Pulsadores: MARCHA, START, BOT, PULS, PARAR, STOP, RESET.
    if _contains_any(text, (
        "MARCHA", "START", "ARRANQ", "BOT ", "BOT_", "BOTON",
        "PULS", "RESET", "PARAR", " STOP",
    )):
        return Pulse(
            mean_press_gap_s=rng.uniform(20.0, 90.0),
            press_length_s=rng.uniform(0.2, 0.6),
        )

    # Alarmas / fallas.
    if _contains_any(text, ("ALARM", "FALLA", "FALLO", "ERROR", "FAULT", "SOBRECARG")):
        return RareOn(
            mean_activation_gap_s=rng.uniform(600.0, 2400.0),
            activation_length_s=rng.uniform(5.0, 20.0),
        )

    # Presencias / tensiones / habilitaciones / "listo": steady ON.
    if _contains_any(text, (
        "PRESENCIA", "TENSION", "HABIL", "LISTO", "OK",
        "ALIMENT", "ENERG", "DISPONIB",
    )):
        return SteadyOn(
            boot_delay_s=rng.uniform(0.5, 1.5),
            mean_dropout_gap_s=rng.uniform(1800.0, 7200.0),
        )

    # Finales de carrera / sensores cíclicos / posiciones.
    if _contains_any(text, ("FIN_", "FDC", "SENS", "POSIC", "DETECT")):
        return Toggle(mean_period_s=rng.uniform(8.0, 25.0))

    # Default: salidas → toggle lento; entradas → toggle medio.
    if v.kind == "Q":
        return Toggle(mean_period_s=rng.uniform(30.0, 120.0))
    return Toggle(mean_period_s=rng.uniform(10.0, 30.0))


# ======================================================================
# Acople input → output (controlador de marcha/paro)
# ======================================================================


def _strip_prefix(symbol: str) -> str:
    """Quita prefijos de rol comunes para poder comparar 'raíces' de símbolos.

    Ej: 'BOT_MARCHA_BOMBA1' y 'BOMBA1' comparten la raíz 'BOMBA1'.
    """
    upper = symbol.upper()
    for prefix in (
        "BOT_MARCHA_", "BOT_START_", "BOT_ARRANQ_", "BOT_PARAR_", "BOT_STOP_",
        "MARCHA_", "START_", "ARRANQ_", "PARAR_", "STOP_", "BOT_", "PUL_", "PULS_",
    ):
        if upper.startswith(prefix):
            return upper[len(prefix):]
    return upper


def build_couplings(variables: list[Variable]) -> dict[str, Callable[[dict[str, int]], int]]:
    """Arma controladores marcha/paro cuando hay pulsadores que nombran una salida.

    Devuelve `{addr_output: follow_fn}`. La `follow_fn` recibe un dict
    `{addr: state}` con todas las entradas pulsadoras y devuelve 0/1.
    """
    # Indexamos salidas por raíz del símbolo.
    outputs_by_root: dict[str, list[Variable]] = {}
    for v in variables:
        if v.kind != "Q":
            continue
        root = _strip_prefix(v.symbol)
        outputs_by_root.setdefault(root, []).append(v)

    # Indexamos pulsadores (marcha/paro) que apunten a una salida por raíz.
    class _Latch:
        """Mini máquina de estados: marcha ↔ paro, con edge-detection."""
        def __init__(self) -> None:
            self.running = 0
            self.prev_start = 0
            self.prev_stop = 0

        def __call__(
            self,
            states: dict[str, int],
            start_addrs: list[str],
            stop_addrs: list[str],
            emergency_ok: Callable[[], bool],
        ) -> int:
            start_now = any(states.get(a, 0) for a in start_addrs)
            stop_now = any(states.get(a, 0) for a in stop_addrs)
            # Flanco de subida en START → running=1. En STOP → running=0.
            if start_now and not self.prev_start:
                self.running = 1
            if stop_now and not self.prev_stop:
                self.running = 0
            if not emergency_ok():
                self.running = 0
            self.prev_start = start_now
            self.prev_stop = stop_now
            return self.running

    latches: dict[str, _Latch] = {}
    start_map: dict[str, list[str]] = {}   # addr_output → list de addrs de inputs MARCHA
    stop_map: dict[str, list[str]] = {}    # idem STOP
    emerg_addrs: list[str] = [v.address for v in variables
                              if v.kind == "I"
                              and _contains_any(f"{v.symbol} {v.description}".upper(),
                                                ("EMERG", "SETA", "PARO SEG"))]

    for v in variables:
        if v.kind != "I":
            continue
        text = f"{v.symbol} {v.description}".upper()
        is_start = _contains_any(text, ("MARCHA", "START", "ARRANQ"))
        is_stop = _contains_any(text, ("PARAR", " STOP", "DETENER"))
        if not (is_start or is_stop):
            continue
        root = _strip_prefix(v.symbol)
        # Buscamos una salida cuya raíz coincida con la del pulsador.
        for out_root, outs in outputs_by_root.items():
            if out_root in root or root in out_root:
                for out_v in outs:
                    if is_start:
                        start_map.setdefault(out_v.address, []).append(v.address)
                    else:
                        stop_map.setdefault(out_v.address, []).append(v.address)
                break

    # Construye una follow_fn por salida ligada.
    follows: dict[str, Callable[[dict[str, int]], int]] = {}
    linked_outputs = set(start_map.keys()) | set(stop_map.keys())
    for addr in linked_outputs:
        latches[addr] = _Latch()
        starts = start_map.get(addr, [])
        stops = stop_map.get(addr, [])
        latch = latches[addr]

        def make_follow(l: _Latch, s: list[str], p: list[str]):
            def follow(states: dict[str, int]) -> int:
                emergency_ok = lambda: all(states.get(a, 1) for a in emerg_addrs)
                return l(states, s, p, emergency_ok)
            return follow

        follows[addr] = make_follow(latch, starts, stops)
    return follows


# ======================================================================
# Simulador principal
# ======================================================================


class PlcSim:
    """Mantiene estado, corre el scan cycle y lo refleja en el datastore Modbus."""

    def __init__(self, variables: list[Variable], seed: int | None = None) -> None:
        self.rng = random.Random(seed)
        self.variables = variables
        self.behaviors: dict[str, Behavior] = {v.address: classify(v, self.rng) for v in variables}
        self.state: dict[str, int] = {v.address: self.behaviors[v.address].initial() for v in variables}
        # Acoples input → output (marcha/paro). Si hay, reemplaza el comportamiento.
        self.follows = build_couplings(variables)
        for addr in self.follows:
            self.behaviors[addr] = Driven(follow=self.follows[addr])

        self._log_behavior_summary()

    def _log_behavior_summary(self) -> None:
        """Loguea cuántas variables cayeron en cada categoría (útil para depurar keywords)."""
        counts: dict[str, int] = {}
        for beh in self.behaviors.values():
            name = type(beh).__name__
            counts[name] = counts.get(name, 0) + 1
        log.info("Comportamientos asignados: %s",
                 ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        log.info("Salidas ligadas a pulsadores (marcha/paro): %d", len(self.follows))

    def tick(self, dt: float) -> None:
        """Avanza todos los comportamientos un paso dt y resuelve los acoples."""
        for v in self.variables:
            beh = self.behaviors[v.address]
            if isinstance(beh, Driven):
                continue  # las Driven se resuelven después, con el estado completo
            self.state[v.address] = beh.tick(self.state[v.address], dt, self.rng)
        # Segunda pasada: salidas ligadas a la lógica de entradas.
        for addr, follow in self.follows.items():
            self.state[addr] = follow(self.state)

    def write_to_context(self, device: ModbusDeviceContext) -> None:
        """Vuelca el estado interno a los bloques Modbus DI/CO."""
        for v in self.variables:
            fc = 2 if v.kind == "I" else 1
            device.setValues(fc, v.offset, [self.state[v.address]])


async def scan_loop(sim: PlcSim, context: ModbusServerContext, scan_ms: int) -> None:
    """Scan cycle tipo PLC: tick + write al datastore cada `scan_ms` ms."""
    device = context[0]
    dt = scan_ms / 1000.0
    last = time.monotonic()
    cycles = 0
    while True:
        target = last + dt
        sim.tick(dt)
        sim.write_to_context(device)
        cycles += 1
        if cycles % 500 == 0:
            ons = sum(1 for s in sim.state.values() if s == 1)
            log.info("Scan #%d — %d/%d señales en ON", cycles, ons, len(sim.state))
        now = time.monotonic()
        sleep_s = target - now
        if sleep_s > 0:
            await asyncio.sleep(sleep_s)
        else:
            # Scan overrun: el PLC real se queja; acá sólo lo logueamos en DEBUG.
            log.debug("Scan overrun %.1fms", -sleep_s * 1000)
        last = time.monotonic()


def build_context() -> ModbusServerContext:
    """Crea el datastore Modbus (64 DI + 64 CO en cero) con un único esclavo."""
    di = ModbusSequentialDataBlock(0, [0] * N_DI)
    co = ModbusSequentialDataBlock(0, [0] * N_CO)
    device = ModbusDeviceContext(di=di, co=co)
    return ModbusServerContext(devices=device, single=True)


def _default_xlsx() -> Path:
    """Ubicación por defecto del xlsx: mismo directorio raíz del proyecto."""
    return Path(__file__).resolve().parent.parent / "Programa_TTA_IRSA_convertido v3.xlsx"


async def main() -> None:
    """CLI + wire-up: carga catálogo, arma sim, corre scan loop + servidor Modbus."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5020,
                        help="Puerto TCP (5020 para dev; 502 es el del M221 real)")
    parser.add_argument("--xlsx", type=Path, default=_default_xlsx(),
                        help="Ruta al xlsx del catálogo (Sheet2).")
    parser.add_argument("--sheet", default="Sheet2")
    parser.add_argument("--scan-ms", type=int, default=20,
                        help="Tiempo de scan (ms). Default 20 ≈ M221.")
    parser.add_argument("--seed", type=int, default=None,
                        help="Seed RNG para reproducibilidad.")
    args = parser.parse_args()

    variables = load_catalog(args.xlsx, args.sheet)
    n_i = sum(1 for v in variables if v.kind == "I")
    n_q = sum(1 for v in variables if v.kind == "Q")

    context = build_context()
    sim = PlcSim(variables, seed=args.seed)
    sim.write_to_context(context[0])  # estado inicial visible desde el primer request

    log.info(
        "Sim M221 escuchando en %s:%d — %d variables (%d %%I + %d %%Q), scan=%dms, seed=%s",
        args.host, args.port, len(variables), n_i, n_q, args.scan_ms, args.seed,
    )
    log.info("Ctrl+C para detener.")

    asyncio.create_task(scan_loop(sim, context, args.scan_ms))
    await StartAsyncTcpServer(context=context, address=(args.host, args.port))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
