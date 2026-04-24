"""Parseo de direcciones Schneider (%Ix.y / %Qx.y) y asignación de offsets Modbus.

El Modicon M221 no tiene un mapeo Modbus único para %I y %Q: depende de cómo
se configuró el PLC en EcoStruxure Machine Expert - Basic. Este módulo:

  1) Valida y descompone direcciones tipo `%I0.3` o `%Q2.5` en (kind, módulo, bit).
  2) Asigna a cada dirección un offset Modbus dentro de su tabla (discrete
     inputs para `%I`, coils para `%Q`) según el esquema configurado:

  - "sequential":
        Los %I se asignan a offsets contiguos (0..N-1) en el orden
        (módulo ascendente, bit ascendente). Idem %Q. Suele coincidir con el
        mapa Modbus por defecto cuando el PLC expone %I/%Q de forma directa.

  - "module_stride:<N>":
        offset = módulo * N + bit, con N entero (ej. "module_stride:32").
        Útil cuando cada módulo reserva un bloque fijo de direcciones.

Si ninguno encaja con el mapa real del PLC, agregar un tercer esquema acá
y exponerlo desde `assign_offsets()`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

IoKind = Literal["I", "Q"]
ADDR_RE = re.compile(r"^%([IQ])(\d+)\.(\d+)$")


@dataclass(frozen=True)
class ParsedAddress:
    """Dirección %Ix.y / %Qx.y ya descompuesta en sus componentes."""
    raw: str       # texto original, ej. "%I0.3"
    kind: IoKind   # "I" (entrada discreta) o "Q" (salida / coil)
    module: int    # número de módulo (x en %Ix.y)
    bit: int       # bit dentro del módulo (y en %Ix.y)


def parse_address(address: str) -> ParsedAddress:
    """Convierte un string tipo '%I0.3' en un `ParsedAddress`.

    Lanza ValueError si no encaja con el formato `%[IQ]<módulo>.<bit>`.
    """
    m = ADDR_RE.match(address.strip())
    if not m:
        raise ValueError(f"Dirección inválida (se esperaba %Ix.y o %Qx.y): {address!r}")
    return ParsedAddress(
        raw=address.strip(),
        kind=m.group(1),  # type: ignore[arg-type]
        module=int(m.group(2)),
        bit=int(m.group(3)),
    )


def assign_offsets(parsed: list[ParsedAddress], scheme: str = "sequential") -> dict[str, int]:
    """Devuelve {raw_address: modbus_offset} según el esquema indicado.

    El offset es relativo *por kind* (I o Q): los %I arrancan en 0 y los %Q
    también, porque viajan en tablas Modbus distintas (discrete inputs vs coils).
    Ver el docstring del módulo para el detalle de cada esquema.
    """
    s = scheme.strip().lower()
    if s == "sequential":
        return _sequential(parsed)
    if s.startswith("module_stride:"):
        try:
            n = int(s.split(":", 1)[1])
        except ValueError as e:
            raise ValueError(f"Esquema inválido: {scheme!r}") from e
        return _module_stride(parsed, n)
    raise ValueError(f"Esquema de direccionamiento desconocido: {scheme!r}")


def _sequential(parsed: list[ParsedAddress]) -> dict[str, int]:
    """Esquema 'sequential': enumera por kind en orden (módulo, bit)."""
    out: dict[str, int] = {}
    for kind in ("I", "Q"):
        subset = sorted(
            (p for p in parsed if p.kind == kind),
            key=lambda p: (p.module, p.bit),
        )
        for off, p in enumerate(subset):
            out[p.raw] = off
    return out


def _module_stride(parsed: list[ParsedAddress], n: int) -> dict[str, int]:
    """Esquema 'module_stride:N': offset = módulo*N + bit (bloques fijos)."""
    if n <= 0:
        raise ValueError("module_stride debe ser > 0")
    return {p.raw: p.module * n + p.bit for p in parsed}
