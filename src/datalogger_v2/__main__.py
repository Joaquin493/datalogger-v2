"""Permite ejecutar el paquete con `python -m datalogger_v2`.

Es un simple shim: delega en `main.run()`, que es el mismo entrypoint
instalado como comando `datalogger_v2` por `pyproject.toml`.
"""

from .main import run

if __name__ == "__main__":
    run()
