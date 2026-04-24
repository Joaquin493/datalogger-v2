"""Paquete `datalogger_v2` — implementación de **Datalogger V2**.

Registrador de eventos digitales para PLCs Schneider Modicon M221 vía Modbus TCP.
Expone una API web (FastAPI) para consultar, filtrar y exportar eventos desde
la LAN, y guarda los cambios de estado en SQLite con FIFO de 1 M registros.

Submódulos principales:
  - config     : carga y valida config.yaml
  - catalog    : lee Sheet2 del xlsx con las 104 I/O del PLC
  - addressing : parsea %Ix.y / %Qx.y y calcula offsets Modbus
  - db         : SQLite (schema, FIFO, queries)
  - poller     : loop de lectura Modbus + detección de cambios
  - state      : estado vivo compartido entre poller y web
  - auth       : bcrypt + sesión firmada
  - web        : API FastAPI + SPA estática
  - main       : entrypoint que orquesta todo
"""

__version__ = "0.2.0"
