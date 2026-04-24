"""Usuarios + bcrypt + sesión por cookie firmada (Starlette SessionMiddleware).

Modelo:
  - La tabla `users` (en `db`) guarda hash bcrypt, nunca el password en claro.
  - La sesión vive en una cookie firmada con `web.session_secret` y caduca
    tras `web.session_max_age_s` (8 h por defecto).
  - El endpoint web valida acceso con `Depends(require_user)`.

Expone:
  - hash_password / verify_password  — helpers sobre bcrypt.
  - authenticate                     — verifica credenciales contra la DB.
  - seed_users                       — crea los usuarios iniciales si no hay.
  - require_user                     — dependencia FastAPI; 401 si no hay sesión.
"""

from __future__ import annotations

import logging

import bcrypt
from fastapi import HTTPException, Request, status

from .config import InitialUser
from .db import Database

log = logging.getLogger("datalogger_v2.auth")


def hash_password(pw: str) -> str:
    """Genera un hash bcrypt (con salt aleatorio) listo para guardar en DB."""
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("ascii")


def verify_password(pw: str, hashed: str) -> bool:
    """Compara una contraseña en claro contra el hash bcrypt almacenado.

    Devuelve False (en vez de excepción) si el hash está corrupto.
    """
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), hashed.encode("ascii"))
    except ValueError:
        return False


def authenticate(db: Database, username: str, password: str) -> bool:
    """Busca el usuario y valida la contraseña. Loguea éxito/fallo."""
    row = db.get_user(username)
    if not row:
        log.warning("Login fallido — usuario inexistente: %r", username)
        return False
    ok = verify_password(password, row["password_hash"])
    if ok:
        log.info("Login OK user=%s", username)
    else:
        log.warning("Login fallido — contraseña incorrecta user=%s", username)
    return ok


def seed_users(db: Database, users: list[InitialUser]) -> None:
    """Siembra los usuarios de `config.initial_users` si la tabla está vacía.

    No hace nada si ya hay usuarios creados — evita pisar el password que
    el operador haya cambiado manualmente después del primer arranque.
    """
    if db.users_exist():
        log.debug("seed_users: ya existen usuarios, no se crean iniciales")
        return
    if not users:
        log.warning("seed_users: no hay usuarios iniciales configurados; /login no tendrá cuentas")
        return
    for u in users:
        db.create_user(u.username, hash_password(u.password))
        log.info("Usuario inicial creado: %s", u.username)


def require_user(request: Request) -> str:
    """Dependencia FastAPI: lanza 401 si no hay sesión válida.

    Úsala como `@app.get("/ruta", dependencies=[Depends(require_user)])`.
    Devuelve el username (disponible al handler si lo inyecta como parámetro).
    """
    username = request.session.get("user")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Autenticación requerida",
        )
    return username
