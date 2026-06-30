"""Users module.

Owns the ``users`` PostgreSQL schema. Hosts the User aggregate
and the registration / authentication services.

Public surface:
    * :class:`User` — the SQLAlchemy model for a user.
    * :class:`UserAlreadyExistsError` — raised on duplicate email.
    * :class:`InvalidCredentialsError` — raised on bad password.
    * :class:`UserNotFoundError` — raised on missing user.
    * :func:`create_user` — register a new user.
    * :func:`authenticate_user` — verify email + password.
    * :func:`get_user_by_id` — fetch a user by UUID.
"""

from __future__ import annotations

from src.modules.users.models import Base, User
from src.modules.users.services import (
    AuthenticateRequest,
    CreateUserRequest,
    InvalidCredentialsError,
    UserAlreadyExistsError,
    UserNotFoundError,
    authenticate_user,
    create_user,
    get_user_by_id,
)

__all__ = [
    "AuthenticateRequest",
    "Base",
    "CreateUserRequest",
    "InvalidCredentialsError",
    "User",
    "UserAlreadyExistsError",
    "UserNotFoundError",
    "authenticate_user",
    "create_user",
    "get_user_by_id",
]
