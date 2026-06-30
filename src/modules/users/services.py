"""User services: registration, authentication, lookup."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import (
    AuthenticationError,
    ConflictError,
    ResourceNotFoundError,
)
from src.modules.gateway.security import (
    PasswordError,
    hash_password,
    verify_password,
)
from src.modules.users.models import User

_log = structlog.get_logger("nimbus.users")


class UserAlreadyExistsError(ConflictError):
    code = "users.email_taken"
    safe_message = "An account with that email already exists."


class InvalidCredentialsError(AuthenticationError):
    code = "users.invalid_credentials"
    safe_message = "Invalid email or password."


class UserNotFoundError(ResourceNotFoundError):
    code = "users.not_found"
    safe_message = "User not found."


@dataclass(frozen=True, slots=True)
class CreateUserRequest:
    email: str
    password: str


@dataclass(frozen=True, slots=True)
class AuthenticateRequest:
    email: str
    password: str


def _normalize_email(email: str) -> str:
    return email.strip().lower()


async def create_user(
    request: CreateUserRequest,
    *,
    session: AsyncSession,
) -> User:
    email = _normalize_email(request.email)
    encoded = hash_password(request.password)

    existing = await _get_user_by_email(session, email)
    if existing is not None:
        raise UserAlreadyExistsError(
            "Email is already registered.",
            details={"email": email},
        )

    user = User(email=email, password_hash=encoded)
    session.add(user)
    try:
        await session.flush()
    except IntegrityError as exc:
        await session.rollback()
        raise UserAlreadyExistsError(
            "Email is already registered.",
            details={"email": email},
        ) from exc

    return user


async def authenticate_user(
    request: AuthenticateRequest,
    *,
    session: AsyncSession,
) -> User:
    email = _normalize_email(request.email)
    user = await _get_user_by_email(session, email)
    if user is None:
        raise InvalidCredentialsError("Invalid email or password.")

    try:
        ok = verify_password(request.password, user.password_hash)
    except PasswordError:
        _log.error("auth.malformed_hash", user_id=str(user.id))
        raise

    if not ok:
        raise InvalidCredentialsError("Invalid email or password.")

    return user


async def get_user_by_id(
    user_id: uuid.UUID | str,
    *,
    session: AsyncSession,
) -> User:
    if isinstance(user_id, str):
        user_id = uuid.UUID(user_id)
    stmt = select(User).where(User.id == user_id)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        raise UserNotFoundError(
            "User does not exist.",
            details={"user_id": str(user_id)},
        )
    return user


async def _get_user_by_email(
    session: AsyncSession,
    email: str,
) -> User | None:
    stmt = select(User).where(User.email == email)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()
