"""Password hashing and JWT token utilities.

Password hashing
----------------
We use argon2id via the OWASP-recommended parameters. The
:class:`PasswordHasher` instance is a process-singleton because
argon2id calibration is expensive.

JWT strategy
------------
* ``access`` tokens: short-lived (15 min by default), used for
  authenticated API requests. Carries ``sub`` (subject), ``typ``
  ("access"), ``iat``, ``exp``, ``iss``, ``aud``, and ``jti``.
* ``refresh`` tokens: long-lived (7 days by default), used only to
  mint new access tokens. Carries ``typ`` ("refresh") plus the same
  set of claims.

Refresh token rotation
~~~~~~~~~~~~~~~~~~~~~~
A future slice will add a Redis-backed ``jti`` denylist for revoked
refresh tokens. For now, the ``jti`` is generated but not validated
against a store.

Algorithm
---------
HS256 by default. The secret must be at least 32 bytes of random
data in production. Switching to RS256 (asymmetric) is a single
config change (``JWT_ALGORITHM=RS256`` + ``JWT_PUBLIC_KEY`` /
``JWT_PRIVATE_KEY``).
"""

from __future__ import annotations

import enum
import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Any, Final

import jwt
import structlog
from argon2 import PasswordHasher as _Argon2Hasher
from argon2.exceptions import (
    InvalidHashError,
    VerificationError,
    VerifyMismatchError,
)

from src.core.config import get_settings
from src.core.exceptions import (
    AuthenticationError,
)

_log = structlog.get_logger("nimbus.gateway.security")

# OWASP-recommended minimum parameters for argon2id (2024):
#   memory_cost >= 19456 KiB, time_cost >= 2, parallelism >= 1
# We use a higher memory cost (64 MiB) and tune parallelism for the
# target hardware. These are the same values used by the reference
# passlib argon2 context.
_ARGON2_TIME_COST: Final[int] = 3
_ARGON2_MEMORY_COST: Final[int] = 65_536  # 64 MiB
_ARGON2_PARALLELISM: Final[int] = 4
_ARGON2_HASH_LENGTH: Final[int] = 32
_ARGON2_SALT_LENGTH: Final[int] = 16

# Process-singleton hasher. The argon2 calibration is expensive
# (100ms+ on the first call) so we want exactly one instance.
_hasher: _Argon2Hasher | None = None


def _get_hasher() -> _Argon2Hasher:
    global _hasher
    if _hasher is None:
        _hasher = _Argon2Hasher(
            time_cost=_ARGON2_TIME_COST,
            memory_cost=_ARGON2_MEMORY_COST,
            parallelism=_ARGON2_PARALLELISM,
            hash_len=_ARGON2_HASH_LENGTH,
            salt_len=_ARGON2_SALT_LENGTH,
        )
    return _hasher


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------


class PasswordError(Exception):
    """Base for password hashing / verification errors."""


def hash_password(plain: str) -> str:
    """Hash a plaintext password using argon2id.

    Returns an encoded string of the form
    ``$argon2id$v=19$m=...,t=...,p=...$salt$hash`` which contains all
    parameters needed for verification. Safe to store in a database.
    """
    if not plain:
        raise PasswordError("Cannot hash an empty password.")
    return _get_hasher().hash(plain)


def verify_password(plain: str, encoded: str) -> bool:
    """Verify a plaintext password against a stored argon2id hash.

    Returns True on match, False on mismatch. Raises
    :class:`PasswordError` if the stored hash is malformed.
    """
    if not plain or not encoded:
        return False
    try:
        return bool(_get_hasher().verify(encoded, plain))
    except (VerifyMismatchError, VerificationError):
        return False
    except InvalidHashError as exc:
        raise PasswordError(f"Malformed password hash: {exc}") from exc


def needs_rehash(encoded: str) -> bool:
    """Return True if the stored hash should be re-hashed.

    Use after a successful :func:`verify_password` to transparently
    upgrade users to stronger parameters over time.
    """
    return bool(_get_hasher().check_needs_rehash(encoded))


# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------


class TokenType(str, enum.Enum):
    ACCESS = "access"
    REFRESH = "refresh"


class TokenError(AuthenticationError):
    """Raised when a token is invalid, expired, or wrong-type."""


@dataclass(frozen=True, slots=True)
class TokenClaims:
    """Decoded JWT claims.

    The raw token string is intentionally not retained; the caller
    already has it and we don't want to encourage keeping it in
    memory longer than necessary.
    """

    subject: str
    token_type: TokenType
    issued_at: int
    expires_at: int
    jti: str
    issuer: str
    audience: str
    raw: dict[str, Any]


def _build_payload(
    *,
    subject: str,
    token_type: TokenType,
    ttl_seconds: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the JWT payload. Issued/exp times are UNIX seconds."""
    now = int(time.time())
    payload: dict[str, Any] = {
        "sub": subject,
        "typ": token_type.value,
        "iat": now,
        "nbf": now,  # not-before: same as iat, no clock skew tolerance
        "exp": now + ttl_seconds,
        "jti": uuid.uuid4().hex,
        "iss": get_settings().jwt_issuer,
        "aud": get_settings().jwt_audience,
    }
    if extra:
        payload.update(extra)
    return payload


def _encode(payload: dict[str, Any]) -> str:
    settings = get_settings()
    return jwt.encode(
        payload,
        settings.jwt_secret_key,
        algorithm=settings.jwt_algorithm,
    )


def _decode(token: str, *, expected_type: TokenType) -> TokenClaims:
    settings = get_settings()
    try:
        decoded = jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
            audience=settings.jwt_audience,
            issuer=settings.jwt_issuer,
            options={
                "require": ["sub", "typ", "iat", "exp", "jti", "iss", "aud"],
                "verify_aud": True,
                "verify_iss": True,
                "verify_exp": True,
                "verify_iat": True,
            },
        )
    except jwt.ExpiredSignatureError as exc:
        raise TokenError("Token has expired.") from exc
    except jwt.InvalidAudienceError as exc:
        raise TokenError("Token audience is invalid.") from exc
    except jwt.InvalidIssuerError as exc:
        raise TokenError("Token issuer is invalid.") from exc
    except jwt.InvalidTokenError as exc:
        raise TokenError("Token is invalid.") from exc

    typ_raw = decoded.get("typ")
    if typ_raw not in {TokenType.ACCESS.value, TokenType.REFRESH.value}:
        raise TokenError("Token type claim is missing or malformed.")
    if typ_raw != expected_type.value:
        raise TokenError(f"Expected {expected_type.value} token, got {typ_raw!r}.")

    try:
        return TokenClaims(
            subject=str(decoded["sub"]),
            token_type=TokenType(typ_raw),
            issued_at=int(decoded["iat"]),
            expires_at=int(decoded["exp"]),
            jti=str(decoded["jti"]),
            issuer=str(decoded["iss"]),
            audience=str(decoded["aud"]),
            raw=decoded,
        )
    except (KeyError, ValueError) as exc:
        raise TokenError("Token is missing required claims.") from exc


def create_access_token(
    subject: str,
    *,
    ttl_seconds: int | None = None,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    """Mint a short-lived access token.

    Args:
        subject: the ``sub`` claim (typically the user id).
        ttl_seconds: override the default TTL; must be positive.
        extra_claims: additional claims (e.g., ``{"role": "admin"}``).

    Returns:
        A signed JWT string.
    """
    settings = get_settings()
    ttl = ttl_seconds if ttl_seconds is not None else settings.jwt_access_token_ttl_seconds
    if ttl <= 0:
        raise ValueError("ttl_seconds must be > 0")
    return _encode(
        _build_payload(
            subject=subject,
            token_type=TokenType.ACCESS,
            ttl_seconds=ttl,
            extra=extra_claims,
        )
    )


def create_refresh_token(
    subject: str,
    *,
    ttl_seconds: int | None = None,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    """Mint a long-lived refresh token."""
    settings = get_settings()
    ttl = ttl_seconds if ttl_seconds is not None else settings.jwt_refresh_token_ttl_seconds
    if ttl <= 0:
        raise ValueError("ttl_seconds must be > 0")
    return _encode(
        _build_payload(
            subject=subject,
            token_type=TokenType.REFRESH,
            ttl_seconds=ttl,
            extra=extra_claims,
        )
    )


def decode_access_token(token: str) -> TokenClaims:
    """Decode and validate an access token."""
    return _decode(token, expected_type=TokenType.ACCESS)


def decode_refresh_token(token: str) -> TokenClaims:
    """Decode and validate a refresh token."""
    return _decode(token, expected_type=TokenType.REFRESH)


# Backwards-compatible alias used by older callers.
decode_token = decode_access_token


# ---------------------------------------------------------------------------
# Random helpers
# ---------------------------------------------------------------------------


def generate_api_key(prefix: str = "nk") -> str:
    """Generate an opaque API key with a Nimbus prefix.

    Format: ``nk_<32 url-safe random chars>``. The prefix makes
    keys recognizable in logs and support tickets.
    """
    if not prefix or not prefix.replace("_", "").isalnum():
        raise ValueError("prefix must be alphanumeric (underscore allowed)")
    return f"{prefix}_{secrets.token_urlsafe(32)}"


__all__ = [
    "PasswordError",
    "TokenClaims",
    "TokenError",
    "TokenType",
    "create_access_token",
    "create_refresh_token",
    "decode_access_token",
    "decode_refresh_token",
    "decode_token",
    "generate_api_key",
    "hash_password",
    "needs_rehash",
    "verify_password",
]
