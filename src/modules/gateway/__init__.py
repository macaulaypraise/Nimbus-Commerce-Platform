"""API Gateway module.

The gateway owns:
  * authentication (password hashing, JWT issuance, JWT validation)
  * abuse prevention (per-fingerprint rate limiting, graduated response)
  * request routing to internal modules

It is the only module that talks to clients directly. All other
modules expose their services through gateway-mediated routes.

Note: this __init__.py intentionally does NOT import the
router or the dependencies module. Both depend on the users
module, which would create a circular import: users ->
gateway.security -> gateway.__init__ -> router -> users.services.
The router is imported directly by main.py where needed.
"""

from __future__ import annotations

from src.modules.gateway.abuse import (
    AbuseLayer,
    RateLimitResult,
    compute_fingerprint,
)
from src.modules.gateway.security import (
    TokenError,
    TokenType,
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)

__all__ = [
    "AbuseLayer",
    "RateLimitResult",
    "TokenError",
    "TokenType",
    "compute_fingerprint",
    "create_access_token",
    "create_refresh_token",
    "decode_token",
    "hash_password",
    "verify_password",
]
