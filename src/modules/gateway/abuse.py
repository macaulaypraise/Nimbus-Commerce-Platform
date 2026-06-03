"""User-agent fingerprinting and graduated rate limiting.

Fingerprint
-----------
We compute ``SHA-256(client_ip + "|" + user_agent + "|" + accept_language)``
to identify a client device. The hash is one-way; the raw components
are never logged or stored. If the app is behind a trusted reverse
proxy, set ``ABUSE_TRUST_FORWARDED_FOR=true`` and the leftmost
``X-Forwarded-For`` entry is used as the client IP.

Rate limiting
-------------
A sliding-window counter is kept in Redis per fingerprint. The window
is one minute by default. The increment happens in an atomic Lua
script:

  1. ``ZREMRANGEBYSCORE`` removes entries older than ``now - window``.
  2. ``ZADD`` inserts the current request (member = timestamp + random).
  3. ``EXPIRE`` ensures the key is cleaned up if the client disappears.
  4. ``ZCARD`` returns the count.
  5. If count > ``blacklist_threshold``, ``SET key 1 EX blacklist_ttl``
     adds the fingerprint to the blacklist.
  6. If count > ``rate_limit_max``, return 429 (rate limited).
  7. If the blacklist already contains the fingerprint, return 403.

Graduated response
------------------
* Count > ``abuse_rate_limit_max`` (default 100) → HTTP 429.
* Count > ``abuse_blacklist_threshold`` (default 200) → fingerprint
  is blacklisted in Redis for ``abuse_blacklist_ttl_seconds`` (default
  1 hour); subsequent requests get HTTP 403.

Failure mode
------------
If the Redis circuit breaker is open (cache unavailable), the abuse
layer logs a CRITICAL warning and lets the request through. Rationale:
better to accept some over-rate traffic than to take the entire API
down with Redis. This is configurable; set
``NIMBUS_ABUSE_FAIL_CLOSED=true`` in the future to fail closed.
"""

from __future__ import annotations

import hashlib
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

from src.core.config import get_settings
from src.core.exceptions import CacheUnavailableError

if TYPE_CHECKING:
    from src.core.cache import AsyncCircuitBreaker
    from src.core.protocols import RedisLike


_log = structlog.get_logger("nimbus.gateway.abuse")


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


# Characters that cannot appear in a SHA-256 hex digest; we use this
# to validate the fingerprint format at API boundaries.
_FINGERPRINT_RE = __import__("re").compile(r"^[a-f0-9]{64}$")


def compute_fingerprint(
    *,
    client_ip: str,
    user_agent: str,
    accept_language: str = "",
    extra: str = "",
) -> str:
    """Compute a stable per-device fingerprint.

    The output is a 64-character SHA-256 hex digest. The raw inputs
    are not retained; only the digest is exposed to callers.

    Args:
        client_ip: the request's source IP. May be IPv4 or IPv6.
        user_agent: the ``User-Agent`` header value (empty string if
            absent).
        accept_language: the ``Accept-Language`` header value. Used
            as additional entropy to resist UA-only rotation attacks.
        extra: a server-controlled salt (e.g., the auth realm). Pass
            an empty string for unauthenticated endpoints.

    Returns:
        A 64-character SHA-256 hex digest.
    """
    payload = f"{client_ip}|{user_agent}|{accept_language}|{extra}".encode()
    return hashlib.sha256(payload).hexdigest()


def _extract_client_ip(request: Request, trust_forwarded_for: bool) -> str:
    """Resolve the client IP, honoring X-Forwarded-For if configured."""
    if trust_forwarded_for:
        xff = request.headers.get("X-Forwarded-For")
        if xff:
            # Leftmost entry is the original client.
            first = xff.split(",", 1)[0].strip()
            if first:
                return first
    # Starlette puts the immediate peer here.
    if request.client is not None:
        return request.client.host
    return "unknown"


# ---------------------------------------------------------------------------
# Rate limit Lua script
# ---------------------------------------------------------------------------
#
# KEYS[1] = sliding-window key (e.g. "rl:fp:<hash>")
# KEYS[2] = blacklist key    (e.g. "bl:fp:<hash>")
# ARGV[1] = current timestamp (float seconds)
# ARGV[2] = window size in seconds
# ARGV[3] = rate-limit threshold (e.g. 100)
# ARGV[4] = blacklist threshold (e.g. 200)
# ARGV[5] = blacklist TTL in seconds
# ARGV[6] = random suffix for unique member (so ZADD does not dedup)
#
# Returns: {count, status}
#   status: 0 = OK, 1 = rate limited, 2 = just blacklisted,
#           3 = already blacklisted
# ---------------------------------------------------------------------------

_SLIDING_WINDOW_LUA = """
local rl_key   = KEYS[1]
local bl_key   = KEYS[2]
local now      = tonumber(ARGV[1])
local window   = tonumber(ARGV[2])
local max_req  = tonumber(ARGV[3])
local bl_thr   = tonumber(ARGV[4])
local bl_ttl   = tonumber(ARGV[5])
local rand     = ARGV[6]

-- Already blacklisted?
if redis.call("EXISTS", bl_key) == 1 then
    return {0, 3}
end

-- Drop entries outside the window.
redis.call("ZREMRANGEBYSCORE", rl_key, "-inf", now - window)

-- Add this request.
local member = tostring(now) .. ":" .. rand
redis.call("ZADD", rl_key, now, member)
redis.call("EXPIRE", rl_key, math.ceil(window) + 1)

local count = redis.call("ZCARD", rl_key)

-- Over blacklist threshold?
if count > bl_thr then
    redis.call("SET", bl_key, "1", "EX", bl_ttl)
    return {count, 2}
end

-- Over rate-limit threshold?
if count > max_req then
    return {count, 1}
end

return {count, 0}
"""


# Lua scripts in redis-py are registered with ``register_script`` and
# the resulting ``Script`` object is invoked with ``keys=[...],
# args=[...]``. We register the script lazily on first use and
# cache the resulting object on the class.
_script_handle: Any = None


async def _get_script(redis: RedisLike) -> Any:
    """Return the cached sliding-window script for this redis instance.

    The script is registered on the redis instance itself (not in a
    module-level global) so each ``FakeRedis`` (used in tests) and each
    real ``Redis`` connection pool gets its own script handle. The
    redis-py :class:`Script` object is cheap to construct but holds a
    reference to the original redis client, so caching per-instance
    also avoids stale-client bugs in tests.

    We use :func:`setattr` rather than direct attribute assignment
    because the redis client's type stubs do not declare a slot for
    our private cache attribute. ``setattr`` makes the runtime
    mutation explicit and satisfies strict type checkers.
    """
    script = getattr(redis, "_nimbus_sliding_window_script", None)
    if script is None:
        script = redis.register_script(_SLIDING_WINDOW_LUA)
        redis._nimbus_sliding_window_script = script  # type: ignore[attr-defined]
    return script


# ---------------------------------------------------------------------------
# Rate limit result
# ---------------------------------------------------------------------------


class RateLimitStatus(str, Enum):
    OK = "ok"
    RATE_LIMITED = "rate_limited"
    JUST_BLACKLISTED = "just_blacklisted"
    ALREADY_BLACKLISTED = "already_blacklisted"
    BYPASSED = "bypassed"  # cache unavailable; fail-open path


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    status: RateLimitStatus
    count: int
    fingerprint: str
    headers: dict[str, str]


# ---------------------------------------------------------------------------
# Abuse layer
# ---------------------------------------------------------------------------


class AbuseLayer:
    """Per-request abuse checks: fingerprinting and rate limiting.

    The layer is callable as an async function returning a
    :class:`RateLimitResult`. The HTTP middleware below translates
    the result into a response.

    A single instance is created at app startup and stored on
    ``app.state.abuse``; the middleware reads it from there.
    """

    def __init__(
        self,
        *,
        redis: RedisLike,  # was: redis: Redis
        breaker: AsyncCircuitBreaker,
    ) -> None:
        self._redis = redis
        self._breaker = breaker
        self._settings = get_settings()

    # @property
    # def settings(self) -> None:
    #     return self._settings

    async def check(
        self,
        *,
        client_ip: str,
        user_agent: str,
        accept_language: str = "",
        extra: str = "",
    ) -> RateLimitResult:
        """Run fingerprint + rate-limit check for one request.

        If the circuit breaker is open, returns a BYPASSED result
        after logging a CRITICAL warning. Callers (the middleware)
        treat BYPASSED as "let the request through".
        """
        fp = compute_fingerprint(
            client_ip=client_ip,
            user_agent=user_agent,
            accept_language=accept_language,
            extra=extra,
        )

        try:
            return await self._breaker.call(self._do_check, fp)
        except CacheUnavailableError:
            _log.critical(
                "abuse.bypassed",
                fingerprint=fp,
                reason="redis_circuit_open",
            )
            return RateLimitResult(
                status=RateLimitStatus.BYPASSED,
                count=0,
                fingerprint=fp,
                headers={},
            )

    async def _do_check(self, fingerprint: str) -> RateLimitResult:
        s = self._settings
        now = time.time()
        window = float(s.abuse_rate_limit_window_seconds)
        rl_key = f"rl:fp:{fingerprint}"
        bl_key = f"bl:fp:{fingerprint}"

        script = await _get_script(self._redis)
        result = await script(
            keys=[rl_key, bl_key],
            args=[
                f"{now:.6f}",
                int(window),
                int(s.abuse_rate_limit_max),
                int(s.abuse_blacklist_threshold),
                int(s.abuse_blacklist_ttl_seconds),
                secrets.token_hex(4),
            ],
        )
        # ``result`` is a list: [count (int or str), status (int)]
        count_raw, status_raw = result
        try:
            count = int(count_raw)
        except (TypeError, ValueError):
            count = 0
        try:
            status_int = int(status_raw)
        except (TypeError, ValueError):
            status_int = 0

        if status_int == 3:
            return RateLimitResult(
                status=RateLimitStatus.ALREADY_BLACKLISTED,
                count=count,
                fingerprint=fingerprint,
                headers={"Retry-After": str(s.abuse_blacklist_ttl_seconds)},
            )
        if status_int == 2:
            _log.warning(
                "abuse.blacklisted",
                fingerprint=fingerprint,
                count=count,
                ttl=s.abuse_blacklist_ttl_seconds,
            )
            return RateLimitResult(
                status=RateLimitStatus.JUST_BLACKLISTED,
                count=count,
                fingerprint=fingerprint,
                headers={"Retry-After": str(s.abuse_blacklist_ttl_seconds)},
            )
        if status_int == 1:
            return RateLimitResult(
                status=RateLimitStatus.RATE_LIMITED,
                count=count,
                fingerprint=fingerprint,
                headers={"Retry-After": str(s.abuse_rate_limit_window_seconds)},
            )
        return RateLimitResult(
            status=RateLimitStatus.OK,
            count=count,
            fingerprint=fingerprint,
            headers={},
        )


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class AbuseMiddleware(BaseHTTPMiddleware):
    """ASGI middleware that enforces the abuse layer on every request.

    On any non-OK status other than BYPASSED, short-circuits with
    the appropriate HTTP response. On OK or BYPASSED, calls the
    downstream app and lets it run.
    """

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)
        # The layer is fetched lazily in ``dispatch`` from
        # ``request.app.state.abuse``. This avoids constructing the
        # layer at import time (which would force a Redis connection
        # even when running tests that don't need it).
        self._settings = get_settings()

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        # Allow a small allowlist of paths to bypass abuse checks
        # (e.g. internal health probes).
        if request.url.path in self._settings.cors_origins or False:
            pass  # placeholder; kept for readability
        if request.url.path in {"/health", "/ready", "/metrics"}:
            return await call_next(request)

        layer: AbuseLayer | None = getattr(request.app.state, "abuse", None)
        if layer is None:
            # Layer not initialized; fail open with a critical log so
            # we know misconfiguration happened.
            _log.critical("abuse.layer_missing", path=request.url.path)
            return await call_next(request)

        client_ip = _extract_client_ip(request, self._settings.abuse_trust_forwarded_for)
        user_agent = request.headers.get("User-Agent", "")
        accept_language = request.headers.get("Accept-Language", "")

        result = await layer.check(
            client_ip=client_ip,
            user_agent=user_agent,
            accept_language=accept_language,
        )

        if result.status == RateLimitStatus.OK:
            return await call_next(request)
        if result.status == RateLimitStatus.BYPASSED:
            return await call_next(request)

        if result.status == RateLimitStatus.RATE_LIMITED:
            return _json_error(
                status_code=429,
                code="nimbus.rate_limited",
                message="Too many requests. Please retry later.",
                details={"retry_after_seconds": self._settings.abuse_rate_limit_window_seconds},
                headers={
                    "Retry-After": str(self._settings.abuse_rate_limit_window_seconds),
                    "X-RateLimit-Limit": str(self._settings.abuse_rate_limit_max),
                    "X-RateLimit-Remaining": "0",
                },
            )
        # JUST_BLACKLISTED or ALREADY_BLACKLISTED
        return _json_error(
            status_code=403,
            code="nimbus.blacklisted",
            message="Origin has been temporarily blocked due to abuse.",
            details={"retry_after_seconds": self._settings.abuse_blacklist_ttl_seconds},
            headers={
                "Retry-After": str(self._settings.abuse_blacklist_ttl_seconds),
            },
        )


def _json_error(
    *,
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any],
    headers: dict[str, str],
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "details": details}},
        headers=headers,
    )
