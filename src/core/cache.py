"""Async Redis client factory and async circuit breaker.

The :class:`AsyncCircuitBreaker` follows the same state machine as
``pybreaker.CircuitBreaker`` (closed -> open -> half-open -> closed)
so existing observability dashboards and alert rules can match. The
breaker is implemented as a thin async-friendly wrapper so we can
``await`` cache operations without spawning threads.

When the breaker is open, all wrapped operations raise
:class:`CacheUnavailableError` immediately. Callers (e.g. the abuse
middleware) can choose to fail open by catching this exception and
logging a CRITICAL warning instead of propagating.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any, Literal, TypeVar

import structlog
from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from src.core.config import Settings, get_settings
from src.core.exceptions import CacheUnavailableError

_log = structlog.get_logger("nimbus.cache")

T = TypeVar("T")

# Process-singleton pool and client. Created on first call to
# :func:`get_redis` and disposed via :func:`dispose_redis`.
_pool: ConnectionPool[Any] | None = None
_redis: Redis[Any] | None = None
_breaker: AsyncCircuitBreaker | None = None


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


BreakerState = Literal["closed", "open", "half-open"]


class AsyncCircuitBreaker:
    """Async-friendly circuit breaker.

    State machine matches :class:`pybreaker.CircuitBreaker`:

        closed  --(N consecutive failures)-->  open
        open    --(reset_timeout elapsed)-->  half-open
        half-open --(success)--> closed
        half-open --(failure)--> open

    State transitions are protected by an asyncio lock so concurrent
    tasks cannot race the counters. The breaker is designed to wrap a
    single dependency (Redis in our case); a new instance per dep.
    """

    def __init__(
        self,
        *,
        name: str,
        fail_max: int,
        reset_timeout: float,
    ) -> None:
        if fail_max <= 0:
            raise ValueError("fail_max must be > 0")
        if reset_timeout <= 0:
            raise ValueError("reset_timeout must be > 0")
        self._name = name
        self._fail_max = fail_max
        self._reset_timeout = reset_timeout
        self._state: BreakerState = "closed"
        self._failure_count = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> BreakerState:
        return self._state

    @property
    def is_open(self) -> bool:
        return self._state == "open"

    @property
    def failure_count(self) -> int:
        return self._failure_count

    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Invoke ``func`` through the breaker.

        Raises:
            CacheUnavailableError: if the breaker is open.
            Exception: any exception raised by ``func``.
        """
        async with self._lock:
            self._maybe_transition_to_half_open()

        if self._state == "open":
            raise CacheUnavailableError(f"Circuit breaker {self._name!r} is open")

        try:
            result = await func(*args, **kwargs)
        except Exception:
            await self._record_failure()
            raise
        else:
            await self._record_success()
            return result

    def _maybe_transition_to_half_open(self) -> None:
        if (
            self._state == "open"
            and self._opened_at is not None
            and time.monotonic() - self._opened_at >= self._reset_timeout
        ):
            self._state = "half-open"
            _log.info("cache.breaker.half_open", breaker=self._name)

    async def _record_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            if self._state == "half-open" or self._failure_count >= self._fail_max:
                self._state = "open"
                self._opened_at = time.monotonic()
                _log.error(
                    "cache.breaker.open",
                    breaker=self._name,
                    failure_count=self._failure_count,
                )

    async def _record_success(self) -> None:
        async with self._lock:
            if self._state == "half-open" or self._state == "open":
                _log.info("cache.breaker.closed", breaker=self._name)
            self._state = "closed"
            self._failure_count = 0
            self._opened_at = None

    async def reset(self) -> None:
        """Force the breaker back to closed. Used in tests."""
        async with self._lock:
            self._state = "closed"
            self._failure_count = 0
            self._opened_at = None


# ---------------------------------------------------------------------------
# Redis client
# ---------------------------------------------------------------------------


def _build_pool(settings: Settings) -> ConnectionPool[Any]:
    """Build a connection pool sized for the environment.

    In tests we use a smaller pool (max 5) to surface connection
    leaks quickly. In production we use the configured max.
    """
    max_connections = settings.redis_max_connections
    if settings.is_test:
        max_connections = min(max_connections, 5)

    return ConnectionPool.from_url(
        settings.redis_url,
        max_connections=max_connections,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
        health_check_interval=settings.redis_health_check_interval_seconds,
        retry_on_timeout=True,
        retry=Retry(ExponentialBackoff(base=0.1, cap=1.0), retries=2),
        decode_responses=False,  # we handle encoding explicitly
    )


def get_redis() -> Redis[Any]:
    """Return the process-singleton :class:`redis.asyncio.Redis`."""
    global _pool, _redis, _breaker
    if _redis is None:
        settings = get_settings()
        _pool = _build_pool(settings)
        _redis = Redis(connection_pool=_pool)
        _breaker = AsyncCircuitBreaker(
            name="nimbus.redis",
            fail_max=settings.redis_circuit_breaker_fail_max,
            reset_timeout=settings.redis_circuit_breaker_reset_timeout_seconds,
        )
        _log.info(
            "cache.client_created",
            max_connections=settings.redis_max_connections,
            socket_timeout=settings.redis_socket_timeout_seconds,
        )
    return _redis


def get_breaker() -> AsyncCircuitBreaker:
    """Return the process-singleton :class:`AsyncCircuitBreaker`."""
    if _breaker is None:
        get_redis()  # triggers initialization
    assert _breaker is not None
    return _breaker


async def dispose_redis() -> None:
    """Dispose the connection pool. Idempotent."""
    global _pool, _redis, _breaker
    if _redis is not None:
        await _redis.aclose()  # type: ignore[attr-defined]
        _log.info("cache.client_disposed")
    if _pool is not None:
        await _pool.aclose()  # type: ignore[attr-defined]
    _pool = None
    _redis = None
    _breaker = None


async def health_check() -> bool:
    """Return True if Redis answers PING. Never raises."""
    try:
        client = get_redis()
        return bool(await client.ping())
    except Exception as exc:
        _log.warning("cache.health_check_failed", error=str(exc))
        return False
