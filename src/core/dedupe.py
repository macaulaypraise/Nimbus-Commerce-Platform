"""Redis-backed dedupe for at-least-once message delivery.

Uses atomic SETNX with TTL. First caller wins; subsequent
callers see the existing key and skip.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from src.core.config import Settings, get_settings
from src.core.exceptions import CacheUnavailableError

if TYPE_CHECKING:
    from src.core.cache import AsyncCircuitBreaker
    from src.core.protocols import RedisLike

_log = structlog.get_logger("nimbus.dedupe")


class RedisDedupe:
    """Atomic check-and-set dedupe backed by Redis."""

    def __init__(
        self,
        *,
        redis: RedisLike,
        breaker: AsyncCircuitBreaker,
        settings: Settings | None = None,
        prefix: str = "dedupe:",
    ) -> None:
        self._redis = redis
        self._breaker = breaker
        self._settings = settings or get_settings()
        self._prefix = prefix

    async def check_and_record(
        self,
        dedupe_id: str,
        *,
        ttl_seconds: int | None = None,
    ) -> bool:
        """Atomically check and record ``dedupe_id``.

        Returns True if newly recorded (caller should process),
        False if already recorded (caller should skip).
        """
        if not dedupe_id:
            raise ValueError("dedupe_id must be a non-empty string")

        ttl = ttl_seconds if ttl_seconds is not None else 7 * 24 * 60 * 60
        if ttl <= 0:
            raise ValueError("ttl_seconds must be > 0")

        key = f"{self._prefix}{dedupe_id}"

        async def _set() -> bool:
            result = await self._redis.set(key, "1", nx=True, ex=ttl)
            return bool(result)

        try:
            return await self._breaker.call(_set)
        except CacheUnavailableError:
            # Redis is down. Fail open: assume the message is
            # new. Handlers should be idempotent for safety.
            _log.warning("dedupe.cache_unavailable", dedupe_id=dedupe_id)
            return True
