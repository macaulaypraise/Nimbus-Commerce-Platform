"""Structural type contracts shared by real and fake clients.

The ``RedisLike`` protocol is what the application code depends on,
not the concrete :class:`redis.asyncio.Redis`. Tests use the
in-process :class:`tests.fakes.fake_redis.FakeRedis`, which is a
duck-typed implementation that satisfies this protocol without
inheriting from :class:`redis.asyncio.Redis`.

We only declare the methods the application code actually calls.
Adding a new method to the protocol is a deliberate API change.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class RedisLike(Protocol):
    """Minimal surface area of a Redis client the app depends on.

    Signatures are intentionally permissive (use ``Any``/``Mapping``)
    so the real :class:`redis.asyncio.Redis` satisfies the protocol
    structurally. The production app only ever calls these methods
    with the documented argument types; the Protocol just guarantees
    the methods exist.

    Methods:
        set(key, value, ex=...): set a key with optional TTL.
        get(key): retrieve a key.
        exists(key): check key existence.
        delete(*keys): delete keys.
        ping(): health check.
        aclose(): close the client.
        zadd(key, mapping): add to a sorted set.
        zremrangebyscore(key, min, max): drop entries by score range.
        zcard(key): count entries in a sorted set.
        expire(key, seconds): set TTL on a key.
        register_script(source): return a Script callable.
    """

    # Key/value
    async def set(
        self,
        key: Any,
        value: Any,
        ex: int | None = ...,
        **kwargs: Any,
    ) -> Any: ...

    async def get(self, key: Any) -> Any: ...
    async def exists(self, key: Any) -> int: ...
    async def delete(self, *keys: Any) -> int: ...
    async def ping(self) -> bool: ...

    # Lifecycle. Note: redis-py 5.2.x stubs declare ``close()``;
    # runtime supports both ``close()`` and ``aclose()``.
    async def aclose(self) -> None: ...

    # Sorted sets
    async def zadd(self, key: Any, mapping: Mapping[Any, Any], **kwargs: Any) -> int: ...
    async def zremrangebyscore(self, key: Any, min_score: Any, max_score: Any) -> int: ...
    async def zcard(self, key: Any) -> int: ...
    async def expire(self, key: Any, seconds: int) -> bool: ...

    # Script registration
    def register_script(self, source: str) -> Any: ...
