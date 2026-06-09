"""In-process fakes for external services used in tests.

Each fake in this package implements a minimal subset of the real
client API sufficient for the unit tests. They live in their own
package (not in conftest.py) so they can be imported by type
checkers and reused across test modules.
"""

from __future__ import annotations

from typing import cast

from src.core.protocols import RedisLike
from tests.fakes.fake_redis import FakeRedis, FakeScript

__all__ = ["FakeRedis", "FakeScript", "as_redis_like"]


def as_redis_like(redis: FakeRedis) -> RedisLike:
    """Cast a :class:`FakeRedis` to :class:`RedisLike` for production-code
    type checks.

    ``FakeRedis`` structurally satisfies the ``RedisLike`` protocol;
    the cast is a static-only annotation that silences the strict
    ``ty`` type checker's known false positive on Protocol member
    resolution for concrete classes. At runtime this is a no-op.

    Usage::

        await acquire_idempotency_lock(
            idempotency_key="abc",
            redis=as_redis_like(fake_redis),
            ttl_seconds=30,
        )
    """
    return cast(RedisLike, redis)
