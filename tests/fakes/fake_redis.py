"""In-process Redis stub for unit tests.

Implements only the methods the abuse layer (and the rest of the
test suite) actually use:

  * String ops: ``set``, ``get``, ``exists``, ``delete``, ``ping``
  * Sorted-set ops: ``zadd``, ``zremrangebyscore``, ``zcard``
  * Expiration: ``expire``
  * Script registration: ``register_script`` -> :class:`FakeScript`

The :class:`FakeScript` reproduces the sliding-window logic in
Python so tests can assert exact behavior without a real Redis.
Production code never imports from this module.
"""

from __future__ import annotations

import time
from typing import Any


class FakeScript:
    """Mirrors redis-py's ``Script`` interface with a Python implementation.

    Captures every call in :attr:`FakeRedis._script_call_log` so
    tests can assert on the inputs to the Lua script.
    """

    def __init__(self, redis: FakeRedis, lua_source: str) -> None:
        self._redis = redis
        self._lua_source = lua_source

    async def __call__(
        self,
        *,
        keys: list[str],
        args: list[Any],
    ) -> list[int]:
        rl_key, bl_key = keys[0], keys[1]
        now = float(args[0])
        window = int(args[1])
        max_req = int(args[2])
        bl_thr = int(args[3])
        bl_ttl = int(args[4])
        rand = str(args[5])
        redis = self._redis

        redis._script_call_log.append(
            {
                "ts": now,
                "window": window,
                "max_req": max_req,
                "bl_thr": bl_thr,
                "bl_ttl": bl_ttl,
            }
        )

        if await redis.exists(bl_key) == 1:
            return [0, 3]

        await redis.zremrangebyscore(rl_key, float("-inf"), now - window)
        member = f"{now}:{rand}"
        await redis.zadd(rl_key, {member: now})
        await redis.expire(rl_key, window + 1)
        count = await redis.zcard(rl_key)

        if count > bl_thr:
            await redis.set(bl_key, "1", ex=bl_ttl)
            return [count, 2]
        if count > max_req:
            return [count, 1]
        return [count, 0]


class FakeRedis:
    """Async Redis stub backed by in-process dicts.

    Use via the ``fake_redis`` fixture in conftest. The stub tracks
    expirations in wall-clock time and evicts keys lazily on access
    (mirroring real Redis behavior closely enough for unit tests).
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.sorted_sets: dict[str, list[tuple[float, str]]] = {}
        self.expirations: dict[str, float] = {}
        self.closed = False
        self._last_ping: bool | None = None
        self._script_call_log: list[dict[str, Any]] = []

    # -- key/value --------------------------------------------------------

    def _is_expired(self, key: str) -> bool:
        deadline = self.expirations.get(key)
        return deadline is not None and time.time() > deadline

    def _evict_if_expired(self, key: str) -> None:
        if self._is_expired(key):
            self.store.pop(key, None)
            self.sorted_sets.pop(key, None)
            self.expirations.pop(key, None)

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
        **kwargs: Any,
    ) -> bool | None:
        """Set a key with optional NX (set only if not exists) and EX (TTL)."""
        self._evict_if_expired(key)
        if nx and key in self.store:
            return None  # NX failure: key already exists
        self.store[key] = value
        if ex is not None:
            self.expirations[key] = time.time() + ex
        return True

    async def eval(
        self,
        script: str,
        numkeys: int,
        *args: Any,
    ) -> Any:
        """Minimal Lua eval support.

        We only need to support one script: the lock-release script
        that does ``if GET key == ARGV[1] then DEL key``. We
        implement it directly in Python rather than parsing Lua.
        """
        if numkeys != 1:
            raise NotImplementedError(f"FakeRedis.eval only supports numkeys=1, got {numkeys}")
        key = args[0]
        expected_token = args[1]
        current = await self.get(key)
        if current is None:
            return 0
        current_str = current.decode("utf-8") if isinstance(current, bytes) else str(current)
        if current_str == expected_token:
            await self.delete(key)
            return 1
        return 0

    async def get(self, key: str) -> bytes | None:
        self._evict_if_expired(key)
        value = self.store.get(key)
        return value.encode("utf-8") if isinstance(value, str) else value

    async def exists(self, key: str) -> int:
        self._evict_if_expired(key)
        if key in self.store or key in self.sorted_sets:
            return 1
        return 0

    async def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                deleted += 1
            if key in self.sorted_sets:
                del self.sorted_sets[key]
                deleted += 1
            self.expirations.pop(key, None)
        return deleted

    async def ping(self) -> bool:
        self._last_ping = True
        return True

    async def aclose(self) -> None:
        self.closed = True

    # -- sorted sets ------------------------------------------------------

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        self._evict_if_expired(key)
        existing = {m for _, m in self.sorted_sets.get(key, [])}
        added = 0
        for member, score in mapping.items():
            if member not in existing:
                added += 1
            self.sorted_sets.setdefault(key, []).append((float(score), member))
        self.sorted_sets[key].sort(key=lambda x: x[0])
        return added

    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        self._evict_if_expired(key)
        before = len(self.sorted_sets.get(key, []))
        self.sorted_sets[key] = [
            (s, m) for s, m in self.sorted_sets.get(key, []) if not (min_score <= s <= max_score)
        ]
        return before - len(self.sorted_sets[key])

    async def zcard(self, key: str) -> int:
        self._evict_if_expired(key)
        return len(self.sorted_sets.get(key, []))

    async def expire(self, key: str, seconds: int) -> bool:
        self.expirations[key] = time.time() + seconds
        return True

    # -- script registration --------------------------------------------

    def register_script(self, lua_source: str) -> FakeScript:
        return FakeScript(self, lua_source)
