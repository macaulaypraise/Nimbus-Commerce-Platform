"""Tests for the RedisDedupe helper."""

from __future__ import annotations

import pytest

from src.core.cache import AsyncCircuitBreaker
from src.core.dedupe import RedisDedupe
from tests.fakes.fake_redis import FakeRedis


class TestRedisDedupe:
    @pytest.mark.asyncio
    async def test_first_call_records(self) -> None:
        redis = FakeRedis()
        dedupe = RedisDedupe(
            redis=redis,  # type: ignore[arg-type]
            breaker=AsyncCircuitBreaker(name="t", fail_max=3, reset_timeout=1.0),
        )
        result = await dedupe.check_and_record("dedupe-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_second_call_returns_false(self) -> None:
        redis = FakeRedis()
        dedupe = RedisDedupe(
            redis=redis,  # type: ignore[arg-type]
            breaker=AsyncCircuitBreaker(name="t", fail_max=3, reset_timeout=1.0),
        )
        assert await dedupe.check_and_record("dedupe-1") is True
        assert await dedupe.check_and_record("dedupe-1") is False

    @pytest.mark.asyncio
    async def test_different_ids_are_independent(self) -> None:
        redis = FakeRedis()
        dedupe = RedisDedupe(
            redis=redis,  # type: ignore[arg-type]
            breaker=AsyncCircuitBreaker(name="t", fail_max=3, reset_timeout=1.0),
        )
        assert await dedupe.check_and_record("dedupe-1") is True
        assert await dedupe.check_and_record("dedupe-2") is True
        assert await dedupe.check_and_record("dedupe-1") is False
        assert await dedupe.check_and_record("dedupe-2") is False

    @pytest.mark.asyncio
    async def test_rejects_empty_id(self) -> None:
        redis = FakeRedis()
        dedupe = RedisDedupe(
            redis=redis,  # type: ignore[arg-type]
            breaker=AsyncCircuitBreaker(name="t", fail_max=3, reset_timeout=1.0),
        )
        with pytest.raises(ValueError):
            await dedupe.check_and_record("")

    @pytest.mark.asyncio
    async def test_rejects_zero_ttl(self) -> None:
        redis = FakeRedis()
        dedupe = RedisDedupe(
            redis=redis,  # type: ignore[arg-type]
            breaker=AsyncCircuitBreaker(name="t", fail_max=3, reset_timeout=1.0),
        )
        with pytest.raises(ValueError):
            await dedupe.check_and_record("dedupe-1", ttl_seconds=0)
