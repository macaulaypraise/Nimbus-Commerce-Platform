"""Tests for the gateway abuse layer.

Uses the in-process FakeRedis to exercise the rate limiter and
blacklist logic without requiring a real Redis server.
"""

from __future__ import annotations

import pytest

from src.core.cache import AsyncCircuitBreaker
from src.core.exceptions import CacheUnavailableError
from src.modules.gateway.abuse import (
    AbuseLayer,
    RateLimitStatus,
    compute_fingerprint,
)
from tests.fakes.fake_redis import FakeRedis  # noqa: TC001


class TestFingerprint:
    def test_deterministic(self) -> None:
        a = compute_fingerprint(
            client_ip="1.2.3.4", user_agent="Mozilla/5.0", accept_language="en-US"
        )
        b = compute_fingerprint(
            client_ip="1.2.3.4", user_agent="Mozilla/5.0", accept_language="en-US"
        )
        assert a == b
        assert len(a) == 64

    def test_different_ip_differs(self) -> None:
        a = compute_fingerprint(client_ip="1.2.3.4", user_agent="x")
        b = compute_fingerprint(client_ip="1.2.3.5", user_agent="x")
        assert a != b

    def test_different_user_agent_differs(self) -> None:
        a = compute_fingerprint(client_ip="1.2.3.4", user_agent="x")
        b = compute_fingerprint(client_ip="1.2.3.4", user_agent="y")
        assert a != b

    def test_extra_salt_changes_digest(self) -> None:
        a = compute_fingerprint(client_ip="1.2.3.4", user_agent="x", extra="")
        b = compute_fingerprint(client_ip="1.2.3.4", user_agent="x", extra="salt")
        assert a != b


class TestRateLimit:
    @pytest.fixture
    def layer(self, fake_redis: FakeRedis, monkeypatch: pytest.MonkeyPatch) -> AbuseLayer:
        # Lower thresholds so the test doesn't need 100+ requests.
        s = get_settings_cache(monkeypatch)
        s.abuse_rate_limit_max = 5
        s.abuse_blacklist_threshold = 10
        s.abuse_blacklist_ttl_seconds = 60
        s.abuse_rate_limit_window_seconds = 60
        breaker = AsyncCircuitBreaker(name="test", fail_max=3, reset_timeout=0.1)
        return AbuseLayer(redis=fake_redis, breaker=breaker)  # type: ignore[invalid-argument-type]

    @pytest.mark.asyncio
    async def test_first_request_is_ok(self, layer: AbuseLayer) -> None:
        result = await layer.check(client_ip="1.1.1.1", user_agent="ua")
        assert result.status is RateLimitStatus.OK
        assert result.count == 1

    @pytest.mark.asyncio
    async def test_under_threshold_all_ok(self, layer: AbuseLayer) -> None:
        for _ in range(5):
            result = await layer.check(client_ip="1.1.1.1", user_agent="ua")
            assert result.status is RateLimitStatus.OK
        assert result.count == 5

    @pytest.mark.asyncio
    async def test_over_max_returns_rate_limited(self, layer: AbuseLayer) -> None:
        for _ in range(5):
            await layer.check(client_ip="1.1.1.1", user_agent="ua")
        # 6th request should be rate limited.
        result = await layer.check(client_ip="1.1.1.1", user_agent="ua")
        assert result.status is RateLimitStatus.RATE_LIMITED
        assert result.count == 6
        assert "Retry-After" in result.headers

    @pytest.mark.asyncio
    async def test_over_threshold_blacklists(self, layer: AbuseLayer) -> None:
        # With abuse_rate_limit_max=5 and abuse_blacklist_threshold=10:
        #   requests 1-5: OK
        #   requests 6-10: RATE_LIMITED (5 < count <= 10)
        #   request 11:    JUST_BLACKLISTED (count > 10)
        # We capture each result so we can assert on the exact moment
        # of transition, not on a follow-up call.
        statuses: list[RateLimitStatus] = []
        for _ in range(11):
            result = await layer.check(client_ip="2.2.2.2", user_agent="ua")
            statuses.append(result.status)

        assert statuses[:5] == [RateLimitStatus.OK] * 5
        assert statuses[5:10] == [RateLimitStatus.RATE_LIMITED] * 5
        assert statuses[10] is RateLimitStatus.JUST_BLACKLISTED

        # Subsequent request: already blacklisted.
        result = await layer.check(client_ip="2.2.2.2", user_agent="ua")
        assert result.status is RateLimitStatus.ALREADY_BLACKLISTED

    @pytest.mark.asyncio
    async def test_different_fingerprints_isolated(self, layer: AbuseLayer) -> None:
        for _ in range(6):
            await layer.check(client_ip="3.3.3.3", user_agent="ua")
        # A different IP is a different fingerprint.
        result = await layer.check(client_ip="3.3.3.4", user_agent="ua")
        assert result.status is RateLimitStatus.OK


class TestCircuitBreakerBypass:
    @pytest.mark.asyncio
    async def test_breaker_open_returns_bypassed(
        self, fake_redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Force the breaker open by triggering the fail_max.
        breaker = AsyncCircuitBreaker(name="test", fail_max=2, reset_timeout=60.0)
        layer = AbuseLayer(redis=fake_redis, breaker=breaker)  # type: ignore[invalid-argument-type]

        # Make the inner call fail by patching the script handle.
        from src.modules.gateway import abuse as abuse_mod

        original_get_script = abuse_mod._get_script

        async def _failing_script(_redis: object) -> object:
            raise CacheUnavailableError("simulated")

        monkeypatch.setattr(abuse_mod, "_get_script", _failing_script)

        # Two failures to open the breaker.
        for _ in range(2):
            await layer.check(client_ip="4.4.4.4", user_agent="ua")

        # Reset the script so the next call would otherwise succeed.
        monkeypatch.setattr(abuse_mod, "_get_script", original_get_script)

        # Now the breaker is open, so the layer must bypass and log.
        result = await layer.check(client_ip="4.4.4.4", user_agent="ua")
        assert result.status is RateLimitStatus.BYPASSED


# Helper: get a settings instance and override thresholds via monkeypatch.
def get_settings_cache(monkeypatch: pytest.MonkeyPatch):
    from src.core.config import get_settings

    s = get_settings()
    return s
