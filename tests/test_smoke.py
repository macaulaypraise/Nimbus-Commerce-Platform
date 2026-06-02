"""Smoke tests for the foundation layer.

These tests verify the application boots, the test-isolation guard
passes, the /health endpoint responds, and the request-id middleware
round-trips. They do NOT touch the database, cache, or message broker.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_endpoint_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["environment"] in {"development", "staging", "production", "test"}


@pytest.mark.asyncio
async def test_request_id_round_trips(http_client: AsyncClient) -> None:
    response = await http_client.get("/health", headers={"X-Request-ID": "smoke-req-12345"})
    assert response.headers["X-Request-ID"] == "smoke-req-12345"


@pytest.mark.asyncio
async def test_request_id_generated_when_absent(http_client: AsyncClient) -> None:
    response = await http_client.get("/health")
    request_id = response.headers.get("X-Request-ID")
    assert request_id is not None
    assert len(request_id) > 0


def test_settings_construction_succeeds() -> None:
    """The Settings class must construct cleanly in the test env."""
    from src.core.config import Settings

    settings = Settings()
    assert settings.app_name == "nimbus-test"
    assert settings.is_test
    assert settings.cors_origins == []  # default; empty in .env.test
    assert isinstance(settings.kafka_bootstrap_servers, list)


def test_test_isolation_guard_passes() -> None:
    """assert_test_isolation must not raise in the test env."""
    from src.core.config import Settings

    Settings().assert_test_isolation()  # would raise ValueError on violation
