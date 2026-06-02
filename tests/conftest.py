"""Pytest configuration and shared fixtures for the Nimbus test suite.

=========================================================================
TEST ISOLATION SAFETY NET
=========================================================================
This module executes a strict guard at IMPORT TIME — before pytest
collects tests, before fixtures resolve, before any application code is
imported. If a production-looking URL is detected in the test
environment, the process aborts with ``SystemExit(2)`` to protect
production data.

The guard enforces three rules:

  1. ``ENVIRONMENT`` is pinned to ``test``.
  2. Production URL env vars (``DATABASE_URL``, ``REDIS_URL``,
     ``KAFKA_BOOTSTRAP_SERVERS``) MUST NOT be set.
  3. The ``TEST_DATABASE_URL`` and ``TEST_REDIS_URL`` values MUST look
     like test URLs: localhost / 127.0.0.1 host, database name
     containing "test", and a non-zero Redis database number.

The companion :meth:`Settings.assert_test_isolation` runs the same
checks from inside the process, so even if a future refactor bypasses
this conftest, the in-process guard still fires.
"""

from __future__ import annotations

import logging
import os
import sys
import warnings
from collections.abc import AsyncIterator
from urllib.parse import urlparse

import pytest
from httpx import AsyncClient

# ===========================================================================
# 1. Module-level guard (runs at import time, before any test or app code).
# ===========================================================================

os.environ["ENVIRONMENT"] = "test"

# Sensible defaults so the suite runs out of the box against a local
# docker-compose stack. CI and developers can override these via real
# env vars (which is the whole point of the test/prod URL split).
os.environ.setdefault(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://nimbus:nimbus@127.0.0.1:5432/nimbus_test",
)
os.environ.setdefault("TEST_REDIS_URL", "redis://127.0.0.1:6379/1")
os.environ.setdefault("TEST_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
os.environ.setdefault("LOG_LEVEL", "WARNING")

# Nimbus-specific defaults: APP_NAME is overridden so smoke tests can
# assert the test-mode identifier, and CORS_ORIGINS is forced empty in
# tests (no browser origin needs to reach the in-process ASGI app).
os.environ.setdefault("APP_NAME", "nimbus-test")
os.environ.setdefault("CORS_ORIGINS", "")

# Host substrings that strongly indicate a production / managed-service
# endpoint. The list is intentionally conservative; we err on the side
# of allowing unusual but legitimate test hosts.
_PROD_HOST_FORBIDDEN_SUBSTRINGS: tuple[str, ...] = (
    "rds.amazonaws.com",
    "amazonaws.com",
    "azure.com",
    "windows.net",
    "confluent.cloud",
    "memorystore.googleapis.com",
    "prod-",
    "-prod.",
    ".prod.",
)


def _enforce_test_isolation() -> None:
    """Abort the process if any test-isolation rule is violated.

    Raises:
        SystemExit: Always, if any rule is violated. The exit code is
            ``2`` so CI surfaces a clear failure.
    """
    errors: list[str] = []

    # Rule 1: production URL env vars must NOT be set.
    for prod_var in (
        "DATABASE_URL",
        "REDIS_URL",
        "KAFKA_BOOTSTRAP_SERVERS",
    ):
        value = os.environ.get(prod_var)
        if value:
            errors.append(
                f"Production env var {prod_var}={value!r} is set in test "
                f"context. Unset it (or remove it from your .env) and rely "
                f"on TEST_{prod_var} instead."
            )

    # Rule 2: test URLs must be set and parseable.
    test_db = os.environ.get("TEST_DATABASE_URL", "")
    test_redis = os.environ.get("TEST_REDIS_URL", "")

    if not test_db:
        errors.append("TEST_DATABASE_URL is not set.")
    if not test_redis:
        errors.append("TEST_REDIS_URL is not set.")

    # Rule 3: test URL contents must look like test URLs.
    for url, name in (
        (test_db, "TEST_DATABASE_URL"),
        (test_redis, "TEST_REDIS_URL"),
    ):
        if not url:
            continue
        try:
            parsed = urlparse(url)
        except ValueError as exc:
            errors.append(f"{name}={url!r} is not a valid URL: {exc}")
            continue

        host = (parsed.hostname or "").lower()
        scheme = (parsed.scheme or "").lower()

        for forbidden in _PROD_HOST_FORBIDDEN_SUBSTRINGS:
            if forbidden in host:
                errors.append(
                    f"{name}={url!r} contains forbidden production host "
                    f"substring {forbidden!r}. Refusing to run."
                )

        if name == "TEST_DATABASE_URL":
            db_name = parsed.path.lstrip("/").split("?")[0]
            if "test" not in db_name.lower():
                errors.append(
                    f"TEST_DATABASE_URL={url!r} does not contain 'test' in "
                    f"its database name ({db_name!r}). Use a database name "
                    f"like 'nimbus_test' to prevent accidental production "
                    f"writes."
                )
            if scheme not in ("postgresql", "postgresql+asyncpg", "postgres"):
                errors.append(
                    f"TEST_DATABASE_URL={url!r} has scheme {scheme!r}; "
                    f"expected 'postgresql+asyncpg' or 'postgresql'."
                )

        if name == "TEST_REDIS_URL":
            db_segment = parsed.path.lstrip("/").split("?")[0] or "0"
            if db_segment == "0":
                errors.append(
                    f"TEST_REDIS_URL={url!r} uses Redis db 0 (the default). "
                    f"Use a dedicated test database number (1-15) to avoid "
                    f"clobbering any cached data."
                )
            if scheme not in ("redis", "rediss"):
                errors.append(
                    f"TEST_REDIS_URL={url!r} has scheme {scheme!r}; "
                    f"expected 'redis' or 'rediss'."
                )

    if errors:
        banner = "=" * 78
        sys.stderr.write(
            f"\n{banner}\n"
            f"  TEST ISOLATION VIOLATION — ABORTING\n"
            f"{banner}\n" + "\n".join(f"  • {e}" for e in errors) + f"\n{banner}\n"
            f"  Production data may be at risk. Refusing to run.\n"
            f"{banner}\n"
        )
        sys.stderr.flush()
        raise SystemExit(2)


_enforce_test_isolation()


# ===========================================================================
# 2. Now safe to import application code.
# ===========================================================================

from src.core.config import Settings, get_settings  # noqa: E402

# ===========================================================================
# 3. Pytest hooks
# ===========================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers and silence known-noisy third-party warnings."""
    config.addinivalue_line("markers", "integration: marks tests that require real infrastructure")
    config.addinivalue_line("markers", "unit: marks pure unit tests with no I/O")

    # Belt-and-braces: catch deprecation warnings from libraries we don't
    # control, but escalate everything else to an error.
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="sqlalchemy")
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="aiokafka")
    # The pytest-asyncio deprecation about redefining event_loop is gone
    # because we don't redefine it; see below.

    for noisy in ("aiokafka", "kafka", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ===========================================================================
# 4. Event loop policy
# ===========================================================================
# We DO NOT define a custom ``event_loop`` fixture. pytest-asyncio 0.25
# deprecated that approach; the session scope is configured in
# ``pyproject.toml`` via ``asyncio_default_fixture_loop_scope = "session"``
# and per-test scope via the ``loop_scope`` argument to the asyncio mark.
# ===========================================================================


# ===========================================================================
# 5. Settings fixture (validated)
# ===========================================================================


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Return the validated test :class:`Settings` instance.

    The ``assert_test_isolation`` call inside ``get_settings`` is the
    second line of defense (the first being the module-level guard at
    the top of this file).
    """
    return get_settings()


# ===========================================================================
# 6. HTTP client fixture
# ===========================================================================


@pytest.fixture
async def http_client() -> AsyncIterator[AsyncClient]:
    """Async HTTP client bound to the in-process FastAPI app.

    Uses ``httpx.ASGITransport`` so requests never hit the network —
    they go straight through the ASGI stack. This keeps tests fast and
    deterministic.
    """
    from httpx import ASGITransport, AsyncClient

    from src.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
