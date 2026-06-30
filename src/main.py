"""FastAPI application entry point.

The :func:`create_app` factory is the single place where the application
graph is assembled. It is safe to call multiple times (e.g., from tests
that want isolated app instances); the module-level ``app`` is provided
for ``uvicorn src.main:app``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import structlog
from fastapi import FastAPI

from src.core.cache import get_breaker, get_redis
from src.core.config import SettingsDep, get_settings
from src.core.database import get_engine
from src.core.exceptions import _install_handlers
from src.core.messaging import get_producer
from src.core.telemetry import RequestContextMiddleware, configure_logging
from src.modules.gateway.abuse import AbuseLayer, AbuseMiddleware
from src.modules.orders.consumer import OrderConsumer
from src.workers.outbox_relay import OutboxRelay

API_TITLE = "Nimbus Commerce Platform"
API_DESCRIPTION = (
    "Modular monolith backend for the Nimbus commerce platform. "
    "Domains (gateway, orders, inventory, payments, notifications, admin) "
    "are isolated under ``src/modules/`` and communicate via synchronous "
    "Python interfaces and asynchronous Kafka / Redis channels."
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Configure logging, build singletons, start workers, log shutdown."""
    settings = get_settings()
    configure_logging(settings)

    log = structlog.get_logger("nimbus.startup")
    log.info(
        "app.startup",
        app=settings.app_name,
        version=settings.app_version,
        environment=settings.environment,
    )

    # --- Build singletons -------------------------------------------
    app.state.abuse = AbuseLayer(
        redis=get_redis(),  # type: ignore[arg-type]
        breaker=get_breaker(),
    )

    # --- Start background workers -----------------------------------
    relay: OutboxRelay | None = None
    if settings.outbox_relay_enabled:
        try:
            producer = get_producer()
            await producer.start()
            relay = OutboxRelay(engine=get_engine(), producer=producer)
            await relay.start()
        except Exception as exc:
            log.error(
                "outbox_relay.start_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            # Don't prevent the app from starting; the relay
            # being down is a degraded mode, not a fatal error.
            relay = None

    # Start the order consumer
    order_consumer: OrderConsumer | None = None
    if settings.kafka_consumer_enabled:
        try:
            order_consumer = OrderConsumer()
            await order_consumer.start()
        except Exception as exc:
            log.error(
                "order_consumer.start_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            order_consumer = None
    try:
        yield
    finally:
        log.info("app.shutdown", app=settings.app_name, version=settings.app_version)
        if relay is not None:
            await relay.stop()
        try:
            producer = get_producer()
            if producer.started:
                await producer.stop()
            if order_consumer is not None:
                await order_consumer.stop()
        except Exception:
            pass


def create_app() -> FastAPI:
    """Construct and configure the FastAPI application."""
    settings = get_settings()
    configure_logging(settings)

    docs_enabled = not settings.is_production
    app = FastAPI(
        title=API_TITLE,
        description=API_DESCRIPTION,
        version=settings.app_version,
        lifespan=lifespan,
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json" if docs_enabled else None,
    )

    # Middleware: outermost added last runs first. Order matters:
    # 1. RequestContextMiddleware attaches request_id and binds contextvars.
    # 2. AbuseMiddleware runs the rate-limit / blacklist check.
    # Starlette's type stubs use a private _MiddlewareFactory alias that
    # ty doesn't fully resolve yet. Both middleware classes are valid
    # BaseHTTPMiddleware subclasses. See ty#1234 if you want to track
    # upstream.
    app.add_middleware(cast(Any, AbuseMiddleware))
    app.add_middleware(cast(Any, RequestContextMiddleware))

    # Exception handlers.
    _install_handlers(app)

    @app.get("/health", tags=["meta"], summary="Liveness probe")
    async def health(settings: SettingsDep) -> dict[str, str]:
        """Lightweight liveness probe. Does NOT touch external systems."""
        return {
            "status": "ok",
            "app": settings.app_name,
            "version": settings.app_version,
            "environment": settings.environment,
        }

    @app.get("/ready", tags=["meta"], summary="Readiness probe")
    async def ready() -> dict[str, str]:
        """Readiness probe. Touches DB and Redis; reports component status."""
        from src.core.cache import health_check as redis_health
        from src.core.database import health_check as db_health

        db_ok = await db_health()
        redis_ok = await redis_health()
        return {
            "status": "ok" if (db_ok and redis_ok) else "degraded",
            "database": "ok" if db_ok else "down",
            "redis": "ok" if redis_ok else "down",
        }

    return app


app = create_app()
