"""FastAPI application entry point.

The :func:`create_app` factory is the single place where the application
graph is assembled. It is safe to call multiple times (e.g., from tests
that want isolated app instances); the module-level ``app`` is provided
for ``uvicorn src.main:app``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from src.core.config import SettingsDep, get_settings
from src.core.exceptions import _install_handlers
from src.core.telemetry import RequestContextMiddleware, configure_logging

API_TITLE = "Nimbus Commerce Platform"
API_DESCRIPTION = (
    "Modular monolith backend for the Nimbus commerce platform. "
    "Domains (gateway, orders, inventory, payments, notifications, admin) "
    "are isolated under ``src/modules/`` and communicate via synchronous "
    "Python interfaces and asynchronous Kafka / Redis channels."
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: configure logging on startup, log on shutdown."""
    settings = get_settings()
    configure_logging(settings)

    log = structlog.get_logger("nimbus.startup")
    log.info(
        "app.startup",
        app=settings.app_name,
        version=settings.app_version,
        environment=settings.environment,
    )
    try:
        yield
    finally:
        log.info("app.shutdown", app=settings.app_name, version=settings.app_version)


def create_app() -> FastAPI:
    """Construct and configure the FastAPI application."""
    settings = get_settings()
    configure_logging(settings)

    # Disable interactive docs in production to reduce attack surface.
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

    # Middleware (outermost added last is the outermost in execution order).
    app.add_middleware(RequestContextMiddleware)  # pyright: ignore[reportArgumentType]

    # Exception handlers.
    _install_handlers(app)

    # ---- Routes --------------------------------------------------------
    @app.get("/health", tags=["meta"], summary="Liveness probe")
    async def health(settings: SettingsDep) -> dict[str, str]:
        """Lightweight liveness probe.

        Returns 200 as long as the process is up and able to serve
        requests. Intentionally does NOT touch the database, cache, or
        message broker — use a dedicated readiness probe for that.
        """
        return {
            "status": "ok",
            "app": settings.app_name,
            "version": settings.app_version,
            "environment": settings.environment,
        }

    return app


# Module-level instance for ``uvicorn src.main:app``.
app = create_app()
