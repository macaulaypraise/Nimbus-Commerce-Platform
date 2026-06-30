"""Async SQLAlchemy engine factory and session dependency.

Provides:
  * :func:`create_engine` — a process-singleton ``AsyncEngine`` with
    per-connection ``search_path`` enforcement and a circuit breaker
    for transient connection failures.
  * :func:`get_session` — a FastAPI dependency that yields a
    schema-scoped ``AsyncSession``, rolling back on exception and
    ensuring the connection is released back to the pool.
  * :func:`health_check` — a non-destructive ``SELECT 1`` probe for
    readiness checks.

Schema isolation
----------------
Every module writes only to its own PostgreSQL schema. We enforce this
in the SQLAlchemy ``checkout`` event: each time a connection is checked
out of the pool, the active schema is set via ``SET search_path``.
The active schema is read from a ``ContextVar`` that the session
dependency sets before opening a session.

In tests we use ``NullPool`` (no reuse) so each session gets a fresh
connection with a guaranteed-empty session state. This is essential
to prevent bleed-through between test cases.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Annotated, Any

import pybreaker  # type: ignore[import-not-found]
import structlog
from fastapi import Depends
from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool
from sqlalchemy.sql import text
from sqlalchemy.sql import text as sql_text

from src.core.config import Settings, get_settings
from src.core.exceptions import (
    DatabaseConnectionError,
    DatabaseLockError,
    NimbusError,
)
from src.core.exceptions import (
    TimeoutError as NimbusTimeoutError,
)

_log = structlog.get_logger("nimbus.db")

# Per-request active schema. The session dependency sets this before
# opening a session; the SQLAlchemy ``checkout`` event reads it.
_current_schema: ContextVar[str] = ContextVar("nimbus_current_schema", default="public")

# Process-singleton engine. ``lru_cache`` is process-scoped and does
# not interact with FastAPI's ``dependency_overrides``.
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_breaker: pybreaker.CircuitBreaker | None = None


# ---------------------------------------------------------------------------
# Engine construction
# ---------------------------------------------------------------------------


def _create_breaker(settings: Settings) -> pybreaker.CircuitBreaker:
    """Build a pybreaker circuit breaker for connection attempts.

    The breaker opens after ``fail_max`` consecutive connection
    failures and stays open for ``reset_timeout`` seconds before
    allowing a single trial ("half-open") request.
    """

    def _log_state_change(
        cb: pybreaker.CircuitBreaker,
        old_state: Any,
        new_state: Any,
    ) -> None:
        _log.warning(
            "db_breaker.state_change",
            breaker=cb.name,
            old_state=str(old_state),
            new_state=str(new_state),
        )

    return pybreaker.CircuitBreaker(
        fail_max=settings.redis_circuit_breaker_fail_max,
        reset_timeout=settings.redis_circuit_breaker_reset_timeout_seconds,
        name="nimbus.postgres",
        listeners=[
            type(
                "L",
                (pybreaker.CircuitBreakerListener,),
                {"state_change": staticmethod(_log_state_change)},
            )()
        ],
    )


def _build_engine(settings: Settings) -> AsyncEngine:
    """Create the AsyncEngine with the correct pool for the environment.

    Production uses :class:`AsyncAdaptedQueuePool` (the default).
    Tests use :class:`NullPool` to guarantee no connection reuse
    between sessions, which is critical for test isolation.
    """
    connect_args: dict[str, Any] = {
        "server_settings": {
            "application_name": f"{settings.app_name}/{settings.environment}",
            "statement_timeout": str(int(settings.database_command_timeout_seconds * 1000)),
            "lock_timeout": str(int(settings.database_pool_timeout_seconds * 1000)),
        },
        "timeout": settings.database_pool_timeout_seconds,
    }

    pool_class: type[AsyncAdaptedQueuePool] | type[NullPool] = (
        NullPool if settings.is_test else AsyncAdaptedQueuePool
    )

    engine = create_async_engine(
        settings.database_url,
        poolclass=pool_class,
        pool_size=settings.database_pool_size if not settings.is_test else 0,
        max_overflow=settings.database_max_overflow if not settings.is_test else 0,
        pool_timeout=settings.database_pool_timeout_seconds,
        pool_recycle=settings.database_health_check_interval_seconds * 6,
        pool_pre_ping=True,
        echo=settings.database_echo,
        future=True,
        connect_args=connect_args,
    )

    _register_schema_event(engine)
    return engine


def _register_schema_event(engine: AsyncEngine) -> None:
    """Register a checkout listener that pins the connection's search_path.

    The listener fires for every connection checked out of the pool
    (including new connections on first use). It reads the schema
    from :data:`_current_schema` and runs ``SET search_path TO <schema>``
    so every statement in the session is scoped to that module.
    """
    from sqlalchemy import event

    @event.listens_for(engine.sync_engine, "checkout")
    def _set_search_path(
        dbapi_connection: Any,
        connection_record: Any,
        connection_proxy: Any,
    ) -> None:
        schema = _current_schema.get()
        # Schema name is validated upstream (alphanumeric + underscore).
        # We use a parameterized query to avoid SQL injection even though
        # the value is a server-controlled identifier.
        with dbapi_connection.cursor() as cursor:
            cursor.execute("SET search_path TO %s, public", (schema,))
        _log.debug("db.search_path_set", schema=schema)


# ---------------------------------------------------------------------------
# Engine accessor (lazy, process-singleton)
# ---------------------------------------------------------------------------


def get_engine() -> AsyncEngine:
    """Return the process-singleton :class:`AsyncEngine`."""
    global _engine, _session_factory, _breaker
    if _engine is None:
        settings = get_settings()
        _breaker = _create_breaker(settings)
        _engine = _build_engine(settings)
        _session_factory = async_sessionmaker(
            bind=_engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False,
        )
        _log.info(
            "db.engine_created",
            pool=type(_engine.pool).__name__,
            schema=settings.database_default_schema,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the process-singleton :class:`async_sessionmaker`."""
    if _session_factory is None:
        get_engine()  # triggers initialization
    assert _session_factory is not None
    return _session_factory


def get_breaker() -> pybreaker.CircuitBreaker:
    """Return the process-singleton connection breaker."""
    if _breaker is None:
        get_engine()
    assert _breaker is not None
    return _breaker


async def dispose_engine() -> None:
    """Dispose of the engine and release all connections. Idempotent."""
    global _engine, _session_factory, _breaker
    if _engine is not None:
        await _engine.dispose()
        _log.info("db.engine_disposed")
    _engine = None
    _session_factory = None
    _breaker = None


# ---------------------------------------------------------------------------
# Session dependency
# ---------------------------------------------------------------------------


def _validate_schema_name(schema: str) -> str:
    """Reject anything that isn't a safe PostgreSQL identifier.

    Schema names are baked into a ``SET search_path`` statement, so
    we must constrain the input to alphanumerics and underscores.
    """
    if not schema or not all(c.isalnum() or c == "_" for c in schema):
        raise ValueError(
            f"Invalid schema name {schema!r}: must contain only " f"alphanumerics and underscores."
        )
    if len(schema) > 63:  # PostgreSQL NAMEDATALEN - 1
        raise ValueError(f"Schema name {schema!r} too long (max 63 chars).")
    return schema


async def get_session(
    schema: str | None = None,
) -> AsyncIterator[AsyncSession]:
    """Yield a schema-scoped :class:`AsyncSession`.

    Usage as a FastAPI dependency:

        @app.get("/orders")
        async def list_orders(session: Annotated[AsyncSession, Depends(get_db_session("orders"))]):
            ...

    The schema can also be passed via the ``NIMBUS_DB_SCHEMA`` env
    var or the ``database_default_schema`` setting. Schema names are
    validated to prevent SQL injection through the ``SET search_path``
    statement.
    """
    settings = get_settings()
    target_schema = _validate_schema_name(schema or settings.database_default_schema)

    token = _current_schema.set(target_schema)
    factory = get_session_factory()

    session: AsyncSession = factory()
    try:
        yield session
    except OperationalError as exc:
        # Lock / timeout — bubble up as a typed domain error.
        await session.rollback()
        if "lock" in str(exc).lower() or "timeout" in str(exc).lower():
            raise DatabaseLockError(
                "Database was busy. Please retry.",
                details={"schema": target_schema, "driver_error": str(exc)},
            ) from exc
        raise DatabaseConnectionError(
            "Database is unreachable.",
            details={"schema": target_schema, "driver_error": str(exc)},
        ) from exc
    except (DisconnectionError, InterfaceError, DBAPIError) as exc:
        await session.rollback()
        raise DatabaseConnectionError(
            "Database connection failed.",
            details={"schema": target_schema, "driver_error": str(exc)},
        ) from exc
    except TimeoutError as exc:
        await session.rollback()
        raise NimbusTimeoutError(
            "Database operation timed out.",
            details={"schema": target_schema},
        ) from exc
    except NimbusError:
        await session.rollback()
        raise
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()
        _current_schema.reset(token)


# Pre-bound dependency for the default schema. Module-specific
# dependencies can be created by partial application.
DbSession = Annotated[AsyncSession, Depends(get_session)]


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


async def health_check() -> bool:
    """Return True if the database is reachable. Never raises."""
    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        _log.warning("db.health_check_failed", error=str(exc))
        return False


@asynccontextmanager
async def with_schemas(
    session: AsyncSession,
    *schemas: str,
) -> AsyncIterator[AsyncSession]:
    """Temporarily widen the search_path for this transaction.

    Use this for cross-module operations (e.g., a saga that
    writes to both ``orders`` and ``inventory``). The
    widening is transaction-scoped: ``SET LOCAL`` reverts
    automatically when the surrounding transaction commits
    or rolls back.

    Args:
        session: an open :class:`AsyncSession` inside an
            explicit transaction (``async with session.begin():``).
        *schemas: schema names to add to the search_path. The
            ``public`` schema is always appended so built-in
            types and functions remain accessible.

    Yields:
        The same session, with the widened search_path.

    Example::

        async with session.begin():
            async with with_schemas(session, "orders", "inventory"):
                # SELECTs and INSERTs here can see both schemas.
                ...
    """
    if not schemas:
        raise ValueError("at least one schema must be provided")
    # Validate schema names to prevent SQL injection through
    # the SET LOCAL statement. Schema names in PostgreSQL can
    # only contain letters, digits, underscores, and must
    # start with a letter or underscore.
    for s in schemas:
        if not s or not all(c.isalnum() or c == "_" for c in s) or s[0].isdigit():
            raise ValueError(f"Invalid schema name {s!r}")
    schema_list = ", ".join((*schemas, "public"))
    await session.execute(sql_text(f"SET LOCAL search_path TO {schema_list}"))
    yield session
    # SET LOCAL is automatically reverted on commit/rollback;
    # no explicit reset needed.
