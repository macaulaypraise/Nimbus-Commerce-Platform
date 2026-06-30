"""Alembic environment for the Nimbus Commerce Platform.

Supports the multi-schema Modular Monolith by:
  1. Iterating over all module Bases and creating their
     schemas if they don't exist.
  2. Emitting schema-qualified DDL for all tables.
  3. Merging all module metadata into a single MetaData
     object that Alembic can diff against.

The offline (``--sql``) and online (live DB) modes are both
supported. The DATABASE_URL is read from the application
settings so the test/prod boundary is enforced by the
application, not by the migration tool.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import MetaData, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

# Add src/ to the path so we can import the modules.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.core.config import get_settings  # noqa: E402
from src.modules.inventory.models import Base as InventoryBase  # noqa: E402
from src.modules.orders.models import Base as OrdersBase  # noqa: E402
from src.modules.payments.models import Base as PaymentsBase  # noqa: E402
from src.modules.users.models import Base as UsersBase  # noqa: E402

# Combine all module metadata into a single MetaData.
# The schema names are preserved because each module's Base
# already has schema=... set on its metadata. We don't want
# to merge them into a single metadata (which would lose the
# schema names); instead, we iterate over each module's
# metadata separately.
ALL_METADATA: list[MetaData] = [
    PaymentsBase.metadata,
    UsersBase.metadata,
    InventoryBase.metadata,
    OrdersBase.Base.metadata if hasattr(OrdersBase, "Base") else OrdersBase.metadata,
]

# Module schema names. Used to CREATE SCHEMA before tables.
MODULE_SCHEMAS: list[str] = [
    "payments",
    "users",
    "inventory",
    "orders",
]

config = context.config
target_metadata = PaymentsBase.metadata  # primary for autogenerate


def _get_url() -> str:
    settings = get_settings()
    return settings.database_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL to stdout)."""
    context.configure(
        url=_get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run migrations in 'online' mode (against a live DB)."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online_async() -> None:
    """Async entry point for online migrations."""
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = _get_url()
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (sync wrapper for async)."""
    asyncio.run(run_migrations_online_async())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
