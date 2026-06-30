"""SQLAlchemy models for the Inventory module."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import CheckConstraint, DateTime, MetaData, String
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

INVENTORY_SCHEMA = "inventory"


class Base(DeclarativeBase):
    """Declarative base for the Inventory module.

    Bound to the ``inventory`` schema so every emitted DDL
    is namespaced correctly.
    """

    metadata = MetaData(schema=INVENTORY_SCHEMA)


class Stock(Base):
    """Inventory for a single product with optimistic locking.

    The ``version`` column is the optimistic-lock counter.
    Every successful ``UPDATE`` increments it by one. A
    concurrent update that sees a stale version fails the
    ``WHERE version = ?`` clause atomically and is reported
    back as :class:`ConcurrentUpdateError`.
    """

    __tablename__ = "stock"

    # --- Identity -------------------------------------------------------
    id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    # --- Catalog --------------------------------------------------------
    sku: Mapped[str] = mapped_column(
        String(length=64),
        nullable=False,
        unique=True,
        index=True,
        comment="Stock Keeping Unit. Unique product code.",
    )
    name: Mapped[str] = mapped_column(
        String(length=255),
        nullable=False,
    )
    price_minor_units: Mapped[int] = mapped_column(
        nullable=False,
        comment="Price in minor units (e.g., cents) to avoid float drift.",
    )
    currency: Mapped[str] = mapped_column(
        String(length=3),
        nullable=False,
    )

    # --- Inventory state ------------------------------------------------
    available: Mapped[int] = mapped_column(
        nullable=False,
        default=0,
        comment="Quantity currently available for reservation.",
    )
    version: Mapped[int] = mapped_column(
        nullable=False,
        default=0,
        comment="Optimistic-lock counter. Incremented on every successful UPDATE.",
    )

    # --- Audit ----------------------------------------------------------
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        CheckConstraint("available >= 0", name="ck_stock_available_non_negative"),
        CheckConstraint("price_minor_units >= 0", name="ck_stock_price_non_negative"),
        CheckConstraint("version >= 0", name="ck_stock_version_non_negative"),
    )

    def __repr__(self) -> str:
        return (
            f"<Stock id={self.id} sku={self.sku} available={self.available} "
            f"version={self.version}>"
        )
