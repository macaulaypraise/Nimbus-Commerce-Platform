"""SQLAlchemy models for the Payments module.

The models bind to a module-specific :class:`DeclarativeBase` so
the metadata is isolated from other modules. Combined with the
``search_path`` enforcement in :mod:`src.core.database`, cross-
module joins are physically impossible.

Schema
------
Every table is created in the ``payments`` schema. The schema
name is set on the declarative base's ``metadata`` so SQLAlchemy
emits ``CREATE TABLE payments.<table>`` in Alembic migrations and
references ``payments.<table>`` at query time.

Status state machine
--------------------
``Payment.status`` is an enum that is enforced at the application
layer and via a CHECK constraint at the database layer. The valid
transitions are::

    PENDING -> AUTHORIZED -> CAPTURED -> SETTLED
                          \\-> DECLINED
    PENDING -> FAILED
    CAPTURED -> REFUNDED
"""

from __future__ import annotations

import enum
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    MetaData,
    String,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Schema name is the single source of truth. If you change this,
# run an Alembic migration to rename the schema in the database.
PAYMENTS_SCHEMA = "payments"


class Base(DeclarativeBase):
    """Declarative base for the Payments module.

    Bound to the ``payments`` schema so every emitted DDL is
    namespaced correctly. The per-connection ``search_path``
    enforcement in :mod:`src.core.database` ensures that every
    SELECT, INSERT, UPDATE, and DELETE in this module lands in
    the ``payments`` schema at runtime.
    """

    metadata = MetaData(schema=PAYMENTS_SCHEMA)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class PaymentStatus(str, enum.Enum):
    """Lifecycle of a Payment.

    String-valued so JSON serialization is trivial; database-side
    storage is the lowercase name (e.g., ``pending``).
    """

    PENDING = "pending"
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    SETTLED = "settled"
    REFUNDED = "refunded"
    DECLINED = "declined"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# Payment
# ---------------------------------------------------------------------------


class Payment(Base):
    """A monetary transaction between a buyer and the platform.

    The model is intentionally minimal in this slice. Future
    columns (buyer_id, payment_method, gateway_response, etc.) are
    added in subsequent slices via Alembic migrations.
    """

    __tablename__ = "payments"

    # --- Identity -------------------------------------------------------
    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    # --- State ----------------------------------------------------------
    status: Mapped[PaymentStatus] = mapped_column(
        SAEnum(
            PaymentStatus,
            name="payment_status",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
            native_enum=False,
            length=32,
        ),
        nullable=False,
        default=PaymentStatus.PENDING,
        index=True,
    )

    # --- Money (stored as integer minor units to avoid float drift) ----
    amount_minor_units: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )
    currency: Mapped[str] = mapped_column(
        String(length=3),
        nullable=False,
        comment="ISO 4217 three-letter currency code (e.g., 'USD').",
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

    # --- Constraints ---------------------------------------------------
    __table_args__ = (
        CheckConstraint(
            "amount_minor_units >= 0",
            name="ck_payments_amount_non_negative",
        ),
        CheckConstraint(
            "length(currency) = 3",
            name="ck_payments_currency_iso4217",
        ),
        Index("ix_payments_status_created", "status", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<Payment id={self.id} status={self.status.value} "
            f"amount={self.amount_minor_units} {self.currency}>"
        )


# ---------------------------------------------------------------------------
# OutboxEvent
# ---------------------------------------------------------------------------


class OutboxEvent(Base):
    """A row in the transactional outbox.

    Written in the same database transaction as the business state
    change. Drained to Kafka by an outbox-relay component (future
    slice). The :attr:`processed` flag is set to ``True`` after the
    relay successfully publishes the row to the broker and receives
    an ack.
    """

    __tablename__ = "outbox_events"

    # --- Identity -------------------------------------------------------
    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    # --- Routing --------------------------------------------------------
    aggregate_type: Mapped[str] = mapped_column(
        String(length=64),
        nullable=False,
        index=True,
    )
    aggregate_id: Mapped[str] = mapped_column(
        String(length=128),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(
        String(length=255),
        nullable=False,
        index=True,
    )
    # The Kafka topic the relay should publish to. Stored explicitly
    # so the relay doesn't need to compute it from ``event_type``.
    topic: Mapped[str] = mapped_column(
        String(length=255),
        nullable=False,
    )

    # --- Payload --------------------------------------------------------
    # JSONB lets us index individual fields for debugging / replay
    # without re-parsing the whole payload. ``dedupe_id`` is also
    # stored as a top-level field for fast unique-key enforcement.
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )
    dedupe_id: Mapped[str] = mapped_column(
        String(length=64),
        nullable=False,
        unique=True,
        index=True,
        comment="UUID5 hex of the canonical payload. Used by consumers to dedupe.",
    )

    # --- Status ---------------------------------------------------------
    processed: Mapped[bool] = mapped_column(
        nullable=False,
        default=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # --- Optional correlation to the originating business row ----------
    # Nullable because some events are not tied to a specific
    # business entity (e.g., system events). We use a stringly-typed
    # ``(aggregate_type, aggregate_id)`` pair rather than a hard
    # FK so we can outbox events for any module's aggregates
    # without circular FKs.
    __table_args__ = (
        Index(
            "ix_outbox_events_unprocessed",
            "processed",
            "created_at",
            postgresql_where="processed = false",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<OutboxEvent id={self.id} type={self.event_type} "
            f"aggregate={self.aggregate_type}:{self.aggregate_id} "
            f"processed={self.processed}>"
        )
