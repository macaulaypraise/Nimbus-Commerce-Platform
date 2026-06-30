"""SQLAlchemy models for the Orders module."""

from __future__ import annotations

import enum
import uuid
from datetime import UTC, datetime

from sqlalchemy import CheckConstraint, DateTime, MetaData, String
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

ORDERS_SCHEMA = "orders"


class Base(DeclarativeBase):
    metadata = MetaData(schema=ORDERS_SCHEMA)


class OrderStatus(str, enum.Enum):
    """Order lifecycle.

    Transitions:
        RESERVED -> PAID   (on payment_captured)
        RESERVED -> FAILED (on payment_failed; inventory released)
        PAID -> SHIPPED    (future slice; out of scope here)
    """

    PENDING = "pending"
    RESERVED = "reserved"
    PAID = "paid"
    FAILED = "failed"
    SHIPPED = "shipped"


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    # We store stock_id and quantity so the payment_failed
    # handler can call inventory.release without re-fetching
    # the order's line items (which don't exist as a separate
    # model in this slice).
    stock_id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    quantity: Mapped[int] = mapped_column(nullable=False)
    expected_version: Mapped[int] = mapped_column(
        nullable=False,
        comment="The stock version at the time of reservation. "
        "Used as the expected_version for release on payment_failed.",
    )
    total_amount_minor_units: Mapped[int] = mapped_column(nullable=False)
    currency: Mapped[str] = mapped_column(String(length=3), nullable=False)
    status: Mapped[OrderStatus] = mapped_column(
        SAEnum(
            OrderStatus,
            name="order_status",
            values_callable=lambda e: [m.value for m in e],
            native_enum=False,
            length=32,
        ),
        nullable=False,
        default=OrderStatus.RESERVED,
        index=True,
    )
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
        CheckConstraint("quantity > 0", name="ck_orders_quantity_positive"),
        CheckConstraint(
            "total_amount_minor_units >= 0",
            name="ck_orders_total_non_negative",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<Order id={self.id} user_id={self.user_id} status={self.status.value} "
            f"total={self.total_amount_minor_units} {self.currency}>"
        )
