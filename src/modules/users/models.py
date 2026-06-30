"""SQLAlchemy models for the Users module."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    MetaData,
    String,
)
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

USERS_SCHEMA = "users"


class Base(DeclarativeBase):
    """Declarative base for the Users module."""

    metadata = MetaData(schema=USERS_SCHEMA)


class User(Base):
    """A registered user of the platform.

    Email is stored lowercased; the application lowercases on
    write so the database is the canonical source of truth for
    case-insensitive uniqueness.
    """

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    email: Mapped[str] = mapped_column(
        String(length=255),
        nullable=False,
        unique=True,
        index=True,
    )

    password_hash: Mapped[str] = mapped_column(
        String(length=255),
        nullable=False,
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
        CheckConstraint(
            "length(email) >= 3 AND length(email) <= 254",
            name="ck_users_email_length",
        ),
        CheckConstraint(
            "email = lower(email)",
            name="ck_users_email_lowercased",
        ),
    )

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email}>"
