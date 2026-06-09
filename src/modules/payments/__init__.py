"""Payments module.

Owns the ``payments`` PostgreSQL schema. Hosts the Payment aggregate,
the OutboxEvent log, and the idempotent ``process_payment`` service
that writes both atomically.

Public surface:
    * :class:`Payment` — the SQLAlchemy model for a payment.
    * :class:`OutboxEvent` — the SQLAlchemy model for outbox rows.
    * :func:`process_payment` — the idempotent service entry point.
    * :func:`dedupe_event` — content-addressed event dedupe helper.
"""

from __future__ import annotations

from src.modules.payments.models import (
    Base,
    OutboxEvent,
    Payment,
    PaymentStatus,
)
from src.modules.payments.services import (
    IdempotencyLock,
    PaymentAlreadyProcessedError,
    PaymentNotFoundError,
    dedupe_event,
    process_payment,
)

__all__ = [
    "Base",
    "IdempotencyLock",
    "OutboxEvent",
    "Payment",
    "PaymentAlreadyProcessedError",
    "PaymentNotFoundError",
    "PaymentStatus",
    "dedupe_event",
    "process_payment",
]
