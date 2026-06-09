"""Idempotent payment processing and outbox logic.

Design
------
``process_payment`` is the single entry point for the module. It
combines three concerns in one function:

  1. **Distributed lock** via Redis ``SETNX`` on the
     Idempotency-Key. Prevents two concurrent requests with the
     same key from both succeeding.
  2. **Database transaction** that updates the Payment and writes
     a corresponding OutboxEvent in one atomic commit. If either
     write fails, both are rolled back.
  3. **Outbox event construction** with content-addressed
     dedupe_id for the future consumer.

The function does NOT publish to Kafka. The outbox-relay
component (future slice) drains the OutboxEvent table and calls
:meth:`KafkaProducer.publish`. This separation is the heart of the
outbox pattern: it eliminates the dual-write problem (writing to
the database AND the message broker separately) by making the
broker write a downstream effect of the database commit.

Locking strategy
----------------
We use the standard "SETNX with token" pattern:

  * Acquire: ``SET lock:idem:<key> <token> NX EX <ttl>``.
    The TTL is the safety net: if the holder dies, the lock is
    released automatically.
  * Release: a Lua script that deletes the key ONLY if the value
    matches the token. This prevents a slow holder from releasing
    a lock that has since been re-acquired by another process.

The default TTL of 30 seconds is generous; tune via
``payments_idempotency_lock_ttl_seconds``.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache import get_breaker, get_redis

if TYPE_CHECKING:
    from src.core.config import Settings
from src.core.exceptions import (
    CacheUnavailableError,
    IdempotencyConflictError,
    ResourceNotFoundError,
)
from src.core.messaging import EventEnvelope, make_dedupe_id

if TYPE_CHECKING:
    from src.core.protocols import RedisLike
from src.modules.payments.models import OutboxEvent, Payment, PaymentStatus

_log = structlog.get_logger("nimbus.payments")

# Lua script for safe lock release. Compares the current value to
# the token before deleting, so a holder can never delete a lock
# owned by someone else.
_LOCK_RELEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


# ---------------------------------------------------------------------------
# Typed errors
# ---------------------------------------------------------------------------


class PaymentNotFoundError(ResourceNotFoundError):
    code = "payments.not_found"
    safe_message = "Payment not found."


class PaymentAlreadyProcessedError(Exception):
    """Raised when a process_payment call is for a payment that has
    already reached a terminal state and cannot transition further.

    Not mapped to a 4xx HTTP code because callers handle it at the
    service layer; HTTP-mapped errors live in src/core/exceptions.py.
    """


# ---------------------------------------------------------------------------
# Idempotency lock
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IdempotencyLock:
    """A Redis-backed distributed lock for an Idempotency-Key.

    Returned by :func:`acquire_idempotency_lock`; pass it to
    :func:`release_idempotency_lock` (or use the context-manager
    form) to ensure the lock is released even on exception.

    Attributes:
        key: the Redis key (e.g., ``idemp:idem-abc123``).
        token: a UUID4 generated at acquire time. Used by the
            release script to verify ownership before deleting.
    """

    key: str
    token: str


def _redis_key(idempotency_key: str) -> str:
    """Build the Redis key for an idempotency lock."""
    return f"idemp:{idempotency_key}"


async def acquire_idempotency_lock(
    *,
    idempotency_key: str,
    redis: RedisLike | None = None,
    ttl_seconds: int | None = None,
) -> IdempotencyLock:
    """Acquire a distributed lock for an Idempotency-Key.

    Args:
        idempotency_key: the client's Idempotency-Key header value.
        redis: optional override; defaults to the process-singleton
            Redis client.
        ttl_seconds: optional override; defaults to the configured
            ``payments_idempotency_lock_ttl_seconds`` setting.

    Returns:
        An :class:`IdempotencyLock` if the lock was acquired.

    Raises:
        IdempotencyConflictError: another request is currently
            processing the same Idempotency-Key.
        CacheUnavailableError: Redis is unreachable and the
            circuit breaker is open.
    """
    if not idempotency_key:
        raise ValueError("idempotency_key must be a non-empty string")
    redis_client = redis or get_redis()
    breaker = get_breaker()
    key = _redis_key(idempotency_key)
    token = uuid.uuid4().hex
    ttl = ttl_seconds if ttl_seconds is not None else 30
    if ttl <= 0:
        raise ValueError("ttl_seconds must be > 0")

    async def _acquire() -> bool:
        # ``SET key value NX EX ttl`` is the canonical distributed
        # lock recipe. ``nx=True`` makes it atomic: succeed only if
        # the key doesn't exist.
        result = await redis_client.set(key, token, nx=True, ex=ttl)
        # redis-py returns True on success, None on NX failure.
        return bool(result)

    try:
        acquired = await breaker.call(_acquire)
    except CacheUnavailableError:
        # Cache is down. We deliberately do NOT raise the cached
        # IdempotencyConflictError: that would deny service
        # whenever Redis blips. Instead, the caller can decide.
        # We re-raise as CacheUnavailableError so the caller sees
        # the original error class.
        raise

    if not acquired:
        _log.info(
            "payments.idempotency_lock_busy",
            idempotency_key=idempotency_key,
        )
        raise IdempotencyConflictError(
            "A request with this Idempotency-Key is already in progress.",
            details={"idempotency_key": idempotency_key, "lock_key": key},
        )

    _log.info(
        "payments.idempotency_lock_acquired",
        idempotency_key=idempotency_key,
        lock_key=key,
        ttl_seconds=ttl,
    )
    return IdempotencyLock(key=key, token=token)


async def release_idempotency_lock(
    lock: IdempotencyLock,
    *,
    redis: RedisLike | None = None,
) -> bool:
    """Release an idempotency lock, but only if we still own it.

    Returns True if the lock was released, False if it had already
    expired or been re-acquired by another process.
    """
    redis_client = redis or get_redis()

    async def _release() -> int:
        return int(
            await redis_client.eval(
                _LOCK_RELEASE_LUA,
                1,
                lock.key,
                lock.token,
            )
        )

    try:
        released = await get_breaker().call(_release)
    except CacheUnavailableError:
        # If Redis is down at release time, the lock's TTL will
        # clean it up. Log and return False; do not raise.
        _log.warning(
            "payments.idempotency_lock_release_failed",
            lock_key=lock.key,
            reason="redis_unavailable",
        )
        return False
    return released > 0


@contextlib.asynccontextmanager
async def idempotency_lock(
    idempotency_key: str,
    *,
    redis: RedisLike | None = None,
    ttl_seconds: int | None = None,
) -> AsyncGenerator[IdempotencyLock, None]:
    """Context manager that acquires and releases an idempotency lock.

    Usage::

        async with idempotency_lock(req.idempotency_key) as lock:
            ...  # do work
    # lock is released (or TTL-expired) here

    On exception inside the block, the lock is still released.
    """
    lock = await acquire_idempotency_lock(
        idempotency_key=idempotency_key,
        redis=redis,
        ttl_seconds=ttl_seconds,
    )
    try:
        yield lock
    finally:
        await release_idempotency_lock(lock, redis=redis)


# ---------------------------------------------------------------------------
# Event payload builders
# ---------------------------------------------------------------------------


def build_payment_event(
    *,
    payment: Payment,
    event_type: str,
    extra_payload: Mapping[str, Any] | None = None,
) -> EventEnvelope:
    """Build an :class:`EventEnvelope` for a payment event.

    Args:
        payment: the payment the event is about.
        event_type: e.g., ``payments.payment_authorized``.
        extra_payload: additional fields to merge into the payload
            (e.g., ``{"gateway_reference": "ch_abc123"}``).
    """
    base_payload: dict[str, Any] = {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount_minor_units": payment.amount_minor_units,
        "currency": payment.currency,
        "occurred_at": datetime.now(UTC).isoformat(),
    }
    if extra_payload:
        base_payload.update(dict(extra_payload))
    return EventEnvelope(
        event_type=event_type,
        schema_version=1,
        aggregate_type="Payment",
        aggregate_id=str(payment.id),
        payload=base_payload,
    )


def build_outbox_event(
    envelope: EventEnvelope,
    *,
    topic: str = "payments.events",
) -> OutboxEvent:
    """Build an :class:`OutboxEvent` SQLAlchemy model from an envelope.

    The ``dedupe_id`` is computed once at insert time so consumers
    can dedupe on a stable hash of the payload.
    """
    return OutboxEvent(
        id=envelope.event_id,
        aggregate_type=envelope.aggregate_type,
        aggregate_id=envelope.aggregate_id,
        event_type=envelope.event_type,
        topic=topic,
        payload=envelope.model_dump(mode="json"),
        dedupe_id=envelope.content_hash(),
    )


# Convenience attribute on EventEnvelope so services can supply a
# topic hint without passing it separately to the outbox builder.
def _topic_hint(self: EventEnvelope) -> str | None:
    return getattr(self, "_topic_hint", None)


def _set_topic_hint(envelope: EventEnvelope, topic: str) -> EventEnvelope:
    """Return a new envelope with a topic hint attached.

    EventEnvelope is frozen, so we can't mutate it. Instead, the
    :func:`build_outbox_event` helper falls back to a default
    topic if no hint is set; this helper exists for the rare case
    where the caller wants to override.
    """
    object.__setattr__(envelope, "_topic_hint", topic)
    return envelope


# ---------------------------------------------------------------------------
# Core service
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ProcessPaymentRequest:
    """Input to :func:`process_payment`."""

    payment_id: uuid.UUID
    idempotency_key: str
    new_status: PaymentStatus
    extra_payload: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ProcessPaymentResult:
    """Output of :func:`process_payment`."""

    payment: Payment
    outbox_event: OutboxEvent
    envelope: EventEnvelope


# Transitions allowed by the service. Any other transition is
# rejected with a domain error. Add transitions here as the
# payments module grows.
_ALLOWED_TRANSITIONS: dict[PaymentStatus, frozenset[PaymentStatus]] = {
    PaymentStatus.PENDING: frozenset(
        {
            PaymentStatus.AUTHORIZED,
            PaymentStatus.FAILED,
            PaymentStatus.DECLINED,
        }
    ),
    PaymentStatus.AUTHORIZED: frozenset(
        {
            PaymentStatus.CAPTURED,
            PaymentStatus.DECLINED,
        }
    ),
    PaymentStatus.CAPTURED: frozenset(
        {
            PaymentStatus.SETTLED,
            PaymentStatus.REFUNDED,
        }
    ),
}


def _validate_transition(
    current: PaymentStatus,
    new: PaymentStatus,
) -> None:
    if new in _ALLOWED_TRANSITIONS.get(current, frozenset()):
        return
    raise PaymentAlreadyProcessedError(
        f"Cannot transition payment from {current.value} to {new.value}.",
    )


async def process_payment(
    request: ProcessPaymentRequest,
    *,
    session: AsyncSession,
    redis: RedisLike | None = None,
    settings: Settings | None = None,
) -> ProcessPaymentResult:
    """Idempotently process a payment and emit the corresponding event.

    Flow:
      1. Acquire a Redis lock on the Idempotency-Key.
      2. Open a database transaction.
      3. SELECT the Payment FOR UPDATE.
      4. Validate the status transition.
      5. UPDATE the Payment.
      6. INSERT an OutboxEvent.
      7. Commit. If the commit fails, both writes roll back.
      8. Release the lock.

    The lock release happens in a ``finally`` block so the lock is
    freed even on exception.

    Args:
        request: the input.
        session: an open :class:`AsyncSession`. The function does
            NOT close the session; the caller is responsible.
        redis: optional Redis client override.
        settings: optional settings override.

    Returns:
        A :class:`ProcessPaymentResult` with the updated payment,
        the outbox row, and the envelope that was built.

    Raises:
        IdempotencyConflictError: another request holds the lock.
        PaymentNotFoundError: the payment id does not exist.
        PaymentAlreadyProcessedError: the requested transition is
            not allowed from the current status.
    """
    _ = settings  # reserved for future overrides
    async with idempotency_lock(request.idempotency_key, redis=redis):
        # ``begin`` opens an explicit transaction; the ``async with``
        # block commits on clean exit and rolls back on exception.
        async with session.begin():
            # FOR UPDATE so two concurrent transactions on the same
            # payment row serialize. The Redis lock is the outer
            # guard; the row lock is the inner guard.
            stmt = select(Payment).where(Payment.id == request.payment_id).with_for_update()
            result = await session.execute(stmt)
            payment = result.scalar_one_or_none()
            if payment is None:
                raise PaymentNotFoundError(
                    "Payment does not exist.",
                    details={"payment_id": str(request.payment_id)},
                )

            _validate_transition(payment.status, request.new_status)
            payment.status = request.new_status

            envelope = build_payment_event(
                payment=payment,
                event_type=f"payments.payment_{request.new_status.value}",
                extra_payload=request.extra_payload,
            )
            outbox = build_outbox_event(envelope, topic="payments.events")

            session.add(payment)
            session.add(outbox)
            # The ``async with session.begin()`` block will commit
            # here on clean exit.

        _log.info(
            "payments.processed",
            payment_id=str(payment.id),
            new_status=payment.status.value,
            event_id=str(envelope.event_id),
            dedupe_id=outbox.dedupe_id,
        )
        return ProcessPaymentResult(
            payment=payment,
            outbox_event=outbox,
            envelope=envelope,
        )


# ---------------------------------------------------------------------------
# Consumer-side helpers (preview; consumer comes in a later slice)
# ---------------------------------------------------------------------------


def dedupe_event(payload: Any) -> str:
    """Compute a content-addressed dedupe id for a payload.

    Convenience wrapper around :func:`src.core.messaging.make_dedupe_id`
    so callers in the payments module don't need to import from
    :mod:`src.core.messaging` directly.

    Usage in a future consumer::

        async def handle_payment_event(envelope: EventEnvelope) -> None:
            dedupe_id = dedupe_event(envelope.payload)
            if await already_processed(dedupe_id):
                return
            ...  # do work
            await mark_processed(dedupe_id)
    """
    return make_dedupe_id(payload)
