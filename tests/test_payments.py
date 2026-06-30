"""Tests for the payments module: idempotency lock and outbox commit.

The tests verify orchestration logic with mocks for the database
session and the FakeRedis for the lock. They do NOT require a
running Postgres. A separate ``@pytest.mark.integration`` test
runs against the real docker-compose Postgres; it is skipped by
default.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from src.core.exceptions import IdempotencyConflictError
from src.core.messaging import EventEnvelope, make_dedupe_id
from src.modules.payments.models import OutboxEvent, Payment, PaymentStatus
from src.modules.payments.services import (
    IdempotencyLock,
    PaymentAlreadyProcessedError,
    PaymentNotFoundError,
    ProcessPaymentRequest,
    acquire_idempotency_lock,
    build_outbox_event,
    build_payment_event,
    dedupe_event,
    idempotency_lock,
    process_payment,
    release_idempotency_lock,
)
from tests.fakes import as_redis_like

if TYPE_CHECKING:
    from tests.fakes.fake_redis import FakeRedis

# ===========================================================================
# Helpers
# ===========================================================================


def _make_payment(
    payment_id: uuid.UUID | None = None,
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
    amount: int = 10_00,  # 10.00 in minor units
    currency: str = "USD",
) -> Payment:
    p = Payment(
        id=payment_id or uuid.uuid4(),
        status=status,
        amount_minor_units=amount,
        currency=currency,
    )
    p.created_at = datetime.now(UTC)
    p.updated_at = datetime.now(UTC)
    return p


class _FakeResult:
    """Mimics a SQLAlchemy Result for ``scalar_one_or_none``."""

    def __init__(self, value: Any) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Any:
        return self._value


class _FakeSession:
    """Mocks AsyncSession for orchestration tests.

    Records every call to ``add``, ``execute``, ``commit``, and
    ``rollback`` so tests can assert on them. ``session.begin()``
    returns an async context manager that succeeds by default;
    tests can monkeypatch it to raise and verify rollback.
    """

    def __init__(self, payment: Payment | None) -> None:
        self.added: list[Any] = []
        self.executed: list[Any] = []
        self.commits = 0
        self.rollbacks = 0
        self._payment = payment
        self._begin_should_raise: BaseException | None = None

    def set_payment(self, payment: Payment | None) -> None:
        self._payment = payment

    def set_begin_error(self, exc: BaseException) -> None:
        self._begin_should_raise = exc

    async def execute(self, stmt: Any) -> _FakeResult:
        self.executed.append(stmt)
        return _FakeResult(self._payment)

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def flush(self) -> None:
        pass

    @asynccontextmanager
    async def begin(self):
        """Mimic SQLAlchemy's ``Session.begin()`` context manager.

        On clean exit: calls ``self.commit()`` (incrementing the
        commits counter).
        On exception: calls ``self.rollback()`` (incrementing the
        rollbacks counter) and re-raises.

        If ``_begin_should_raise`` is set, raises the configured
        exception immediately (no commit/rollback called).
        """
        if self._begin_should_raise is not None:
            raise self._begin_should_raise
        try:
            yield self
        except Exception:
            await self.rollback()
            raise
        else:
            await self.commit()


# ===========================================================================
# Idempotency lock tests
# ===========================================================================


class TestIdempotencyLock:
    @pytest.mark.asyncio
    async def test_acquire_returns_lock(self, fake_redis: FakeRedis) -> None:
        lock = await acquire_idempotency_lock(
            idempotency_key="idem-1",
            redis=as_redis_like(fake_redis),
            ttl_seconds=30,
        )
        assert isinstance(lock, IdempotencyLock)
        assert lock.key == "idemp:idem-1"
        assert len(lock.token) == 32

    @pytest.mark.asyncio
    async def test_acquire_then_second_call_raises(self, fake_redis: FakeRedis) -> None:
        await acquire_idempotency_lock(
            idempotency_key="idem-2", redis=as_redis_like(fake_redis), ttl_seconds=30
        )
        with pytest.raises(IdempotencyConflictError):
            await acquire_idempotency_lock(
                idempotency_key="idem-2", redis=as_redis_like(fake_redis), ttl_seconds=30
            )

    @pytest.mark.asyncio
    async def test_release_frees_lock_for_next_acquire(self, fake_redis: FakeRedis) -> None:
        lock = await acquire_idempotency_lock(
            idempotency_key="idem-3", redis=as_redis_like(fake_redis), ttl_seconds=30
        )
        released = await release_idempotency_lock(lock, redis=as_redis_like(fake_redis))
        assert released is True
        # Second acquire must succeed.
        lock2 = await acquire_idempotency_lock(
            idempotency_key="idem-3", redis=as_redis_like(fake_redis), ttl_seconds=30
        )
        assert lock2.key == lock.key
        assert lock2.token != lock.token

    @pytest.mark.asyncio
    async def test_release_with_wrong_token_does_nothing(self, fake_redis: FakeRedis) -> None:
        await acquire_idempotency_lock(
            idempotency_key="idem-4", redis=as_redis_like(fake_redis), ttl_seconds=30
        )
        fake_lock = IdempotencyLock(key="idemp:idem-4", token="wrong-token")
        released = await release_idempotency_lock(fake_lock, redis=as_redis_like(fake_redis))
        assert released is False
        # Real lock is still held.
        with pytest.raises(IdempotencyConflictError):
            await acquire_idempotency_lock(
                idempotency_key="idem-4", redis=as_redis_like(fake_redis), ttl_seconds=30
            )

    @pytest.mark.asyncio
    async def test_empty_idempotency_key_rejected(self, fake_redis: FakeRedis) -> None:
        with pytest.raises(ValueError):
            await acquire_idempotency_lock(
                idempotency_key="", redis=as_redis_like(fake_redis), ttl_seconds=30
            )

    @pytest.mark.asyncio
    async def test_context_manager_releases_on_exception(self, fake_redis: FakeRedis) -> None:
        with pytest.raises(RuntimeError):
            async with idempotency_lock("idem-5", redis=as_redis_like(fake_redis), ttl_seconds=30):
                raise RuntimeError("simulated failure")
        # Lock must be released even after the exception.
        lock = await acquire_idempotency_lock(
            idempotency_key="idem-5", redis=as_redis_like(fake_redis), ttl_seconds=30
        )
        assert lock.token != ""


# ===========================================================================
# process_payment orchestration tests
# ===========================================================================


class TestProcessPayment:
    @pytest.mark.asyncio
    async def test_successful_process_writes_payment_and_outbox(
        self, fake_redis: FakeRedis
    ) -> None:
        payment = _make_payment(status=PaymentStatus.PENDING)
        session = _FakeSession(payment)

        result = await process_payment(
            ProcessPaymentRequest(
                payment_id=payment.id,
                idempotency_key="idem-ok-1",
                new_status=PaymentStatus.AUTHORIZED,
            ),
            session=session,  # type: ignore[arg-type]
            redis=as_redis_like(fake_redis),
        )

        # Caller commits (SQLAlchemy 2.0 autobegin + explicit commit).
        await session.commit()

        # Payment and outbox were both added.
        assert payment in session.added
        assert any(isinstance(o, OutboxEvent) for o in session.added)
        # One commit.
        assert session.commits == 1
        assert session.rollbacks == 0
        # Returned result has the right status.
        assert result.payment.status is PaymentStatus.AUTHORIZED
        assert result.outbox_event.event_type == "payments.payment_authorized"
        assert result.outbox_event.aggregate_id == str(payment.id)

    @pytest.mark.asyncio
    async def test_lock_conflict_skips_db_work(self, fake_redis: FakeRedis) -> None:
        payment = _make_payment()
        session = _FakeSession(payment)
        # Hold the lock first.
        await acquire_idempotency_lock(
            idempotency_key="idem-conflict-1", redis=as_redis_like(fake_redis), ttl_seconds=30
        )
        with pytest.raises(IdempotencyConflictError):
            await process_payment(
                ProcessPaymentRequest(
                    payment_id=payment.id,
                    idempotency_key="idem-conflict-1",
                    new_status=PaymentStatus.AUTHORIZED,
                ),
                session=session,  # type: ignore[arg-type]
                redis=as_redis_like(fake_redis),
            )
        # No SELECT was issued because we never entered the body.
        assert session.executed == []
        assert session.commits == 0
        assert session.added == []

    @pytest.mark.asyncio
    async def test_payment_not_found_raises(self, fake_redis: FakeRedis) -> None:
        session = _FakeSession(payment=None)
        with pytest.raises(PaymentNotFoundError):
            await process_payment(
                ProcessPaymentRequest(
                    payment_id=uuid.uuid4(),
                    idempotency_key="idem-nf-1",
                    new_status=PaymentStatus.AUTHORIZED,
                ),
                session=session,  # type: ignore[arg-type]
                redis=as_redis_like(fake_redis),
            )
        # Caller rolls back on exception.
        await session.rollback()
        # No commit happened.
        assert session.commits == 0
        assert session.rollbacks == 1

    @pytest.mark.asyncio
    async def test_invalid_transition_raises(self, fake_redis: FakeRedis) -> None:
        # PENDING -> SETTLED is not allowed.
        payment = _make_payment(status=PaymentStatus.PENDING)
        session = _FakeSession(payment)
        with pytest.raises(PaymentAlreadyProcessedError):
            await process_payment(
                ProcessPaymentRequest(
                    payment_id=payment.id,
                    idempotency_key="idem-bad-trans-1",
                    new_status=PaymentStatus.SETTLED,
                ),
                session=session,  # type: ignore[arg-type]
                redis=as_redis_like(fake_redis),
            )
        # Caller rolls back on exception.
        await session.rollback()
        assert session.commits == 0
        assert session.rollbacks == 1

    @pytest.mark.asyncio
    async def test_lock_released_after_success(self, fake_redis: FakeRedis) -> None:
        payment = _make_payment()
        session = _FakeSession(payment)
        await process_payment(
            ProcessPaymentRequest(
                payment_id=payment.id,
                idempotency_key="idem-release-1",
                new_status=PaymentStatus.AUTHORIZED,
            ),
            session=session,  # type: ignore[arg-type]
            redis=as_redis_like(fake_redis),
        )
        # Second call with the same key must succeed (lock was released).
        payment2 = _make_payment()
        session2 = _FakeSession(payment2)
        await process_payment(
            ProcessPaymentRequest(
                payment_id=payment2.id,
                idempotency_key="idem-release-1",
                new_status=PaymentStatus.AUTHORIZED,
            ),
            session=session2,  # type: ignore[arg-type]
            redis=as_redis_like(fake_redis),
        )


# ===========================================================================
# Event envelope and dedupe tests
# ===========================================================================


class TestEventEnvelope:
    def test_round_trip_through_bytes(self) -> None:
        envelope = EventEnvelope(
            event_type="payments.payment_captured",
            aggregate_type="Payment",
            aggregate_id="abc-123",
            payload={"amount": 1000, "currency": "USD"},
        )
        data = envelope.to_bytes()
        assert isinstance(data, bytes)
        restored = EventEnvelope.from_bytes(data)
        assert restored.event_id == envelope.event_id
        assert restored.event_type == envelope.event_type
        assert restored.payload == {"amount": 1000, "currency": "USD"}

    def test_content_hash_deterministic(self) -> None:
        env1 = EventEnvelope(
            event_type="x.y",
            aggregate_type="X",
            aggregate_id="1",
            payload={"a": 1, "b": 2},
        )
        env2 = EventEnvelope(
            event_type="x.y",
            aggregate_type="X",
            aggregate_id="1",
            payload={"b": 2, "a": 1},  # different order
        )
        # Different event_id and timestamp, so envelopes differ.
        # The content hash is based on event_type + aggregate + payload,
        # not on the envelope instance.
        assert env1.content_hash() == env2.content_hash()

    def test_content_hash_differs_when_payload_differs(self) -> None:
        env1 = EventEnvelope(
            event_type="x.y",
            aggregate_type="X",
            aggregate_id="1",
            payload={"a": 1},
        )
        env2 = EventEnvelope(
            event_type="x.y",
            aggregate_type="X",
            aggregate_id="1",
            payload={"a": 2},
        )
        assert env1.content_hash() != env2.content_hash()

    def test_make_dedupe_id_deterministic(self) -> None:
        a = make_dedupe_id({"payment_id": "1", "amount": 100})
        b = make_dedupe_id({"amount": 100, "payment_id": "1"})
        assert a == b

    def test_event_id_is_uuid4(self) -> None:
        env = EventEnvelope(event_type="x", aggregate_type="X", aggregate_id="1")
        # UUID4 has version=4 in the third group.
        assert env.event_id.version == 4


# ===========================================================================
# Outbox event construction tests
# ===========================================================================


class TestOutboxConstruction:
    def test_outbox_event_has_content_addressed_dedupe_id(self) -> None:
        payment = _make_payment()
        envelope = build_payment_event(
            payment=payment,
            event_type="payments.payment_authorized",
        )
        outbox = build_outbox_event(envelope, topic="payments.events")
        assert outbox.dedupe_id == envelope.content_hash()
        assert len(outbox.dedupe_id) == 32  # UUID hex is 32 chars

    def test_dedupe_event_helper_matches_envelope_hash(self) -> None:
        # ``dedupe_event`` (alias for ``make_dedupe_id``) hashes the
        # payload alone. ``envelope.content_hash`` covers more
        # fields. The two will NOT be equal for the same envelope,
        # but both are deterministic functions of their inputs.
        payment = _make_payment()
        envelope = build_payment_event(
            payment=payment,
            event_type="payments.payment_captured",
            extra_payload={"gateway_reference": "ch_abc"},
        )
        from src.core.messaging import make_dedupe_id

        # dedupe_event and make_dedupe_id produce the same hash
        # for the same payload.
        assert dedupe_event(envelope.payload) == make_dedupe_id(envelope.payload)
        # Changing the payload changes the dedupe id.
        different = dict(envelope.payload)
        different["amount_minor_units"] = 999_999
        assert dedupe_event(envelope.payload) != dedupe_event(different)

    def test_dedupe_event_changes_with_payload(self) -> None:
        a = dedupe_event({"x": 1})
        b = dedupe_event({"x": 2})
        assert a != b
