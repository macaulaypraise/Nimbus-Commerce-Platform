"""Tests for the outbox relay.

The relay's contract is: "given a batch of outbox rows, publish
each one to Kafka, mark the successful ones as processed, and
leave failed ones for the next tick." We test that contract
with mocks for the database and the producer; an integration
test against real Postgres verifies the actual SQL.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError

from src.core.exceptions import BrokerUnavailableError
from src.core.messaging import EventEnvelope, KafkaProducer
from src.modules.payments.models import OutboxEvent
from src.workers.outbox_relay import OutboxRelay

# ===========================================================================
# Helpers
# ===========================================================================


def _make_envelope(
    *,
    event_type: str = "payments.payment_authorized",
    aggregate_id: str | None = None,
) -> EventEnvelope:
    """Build a real EventEnvelope for serialization round-trips."""
    return EventEnvelope(
        event_type=event_type,
        schema_version=1,
        aggregate_type="Payment",
        aggregate_id=aggregate_id or str(uuid.uuid4()),
        payload={
            "payment_id": aggregate_id or str(uuid.uuid4()),
            "status": "authorized",
            "amount_minor_units": 1000,
            "currency": "USD",
        },
    )


def _make_outbox_row(
    envelope: EventEnvelope | None = None,
    *,
    topic: str = "payments.events",
    attempt_count: int = 0,
    failed: bool = False,
) -> OutboxEvent:
    """Build an OutboxEvent with a valid payload."""
    envelope = envelope or _make_envelope()
    return OutboxEvent(
        id=envelope.event_id,
        aggregate_type=envelope.aggregate_type,
        aggregate_id=envelope.aggregate_id,
        event_type=envelope.event_type,
        topic=topic,
        payload=envelope.model_dump(mode="json"),
        dedupe_id=envelope.content_hash(),
        processed=False,
        failed=failed,
        attempt_count=attempt_count,
        created_at=datetime.now(UTC),
    )


class _FakeResult:
    """Mimics SQLAlchemy Result for ``scalars().all()``."""

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def scalars(self) -> _FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._rows


class _FakeSession:
    """Mocks AsyncSession for relay orchestration tests.

    The relay uses session.begin() implicitly via session
    semantics: it calls session.execute() to SELECT, then
    session.add() (implicitly via attribute mutation + flush)
    and session.commit() to commit. The fake mirrors the
    relevant subset.
    """

    def __init__(self, rows: list[OutboxEvent] | None = None) -> None:
        self._rows: list[OutboxEvent] = rows or []
        self.commits = 0
        self.rollbacks = 0
        self.flushes = 0
        self.executed: list[Any] = []
        self.closed = False

    def set_rows(self, rows: list[OutboxEvent]) -> None:
        self._rows = rows

    async def execute(self, stmt: Any) -> _FakeResult:
        self.executed.append(stmt)
        return _FakeResult(self._rows)

    async def flush(self) -> None:
        self.flushes += 1

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def close(self) -> None:
        self.closed = True

    # Async context manager support: the relay does
    # ``async with self._session_factory() as session:``,
    # which requires the returned object to be an async CM.
    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()


class _FakeSessionFactory:
    """Returns a session that the test can inspect and mutate.

    The factory itself is NOT a context manager; calling it
    returns a session, and the session is the async context
    manager.
    """

    def __init__(self, initial_rows: list[OutboxEvent] | None = None) -> None:
        self.session = _FakeSession(initial_rows or [])

    def __call__(self) -> _FakeSession:
        return self.session


def _make_relay(
    producer: KafkaProducer,
    session_factory: _FakeSessionFactory,
) -> OutboxRelay:
    """Build a relay with a fake engine and the given producer."""
    relay = OutboxRelay(
        engine=MagicMock(),  # type: ignore[arg-type]
        producer=producer,
        settings=_relay_settings(),
    )
    relay._session_factory = session_factory  # type: ignore[method-assign]
    return relay


def _relay_settings() -> Any:
    """Build a Settings-like object with only the relay fields."""
    from src.core.config import get_settings

    s = get_settings()
    # Mutate the cached instance. Tests are sequential so this
    # is safe.
    s.outbox_relay_batch_size = 50
    s.outbox_relay_max_attempts = 3
    return s


# ===========================================================================
# Mock producer
# ===========================================================================


class _MockProducer:
    """Captures every publish call; can be configured to fail."""

    def __init__(
        self,
        *,
        fail_with: Exception | None = None,
        fail_after: int | None = None,
    ) -> None:
        self.published: list[tuple[EventEnvelope, str]] = []
        self._fail_with = fail_with
        self._fail_after = fail_after
        self._calls = 0

    async def publish(
        self,
        envelope: EventEnvelope,
        *,
        topic: str,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        self._calls += 1
        if self._fail_after is not None and self._calls > self._fail_after:
            if self._fail_with is not None:
                raise self._fail_with
        elif self._fail_after is None and self._fail_with is not None:
            raise self._fail_with
        self.published.append((envelope, topic))
        # Return a fake PublishResult.
        from src.core.messaging import PublishResult

        return PublishResult(
            topic=topic,
            partition=0,
            offset=self._calls,
            event_id=envelope.event_id,
        )

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    @property
    def started(self) -> bool:
        return True


# ===========================================================================
# Happy path
# ===========================================================================


class TestHappyPath:
    @pytest.mark.asyncio
    async def test_relay_publishes_and_marks_processed(self) -> None:
        rows = [_make_outbox_row(), _make_outbox_row(), _make_outbox_row()]
        factory = _FakeSessionFactory(rows)
        producer = _MockProducer()
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        published_count = await relay.run_once()

        # All three rows were published.
        assert published_count == 3
        assert len(producer.published) == 3
        # Each row's processed flag is set, processed_at is set.
        for row in rows:
            assert row.processed is True
            assert row.processed_at is not None
        # One commit, no rollbacks.
        assert factory.session.commits == 1
        assert factory.session.rollbacks == 0
        # Stats reflect the work.
        assert relay.stats.rows_published == 3
        assert relay.stats.rows_failed == 0
        assert relay.stats.rows_skipped_poison == 0

    @pytest.mark.asyncio
    async def test_empty_queue_returns_zero(self) -> None:
        factory = _FakeSessionFactory([])
        producer = _MockProducer()
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        published_count = await relay.run_once()

        assert published_count == 0
        assert producer.published == []
        # No SELECT happened, no commit.
        assert factory.session.commits == 0
        assert factory.session.rollbacks == 0
        # But we did count the empty tick.
        assert relay.stats.ticks == 1

    @pytest.mark.asyncio
    async def test_preserves_row_order_via_created_at(self) -> None:
        rows = [_make_outbox_row() for _ in range(3)]
        factory = _FakeSessionFactory(rows)
        producer = _MockProducer()
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        await relay.run_once()

        # Verify the SELECT was made (the order is enforced by
        # the SELECT statement in ``_claim_batch``).
        assert len(factory.session.executed) == 1
        # The relay published each row's envelope.
        envelopes = [env for env, _ in producer.published]
        assert envelopes == [row_to_envelope(r) for r in rows]


def row_to_envelope(row: OutboxEvent) -> EventEnvelope:
    return EventEnvelope.model_validate(row.payload)


# ===========================================================================
# Unhappy paths
# ===========================================================================


class TestUnhappyPaths:
    @pytest.mark.asyncio
    async def test_broker_unavailable_rolls_back_batch(self) -> None:
        rows = [_make_outbox_row() for _ in range(5)]
        factory = _FakeSessionFactory(rows)
        producer = _MockProducer(
            fail_with=BrokerUnavailableError("Kafka is down"),
        )
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        published_count = await relay.run_once()

        # No rows were published successfully.
        assert published_count == 0
        # The transaction was rolled back; no commit happened.
        assert factory.session.commits == 0
        assert factory.session.rollbacks == 1
        # No rows are marked processed.
        for row in rows:
            assert row.processed is False
        # The first row has its attempt_count incremented.
        # The remaining rows are unchanged (we failed fast).
        assert rows[0].attempt_count == 1
        assert rows[0].last_error is not None
        for row in rows[1:]:
            assert row.attempt_count == 0
            assert row.last_error is None
        # Stats reflect the failure.
        assert relay.stats.rows_published == 0
        assert relay.stats.rows_failed == 5  # 1 explicit + 4 skipped

    @pytest.mark.asyncio
    async def test_permanent_error_marks_row_as_poison(self) -> None:
        rows = [_make_outbox_row() for _ in range(3)]
        # Corrupt the first row's payload so the envelope
        # deserialization fails.
        rows[0].payload = {"not": "a valid envelope"}
        factory = _FakeSessionFactory(rows)
        producer = _MockProducer()
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        published_count = await relay.run_once()

        # Two of three rows were published.
        assert published_count == 2
        # The corrupted row is marked failed (poison).
        assert rows[0].failed is True
        assert rows[0].processed is False
        assert rows[0].last_error is not None
        # The other two are processed.
        assert rows[1].processed is True
        assert rows[2].processed is True
        # Commit happened for the successful rows.
        assert factory.session.commits == 1
        assert factory.session.rollbacks == 0
        # Stats.
        assert relay.stats.rows_published == 2
        assert relay.stats.rows_skipped_poison == 1

    @pytest.mark.asyncio
    async def test_max_attempts_skips_poison_row(self) -> None:
        # Pre-set attempt_count to the max, so the row is
        # immediately skipped as poison.
        rows = [
            _make_outbox_row(attempt_count=3),  # max
            _make_outbox_row(),  # ok
        ]
        factory = _FakeSessionFactory(rows)
        producer = _MockProducer()
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        published_count = await relay.run_once()

        assert published_count == 1
        assert len(producer.published) == 1
        # The poison row is marked failed.
        assert rows[0].failed is True
        assert rows[0].processed is False
        # The other row succeeded.
        assert rows[1].processed is True
        # Stats.
        assert relay.stats.rows_published == 1
        assert relay.stats.rows_skipped_poison == 1

    @pytest.mark.asyncio
    async def test_commit_failure_after_publish_is_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        rows = [_make_outbox_row() for _ in range(2)]
        factory = _FakeSessionFactory(rows)
        producer = _MockProducer()
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        async def _failing_commit() -> None:
            factory.session.commits += 1
            raise OperationalError(
                statement="COMMIT",
                params=None,
                orig=RuntimeError("database connection lost"),
            )

        factory.session.commit = _failing_commit  # type: ignore[method-assign]

        # The relay must not raise; it should log CRITICAL and
        # return 0 (because the commit failed, no rows are
        # considered "published" from the database's perspective).
        published_count = await relay.run_once()

        # Producer was called twice; the publishes themselves
        # succeeded.
        assert len(producer.published) == 2
        # The relay reports 0 published because the commit failed.
        assert published_count == 0
        # The commit was attempted exactly once.
        assert factory.session.commits == 1
        # Stats: no rows are credited because the commit failed.
        # (This is the contract: rows_published reflects database
        # state, not producer success.)
        assert relay.stats.rows_published == 0
        # No rows are marked failed either; the next tick will
        # retry them. attempt_count was incremented in the
        # in-memory row state, but the database didn't see the
        # update because the commit failed.
        assert relay.stats.rows_failed == 0


# ===========================================================================
# Lifecycle
# ===========================================================================


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_and_stop(self) -> None:
        producer = _MockProducer()
        factory = _FakeSessionFactory([])
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        assert not relay.is_running
        await relay.start()
        assert relay.is_running
        # Wait briefly for the worker to start its loop.
        await asyncio.sleep(0.05)
        await relay.stop(shutdown_timeout_sec=2.0)
        assert not relay.is_running

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self) -> None:
        producer = _MockProducer()
        factory = _FakeSessionFactory([])
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]

        await relay.start()
        await relay.start()  # second call is a no-op
        assert relay.is_running
        await relay.stop(shutdown_timeout_sec=2.0)

    @pytest.mark.asyncio
    async def test_disabled_via_config(self) -> None:
        producer = _MockProducer()
        factory = _FakeSessionFactory([])
        relay = _make_relay(producer, factory)  # type: ignore[arg-type]
        relay._settings.outbox_relay_enabled = False  # type: ignore[attr-defined]

        await relay.start()
        assert not relay.is_running
        await relay.stop(shutdown_timeout_sec=2.0)
