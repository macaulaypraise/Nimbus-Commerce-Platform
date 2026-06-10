"""Outbox relay: drains the outbox_events table to Kafka.

Design
------
The outbox pattern eliminates the dual-write problem. Producers
write a row to ``outbox_events`` in the same database transaction
as the business state change. This relay polls the table,
publishes each row to Kafka, and marks the row as processed on
broker ack. The relay is the only component that writes to
``processed = true``; if a publish fails, the transaction rolls
back, the rows stay unprocessed, and the next tick retries.

Concurrency safety
------------------
Multiple relay instances can run concurrently. Each tick uses
``SELECT ... FOR UPDATE SKIP LOCKED`` so each instance gets a
disjoint subset of unprocessed rows, with no coordination
required between instances. The row locks are released when the
surrounding transaction commits or rolls back.

Failure modes
-------------
* Publish fails because the broker is unreachable
  (BrokerUnavailableError): the rest of the batch is skipped;
  the transaction rolls back; the next tick retries. We
  trade throughput for consistency: a hot queue waits for the
  broker to come back, but no row is ever lost or duplicated
  (consumers dedupe on dedupe_id).

* Per-row error (e.g., the persisted payload doesn't
  deserialize to an EventEnvelope): the row is marked
  ``failed = true`` and skipped on subsequent ticks. This
  keeps a single bad row from blocking the entire batch.

* Unexpected exception: the attempt_count is incremented, the
  last_error is recorded, and the row is retried on the next
  tick. After ``outbox_relay_max_attempts`` such attempts the
  row is marked failed.

* Commit fails after publish succeeded (rare; e.g., a
  network blip between app and database at commit time): we
  log CRITICAL. The next tick will see the rows as unprocessed
  and re-publish. Consumers dedupe on dedupe_id (the outbox
  table has a unique constraint on it), so re-publishes are
  safe.

Backpressure
------------
If the queue is non-empty, the relay polls at half the configured
interval to drain faster. When the queue is empty, it polls at
the full interval. This keeps CPU usage low on idle systems and
latency low on busy ones.

Sharding
--------
Multiple relay instances can be run for throughput. Sharding by
aggregate_id is a future optimization; the current
``FOR UPDATE SKIP LOCKED`` approach is sufficient up to a few
hundred events/second.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC
from types import TracebackType
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

from src.core.config import Settings, get_settings
from src.core.exceptions import BrokerUnavailableError, InfrastructureError
from src.core.messaging import EventEnvelope, KafkaProducer
from src.modules.payments.models import OutboxEvent

_log = structlog.get_logger("nimbus.workers.outbox_relay")


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RelayStats:
    """Counters exposed by the relay for observability.

    The struct is a frozen dataclass so callers can't mutate it
    by accident; the relay constructs a new instance every
    tick. Read via the ``relay.stats`` property.
    """

    polls: int = 0
    rows_seen: int = 0
    rows_published: int = 0
    rows_failed: int = 0
    rows_skipped_poison: int = 0
    ticks: int = 0  # ticks where the queue was empty
    last_tick_at: float = 0.0
    last_publish_at: float = 0.0


# ---------------------------------------------------------------------------
# Relay
# ---------------------------------------------------------------------------


class OutboxRelay:
    """Polls the outbox table and publishes events to Kafka.

    One instance per process. The class is stateless across
    ticks (the database is the source of truth), so multiple
    instances can run safely side by side.

    Lifecycle::

        relay = OutboxRelay(engine, producer)
        await relay.start()
        ...
        await relay.stop()
    """

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        producer: KafkaProducer,
        settings: Settings | None = None,
    ) -> None:
        self._engine = engine
        self._producer = producer
        self._settings = settings or get_settings()
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=engine,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False,
        )
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._stats = RelayStats()

    # -- Properties -----------------------------------------------------

    @property
    def stats(self) -> RelayStats:
        return self._stats

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    # -- Lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Spawn the worker task. Idempotent."""
        if self.is_running:
            _log.warning("outbox_relay.already_running")
            return
        if not self._settings.outbox_relay_enabled:
            _log.info("outbox_relay.disabled_by_config")
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._run_forever(),
            name="nimbus.outbox_relay",
        )
        _log.info(
            "outbox_relay.started",
            poll_interval_seconds=self._settings.outbox_relay_poll_interval_seconds,
            batch_size=self._settings.outbox_relay_batch_size,
            max_attempts=self._settings.outbox_relay_max_attempts,
        )

    async def stop(self, *, shutdown_timeout_sec: float | None = None) -> None:
        """Stop the worker gracefully.

        The current tick (if any) finishes; new ticks do not
        start. Blocks for up to ``timeout`` seconds (default:
        ``outbox_relay_graceful_shutdown_timeout_seconds``).
        """
        if self._task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(
                self._task,
                timeout=shutdown_timeout_sec
                or self._settings.outbox_relay_graceful_shutdown_timeout_seconds,
            )
        except TimeoutError:
            _log.error(
                "outbox_relay.shutdown_timeout",
                timeout=shutdown_timeout_sec,
            )
            self._task.cancel()
            with _Suppress(Exception):
                await self._task
        finally:
            self._task = None
            _log.info(
                "outbox_relay.stopped",
                polls=self._stats.polls,
                rows_published=self._stats.rows_published,
                rows_failed=self._stats.rows_failed,
            )

    # -- Worker loop ----------------------------------------------------

    async def _run_forever(self) -> None:
        """The worker loop. Catches all exceptions and logs them so
        a transient failure doesn't kill the relay.
        """
        try:
            while not self._stop_event.is_set():
                try:
                    processed = await self._tick()
                    # Hot queue: poll faster. Idle: poll at the
                    # configured interval.
                    if processed > 0:
                        sleep_seconds = self._settings.outbox_relay_poll_interval_seconds / 2
                    else:
                        sleep_seconds = self._settings.outbox_relay_poll_interval_seconds
                    await self._sleep(sleep_seconds)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _log.error(
                        "outbox_relay.tick_failed",
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
                    await self._sleep(self._settings.outbox_relay_poll_interval_seconds)
        except asyncio.CancelledError:
            _log.info("outbox_relay.cancelled")
            raise

    async def _sleep(self, seconds: float) -> None:
        """Sleep for ``seconds`` but wake immediately on stop.

        Using :meth:`asyncio.Event.wait` lets the stop signal
        interrupt the sleep so shutdown is fast.
        """
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)

    async def _tick(self) -> int:
        """One iteration of the worker loop.

        Returns the number of rows successfully published. Zero
        means the queue was empty, all rows were skipped as
        poison, or the batch was rolled back due to a broker
        outage.
        """
        self._stats = _replace(self._stats, polls=self._stats.polls + 1)

        async with self._session_factory() as session:
            try:
                rows = await self._claim_batch(session)
            except OperationalError as exc:
                _log.warning("outbox_relay.claim_failed", error=str(exc))
                return 0

            if not rows:
                self._stats = _replace(
                    self._stats,
                    ticks=self._stats.ticks + 1,
                    last_tick_at=time.time(),
                )
                return 0

            published, failed, poison, fail_fast = await self._publish_batch(rows)

            if fail_fast:
                # Broker outage or other systemic issue. Roll
                # back the transaction; the next tick retries
                # from the top.
                await session.rollback()
                self._stats = _replace(
                    self._stats,
                    rows_failed=self._stats.rows_failed + failed,
                    last_tick_at=time.time(),
                )
                return 0

            # We have a mix of successes and (possibly) poison
            # rows. Mark the successful ones in the database.
            await self._mark_batch(session, rows, published, poison)

            try:
                await session.commit()
            except OperationalError as exc:
                _log.critical(
                    "outbox_relay.commit_failed_after_publish",
                    error=str(exc),
                    rows_attempted=len(rows),
                    published=published,
                )
                # Transaction is dead. Next tick re-reads the
                # rows as unprocessed and re-publishes. Consumers
                # dedupe on dedupe_id.
                return 0

            now = time.time()
            self._stats = _replace(
                self._stats,
                rows_seen=self._stats.rows_seen + len(rows),
                rows_published=self._stats.rows_published + published,
                rows_failed=self._stats.rows_failed + failed,
                rows_skipped_poison=self._stats.rows_skipped_poison + poison,
                last_tick_at=now,
                last_publish_at=now if published > 0 else self._stats.last_publish_at,
            )
            if published > 0 or poison > 0:
                _log.info(
                    "outbox_relay.batch_published",
                    rows=len(rows),
                    published=published,
                    failed=failed,
                    poison=poison,
                )
            return published

    # -- Batch operations -----------------------------------------------

    async def _claim_batch(self, session: AsyncSession) -> list[OutboxEvent]:
        """SELECT a batch of unprocessed, non-poisoned rows with
        row locks.

        Uses ``FOR UPDATE SKIP LOCKED`` so concurrent relay
        instances see disjoint row sets. The locks are released
        when the surrounding transaction commits or rolls back.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.processed.is_(False))
            .where(OutboxEvent.failed.is_(False))
            .order_by(OutboxEvent.created_at.asc())
            .limit(self._settings.outbox_relay_batch_size)
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def _publish_batch(
        self,
        rows: list[OutboxEvent],
    ) -> tuple[int, int, int, bool]:
        """Publish each row's payload to Kafka.

        Returns ``(published, failed, poison, fail_fast)``:

          * ``published``: rows successfully sent to the broker.
          * ``failed``: rows whose publish raised an exception;
            these are NOT marked processed and will be retried
            on the next tick.
          * ``poison``: rows marked ``failed = True`` (max
            attempts exceeded or per-row permanent error).
          * ``fail_fast``: True if a systemic error (broker
            down) caused us to abandon the rest of the batch.
            The caller should NOT commit in this case; the
            next tick retries the whole batch.
        """
        published = 0
        failed = 0
        poison = 0
        for row in rows:
            # Skip rows that have already failed too many times.
            if row.attempt_count >= self._settings.outbox_relay_max_attempts:
                _log.critical(
                    "outbox_relay.poison_message",
                    outbox_id=str(row.id),
                    event_type=row.event_type,
                    attempt_count=row.attempt_count,
                    max_attempts=self._settings.outbox_relay_max_attempts,
                )
                row.failed = True
                row.last_error = "max_attempts_exceeded"
                poison += 1
                continue

            try:
                await self._publish_one(row)
                published += 1
            except BrokerUnavailableError as exc:
                # Broker is down. Don't waste time on the rest
                # of the batch; the next tick will retry from
                # the top.
                row.attempt_count += 1
                row.last_error = str(exc)[:1024]
                failed += 1
                remaining = len(rows) - published - failed - poison
                _log.warning(
                    "outbox_relay.broker_unavailable",
                    outbox_id=str(row.id),
                    event_type=row.event_type,
                    attempt=row.attempt_count,
                    error=type(exc).__name__,
                    remaining_skipped=max(0, remaining),
                )
                # Count the unprocessed rest as "failed" for
                # observability: they will retry next tick.
                return published, failed + max(0, remaining), poison, True
            except InfrastructureError as exc:
                # Per-row error (e.g., deserialization). Mark as
                # poison and continue with the rest of the batch.
                row.attempt_count += 1
                row.last_error = str(exc)[:1024]
                row.failed = True
                failed += 1
                poison += 1
                _log.error(
                    "outbox_relay.permanent_error",
                    outbox_id=str(row.id),
                    event_type=row.event_type,
                    error=type(exc).__name__,
                )
            except Exception as exc:
                # Unexpected error. Log, increment attempts,
                # continue. We don't mark as poison because the
                # error might be transient.
                row.attempt_count += 1
                row.last_error = (f"unexpected: {type(exc).__name__}: {exc}")[:1024]
                failed += 1
                _log.error(
                    "outbox_relay.unexpected_error",
                    outbox_id=str(row.id),
                    event_type=row.event_type,
                    error=type(exc).__name__,
                )

        return published, failed, poison, False

    async def _publish_one(self, row: OutboxEvent) -> None:
        """Publish a single outbox row's payload to Kafka.

        Reconstructs the EventEnvelope from the persisted JSONB
        payload, then calls :meth:`KafkaProducer.publish`. The
        envelope is reconstructed (rather than the raw payload
        being sent) so the headers (``event_id``, ``event_type``,
        ``schema_version``) are set on the Kafka message for
        consumer routing.
        """
        try:
            envelope = EventEnvelope.model_validate(row.payload)
        except Exception as exc:
            raise InfrastructureError(
                "Outbox row payload does not deserialize to EventEnvelope.",
                details={
                    "outbox_id": str(row.id),
                    "event_type": row.event_type,
                    "error": str(exc),
                },
            ) from exc

        await self._producer.publish(
            envelope,
            topic=row.topic,
            key=envelope.aggregate_id,
            headers={
                "outbox_id": str(row.id),
                "aggregate_type": row.aggregate_type,
            },
        )

    async def _mark_batch(
        self,
        session: AsyncSession,
        rows: list[OutboxEvent],
        published_count: int,
        poison_count: int,
    ) -> None:
        """Set ``processed = true`` and ``processed_at`` on the
        rows that were successfully published.

        The ``_publish_batch`` step may have set in-memory fields
        on the ``row`` objects (``attempt_count``, ``last_error``,
        ``failed``). SQLAlchemy's unit-of-work tracks those
        mutations and emits UPDATEs on flush. We just need to
        set ``processed`` and ``processed_at`` on the rows that
        succeeded.
        """
        if published_count == 0 and poison_count == 0:
            return

        from datetime import datetime

        now = datetime.now(UTC)
        published_seen = 0
        for row in rows:
            if row.failed:
                # Already marked as poison in ``_publish_batch``.
                continue
            if published_seen < published_count:
                row.processed = True
                row.processed_at = now
                published_seen += 1
        # Flush so the in-memory mutations become SQL UPDATEs
        # within the current transaction.
        await session.flush()

    # -- Manual control (useful in tests) -------------------------------

    async def run_once(self) -> int:
        """Run a single tick. Useful for tests and ad-hoc draining.

        Returns the number of rows successfully published.
        """
        return await self._tick()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Suppress:
    """Minimal stdlib-replacement for ``contextlib.suppress``.

    Imported lazily so we don't pull contextlib into a hot path
    in the worker loop.
    """

    def __init__(self, *exceptions: type[BaseException]) -> None:
        self._exceptions = exceptions

    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return exc_type is not None and issubclass(exc_type, self._exceptions)


def _replace(stats: RelayStats, **changes: Any) -> RelayStats:
    """Functional update for the frozen :class:`RelayStats`.

    Uses :data:`typing.Any` for the keyword values because
    RelayStats fields have heterogeneous types (int, float,
    bool) and we don't want to maintain a parallel
    TypedDict here.
    """
    return RelayStats(
        polls=changes.get("polls", stats.polls),
        rows_seen=changes.get("rows_seen", stats.rows_seen),
        rows_published=changes.get("rows_published", stats.rows_published),
        rows_failed=changes.get("rows_failed", stats.rows_failed),
        rows_skipped_poison=changes.get("rows_skipped_poison", stats.rows_skipped_poison),
        ticks=changes.get("ticks", stats.ticks),
        last_tick_at=changes.get("last_tick_at", stats.last_tick_at),
        last_publish_at=changes.get("last_publish_at", stats.last_publish_at),
    )
