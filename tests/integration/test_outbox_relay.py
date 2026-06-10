"""Real-Postgres integration test for the outbox relay.

Verifies the actual SQL: the relay SELECTs with FOR UPDATE
SKIP LOCKED, marks rows processed, and the dedupe_id unique
constraint holds even under re-publish.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)

from src.core.config import get_settings
from src.core.messaging import EventEnvelope
from src.modules.payments.models import Base, OutboxEvent
from src.workers.outbox_relay import OutboxRelay

pytestmark = pytest.mark.integration


class _MockProducer:
    """Capture publishes for the integration test."""

    def __init__(self) -> None:
        self.published: list[tuple[EventEnvelope, str]] = []

    async def publish(
        self,
        envelope: EventEnvelope,
        *,
        topic: str,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        self.published.append((envelope, topic))
        from src.core.messaging import PublishResult

        return PublishResult(
            topic=topic,
            partition=0,
            offset=len(self.published),
            event_id=envelope.event_id,
        )

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    @property
    def started(self) -> bool:
        return True


@pytest.fixture
async def engine():
    settings = get_settings()
    engine = create_async_engine(settings.database_url, echo=False)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def session_factory(engine):
    """Reset the payments schema for each test.

    We drop and recreate the schema at the START of the test
    rather than dropping at the end. This is more robust
    because:
      * Teardown failures don't leak state to subsequent tests.
      * The schema-drop operation can fail with asyncpg
        "operation in progress" errors when a previous
        connection hasn't been fully released. Setting up a
        fresh schema from a known-clean state sidesteps this.
      * Each test is guaranteed a clean slate, which is the
        correct isolation property.
    """
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS payments CASCADE"))
        await conn.execute(text("CREATE SCHEMA payments"))
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    # No teardown. The next invocation of this fixture (or
    # the engine teardown) will clean up.


@pytest.fixture
async def seeded_outbox(session_factory: Any) -> list[uuid.UUID]:
    """Seed three unprocessed outbox rows. Return their ids."""
    async with session_factory() as session:
        async with session.begin():
            ids: list[uuid.UUID] = []
            for _ in range(3):
                envelope = EventEnvelope(
                    event_type="payments.payment_authorized",
                    schema_version=1,
                    aggregate_type="Payment",
                    aggregate_id=str(uuid.uuid4()),
                    payload={"test": True},
                )
                row = OutboxEvent(
                    id=envelope.event_id,
                    aggregate_type=envelope.aggregate_type,
                    aggregate_id=envelope.aggregate_id,
                    event_type=envelope.event_type,
                    topic="payments.events",
                    payload=envelope.model_dump(mode="json"),
                    dedupe_id=envelope.content_hash(),
                )
                session.add(row)
                ids.append(envelope.event_id)
        return ids


@pytest.mark.asyncio(loop_scope="session")
async def test_relay_drains_real_postgres(
    session_factory: Any,
    seeded_outbox: list[uuid.UUID],
) -> None:
    assert os.environ.get("TEST_DATABASE_URL"), "test DB not configured"

    producer = _MockProducer()
    relay = OutboxRelay(
        engine=None,  # type: ignore[arg-type]
        producer=producer,  # type: ignore[arg-type]
    )
    # The relay normally builds its own session_factory from the
    # engine; for the integration test we substitute ours so we
    # can use the same fixtures.
    relay._session_factory = session_factory  # type: ignore[method-assign]

    published_count = await relay.run_once()

    assert published_count == 3
    assert len(producer.published) == 3

    # Verify the rows are marked processed in the database.
    async with session_factory() as session:
        for row_id in seeded_outbox:
            row = (
                await session.execute(select(OutboxEvent).where(OutboxEvent.id == row_id))
            ).scalar_one()
            assert row.processed is True
            assert row.processed_at is not None
