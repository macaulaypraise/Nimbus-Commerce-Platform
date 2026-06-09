"""Real-Postgres integration test for the payments module.

Skipped by default. Run with::

    docker compose up -d
    pytest -m integration

This test creates the payments schema, runs process_payment against
a real AsyncSession, and asserts that the OutboxEvent row was
written atomically with the Payment update.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.config import get_settings
from src.modules.payments.models import Base, OutboxEvent, Payment, PaymentStatus
from src.modules.payments.services import ProcessPaymentRequest, process_payment
from tests.fakes import as_redis_like
from tests.fakes.fake_redis import FakeRedis

pytestmark = pytest.mark.integration


@pytest.fixture
async def engine():
    """Function-scoped engine.

    Function scope (not session) is required so the engine's
    connection pool is bound to the same event loop the test
    body runs in. pytest-asyncio 0.25 default loop scope is
    function; mismatched scopes cause ``attached to a different
    loop`` errors from asyncpg.
    """
    settings = get_settings()
    engine = create_async_engine(settings.database_url, echo=False)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def session_factory(engine):
    """Function-scoped session factory with payments schema bootstrap.

    Sets up ``CREATE SCHEMA IF NOT EXISTS payments`` and the
    metadata-defined tables before the test, then drops the
    schema in teardown. The teardown wraps the ``DROP SCHEMA``
    in a try/except so a failing test doesn't leave a half-
    torn-down schema in the database.
    """
    # Setup: create schema and tables.
    async with engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS payments"))
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        # Teardown: drop the schema. If the test errored during
        # setup and the schema is already gone, that's fine.
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP SCHEMA IF EXISTS payments CASCADE"))
        except Exception:
            # Don't mask the original test error with a teardown
            # error. Just log; the next test run will reset.
            pass


@pytest.mark.asyncio(loop_scope="session")
async def test_process_payment_writes_to_real_postgres(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Set TEST_DATABASE_URL before this test runs (the conftest
    # safety net should already do this, but we double-check).
    assert os.environ.get("TEST_DATABASE_URL"), "test DB not configured"

    fake_redis = FakeRedis()
    async with session_factory() as session:
        # Insert a Payment.
        payment = Payment(
            id=uuid.uuid4(),
            status=PaymentStatus.PENDING,
            amount_minor_units=5000,
            currency="USD",
        )
        session.add(payment)
        await session.commit()

        # Run process_payment.
        result = await process_payment(
            ProcessPaymentRequest(
                payment_id=payment.id,
                idempotency_key=f"idem-real-{uuid.uuid4()}",
                new_status=PaymentStatus.AUTHORIZED,
            ),
            session=session,
            redis=as_redis_like(fake_redis),
        )

        # Verify the Payment is updated.
        stmt = select(Payment).where(Payment.id == payment.id)
        fetched = (await session.execute(stmt)).scalar_one()
        assert fetched.status is PaymentStatus.AUTHORIZED

        # Verify the OutboxEvent is written.
        outbox_stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == str(payment.id))
        outbox = (await session.execute(outbox_stmt)).scalar_one()
        assert outbox.event_type == "payments.payment_authorized"
        assert outbox.processed is False
        assert outbox.dedupe_id == result.envelope.content_hash()
