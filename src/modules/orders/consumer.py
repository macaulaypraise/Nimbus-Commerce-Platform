"""Kafka consumer for the Orders module.

Subscribes to ``payments.events`` and handles:

  * ``payments.payment_captured`` → set order to PAID.
  * ``payments.payment_failed``  → set order to FAILED and
    release the reserved stock (saga compensation).

Both handlers are idempotent at the application level (via
the dedupe check in the base consumer and the status-based
guard in ``update_order_status``).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache import get_breaker, get_redis
from src.core.consumer import KafkaConsumer
from src.core.database import get_session
from src.modules.inventory.services import (
    ReleaseRequest,
)
from src.modules.inventory.services import (
    release as inventory_release,
)
from src.modules.orders.models import Order, OrderStatus
from src.modules.orders.services import update_order_status

if TYPE_CHECKING:
    from src.core.cache import AsyncCircuitBreaker
    from src.core.messaging import EventEnvelope
    from src.core.protocols import RedisLike

_log = structlog.get_logger("nimbus.orders.consumer")


# We reuse the payments schema for the orders outbox. A
# future slice will move the outbox to a shared ``events``
# schema. For now, the consumer doesn't write to the
# outbox -- it only updates the order status.


@asynccontextmanager
async def _order_session() -> AsyncIterator[Any]:
    """Async context manager for orders-schema sessions.

    Uses ``@asynccontextmanager`` explicitly so the
    ``finally`` block runs deterministically. The
    underlying :func:`get_session` releases the connection
    to the pool in its own finally block; we add an
    explicit context manager wrapper here for clarity
    and so the consumer's call site reads naturally.
    """
    async for session in get_session(schema="orders"):
        yield session


def _make_session_factory() -> Callable[[], AbstractAsyncContextManager[Any]]:
    """Build a callable that yields a fresh orders session.

    The returned callable is suitable for passing to
    :class:`KafkaConsumer` (which calls it once per
    message). Each invocation opens a new session and
    closes it when the consumer's handler returns.
    """

    def factory() -> AbstractAsyncContextManager[Any]:
        return _order_session()

    return factory


class OrderConsumer(KafkaConsumer):
    """Consumes payment events and updates order status."""

    def __init__(
        self,
        *,
        redis: RedisLike | None = None,
        breaker: AsyncCircuitBreaker | None = None,
        session_factory: Any | None = None,
    ) -> None:
        from src.core.config import get_settings

        settings = get_settings()
        super().__init__(
            topics=["payments.events"],
            redis=redis or get_redis(),  # type: ignore[arg-type]
            breaker=breaker or get_breaker(),
            settings=settings,
            consumer_group=settings.kafka_consumer_group,
            session_factory=session_factory or _make_session_factory(),
        )

    def _register_handlers(self) -> None:
        self._handlers[
            (
                "payments.events",
                "payments.payment_captured",
            )
        ] = self._on_payment_captured
        self._handlers[
            (
                "payments.events",
                "payments.payment_failed",
            )
        ] = self._on_payment_failed

    async def _on_payment_captured(self, envelope: EventEnvelope, session: AsyncSession) -> None:
        """Set the order to PAID."""
        order_id_str = envelope.payload.get("order_id")
        if not isinstance(order_id_str, str):
            _log.warning(
                "orders.payment_captured.no_order_id",
                event_id=str(envelope.event_id),
            )
            return
        try:
            order_id = uuid.UUID(order_id_str)
        except ValueError:
            _log.warning(
                "orders.payment_captured.invalid_order_id",
                order_id_str=order_id_str,
            )
            return
        await update_order_status(
            order_id,
            new_status=OrderStatus.PAID,
            session=session,
        )

    async def _on_payment_failed(self, envelope: EventEnvelope, session: AsyncSession) -> None:
        """Set the order to FAILED and release the reserved stock."""
        order_id_str = envelope.payload.get("order_id")
        if not isinstance(order_id_str, str):
            _log.warning(
                "orders.payment_failed.no_order_id",
                event_id=str(envelope.event_id),
            )
            return
        try:
            order_id = uuid.UUID(order_id_str)
        except ValueError:
            _log.warning(
                "orders.payment_failed.invalid_order_id",
                order_id_str=order_id_str,
            )
            return

        # Update the order first. If this fails, the
        # compensation below doesn't run; the message
        # will be redelivered.
        await update_order_status(
            order_id,
            new_status=OrderStatus.FAILED,
            session=session,
        )

        # Saga compensation: release the reserved stock.
        # The order's stock_id, quantity, and
        # expected_version are stored on the order itself.
        from src.core.database import with_schemas

        async with with_schemas(session, "orders", "inventory"):
            stmt = select(Order).where(Order.id == order_id)
            result = await session.execute(stmt)
            order = result.scalar_one_or_none()
            if order is None:
                _log.warning(
                    "orders.payment_failed.order_missing",
                    order_id=str(order_id),
                )
                return
            try:
                await inventory_release(
                    ReleaseRequest(
                        stock_id=order.stock_id,
                        quantity=order.quantity,
                        expected_version=order.expected_version,
                    ),
                    session=session,
                )
            except Exception as exc:
                # The release is idempotent at the
                # application level; if it fails for
                # some other reason, log and continue.
                _log.error(
                    "orders.payment_failed.release_failed",
                    order_id=str(order_id),
                    error=str(exc),
                )
                raise

        _log.info(
            "orders.payment_failed.compensated",
            order_id=str(order_id),
            stock_id=str(order.stock_id),
            quantity=order.quantity,
        )
