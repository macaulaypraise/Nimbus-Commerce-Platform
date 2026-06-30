"""Order services: place_order and update_order_status.

Saga coordination
------------------
``place_order`` performs the first half of the order saga:

  1. Read the current stock version.
  2. Try to reserve the requested quantity (optimistic lock).
  3. If ConcurrentUpdateError, retry from step 1.
  4. If OutOfStockError, return error.
  5. Create the order in RESERVED status.
  6. Write the outbox event for orders.order_created.
  7. Commit. If commit fails, the whole transaction rolls
     back (including the reserve), so the caller can safely
     retry with the same idempotency key.

The consumer side (payment_captured, payment_failed) is in
``consumer.py``.

Cross-schema atomicity
-----------------------
The place_order transaction needs to write to both the
``orders`` and ``inventory`` schemas atomically. We use
``with_schemas(session, "orders", "inventory")`` to widen
the search_path for the transaction. This is transaction-
scoped, so the connection's normal per-module search_path
is unaffected for subsequent transactions.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import with_schemas
from src.core.exceptions import ResourceNotFoundError
from src.core.messaging import EventEnvelope
from src.modules.inventory.services import (
    ConcurrentUpdateError,
    OutOfStockError,
    ReserveRequest,
    ReserveResult,
    get_stock,
)
from src.modules.inventory.services import (
    reserve as inventory_reserve,
)
from src.modules.orders.models import Order, OrderStatus
from src.modules.payments.models import OutboxEvent

_log = structlog.get_logger("nimbus.orders")


class OrderNotFoundError(ResourceNotFoundError):
    code = "orders.not_found"
    safe_message = "Order not found."


@dataclass(frozen=True, slots=True)
class PlaceOrderRequest:
    user_id: uuid.UUID
    stock_id: uuid.UUID
    quantity: int


@dataclass(frozen=True, slots=True)
class PlaceOrderResult:
    order: Order
    outbox_event: OutboxEvent
    envelope: EventEnvelope


_MAX_RESERVE_RETRIES = 3


async def place_order(
    request: PlaceOrderRequest,
    *,
    session: AsyncSession,
) -> PlaceOrderResult:
    """Place an order. Atomic: reserve + order + outbox.

    Uses SQLAlchemy 2.0's autobegin semantics: the first
    ``session.execute()`` or ORM operation begins a transaction,
    and we commit at the end. We do NOT wrap in
    ``async with session.begin():`` because that conflicts
    with the auto-begun transaction and raises
    ``InvalidRequestError``.
    """
    if request.quantity <= 0:
        raise ValueError("quantity must be > 0")

    # The transaction must see both orders and inventory
    # schemas. We widen the search_path for this
    # transaction.
    async with with_schemas(session, "orders", "inventory"):
        last_exc: Exception | None = None
        for attempt in range(_MAX_RESERVE_RETRIES):
            stock = await get_stock(request.stock_id, session=session)
            try:
                reserve_result: ReserveResult = await inventory_reserve(
                    ReserveRequest(
                        stock_id=request.stock_id,
                        quantity=request.quantity,
                        expected_version=stock.version,
                    ),
                    session=session,
                )
            except ConcurrentUpdateError as exc:
                last_exc = exc
                _log.info(
                    "orders.reserve_retry",
                    attempt=attempt,
                    stock_id=str(request.stock_id),
                )
                continue
            except OutOfStockError:
                raise

            total = stock.price_minor_units * request.quantity
            order = Order(
                id=uuid.uuid4(),
                user_id=request.user_id,
                stock_id=request.stock_id,
                quantity=request.quantity,
                expected_version=reserve_result.new_version,
                total_amount_minor_units=total,
                currency=stock.currency,
                status=OrderStatus.RESERVED,
            )
            envelope = EventEnvelope(
                event_type="orders.order_created",
                schema_version=1,
                aggregate_type="Order",
                aggregate_id=str(order.id),
                payload={
                    "order_id": str(order.id),
                    "user_id": str(order.user_id),
                    "stock_id": str(request.stock_id),
                    "quantity": order.quantity,
                    "total_amount_minor_units": total,
                    "currency": stock.currency,
                },
            )
            outbox = OutboxEvent(
                id=envelope.event_id,
                aggregate_type=envelope.aggregate_type,
                aggregate_id=envelope.aggregate_id,
                event_type=envelope.event_type,
                topic="orders.events",
                payload=envelope.model_dump(mode="json"),
                dedupe_id=envelope.content_hash(),
            )
            session.add(order)
            session.add(outbox)
            await session.flush()
            _log.info(
                "orders.placed",
                order_id=str(order.id),
                user_id=str(request.user_id),
                total=total,
                currency=stock.currency,
            )
            return PlaceOrderResult(order=order, outbox_event=outbox, envelope=envelope)
        assert last_exc is not None
        raise last_exc


async def update_order_status(
    order_id: uuid.UUID,
    *,
    new_status: OrderStatus,
    session: AsyncSession,
) -> Order:
    """Idempotently update an order's status.

    Used by the Kafka consumer to transition an order to
    PAID (on payment_captured) or FAILED (on payment_failed).

    Idempotency: a status transition is only applied if the
    current status is the expected predecessor. A second
    delivery of the same event sees the order already in
    the new state and is a no-op.
    """
    if new_status not in (OrderStatus.PAID, OrderStatus.FAILED):
        raise ValueError(f"update_order_status only supports PAID or FAILED, got {new_status}")
    # Read with FOR UPDATE so the status check and the
    # UPDATE are serialized.
    stmt = select(Order).where(Order.id == order_id).with_for_update()
    result = await session.execute(stmt)
    order = result.scalar_one_or_none()
    if order is None:
        raise OrderNotFoundError(
            "Order does not exist.",
            details={"order_id": str(order_id)},
        )
    if order.status == new_status:
        # Already in the desired state; no-op. This is the
        # idempotency path for redelivered messages.
        _log.info(
            "orders.status_noop",
            order_id=str(order_id),
            status=new_status.value,
        )
        return order
    if new_status == OrderStatus.PAID and order.status != OrderStatus.RESERVED:
        # Only RESERVED orders can transition to PAID.
        # A SHIPPED order is a no-op (already terminal);
        # a FAILED order means we got a payment_captured
        # for an order whose payment failed -- unusual
        # but possible if events cross; we skip.
        _log.warning(
            "orders.status_skip",
            order_id=str(order_id),
            current=order.status.value,
            target=new_status.value,
        )
        return order
    if new_status == OrderStatus.FAILED and order.status not in (
        OrderStatus.PENDING,
        OrderStatus.RESERVED,
    ):
        # Only PENDING or RESERVED orders can transition
        # to FAILED.
        _log.warning(
            "orders.status_skip",
            order_id=str(order_id),
            current=order.status.value,
            target=new_status.value,
        )
        return order
    order.status = new_status
    session.add(order)
    _log.info(
        "orders.status_updated",
        order_id=str(order_id),
        new_status=new_status.value,
    )
    return order
