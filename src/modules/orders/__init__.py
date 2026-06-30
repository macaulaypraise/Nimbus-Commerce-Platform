"""Orders module.

Owns the ``orders`` PostgreSQL schema. Hosts the Order
aggregate, the ``place_order`` service, and the Kafka(saga)
consumers that update order status in response to payment
events.

Public surface:
    * :class:`Order` — the SQLAlchemy model.
    * :class:`OrderStatus` — the order state machine.
    * :func:`place_order` — the idempotent service entry point.
    * :class:`OrderConsumer` — the Kafka consumer for payment
      events.
"""

from __future__ import annotations

from src.modules.orders.consumer import OrderConsumer
from src.modules.orders.models import Base, Order, OrderStatus
from src.modules.orders.services import (
    OrderNotFoundError,
    PlaceOrderRequest,
    PlaceOrderResult,
    place_order,
    update_order_status,
)

__all__ = [
    "Base",
    "Order",
    "OrderConsumer",
    "OrderNotFoundError",
    "OrderStatus",
    "PlaceOrderRequest",
    "PlaceOrderResult",
    "place_order",
    "update_order_status",
]
