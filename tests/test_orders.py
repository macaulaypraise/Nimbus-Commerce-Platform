"""Tests for the orders module."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import Select, Update

from src.modules.inventory.services import (
    OutOfStockError,
)
from src.modules.orders.models import Order, OrderStatus
from src.modules.orders.services import (
    OrderNotFoundError,
    PlaceOrderRequest,
    place_order,
    update_order_status,
)
from src.modules.payments.models import OutboxEvent


class _FakeResult:
    def __init__(self, value: Any = None, rowcount: int = 0) -> None:
        self._value = value
        self.rowcount = rowcount

    def scalar_one_or_none(self) -> Any:
        return self._value


class _FakeStock:
    def __init__(self, available: int = 100, version: int = 5) -> None:
        self.id = uuid.uuid4()
        self.available = available
        self.version = version
        self.price_minor_units = 1000
        self.currency = "USD"


class _FakeSession:
    """Mocks AsyncSession for orders service tests."""

    def __init__(self, stock: Any = None) -> None:
        self._stock = stock
        self.added: list[Any] = []
        self.executed: list[Any] = []

    async def execute(self, stmt: Any) -> _FakeResult:
        self.executed.append(stmt)
        if isinstance(stmt, Update):
            return _FakeResult(rowcount=1)
        if isinstance(stmt, Select):
            return _FakeResult(value=self._stock)
        return _FakeResult()

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        pass

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    @asynccontextmanager
    async def begin(self):
        """No-op async context manager for async with session.begin():."""
        yield self


class TestPlaceOrder:
    @pytest.mark.asyncio
    async def test_creates_reserved_order_and_outbox(self) -> None:
        stock = _FakeStock(available=100, version=5)
        session = _FakeSession(stock=stock)
        result = await place_order(
            PlaceOrderRequest(
                user_id=uuid.uuid4(),
                stock_id=stock.id,
                quantity=2,
            ),
            session=session,  # type: ignore[arg-type]
        )
        assert result.order.status is OrderStatus.RESERVED
        assert result.order.quantity == 2
        assert result.order.total_amount_minor_units == 2000
        # Order and outbox event were both added.
        assert any(isinstance(o, Order) for o in session.added)
        assert any(isinstance(o, OutboxEvent) for o in session.added)
        # Envelope is the order_created event.
        assert result.envelope.event_type == "orders.order_created"
        assert result.envelope.aggregate_id == str(result.order.id)

    @pytest.mark.asyncio
    async def test_propagates_out_of_stock(self) -> None:
        stock = _FakeStock(available=100, version=5)
        session = _FakeSession(stock=stock)

        async def _failing_reserve(*args: Any, **kwargs: Any) -> Any:
            raise OutOfStockError("Insufficient stock.")

        with (
            patch(
                "src.modules.orders.services.inventory_reserve",
                new=_failing_reserve,
            ),
            pytest.raises(OutOfStockError),
        ):
            await place_order(
                PlaceOrderRequest(
                    user_id=uuid.uuid4(),
                    stock_id=stock.id,
                    quantity=200,
                ),
                session=session,  # type: ignore[arg-type]
            )

    @pytest.mark.asyncio
    async def test_rejects_zero_quantity(self) -> None:
        with pytest.raises(ValueError):
            await place_order(
                PlaceOrderRequest(
                    user_id=uuid.uuid4(),
                    stock_id=uuid.uuid4(),
                    quantity=0,
                ),
                session=_FakeSession(),  # type: ignore[arg-type]
            )


class TestUpdateOrderStatus:
    @pytest.mark.asyncio
    async def test_paid_from_reserved(self) -> None:
        order = Order(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            stock_id=uuid.uuid4(),
            quantity=2,
            expected_version=1,
            total_amount_minor_units=2000,
            currency="USD",
            status=OrderStatus.RESERVED,
        )
        session = _FakeSession(stock=order)
        result = await update_order_status(
            order.id,
            new_status=OrderStatus.PAID,
            session=session,  # type: ignore[arg-type]
        )
        assert result.status is OrderStatus.PAID
        assert order.status is OrderStatus.PAID

    @pytest.mark.asyncio
    async def test_paid_is_noop_when_already_paid(self) -> None:
        order = Order(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            stock_id=uuid.uuid4(),
            quantity=2,
            expected_version=1,
            total_amount_minor_units=2000,
            currency="USD",
            status=OrderStatus.PAID,
        )
        session = _FakeSession(stock=order)

        result = await update_order_status(
            order.id,
            new_status=OrderStatus.PAID,
            session=session,  # type: ignore[arg-type]
        )
        assert result.status is OrderStatus.PAID

    @pytest.mark.asyncio
    async def test_raises_on_missing_order(self) -> None:
        session = _FakeSession(stock=None)
        with pytest.raises(OrderNotFoundError):
            await update_order_status(
                uuid.uuid4(),
                new_status=OrderStatus.PAID,
                session=session,  # type: ignore[arg-type]
            )
