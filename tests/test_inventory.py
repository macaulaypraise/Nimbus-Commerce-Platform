"""Tests for the inventory services."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import Select, Update

from src.modules.inventory.services import (
    ConcurrentUpdateError,
    OutOfStockError,
    ReleaseRequest,
    ReserveRequest,
    StockNotFoundError,
    release,
    reserve,
)


class _FakeResult:
    def __init__(self, value: Any = None, rowcount: int = 0) -> None:
        self._value = value
        self.rowcount = rowcount

    def scalar_one_or_none(self) -> Any:
        return self._value


class _FakeStock:
    def __init__(self, available: int, version: int, stock_id: uuid.UUID | None = None) -> None:
        self.id = stock_id or uuid.uuid4()
        self.available = available
        self.version = version
        self.price_minor_units = 1000
        self.currency = "USD"


class _FakeSession:
    """Mocks AsyncSession for inventory service tests.

    ``update_rowcount`` controls what UPDATE returns (0 or 1).
    ``row_to_return`` controls what SELECT returns.
    """

    def __init__(
        self,
        *,
        row_to_return: Any = None,
        update_rowcount: int = 1,
    ) -> None:
        self.row_to_return = row_to_return
        self.update_rowcount = update_rowcount
        self.executed: list[Any] = []

    async def execute(self, stmt: Any) -> _FakeResult:
        self.executed.append(stmt)
        if isinstance(stmt, Update):
            return _FakeResult(rowcount=self.update_rowcount)
        if isinstance(stmt, Select):
            return _FakeResult(value=self.row_to_return)
        return _FakeResult()

    async def flush(self) -> None:
        pass

    async def commit(self) -> None:
        pass


class TestReserve:
    @pytest.mark.asyncio
    async def test_succeeds_when_stock_and_version_match(self) -> None:
        stock_id = uuid.uuid4()
        stock = _FakeStock(available=100, version=5, stock_id=stock_id)
        session = _FakeSession(row_to_return=stock)

        result = await reserve(
            ReserveRequest(stock_id=stock_id, quantity=10, expected_version=5),
            session=session,  # type: ignore[arg-type]
        )
        assert result.new_version == 6
        assert result.new_available == 90

    @pytest.mark.asyncio
    async def test_raises_out_of_stock(self) -> None:
        stock_id = uuid.uuid4()
        stock = _FakeStock(available=5, version=5, stock_id=stock_id)
        session = _FakeSession(row_to_return=stock, update_rowcount=0)

        with pytest.raises(OutOfStockError):
            await reserve(
                ReserveRequest(stock_id=stock_id, quantity=10, expected_version=5),
                session=session,  # type: ignore[arg-type]
            )

    @pytest.mark.asyncio
    async def test_raises_concurrent_update(self) -> None:
        stock_id = uuid.uuid4()
        stock = _FakeStock(available=100, version=6, stock_id=stock_id)
        session = _FakeSession(row_to_return=stock, update_rowcount=0)

        with pytest.raises(ConcurrentUpdateError):
            await reserve(
                ReserveRequest(stock_id=stock_id, quantity=10, expected_version=5),
                session=session,  # type: ignore[arg-type]
            )

    @pytest.mark.asyncio
    async def test_raises_not_found(self) -> None:
        stock_id = uuid.uuid4()
        session = _FakeSession(row_to_return=None, update_rowcount=0)

        with pytest.raises(StockNotFoundError):
            await reserve(
                ReserveRequest(stock_id=stock_id, quantity=10, expected_version=5),
                session=session,  # type: ignore[arg-type]
            )

    @pytest.mark.asyncio
    async def test_rejects_zero_quantity(self) -> None:
        with pytest.raises(ValueError):
            await reserve(
                ReserveRequest(stock_id=uuid.uuid4(), quantity=0, expected_version=0),
                session=_FakeSession(),  # type: ignore[arg-type]
            )


class TestRelease:
    @pytest.mark.asyncio
    async def test_succeeds(self) -> None:
        session = _FakeSession(update_rowcount=1)
        result = await release(
            ReleaseRequest(stock_id=uuid.uuid4(), quantity=10, expected_version=5),
            session=session,  # type: ignore[arg-type]
        )
        # The new release is unconditional; the return value
        # is 0 on success or -1 on no-op.
        assert result == 0

    @pytest.mark.asyncio
    async def test_idempotent_returns_minus_one(self) -> None:
        session = _FakeSession(update_rowcount=0)
        new_version = await release(
            ReleaseRequest(stock_id=uuid.uuid4(), quantity=10, expected_version=5),
            session=session,  # type: ignore[arg-type]
        )
        assert new_version == -1

    @pytest.mark.asyncio
    async def test_raises_concurrent_update_on_update_failure(self) -> None:
        """Race condition: pre-check passes, but UPDATE fails.

        This simulates a concurrent transaction that modifies the
        stock between our read and our write. The UPDATE returns
        rowcount=0, and we raise ConcurrentUpdateError.
        """
        stock_id = uuid.uuid4()
        stock = _FakeStock(available=100, version=5, stock_id=stock_id)
        # update_rowcount=0 simulates the UPDATE finding no
        # matching row (concurrent modification).
        session = _FakeSession(row_to_return=stock, update_rowcount=0)
        with pytest.raises(ConcurrentUpdateError):
            await reserve(
                ReserveRequest(
                    stock_id=stock_id,
                    quantity=10,
                    expected_version=5,
                ),
                session=session,  # type: ignore[arg-type]
            )
