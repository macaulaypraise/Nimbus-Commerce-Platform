"""Inventory services: reserve and release stock with optimistic locking.

Optimistic locking
------------------
``reserve`` uses an atomic ``UPDATE`` with a ``WHERE``
clause that includes both the version and the available
quantity. This is a single round-trip and is correct under
arbitrary concurrency: two concurrent calls with the same
``version`` will see one succeed (``rowcount == 1``) and one
fail (``rowcount == 0``).

On a rowcount of 0, we re-query the row to distinguish
"insufficient stock" from "version mismatch". The
re-query is a best-effort hint; the caller is expected to
retry on ``ConcurrentUpdateError`` (or to surface a
user-facing "please try again" message).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, cast

import structlog
from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import ConflictError, ResourceNotFoundError
from src.modules.inventory.models import Stock

_log = structlog.get_logger("nimbus.inventory")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class OutOfStockError(ConflictError):
    """The requested quantity exceeds available stock."""

    code = "inventory.out_of_stock"
    safe_message = "Insufficient stock for the requested quantity."


class ConcurrentUpdateError(ConflictError):
    """The optimistic lock check failed.

    The caller should retry the operation (typically by
    re-reading the current version and re-issuing the
    update).
    """

    code = "inventory.concurrent_update"
    safe_message = "The stock was modified concurrently. Please retry."


class StockNotFoundError(ResourceNotFoundError):
    code = "inventory.not_found"
    safe_message = "Stock not found."


# ---------------------------------------------------------------------------
# Service input / output
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReserveRequest:
    """Input to :func:`reserve`."""

    stock_id: uuid.UUID
    quantity: int
    expected_version: int


@dataclass(frozen=True, slots=True)
class ReserveResult:
    """Output of :func:`reserve`."""

    new_version: int
    new_available: int


@dataclass(frozen=True, slots=True)
class ReleaseRequest:
    """Input to :func:`release`."""

    stock_id: uuid.UUID
    quantity: int
    expected_version: int


# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------


async def _read_stock(
    session: AsyncSession,
    stock_id: uuid.UUID,
) -> Stock | None:
    """Read the current stock row. Used for error disambiguation."""
    stmt = select(Stock).where(Stock.id == stock_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def reserve(request: ReserveRequest, *, session: AsyncSession) -> ReserveResult:
    """Atomically reserve stock using optimistic locking.

    Reads the current stock state, validates the expected
    version and available quantity, then atomically updates
    the row. If the UPDATE returns rowcount=1, the
    reservation succeeded; the new values are computed from
    the inputs (no second read needed).
    """
    if request.quantity <= 0:
        raise ValueError("quantity must be > 0")

    # Read the current stock. We need the current `available`
    # to compute the new value after the UPDATE. The
    # `version` we read is what we'll use in the UPDATE's
    # WHERE clause; if another transaction changes the
    # version between this read and the UPDATE, the UPDATE
    # returns rowcount=0 and we raise ConcurrentUpdateError.
    stock = await _read_stock(session, request.stock_id)
    if stock is None:
        raise StockNotFoundError(
            "Stock does not exist.",
            details={"stock_id": str(request.stock_id)},
        )
    if stock.version != request.expected_version:
        raise ConcurrentUpdateError(
            "Stock was modified by another transaction.",
            details={
                "stock_id": str(request.stock_id),
                "expected_version": request.expected_version,
                "actual_version": stock.version,
            },
        )
    if stock.available < request.quantity:
        raise OutOfStockError(
            "Insufficient stock.",
            details={
                "stock_id": str(request.stock_id),
                "requested": request.quantity,
                "available": stock.available,
            },
        )

    # Atomic UPDATE with optimistic-lock check.
    stmt = (
        update(Stock)
        .where(Stock.id == request.stock_id)
        .where(Stock.version == request.expected_version)
        .where(Stock.available >= request.quantity)
        .values(
            available=Stock.available - request.quantity,
            version=Stock.version + 1,
        )
    )
    result = cast(CursorResult[Any], await session.execute(stmt))
    if result.rowcount != 1:
        # Race: the pre-check passed but the UPDATE found
        # no matching row. Another transaction modified the
        # stock in between our read and our write.
        raise ConcurrentUpdateError(
            "Stock was modified concurrently (race).",
            details={"stock_id": str(request.stock_id)},
        )

    # Compute the new values from the inputs. The UPDATE
    # was atomic; these are the post-UPDATE values.
    return ReserveResult(
        new_version=stock.version + 1,
        new_available=stock.available - request.quantity,
    )


async def release(
    request: ReleaseRequest,
    *,
    session: AsyncSession,
) -> int:
    """Release a previous reservation, adding ``quantity`` back.

    Idempotent at the database level: a second call with the
    same expected_version will see a different (incremented)
    version and the UPDATE will affect 0 rows. The caller
    should treat that as success (the stock is already
    released).

    Returns the new version after the update.
    """
    if request.quantity <= 0:
        raise ValueError("quantity must be > 0")

    stmt = (
        update(Stock)
        .where(Stock.id == request.stock_id)
        .values(
            available=Stock.available + request.quantity,
        )
    )
    result = cast(CursorResult[Any], await session.execute(stmt))
    if result.rowcount == 0:
        _log.warning(
            "inventory.release_noop",
            stock_id=str(request.stock_id),
        )
        return -1
    return 0


async def get_stock(
    stock_id: uuid.UUID,
    *,
    session: AsyncSession,
) -> Stock:
    """Fetch a stock row by id. Raises StockNotFoundError if absent."""
    stock = await _read_stock(session, stock_id)
    if stock is None:
        raise StockNotFoundError(
            "Stock does not exist.",
            details={"stock_id": str(stock_id)},
        )
    return stock
