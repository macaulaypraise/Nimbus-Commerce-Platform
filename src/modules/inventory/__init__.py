"""Inventory module.

Owns the ``inventory`` PostgreSQL schema. Hosts the Stock
aggregate and the reserve / release services used by the
orders saga.

Public surface:
    * :class:`Stock` — the SQLAlchemy model.
    * :func:`reserve` — atomically reserve stock.
    * :func:`release` — release a previous reservation.
    * :class:`OutOfStockError` — insufficient stock.
    * :class:`ConcurrentUpdateError` — optimistic lock failure.
"""

from __future__ import annotations

from src.modules.inventory.models import Base, Stock
from src.modules.inventory.services import (
    ConcurrentUpdateError,
    OutOfStockError,
    release,
    reserve,
)

__all__ = [
    "Base",
    "ConcurrentUpdateError",
    "OutOfStockError",
    "Stock",
    "release",
    "reserve",
]
