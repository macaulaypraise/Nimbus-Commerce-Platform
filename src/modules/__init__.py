"""Domain modules of the Nimbus Commerce Platform.

Each module under this directory is isolated:
  * Each owns its own PostgreSQL schema (set via the SQLAlchemy
    ``checkout`` event in :mod:`src.core.database`).
  * Each exposes its public surface through its ``__init__.py``.
  * Cross-module SQL joins are physically impossible because the
    search_path is restricted to a single module's schema.
  * Cross-module communication happens via Python interfaces
    (synchronous, in-process) or via Kafka events and Redis jobs
    (asynchronous, cross-process).
"""

from __future__ import annotations
