"""Background workers for the Nimbus platform.

Workers are long-running asyncio tasks that process background
work outside the request/response cycle. The first worker is the
outbox relay, which drains the outbox_events table to Kafka.
Future workers will include:

  * Notification dispatcher (consumes user-facing events)
  * Email / push workers
  * Periodic cleanup (expired tokens, abandoned carts, etc.)
"""

from __future__ import annotations
