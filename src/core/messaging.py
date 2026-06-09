"""Async Kafka producer and the event envelope used across modules.

Scope of this slice
-------------------
This module is intentionally focused on the **publisher** side of the
messaging layer. It provides:

  * :class:`EventEnvelope` — a strongly-typed, JSON-serializable
    envelope for every message. Schema is versioned and content-
    addressed so consumers can dedupe.
  * :class:`KafkaProducer` — an aiokafka-backed publisher with
    health checks, structured logging, and graceful shutdown.
  * :class:`EventPublishError` — a typed exception raised when the
    broker is unreachable or rejects a message.

The consumer side (consumer groups, manual commits, dead-letter
queues, rebalancing) is deliberately deferred to a later slice so we
can shape it around the actual subscription patterns of the modules
that will consume.

Outbox pattern
--------------
The producer is designed to be called from an outbox-relay
component, not directly from request handlers. The relay will:

  1. SELECT a batch of unprocessed OutboxEvent rows FOR UPDATE SKIP
     LOCKED.
  2. For each row, build an :class:`EventEnvelope` and call
     :meth:`KafkaProducer.publish`.
  3. On successful broker ack, mark the row as processed.
  4. On failure, leave the row unprocessed and retry on the next
     tick.

This slice ships the publisher; the relay and consumer are future
work.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeVar

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import (
    KafkaConnectionError,
    KafkaError,
    KafkaTimeoutError,
)
from pydantic import BaseModel, ConfigDict, Field

from src.core.config import Settings, get_settings
from src.core.exceptions import BrokerUnavailableError, InfrastructureError

_log = structlog.get_logger("nimbus.messaging")

# Generic payload type. The envelope is parameterized so callers
# can declare the expected payload schema at the type level.
T = TypeVar("T", bound=BaseModel | Mapping[str, Any] | None)


# ---------------------------------------------------------------------------
# Event envelope
# ---------------------------------------------------------------------------


# Well-known UUID namespace for content-addressed event IDs.
# Generated once and frozen; do not change.
NIMBUS_EVENT_NAMESPACE: uuid.UUID = uuid.UUID("5d8e7c3a-1b4f-4a2e-9c5b-8e7f6a5b4c3d")


class EventEnvelope(BaseModel):
    """Strongly-typed, JSON-serializable envelope for every Kafka event.

    Fields:
        event_id: UUID4 generated at publish time. Unique per event;
            consumers use it for deduplication at the broker level.
        event_type: dotted string identifying the event (e.g.,
            ``payments.payment_captured``). Convention is
            ``<module>.<past-tense-verb>``.
        schema_version: integer, defaults to 1. Bump when the
            payload structure changes in a backward-incompatible
            way. Consumers check this and route to the correct
            deserializer.
        aggregate_type: the domain entity the event is about
            (e.g., ``Payment``).
        aggregate_id: stringified id of the aggregate. Always a
            string even for UUID ids so the envelope stays valid
            JSON without further coercion.
        correlation_id: optional cross-system trace id, typically
            the request id from the inbound HTTP request.
        causation_id: optional id of the parent event, if this
            event was triggered by another event (event chains).
        occurred_at: UTC timestamp of when the event occurred
            (i.e., when the database commit happened, not when
            the relay published it).
        payload: the event body. Use :class:`pydantic.BaseModel`
            for the typed variant, :class:`dict` for ad-hoc
            payloads, or ``None`` for marker events.
    """

    model_config = ConfigDict(
        # Allow arbitrary mapping for the payload so callers can
        # pass either a BaseModel or a dict without an explicit cast.
        arbitrary_types_allowed=True,
        # Freeze so an event is immutable after creation. This
        # matters because the envelope is shared between the
        # publisher, the broker, and (eventually) the consumer.
        frozen=True,
    )

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_type: str = Field(min_length=1, max_length=255)
    schema_version: int = Field(default=1, ge=1)
    aggregate_type: str = Field(min_length=1, max_length=64)
    aggregate_id: str = Field(min_length=1, max_length=128)
    correlation_id: str | None = None
    causation_id: uuid.UUID | None = None
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: Any = None

    def to_bytes(self) -> bytes:
        """Serialize to UTF-8 JSON bytes. The on-wire format."""
        # ``model_dump_json`` produces a canonical JSON string
        # consistent with Pydantic v2's encoding (sorted keys via
        # ``by_alias=True`` if you use aliases). Datetimes serialize
        # as ISO 8601 strings.
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> EventEnvelope:
        """Deserialize from on-wire bytes. Raises ``ValueError`` on
        malformed input; ``pydantic.ValidationError`` on schema
        violations.
        """
        obj = json.loads(data.decode("utf-8"))
        return cls.model_validate(obj)

    def content_hash(self) -> str:
        """Stable hash of (event_type, aggregate_type, aggregate_id,
        schema_version, payload). Used by consumers to dedupe
        duplicate deliveries from the broker.
        """
        # Sort keys for determinism so semantically equal events
        # produce the same hash regardless of dict ordering.
        canonical = json.dumps(
            {
                "event_type": self.event_type,
                "aggregate_type": self.aggregate_type,
                "aggregate_id": self.aggregate_id,
                "schema_version": self.schema_version,
                "payload": self.payload,
            },
            sort_keys=True,
            default=str,
        )
        return uuid.uuid5(NIMBUS_EVENT_NAMESPACE, canonical).hex


def make_dedupe_id(payload: Any) -> str:
    """Compute a content-addressed dedupe id for a payload.

    Used by consumer-side handlers to short-circuit duplicate
    processing when the broker redelivers a message (e.g., after
    a consumer crash before the offset was committed).

    Args:
        payload: any JSON-serializable Python value. Typically
            the :attr:`EventEnvelope.payload`.

    Returns:
        A UUID5 hex string. The same payload always produces the
        same id.
    """
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return uuid.uuid5(NIMBUS_EVENT_NAMESPACE, canonical).hex


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PublishResult:
    """The result of a successful publish."""

    topic: str
    partition: int
    offset: int
    event_id: uuid.UUID


class KafkaProducer:
    """Async Kafka producer wrapper.

    The wrapper owns a single :class:`aiokafka.AIOKafkaProducer`
    instance per process. It is safe to call :meth:`publish` from
    multiple coroutines; aiokafka serializes the actual broker
    writes internally. We add a small per-process lock around
    ``start`` / ``stop`` so the wrapper can be re-initialized
    during tests without races.

    Lifecycle:
        >>> producer = KafkaProducer()
        >>> await producer.start()
        >>> try:
        ...     await producer.publish(envelope, topic="payments.events")
        ... finally:
        ...     await producer.stop()
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._producer: AIOKafkaProducer | None = None
        self._lock = asyncio.Lock()
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        """Create and start the underlying aiokafka producer.

        Idempotent: subsequent calls are no-ops. Safe to call from
        FastAPI's ``lifespan`` context.
        """
        async with self._lock:
            if self._started:
                return
            client_id = f"{self._settings.app_name}-{self._settings.environment}"
            producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                client_id=client_id,
                # ``acks=all`` requires all in-sync replicas to
                # acknowledge the write. Slower but durable: the
                # broker doesn't return success until the message
                # is replicated.
                acks="all",
                # ``enable_idempotence=True`` deduplicates within
                # a single producer session. This is distinct from
                # our application-level idempotency (the
                # Idempotency-Key lock); it protects against
                # broker-side retries.
                enable_idempotence=True,
                # Compression reduces network bytes at the cost of
                # CPU on the producer. LZ4 is a good default for
                # JSON-ish payloads.
                compression_type="lz4",
                # Per-request timeout. If the broker doesn't ack
                # within this window, :meth:`send_and_wait` raises
                # :class:`KafkaTimeoutError`.
                request_timeout_ms=self._settings.kafka_request_timeout_ms,
                # Reasonable defaults; tuned in production based
                # on observed throughput.
                linger_ms=5,
                max_batch_size=32 * 1024,
            )
            try:
                await producer.start()
            except (KafkaConnectionError, KafkaError) as exc:
                raise BrokerUnavailableError(
                    "Cannot reach Kafka cluster.",
                    details={
                        "bootstrap_servers": self._settings.kafka_bootstrap_servers,
                        "driver_error": str(exc),
                    },
                ) from exc
            self._producer = producer
            self._started = True
            _log.info(
                "messaging.producer_started",
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                client_id=client_id,  # read from the local var, not producer.client_id
            )

    async def stop(self) -> None:
        """Flush pending messages and stop the producer. Idempotent."""
        async with self._lock:
            if not self._started:
                return
            assert self._producer is not None
            try:
                await self._producer.flush()
            finally:
                await self._producer.stop()
                self._producer = None
                self._started = False
                _log.info("messaging.producer_stopped")

    async def publish(
        self,
        envelope: EventEnvelope,
        *,
        topic: str,
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> PublishResult:
        """Publish an :class:`EventEnvelope` to ``topic``.

        Args:
            envelope: the event to publish.
            topic: Kafka topic name. Convention is
                ``<module>.events`` (e.g., ``payments.events``).
            key: optional partition key. If supplied, the broker
                hashes the key and routes all events with the same
                key to the same partition (preserves per-aggregate
                ordering). Use ``aggregate_id`` for this.
            headers: optional Kafka message headers. Useful for
                trace propagation (``X-Request-ID`` etc.).

        Returns:
            :class:`PublishResult` with the assigned partition and
            offset.

        Raises:
            BrokerUnavailableError: the producer has not been
                started, or the broker is unreachable.
            InfrastructureError: the broker rejected the message
                for some other reason (e.g., the topic doesn't
                exist and auto-create is disabled).
        """
        if not self._started or self._producer is None:
            raise BrokerUnavailableError(
                "Producer has not been started. Call start() first.",
            )
        value = envelope.to_bytes()
        # Default the partition key to the aggregate id so all
        # events for one Payment land on the same partition.
        effective_key = (key or envelope.aggregate_id).encode("utf-8")
        # Convert headers to bytes for aiokafka.
        wire_headers: list[tuple[str, bytes]] = []
        if headers:
            wire_headers = [(k, v.encode("utf-8")) for k, v in headers.items()]
        # Always include the event id and type as headers so
        # consumers can route without deserializing the body.
        wire_headers.extend(
            [
                ("event_id", str(envelope.event_id).encode("utf-8")),
                ("event_type", envelope.event_type.encode("utf-8")),
                ("schema_version", str(envelope.schema_version).encode("utf-8")),
            ]
        )

        try:
            send_coro = await self._producer.send(
                topic=topic,
                value=value,
                key=effective_key,
                headers=wire_headers,
            )
            # ``send`` returns a future of a ``RecordMetadata``;
            # awaiting it gives us partition + offset.
            metadata = await send_coro
        except KafkaTimeoutError as exc:
            raise BrokerUnavailableError(
                "Kafka publish timed out.",
                details={"topic": topic, "driver_error": str(exc)},
            ) from exc
        except KafkaConnectionError as exc:
            raise BrokerUnavailableError(
                "Lost connection to Kafka during publish.",
                details={"topic": topic, "driver_error": str(exc)},
            ) from exc
        except KafkaError as exc:
            raise InfrastructureError(
                "Kafka rejected the message.",
                details={"topic": topic, "driver_error": str(exc)},
            ) from exc

        _log.info(
            "messaging.published",
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
            event_id=str(envelope.event_id),
            event_type=envelope.event_type,
            aggregate_type=envelope.aggregate_type,
            aggregate_id=envelope.aggregate_id,
        )
        return PublishResult(
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
            event_id=envelope.event_id,
        )

    async def publish_many(
        self,
        envelopes_with_topics: Iterable[tuple[EventEnvelope, str]],
    ) -> list[PublishResult]:
        """Publish a batch of envelopes. Best-effort: if any single
        publish fails, the call raises and the caller is responsible
        for retry semantics. The envelope-to-topic tuples are
        iterated lazily.
        """
        results: list[PublishResult] = []
        for envelope, topic in envelopes_with_topics:
            results.append(await self.publish(envelope, topic=topic))
        return results

    async def health_check(self) -> bool:
        """Return True if the producer is started and the broker
        connection is alive. Never raises.
        """
        if not self._started or self._producer is None:
            return False
        try:
            # ``partitions_for`` is a cheap metadata call that
            # round-trips to the broker.
            partitions = await self._producer.partitions_for("__nimbus_health__")
            return partitions is not None
        except Exception as exc:
            _log.warning("messaging.health_check_failed", error=str(exc))
            return False


# ---------------------------------------------------------------------------
# Process-singleton accessor (matches the pattern in database.py,
# cache.py).
# ---------------------------------------------------------------------------

_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    """Return the process-singleton :class:`KafkaProducer`."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer()
    return _producer


async def dispose_producer() -> None:
    """Stop and drop the producer. Idempotent. Used in shutdown."""
    global _producer
    if _producer is not None:
        await _producer.stop()
    _producer = None
