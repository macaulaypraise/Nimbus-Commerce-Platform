"""Base Kafka consumer with manual offset commit and dedupe.

Provides :class:`KafkaConsumer`, a long-running worker that
subscribes to Kafka topics, deserializes EventEnvelopes,
checks dedupe, and routes to handler functions. Subclass and
override `_register_handlers` to add per-(topic, event_type)
handler mappings.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata, TopicPartition

from src.core.config import Settings, get_settings
from src.core.dedupe import RedisDedupe
from src.core.exceptions import BrokerUnavailableError
from src.core.messaging import EventEnvelope

if TYPE_CHECKING:
    from src.core.protocols import RedisLike

_log = structlog.get_logger("nimbus.consumer")

# Handler signature.
EventHandler = Callable[[EventEnvelope, Any], Any]


class KafkaConsumer:
    """Long-running consumer with dedupe and manual commits."""

    def __init__(
        self,
        *,
        topics: list[str],
        redis: RedisLike,
        breaker: Any,
        settings: Settings | None = None,
        consumer_group: str | None = None,
        session_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._topics = topics
        self._redis = redis
        self._breaker = breaker
        self._settings = settings or get_settings()
        self._consumer_group = consumer_group or self._settings.kafka_consumer_group
        self._session_factory = session_factory
        self._handlers: dict[tuple[str, str], EventHandler] = {}
        self._dedupe: RedisDedupe | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        if not self._settings.kafka_consumer_enabled:
            return

        self._stop_event.clear()
        self._dedupe = RedisDedupe(
            redis=self._redis, breaker=self._breaker, settings=self._settings
        )
        self._register_handlers()
        self._task = asyncio.create_task(
            self._run_forever(), name=f"nimbus.consumer.{self._consumer_group}"
        )
        _log.info(
            "consumer.started",
            topics=self._topics,
            group=self._consumer_group,
            handler_count=len(self._handlers),
        )

    async def stop(self, *, shutdown_timeout: float | None = None) -> None:
        if self._task is None:
            return

        self._stop_event.set()
        try:
            await asyncio.wait_for(self._task, timeout=shutdown_timeout or 10.0)
        except TimeoutError:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
        finally:
            await self._close_consumer()
            self._task = None
            _log.info("consumer.stopped", group=self._consumer_group)

    async def _close_consumer(self) -> None:
        if self._consumer is not None:
            with contextlib.suppress(Exception):
                await self._consumer.stop()
            self._consumer = None

    def _register_handlers(self) -> None:
        """Override in subclasses to populate ``self._handlers``."""
        pass

    async def _run_forever(self) -> None:
        try:
            await self._connect()
            while not self._stop_event.is_set():
                try:
                    await self._poll_and_dispatch()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _log.error(
                        "consumer.poll_failed",
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
                    await self._sleep(1.0)
        except asyncio.CancelledError:
            _log.info("consumer.cancelled")
            raise

    async def _connect(self) -> None:
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._consumer_group,
            client_id=(
                f"{self._settings.app_name}-{self._settings.environment}-"
                f"consumer-{self._consumer_group}"
            ),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            request_timeout_ms=self._settings.kafka_request_timeout_ms,
        )
        try:
            await self._consumer.start()
        except KafkaError as exc:
            raise BrokerUnavailableError(
                "Cannot connect to Kafka cluster.",
                details={
                    "bootstrap_servers": self._settings.kafka_bootstrap_servers,
                    "driver_error": str(exc),
                },
            ) from exc

        self._consumer.subscribe(topics=self._topics)
        _log.info("consumer.connected", topics=self._topics, group=self._consumer_group)

    async def _poll_and_dispatch(self) -> None:
        if self._consumer is None:
            return

        records = await self._consumer.getmany(timeout_ms=1000, max_records=100)
        if not records:
            return

        committed_offsets: dict[TopicPartition, int] = {}
        for tp, batch in records.items():
            for record in batch:
                try:
                    await self._handle_message(record)
                    committed_offsets[tp] = record.offset + 1
                except Exception as exc:
                    _log.error(
                        "consumer.handler_failed",
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
                    # Don't update committed_offsets; message
                    # will be redelivered.

        if committed_offsets and self._consumer is not None:
            try:
                await self._consumer.commit(
                    {tp: OffsetAndMetadata(offset, "") for tp, offset in committed_offsets.items()}
                )
            except Exception as exc:
                _log.error(
                    "consumer.commit_failed",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    async def _handle_message(self, record: ConsumerRecord) -> None:
        if self._dedupe is None:
            raise RuntimeError("dedupe not initialized; call start() first")

        if record.value is None:
            _log.warning(
                "consumer.tombstone_skip",
                topic=record.topic,
                offset=record.offset,
            )
            return

        try:
            envelope = EventEnvelope.from_bytes(record.value)
        except Exception as exc:
            _log.error(
                "consumer.malformed_envelope",
                topic=record.topic,
                offset=record.offset,
                error=str(exc),
            )
            return  # poison message; skip

        if not await self._dedupe.check_and_record(envelope.content_hash()):
            _log.info(
                "consumer.dedupe_skip",
                dedupe_id=envelope.content_hash(),
                event_type=envelope.event_type,
            )
            return

        handler = self._handlers.get((record.topic, envelope.event_type))
        if handler is None:
            _log.warning(
                "consumer.no_handler",
                topic=record.topic,
                event_type=envelope.event_type,
            )
            return

        if self._session_factory is None:
            raise RuntimeError("session_factory not configured")

        async with self._session_factory() as session:
            try:
                await handler(envelope, session)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
