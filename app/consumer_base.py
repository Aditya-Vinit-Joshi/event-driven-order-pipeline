"""Reusable consumer-group worker loop shared by all three pipeline stages.

Responsibilities handled here so each stage only implements ``process()``:

* ``XREADGROUP`` batch reads with blocking.
* ``XAUTOCLAIM`` recovery of messages abandoned by a crashed consumer
  (Redis Streams' equivalent of Kafka offset recovery via the PEL).
* Exponential-backoff retry, then dead-letter on exhaustion.
* Forwarding the (possibly enriched) event to the next stream, then ``XACK``.
* Distributed tracing: continue the producer's trace via the message headers.
* Graceful shutdown on SIGINT/SIGTERM (finishes in-flight work, no data loss).
"""
from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable

import redis.asyncio as aioredis
from opentelemetry.trace import SpanKind, Status, StatusCode

from .config import Settings
from .errors import PermanentError
from .logging_config import get_logger
from .retry import RetryPolicy, run_with_retry
from .schemas import OrderEvent
from .streams import (
    ensure_group,
    parse_event,
    publish_event,
    send_to_dlq,
)
from .telemetry import extract_trace_context, get_tracer

Processor = Callable[[OrderEvent], Awaitable[OrderEvent]]


class Consumer:
    def __init__(
        self,
        *,
        settings: Settings,
        redis: aioredis.Redis,
        stream: str,
        group: str,
        consumer_name: str,
        processor: Processor,
        next_stream: str | None,
    ) -> None:
        self.settings = settings
        self.redis = redis
        self.stream = stream
        self.group = group
        self.consumer_name = consumer_name
        self.processor = processor
        self.next_stream = next_stream

        self.log = get_logger(f"pipeline.consumer.{group}")
        self.tracer = get_tracer(f"consumer.{group}")
        self.policy = RetryPolicy(
            max_retries=settings.max_retries,
            base_s=settings.backoff_base_s,
            max_s=settings.backoff_max_s,
            jitter_s=settings.backoff_jitter_s,
        )
        self._stop = asyncio.Event()

    # ----------------------------------------------------------------- run --
    async def run(self) -> None:
        await ensure_group(self.redis, self.stream, self.group)
        self._install_signal_handlers()
        self.log.info(
            "consumer started",
            extra={"ctx_stream": self.stream, "ctx_group": self.group,
                   "ctx_consumer": self.consumer_name},
        )
        try:
            while not self._stop.is_set():
                await self._reclaim_stale()
                await self._read_batch()
        finally:
            self.log.info("consumer stopping",
                          extra={"ctx_consumer": self.consumer_name})

    def request_stop(self) -> None:
        self._stop.set()

    # --------------------------------------------------------------- reads --
    async def _read_batch(self) -> None:
        # block<=0 -> non-blocking read (used in tests); >0 -> long-poll.
        kwargs: dict = {}
        if self.settings.block_ms > 0:
            kwargs["block"] = self.settings.block_ms
        resp = await self.redis.xreadgroup(
            groupname=self.group,
            consumername=self.consumer_name,
            streams={self.stream: ">"},
            count=self.settings.read_count,
            **kwargs,
        )
        if not resp:
            return
        for _stream, messages in resp:
            for msg_id, fields in messages:
                await self._handle(msg_id, fields)

    async def _reclaim_stale(self) -> None:
        """Reclaim messages another consumer read but never ack'd (crash)."""
        try:
            _next, claimed, _deleted = await self.redis.xautoclaim(
                name=self.stream,
                groupname=self.group,
                consumername=self.consumer_name,
                min_idle_time=self.settings.idle_reclaim_ms,
                start_id="0-0",
                count=self.settings.reclaim_count,
            )
        except aioredis.ResponseError:
            return
        for msg_id, fields in claimed:
            if fields:  # XAUTOCLAIM can yield tombstones with empty fields
                self.log.info("reclaimed stale message",
                              extra={"ctx_msg_id": msg_id})
                await self._handle(msg_id, fields, reclaimed=True)

    # -------------------------------------------------------------- handle --
    async def _handle(
        self, msg_id: str, fields: dict[str, str], *, reclaimed: bool = False
    ) -> None:
        parent = extract_trace_context(fields)
        with self.tracer.start_as_current_span(
            f"{self.group} process",
            context=parent,
            kind=SpanKind.CONSUMER,
        ) as span:
            span.set_attribute("messaging.system", "redis")
            span.set_attribute("messaging.source", self.stream)
            span.set_attribute("messaging.message.id", msg_id)
            span.set_attribute("reclaimed", reclaimed)

            attempts = {"n": 0}

            try:
                event = parse_event(fields)
            except Exception as exc:  # malformed body — cannot ever succeed
                span.set_status(Status(StatusCode.ERROR))
                await self._dead_letter(fields, "malformed_message",
                                        str(exc), attempts=1)
                await self._ack(msg_id)
                return

            span.set_attribute("order.id", event.order_id)

            async def _attempt() -> OrderEvent:
                attempts["n"] += 1
                return await self.processor(event)

            try:
                result = await run_with_retry(
                    _attempt, self.policy, op=self.group
                )
            except PermanentError as exc:
                span.set_status(Status(StatusCode.ERROR, "permanent"))
                await self._dead_letter(
                    fields, exc.reason_code, str(exc), attempts["n"]
                )
                await self._ack(msg_id)
                return
            except Exception as exc:  # noqa: BLE001
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, "retries exhausted"))
                reason = getattr(exc, "reason_code", "processing_error")
                await self._dead_letter(fields, reason, str(exc), attempts["n"])
                await self._ack(msg_id)
                return

            # Success: forward downstream (if any) then acknowledge.
            if self.next_stream:
                await publish_event(self.redis, self.next_stream, result)
            await self._ack(msg_id)
            span.set_status(Status(StatusCode.OK))

    # ------------------------------------------------------------- helpers --
    async def _ack(self, msg_id: str) -> None:
        await self.redis.xack(self.stream, self.group, msg_id)

    async def _dead_letter(
        self,
        fields: dict[str, str],
        reason_code: str,
        error: str,
        attempts: int,
    ) -> None:
        await send_to_dlq(
            self.redis,
            self.settings.stream_dlq,
            source_stream=self.stream,
            original=fields,
            reason_code=reason_code,
            error=error,
            attempts=attempts,
        )

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except NotImplementedError:  # pragma: no cover - Windows fallback
                pass


async def run_consumer(
    *,
    settings: Settings,
    stream: str,
    group: str,
    consumer_name: str,
    processor: Processor,
    next_stream: str | None,
    redis: aioredis.Redis,
) -> None:
    consumer = Consumer(
        settings=settings,
        redis=redis,
        stream=stream,
        group=group,
        consumer_name=consumer_name,
        processor=processor,
        next_stream=next_stream,
    )
    await consumer.run()
