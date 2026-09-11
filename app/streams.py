"""Redis Streams helpers: connection, publishing, DLQ, and group bootstrap.

All stream I/O funnels through here so message encoding (JSON body +
``traceparent`` propagation fields) is identical everywhere.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import redis.asyncio as aioredis

from .config import Settings
from .logging_config import get_logger
from .schemas import OrderEvent
from .telemetry import inject_trace_context

log = get_logger("pipeline.streams")

# Fields reserved for the framework; everything else is user data.
_BODY_FIELD = "data"


def create_redis(settings: Settings) -> aioredis.Redis:
    """Create an async Redis client that decodes responses to ``str``."""
    return aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        health_check_interval=30,
    )


async def ensure_group(
    redis: aioredis.Redis, stream: str, group: str
) -> None:
    """Create the consumer group (and the stream) if it doesn't exist yet."""
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
        log.info("created consumer group",
                 extra={"ctx_stream": stream, "ctx_group": group})
    except aioredis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _event_fields(event: OrderEvent) -> dict[str, str]:
    fields = {_BODY_FIELD: event.to_json()}
    # Attach the current span so downstream consumers continue the trace.
    return inject_trace_context(fields)


async def publish_event(
    redis: aioredis.Redis,
    stream: str,
    event: OrderEvent,
    *,
    maxlen: int | None = None,
) -> str:
    """XADD an :class:`OrderEvent` (approx-trimmed) and return its message id."""
    fields = _event_fields(event)
    kwargs: dict = {}
    if maxlen is not None:
        kwargs.update(maxlen=maxlen, approximate=True)
    msg_id = await redis.xadd(stream, fields, **kwargs)
    return msg_id


def parse_event(fields: dict[str, str]) -> OrderEvent:
    return OrderEvent.from_json(fields[_BODY_FIELD])


async def send_to_dlq(
    redis: aioredis.Redis,
    dlq_stream: str,
    *,
    source_stream: str,
    original: dict[str, str],
    reason_code: str,
    error: str,
    attempts: int,
) -> str:
    """Preserve a failed message plus failure metadata in the DLQ stream."""
    entry = {
        "source_stream": source_stream,
        "reason_code": reason_code,
        "error": error[:1000],
        "attempts": str(attempts),
        "failed_at": datetime.now(timezone.utc).isoformat(),
        # Keep the raw body so the message can be replayed later.
        "original": json.dumps(original),
    }
    msg_id = await redis.xadd(dlq_stream, entry)
    log.warning(
        "message dead-lettered",
        extra={"ctx_source": source_stream, "ctx_reason": reason_code,
               "ctx_attempts": attempts, "ctx_error": error[:200]},
    )
    return msg_id
