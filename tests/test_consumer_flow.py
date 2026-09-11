"""Consumer-loop tests over a fake Redis: forward-on-success, DLQ-on-failure."""
from __future__ import annotations

from app.consumer_base import Consumer
from app.consumers.validation import validate
from app.errors import ValidationRejected
from app.schemas import OrderCreateRequest, OrderEvent
from app.streams import ensure_group, parse_event, publish_event


def _make_event() -> OrderEvent:
    return OrderEvent.from_request(
        OrderCreateRequest(
            customer_id="cust_1",
            currency="USD",
            items=[{"product_id": "sku-1", "quantity": 2, "unit_price": 10.0}],
        )
    )


async def _pending_count(redis, stream, group) -> int:
    info = await redis.xpending(stream, group)
    # xpending summary -> {'pending': N, ...}
    return info["pending"] if isinstance(info, dict) else info[0]


async def test_success_forwards_and_acks(settings, redis):
    await ensure_group(redis, settings.stream_created, settings.group_validation)
    await publish_event(redis, settings.stream_created, _make_event())

    consumer = Consumer(
        settings=settings,
        redis=redis,
        stream=settings.stream_created,
        group=settings.group_validation,
        consumer_name="test-worker",
        processor=validate,
        next_stream=settings.stream_validated,
    )
    await consumer._read_batch()

    # Forwarded downstream, original acknowledged, nothing dead-lettered.
    forwarded = await redis.xrange(settings.stream_validated)
    assert len(forwarded) == 1
    assert parse_event(forwarded[0][1]).status == "validated"
    assert await _pending_count(redis, settings.stream_created,
                                settings.group_validation) == 0
    assert await redis.xlen(settings.stream_dlq) == 0


async def test_permanent_failure_goes_to_dlq(settings, redis):
    await ensure_group(redis, settings.stream_created, settings.group_validation)
    await publish_event(redis, settings.stream_created, _make_event())

    async def always_reject(_event):
        raise ValidationRejected("bad order")

    consumer = Consumer(
        settings=settings,
        redis=redis,
        stream=settings.stream_created,
        group=settings.group_validation,
        consumer_name="test-worker",
        processor=always_reject,
        next_stream=settings.stream_validated,
    )
    await consumer._read_batch()

    # Dead-lettered, acked, not forwarded.
    assert await redis.xlen(settings.stream_dlq) == 1
    dlq = await redis.xrange(settings.stream_dlq)
    assert dlq[0][1]["reason_code"] == "validation_rejected"
    assert await redis.xlen(settings.stream_validated) == 0
    assert await _pending_count(redis, settings.stream_created,
                                settings.group_validation) == 0


async def test_transient_failure_retries_then_dlq(settings, redis):
    await ensure_group(redis, settings.stream_created, settings.group_validation)
    await publish_event(redis, settings.stream_created, _make_event())

    calls = {"n": 0}

    async def flaky(_event):
        calls["n"] += 1
        raise RuntimeError("transient boom")

    consumer = Consumer(
        settings=settings,
        redis=redis,
        stream=settings.stream_created,
        group=settings.group_validation,
        consumer_name="test-worker",
        processor=flaky,
        next_stream=settings.stream_validated,
    )
    await consumer._read_batch()

    # Retried up to the policy limit, then dead-lettered.
    assert calls["n"] == settings.max_retries
    assert await redis.xlen(settings.stream_dlq) == 1
