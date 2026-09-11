"""Validation consumer — first business-logic stage.

Reads ``orders:created``, applies business rules, and forwards passing orders
to ``orders:validated``. Rule violations raise :class:`ValidationRejected`
(a permanent error) so they skip retries and go straight to the DLQ, while a
chaos-injected fault raises a transient error to exercise the retry path.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from ..chaos import maybe_fail
from ..config import get_settings
from ..consumer_base import run_consumer
from ..errors import ValidationRejected
from ..logging_config import get_logger
from ..schemas import OrderEvent, OrderStatus
from ..streams import create_redis
from ._runner import bootstrap, consumer_name

settings = get_settings()
log = get_logger("pipeline.validation")

# Guardrails for basic order sanity checks.
MAX_UNIT_PRICE = Decimal("100000.00")
MAX_ORDER_TOTAL = Decimal("1000000.00")


async def validate(event: OrderEvent) -> OrderEvent:
    maybe_fail(settings.fail_rate, where="validation")

    if not event.items:
        raise ValidationRejected("order has no line items")

    recomputed = sum(
        (i.unit_price * i.quantity for i in event.items), Decimal("0")
    )
    for item in event.items:
        if item.quantity <= 0:
            raise ValidationRejected(f"non-positive quantity for {item.product_id}")
        if item.unit_price <= 0 or item.unit_price > MAX_UNIT_PRICE:
            raise ValidationRejected(f"price out of range for {item.product_id}")

    if recomputed != event.subtotal:
        raise ValidationRejected(
            f"subtotal mismatch: claimed {event.subtotal}, computed {recomputed}"
        )
    if recomputed > MAX_ORDER_TOTAL:
        raise ValidationRejected(f"order total {recomputed} exceeds limit")

    event.status = OrderStatus.VALIDATED
    event.validated_at = datetime.now(timezone.utc)
    log.info("order validated", extra={"ctx_order_id": event.order_id})
    return event


async def main() -> None:
    bootstrap(settings)
    redis = create_redis(settings)
    await run_consumer(
        settings=settings,
        stream=settings.stream_created,
        group=settings.group_validation,
        consumer_name=consumer_name("validation"),
        processor=validate,
        next_stream=settings.stream_validated,
        redis=redis,
    )


if __name__ == "__main__":
    asyncio.run(main())
