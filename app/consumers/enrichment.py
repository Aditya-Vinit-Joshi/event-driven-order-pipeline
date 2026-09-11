"""Enrichment consumer — second stage.

Reads ``orders:validated`` and attaches mock pricing (tax), a grand total, and
a mock inventory/warehouse assignment, then forwards to ``orders:enriched``.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

from ..chaos import maybe_fail
from ..config import get_settings
from ..consumer_base import run_consumer
from ..logging_config import get_logger
from ..schemas import Enrichment, OrderEvent, OrderStatus
from ..streams import create_redis
from ._runner import bootstrap, consumer_name

settings = get_settings()
log = get_logger("pipeline.enrichment")

TAX_RATE = Decimal("0.0925")  # mock blended sales-tax rate
_WAREHOUSES = ("us-west-1", "us-east-1", "eu-central-1")


def _pick_warehouse(order_id: str) -> str:
    return _WAREHOUSES[hash(order_id) % len(_WAREHOUSES)]


async def enrich(event: OrderEvent) -> OrderEvent:
    maybe_fail(settings.fail_rate, where="enrichment")

    tax = (event.subtotal * TAX_RATE).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    event.enrichment = Enrichment(
        tax_amount=tax,
        grand_total=event.subtotal + tax,
        warehouse=_pick_warehouse(event.order_id),
        in_stock=True,
    )
    event.status = OrderStatus.ENRICHED
    event.enriched_at = datetime.now(timezone.utc)
    log.info(
        "order enriched",
        extra={"ctx_order_id": event.order_id,
               "ctx_grand_total": str(event.enrichment.grand_total)},
    )
    return event


async def main() -> None:
    bootstrap(settings)
    redis = create_redis(settings)
    await run_consumer(
        settings=settings,
        stream=settings.stream_validated,
        group=settings.group_enrichment,
        consumer_name=consumer_name("enrichment"),
        processor=enrich,
        next_stream=settings.stream_enriched,
        redis=redis,
    )


if __name__ == "__main__":
    asyncio.run(main())
