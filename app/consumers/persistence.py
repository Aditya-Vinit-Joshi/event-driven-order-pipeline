"""Persistence consumer — terminal stage.

Reads ``orders:enriched`` and writes each order to PostgreSQL with an
idempotent upsert (unique ``order_id``), so at-least-once delivery never
produces duplicate rows. There is no downstream stream — this is the end of
the pipeline.
"""
from __future__ import annotations

import asyncio

from ..chaos import maybe_fail
from ..config import get_settings
from ..consumer_base import run_consumer
from ..db import Database
from ..logging_config import get_logger
from ..schemas import OrderEvent, OrderStatus
from ..streams import create_redis
from ..telemetry import instrument_asyncpg
from ._runner import bootstrap, consumer_name

settings = get_settings()
log = get_logger("pipeline.persistence")


def make_processor(db: Database):
    async def persist(event: OrderEvent) -> OrderEvent:
        maybe_fail(settings.fail_rate, where="persistence")
        event.status = OrderStatus.PERSISTED
        inserted = await db.upsert_order(event)
        log.info(
            "order persisted",
            extra={"ctx_order_id": event.order_id,
                   "ctx_inserted": inserted},
        )
        return event

    return persist


async def main() -> None:
    bootstrap(settings)
    instrument_asyncpg()
    db = Database(settings)
    await db.connect()
    redis = create_redis(settings)
    try:
        await run_consumer(
            settings=settings,
            stream=settings.stream_enriched,
            group=settings.group_persistence,
            consumer_name=consumer_name("persistence"),
            processor=make_processor(db),
            next_stream=None,  # terminal stage
            redis=redis,
        )
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
