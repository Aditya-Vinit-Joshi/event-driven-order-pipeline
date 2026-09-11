"""PostgreSQL access via an asyncpg connection pool.

Persistence is idempotent: the ``orders`` table keys on ``order_id`` and writes
use ``INSERT ... ON CONFLICT (order_id) DO UPDATE``. Redis Streams guarantee
*at-least-once* delivery, so a duplicate consume simply refreshes the row
instead of erroring or double-inserting.
"""
from __future__ import annotations

import asyncpg

from .config import Settings
from .logging_config import get_logger
from .schemas import OrderEvent

log = get_logger("pipeline.db")

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT        NOT NULL,
    status        TEXT        NOT NULL,
    currency      CHAR(3)     NOT NULL,
    item_count    INTEGER     NOT NULL,
    subtotal      NUMERIC(14,2) NOT NULL,
    tax_amount    NUMERIC(14,2) NOT NULL DEFAULT 0,
    grand_total   NUMERIC(14,2) NOT NULL DEFAULT 0,
    warehouse     TEXT,
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status   ON orders (status);
"""

UPSERT_SQL = """
INSERT INTO orders (
    order_id, customer_id, status, currency, item_count,
    subtotal, tax_amount, grand_total, warehouse, payload, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, now()
)
ON CONFLICT (order_id) DO UPDATE SET
    status      = EXCLUDED.status,
    tax_amount  = EXCLUDED.tax_amount,
    grand_total = EXCLUDED.grand_total,
    warehouse   = EXCLUDED.warehouse,
    payload     = EXCLUDED.payload,
    updated_at  = now()
RETURNING (xmax = 0) AS inserted;
"""


class Database:
    """Thin async wrapper around an asyncpg pool."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=self._settings.database_url,
            min_size=self._settings.db_pool_min,
            max_size=self._settings.db_pool_max,
            command_timeout=10,
        )
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_DDL)
        log.info("database pool ready")

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def upsert_order(self, event: OrderEvent) -> bool:
        """Idempotently persist ``event``. Returns True if a new row was inserted."""
        assert self._pool is not None, "connect() must be called first"
        enr = event.enrichment
        tax = enr.tax_amount if enr else 0
        grand = enr.grand_total if enr else event.subtotal
        warehouse = enr.warehouse if enr else None

        async with self._pool.acquire() as conn:
            inserted = await conn.fetchval(
                UPSERT_SQL,
                event.order_id,
                event.customer_id,
                event.status,
                event.currency,
                event.item_count,
                event.subtotal,
                tax,
                grand,
                warehouse,
                event.to_json(),
                event.created_at,
            )
        return bool(inserted)

    async def count_orders(self) -> int:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            return await conn.fetchval("SELECT count(*) FROM orders")
