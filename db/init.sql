-- Schema for the persistence consumer's target table.
-- The consumer also creates this at startup (idempotently), so this file is
-- mainly for psql inspection and to make the schema explicit/reviewable.

CREATE TABLE IF NOT EXISTS orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT          NOT NULL,
    status        TEXT          NOT NULL,
    currency      CHAR(3)       NOT NULL,
    item_count    INTEGER       NOT NULL,
    subtotal      NUMERIC(14,2) NOT NULL,
    tax_amount    NUMERIC(14,2) NOT NULL DEFAULT 0,
    grand_total   NUMERIC(14,2) NOT NULL DEFAULT 0,
    warehouse     TEXT,
    payload       JSONB         NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status   ON orders (status);
