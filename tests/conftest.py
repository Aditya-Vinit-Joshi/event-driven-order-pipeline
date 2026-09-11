"""Shared test fixtures.

Telemetry export is disabled for tests so no gRPC collector is required, and a
fake in-memory Redis stands in for a real broker.
"""
from __future__ import annotations

import os

# Must be set before any app module reads settings.
os.environ.setdefault("OTEL_ENABLED", "false")
os.environ.setdefault("FAIL_RATE", "0.0")
os.environ.setdefault("LOG_LEVEL", "WARNING")
# Keep retry-based tests fast (no real backoff sleeps).
os.environ.setdefault("BACKOFF_BASE_S", "0.0")
os.environ.setdefault("BACKOFF_MAX_S", "0.0")
os.environ.setdefault("BACKOFF_JITTER_S", "0.0")
# Non-blocking reads: fakeredis doesn't honour blocking XREADGROUP on ">".
os.environ.setdefault("BLOCK_MS", "0")

import fakeredis  # noqa: E402
import fakeredis.aioredis  # noqa: E402
import pytest  # noqa: E402

from app.config import get_settings  # noqa: E402


def make_fake_redis():
    """Fresh, isolated fake Redis (its own server so tests don't share state)."""
    return fakeredis.aioredis.FakeRedis(
        server=fakeredis.FakeServer(), decode_responses=True
    )


@pytest.fixture
def settings():
    return get_settings()


@pytest.fixture
async def redis():
    client = make_fake_redis()
    yield client
    await client.aclose()


@pytest.fixture
def sample_order_request() -> dict:
    return {
        "customer_id": "cust_123",
        "currency": "usd",
        "items": [
            {"product_id": "sku-1", "quantity": 2, "unit_price": 19.99},
            {"product_id": "sku-2", "quantity": 1, "unit_price": 5.00},
        ],
    }
