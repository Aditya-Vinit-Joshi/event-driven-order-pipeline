"""Producer API tests via httpx ASGI transport against a fake Redis backend."""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

import app.producer.main as producer
from tests.conftest import make_fake_redis


@pytest.fixture
async def client():
    # Bypass lifespan; inject a fake Redis directly onto app state.
    producer.app.state.redis = make_fake_redis()
    transport = ASGITransport(app=producer.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await producer.app.state.redis.aclose()


async def test_health_ok(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


async def test_post_order_returns_202_and_publishes(client, sample_order_request):
    resp = await client.post("/orders", json=sample_order_request)
    assert resp.status_code == 202
    body = resp.json()
    assert body["order_id"].startswith("ord_")
    assert body["status"] == "created"
    assert resp.headers["Location"] == f"/orders/{body['order_id']}"

    # The event landed on the created stream.
    length = await producer.app.state.redis.xlen(producer.settings.stream_created)
    assert length == 1


async def test_post_order_rejects_bad_payload(client):
    resp = await client.post("/orders", json={"customer_id": "c", "items": []})
    assert resp.status_code == 422
