from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.schemas import OrderCreateRequest, OrderEvent, OrderStatus


def test_from_request_computes_subtotal(sample_order_request):
    req = OrderCreateRequest(**sample_order_request)
    event = OrderEvent.from_request(req)
    assert event.subtotal == Decimal("44.98")  # 2*19.99 + 1*5.00
    assert event.currency == "USD"  # normalised to upper-case
    assert event.status == OrderStatus.CREATED
    assert event.order_id.startswith("ord_")
    assert event.item_count == 3


def test_json_roundtrip(sample_order_request):
    event = OrderEvent.from_request(OrderCreateRequest(**sample_order_request))
    restored = OrderEvent.from_json(event.to_json())
    assert restored.order_id == event.order_id
    assert restored.subtotal == event.subtotal
    assert restored.items == event.items


def test_rejects_empty_items():
    with pytest.raises(ValidationError):
        OrderCreateRequest(customer_id="c", currency="USD", items=[])


def test_rejects_negative_quantity():
    with pytest.raises(ValidationError):
        OrderCreateRequest(
            customer_id="c",
            currency="USD",
            items=[{"product_id": "p", "quantity": -1, "unit_price": 1.0}],
        )


def test_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        OrderCreateRequest(
            customer_id="c",
            currency="USD",
            items=[{"product_id": "p", "quantity": 1, "unit_price": 1.0}],
            surprise="boom",
        )
