from decimal import Decimal

import pytest

from app.consumers.enrichment import enrich
from app.consumers.validation import validate
from app.errors import ValidationRejected
from app.schemas import OrderCreateRequest, OrderEvent, OrderStatus


def _event(**overrides) -> OrderEvent:
    req = OrderCreateRequest(
        customer_id="cust_1",
        currency="USD",
        items=[{"product_id": "sku-1", "quantity": 2, "unit_price": 10.00}],
    )
    event = OrderEvent.from_request(req)
    for key, value in overrides.items():
        setattr(event, key, value)
    return event


async def test_validate_accepts_good_order():
    result = await validate(_event())
    assert result.status == OrderStatus.VALIDATED
    assert result.validated_at is not None


async def test_validate_rejects_subtotal_tampering():
    bad = _event(subtotal=Decimal("1.00"))  # real subtotal is 20.00
    with pytest.raises(ValidationRejected):
        await validate(bad)


async def test_enrich_adds_tax_and_total():
    event = await validate(_event())
    enriched = await enrich(event)
    assert enriched.status == OrderStatus.ENRICHED
    assert enriched.enrichment is not None
    # 20.00 * 9.25% = 1.85
    assert enriched.enrichment.tax_amount == Decimal("1.85")
    assert enriched.enrichment.grand_total == Decimal("21.85")
    assert enriched.enrichment.warehouse is not None
