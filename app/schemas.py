"""Pydantic models for API I/O and the event envelope carried on the streams.

The pipeline speaks a single JSON ``OrderEvent`` that is progressively
enriched as it moves through the stages:

    created -> validated -> enriched -> persisted
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_order_id() -> str:
    return f"ord_{uuid.uuid4().hex[:24]}"


class OrderStatus(str, Enum):
    CREATED = "created"
    VALIDATED = "validated"
    ENRICHED = "enriched"
    PERSISTED = "persisted"
    REJECTED = "rejected"


# --------------------------------------------------------------------------- #
# Inbound API models                                                          #
# --------------------------------------------------------------------------- #
class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: str = Field(..., min_length=1, max_length=64)
    quantity: int = Field(..., gt=0, le=10_000)
    unit_price: Decimal = Field(..., gt=0, max_digits=12, decimal_places=2)


class OrderCreateRequest(BaseModel):
    """Payload accepted by ``POST /orders``."""

    model_config = ConfigDict(extra="forbid")

    customer_id: str = Field(..., min_length=1, max_length=64)
    currency: str = Field("USD", min_length=3, max_length=3)
    items: list[OrderItem] = Field(..., min_length=1, max_length=100)

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()


class OrderAccepted(BaseModel):
    """``202 Accepted`` response body — processing continues asynchronously."""

    order_id: str
    status: OrderStatus
    accepted_at: datetime


# --------------------------------------------------------------------------- #
# Event envelope (what actually travels on the Redis Streams)                 #
# --------------------------------------------------------------------------- #
class Enrichment(BaseModel):
    """Values attached by the enrichment consumer."""

    tax_amount: Decimal = Field(default=Decimal("0"))
    grand_total: Decimal = Field(default=Decimal("0"))
    warehouse: str | None = None
    in_stock: bool = True


class OrderEvent(BaseModel):
    """Canonical event serialised onto every stream in the pipeline."""

    model_config = ConfigDict(use_enum_values=True)

    order_id: str = Field(default_factory=_new_order_id)
    customer_id: str
    currency: str
    items: list[OrderItem]
    subtotal: Decimal
    status: OrderStatus = OrderStatus.CREATED
    enrichment: Enrichment | None = None

    created_at: datetime = Field(default_factory=_utcnow)
    validated_at: datetime | None = None
    enriched_at: datetime | None = None

    # --- helpers ----------------------------------------------------------
    @classmethod
    def from_request(cls, req: OrderCreateRequest) -> OrderEvent:
        subtotal = sum(
            (item.unit_price * item.quantity for item in req.items),
            Decimal("0"),
        )
        return cls(
            customer_id=req.customer_id,
            currency=req.currency,
            items=req.items,
            subtotal=subtotal,
        )

    @property
    def item_count(self) -> int:
        return sum(item.quantity for item in self.items)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str) -> OrderEvent:
        return cls.model_validate_json(raw)
