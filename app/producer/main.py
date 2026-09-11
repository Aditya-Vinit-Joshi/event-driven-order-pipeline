"""Producer API — the pipeline's front door.

``POST /orders`` validates the payload with Pydantic, publishes an
``order.created`` event to the ``orders:created`` Redis Stream, and returns
``202 Accepted`` immediately. Persistence happens asynchronously downstream, so
the API stays fast and is decoupled from validation/enrichment/DB latency.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse

from ..config import get_settings
from ..logging_config import configure_logging, get_logger
from ..schemas import OrderAccepted, OrderCreateRequest, OrderEvent, OrderStatus
from ..streams import create_redis, publish_event
from ..telemetry import (
    get_tracer,
    instrument_fastapi,
    instrument_redis,
    setup_telemetry,
)

settings = get_settings()
configure_logging(settings.log_level)
log = get_logger("pipeline.producer")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_telemetry(settings)
    instrument_redis()
    app.state.redis = create_redis(settings)
    await app.state.redis.ping()
    log.info("producer ready", extra={"ctx_stream": settings.stream_created})
    yield
    await app.state.redis.aclose()


app = FastAPI(
    title="Order Pipeline — Producer API",
    version="1.0.0",
    lifespan=lifespan,
)
setup_telemetry(settings)
instrument_fastapi(app)
tracer = get_tracer("producer")


@app.get("/health", tags=["ops"])
async def health() -> dict:
    try:
        await app.state.redis.ping()
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "degraded", "error": str(exc)},
        )


@app.post(
    "/orders",
    response_model=OrderAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["orders"],
)
async def create_order(req: OrderCreateRequest, response: Response) -> OrderAccepted:
    event = OrderEvent.from_request(req)
    with tracer.start_as_current_span("publish order.created") as span:
        span.set_attribute("order.id", event.order_id)
        span.set_attribute("order.customer_id", event.customer_id)
        span.set_attribute("order.subtotal", float(event.subtotal))
        msg_id = await publish_event(
            app.state.redis,
            settings.stream_created,
            event,
            maxlen=settings.stream_maxlen,
        )
        span.set_attribute("messaging.message.id", msg_id)

    log.info(
        "order accepted",
        extra={"ctx_order_id": event.order_id, "ctx_msg_id": msg_id},
    )
    response.headers["Location"] = f"/orders/{event.order_id}"
    return OrderAccepted(
        order_id=event.order_id,
        status=OrderStatus.CREATED,
        accepted_at=datetime.now(timezone.utc),
    )
