"""OpenTelemetry wiring + trace-context propagation across Redis Streams.

Auto-instrumentation captures FastAPI, redis and asyncpg spans. But a message
bus breaks the automatic in-process context chain, so we manually
``inject``/``extract`` W3C ``traceparent`` headers into the stream fields. That
is what stitches "API request -> publish -> consume -> DB write" into one
end-to-end trace in Jaeger.
"""
from __future__ import annotations

from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Tracer

from .config import Settings

_CARRIER_KEYS = ("traceparent", "tracestate")
_initialised = False


def setup_telemetry(settings: Settings) -> None:
    """Idempotently configure the global tracer provider + OTLP exporter."""
    global _initialised
    if _initialised:
        return

    resource = Resource.create({SERVICE_NAME: settings.service_name})
    provider = TracerProvider(resource=resource)

    if settings.otel_enabled:
        # Imported lazily so unit tests don't need the gRPC exporter installed.
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )

        exporter = OTLPSpanExporter(
            endpoint=settings.otel_exporter_otlp_endpoint, insecure=True
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)
    _initialised = True


def instrument_fastapi(app) -> None:
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except Exception:  # pragma: no cover - instrumentation is best-effort
        pass


def instrument_redis() -> None:
    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentor

        RedisInstrumentor().instrument()
    except Exception:  # pragma: no cover - instrumentation is best-effort
        pass


def instrument_asyncpg() -> None:
    try:
        from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor

        AsyncPGInstrumentor().instrument()
    except Exception:  # pragma: no cover
        pass


def get_tracer(name: str) -> Tracer:
    return trace.get_tracer(name)


# --------------------------------------------------------------------------- #
# Context propagation helpers                                                 #
# --------------------------------------------------------------------------- #
def inject_trace_context(fields: dict[str, str]) -> dict[str, str]:
    """Add W3C trace headers for the *current* span into stream ``fields``."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    for key in _CARRIER_KEYS:
        if key in carrier:
            fields[key] = carrier[key]
    return fields


def extract_trace_context(fields: dict[str, str]) -> Context | None:
    """Rebuild the parent :class:`Context` from stream ``fields`` (if present)."""
    carrier = {key: fields[key] for key in _CARRIER_KEYS if key in fields}
    if not carrier:
        return None
    return propagate.extract(carrier)
