"""Event-driven order processing pipeline.

A cloud-agnostic pipeline built on FastAPI + Redis Streams + PostgreSQL:

    Producer API  ->  orders:created  ->  Validation  ->  orders:validated
                  ->  Enrichment      ->  orders:enriched
                  ->  Persistence      ->  PostgreSQL

Failed messages land in ``orders:dlq`` after exponential-backoff retries.
All services emit OpenTelemetry traces stitched together across the streams.
"""

__version__ = "1.0.0"
