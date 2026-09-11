"""Centralised, environment-driven configuration.

Every service reads the same :class:`Settings` object so that stream names,
consumer-group names, retry policy and telemetry endpoints stay consistent
across the producer and all three consumers.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Service identity (overridden per-container) -----------------------
    service_name: str = "order-pipeline"
    log_level: str = "INFO"

    # --- Redis / streams ---------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    stream_created: str = "orders:created"
    stream_validated: str = "orders:validated"
    stream_enriched: str = "orders:enriched"
    stream_dlq: str = "orders:dlq"

    # Consumer-group names (one group per stage, many consumers per group).
    group_validation: str = "validation-workers"
    group_enrichment: str = "enrichment-workers"
    group_persistence: str = "persistence-workers"

    # Trim the created stream so it can't grow unbounded under load.
    stream_maxlen: int = 100_000

    # --- Consumer loop tuning ---------------------------------------------
    read_count: int = 50          # messages pulled per XREADGROUP batch
    block_ms: int = 5_000         # XREADGROUP block timeout
    idle_reclaim_ms: int = 30_000  # min-idle before XAUTOCLAIM reclaims a msg
    reclaim_count: int = 50

    # --- Retry / DLQ policy ------------------------------------------------
    max_retries: int = 4          # attempts before a message is dead-lettered
    backoff_base_s: float = 0.5   # first retry delay; doubles each attempt
    backoff_max_s: float = 8.0
    backoff_jitter_s: float = 0.25

    # --- Chaos engineering (failure injection for retry/DLQ testing) -------
    fail_rate: float = 0.0        # probability a consumer step raises

    # --- Database ----------------------------------------------------------
    database_url: str = (
        "postgresql://orders:orders@localhost:5432/orders"
    )
    db_pool_min: int = 2
    db_pool_max: int = 10

    # --- Producer API ------------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # --- OpenTelemetry -----------------------------------------------------
    otel_enabled: bool = True
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"


@lru_cache
def get_settings() -> Settings:
    """Return a process-wide cached settings instance."""
    return Settings()
