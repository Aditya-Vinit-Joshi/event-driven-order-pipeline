"""Shared bootstrap for the stage entry points (logging, OTel, consumer name)."""
from __future__ import annotations

import os
import socket

from ..config import Settings
from ..logging_config import configure_logging
from ..telemetry import instrument_redis, setup_telemetry


def consumer_name(prefix: str) -> str:
    """Stable, unique name per worker for the Redis consumer group.

    Uses ``CONSUMER_NAME`` when set (e.g. injected per replica), otherwise the
    container hostname — which Docker/Compose makes unique per instance.
    """
    return os.getenv("CONSUMER_NAME") or f"{prefix}-{socket.gethostname()}"


def bootstrap(settings: Settings) -> None:
    configure_logging(settings.log_level)
    setup_telemetry(settings)
    instrument_redis()
