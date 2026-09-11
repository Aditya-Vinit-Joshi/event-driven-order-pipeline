"""Exponential-backoff retry for async processing steps.

The retry loop is deliberately explicit (rather than a third-party library) so
the DLQ handoff can distinguish *retryable* from *permanent* failures:

    attempt 1 fails  -> sleep base
    attempt 2 fails  -> sleep base*2
    attempt 3 fails  -> sleep base*4  (capped at backoff_max, plus jitter)
    ...
    max_retries hit  -> re-raise so the caller can dead-letter the message
"""
from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from .errors import PermanentError
from .logging_config import get_logger

T = TypeVar("T")
log = get_logger("pipeline.retry")


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 4
    base_s: float = 0.5
    max_s: float = 8.0
    jitter_s: float = 0.25

    def delay_for(self, attempt: int) -> float:
        """Delay (seconds) before the retry that follows ``attempt`` (1-based)."""
        raw = self.base_s * (2 ** (attempt - 1))
        capped = min(raw, self.max_s)
        return capped + random.uniform(0, self.jitter_s)


async def run_with_retry(
    func: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    *,
    op: str = "operation",
) -> T:
    """Run ``func`` with exponential backoff.

    :class:`PermanentError` short-circuits immediately (no retries). Any other
    exception is retried up to ``policy.max_retries`` times, after which it is
    re-raised for the caller to dead-letter.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return await func()
        except PermanentError:
            # Business-rule failure: retrying cannot help.
            raise
        except Exception as exc:  # noqa: BLE001 - intentional broad retry net
            if attempt >= policy.max_retries:
                log.warning(
                    "giving up after retries",
                    extra={"ctx_op": op, "ctx_attempt": attempt,
                           "ctx_error": str(exc)},
                )
                raise
            delay = policy.delay_for(attempt)
            log.info(
                "retrying after transient failure",
                extra={"ctx_op": op, "ctx_attempt": attempt,
                       "ctx_delay_s": round(delay, 3), "ctx_error": str(exc)},
            )
            await asyncio.sleep(delay)
