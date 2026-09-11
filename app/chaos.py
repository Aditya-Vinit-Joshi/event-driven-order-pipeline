"""Deterministic-ish failure injection for exercising retry + DLQ paths.

Set ``FAIL_RATE=0.2`` on any consumer to make ~20% of processing attempts
raise a :class:`TransientError`. Because it raises a *transient* error, the
retry loop kicks in first; only if every attempt is unlucky does the message
land in the DLQ — exactly the behaviour we want to demonstrate.
"""
from __future__ import annotations

import random

from .errors import TransientError


def maybe_fail(fail_rate: float, *, where: str) -> None:
    if fail_rate <= 0:
        return
    if random.random() < fail_rate:
        raise TransientError(f"chaos-injected failure in {where}")
