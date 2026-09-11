"""Exception taxonomy that drives retry vs. dead-letter decisions.

- :class:`TransientError` (and any unexpected ``Exception``) is retried with
  exponential backoff, then dead-lettered if it never succeeds.
- :class:`PermanentError` skips retries and goes straight to the DLQ — there's
  no point retrying a business-rule violation such as a negative price.
"""
from __future__ import annotations


class PipelineError(Exception):
    """Base class for pipeline-domain failures."""

    reason_code: str = "pipeline_error"


class TransientError(PipelineError):
    """A failure that may succeed on retry (network blip, chaos, DB hiccup)."""

    reason_code = "transient_error"


class PermanentError(PipelineError):
    """A failure that will never succeed on retry — dead-letter immediately."""

    reason_code = "permanent_error"


class ValidationRejected(PermanentError):
    """An order violated a business rule and is rejected outright."""

    reason_code = "validation_rejected"
