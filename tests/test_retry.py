import pytest

from app.errors import PermanentError, TransientError
from app.retry import RetryPolicy, run_with_retry


def test_delay_grows_exponentially():
    p = RetryPolicy(base_s=1.0, max_s=100.0, jitter_s=0.0)
    assert p.delay_for(1) == 1.0
    assert p.delay_for(2) == 2.0
    assert p.delay_for(3) == 4.0


def test_delay_is_capped():
    p = RetryPolicy(base_s=1.0, max_s=3.0, jitter_s=0.0)
    assert p.delay_for(10) == 3.0


async def test_succeeds_after_transient_failures():
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("blip")
        return "ok"

    policy = RetryPolicy(max_retries=5, base_s=0.0, max_s=0.0, jitter_s=0.0)
    result = await run_with_retry(flaky, policy, op="test")
    assert result == "ok"
    assert calls["n"] == 3


async def test_permanent_error_not_retried():
    calls = {"n": 0}

    async def boom():
        calls["n"] += 1
        raise PermanentError("nope")

    policy = RetryPolicy(max_retries=5, base_s=0.0, max_s=0.0, jitter_s=0.0)
    with pytest.raises(PermanentError):
        await run_with_retry(boom, policy)
    assert calls["n"] == 1


async def test_exhaustion_reraises():
    calls = {"n": 0}

    async def always_fail():
        calls["n"] += 1
        raise TransientError("still failing")

    policy = RetryPolicy(max_retries=3, base_s=0.0, max_s=0.0, jitter_s=0.0)
    with pytest.raises(TransientError):
        await run_with_retry(always_fail, policy)
    assert calls["n"] == 3
