"""A driver's calls spread over the run's groups only while it submits fast:
a small DAG stays in one group (no notices between groups), a burst spreads."""

from __future__ import annotations

import time
from types import SimpleNamespace

from chimaera import _runtime


def _driver() -> SimpleNamespace:
    return SimpleNamespace(_rate=0.0, _rate_n=0, _rate_t0=time.monotonic())


def test_a_slow_driver_keeps_its_calls_together_and_a_burst_spreads(monkeypatch):
    monkeypatch.setattr(_runtime, "SPREAD_RATE", 2000.0)
    fast = _runtime.Runtime._submitting_fast
    slow = _driver()
    assert not any(fast(slow) for _ in range(100)), "100 calls is a small DAG, not a burst"
    burst = _driver()
    seen = [fast(burst) for _ in range(1000)]
    assert seen[-1] and not seen[0], "a burst spreads once it passes the rate"
    # The rate carries into the next window, then lapses when submissions stop.
    burst._rate_t0 -= 0.3
    assert fast(burst)
    burst._rate_t0 -= 0.3
    assert not fast(burst), "one call in the last window is not a burst"
