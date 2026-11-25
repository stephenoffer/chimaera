"""Parsing of byte sizes and durations used in options.

Sizes need an explicit byte unit ("512 MiB", "2GB", "1024 B"). A bare "512m"
is rejected because Docker reads it as MiB and Kubernetes as millibytes.
Durations accept "250ms", "30s", "5m", "2h", a float or int of seconds, or a
datetime.timedelta.
"""

from __future__ import annotations

import datetime as _dt
import math
import re
from decimal import Decimal
from fractions import Fraction

_SIZE_UNITS = {
    "b": 1,
    "kb": 10**3,
    "mb": 10**6,
    "gb": 10**9,
    "tb": 10**12,
    "kib": 2**10,
    "mib": 2**20,
    "gib": 2**30,
    "tib": 2**40,
}
_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\s*$")
_DUR_UNITS = {"ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}
_DUR_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(ms|s|m|h)\s*$")
# Sizes travel as unsigned 64-bit integers.
MAX_BYTES = 2**64 - 1
# Deadlines and expiries are times on the hybrid clock, which counts
# milliseconds since 1970 in 44 bits and so ends in the year 2527 (CLOCK_END,
# in seconds), and waits go to the OS timer, which takes about 292 years at
# most: a duration is at most a century.
CLOCK_END = (1 << 44) / 1000
MAX_SECONDS = 100 * 365 * 86400.0


def parse_bytes(value: int | str, field: str = "size") -> int:
    """Return a size in bytes. Ints are bytes; strings need a byte unit."""
    if isinstance(value, bool):
        raise TypeError(f"{field}: expected a byte size, got a bool")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{field}: size must not be negative, got {value}")
        total = value
    elif isinstance(value, str):
        m = _SIZE_RE.match(value)
        if not m or m.group(2).lower() not in _SIZE_UNITS:
            raise ValueError(
                f"{field}: {value!r} is not a byte size; use a byte unit such as '512 MiB', '2 GB' or '1024 B'"
            )
        # Exact: a float would round '9007199254740993 B' down a byte.
        exact = Fraction(Decimal(m.group(1))) * _SIZE_UNITS[m.group(2).lower()]
        if exact.denominator != 1:
            raise ValueError(f"{field}: {value!r} is not a whole number of bytes")
        total = int(exact)
    else:
        raise TypeError(f"{field}: expected an int of bytes or a string like '32 GiB', got {type(value).__name__}")
    if total > MAX_BYTES:
        raise ValueError(f"{field}: {value!r} is more than 16 EiB ({MAX_BYTES} bytes), the largest size there is")
    return total


def as_float(value: int | float) -> float:
    """float(value), or an infinity for an int too large for a float."""
    try:
        return float(value)
    except OverflowError:
        return math.inf if value > 0 else -math.inf


def parse_duration(value: float | int | str | _dt.timedelta, field: str = "duration", *, clock: bool = True) -> float:
    """Return a duration in seconds: finite, and at most MAX_SECONDS unless
    `clock` is False (an amount of compute time, which no clock or timer holds)."""
    if isinstance(value, bool):
        raise TypeError(f"{field}: expected a duration, got a bool")
    if isinstance(value, _dt.timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, (int, float)):
        seconds = as_float(value)
    elif isinstance(value, str):
        m = _DUR_RE.match(value)
        if not m:
            raise ValueError(f"{field}: {value!r} is not a duration; use e.g. '250ms', '30s', '5m' or '2h'")
        seconds = float(m.group(1)) * _DUR_UNITS[m.group(2)]
    else:
        raise TypeError(f"{field}: expected a duration, got {type(value).__name__}")
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError(f"{field}: duration must be a finite, non-negative number, got {value!r}")
    if clock and seconds > MAX_SECONDS:
        raise ValueError(f"{field}: {value!r} is longer than 100 years, the longest duration there is")
    return seconds


def start_position(start: object) -> str | int:
    """Where a subscription, watch or event stream begins: ``"latest"`` (what
    happens from now on), ``"earliest"`` (everything still held) or a position
    (an offset, revision or sequence number) >= 0."""
    if start in ("latest", "earliest") or (isinstance(start, int) and not isinstance(start, bool) and start >= 0):
        return start  # type: ignore[return-value]
    raise ValueError(f"start: expected 'latest', 'earliest' or a position >= 0, got {start!r}")
