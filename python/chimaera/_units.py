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


def parse_bytes(value: int | str, field: str = "size") -> int:
    """Return a size in bytes. Ints are bytes; strings need a byte unit."""
    if isinstance(value, bool):
        raise TypeError(f"{field}: expected a byte size, got a bool")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{field}: size must not be negative, got {value}")
        return value
    if not isinstance(value, str):
        raise TypeError(f"{field}: expected an int of bytes or a string like '32 GiB', got {type(value).__name__}")
    m = _SIZE_RE.match(value)
    if not m or m.group(2).lower() not in _SIZE_UNITS:
        raise ValueError(
            f"{field}: {value!r} is not a byte size; use a byte unit such as '512 MiB', '2 GB' or '1024 B'"
        )
    number, unit = float(m.group(1)), _SIZE_UNITS[m.group(2).lower()]
    total = number * unit
    if total != math.floor(total):
        raise ValueError(f"{field}: {value!r} is not a whole number of bytes")
    return int(total)


def parse_duration(value: float | int | str | _dt.timedelta, field: str = "duration") -> float:
    """Return a duration in seconds."""
    if isinstance(value, bool):
        raise TypeError(f"{field}: expected a duration, got a bool")
    if isinstance(value, _dt.timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, (int, float)):
        seconds = float(value)
    elif isinstance(value, str):
        m = _DUR_RE.match(value)
        if not m:
            raise ValueError(f"{field}: {value!r} is not a duration; use e.g. '250ms', '30s', '5m' or '2h'")
        seconds = float(m.group(1)) * _DUR_UNITS[m.group(2)]
    else:
        raise TypeError(f"{field}: expected a duration, got {type(value).__name__}")
    if math.isnan(seconds) or seconds < 0:
        raise ValueError(f"{field}: duration must be a non-negative number, got {value!r}")
    return seconds
