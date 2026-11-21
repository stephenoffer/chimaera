"""How the shim says no: a clear error naming the Chimaera alternative, or a
one-time warning when an option is accepted but means something different."""

from __future__ import annotations

import warnings


class Unsupported(NotImplementedError):
    """A Ray feature this shim does not map. The message names what to use instead."""


class UnsupportedAttribute(Unsupported, AttributeError):
    """``ray.<name>`` for a name the shim does not provide (``hasattr`` returns False)."""


class ChimaeraCompatWarning(UserWarning):
    """A Ray option was accepted but behaves differently on Chimaera.

    CHR001: ignored, because Chimaera handles it automatically.
    CHR002: accepted, but not enforced yet; the message names the phase that adds it.
    CHR003: different semantics; the message says how.
    """


def unsupported(feature: str, *, use: str = "", lands: str = "", why: str = "") -> Unsupported:
    parts = [f"{feature} is not supported by chimaera.compat.ray"]
    if why:
        parts.append(why)
    if use:
        parts.append(f"use {use}" + (f" (available from {lands})" if lands else ""))
    elif lands:
        parts.append(f"planned for {lands}")
    return Unsupported("; ".join(parts) + ". See docs/compat-ray.md.")


_warned: set[tuple[str, str]] = set()


def warn_once(code: str, subject: str, message: str) -> None:
    if (code, subject) in _warned:
        return
    _warned.add((code, subject))
    warnings.warn(f"{code}: {subject}: {message}", ChimaeraCompatWarning, stacklevel=4)
