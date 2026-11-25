"""A bound on one actor method's calls: ``@ch.method(mailbox=ch.Mailbox(...))``.

    @ch.method(mailbox=ch.Mailbox(max_calls=8, on_full="reject"))
    def render(self, page): ...

The actor's process holds at most ``max_calls`` of the method's calls at
once, running or waiting there for a thread. A call past that either fails
at once with ``ch.errors.Overloaded`` (``on_full="reject"``) or waits for
one of them to finish, at most ``max_wait`` (``on_full="wait"``), then
fails the same way.

The method's calls are counted apart from the actor's other calls, so a
flood of them, held back or turned away at its limit, never takes the room
another method needs: an admin or health-check method in its own group
stays responsive while this one is overloaded.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Literal

from . import _units

DEFAULT_WAIT = 30.0  # on_full="wait" without max_wait, in seconds


@dataclass(frozen=True)
class Mailbox:
    """At most `max_calls` of the method's calls at its actor; a call past
    that fails with ``ch.errors.Overloaded`` at once (``on_full="reject"``)
    or after waiting up to `max_wait` (seconds or "2s"; default 30 s) for
    room (``on_full="wait"``)."""

    max_calls: int
    on_full: Literal["wait", "reject"] = "wait"
    max_wait: float | str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.max_calls, bool) or not isinstance(self.max_calls, int) or self.max_calls < 1:
            raise ValueError(f"Mailbox(max_calls=...) must be a positive int, got {self.max_calls!r}")
        if self.on_full not in ("wait", "reject"):
            raise ValueError(f"Mailbox(on_full=...) is 'wait' or 'reject', got {self.on_full!r}")
        if self.on_full == "reject" and self.max_wait is not None:
            raise ValueError("Mailbox(on_full='reject') turns calls away at once: drop max_wait")
        wait = DEFAULT_WAIT if self.max_wait is None else _units.parse_duration(self.max_wait, "Mailbox(max_wait=...)")
        if self.on_full == "wait" and wait <= 0:
            raise ValueError(f"Mailbox(max_wait=...) must be positive, got {self.max_wait!r}")
        object.__setattr__(self, "max_wait", wait if self.on_full == "wait" else None)


def limits(cls: type[Any]) -> list[tuple[str, int, int, bool, int]]:
    """The per-method limits its peer keeps for an actor of `cls`: each
    method's mailbox, and each batched method's queue (two batches of calls,
    and at most its max_bytes of arguments). As (method, max_calls,
    max_bytes, reject, max_wait_ms)."""
    out = []
    for name in dir(cls):
        declared = getattr(inspect.getattr_static(cls, name, None), "__chimaera_method__", None) or {}
        mailbox, batch = declared.get("mailbox"), declared.get("batch")
        if mailbox is not None:
            wait_ms = 0 if mailbox.max_wait is None else max(1, round(mailbox.max_wait * 1000))
            out.append((name, mailbox.max_calls, 0, mailbox.on_full == "reject", wait_ms))
        elif batch is not None:
            out.append((name, batch.lane, batch.max_bytes or 0, False, 0))
    return out
