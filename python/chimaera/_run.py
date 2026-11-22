"""Reattaching to a run by id: ``ch.runs.attach(run_id)``."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

from ._errors import from_outcome
from ._runtime import Runtime


class Run:
    """A run: the calls one script (or one detached job) made, by id.

    ``ch.init()`` and ``ch.runs.current_run()`` return the current run;
    ``ch.runs.attach(run_id)`` reopens any run, from any process. ``str(run)`` is
    its id. ``with ch.init() as run:`` ends it (``ch.shutdown()``) when the
    block ends.
    """

    def __init__(self, rt: Runtime, run: bytes) -> None:
        self._rt, self._run = rt, run

    @property
    def id(self) -> str:
        return self._run.hex()

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return f"<Run {self.id[:12]}>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Run) and other._run == self._run

    def __hash__(self) -> int:
        return hash(self._run)

    def __reduce__(self) -> tuple[Any, ...]:
        return (attach, (self.id,))

    def __enter__(self) -> Run:
        return self

    def __exit__(self, *exc: object) -> None:
        from . import shutdown

        shutdown(cancel=exc[0] is not None)

    def tasks(self, state: str | None = None) -> list[tuple[str, str]]:
        """(task_id, state) for every call in the run, optionally only one
        state: ``"Pending"``, ``"Ready"``, ``"Running"`` or ``"Done"``."""
        return [(key[16:].hex(), s) for key, s, _ in self._pages() if state is None or s == state]

    def cancel(self) -> None:
        """Cancel every unfinished call in the run; it accepts no new calls."""
        self._rt.client.end_run(self._run, True)

    def finish(self) -> None:
        """End the run: it accepts no new calls and its unfinished calls are
        cancelled. A detached run lives until this (or its retention) ends it."""
        self._rt.client.end_run(self._run, False)

    def logs(self, *, follow: bool = False) -> Iterator[str]:
        """What the run's calls printed, line by line (``<task> <stream>| text``),
        kept on the peer after the driver has gone. ``follow=True`` waits for
        more (break out of the loop to stop). Each peer keeps the output of the
        calls it ran; this reads the peer this process is connected to."""
        offset, partial = 0, b""
        while True:
            text, offset, _ = self._rt.client.logs(self._run, offset, 10_000 if follow else 0)
            *lines, partial = (partial + text).split(b"\n")
            for line in lines:
                yield line.decode(errors="replace")
            if not text and not follow:
                if partial:
                    yield partial.decode(errors="replace")
                return

    def _pages(self) -> Iterator[tuple[bytes, str, tuple[Any, ...] | None]]:
        after = b""
        while True:
            items, nxt = self._rt.client.list_tasks(self._run, after, 5000)
            yield from items
            if nxt is None:
                return
            after = nxt

    def status(self) -> dict[str, int]:
        """Task counts by state: Pending, Ready, Running, Done."""
        counts: dict[str, int] = {}
        for _, state, _ in self._pages():
            counts[state] = counts.get(state, 0) + 1
        return counts

    def wait(self, timeout: float | None = None, poll: float = 0.2) -> dict[str, int]:
        """Block until no task is pending, ready or running; return the counts."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            counts = self.status()
            if set(counts) <= {"Done"}:
                return counts
            if deadline is not None and time.monotonic() > deadline:
                raise TimeoutError(f"run {self.id} still has work: {counts}")
            time.sleep(poll)

    def outcomes(self) -> Iterator[tuple[str, Any, BaseException | None]]:
        """(task_id, value, error) for every finished task, in task-id order."""
        for key, state, outcome in self._pages():
            if state != "Done" or outcome is None:
                continue
            if outcome[0] == "ok":
                try:
                    value = self._rt.read_value(outcome)
                except Exception as e:  # e.g. the value was released: report it for this call only
                    yield key[16:].hex(), None, e
                    continue
                yield key[16:].hex(), value, None
            else:
                yield key[16:].hex(), None, from_outcome(outcome, key[16:].hex())


def attach(run_id: str | Run) -> Run:
    """Reattach to a run by id, from any process, after the driver or peer restarted."""
    rt = Runtime.get()
    return Run(rt, bytes.fromhex(str(run_id)))


def current_run() -> Run:
    """The run this code belongs to: the script's, or inside a call, the run
    of the script that made the call."""
    from ._context import current_task

    rt = Runtime.get()
    ctx = current_task()
    run = ctx.run if ctx is not None else rt.run
    if run is None:
        raise RuntimeError("no run: this process is neither a script that made calls nor a call")
    return Run(rt, run)
