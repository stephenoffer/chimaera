"""Reattaching to a run by id: ``ch.attach(run_id)``."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

from ._errors import from_outcome
from ._runtime import Runtime


class Run:
    """A run seen from any session: its tasks, their states and outcomes."""

    def __init__(self, rt: Runtime, run: bytes) -> None:
        self._rt = rt
        self.id = run.hex()
        self._run = run

    def __repr__(self) -> str:
        return f"<Run {self.id[:12]}>"

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
                yield key[16:].hex(), self._rt.read_value(outcome), None
            else:
                yield key[16:].hex(), None, from_outcome(outcome, key[16:].hex())


def attach(run_id: str) -> Run:
    """Reattach to a run by id, from any process, after the driver or peer restarted."""
    rt = Runtime.get()
    return Run(rt, bytes.fromhex(run_id))


def current_run() -> Run:
    rt = Runtime.get()
    assert rt.run is not None
    return Run(rt, rt.run)
