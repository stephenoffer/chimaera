"""``ray.util.ActorPool``: spread work over a fixed set of actors."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any

import chimaera as ch

Submit = Callable[[Any, Any], ch.Future[Any]]


class ActorPool:
    """Each actor runs one submitted call at a time; extra submissions wait
    until an actor frees up. ``get_next`` returns results in submission order,
    ``get_next_unordered`` in completion order."""

    def __init__(self, actors: Iterable[Any]) -> None:
        self._idle = list(actors)
        self._in_flight: dict[int, tuple[ch.Future[Any], Any]] = {}  # index -> (future, actor)
        self._pending: list[tuple[Submit, Any]] = []
        self._next_index = 0
        self._next_return = 0

    def submit(self, fn: Submit, value: Any) -> None:
        if self._idle:
            actor = self._idle.pop()
            self._in_flight[self._next_index] = (fn(actor, value), actor)
            self._next_index += 1
        else:
            self._pending.append((fn, value))

    def has_next(self) -> bool:
        return bool(self._in_flight)

    def has_free(self) -> bool:
        return bool(self._idle) and not self._pending

    def pop_idle(self) -> Any | None:
        return self._idle.pop() if self.has_free() else None

    def push(self, actor: Any) -> None:
        if any(actor is a for _, a in self._in_flight.values()) or any(actor is a for a in self._idle):
            raise ValueError("Actor already belongs to current ActorPool")
        self._return(actor)

    def _return(self, actor: Any) -> None:
        self._idle.append(actor)
        if self._pending:
            self.submit(*self._pending.pop(0))

    def _take(self, index: int, timeout: float | None) -> Any:
        from .. import get, wait

        future, actor = self._in_flight[index]
        ready, _ = wait([future], timeout=timeout)
        if not ready:
            raise TimeoutError("Timed out waiting for result")
        del self._in_flight[index]
        self._return(actor)  # before get(), so an error still frees the actor
        return get(future)

    def get_next(self, timeout: float | None = None, ignore_if_timedout: bool = False) -> Any:
        if not self.has_next():
            raise StopIteration("No more results to get")
        while self._next_return not in self._in_flight:
            self._next_return += 1
        index = self._next_return
        try:
            value = self._take(index, timeout)
        except TimeoutError:
            if ignore_if_timedout:  # give up on this result: skip it and free its actor
                _, actor = self._in_flight.pop(index)
                self._next_return += 1
                self._return(actor)
            raise
        self._next_return += 1
        return value

    def get_next_unordered(self, timeout: float | None = None, ignore_if_timedout: bool = False) -> Any:
        from .. import wait

        if not self.has_next():
            raise StopIteration("No more results to get")
        futures = {id(f): i for i, (f, _) in self._in_flight.items()}
        ready, _ = wait([f for f, _ in self._in_flight.values()], num_returns=1, timeout=timeout)
        if not ready:
            raise TimeoutError("Timed out waiting for result")
        return self._take(futures[id(ready[0])], 0)

    def map(self, fn: Submit, values: Iterable[Any]) -> Iterator[Any]:
        self._discard_previous()
        for v in values:
            self.submit(fn, v)
        while self.has_next():
            yield self.get_next()

    def map_unordered(self, fn: Submit, values: Iterable[Any]) -> Iterator[Any]:
        self._discard_previous()
        for v in values:
            self.submit(fn, v)
        while self.has_next():
            yield self.get_next_unordered()

    def _discard_previous(self) -> None:
        # Ray drops results nobody collected before a new map starts.
        while self.has_next():
            try:
                self.get_next_unordered()
            except Exception:  # noqa: S110 -- a dropped result's error is dropped too
                pass
