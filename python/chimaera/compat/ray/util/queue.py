"""``ray.util.queue``: Ray's blocking queue API over ``ch.Queue``."""

from __future__ import annotations

import queue as _queue
from typing import Any

import chimaera as ch

from .._unsupported import unsupported


class Empty(_queue.Empty):
    pass


class Full(_queue.Full):
    pass


class Queue:
    """A first-in, first-out queue shared across tasks and actors (Ray's API)."""

    def __init__(self, maxsize: int = 0, actor_options: dict[str, Any] | None = None) -> None:
        if actor_options:
            raise unsupported("Queue(actor_options=...)", why="the queue's host actor is managed by Chimaera")
        self.maxsize = maxsize
        self._q: ch.Queue[Any] = ch.Queue(maxsize)

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        try:
            self._q.put(item, timeout=timeout if block else 0).result()
        except _queue.Full:
            raise Full from None

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        try:
            return self._q.get(timeout=timeout if block else 0).result()
        except _queue.Empty:
            raise Empty from None

    def put_nowait(self, item: Any) -> None:
        self.put(item, block=False)

    def get_nowait(self) -> Any:
        return self.get(block=False)

    def put_nowait_batch(self, items: list[Any]) -> None:
        if self.maxsize and self.qsize() + len(items) > self.maxsize:
            raise Full(f"Cannot add {len(items)} items to queue of size {self.qsize()} and maxsize {self.maxsize}.")
        for item in items:
            self.put_nowait(item)

    def get_nowait_batch(self, num_items: int) -> list[Any]:
        if num_items > self.qsize():
            raise Empty(f"Cannot get {num_items} items from queue of size {self.qsize()}.")
        return [self.get_nowait() for _ in range(num_items)]

    def qsize(self) -> int:
        return self._q.qsize()

    size = qsize

    def empty(self) -> bool:
        return self.qsize() == 0

    def full(self) -> bool:
        return bool(self.maxsize) and self.qsize() >= self.maxsize

    def shutdown(self, force: bool = False, grace_period_s: int = 5) -> None:
        self._q.close()
