"""Waiting on futures through their callbacks: futures in the order they
finish, and waking a coroutine from the thread that delivers outcomes."""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from typing import Any


def wake(waiter: asyncio.Future[None]) -> None:
    """Resolve a coroutine's waiter from any thread. One cancelled meanwhile,
    or whose loop has closed, needs nothing."""
    try:
        waiter.get_loop().call_soon_threadsafe(_resolve, waiter)
    except RuntimeError:  # its loop closed: nobody waits there now
        pass


def _resolve(waiter: asyncio.Future[None]) -> None:
    if not waiter.done():
        waiter.set_result(None)


async def any_done(futures: list[Any], waiters: list[asyncio.Future[None]] | None = None) -> None:
    """Until one of `futures` has finished, or something resolves the waiter
    this lists in `waiters`. Cancelled, it leaves no callback on them."""
    waiter = asyncio.get_running_loop().create_future()
    subs = [(f, f._on_done(lambda _: wake(waiter))) for f in futures]
    if waiters is not None:
        waiters.append(waiter)
    try:
        await waiter
    finally:
        for f, fn in subs:
            if fn is not None:
                f._off_done(fn)
        if waiters is not None:
            waiters.remove(waiter)


class Completions:
    """Futures in the order they finish. Each has one callback here, so taking
    all n costs O(n) whatever order they finish in (nothing rescans the
    unfinished); ``close`` drops the callbacks of those not finished, for a
    caller that stops early."""

    def __init__(self, futures: list[Any]) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._ready: deque[Any] = deque()  # finished, not taken
        self._waiters: list[asyncio.Future[None]] = []  # coroutines waiting for one
        # The futures not finished, by id, with the callbacks ``close`` drops.
        # Only these are held here (the runtime holds futures weakly): one
        # taken and let go is freed, and its result with it. An entry leaves
        # in one dict operation, in _push or in close, so they need no lock.
        self._subs: dict[int, tuple[Any, list[Any]]] = {}
        push = self._push
        for f in futures:
            fns = self._subs.setdefault(id(f), (f, []))[1]  # listed before its callback can run
            fn = f._on_done(push)
            if fn is not None:
                fns.append(fn)

    def _push(self, f: Any) -> None:
        self._subs.pop(id(f), None)  # finished: its callbacks have all been handed out
        with self._cond:
            self._ready.append(f)
            self._cond.notify_all()
            waiters = list(self._waiters)
        for w in waiters:
            wake(w)

    def wait(self, n: int, deadline: float | None) -> None:
        """Until `n` have finished, or `deadline` passes."""
        with self._cond:
            while len(self._ready) < n:
                left = None if deadline is None else deadline - time.monotonic()
                if left is not None and left <= 0:
                    return
                self._cond.wait(left)

    def take(self, deadline: float | None) -> Any:
        """The next to finish, waiting until `deadline` (None if it passes first)."""
        with self._cond:
            while not self._ready:
                left = None if deadline is None else deadline - time.monotonic()
                if left is not None and left <= 0:
                    return None
                self._cond.wait(left)
            return self._ready.popleft()

    async def take_async(self, deadline: float | None) -> Any:
        """``take`` for a coroutine. Cancelled while it waits, it takes nothing."""
        loop = asyncio.get_running_loop()
        while True:
            with self._cond:
                if self._ready:
                    return self._ready.popleft()
                left = None if deadline is None else deadline - time.monotonic()
                if left is not None and left <= 0:
                    return None
                waiter = loop.create_future()
                self._waiters.append(waiter)
            try:
                await asyncio.wait_for(waiter, left)
            except asyncio.TimeoutError:
                pass  # looks once more, then finds the deadline passed
            finally:
                with self._cond:
                    self._waiters.remove(waiter)

    def close(self) -> None:
        """Drop the callbacks of the futures not finished: nothing here waits for them now."""
        subs = self._subs
        while subs:
            try:
                f, fns = subs.popitem()[1]
            except KeyError:  # the last one finished meanwhile
                return
            for fn in fns:
                f._off_done(fn)
