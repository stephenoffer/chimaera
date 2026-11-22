"""Queues and topics shared across processes: ``ch.coord.Queue`` and ``ch.coord.Topic``.

Each is hosted by an actor, so it lives as long as its run (or, with
``detach=True``, until closed), is shared by ``name=``, and works from
the driver and from inside calls alike. They are not replicated: if the
hosting process dies, its contents are lost and further calls raise
``ch.ActorDied``. ``ch.coord.WorkQueue`` is the replicated one: it lives
in ``ch.coord.Store``.
"""

from __future__ import annotations

import asyncio
import collections
import pickle
import queue as _stdqueue
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any, Generic, TypeVar

from . import _units
from ._errors import Closed
from ._future import Future
from ._remote import method, remote

T = TypeVar("T")

# Concurrent waiters a host serves at once (each holds one thread in the host).
_WAITERS = 1024


class _Waitable:
    """An asyncio condition created on the host's event loop when first used."""

    _cond: asyncio.Condition | None = None

    def cond(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    async def until(self, ready: Any, timeout: float | None) -> bool:
        """Wait (holding the condition) until `ready()`; False if `timeout` passed.
        ``timeout=0`` only checks, on every Python version."""
        if ready():
            return True
        if timeout == 0:
            return False
        try:
            await asyncio.wait_for(self.cond().wait_for(ready), timeout)
        except TimeoutError:
            return False
        return True


@remote(groups={"wait": _WAITERS, "admin": 4}, max_attempts=1)
class _QueueHost(_Waitable):
    # put runs in the default lane, one call at a time, so puts keep their
    # arrival order and a full queue holds back the puts behind it.
    def __init__(self, maxsize: int) -> None:
        self.maxsize = maxsize
        self.items: collections.deque[Any] = collections.deque()
        self.closed = False

    async def put(self, item: Any, timeout: float | None) -> None:
        c = self.cond()
        async with c:
            if not await self.until(lambda: self.closed or not self.maxsize or len(self.items) < self.maxsize, timeout):
                raise _stdqueue.Full(f"the queue stayed full for {timeout} s")
            if self.closed:
                raise Closed("put on a closed queue")
            self.items.append(item)
            c.notify_all()

    @method(group="wait")
    async def get(self, timeout: float | None) -> Any:
        c = self.cond()
        async with c:
            if not await self.until(lambda: bool(self.items) or self.closed, timeout):
                raise _stdqueue.Empty(f"the queue stayed empty for {timeout} s")
            if not self.items:
                raise Closed("the queue is closed and empty")
            item = self.items.popleft()
            c.notify_all()
            return item

    @method(group="admin")
    async def seal(self) -> None:
        c = self.cond()
        async with c:
            self.closed = True
            c.notify_all()

    @method(group="admin")
    def qsize(self) -> int:
        return len(self.items)


class Queue(Generic[T]):
    """A bounded first-in, first-out queue that many processes can use.

    ``put`` and ``get`` return futures. A full queue makes ``put`` wait (in
    the queue, not in your process) until there is room; ``maxsize=0`` means
    unbounded. Iterating (``for x in q`` or ``async for x in q``) takes items
    until the queue is closed and empty. ``name=`` shares one queue between
    everyone who names it; the first creator's ``maxsize`` applies.
    """

    def __init__(self, maxsize: int = 0, *, name: str | None = None, detach: bool = False) -> None:
        if isinstance(maxsize, bool) or not isinstance(maxsize, int) or maxsize < 0:
            raise ValueError(f"maxsize: expected an integer >= 0, got {maxsize!r}")
        opts: dict[str, Any] = {"detach": detach}
        if name is not None:
            opts["name"] = f"queue/{name}"
        self._host = _QueueHost.options(**opts).remote(maxsize)

    def put(self, item: T, *, timeout: float | None = None) -> Future[None]:
        """Add `item`. The future fails with ``queue.Full`` if there was no room
        within `timeout` seconds, or with ``ch.errors.Closed`` after ``close()``."""
        return self._host.put.remote(item, timeout)

    def get(self, *, timeout: float | None = None) -> Future[T]:
        """The next item. Fails with ``queue.Empty`` if none arrived within
        `timeout` seconds, or ``ch.errors.Closed`` once the queue is closed and empty."""
        return self._host.get.remote(timeout)

    def close(self) -> None:
        """No more puts. Waiting and later gets drain what is left, then raise ``ch.errors.Closed``."""
        self._host.seal.remote().result()

    def qsize(self) -> int:
        return self._host.qsize.remote().result()

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                yield self.get().result()
            except Closed:
                return

    async def __aiter__(self) -> AsyncIterator[T]:
        while True:
            try:
                yield await self.get()
            except Closed:
                return


class WorkQueue(Generic[T]):
    """``ch.coord.WorkQueue(name)``: a queue of work items kept in a ``ch.coord.Store``.

    It survives the loss of any process and a minority of peers, and lives
    until closed. Items are delivered first-in, first-out to consumers that
    ``claim`` them: a claim holds the item under a lease, and the item is
    removed when the ``with`` block exits normally. If the block raises, or
    its process dies and the lease lapses, the item is delivered again.
    ``get()`` claims and removes in one step.

    Unlike ``Queue``, its methods are synchronous: ``put`` returns once the
    item is stored and ``get`` returns the item.
    """

    _POLL_MAX = 0.25

    def __init__(self, name: str, *, maxsize: int = 0) -> None:
        from ._store import Store

        if isinstance(maxsize, bool) or not isinstance(maxsize, int) or maxsize < 0:
            raise ValueError(f"maxsize: expected an integer >= 0, got {maxsize!r}")
        if not isinstance(name, str) or not name:
            raise ValueError("a work queue is found by its name: WorkQueue(name)")
        self._store = Store(f"queue/{name}")
        self._maxsize = maxsize

    def __repr__(self) -> str:
        return f"<WorkQueue {self._store.namespace[len('queue/') :]!r}>"

    def __reduce__(self) -> tuple[Any, ...]:
        return (_work_queue, (self._store.namespace[len("queue/") :], self._maxsize))

    def _resize(self, delta: int, *, timeout: float | None = None) -> None:
        """Add `delta` to the stored size atomically, waiting for room when growing."""
        from ._errors import VersionConflict
        from ._future import waiting

        deadline = None if timeout is None else time.monotonic() + timeout
        pause = 0.005
        while True:
            size, version = self._store.get("size", 0), self._store.version("size")
            if delta > 0 and self._maxsize and size + delta > self._maxsize:
                if deadline is not None and time.monotonic() >= deadline:
                    raise _stdqueue.Full(f"the queue stayed full for {timeout} s")
                with waiting():
                    time.sleep(pause)
                pause = min(pause * 2, self._POLL_MAX)
                continue
            try:
                self._store.put("size", max(0, size + delta), if_version=version)
                return
            except VersionConflict:
                continue

    def put(self, item: T, *, timeout: float | None = None) -> None:
        """Store `item`, waiting up to `timeout` seconds for room (``queue.Full``
        after that); ``ch.errors.Closed`` after ``close()``."""
        if self._store.get("closed", False):
            raise Closed("put on a closed queue")
        self._resize(1, timeout=timeout)
        seq = self._store.counter("tail").add(1)
        self._store.put(f"item/{seq:020d}", item)

    def claim(self, *, lease: float | str = "30s", timeout: float | None = None) -> Claim[T]:
        """``with q.claim() as item: ...``: the item is removed when the block
        finishes, and delivered again if it raises or its process dies."""
        return Claim(self, lease, timeout)

    def get(self, *, timeout: float | None = None) -> T:
        """Claim the next item and remove it (``queue.Empty`` after `timeout`
        seconds; ``ch.errors.Closed`` once closed and empty)."""
        with self.claim(timeout=timeout) as item:
            return item

    def close(self) -> None:
        """No more puts; consumers drain what is left, then get ``ch.errors.Closed``."""
        self._store.put("closed", True)

    def qsize(self) -> int:
        return self._store.get("size", 0)

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                yield self.get()
            except Closed:
                return


def _work_queue(name: str, maxsize: int) -> WorkQueue[Any]:
    return WorkQueue(name, maxsize=maxsize)


class Claim(Generic[T]):
    """One claimed item of a work queue (see ``WorkQueue.claim``)."""

    def __init__(self, q: WorkQueue[T], lease: float | str, timeout: float | None) -> None:
        self._q, self._ttl, self._timeout = q, lease, timeout
        self.key: str | None = None
        self.lease: Any = None

    def __enter__(self) -> T:
        from ._future import waiting

        store = self._q._store
        deadline = None if self._timeout is None else time.monotonic() + self._timeout
        pause = 0.005
        while True:
            for key in store.keys("item/"):
                lease = store.lock(f"claim/{key}", ttl=self._ttl, timeout=0)
                try:
                    lease.acquire()
                except TimeoutError:
                    continue  # another consumer holds it
                if store.version(key) == 0:  # removed by a consumer that finished it
                    lease.release()
                    continue
                self.key, self.lease = key, lease
                return store.get(key)
            if store.get("closed", False):
                raise Closed("the queue is closed and empty")
            if deadline is not None and time.monotonic() >= deadline:
                raise _stdqueue.Empty(f"the queue stayed empty for {self._timeout} s")
            with waiting():
                time.sleep(pause)
            pause = min(pause * 2, WorkQueue._POLL_MAX)

    def __exit__(self, exc_type: Any, *exc: object) -> None:
        store, lease = self._q._store, self.lease
        try:
            if exc_type is None and store.delete(self.key, fence=lease):
                self._q._resize(-1)
        finally:
            lease.release()


@remote(groups={"wait": _WAITERS, "admin": 4}, max_attempts=1)
class _TopicHost(_Waitable):
    # publish runs in the default lane, so offsets follow arrival order.
    def __init__(self, retain_items: int, retain_bytes: int) -> None:
        self.log: collections.deque[tuple[Any, int]] = collections.deque()  # (item, size)
        self.base = 0  # offset of log[0]
        self.bytes = 0
        self.retain_items, self.retain_bytes = retain_items, retain_bytes
        self.closed = False

    async def publish(self, item: Any) -> int:
        c = self.cond()
        async with c:
            if self.closed:
                raise Closed("publish on a closed topic")
            size = len(pickle.dumps(item, protocol=5)) if self.retain_bytes else 0
            self.log.append((item, size))
            self.bytes += size
            while self.log and (
                (self.retain_items and len(self.log) > self.retain_items)
                or (self.retain_bytes and self.bytes > self.retain_bytes and len(self.log) > 1)
            ):
                self.bytes -= self.log.popleft()[1]
                self.base += 1
            c.notify_all()
            return self.base + len(self.log) - 1

    @method(group="wait")
    async def read(self, offset: int, limit: int, timeout: float) -> tuple[int, list[Any], int, bool]:
        """(first offset returned, items, offsets skipped because they expired, closed)."""
        c = self.cond()
        async with c:
            end = self.base + len(self.log)
            if offset < 0:  # "latest": from what is published after now
                offset = end
            if not await self.until(lambda: self.base + len(self.log) > offset or self.closed, timeout):
                return offset, [], 0, False
            skipped = max(0, self.base - offset)
            start = max(offset, self.base)
            items = [self.log[i - self.base][0] for i in range(start, min(start + limit, self.base + len(self.log)))]
            return start, items, skipped, self.closed and start + len(items) >= self.base + len(self.log)

    @method(group="admin")
    async def seal(self) -> None:
        c = self.cond()
        async with c:
            self.closed = True
            c.notify_all()

    @method(group="admin")
    async def end(self) -> int:
        """The offset the next published item will get."""
        async with self.cond():
            return self.base + len(self.log)


class Topic(Generic[T]):
    """Publish once, deliver to every subscriber (pub/sub and broadcast).

    The topic keeps a bounded log: the newest `retain` items, given as a count
    (``retain=1024``) or a size (``retain="1 GiB"``). ``publish`` returns the
    item's offset. Each subscriber reads at its own pace from where it starts;
    one that falls behind the retained log skips ahead and its ``skipped``
    count says how many items it missed. ``name=`` shares one topic.
    """

    def __init__(self, *, name: str | None = None, retain: int | str = 1024, detach: bool = False) -> None:
        if isinstance(retain, str):
            items, size = 0, _units.parse_bytes(retain, "retain")
        elif isinstance(retain, int) and not isinstance(retain, bool) and retain > 0:
            items, size = retain, 0
        else:
            raise ValueError(f"retain: expected a positive count or a size like '1 GiB', got {retain!r}")
        opts: dict[str, Any] = {"detach": detach}
        if name is not None:
            opts["name"] = f"topic/{name}"
        self._host = _TopicHost.options(**opts).remote(items, size)

    def publish(self, item: T) -> Future[int]:
        """Append `item`; the future gives its offset."""
        return self._host.publish.remote(item)

    def subscribe(self, start: str | int = "latest") -> Subscription[T]:
        """Items from `start`: ``"latest"`` (published from now on),
        ``"earliest"`` (everything retained) or an offset."""
        start = _units.start_position(start)
        if start == "latest":
            # Fixed now, not at the first read: an item published in between is seen.
            offset = self._host.end.remote().result()
        else:
            offset = 0 if start == "earliest" else start
        return Subscription(self._host, offset)

    def close(self) -> None:
        """No more publishes. Subscribers receive what is retained, then stop."""
        self._host.seal.remote().result()


class Subscription(Generic[T]):
    """An iterator (sync or async) over a topic's items, from one offset on."""

    _BATCH = 256
    _POLL_S = 30.0

    def __init__(self, host: Any, offset: int) -> None:
        self._host = host
        self._offset = offset
        self._buffer: collections.deque[Any] = collections.deque()
        self._ended = False
        self.skipped = 0

    @property
    def offset(self) -> int:
        """The offset of the next item this subscription will return."""
        return self._offset - len(self._buffer)

    def _take(self, batch: tuple[int, list[Any], int, bool]) -> None:
        start, items, skipped, closed = batch
        self.skipped += skipped
        self._buffer.extend(items)
        self._offset = start + len(items)
        self._ended = closed

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        while not self._buffer:
            if self._ended:
                raise StopIteration
            self._take(self._host.read.remote(self._offset, self._BATCH, self._POLL_S).result())
        return self._buffer.popleft()

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        while not self._buffer:
            if self._ended:
                raise StopAsyncIteration
            self._take(await self._host.read.remote(self._offset, self._BATCH, self._POLL_S))
        return self._buffer.popleft()

    def close(self) -> None:
        """Stop reading (the topic itself is not affected)."""
        self._ended = True
        self._buffer.clear()
