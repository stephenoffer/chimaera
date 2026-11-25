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
from typing import Any, Generic, NamedTuple, TypeVar

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

    The queue keeps its ``maxsize``: the first open that gives one stores it,
    later opens without one use it, and opening with a different one raises
    ``ValueError``.
    """

    # The longest one wait for a change lasts before a waiter looks again.
    _WAIT_MS = 30_000

    def __init__(self, name: str, *, maxsize: int | None = None) -> None:
        from ._store import Store

        if maxsize is not None and (isinstance(maxsize, bool) or not isinstance(maxsize, int) or maxsize < 0):
            raise ValueError(f"maxsize: expected an integer >= 0, got {maxsize!r}")
        if not isinstance(name, str) or not name:
            raise ValueError("a work queue is found by its name: WorkQueue(name)")
        self._store = Store(f"queue/{name}")
        self._maxsize, self._cap_version = self._cap(maxsize)

    def __repr__(self) -> str:
        return f"<WorkQueue {self._store.namespace[len('queue/') :]!r}>"

    def __reduce__(self) -> tuple[Any, ...]:
        return (_work_queue, (self._store.namespace[len("queue/") :],))

    def _cap(self, maxsize: int | None) -> tuple[int, int]:
        """The stored capacity and its version; (0, 0) while none is stored.
        Stores `maxsize` if none is, and refuses one that differs."""
        from ._errors import VersionConflict

        while True:
            cap, version = self._store.get_versioned("maxsize", 0)
            if version:
                if maxsize is not None and maxsize != cap:
                    raise ValueError(f"{self!r} has maxsize={cap}; it cannot be opened with maxsize={maxsize}")
                return cap, version
            if maxsize is None:
                return 0, 0
            try:
                return maxsize, self._store.put("maxsize", maxsize, if_version=0)
            except VersionConflict:
                continue  # another client stored one first

    def _wait(self, revision: int, deadline: float | None, ms: int = 0) -> None:
        """Until the queue changes after `revision`, `ms` pass (if not 0) or `deadline`."""
        from ._future import waiting

        wait = ms or self._WAIT_MS
        if deadline is not None:
            wait = min(wait, max(1, int((deadline - time.monotonic()) * 1000)))
        with waiting():
            self._store._read("changes", "", after_revision=revision, limit=1, wait_ms=wait)

    def put(self, item: T, *, timeout: float | None = None) -> None:
        """Store `item`, waiting up to `timeout` seconds for room (``queue.Full``
        after that); ``ch.errors.Closed`` after ``close()``."""
        from ._store import _dumps, _raise, _value

        store, value = self._store, _dumps(item)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            tail, tail_version = store.get_versioned("tail", 0)
            checks = [("closed", 0), ("tail", tail_version), ("maxsize", self._cap_version)]
            if self._maxsize:
                r = store._read("get", "count")
                count, count_version = (_value(r["entry"]), r["entry"]["version"]) if r["ok"] else (0, 0)
                if count >= self._maxsize:
                    if store.version("closed"):
                        raise Closed("put on a closed queue")
                    if deadline is not None and time.monotonic() >= deadline:
                        raise _stdqueue.Full(f"the queue stayed full for {timeout} s")
                    self._wait(r["revision"], deadline)  # for a completion, or the close
                    continue
                checks.append(("count", count_version))
            # One command: refused if the queue closed, or a put (or, when
            # bounded, a completion) landed since the reads. A producer that
            # dies leaves the whole item or nothing.
            ops = [("add", "count", None, 1), ("add", "tail", None, 1), ("put", f"item/{tail + 1:020d}", value, 0)]
            r = self._txn(checks, ops)
            if r["ok"]:
                return
            if r["entry"] and r["entry"]["key"] == "closed":
                raise Closed("put on a closed queue")
            if r["entry"] and r["entry"]["key"] == "maxsize":  # stored by another client since this one opened
                self._maxsize, self._cap_version = self._cap(None)
            elif not r["error"].startswith("version mismatch"):
                _raise(r)

    def _txn(
        self, checks: list[tuple[str, int]], ops: list[tuple[str, str, bytes | None, int]], fence: Any = None
    ) -> dict[str, Any]:
        """``Store._txn`` on this queue's keys. Earlier versions kept the depth
        as a value at "size": a transaction that finds it there first moves it
        to the "count" counter, in one command, then applies."""
        from ._store import _raise, _value

        store = self._store
        while True:
            r = store._txn([("size", 0), *checks], ops, fence)
            old = r["entry"]
            if r["ok"] or old is None or old["key"] != "size":
                return r
            move = [("delete", "size", None, 0), ("add", "count", None, _value(old))]
            moved = store._txn([("size", old["version"])], move)
            if not moved["ok"] and not moved["error"].startswith("version mismatch"):
                _raise(moved)

    def claim(self, *, lease: float | str = "30s", timeout: float | None = None) -> Claim[T]:
        """``with q.claim() as item: ...``: the item is removed when the block
        finishes, and delivered again if it raises or its process dies."""
        return Claim(self, lease, timeout)

    def get(self, *, timeout: float | None = None) -> T:
        """Claim the next item and remove it (``queue.Empty`` after `timeout`
        seconds; ``ch.errors.Closed`` once closed and every item is done)."""
        with self.claim(timeout=timeout) as item:
            return item

    def close(self) -> None:
        """No more puts; consumers drain what is left, including items whose
        consumer dies holding them, then get ``ch.errors.Closed``."""
        self._store.put("closed", True)

    def qsize(self) -> int:
        # A depth an earlier version left at "size" counts until a put or a
        # completion moves it to "count"; nothing writes "count" before that.
        size, version = self._store.get_versioned("size", 0)
        return size if version else self._store.get("count", 0)

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                yield self.get()
            except Closed:
                return


def _work_queue(name: str, maxsize: int = 0) -> WorkQueue[Any]:
    # maxsize: what an earlier version pickled with the queue (0: none given).
    return WorkQueue(name, maxsize=maxsize or None)


class Claim(Generic[T]):
    """One claimed item of a work queue (see ``WorkQueue.claim``)."""

    def __init__(self, q: WorkQueue[T], lease: float | str, timeout: float | None) -> None:
        self._q, self._ttl, self._timeout = q, lease, timeout
        self.key: str | None = None
        self.lease: Any = None
        self._examined = 0  # keys the shard looked at to grant it

    def __enter__(self) -> T:
        from ._store import _raise, _value

        q, store = self._q, self._q._store
        deadline = None if self._timeout is None else time.monotonic() + self._timeout
        closed = False  # as read before the latest claim
        while True:
            # One command: the item given back or lapsed first, else the
            # oldest never claimed; the shard looks at a couple of keys.
            r, lease = store._claim("item/", "claim/", self._ttl)
            if lease is not None:
                self.key, self.lease, self._examined = r["entry"]["key"], lease, r["examined"]
                return _value(r["entry"])
            if r["error"].startswith("busy"):
                continue  # it stopped at its budget of keys looked at; this one goes on
            if not r["error"].startswith("nothing to claim"):
                _raise(r)
            # Closed once no item is left, claimed or not: a claimed item comes
            # back if its consumer dies. No put lands after close, so a claim
            # after seeing it finds every item there will be.
            if not r["more"]:
                if closed:
                    raise Closed("the queue is closed and empty")
                if store.get("closed", False):
                    closed = True
                    continue
            if deadline is not None and time.monotonic() >= deadline:
                raise _stdqueue.Empty(f"the queue stayed empty for {self._timeout} s")
            # Until a put, a completion, an item given back or the close, or until the first claim lapses.
            q._wait(r["revision"], deadline, r["retry_ms"])

    def __exit__(self, exc_type: Any, *exc: object) -> None:
        from ._store import _raise

        lease = self.lease
        try:
            if exc_type is None:
                # The item and its count go in one command, fenced by the
                # claim: a consumer whose lease lapsed removes nothing.
                r = self._q._txn([], [("delete", self.key, None, 0), ("add", "count", None, -1)], fence=lease)
                if not r["ok"] and not r["error"].endswith("does not exist"):
                    _raise(r)
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
        self.cursors: dict[str, int] = {}  # name -> the offset it acknowledged up to

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

    @method(group="admin")
    async def attach(self, cursor: str, offset: int) -> int:
        """Where `cursor` resumes: the offset it acknowledged, or for a new
        cursor `offset` (-1: the end), which it keeps from now."""
        async with self.cond():
            return self.cursors.setdefault(cursor, self.base + len(self.log) if offset < 0 else offset)

    @method(group="admin")
    async def ack(self, cursor: str, offset: int) -> None:
        async with self.cond():
            self.cursors[cursor] = max(self.cursors.get(cursor, 0), min(offset, self.base + len(self.log)))


class Topic(Generic[T]):
    """Publish once, deliver to every subscriber (pub/sub and broadcast).

    The topic keeps a bounded log: the newest `retain` items, given as a count
    (``retain=1024``) or a size (``retain="1 GiB"``). ``publish`` returns the
    item's offset. Each subscriber reads at its own pace from where it starts;
    one that falls behind the retained log skips ahead, and its ``gaps`` say
    exactly which offsets it missed. A subscriber named by ``cursor=`` can
    ``ack()`` what it has handled and resume there after it reconnects.
    ``name=`` shares one topic.
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

    def subscribe(self, start: str | int = "latest", *, cursor: str | None = None) -> Subscription[T]:
        """Items from `start`: ``"latest"`` (published from now on),
        ``"earliest"`` (everything retained) or an offset. With ``cursor=``
        (a name), the topic keeps the offset the subscription acknowledges
        (``ack()``), and subscribing with that name again resumes there:
        `start` counts only the first time."""
        start = _units.start_position(start)
        offset = -1 if start == "latest" else 0 if start == "earliest" else int(start)
        if cursor is not None:
            if not isinstance(cursor, str) or not cursor:
                raise ValueError(f"cursor: expected a non-empty name, got {cursor!r}")
            offset = self._host.attach.remote(cursor, offset).result()
        elif offset < 0:
            # Fixed now, not at the first read: an item published in between is seen.
            offset = self._host.end.remote().result()
        return Subscription(self._host, offset, cursor)

    def close(self) -> None:
        """No more publishes. Subscribers receive what is retained, then stop."""
        self._host.seal.remote().result()


class Gap(NamedTuple):
    """Offsets a subscriber missed because the topic no longer kept them:
    from ``first_missed`` up to, not including, ``first_available``."""

    first_missed: int
    first_available: int


class Subscription(Generic[T]):
    """An iterator (sync or async) over a topic's items, from one offset on.

    ``gaps`` lists each run of offsets it skipped because they had left the
    retained log before it read them, and ``skipped`` counts them all."""

    _BATCH = 256
    _POLL_S = 30.0

    def __init__(self, host: Any, offset: int, cursor: str | None = None) -> None:
        self._host = host
        self._offset = offset
        self._cursor = cursor
        self._buffer: collections.deque[Any] = collections.deque()
        self._ended = False
        self.skipped = 0
        self.gaps: list[Gap] = []

    @property
    def offset(self) -> int:
        """The offset of the next item this subscription will return."""
        return self._offset - len(self._buffer)

    def ack(self, offset: int | None = None) -> Future[None]:
        """Acknowledge every item before `offset` (default: those returned so
        far). Subscribing again with this cursor resumes there."""
        if self._cursor is None:
            raise ValueError("ack() needs a named cursor: topic.subscribe(cursor='name')")
        return self._host.ack.remote(self._cursor, self.offset if offset is None else offset)

    def _take(self, batch: tuple[int, list[Any], int, bool]) -> None:
        start, items, skipped, closed = batch
        if skipped:
            self.gaps.append(Gap(start - skipped, start))
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
