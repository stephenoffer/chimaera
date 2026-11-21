"""Futures, result streams and the helpers that wait on them."""

from __future__ import annotations

import asyncio
import os
import sys
import time
import warnings
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from typing import Any, Generic, NamedTuple, TypeVar

from . import _context, _serde, _values
from ._errors import StreamInterrupted, from_outcome

T = TypeVar("T")

# CH101 detection: a call site whose futures are awaited one at a time.
_serial = {"site": None, "count": 0, "warned": set()}
_SERIAL_WARN_AT = 200
# A wait this long on a call that has not started prints why (once).
STALL_NOTICE_S = float(os.environ.get("CHIMAERA_STALL_NOTICE_S", "60"))


class Future(Generic[T]):
    """The eventual result of a call. Await it, or call ``result()``."""

    __slots__ = (
        "_rt", "key", "_outcome", "_value", "_has_value", "_exc", "_callbacks", "_site", "_escaped", "_arg_bytes", "_put",
        "_returns", "_owned", "__weakref__",
    )

    def __init__(self, rt: Any, key: bytes, site: str | None = None) -> None:
        self._rt = rt
        self.key = key
        self._outcome: tuple[Any, ...] | None = None
        self._value: Any = None
        self._has_value = False
        self._exc: BaseException | None = None
        self._callbacks: list[Callable[[Future[T]], object]] = []
        self._site = site
        self._escaped = False
        self._arg_bytes = 0
        # For a value stored with ch.put: (run, object_id, inline or None, size).
        self._put: tuple[bytes, bytes, bytes | None, int] | None = None
        self._returns: int | None = None  # the call's `returns=` option, for split()
        # Files of a ch.put value this handle owns: deleted with it unless it
        # escaped (was pickled or passed by reference to a call).
        self._owned: list[str] | None = None

    def __repr__(self) -> str:
        state = "done" if self.done() else "pending"
        return f"<Future {self.key[16:].hex()[:12]} {state}>"

    @property
    def id(self) -> str:
        """Stable id of the call (run id + task id, hex)."""
        return self.key.hex()

    def metrics(self) -> list[dict[str, Any]]:
        """What the call has reported so far with ``ch.context().report(...)``,
        oldest first (its latest attempt's reports only)."""
        from ._metrics import reports

        return reports(self.key)

    def done(self) -> bool:
        return self._outcome is not None or self._exc is not None

    def _set(self, outcome: tuple[Any, ...]) -> None:
        if self.done():
            return
        self._outcome = outcome
        self._run_callbacks()

    def _set_exception(self, exc: BaseException) -> None:
        if self.done():
            return
        self._exc = exc
        self._run_callbacks()

    def _run_callbacks(self) -> None:
        cbs, self._callbacks = self._callbacks, []
        for cb in cbs:
            try:
                cb(self)
            except Exception as e:  # a callback's error must not stop delivery to others
                sys.stderr.write(f"chimaera: done-callback raised {e!r}\n")

    def add_done_callback(self, fn: Callable[[Future[T]], object]) -> None:
        if self.done():
            fn(self)
        else:
            self._callbacks.append(fn)

    def _wait(self, timeout: float | None) -> bool:
        if self.done():
            return True
        rt = self._rt
        rt.client.flush()
        ctx = _context.current_task()
        blocked = ctx is not None and _executor_hook is not None
        if blocked:
            _executor_hook(ctx, True, [self.key])
        try:
            start = time.monotonic()
            deadline = None if timeout is None else start + timeout
            noticed = False
            while True:
                with rt.cond:
                    while not self.done():
                        now = time.monotonic()
                        if deadline is not None and now >= deadline:
                            return False
                        if not noticed and now - start >= STALL_NOTICE_S:
                            break
                        rt.cond.wait(min(1.0, deadline - now) if deadline is not None else 1.0)
                    else:
                        return True
                noticed = True
                self._stall_notice(time.monotonic() - start)
        finally:
            if blocked:
                _executor_hook(ctx, False, [])

    def _stall_notice(self, waited: float) -> None:
        """Say once why a wait is long when the call has not started: Ray
        writes this only to a raylet log the caller never sees."""
        try:
            info = self.explain()
        except Exception:  # noqa: BLE001 -- a notice must never break the wait
            return
        reason = info.get("pending_reason") or ""
        if reason and reason != "running":
            sys.stderr.write(f"chimaera: waited {waited:.0f} s on call {self.key[16:].hex()[:12]}: {reason}\n")

    def result(self, timeout: float | None = None) -> T:
        """Wait for the value. A wait timeout raises TimeoutError and leaves the call running."""
        _note_serial_wait(self)
        if not self._wait(timeout):
            raise TimeoutError(f"no result within {timeout} s (the call is still running)")
        return self._value_or_raise()

    def exception(self, timeout: float | None = None) -> BaseException | None:
        if not self._wait(timeout):
            raise TimeoutError(f"no result within {timeout} s")
        try:
            self._value_or_raise()
        except BaseException as e:
            return e
        return None

    def _value_or_raise(self) -> T:
        if self._exc is not None:
            raise self._exc
        if self._has_value:
            return self._value
        assert self._outcome is not None
        if self._outcome[0] == "ok":
            self._value = self._rt.read_value(self._outcome)
            self._has_value = True
            if self._put is None:
                _values.fetched.note_fetched(self._value, self._outcome[4])
            return self._value
        exc = from_outcome(self._outcome, self.key[16:].hex())
        raise exc

    def cancel(self, *, force: bool = False) -> bool:
        """Request cancellation. True if cancellation won; False if the call had
        already produced its outcome (which is kept)."""
        if self.done():
            return self._cancelled()
        self._rt.client.cancel(self.key, force)
        self._wait(30.0)
        return self._cancelled()

    def _cancelled(self) -> bool:
        return self._outcome is not None and self._outcome[0] == "err" and self._outcome[1] in ("cancelled", "deadline")

    def split(self, n: int | None = None) -> tuple[Future[Any], ...]:
        """One future per value of a call that returns several (``returns=n``).

        Each part is its own dependency: pass ``a`` to one call and ``b`` to
        another. The call must return a sequence of exactly ``n`` values; a
        part used on anything else raises ``ValueError``.
        """
        n = n if n is not None else self._returns
        if not isinstance(n, int) or isinstance(n, bool) or n < 1:
            raise TypeError("split() needs the number of values: set returns=n on the call, or pass split(n)")
        return tuple(_Part(self, i, n) for i in range(n))

    def explain(self) -> dict[str, Any]:
        """What happened to this call: state, waiting reason, attempts and measurements."""
        self._rt.client.flush()
        return self._rt.client.explain(self.key)

    def __await__(self):  # type: ignore[no-untyped-def]
        if not self.done():
            loop = asyncio.get_running_loop()
            waiter = loop.create_future()

            def wake(_: Future[T]) -> None:
                loop.call_soon_threadsafe(lambda: waiter.done() or waiter.set_result(None))

            self.add_done_callback(wake)
            self._rt.client.flush()
            yield from waiter.__await__()
        return self._value_or_raise()

    def __reduce__(self) -> tuple[Any, ...]:
        if self._put is not None:
            self._escaped = True  # copies may outlive this handle: keep its files for the run
            return (_resolved_future, self._put)
        # An embedded future is a handle: it pins its result for the run's
        # lifetime and can be awaited wherever it is unpickled.
        self._escaped = True
        return (_future_from_key, (self.key,))

    def __del__(self) -> None:
        rt = self._rt
        if self._escaped or rt is None or sys.is_finalizing():
            return
        if self._put is not None:
            # A ch.put value nothing else refers to: free its files now rather
            # than at the end of the run. Values already read stay mapped.
            for path in self._owned or ():
                try:
                    os.unlink(path)
                except OSError:
                    pass
            return
        try:
            rt.release_later(self.key)
        except Exception:  # noqa: S110 -- best effort during garbage collection
            return


class _Part(Future[Any]):
    """Value ``index`` of a call that returns ``count`` values (``Future.split``).

    It waits on, cancels and explains its parent call. Passed as an argument it
    is a dependency on the parent, and the callee receives just this value.
    """

    __slots__ = ("_parent", "_index", "_count")

    def __init__(self, parent: Future[Any], index: int, count: int) -> None:
        super().__init__(parent._rt, parent.key, parent._site)
        self._parent, self._index, self._count = parent, index, count
        self._escaped = True  # the parent holds the pin

    def __repr__(self) -> str:
        return f"<Future {self.key[16:].hex()[:12]} part {self._index}/{self._count} {'done' if self.done() else 'pending'}>"

    @property
    def id(self) -> str:
        return f"{self._parent.id}#{self._index}"

    def done(self) -> bool:
        return self._parent.done()

    def _wait(self, timeout: float | None) -> bool:
        return self._parent._wait(timeout)

    def _value_or_raise(self) -> Any:
        return pick(self._parent._value_or_raise(), self._index, self._count)

    def add_done_callback(self, fn: Callable[[Future[Any]], object]) -> None:
        self._parent.add_done_callback(lambda _: fn(self))

    def cancel(self, *, force: bool = False) -> bool:
        """Cancels the whole call the part belongs to."""
        return self._parent.cancel(force=force)

    def explain(self) -> dict[str, Any]:
        return self._parent.explain()

    def __reduce__(self) -> tuple[Any, ...]:
        return (_Part, (self._parent, self._index, self._count))


class PartDep:
    """Call-frame placeholder: value ``index`` of dependency ``dep``, a call returning ``count`` values."""

    __slots__ = ("dep", "index", "count")

    def __init__(self, dep: int, index: int, count: int) -> None:
        self.dep, self.index, self.count = dep, index, count

    def __reduce__(self) -> tuple[Any, ...]:
        return (PartDep, (self.dep, self.index, self.count))


def pick(value: Any, index: int, count: int) -> Any:
    """Value ``index`` of a call declared to return ``count`` values (or item
    ``index`` of a map chunk, whose failed items carry their own error)."""
    if not isinstance(value, (tuple, list)) or len(value) != count:
        got = f"a {type(value).__name__} of {len(value)}" if isinstance(value, (tuple, list)) else type(value).__name__
        raise ValueError(f"the call was split into {count} values (returns={count}) but returned {got}")
    v = value[index]
    if isinstance(v, ItemFailed):
        raise from_outcome(v.err)
    return v


class ItemFailed:
    """An item of a map chunk that raised; the other items of the chunk keep
    their values. ``err`` is an error outcome tuple, as the peer reports one."""

    __slots__ = ("err",)

    def __init__(self, err: tuple[Any, ...]) -> None:
        self.err = err

    def __reduce__(self) -> tuple[Any, ...]:
        return (ItemFailed, (self.err,))


class _Chunk(list):  # type: ignore[type-arg]
    """The results of a map chunk, and the seconds its items took on the worker."""

    elapsed: float = 0.0


# Worker time a map chunk aims for: long enough to amortize a call, short
# enough to keep items flowing and a lost chunk cheap to redo.
CHUNK_TARGET_S = 0.1
MAX_CHUNK = 4096


def _future_from_key(key: bytes) -> Future[Any]:
    from ._runtime import Runtime

    rt = Runtime.get()
    f: Future[Any] = Future(rt, key)
    f._escaped = True
    rt.submit_watch(key, f)
    return f


# Set by the executor: tells the peer a task is blocked waiting on futures.
_executor_hook: Callable[[Any, bool, list[bytes]], None] | None = None


class waiting:
    """Mark a wait inside a call (a lock, a barrier, a watch) as blocked, as a
    future's ``result()`` does: the peer may lend the call's CPU meanwhile, and
    the executor starts the next call of its batch."""

    def __enter__(self) -> None:
        self.ctx = _context.current_task()
        if self.ctx is not None and _executor_hook is not None:
            _executor_hook(self.ctx, True, [])

    def __exit__(self, *exc: object) -> None:
        if self.ctx is not None and _executor_hook is not None:
            _executor_hook(self.ctx, False, [])


def _note_serial_wait(f: Future[Any]) -> None:
    """Warn once per call site when results are awaited one at a time (CH101)."""
    site = f._site
    if site is None or f.done():
        _serial["site"], _serial["count"] = None, 0
        return
    if _serial["site"] == site:
        _serial["count"] += 1
    else:
        _serial["site"], _serial["count"] = site, 1
    if _serial["count"] == _SERIAL_WARN_AT and site not in _serial["warned"]:
        _serial["warned"].add(site)
        warnings.warn(
            f"CH101: calls from {site} ran {_SERIAL_WARN_AT} times one at a time: each result was awaited "
            "before the next call was submitted. Submit them all first, or use fn.map(items).",
            stacklevel=3,
        )


class Stream(Generic[T]):
    """The items of a generator call, in order, as the generator yields them.

    Iterate it (``for x in fn.remote(...)``, or ``async for``). The generator
    runs at most a window of items ahead of you (``max_in_flight``, default
    64). If its worker dies, the generator runs again and the items you
    already have are skipped, so a generator should yield the same items each
    time (the default ``deterministic=True``). An error raised in the
    generator is raised here after the items before it. ``close()`` stops it.
    """

    def __init__(self, fut: Future[int], window: int) -> None:
        self._fut = fut
        self.key = fut.key
        self._rt = fut._rt
        self._items: dict[int, tuple[Any, ...]] = {}
        self._next = 0
        self._window = window
        self._credited = 0
        self._rt._streams[self.key] = self

    def __repr__(self) -> str:
        return f"<Stream {self.key[16:].hex()[:12]} at item {self._next}>"

    def _add(self, index: int, outcome: tuple[Any, ...]) -> None:
        """An item arrived (dispatcher thread; a replayed attempt repeats indices)."""
        if index >= self._next:
            self._items.setdefault(index, outcome)

    def __iter__(self) -> Stream[T]:
        return self

    def __next__(self) -> T:
        rt = self._rt
        rt.client.flush()
        with rt.cond:
            while self._next not in self._items:
                if self._fut.done():
                    count = self._fut._value_or_raise()  # the generator's error, if it raised
                    if self._next >= count:
                        raise StopIteration
                    raise StreamInterrupted(f"the stream ended after {self._next} of {count} items")
                rt.cond.wait(1.0)
            outcome = self._items.pop(self._next)
        self._next += 1
        if self._next - self._credited >= max(1, self._window // 2):
            self._credited = self._next
            rt.client.stream_credit(self.key, self._next)
        return rt.read_value(outcome)

    def __aiter__(self) -> Stream[T]:
        return self

    async def __anext__(self) -> T:
        # StopIteration cannot cross a thread boundary into a coroutine.
        end = object()
        item = await asyncio.to_thread(lambda: next(self, end))
        if item is end:
            raise StopAsyncIteration
        return item

    def close(self) -> None:
        """Stop the generator; items not taken yet are dropped."""
        self._fut.cancel()

    def __enter__(self) -> Stream[T]:
        return self

    def __exit__(self, *exc: object) -> None:
        if not self._fut.done():
            self.close()


class ObjectRef:
    """A value stored once with ``ch.put`` and passed by reference."""

    __slots__ = ("run", "object_id", "inline", "size")

    def __init__(self, run: bytes, object_id: bytes, inline: bytes | None, size: int) -> None:
        self.run, self.object_id, self.inline, self.size = run, object_id, inline, size

    def __reduce__(self) -> tuple[Any, ...]:
        return (_load_object, (self.run, self.object_id, self.inline))

    def result(self, timeout: float | None = None) -> Any:
        return _load_object(self.run, self.object_id, self.inline)


def _load_object(run: bytes, object_id: bytes, inline: bytes | None) -> Any:
    if inline is not None:
        return _serde.loads(inline)
    from ._runtime import Runtime, read_object

    return read_object(Runtime.get().object_dir, run, object_id)


def _resolved_future(run: bytes, object_id: bytes, inline: bytes | None, size: int) -> Future[Any]:
    """A finished future for a stored value (what ch.put returns)."""
    from ._runtime import Runtime

    rt = Runtime.get()
    f: Future[Any] = Future(rt, run + object_id)
    f._escaped = True  # nothing to release: the value lives as long as its run
    f._put = (run, object_id, inline, size)
    f._outcome = ("ok", inline, run, object_id, size, 0)
    return f


def put(value: Any) -> Future[Any]:
    """Store `value` once and return a finished future for it. Pass the
    future to any number of calls: each reads the stored value instead of
    receiving a copy. Identical values share one copy."""
    from ._runtime import Runtime

    rt = Runtime.get()
    ctx = _context.current_task()
    run = ctx.run if ctx is not None else rt.run
    assert run is not None
    object_id, inline, size, owned = _values.store(value, run, rt.blob_sink, rt.inline_limit)
    f = _resolved_future(run, object_id, inline, size)
    if owned:
        f._escaped, f._owned = False, owned
    return f


def gather(*futures: Future[Any], timeout: float | None = None) -> list[Any]:
    """Wait for all; return their values in order. Raises the first error."""
    deadline = None if timeout is None else time.monotonic() + timeout
    out = []
    for f in futures:
        left = None if deadline is None else max(0.0, deadline - time.monotonic())
        out.append(f.result(left))
    return out


class _AsyncWaker:
    """Wakes one event loop when any registered future completes.

    Each future gets a single callback, so waiting repeatedly over a shrinking
    set stays linear; cancelling the awaiting coroutine leaves nothing running.
    """

    def __init__(self) -> None:
        self._registered: set[int] = set()
        self._event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def watch(self, futures: Iterable[Future[Any]]) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self._loop, self._event, self._registered = loop, asyncio.Event(), set()
        for f in futures:
            if id(f) not in self._registered:
                self._registered.add(id(f))
                f.add_done_callback(self._wake)

    def _wake(self, _: Future[Any]) -> None:
        loop, event = self._loop, self._event
        if loop is not None and event is not None:
            loop.call_soon_threadsafe(event.set)

    async def wait(self, timeout: float | None) -> None:
        assert self._event is not None
        await asyncio.wait_for(self._event.wait(), timeout)
        self._event.clear()


class as_completed(Generic[T]):  # noqa: N801 -- mirrors concurrent.futures.as_completed
    """Iterate futures in completion order (sync or async)."""

    def __init__(self, futures: Iterable[Future[T]], *, timeout: float | None = None) -> None:
        self._pending = list(futures)
        self._deadline = None if timeout is None else time.monotonic() + timeout
        self._waker = _AsyncWaker()

    def __iter__(self) -> Iterator[Future[T]]:
        return self

    def __next__(self) -> Future[T]:
        if not self._pending:
            raise StopIteration
        rt = self._pending[0]._rt
        rt.client.flush()
        with rt.cond:
            while True:
                for i, f in enumerate(self._pending):
                    if f.done():
                        return self._pending.pop(i)
                left = None if self._deadline is None else self._deadline - time.monotonic()
                if left is not None and left <= 0:
                    raise TimeoutError(f"{len(self._pending)} futures did not complete in time")
                rt.cond.wait(left if left is not None else 1.0)

    def __aiter__(self) -> AsyncIterator[Future[T]]:
        return self

    async def __anext__(self) -> Future[T]:
        if not self._pending:
            raise StopAsyncIteration
        self._pending[0]._rt.client.flush()
        while True:
            for i, f in enumerate(self._pending):
                if f.done():
                    return self._pending.pop(i)
            self._waker.watch(self._pending)
            left = None if self._deadline is None else self._deadline - time.monotonic()
            if left is not None and left <= 0:
                raise TimeoutError(f"{len(self._pending)} futures did not complete in time")
            try:
                await self._waker.wait(left)
            except asyncio.TimeoutError:
                raise TimeoutError(f"{len(self._pending)} futures did not complete in time") from None


class Outcome(NamedTuple):
    index: int
    value: Any
    error: BaseException | None


class _Slot:
    """Calls in flight for a Results: one item, or a chunk of ``k`` items."""

    __slots__ = ("first", "fut", "k", "chunk", "size")

    def __init__(self, first: int, fut: Future[Any], k: int, chunk: bool, size: int) -> None:
        self.first, self.fut, self.k, self.chunk, self.size = first, fut, k, chunk, size


class Results(Generic[T]):
    """A single-pass stream of results from ``fn.map``.

    At most ``window`` items are in flight; more are submitted as results are
    consumed, so a slow consumer bounds the work and memory upstream. An item's
    error is raised at its position; if caught, iteration continues. Leaving a
    ``with`` block or calling ``close()`` cancels outstanding items and closes
    the source.
    """

    def __init__(
        self,
        submit: Callable[[int, Any], Future[T]],
        source: Iterator[Any],
        *,
        ordered: bool,
        window: int,
        window_bytes: int = 256 << 20,
        chunk: Callable[[int, list[Any]], Future[Any]] | None = None,
        parallel: int = 1,
        grow_window: bool = False,
    ) -> None:
        # `chunk(first_index, items)` submits several items as one call whose
        # value is a _Chunk. Chunks start at one item and grow only by the
        # worker time items are measured to take (A1), so slow items are never
        # grouped. A window the caller chose is a hard cap; a default one
        # widens so every worker can hold two chunks.
        self._submit = submit
        self._chunk = chunk
        self._parallel = max(1, parallel)
        self._grow_window = grow_window
        self._item_s: float | None = None
        self._source = source
        self._ordered = ordered
        self._window = max(1, window)
        self._window_bytes = max(1, window_bytes)
        self._slots: dict[int, _Slot] = {}  # by first index, in submission order
        self._items = 0  # items in flight
        self._bytes = 0
        self._out_total = 0
        self._out_n = 0
        self._next = 0
        # Items of slots already finished, waiting to be handed out:
        # (index, value, error, slot, position in the slot).
        self._buf: deque[tuple[int, Any, BaseException | None, _Slot, int]] = deque()
        self._exhausted = False
        self._closed = False
        self._waker = _AsyncWaker()
        self._fill()

    # ---- submission ----------------------------------------------------------

    def _chunk_size(self) -> int:
        if self._chunk is None or self._item_s is None:
            return 1
        return max(1, min(MAX_CHUNK, int(CHUNK_TARGET_S / max(self._item_s, 1e-7))))

    def _fill(self) -> None:
        # In flight is bounded by items and by argument bytes (A3). Refill in
        # bulk once half the window has drained, so the peer receives large
        # groups instead of a trickle of single items.
        window = self._window
        k = min(self._chunk_size(), window)
        if self._grow_window:
            window = max(window, 2 * self._parallel * k)
        if self._slots and self._items > window // 2 and self._bytes < self._window_bytes // 2:
            return
        out_avg = self._out_total // self._out_n if self._out_n else 0
        while not self._exhausted and self._items < window and self._bytes < self._window_bytes:
            want = max(1, min(k, window - self._items))
            items: list[Any] = []
            while len(items) < want:
                try:
                    items.append(next(self._source))
                except StopIteration:
                    self._exhausted = True
                    break
            if not items:
                break
            if self._chunk is not None and isinstance(items[0], Future):
                self._chunk = None  # chained stages pass futures: one call each
            if self._chunk is None:
                for item in items:
                    f = self._submit(self._next, item)
                    self._add(_Slot(self._next, f, 1, False, getattr(f, "_arg_bytes", 0) + out_avg))
            else:
                f = self._chunk(self._next, items)
                self._add(_Slot(self._next, f, len(items), True, getattr(f, "_arg_bytes", 0) + out_avg * len(items)))

    def _add(self, slot: _Slot) -> None:
        self._slots[slot.first] = slot
        self._items += slot.k
        self._bytes += slot.size
        self._next += slot.k

    def _remove(self, slot: _Slot) -> None:
        del self._slots[slot.first]
        self._items -= slot.k
        self._bytes -= slot.size

    # ---- consumption ---------------------------------------------------------

    def _pop_ordered(self) -> _Slot:
        slot = self._slots[next(iter(self._slots))]
        self._remove(slot)
        self._fill()
        return slot

    def _pop_done(self) -> _Slot:
        """The finished slot with the lowest index (completion order)."""
        first = next(iter(self._slots.values())).fut
        rt = first._rt
        rt.client.flush()
        with rt.cond:
            while True:
                done = [s for s in self._slots.values() if s.fut.done()]
                if done:
                    slot = min(done, key=lambda s: s.first)
                    break
                rt.cond.wait(1.0)
        self._remove(slot)
        self._fill()
        return slot

    def _expand(self, slot: _Slot) -> None:
        """Move a finished slot's items into the buffer."""
        f = slot.fut
        outcome = f._outcome
        if outcome is not None and outcome[0] == "ok":
            self._out_total += outcome[4]
            self._out_n += slot.k
        try:
            value = f._value_or_raise()
        except Exception as e:  # the call failed as a whole: every item has its error
            for j in range(slot.k):
                self._buf.append((slot.first + j, None, e, slot, j))
            return
        if not slot.chunk:
            self._buf.append((slot.first, value, None, slot, 0))
            return
        if isinstance(value, _Chunk) and value:
            per = value.elapsed / len(value)
            self._item_s = per if self._item_s is None else 0.7 * self._item_s + 0.3 * per
        for j, v in enumerate(value):
            if isinstance(v, ItemFailed):
                self._buf.append((slot.first + j, None, from_outcome(v.err), slot, j))
            else:
                self._buf.append((slot.first + j, v, None, slot, j))

    def _next_item(self) -> tuple[int, Any, BaseException | None, _Slot, int]:
        if self._buf:
            return self._buf.popleft()
        if self._closed or not self._slots:
            raise StopIteration
        if self._ordered:
            slot = self._pop_ordered()
            slot.fut._wait(None)
        else:
            slot = self._pop_done()
        self._expand(slot)
        return self._buf.popleft()

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        _, value, err, _, _ = self._next_item()
        if err is not None:
            raise err
        return value

    def outcomes(self) -> Iterator[Outcome]:
        """(index, value, error) for every item; never raises an item's error."""
        while True:
            try:
                i, value, err, _, _ = self._next_item()
            except StopIteration:
                return
            yield Outcome(i, value, err)

    @staticmethod
    def _item_future(slot: _Slot, j: int) -> Future[Any]:
        return _Part(slot.fut, j, slot.k) if slot.chunk else slot.fut

    def futures(self) -> Iterator[Future[T]]:
        """Futures in order, without fetching values (for chaining stages).

        Items submitted from here on are one call each, so cancelling an
        item's future cancels only that item."""
        self._chunk = None
        while True:
            if self._buf:
                _, _, _, slot, j = self._buf.popleft()
                yield self._item_future(slot, j)
                continue
            if self._closed or not self._slots:
                return
            slot = self._pop_ordered()
            for j in range(slot.k):
                yield self._item_future(slot, j)

    def map(self, fn: Any, *, ordered: bool = True) -> Results[Any]:
        """Feed this stream into another remote callable. Each item's result is
        passed by handle, so values move between executors, not through here."""
        return fn.map(self.futures(), ordered=ordered)

    def reduce_by_key(self, combine: Any = None, *, partitions: int | None = None) -> Any:
        """Group the (key, value) pairs each result yields by key, across all
        results, folding each key's values with `combine` (associative; without
        it, values are collected into lists). The pairs are repartitioned
        between workers, never through here. Returns a ``Partitioned``."""
        from ._data import reduce_by_key

        return reduce_by_key(self, combine, partitions=partitions)

    def write(self, sink: Any) -> Future[Any]:
        """Write each result as one partition of `sink` (an ``ext.Sink``), from
        the worker that holds it; the future gives the sink's commit."""
        from ._data import write

        return write(list(self.futures()), sink)

    def write_parquet(self, directory: str) -> Future[Any]:
        """Write each result as a Parquet part file in `directory` (needs pyarrow)."""
        from .ext import FileSink

        return self.write(FileSink(directory, format="parquet"))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self._source, "close", None)
        if callable(close):
            close()
        for slot in self._slots.values():
            if not slot.fut.done():
                slot.fut._rt.client.cancel(slot.fut.key, False)
        self._slots.clear()
        self._buf.clear()

    def __enter__(self) -> Results[T]:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        if not self._buf:
            if self._closed or not self._slots:
                raise StopAsyncIteration
            if self._ordered:
                slot = self._pop_ordered()
                try:
                    await slot.fut
                except Exception:
                    pass  # _expand reports the call's error at each item
            else:
                next(iter(self._slots.values())).fut._rt.client.flush()
                while not any(s.fut.done() for s in self._slots.values()):
                    self._waker.watch(s.fut for s in self._slots.values())
                    await self._waker.wait(None)
                slot = min((s for s in self._slots.values() if s.fut.done()), key=lambda s: s.first)
                self._remove(slot)
                self._fill()
            self._expand(slot)
        _, value, err, _, _ = self._buf.popleft()
        if err is not None:
            raise err
        return value


