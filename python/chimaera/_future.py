"""Futures, result streams and the helpers that wait on them."""

from __future__ import annotations

import asyncio
import os
import sys
import time
import warnings
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from typing import Any, Generic, NamedTuple, TypeVar

from . import _context, _serde
from ._errors import from_outcome
from ._native import blake3_digest

T = TypeVar("T")

# CH101 detection: a call site whose futures are awaited one at a time.
_serial = {"site": None, "count": 0, "warned": set()}
_SERIAL_WARN_AT = 200


class Future(Generic[T]):
    """The eventual result of a call. Await it, or call ``result()``."""

    __slots__ = (
        "_rt", "key", "_outcome", "_value", "_has_value", "_exc", "_callbacks", "_site", "_escaped", "_arg_bytes", "__weakref__",
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

    def __repr__(self) -> str:
        state = "done" if self.done() else "pending"
        return f"<Future {self.key[16:].hex()[:12]} {state}>"

    @property
    def id(self) -> str:
        """Stable id of the call (run id + task id, hex)."""
        return self.key.hex()

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
            deadline = None if timeout is None else time.monotonic() + timeout
            with rt.cond:
                while not self.done():
                    left = None if deadline is None else deadline - time.monotonic()
                    if left is not None and left <= 0:
                        return False
                    rt.cond.wait(left if left is not None else 1.0)
            return True
        finally:
            if blocked:
                _executor_hook(ctx, False, [])

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
        # An embedded future is a handle: it pins its result for the run's
        # lifetime and can be awaited wherever it is unpickled.
        self._escaped = True
        return (_future_from_key, (self.key,))

    def __del__(self) -> None:
        rt = self._rt
        if self._escaped or rt is None or sys.is_finalizing():
            return
        try:
            rt.release_later(self.key)
        except Exception:  # noqa: S110 -- best effort during garbage collection
            return


def _future_from_key(key: bytes) -> Future[Any]:
    from ._runtime import Runtime

    rt = Runtime.get()
    f: Future[Any] = Future(rt, key)
    f._escaped = True
    rt.submit_watch(key, f)
    return f


# Set by the executor: tells the peer a task is blocked waiting on futures.
_executor_hook: Callable[[Any, bool, list[bytes]], None] | None = None


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


def put(value: Any) -> ObjectRef:
    """Store `value` once; pass the returned reference to many calls without
    re-sending it. Identical values share one copy."""
    from ._runtime import Runtime

    rt = Runtime.get()
    ctx = _context.current_task()
    run = ctx.run if ctx is not None else rt.run
    parts, size, _oob = _serde.dumps_parts(value)
    data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)
    object_id = blake3_digest(data)[:16]
    if size <= rt.inline_limit:
        return ObjectRef(run, object_id, data, size)
    path = _serde.object_path(rt.object_dir, run, object_id)
    if not os.path.exists(path):
        _serde.write_file(path, [data])
    return ObjectRef(run, object_id, None, size)


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
    ) -> None:
        self._submit = submit
        self._source = source
        self._ordered = ordered
        self._window = max(1, window)
        self._window_bytes = max(1, window_bytes)
        self._bytes = 0
        self._sizes: dict[int, int] = {}
        self._pending: dict[int, Future[T]] = {}
        self._next = 0
        self._yield = 0
        self._exhausted = False
        self._closed = False
        self._waker = _AsyncWaker()
        self._fill()

    def _fill(self) -> None:
        # In flight is bounded by items and by argument bytes (A3), so tiny
        # items can fill a deep window while large ones stay few. Refill in
        # bulk once half the window has drained, so the peer receives large
        # groups it can batch instead of a trickle of single items.
        if self._pending and len(self._pending) > self._window // 2 and self._bytes < self._window_bytes // 2:
            return
        while not self._exhausted and len(self._pending) < self._window and self._bytes < self._window_bytes:
            try:
                item = next(self._source)
            except StopIteration:
                self._exhausted = True
                break
            f = self._submit(self._next, item)
            size = getattr(f, "_arg_bytes", 0)
            self._sizes[self._next] = size
            self._bytes += size
            self._pending[self._next] = f
            self._next += 1

    def _forget(self, i: int) -> None:
        self._bytes -= self._sizes.pop(i, 0)

    def __iter__(self) -> Iterator[T]:
        return self

    def _take(self) -> tuple[int, Future[T]]:
        if self._closed:
            raise StopIteration
        if not self._pending:
            raise StopIteration
        if self._ordered:
            while self._yield not in self._pending:
                self._yield += 1
            i = self._yield
            self._yield += 1
            f = self._pending.pop(i)
            self._forget(i)
            self._fill()
            f._wait(None)
            return i, f
        first = next(iter(self._pending.values()))
        rt = first._rt
        rt.client.flush()
        with rt.cond:
            while True:
                done = [i for i, f in self._pending.items() if f.done()]
                if done:
                    i = min(done)
                    break
                rt.cond.wait(1.0)
        f = self._pending.pop(i)
        self._forget(i)
        self._fill()
        return i, f

    def __next__(self) -> T:
        _, f = self._take()
        return f._value_or_raise()

    def outcomes(self) -> Iterator[Outcome]:
        """(index, value, error) for every item; never raises an item's error."""
        while True:
            try:
                i, f = self._take()
            except StopIteration:
                return
            try:
                yield Outcome(i, f._value_or_raise(), None)
            except Exception as e:
                yield Outcome(i, None, e)

    def futures(self) -> Iterator[Future[T]]:
        """Futures in order, without fetching values (for chaining stages)."""
        while True:
            try:
                yield self._take_unwaited()
            except StopIteration:
                return

    def _take_unwaited(self) -> Future[T]:
        if self._closed or not self._pending:
            raise StopIteration
        while self._yield not in self._pending:
            self._yield += 1
        f = self._pending.pop(self._yield)
        self._forget(self._yield)
        self._yield += 1
        self._fill()
        return f

    def map(self, fn: Any, *, ordered: bool = True) -> Results[Any]:
        """Feed this stream into another remote callable. Each item's result is
        passed by handle, so values move between executors, not through here."""
        return fn.map(self.futures(), ordered=ordered)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self._source, "close", None)
        if callable(close):
            close()
        for f in self._pending.values():
            if not f.done():
                f._rt.client.cancel(f.key, False)
        self._pending.clear()

    def __enter__(self) -> Results[T]:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        if self._closed or not self._pending:
            raise StopAsyncIteration
        if self._ordered:
            return await self._take_unwaited()
        next(iter(self._pending.values()))._rt.client.flush()
        while True:
            done = [i for i, f in self._pending.items() if f.done()]
            if done:
                i = min(done)
                f = self._pending.pop(i)
                self._forget(i)
                self._fill()
                return f._value_or_raise()
            self._waker.watch(self._pending.values())
            await self._waker.wait(None)
