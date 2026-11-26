"""Futures, result streams and the helpers that wait on them."""

from __future__ import annotations

import asyncio
import itertools
import os
import queue
import sys
import threading
import time
import warnings
from collections import deque
from collections.abc import AsyncIterator, Callable, Generator, Iterable, Iterator
from typing import Any, Generic, Literal, NamedTuple, TypeVar, overload

from . import _context, _serde, _values
from ._completions import Completions, any_done, wake
from ._errors import Infeasible, InfrastructureFailure, OptionsError, StreamInterrupted, from_outcome

T = TypeVar("T")

# CH101 detection: a call site whose futures are awaited one at a time.
_serial = {"site": None, "count": 0, "warned": set()}
_SERIAL_WARN_AT = 200
# A wait this long on a call that has not started prints why (once).
STALL_NOTICE_S = float(os.environ.get("CHIMAERA_STALL_NOTICE_S", "60"))
# Held while a future takes its outcome and hands out its callbacks, and while
# a callback is added or dropped, never while one runs: a callback added as the
# call finishes is handed out or runs at once, and only the first outcome
# counts. One lock for every future, held for a few attribute reads. It is
# reentrant: the collector may run an abandoned as_completed's __del__, which
# drops its callbacks, on a thread that holds it.
_done_lock = threading.RLock()


class Future(Generic[T]):
    """The eventual result of a call. Await it, or call ``result()``."""

    __slots__ = (
        "_rt", "key", "_outcome", "_value", "_has_value", "_exc", "_callbacks", "_site", "_escaped", "_arg_bytes", "_put",
        "_returns", "_owned", "_lazy", "_device_holds", "_seen", "__weakref__",
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
        # For a value stored with ch.put: (run, object_id, inline or None, size,
        # the files it is stored in, its durability class or "" for the default).
        self._put: tuple[bytes, bytes, bytes | None, int, tuple[bytes, ...], str] | None = None
        self._returns: int | None = None  # the call's `returns=` option, for split()
        # Files of a ch.put value this handle owns: deleted with it unless it
        # escaped (was pickled or passed by reference to a call).
        self._owned: list[str] | None = None
        # Owned files a call reads only if it needs them (see _values.store).
        self._lazy: frozenset[bytes] = frozenset()
        self._device_holds: list[Any] | None = None  # CUDA tensors a put shares (see _values._cuda_share)
        self._seen = False  # its outcome was read, or handed to a call or callback (see _unread_error)

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
        with _done_lock:  # the first outcome wins, and takes every callback added before it
            if self.done():
                return
            self._outcome = outcome
            cbs, self._callbacks = self._callbacks, []
        self._run_callbacks(cbs)

    def _set_exception(self, exc: BaseException) -> None:
        with _done_lock:
            if self.done():
                return
            self._exc = exc
            cbs, self._callbacks = self._callbacks, []
        self._run_callbacks(cbs)

    def _run_callbacks(self, cbs: list[Callable[[Future[T]], object]]) -> None:
        for cb in cbs:
            try:
                cb(self)
            except Exception as e:  # a callback's error must not stop delivery to others
                sys.stderr.write(f"chimaera: done-callback raised {e!r}\n")

    def add_done_callback(self, fn: Callable[[Future[T]], object]) -> None:
        """Call ``fn(self)`` once the call has finished: at once if it has,
        else on a thread for callbacks, in the order they were added, so a
        slow callback delays only the callbacks after it, never results."""
        self._seen = True  # the callback reads the outcome
        with _done_lock:  # the check and the append, with no outcome between them
            if not self.done():
                self._callbacks.append(lambda f: _callback_queue().put((fn, f)))
                return
        fn(self)

    def _on_done(self, fn: Callable[[Future[T]], object]) -> Callable[[Future[T]], object] | None:
        """For the runtime's own bookkeeping: ``fn`` runs on the thread that
        delivers outcomes, so it must be quick and never wait. Returns what
        ``_off_done`` takes to drop it (None: it has run)."""
        with _done_lock:
            if not self.done():
                self._callbacks.append(fn)
                return fn
        fn(self)
        return None

    def _off_done(self, fn: Callable[[Future[T]], object]) -> None:
        """Drop a callback ``_on_done`` kept, unless it has run: its waiter stopped waiting."""
        with _done_lock:
            if fn in self._callbacks:
                self._callbacks.remove(fn)

    def _wait(self, timeout: float | None, between: Callable[[], None] | None = None) -> bool:
        """Until the call finishes (False if `timeout` passes first).
        `between` runs, outside rt.cond, each time outcomes arrive meanwhile."""
        if self.done():
            return True
        start_pending()
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
                        if between is not None:
                            break
                    else:
                        return True
                if between is not None:
                    between()
                if not noticed and time.monotonic() - start >= STALL_NOTICE_S:
                    noticed = True
                    self._stall_notice(time.monotonic() - start)
        finally:
            if blocked:
                _executor_hook(ctx, False, [self.key])

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
        self._seen = True
        if self._exc is not None:
            raise self._exc
        if self._has_value:
            return self._value
        assert self._outcome is not None
        if self._outcome[0] == "ok":
            # A call's result names its call, which can run again if every
            # copy is lost; a put value has no call to remake it.
            if self._put is None:
                self._value = self._rt.read_value(self._outcome, self.key)
            else:
                self._value = self._rt.read_value(self._outcome, None, durability=self._put[5])
            self._has_value = True
            if self._put is None:
                _values.fetched.note_fetched(self._value, self._outcome[4])
            return self._value
        exc = from_outcome(self._outcome, self.key[16:].hex())
        raise exc

    def cancel(self, *, force: bool = False) -> bool:
        """Ask the call, and every call it started, to stop; returns at once.
        True if it had not finished (its result then raises ``ch.Cancelled``,
        unless it finishes first), False if it had (its outcome is kept).

        A running call that does not stop by itself is interrupted after 2 s
        and its worker stopped after 5 s (``force=True``: at once). An actor
        method's cancellation stays cooperative: check ``ch.context().cancelled()``."""
        if self.done():
            return self._cancelled()
        from . import _direct

        _direct.cancel(self)  # a direct call: its executor hears of it at once
        self._rt.client.cancel(self.key, force)
        return True

    def cancelled(self) -> bool:
        """True once the call ended because it was cancelled (or ran out of
        its deadline or budget)."""
        return self._cancelled()

    def running(self) -> bool:
        """True while the call runs on a worker (asks its peer)."""
        return not self.done() and self.explain().get("state") == "Running"

    def _cancelled(self) -> bool:
        return self._outcome is not None and self._outcome[0] == "err" and self._outcome[1] in ("cancelled", "deadline", "budget")

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
        """What happened to this call: state, waiting reason, attempts and
        measurements; while it is not done, also each managed pool's latest
        plan (``capacity``, when a forecast provisioner runs)."""
        from ._cluster import ClusterView
        from ._errors import ChimaeraError

        self._rt.client.flush()
        d = self._rt.client.explain(self.key)
        if d.get("state") != "Done":
            # Read from the store: a store that cannot answer (no quorum) is
            # said here, not raised, since that is when explain() is wanted.
            try:
                if plans := ClusterView().capacity():
                    d["capacity"] = plans
            except ChimaeraError as e:
                d["capacity_error"] = str(e)
        return d

    def __await__(self) -> Generator[Any, None, T]:
        if not self.done():
            waiter = asyncio.get_running_loop().create_future()
            fn = self._on_done(lambda _: wake(waiter))
            self._rt.client.flush()
            try:
                yield from waiter.__await__()
            finally:  # cancelled or timed out: nothing stays waiting on the call
                if fn is not None:
                    self._off_done(fn)
        return self._value_or_raise()

    def __reduce__(self) -> tuple[Any, ...]:
        if self._put is not None:
            self._escaped = True  # copies may outlive this handle: keep its files for the run
            if self._device_holds:
                _values.keep_device_holds(self._device_holds)  # and its shared GPU memory
            from ._leases import keep, unpickle  # unpickled under another run, it holds the value (see _leases)

            keep(self)  # and another run's value held here stays held for the session
            return (unpickle, self._put)
        # An embedded future is a handle: it pins its result for the run's
        # lifetime and can be awaited wherever it is unpickled.
        self._escaped = True
        return (_future_from_key, (self.key,))

    def __del__(self) -> None:
        rt = self._rt
        if self._escaped or rt is None or sys.is_finalizing() or rt.forked:
            return
        if self._put is not None:
            # A ch.put value nothing else refers to, whose readers have
            # finished: free its files now, here and on the peers that fetched
            # them, rather than at the end of the run. Values already read
            # stay mapped.
            released = []
            for path in self._owned or ():
                try:
                    os.unlink(path)
                except OSError:
                    pass
                try:
                    released.append(bytes.fromhex(os.path.basename(path)))
                except ValueError:
                    pass
            # A task's own puts stay for the run: an executor's session owns
            # no run, and a peer releases only the requesting session's values.
            if released and not rt.in_executor:
                try:
                    rt.client.release_objects(self._put[0], released)
                except Exception:  # noqa: S110 -- best effort during garbage collection
                    pass
            return
        if _unread_failure(self):
            try:
                _note_unread(rt, [self])
            except Exception:  # noqa: S110 -- best effort during garbage collection
                pass
        try:
            rt.release_later(self.key)
        except Exception:  # noqa: S110 -- best effort during garbage collection
            return


UNREAD_SHOWN = 10  # failed calls whose errors nobody read, shown one by one; then counted
_PACKAGE = os.path.dirname(os.path.abspath(__file__))


def _unread_failure(f: Future[Any]) -> bool:
    """True if `f`'s call failed and nothing read its error: not read, not
    handed to another call or a callback, not cancelled (nor its actor closed
    by the program), and made by the program rather than the runtime itself."""
    out = f._outcome
    failed = f._exc is not None or out is not None and out[0] == "err"
    if f._seen or f._escaped or not failed or f._cancelled():
        return False
    if out is not None and out[1] == "actor_died" and "actor closed" in (out[3] or ""):
        return False
    site = f._site or ""
    return bool(site) and not site.startswith(_PACKAGE)


def _note_unread(rt: Any, futures: list[Future[Any]]) -> None:
    """Say on stderr that these calls failed and nothing read their errors,
    as asyncio does for a task's exception never retrieved (Ray Client drops
    such an error silently). Past UNREAD_SHOWN, they are only counted, and
    the count is printed at exit."""
    for f in futures:
        f._seen = True
        rt.unread_errors += 1
        if rt.unread_errors > UNREAD_SHOWN:
            continue
        if f._exc is not None:
            type_name, message = type(f._exc).__name__, str(f._exc)
        else:
            _, kind, type_name, message = f._outcome[:4]  # type: ignore[index]
            type_name = (type_name or kind).removeprefix("builtins.")
        first = (message or "").strip().splitlines()[:1]
        name = rt._names.get(f.key) or "a call"
        sys.stderr.write(
            f"chimaera: {name} (called at {f._site}) failed and nothing read its error: "
            f"{type_name}: {first[0] if first else ''}\n"
        )


_callbacks: queue.SimpleQueue[tuple[Callable[[Any], object], Any]] | None = None
_callbacks_lock = threading.Lock()


def _callback_queue() -> queue.SimpleQueue[tuple[Callable[[Any], object], Any]]:
    """The queue of user done-callbacks, run in order by one daemon thread."""
    global _callbacks
    with _callbacks_lock:
        if _callbacks is None:
            q: queue.SimpleQueue[tuple[Callable[[Any], object], Any]] = queue.SimpleQueue()

            def run() -> None:
                while True:
                    fn, f = q.get()
                    try:
                        fn(f)
                    except Exception as e:  # one callback's error must not stop the others
                        sys.stderr.write(f"chimaera: done-callback raised {e!r}\n")

            threading.Thread(target=run, name="chimaera-callbacks", daemon=True).start()
            _callbacks = q
    return _callbacks


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

    def _wait(self, timeout: float | None, between: Callable[[], None] | None = None) -> bool:
        return self._parent._wait(timeout, between)

    def _value_or_raise(self) -> Any:
        return pick(self._parent._value_or_raise(), self._index, self._count)

    def add_done_callback(self, fn: Callable[[Future[Any]], object]) -> None:
        self._parent.add_done_callback(lambda _: fn(self))

    def _on_done(self, fn: Callable[[Future[Any]], object]) -> Callable[[Future[Any]], object] | None:
        return self._parent._on_done(lambda _: fn(self))

    def _off_done(self, fn: Callable[[Future[Any]], object]) -> None:
        self._parent._off_done(fn)

    def cancel(self, *, force: bool = False) -> bool:
        """Cancels the whole call the part belongs to."""
        return self._parent.cancel(force=force)

    def explain(self) -> dict[str, Any]:
        return self._parent.explain()

    def __reduce__(self) -> tuple[Any, ...]:
        return (_Part, (self._parent, self._index, self._count))


class Handles:
    """``ch.handles(value)``: an argument whose futures reach the call as
    futures (handles to wait on), not as their values."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value


def handles(value: T) -> T:
    """Pass `value` with the futures in it unresolved: the call receives them
    as futures. Otherwise a future passed to a call, directly or one level
    inside a list, tuple or dict argument, arrives as its value, and the call
    starts once it is ready."""
    return Handles(value)  # type: ignore[return-value]


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
_ASYNC_SOURCE = "this map takes its items from an async iterable: read it with async for"

# A function's map made by the driver submits nothing until it is read, or the
# driver submits or waits on anything else: reduce_by_key can then run the
# function inside its scatter calls instead (see Results.claim).
_unstarted: set[Results[Any]] = set()
_unstarted_lock = threading.Lock()


def start_pending() -> None:
    """Start the maps nothing has read yet: the driver moved on."""
    while _unstarted:
        with _unstarted_lock:
            if not _unstarted:
                return
            results = _unstarted.pop()
        results._start()


def _future_from_key(key: bytes) -> Future[Any]:
    from ._runtime import Runtime

    rt = Runtime.get()
    f: Future[Any] = Future(rt, key)
    f._escaped = True
    rt.submit_watch(key, f)
    return f


# Set by the executor: tells the peer a task is blocked waiting on futures.
# A wait passes the same keys when it starts (True) and when it ends.
_executor_hook: Callable[[Any, bool, list[bytes]], None] | None = None


class waiting:
    """Mark a wait inside a call (a lock, a barrier, a watch) as blocked, as a
    future's ``result()`` does: the peer may lend the call's CPU meanwhile, and
    the executor starts the next call of its batch. Naming the `keys` of the
    calls waited on also offers the worker to host another call meanwhile."""

    def __init__(self, keys: list[bytes] | None = None) -> None:
        self.keys = keys or []

    def __enter__(self) -> None:
        self.ctx = _context.current_task()
        if self.ctx is not None and _executor_hook is not None:
            _executor_hook(self.ctx, True, self.keys)

    def __exit__(self, *exc: object) -> None:
        if self.ctx is not None and _executor_hook is not None:
            _executor_hook(self.ctx, False, self.keys)


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


def _resolved_future(
    run: bytes, object_id: bytes, inline: bytes | None, size: int, files: tuple[bytes, ...] = (), durability: str = ""
) -> Future[Any]:
    """A finished future for a stored value (what ch.put returns): `files`
    are those it is stored in, which a holder in another run leases."""
    from ._runtime import Runtime

    rt = Runtime.get()
    f: Future[Any] = Future(rt, run + object_id)
    f._escaped = True  # nothing to release: the value lives as long as its run
    f._put = (run, object_id, inline, size, files, durability)
    f._outcome = ("ok", inline, run, object_id, size, 0)
    return f


# Puts at most this large travel in the handle itself; larger ones are a
# file the handle names. A put exists to be passed to calls, and an inline
# value is copied into every call it is passed to: passing 4,000 handles of
# 64 KiB took 144 us each inline against 4 us as files, so a put and one read
# took 169 us against 62 (a file costs 25 us more at the put).
PUT_INLINE = 4 << 10


def put(value: T, *, durability: Literal["default", "ephemeral", "retained"] | None = None) -> Future[T]:
    """Store `value` once and return a finished future for it. Pass the
    future to any number of calls: each reads the stored value instead of
    receiving a copy. Identical values share one copy.

    ``durability`` is what the value survives. By default it is copied to
    another peer in the background. ``"ephemeral"`` makes no copy: it is lost
    with the peer that holds it, and a CUDA tensor shared with this machine's
    readers keeps no host copy either: only readers that see its GPU can read
    it, while this process lives. ``"retained"`` returns only once copies on
    other peers, in other zones, racks and machines first, are made, and
    raises ``Infeasible`` where there is no other peer. A put has no call to
    run again, so it cannot be ``"replayable"``."""
    from ._runtime import Runtime

    if durability not in (None, "default", "ephemeral", "retained"):
        raise OptionsError(
            f"ch.put: durability={durability!r}: expected 'default', 'ephemeral' or 'retained' "
            "(a put has no call to make it again, so it cannot be 'replayable')"
        )
    rt = Runtime.get()
    ctx = _context.current_task()
    run = ctx.run if ctx is not None else rt.run
    assert run is not None
    # Small values travel in the handle itself (see PUT_INLINE).
    # Inside a call, named like its results (the run's first four bytes):
    # the call may return the handle, and the peer keeps results, not the
    # run's other values, once the run ends.
    prefix = run[:4] if ctx is not None else b""
    holds: list[Any] = []
    limit = min(rt.inline_limit, PUT_INLINE)
    files: list[bytes] = []
    keep_local = durability in ("ephemeral", "retained")  # retained: copied below, not in the background
    lazy: list[bytes] = []
    object_id, inline, size, owned = _values.store(
        value, run, rt.blob_sink, limit, prefix, holds, keep_local, files, ephemeral=durability == "ephemeral", lazy=lazy
    )
    if durability == "retained" and files:
        try:
            rt.client.retain(run, files)
        except (ValueError, RuntimeError, ConnectionError) as e:
            for path in owned:  # no handle refers to them
                try:
                    os.unlink(path)
                except OSError:
                    pass
            raise (Infeasible if isinstance(e, ValueError) else InfrastructureFailure)(f"ch.put: {e}") from None
    kind = "" if durability is None or durability == "default" else durability
    f = _resolved_future(run, object_id, inline, size, tuple(files), kind)
    if holds:
        f._device_holds = holds  # readers on this machine may map them while the future lives
    f._lazy = frozenset(lazy)
    if type(value) in _values._COLLECTIONS:
        caller = sys._getframe(1)
        _values.collection_lint.note(f"{caller.f_code.co_filename}:{caller.f_lineno}", type(value).__name__, size, 3)
    if owned:
        f._escaped, f._owned = False, owned
    return f


@overload
def get(x: Future[T], /, *, timeout: float | None = None) -> T: ...
@overload
def get(x: Iterable[Future[T]], /, *, timeout: float | None = None) -> list[T]: ...
def get(x: Any, /, *, timeout: float | None = None) -> Any:
    """The value of a future, or the values of an iterable of futures in
    order. Waits for them; raises the first error. A ``timeout`` (seconds, for
    all of them) raises ``TimeoutError`` and leaves the calls running."""
    if isinstance(x, Future):
        return x.result(timeout)
    if isinstance(x, (str, bytes)) or not isinstance(x, Iterable):
        raise TypeError(f"ch.get takes a future or an iterable of futures, not {type(x).__name__}")
    futures = list(x)
    for f in futures:
        if not isinstance(f, Future):
            raise TypeError(f"ch.get takes futures, not {type(f).__name__}")
    return _values_of(futures, timeout)


def wait(
    futures: Iterable[Future[T]],
    /,
    *,
    num_returns: int | None = None,
    timeout: float | None = None,
    fetch_local: bool = False,
) -> tuple[list[Future[T]], list[Future[T]]]:
    """Wait until ``num_returns`` of ``futures`` are done (default: all), or
    ``timeout`` seconds pass. Returns ``(done, pending)``, each in the order
    given. Values are not decoded; with ``fetch_local`` the done ones' stored
    bytes are brought to this machine first (as Ray's ``ray.wait`` does by
    default), so reading them later does not wait on the network."""
    fs = list(futures)
    want = len(fs) if num_returns is None else num_returns
    if not 0 <= want <= len(fs):
        raise ValueError(f"num_returns={num_returns}: there are {len(fs)} futures")
    deadline = None if timeout is None else time.monotonic() + timeout
    if fs:
        fs[0]._rt.client.flush()
    # Both lists come from one reading of each future, so one finishing
    # meanwhile lands in exactly one of them.
    flags = [f.done() for f in fs]
    if sum(flags) < want and (deadline is None or time.monotonic() < deadline):  # a poll only looks
        finished = Completions(fs)  # one callback each: no rescan per completion
        try:
            finished.wait(want, deadline)
        finally:
            finished.close()
        flags = [f.done() for f in fs]
    done = [f for f, d in zip(fs, flags) if d]
    if fetch_local and done:
        from . import _objects

        rt = done[0]._rt
        for f in done:
            if f._outcome is not None:
                _objects.prefetch(f._outcome, rt.object_dir, rt.localize)
    return done, [f for f, d in zip(fs, flags) if not d]


def _values_of(futures: list[Future[T]], timeout: float | None) -> list[T]:
    """Every future's value, in order (``ch.get`` of a list). Warns (CH117)
    once the values it holds would outgrow what this process can hold,
    before reading any whose size is known by then."""
    deadline = None if timeout is None else time.monotonic() + timeout
    out = []
    gathered = _Gathered(len(futures))
    for f in futures:
        if f.done():
            gathered.add(f)  # finished already: its stored size is known before it is read
    for f in futures:
        left = None if deadline is None else max(0.0, deadline - time.monotonic())
        gathered.add(f)  # finished by now: counted before it is read
        try:
            out.append(f.result(left))
        except TimeoutError:
            raise
        except BaseException:
            for g in futures:
                g._seen = True  # asked for together: the error raised answers for the rest
            raise
        # A Linux arena array's stored size is only the frame that links it,
        # but it pins its pages while held here: count its bytes.
        nbytes = getattr(out[-1], "nbytes", 0)
        gathered.add(f, nbytes if isinstance(nbytes, int) else 0)
    return out


# CH117: the values one ch.get holds past this many stored bytes (None: a
# quarter of this machine's memory, or half of what this process's cgroup has
# left when the ch.get starts, if that is less).
GATHER_WARN_BYTES: int | None = None
_gather_warned = False
PROC_CGROUP, CGROUP_ROOT = "/proc/self/cgroup", "/sys/fs/cgroup"  # cgroup v2; tests point them elsewhere


def _cgroup_room() -> tuple[int, int] | None:
    """(memory.max, bytes left under it) of the cgroup on this process's
    cgroup v2 path, it or one above it, with the least room now (its limit
    less its current charge). None where none sets a limit (macOS, cgroup
    v1, an unlimited container)."""
    try:
        with open(PROC_CGROUP) as f:
            group = next(line[3:].strip() for line in f if line.startswith("0::"))
    except (OSError, StopIteration):
        return None
    least: tuple[int, int] | None = None
    while True:
        d = os.path.join(CGROUP_ROOT, group.lstrip("/"))
        try:
            with open(os.path.join(d, "memory.max")) as f:
                limit = int(f.read())  # "max": no limit here, a ValueError
            with open(os.path.join(d, "memory.current")) as f:
                room = max(0, limit - int(f.read()))
            if least is None or room < least[1]:
                least = (limit, room)
        except (OSError, ValueError):
            pass
        if group in ("", "/"):
            return least
        group = os.path.dirname(group)


def _gather_limit() -> tuple[int, str] | None:
    """The stored bytes one ch.get may hold before CH117, and what that is."""
    if GATHER_WARN_BYTES is not None:
        return GATHER_WARN_BYTES, f"{GATHER_WARN_BYTES >> 20} MiB"
    try:
        limit = (os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 4, "a quarter of this machine's memory")
    except (ValueError, OSError):
        limit = None
    room = _cgroup_room()
    if room is not None and (limit is None or room[1] // 2 < limit[0]):
        why = f"half of the {room[1] >> 20} MiB left under this process's {room[0] >> 20} MiB cgroup limit"
        limit = (room[1] // 2, why)
    return limit


class _Gathered:
    """The stored bytes of the values one ch.get holds: each stored value
    once, however many of its futures are listed, ch.put values too. Warns
    once (CH117) when they pass `_gather_limit` (a driver that gathers
    everything holds everything: Ray's head node runs out of memory on the
    same program, with no message)."""

    def __init__(self, total: int) -> None:
        self.total, self.held = total, 0
        self.sizes: dict[tuple[bytes, bytes], int] = {}
        self.limit = None if _gather_warned else _gather_limit()

    def add(self, f: Future[Any], nbytes: int = 0) -> None:
        """Count `f`'s value, if it has finished, at least `nbytes` large."""
        o = f._outcome
        if self.limit is None or o is None or o[0] != "ok":
            return
        key, size = (o[2], o[3]), max(o[4], nbytes)
        before = self.sizes.get(key, 0)
        if size <= before:
            return
        self.sizes[key] = size
        self.held += size - before
        self._note()

    def _note(self) -> None:
        global _gather_warned
        assert self.limit is not None
        if _gather_warned or self.held <= self.limit[0]:
            return
        _gather_warned = True
        warnings.warn(
            f"CH117: one ch.get of {self.total} results holds {self.held >> 20} MiB of them stored, past "
            f"{self.limit[1]}, all at once. Consume results as they finish (ch.as_completed, or fn.map(items)) "
            "or reduce them inside calls.",
            stacklevel=4,
        )


class as_completed(Generic[T]):  # noqa: N801 -- mirrors concurrent.futures.as_completed
    """Iterate futures in completion order (sync or async)."""

    _left = 0  # futures not yet taken (none until __init__ has subscribed to them)

    def __init__(self, futures: Iterable[Future[T]], *, timeout: float | None = None) -> None:
        fs = list(futures)
        self._rt: Any = fs[0]._rt if fs else None
        self._deadline = None if timeout is None else time.monotonic() + timeout
        self._completions = Completions(fs)
        self._left = len(fs)

    def __iter__(self) -> Iterator[Future[T]]:
        return self

    def __next__(self) -> Future[T]:
        if not self._left:
            raise StopIteration
        self._rt.client.flush()
        return self._took(self._completions.take(self._deadline))

    def __aiter__(self) -> AsyncIterator[Future[T]]:
        return self

    async def __anext__(self) -> Future[T]:
        if not self._left:
            raise StopAsyncIteration
        self._rt.client.flush()
        return self._took(await self._completions.take_async(self._deadline))

    def _took(self, f: Future[T] | None) -> Future[T]:
        if f is None:
            raise TimeoutError(f"{self._left} futures did not complete in time")
        self._left -= 1
        return f

    def __del__(self) -> None:
        # Left before the end (a break, a timeout): the rest drop their callbacks.
        if self._left and not sys.is_finalizing():
            self._completions.close()


class Outcome(NamedTuple):
    index: int
    value: Any
    error: BaseException | None


class _Slot:
    """Calls in flight for a Results: one item, or a chunk of ``k`` items."""

    __slots__ = ("first", "fut", "k", "chunk", "args", "size", "measured", "timed")

    def __init__(self, first: int, fut: Future[Any], k: int, chunk: bool, args: int, estimate: int) -> None:
        self.first, self.fut, self.k, self.chunk = first, fut, k, chunk
        # Bytes charged to the window: its arguments, and its result's
        # estimated size until the result is measured.
        self.args, self.size, self.measured = args, args + estimate, False
        self.timed = False  # its chunk's time per item was learned (see _behind_head)


class Results(Generic[T]):
    """A single-pass stream of results from ``fn.map``.

    At most ``window`` items are in flight; more are submitted as results are
    consumed, so a slow consumer bounds the work and memory upstream. A
    function's map made by the driver submits its first items when it is first
    read, or when the driver submits or waits on anything else, or at exit
    (see ``claim``). An item's
    error is raised at its position; if caught, iteration continues. Leaving a
    ``with`` block or calling ``close()`` cancels outstanding items and closes
    the source.

    A map over an async iterable is read with ``async for``, which takes
    items from it only as the window allows, and sends them as they arrive.
    An error the iterable raises is raised after the items before it.
    Leaving that loop early (a break, an error, cancellation), an ``async
    with`` block, or ``aclose()`` closes the map and the source. ``close()``
    and a ``with`` block cannot wait for the source: its ``finally`` runs
    then only if it was producing an item, else when it is collected.
    """

    def __init__(
        self,
        submit: Callable[[int, Any], Future[T]],
        source: Iterator[Any] | AsyncIterator[Any],
        *,
        ordered: bool,
        window: int,
        window_bytes: int = 256 << 20,
        chunk: Callable[[int, list[Any]], Future[Any]] | None = None,
        parallel: int = 1,
        at_once: int | None = None,
        grow_window: bool = False,
        origin: tuple[Any, Any, bool] | None = None,
    ) -> None:
        # `origin` (function, options, single source) marks a function's map
        # whose first items wait for a read (see start_pending and claim).
        # `chunk(first_index, items)` submits several items as one call whose
        # value is a _Chunk. Chunks start at one item and grow only by the
        # worker time items are measured to take (A1), so slow items are never
        # grouped. A window the caller chose is a hard cap; a default one
        # widens so every one of the `parallel` workers can hold two chunks.
        # Until a result's size is known, the window is `at_once` (the calls
        # that can run at once; default `parallel`, see _room).
        self._submit = submit
        self._chunk = chunk
        self._parallel = max(1, parallel)
        self._at_once = max(1, at_once or parallel)
        self._grow_window = grow_window
        self._item_s: float | None = None
        # An async source is pulled by async reads only (see _apump).
        is_async = not hasattr(source, "__next__") and hasattr(source, "__anext__")
        self._source: Iterator[Any] | None = None if is_async else source  # type: ignore[assignment]
        self._asource: AsyncIterator[Any] | None = source if is_async else None  # type: ignore[assignment]
        self._pull: asyncio.Task[None] | None = None  # taking items from the async source
        self._pulled: list[Any] = []  # items it took, not sent yet
        self._pull_error: Exception | None = None  # what the async source raised
        self._pull_waiters: list[asyncio.Future[None]] = []  # reads woken when its items can go out
        self._ordered = ordered
        self._window = max(1, window)
        self._window_bytes = max(1, window_bytes)
        self._cap = self._k = self._refill_at = 0  # see _limits
        self._slots: dict[int, _Slot] = {}  # by first index, in submission order
        self._items = 0  # items in flight
        self._bytes = 0  # what the window holds: the slots' sizes, and _held
        self._held = 0  # result bytes of the items in _buf
        self._out_total = 0
        self._out_n = 0
        self._next = 0
        # Items of slots already finished, waiting to be handed out:
        # (index, value, error, slot, position in the slot).
        self._buf: deque[tuple[int, Any, BaseException | None, _Slot, int]] = deque()
        self._exhausted = False
        self._closed = False
        self._registry: set[Any] | None = None
        self._origin = origin
        self._started = False
        self._limits()
        if origin is None:
            self._start()
        else:
            with _unstarted_lock:
                _unstarted.add(self)

    def _start(self) -> None:
        with _unstarted_lock:
            if self._started:
                return
            self._started = True
            _unstarted.discard(self)
        self._fill()
        if self._slots:
            # Until read to the end or closed, the driver finishes it at exit.
            rt = next(iter(self._slots.values())).fut._rt
            if not rt.in_executor:
                self._registry = rt._open_maps
                self._registry.add(self)

    def claim(self) -> tuple[tuple[Any, Any, bool], Iterator[Any]] | None:
        """The function, options and items of a map that has submitted nothing
        yet, taken from this stream (which then yields nothing), for a caller
        that runs the function itself (reduce_by_key); None otherwise."""
        with _unstarted_lock:
            if self._started or self._origin is None or self._source is None:
                return None
            self._started = self._closed = True
            _unstarted.discard(self)
        return self._origin, self._source

    def _end(self) -> None:
        if self._registry is not None:
            self._registry.discard(self)
            self._registry = None

    # ---- submission ----------------------------------------------------------

    def _chunk_size(self) -> int:
        if self._chunk is None or self._item_s is None:
            return 1
        return max(1, min(MAX_CHUNK, int(CHUNK_TARGET_S / max(self._item_s, 1e-7))))

    def _room(self, refill: bool) -> int:
        """How many items to take from the source next (0: none now). The
        window bounds the items taken and not handed out yet (in flight,
        finished and waiting to be read, or taken from an async source and
        not sent yet) and their bytes (A3). A `refill` waits until half the
        window has drained, so the peer receives large groups instead of a
        trickle of single items."""
        if self._exhausted:
            return 0
        held = self._items + len(self._buf) + len(self._pulled)
        if refill:
            if self._slots and held > self._refill_at and self._bytes < self._window_bytes // 2:
                return 0
            self._measure()
            self._limits()
        window = self._cap if self._out_n else min(self._cap, self._at_once)  # no size known yet: one per running call
        if held >= window or self._bytes >= self._window_bytes:
            return 0
        return max(1, min(self._k, window - held))

    def _limits(self) -> None:
        """The window in items, the most items one call takes, and the items
        held at which a refill starts, by what has been measured so far."""
        window = self._window
        k = min(self._chunk_size(), window)
        if self._out_total:  # and its results, by their mean size, at most half the bytes
            k = min(k, max(1, self._window_bytes * self._out_n // (2 * self._out_total)))
        if self._grow_window:
            window = max(window, 2 * self._parallel * k)
        # A chunk is at most half the window: the next runs while it is read.
        self._cap, self._k, self._refill_at = window, min(k, max(1, window // 2)), window // 2

    def _fill(self) -> None:
        if self._source is None:
            return  # an async source: async reads fill (see _apump)
        want = self._room(True)
        while want:
            items = list(itertools.islice(self._source, want))
            self._exhausted = len(items) < want
            if items:
                self._send(items)
            want = self._room(False)

    def _apump(self) -> None:
        """``_fill`` for an async source. A task of its own takes items from
        it while the window has room (_take), so a read never waits on the
        source while results are ready, and a read cancelled while it waits
        takes nothing. What it took goes out a batch at a time, and part of
        a batch only once nothing runs or the task has stopped, so items that
        come a loop step apart still share calls."""
        if self._pulled:
            self._limits()  # batches grow while the task takes items
            while self._pulled and (len(self._pulled) >= self._k or not self._items or self._pull is None):
                items = self._pulled[: self._k]
                del self._pulled[: self._k]  # in place: _take appends to this list as its await ends
                self._send(items)
        if self._pull is None and self._room(True):
            self._pull = asyncio.ensure_future(self._take())

    async def _take(self) -> None:
        """Take items of the async source into _pulled while the window has
        room, waking the reads that wait whenever what it took can go out."""
        assert self._asource is not None
        try:
            while self._room(False):
                self._pulled.append(await anext(self._asource))
                if len(self._pulled) >= self._k or not self._items:
                    self._wake_reads()
        except StopAsyncIteration:
            self._exhausted = True
        except Exception as e:  # raised once the items before it are read
            self._exhausted, self._pull_error = True, e
        finally:
            self._pull = None
            self._wake_reads()

    def _wake_reads(self) -> None:
        for w in self._pull_waiters:  # on this loop: the task that takes items runs on it
            if not w.done():
                w.set_result(None)

    def _send(self, items: list[Any]) -> None:
        out_avg = self._out_total // self._out_n if self._out_n else 0
        if self._chunk is not None and isinstance(items[0], Future):
            self._chunk = None  # chained stages pass futures: one call each
        if self._chunk is None:
            for item in items:
                f = self._submit(self._next, item)
                self._add(_Slot(self._next, f, 1, False, getattr(f, "_arg_bytes", 0), out_avg))
        else:
            f = self._chunk(self._next, items)
            self._add(_Slot(self._next, f, len(items), True, getattr(f, "_arg_bytes", 0), out_avg * len(items)))

    def _measure(self) -> None:
        """Charge each slot whose call has finished its result's stored
        bytes, in place of the estimate it was submitted with: results that
        wait behind a slow first item count as what they are."""
        for slot in self._slots.values():
            if not slot.measured and slot.fut.done():
                size = slot.args + self._learn(slot)
                self._bytes += size - slot.size
                slot.size = size

    def _learn(self, slot: _Slot) -> int:
        """The stored bytes of a finished slot's result (none for an error).
        The first time, the mean result size (the estimate for calls still
        running) learns them."""
        out = slot.fut._outcome
        size = out[4] if out is not None and out[0] == "ok" else 0
        if not slot.measured:
            slot.measured = True
            self._out_total += size
            self._out_n += slot.k
        return size

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

    def _behind_head(self) -> None:
        """While an ordered read waits on its first call: until a result's
        size is known the window is one item per call that can run at once
        (see _room), and until an item's time is known a call takes one item
        (_chunk_size). Once a call behind the first has finished, learn both
        from it and fill the window."""
        if self._out_n:
            return
        self._measure()
        if not self._out_n:
            return
        if self._chunk is not None and self._item_s is None:
            for slot in self._slots.values():
                out = slot.fut._outcome
                if slot.chunk and out is not None and out[0] == "ok":
                    try:  # read before its turn: its value carries the worker's time per item
                        self._time(slot.fut._value_or_raise())
                        slot.timed = True
                    except Exception:  # noqa: BLE001 -- not kept: raised again at its turn (_expand)
                        pass
                    break
        self._fill()

    def _first_done(self) -> _Slot:
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
        return slot

    def _time(self, value: Any) -> None:
        """Learn the worker time per item from a chunk's value (A1)."""
        if isinstance(value, _Chunk) and value:
            per = value.elapsed / len(value)
            self._item_s = per if self._item_s is None else 0.7 * self._item_s + 0.3 * per

    def _expand(self, slot: _Slot) -> None:
        """Move a finished slot's items into the buffer."""
        f = slot.fut
        size = self._learn(slot)
        self._held += size
        self._bytes += size
        try:
            value = f._value_or_raise()
        except Exception as e:  # the call failed as a whole: every item has its error
            for j in range(slot.k):
                self._buf.append((slot.first + j, None, e, slot, j))
            return
        if not slot.chunk:
            self._buf.append((slot.first, value, None, slot, 0))
            return
        if not slot.timed:
            self._time(value)
        for j, v in enumerate(value):
            if isinstance(v, ItemFailed):
                self._buf.append((slot.first + j, None, from_outcome(v.err), slot, j))
            else:
                self._buf.append((slot.first + j, v, None, slot, j))

    def _next_item(self) -> tuple[int, Any, BaseException | None, _Slot, int]:
        self._start()
        if self._buf:
            return self._handout()
        if self._asource is not None and not self._closed:
            raise TypeError(_ASYNC_SOURCE)
        if self._drained():
            self._end()
            self._finished = not self._closed
            raise StopIteration
        if self._ordered:
            slot = next(iter(self._slots.values()))
            slot.fut._wait(None, self._behind_head)
        else:
            slot = self._first_done()
        self._remove(slot)
        self._expand(slot)  # its items are still held: handing them out makes room (see _handout)
        if self._items + len(self._buf) <= self._refill_at:
            self._fill()
        return self._handout()

    def _drained(self) -> bool:
        """True once nothing is left to read: closed, or nothing in flight
        and nothing more to take (a batch it held may have held items back)."""
        if not self._slots and not self._closed:
            self._fill()
        return self._closed or not self._slots and self._pull is None

    def _handout(self) -> tuple[int, Any, BaseException | None, _Slot, int]:
        item = self._buf.popleft()
        if not self._buf:  # its call's last item: nothing of its result is held here now
            self._bytes -= self._held
            self._held = 0
            self._fill()
        elif len(self._buf) + self._items == self._refill_at:
            self._fill()  # half a window handed out of one batch: more run meanwhile
        return item

    def __iter__(self) -> Iterator[T]:
        _not_again(self)
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
        if self._asource is not None:  # here, not at the first item: a chained map is read later
            raise TypeError(_ASYNC_SOURCE)
        return self._futures()

    def _futures(self) -> Iterator[Future[T]]:
        self._chunk = None
        self._start()
        while True:
            if self._buf:
                _, _, _, slot, j = self._handout()
                yield self._item_future(slot, j)
                continue
            if self._drained():
                self._end()
                return
            slot = self._pop_ordered()
            for j in range(slot.k):
                yield self._item_future(slot, j)

    def map(self, fn: Any, *, ordered: bool = True) -> Results[Any]:
        """Feed this stream into another remote callable. Each item's result is
        passed by handle, so values move between executors, not through here."""
        return fn.map(self.futures(), ordered=ordered)

    def reduce_by_key(
        self, combine: Any = None, *, partitions: int | None = None, memory: int | str | None = None
    ) -> Any:
        """Group the (key, value) pairs each result yields by key, across all
        results, folding each key's values with `combine` (associative; without
        it, values are collected into lists). The pairs are repartitioned
        between workers, never through here. Returns a ``Partitioned``.
        `memory` is what each gather may take for its partition (see
        ``_data.reduce_by_key``)."""
        from ._data import reduce_by_key

        return reduce_by_key(self, combine, partitions=partitions, memory=memory)

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
        with _unstarted_lock:
            self._started = self._closed = True  # a map never read submits nothing now
            _unstarted.discard(self)
        self._end()
        close = getattr(self._source, "close", None)
        if callable(close):
            close()
        if self._pull is not None:
            self._pull.cancel()  # an async source stops where it waits
        for slot in self._slots.values():
            if not slot.fut.done():
                slot.fut._rt.client.cancel(slot.fut.key, False)
        self._slots.clear()
        self._buf.clear()
        self._pulled.clear()
        self._pull_error = None  # closed: nothing more is raised
        self._bytes = self._held = 0

    async def aclose(self) -> None:
        """``close()``, then wait for an async source to close: its
        ``finally`` blocks have run when this returns."""
        pull = self._pull
        self.close()
        if pull is not None:
            await asyncio.wait([pull])
        aclose = getattr(self._asource, "aclose", None)
        if aclose is not None:
            await aclose()

    def __enter__(self) -> Results[T]:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    async def __aenter__(self) -> Results[T]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def __aiter__(self) -> AsyncIterator[T]:
        _not_again(self)
        return self if self._asource is None else self._reading()

    async def _reading(self) -> AsyncIterator[T]:
        """``async for`` over a map of an async source: the loop owns the
        map, so leaving it early closes the map and its source."""
        try:
            while True:
                try:
                    value = await self.__anext__()
                except StopAsyncIteration:
                    return
                yield value
        finally:
            await self.aclose()

    async def __anext__(self) -> T:
        self._start()
        # A slot is taken only once its call has finished, with no await in
        # between: a read cancelled while it waits leaves it to the next.
        while True:
            if self._asource is not None and not self._closed:
                self._apump()
            if self._buf:
                break
            if self._drained():
                self._end()
                if self._pull_error is not None:  # the async source's, after the items before it
                    error, self._pull_error = self._pull_error, None
                    raise error
                raise StopAsyncIteration
            if self._ordered:
                head = [next(iter(self._slots.values()))] if self._slots else []
                ready = [s for s in head if s.fut.done()]
                # Until a result's size is known, calls behind the head are watched too (see _behind_head).
                watch = head if self._out_n else list(self._slots.values())
            else:
                watch = list(self._slots.values())
                ready = [s for s in watch if s.fut.done()]
            if not ready:
                if self._ordered and not self._out_n:
                    self._behind_head()
                    if self._out_n:
                        continue  # one behind the head had finished: the window grew
                if watch:
                    watch[0].fut._rt.client.flush()
                # Until a call finishes, or the async source gives items to send.
                await any_done([s.fut for s in watch], self._pull_waiters if self._pull is not None else None)
                continue
            slot = min(ready, key=lambda s: s.first)
            self._remove(slot)
            self._expand(slot)
            if self._items + len(self._buf) <= self._refill_at:
                self._fill()
        _, value, err, _, _ = self._handout()
        if err is not None:
            raise err
        return value


def _not_again(it: Any) -> None:
    """A map's results and a generator call's items are delivered once: a
    second pass after reading one to the end would silently see nothing (as
    Ray's ObjectRefGenerator does), so say so instead."""
    if getattr(it, "_finished", False):
        what = "stream" if isinstance(it, Stream) else "map"
        raise RuntimeError(
            f"this {what} was already read to the end, and its items are delivered once: "
            "keep them with items = list(...) to go over them again"
        )


class Stream(Results[T]):
    """The items of a generator call, in order, as the generator yields them.

    Iterate it (``for x in fn.remote(...)``, or ``async for``). The generator
    runs at most a window of items ahead of you (``max_in_flight``, default
    64, and ``max_in_flight_bytes`` of them, default 256 MiB). If its worker
    dies, the generator runs again and the items you already have are
    skipped, so a generator should yield the same items each time (the
    default ``deterministic=True``). An error raised in the
    generator is raised here after the items before it. ``close()`` stops it.
    """

    def __init__(self, fut: Future[int], window: int, window_bytes: int) -> None:
        self._fut = fut
        self.key = fut.key
        self._rt = fut._rt
        self._items: dict[int, tuple[Any, ...]] = {}
        self._next = 0
        self._window = window
        self._window_bytes = window_bytes
        self._credited = 0
        self._taken = 0  # stored bytes of the items taken since the last credit
        self._waiters: list[asyncio.Future[None]] = []  # async reads waiting for an item
        self._rt._streams[self.key] = self
        if not self._rt.in_executor:
            self._rt._stream_keys.add(self.key)

    def __repr__(self) -> str:
        return f"<Stream {self.key[16:].hex()[:12]} at item {self._next}>"

    def _add(self, index: int, outcome: tuple[Any, ...]) -> None:
        """An item arrived (dispatcher thread; a replayed attempt repeats indices)."""
        if index >= self._next:
            self._items.setdefault(index, outcome)
            if self._waiters:
                for w in list(self._waiters):
                    wake(w)

    def _claim(self) -> tuple[Any, ...] | None:
        """Take the next item if it has arrived (None: not yet). At the end,
        raises StopIteration, or the generator's error."""
        ended = self._fut.done()  # read first: every item arrives before the end
        outcome = self._items.pop(self._next, None)
        if outcome is None:
            if not ended:
                return None
            count = self._fut._value_or_raise()  # the generator's error, if it raised
            if self._next >= count:
                self._finished = True
                raise StopIteration
            raise StreamInterrupted(f"the stream ended after {self._next} of {count} items")
        self._next += 1
        return outcome

    def _deliver(self, outcome: tuple[Any, ...]) -> T:
        # The generator pauses a window ahead of the last credit, in items or
        # in stored bytes: credit after half of either is taken.
        self._taken += outcome[4]
        if self._next - self._credited >= max(1, self._window // 2) or self._taken >= self._window_bytes // 2:
            self._credited, self._taken = self._next, 0
            self._rt.client.stream_credit(self.key, self._next)
        return self._rt.read_value(outcome)

    def __iter__(self) -> Stream[T]:
        _not_again(self)
        return self

    def __next__(self) -> T:
        rt = self._rt
        rt.client.flush()
        with rt.cond:
            while (outcome := self._claim()) is None:
                rt.cond.wait(1.0)
        return self._deliver(outcome)

    def __aiter__(self) -> Stream[T]:
        _not_again(self)
        return self

    async def __anext__(self) -> T:
        self._rt.client.flush()
        while True:
            try:
                outcome = self._claim()
            except StopIteration:
                raise StopAsyncIteration from None
            if outcome is not None:
                return self._deliver(outcome)
            # Only the claim above takes an item, with no await before it: a
            # read cancelled while it waits here leaves the item to the next.
            w = asyncio.get_running_loop().create_future()
            self._waiters.append(w)
            end = self._fut._on_done(lambda _: wake(w))
            try:
                if self._next not in self._items:  # else it arrived before `w` was listed
                    await w
            finally:
                self._waiters.remove(w)
                if end is not None:
                    self._fut._off_done(end)

    def close(self) -> None:
        """Stop the generator; items not taken yet are dropped."""
        self._fut.cancel()

    async def aclose(self) -> None:
        self.close()

    def __enter__(self) -> Stream[T]:
        return self

    def __exit__(self, *exc: object) -> None:
        if not self._fut.done():
            self.close()

    def outcomes(self) -> Iterator[Outcome]:
        """(index, value, None) for every item; the generator's own error, if
        it raised one, is raised after its last item."""
        for i, value in enumerate(self):
            yield Outcome(i, value, None)

    def futures(self) -> Iterator[Future[T]]:
        raise TypeError("a generator call's items are values, not calls: iterate it, or pass it to fn.map(...)")

    def map(self, fn: Any, *, ordered: bool = True) -> Results[Any]:
        return fn.map(self, ordered=ordered)
