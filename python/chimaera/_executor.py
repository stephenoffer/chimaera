"""Executor process: ``python -m chimaera._executor --socket PATH --id N``.

Started by a peer with the run's interpreter, working directory and import
path. It never runs the user's main script: callables arrive as code blobs.
One reader thread receives instructions; calls run on the main thread (or on
a small pool for actor methods declared with ``concurrency > 1``).
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import concurrent.futures
import inspect
import io
import logging
import os
import pickle
import queue
import sys
import sysconfig
import threading
import time
import traceback
from typing import Any

import cloudpickle

from . import _batch, _context, _future, _objects, _project, _serde
from ._native import ExecConn, blake3_digest, child_id
from ._serde import Dep

CODE_CACHE = 256
# Growth in a worker's anonymous memory after which freed memory glibc keeps
# in its arenas is handed back (malloc_trim) before the call is reported, so
# the peer's leak check (R4) sees only memory that is really held.
TRIM_GROWTH = 256 << 20


def _rss_anon() -> int | None:
    """This process's anonymous resident memory, bytes (Linux)."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("RssAnon:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


class _Trimmer:
    """Gives glibc's freed-but-kept memory back once the worker has grown
    TRIM_GROWTH past its baseline. Checked after every call of 10 ms or more
    (growing by that much takes longer), else at most once a second, so tiny
    calls do not pay for reading /proc."""

    def __init__(self) -> None:
        self.trim = None
        if sys.platform.startswith("linux"):
            try:
                import ctypes

                self.trim = ctypes.CDLL("libc.so.6").malloc_trim
            except (OSError, AttributeError):  # musl and other libcs have none
                pass
        self.base = _rss_anon() if self.trim else None
        self.checked = 0.0

    def __call__(self, call_s: float) -> None:
        if self.trim is None or (call_s < 0.01 and time.monotonic() - self.checked < 1.0):
            return
        self.checked = time.monotonic()
        anon = _rss_anon()
        if anon is None or self.base is None or anon < self.base:
            self.base = anon
            return
        if anon - self.base >= TRIM_GROWTH:
            self.trim(0)
            self.base = _rss_anon() or anon  # what is still held is the new baseline

CALL_FUNCTION, CALL_ACTOR_CTOR, CALL_ACTOR_METHOD, CALL_MAP_ITEM = 0, 1, 2, 3


class _TaskOutput(io.TextIOBase):
    """Forwards a running task's prints to its driver; other output goes to the log."""

    def __init__(self, conn: ExecConn, stream: str, fallback: Any) -> None:
        self._conn, self._stream, self._fallback = conn, stream, fallback
        self._buf = threading.local()

    def writable(self) -> bool:
        return True

    def write(self, s: str) -> int:
        ctx = _context.current_task()
        if ctx is None:
            return self._fallback.write(s)
        pending = getattr(self._buf, "text", "") + s
        if "\n" in pending:
            head, _, tail = pending.rpartition("\n")
            self._conn.send_log(ctx.run + ctx.task, self._stream, head)
            pending = tail
        self._buf.text = pending
        return len(s)

    def flush(self) -> None:
        ctx = _context.current_task()
        pending = getattr(self._buf, "text", "")
        if ctx is not None and pending:
            self._conn.send_log(ctx.run + ctx.task, self._stream, pending)
            self._buf.text = ""
        self._fallback.flush()


class Executor:
    def __init__(self, conn: ExecConn) -> None:
        self.conn = conn
        self.trimmer = _Trimmer()
        w = conn.welcome
        self.object_dir: str = w["object_dir"]
        _objects.enable(self.object_dir)
        self.code_dir: str = w["code_dir"]
        self.inline_limit: int = w["inline_limit"]
        # Callables by digest, least recently used first. Bounded: functions
        # defined in a loop each have their own digest.
        self.code: collections.OrderedDict[bytes, Any] = collections.OrderedDict()
        self.instance: Any = None
        self.actor_loop: asyncio.AbstractEventLoop | None = None
        self.loop_lock = threading.Lock()
        # Generator calls: how far each consumer has read (from ExecCredit).
        self.credit: dict[bytes, int] = {}
        self.credit_cond = threading.Condition()
        self.lanes: dict[str, concurrent.futures.ThreadPoolExecutor] = {}
        self.batchers: dict[str, _batch.Batcher | None] = {}  # @ch.method(batch=...) methods
        # The actor class's lane limits, computed once: scanning the class costs
        # more than the rest of a small call (see lane).
        self.lane_limits: dict[type, dict[str, int]] = {}
        # While a batch runs: starts its next call when the current one waits.
        self.on_block: Any = None
        self.running: dict[bytes, _context.TaskContext] = {}
        self.inbox: queue.Queue[tuple[Any, ...] | None] = queue.Queue()

    # ---- inputs -----------------------------------------------------------

    def load(self, digest: bytes) -> Any:
        fn = self.code.get(digest)
        if fn is None:
            with open(os.path.join(self.code_dir, digest.hex()), "rb") as f:
                fn = _serde.load_code(f.read())
            self.code[digest] = fn
            if len(self.code) > CODE_CACHE:
                self.code.popitem(last=False)
        else:
            self.code.move_to_end(digest)
        return fn

    def value(self, outcome: tuple[Any, ...]) -> Any:
        if outcome[0] != "ok":
            raise RuntimeError(f"prerequisite has no value: {outcome[:4]}")
        _, inline, run, obj, _size, _oob = outcome
        if inline is not None:
            return _serde.loads(inline, _serde.run_dir(self.object_dir, run))
        from ._runtime import read_object

        return read_object(self.object_dir, run, obj)

    def call_args(self, args_ref: tuple[Any, ...], deps: list[tuple[Any, ...]]) -> tuple[tuple[Any, ...], dict[str, Any]]:
        frame_args, frame_kwargs = self.value(args_ref)
        resolved: dict[int, Any] = {}

        def one(x: Any) -> Any:
            if isinstance(x, _future.PartDep):
                if x.dep not in resolved:
                    resolved[x.dep] = self.value(deps[x.dep])
                return _future.pick(resolved[x.dep], x.index, x.count)
            if isinstance(x, Dep):
                if x.index not in resolved:
                    resolved[x.index] = self.value(deps[x.index])
                return resolved[x.index]
            return x

        placeholder = (Dep, _future.PartDep)

        def sub(x: Any) -> Any:
            # One level inside list, tuple and dict values, as build_call puts them.
            kind = type(x)
            if deps and (kind is list or kind is tuple) and any(isinstance(e, placeholder) for e in x):
                return kind(one(e) for e in x)
            if deps and kind is dict and any(isinstance(v, placeholder) for v in x.values()):
                return {k: one(v) for k, v in x.items()}
            return one(x)

        return tuple(sub(a) for a in frame_args), {k: sub(v) for k, v in frame_kwargs.items()}

    # ---- outputs ----------------------------------------------------------

    def ok(self, key: bytes, attempt: int, value: Any) -> tuple[Any, ...]:
        run = key[:16]
        return self.ok_object(run, child_id(run, [key[16:], attempt.to_bytes(4, "little")]), value)

    def ok_object(self, run: bytes, obj: bytes, value: Any) -> tuple[Any, ...]:
        """Store `value` as object `obj` of `run` (inline when small). Arena
        arrays in it are linked, not copied (see _objects); their blobs carry
        the run's prefix, so they live as long as the result itself."""
        rdir = _serde.run_dir(self.object_dir, run)

        def claim(view: memoryview) -> tuple[bytes, int] | None:
            return _objects.claim(view, rdir, run[:4]) if view.nbytes >= _objects.ARENA_MIN else None

        parts, size, oob = _serde.dumps_parts(value, external=claim)
        if size <= self.inline_limit:
            data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)
            return ("ok", run, obj, size, data, oob)
        _serde.write_file(_serde.object_path(self.object_dir, run, obj), parts)
        return ("ok", run, obj, size, None, oob)

    @staticmethod
    def err(exc: BaseException) -> tuple[Any, ...]:
        try:
            blob = pickle.dumps(exc)
        except Exception:
            try:
                blob = cloudpickle.dumps(exc)
            except Exception:
                blob = b""
        tb = "".join(traceback.format_exception(type(exc), exc, _user_frames(exc.__traceback__)))
        name = f"{type(exc).__module__}.{type(exc).__qualname__}"
        return ("err", "user", name, str(exc), blob, tb, "")

    # ---- execution --------------------------------------------------------

    def run_one(self, msg: dict[str, Any], item: tuple[Any, ...]) -> None:
        key, attempt, args_ref, deps, item_index = item
        ctx = _context.TaskContext(
            run=key[:16],
            task=key[16:],
            attempt=attempt,
            item_index=item_index,
            policy=msg["policy"],
            deadline_unix_ms=msg["deadline_unix_ms"],
            actor=msg["actor"],
        )
        self.running[key] = ctx
        token = _context.set_task(ctx)
        entry = time.time_ns()
        retryable, fingerprint, state = False, b"", b""
        try:
            result = self.invoke(msg, args_ref, deps)
            if isinstance(result, _future.Future):
                # Returning a future forwards it: this call resolves to that
                # call's outcome, and this worker is free meanwhile.
                result._escaped = True  # the shard now pins it for the run
                result._rt.client.flush()
                outcome = ("forward", result.key)
            else:
                outcome = self.ok(key, attempt, result)
                if msg.get("durable"):
                    state = _durable_state(self.instance)
        except BaseException as exc:  # user code may raise anything, including SystemExit
            outcome = self.err(exc)
            retryable = _matches(exc, msg["retry_exception_types"])
            if retryable:
                frames = "|".join(f"{f.filename}:{f.lineno}" for f in traceback.extract_tb(exc.__traceback__))
                fingerprint = blake3_digest(f"{outcome[2]}|{outcome[3]}|{frames}".encode())
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            _context.reset(token)
            self.running.pop(key, None)
        self.trimmer((time.time_ns() - entry) / 1e9)
        self.conn.send_done(key, attempt, outcome, retryable, fingerprint, entry, time.time_ns(), state)

    def invoke(self, msg: dict[str, Any], args_ref: tuple[Any, ...], deps: list[tuple[Any, ...]]) -> Any:
        kind = msg["kind"]
        args, kwargs = self.call_args(args_ref, deps)
        if kind == CALL_ACTOR_CTOR:
            cls = self.load(msg["fn_digest"])
            self.instance = cls(*args, **kwargs)
            return None
        if kind == CALL_ACTOR_METHOD:
            if self.instance is None:
                raise RuntimeError("actor method called before its constructor ran")
            result = getattr(self.instance, msg["method"])(*args, **kwargs)
            # A returned future is forwarded by run_one, never awaited here:
            # awaiting would hold this worker until the other call finishes.
            if inspect.isawaitable(result) and not isinstance(result, _future.Future):
                return asyncio.run_coroutine_threadsafe(_awaitable(result), self._actor_loop()).result()
            return result
        fn = self.load(msg["fn_digest"])
        if getattr(fn, "__chimaera_workflow__", False):
            ctx = _context.current_task()
            if ctx is not None:
                ctx.workflow = True
        result = fn(*args, **kwargs)
        if inspect.isgenerator(result) or inspect.isasyncgen(result):
            return self.stream(msg, result)
        if inspect.isawaitable(result) and not isinstance(result, _future.Future):
            return asyncio.run(_awaitable(result))
        return result

    def stream(self, msg: dict[str, Any], gen: Any) -> int:
        """Send a generator's items as it yields them, at most a window ahead
        of the consumer. The call's result is the number of items."""
        from ._runtime import STREAM_WINDOW

        ctx = _context.current_task()
        assert ctx is not None
        key = ctx.run + ctx.task
        window = int(msg["policy"].get("max_in_flight") or STREAM_WINDOW)
        sent = 0

        def put(item: Any) -> bool:
            nonlocal sent
            with self.credit_cond:
                while sent >= self.credit.get(key, 0) + window:
                    if ctx._cancelled.is_set():
                        return False
                    self.credit_cond.wait(0.5)
            index = sent
            run = key[:16]
            obj = child_id(run, [key[16:], ctx.attempt.to_bytes(4, "little"), b"item", index.to_bytes(8, "little")])
            self.conn.send_yield(key, ctx.attempt, index, self.ok_object(run, obj, item))
            sent += 1
            return not ctx._cancelled.is_set()

        try:
            if inspect.isasyncgen(gen):

                async def drain() -> None:
                    async for item in gen:
                        if not put(item):
                            break

                asyncio.run(drain())
            else:
                for item in gen:
                    if not put(item):
                        gen.close()
                        break
        finally:
            with self.credit_cond:
                self.credit.pop(key, None)
        return sent

    def _actor_loop(self) -> asyncio.AbstractEventLoop:
        # Calls in several lanes arrive at once: create the one loop under a lock,
        # or two threads could each make one and split the actor's coroutines.
        with self.loop_lock:
            if self.actor_loop is None:
                loop = asyncio.new_event_loop()
                threading.Thread(target=loop.run_forever, name="actor-loop", daemon=True).start()
                self.actor_loop = loop
            return self.actor_loop

    def handle(self, msg: dict[str, Any]) -> None:
        # A remote driver's project modules, before any of its code loads.
        _project.ensure(self.object_dir, msg["items"][0][0][:16], os.path.join(os.path.dirname(self.code_dir), "projects"))
        batcher = self.batcher(msg["method"]) if msg["kind"] == CALL_ACTOR_METHOD else None
        if batcher is not None:
            for item in msg["items"]:
                batcher.submit(msg, item)
            return
        lane = self.lane(msg) if msg["kind"] == CALL_ACTOR_METHOD else None
        if lane is not None:
            for item in msg["items"]:
                lane.submit(self.run_one, msg, item).add_done_callback(_report_failure)
            return
        self.run_batch(msg)

    def run_batch(self, msg: dict[str, Any]) -> None:
        """Run a batch of calls one after another. When one waits (on a future,
        a queue, a lock, a barrier...), the next starts on another thread: calls
        of a batch may be waiting on each other, so running them strictly in
        turn could deadlock. Returns when every call has finished."""
        pending = collections.deque(msg["items"])
        lock = threading.Lock()
        helpers: list[threading.Thread] = []

        def take() -> Any:
            with lock:
                return pending.popleft() if pending else None

        def work() -> None:
            while (item := take()) is not None:
                self.run_one(msg, item)

        def on_block() -> None:
            with lock:
                if not pending:
                    return
            t = threading.Thread(target=work, name="batch-item", daemon=True)
            helpers.append(t)
            t.start()

        self.on_block = on_block
        try:
            work()
            for t in helpers:  # the list only grows while items still run
                t.join()
        finally:
            self.on_block = None

    def batcher(self, method: str) -> _batch.Batcher | None:
        if method not in self.batchers:
            declared = getattr(getattr(type(self.instance), method, None), "__chimaera_method__", None) or {}
            batch = declared.get("batch")
            self.batchers[method] = _batch.Batcher(self, method, batch) if batch is not None else None
        return self.batchers[method]

    def lane(self, msg: dict[str, Any]) -> concurrent.futures.ThreadPoolExecutor | None:
        """The pool an actor method runs in, or None to run it here, one call at a time.

        The actor's concurrency (what the peer may send at once) is shared by
        its lanes: each concurrency group, each method declared with its own
        ``concurrency``, and the default lane, which gets the rest. Calls in a
        lane start in arrival order; lanes do not wait on each other.
        """
        from ._remote import _lane_limits

        total = int(msg["policy"].get("concurrency") or 1)
        cls = type(self.instance)
        limits = self.lane_limits.get(cls)
        if limits is None:
            limits = self.lane_limits[cls] = _lane_limits(cls)
        if not limits and total == 1:
            return None
        declared = getattr(getattr(cls, msg["method"], None), "__chimaera_method__", None) or {}
        name = declared.get("group") or (f"method:{msg['method']}" if f"method:{msg['method']}" in limits else "")
        size = limits[name] if name else max(1, total - sum(limits.values()))
        pool = self.lanes.get(name)
        if pool is None:
            pool = self.lanes[name] = concurrent.futures.ThreadPoolExecutor(size, thread_name_prefix=f"actor-{name or 'lane'}")
        return pool

    def read_loop(self) -> None:
        while True:
            m = self.conn.recv()
            if m is None or m[0] == "shutdown":
                self.inbox.put(None)
                return
            if m[0] == "cancel":
                ctx = self.running.get(m[1])
                if ctx is not None:
                    ctx._cancelled.set()
            elif m[0] == "credit":
                with self.credit_cond:
                    self.credit[m[1]] = max(self.credit.get(m[1], 0), m[2])
                    self.credit_cond.notify_all()
            elif m[0] in ("execute", "restore"):
                # In order: a restore lands between the calls it separates.
                self.inbox.put(m)

    def serve(self) -> None:
        threading.Thread(target=self.read_loop, name="executor-reader", daemon=True).start()
        while True:
            m = self.inbox.get()
            if m is None:
                return
            if m[0] == "restore":
                self.restore(m[1])
            else:
                self.handle(m[1])

    def restore(self, msg: dict[str, Any]) -> None:
        """Make the actor instance equal to its committed state, without
        running ``__init__``."""
        if not msg["state"]:
            return
        cls = self.load(msg["fn_digest"])
        obj = cls.__new__(cls)
        state = pickle.loads(msg["state"])
        if getattr(type(obj), "__setstate__", None) is not None:
            obj.__setstate__(state)
        elif state is not None:
            obj.__dict__.update(state)
        self.instance = obj


_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))


def _user_frames(tb: Any) -> Any:
    """Skip the executor's own frames at the top of a traceback."""
    while tb is not None and os.path.dirname(os.path.abspath(tb.tb_frame.f_code.co_filename)) == _PACKAGE_DIR:
        tb = tb.tb_next
    return tb


def _report_failure(f: concurrent.futures.Future[Any]) -> None:
    exc = f.exception()
    if exc is not None:  # reporting the result itself failed (e.g. the peer went away)
        sys.__stderr__.write(f"chimaera executor: could not report a result: {exc!r}\n")


def _durable_state(instance: Any) -> bytes:
    """A durable actor's state: what ``__getstate__`` returns (its attributes
    by default), pickled."""
    return cloudpickle.dumps(instance.__getstate__(), protocol=5)


async def _awaitable(x: Any) -> Any:
    return await x


def _matches(exc: BaseException, names: list[str]) -> bool:
    if not names:
        return False
    return any(f"{t.__module__}.{t.__qualname__}" in names for t in type(exc).__mro__)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--socket", required=True)
    p.add_argument("--id", type=int, required=True)
    a = p.parse_args()
    abi = f"cp{sys.version_info.major}{sys.version_info.minor}-{sysconfig.get_platform()}"
    conn = ExecConn(a.socket, a.id, sys.version, abi)
    ex = Executor(conn)
    sys.stdout = _TaskOutput(conn, "stdout", sys.__stdout__)
    sys.stderr = _TaskOutput(conn, "stderr", sys.__stderr__)
    # Log records from tasks reach the driver at the driver's level (Ray's
    # workers drop them unless each task configures logging itself).
    level = os.environ.get("CHIMAERA_LOG_LEVEL")
    if level and not logging.getLogger().handlers:
        logging.basicConfig(level=level, stream=sys.stderr, format="%(levelname)s %(name)s: %(message)s")

    def blocked(ctx: _context.TaskContext, is_blocked: bool, waiting: list[bytes]) -> None:
        conn.send_blocked(ctx.run + ctx.task, is_blocked, waiting)
        if is_blocked and ex.on_block is not None:
            ex.on_block()

    _future._executor_hook = blocked

    def localize(run: bytes, obj: bytes) -> None:
        from ._runtime import Runtime

        Runtime.get().client.localize(run, obj)

    _serde.localize_hook = localize
    ex.serve()


if __name__ == "__main__":
    main()
