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
import signal
import sys
import sysconfig
import threading
import time
import traceback
from typing import Any

import cloudpickle

from . import _batch, _context, _future, _objects, _project, _serde
from ._errors import ActorDied
from ._native import ExecConn, blake3_digest, child_id
from ._serde import Dep

CODE_CACHE = 256
# Growth in a worker's anonymous memory after which freed memory glibc keeps
# in its arenas is handed back (malloc_trim) before the call is reported, so
# the peer's leak check (R4) sees only memory that is really held.
TRIM_GROWTH = 256 << 20



# Tuples up to this long are stored element by element (see _serde.dumps_seq).
SEQ_MAX = 4096
# A stored tuple's elements of at least this size can be objects of their own
# (see _serde.PARTS_MAGIC and splits); smaller ones stay in the tuple's file,
# where one fetch brings them all.
PART_FILE_MIN = 64 * 1024
# One more fetch costs about the time of moving this many bytes (a round trip
# of ~1 ms at ~1 GB/s), for deciding whether parts pay (see splits).
FETCH_COST = 1 << 20
# Parts fetched at once for one call (as the peer fetches inputs).
PART_FETCHES = 16
# How long a count of live peers is used before it is asked again.
PEERS_EVERY = 10.0

def worth_splitting(count: int, size: int, peers: int) -> bool:
    """Whether a result of `count` values and `size` bytes is faster to read as
    parts. Its consumers spread over the live peers, about count/peers on each.
    As one file, each peer fetches it once for all of its consumers; as parts,
    each consumer fetches only its own. Parts cost count/peers - 1 more
    fetches per peer and save the other values' bytes: on a few peers with
    many consumers each one file is faster; at scale (a consumer per peer)
    parts move a fraction of the bytes in the same number of fetches."""
    return peers > 1 and size * (1 - 1 / peers) > (count / peers - 1) * FETCH_COST


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
    """Forwards a running task's prints to its driver; other output goes to the
    log. A print from a thread the call started (which has no task context) is
    the call's when it is the only one running here (`owner`)."""

    def __init__(self, conn: ExecConn, stream: str, fallback: Any, owner: Any) -> None:
        self._conn, self._stream, self._fallback, self._owner = conn, stream, fallback, owner
        self._buf = threading.local()

    def writable(self) -> bool:
        return True

    def write(self, s: str) -> int:
        ctx = _context.current_task() or self._owner()
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
        ctx = _context.current_task() or self._owner()
        pending = getattr(self._buf, "text", "")
        if ctx is not None and pending:
            self._conn.send_log(ctx.run + ctx.task, self._stream, pending)
            self._buf.text = ""
        self._fallback.flush()


class _Stdout:
    """What the executor's processes write to file descriptor 1 (programs a
    call runs, C code): read from a pipe and forwarded line by line to the call
    running here when there is exactly one (`owner`), else to the log. Only
    stdout: stderr stays a plain file so a crashing process's last words are
    never left unread in a pipe."""

    _DRAIN = b"\x00chimaera-drain\n"

    def __init__(self, conn: ExecConn, owner: Any) -> None:
        self.conn, self.owner = conn, owner
        self.log = os.dup(1)
        r, w = os.pipe()
        os.dup2(w, 1)
        os.close(w)
        self.r = r
        self.seen = 0  # bytes read so far
        self.drained = threading.Event()
        threading.Thread(target=self.pump, name="stdout-capture", daemon=True).start()

    def pump(self) -> None:
        partial = b""
        while data := os.read(self.r, 65536):
            self.seen += len(data)
            *lines, partial = (partial + data).split(b"\n")
            for line in lines:
                if line == self._DRAIN[:-1]:
                    self.drained.set()
                    continue
                ctx = self.owner()
                if ctx is not None:
                    self.conn.send_log(ctx.run + ctx.task, "stdout", line.decode(errors="replace"))
                else:
                    os.write(self.log, line + b"\n")

    def drain(self, seen_before: int) -> None:
        """Before a call ends: forward what it wrote that is still in the pipe
        (only if it wrote anything, so quiet calls pay nothing)."""
        if self.seen == seen_before and not _unread(self.r):
            return
        self.drained.clear()
        os.write(1, self._DRAIN)
        self.drained.wait(1.0)


def _unread(fd: int) -> int:
    import fcntl
    import termios

    buf = bytearray(4)
    fcntl.ioctl(fd, termios.FIONREAD, buf)
    return int.from_bytes(buf, sys.byteorder)


# What CUDA libraries raise when they are shown no GPU (torch, CuPy, JAX).
_NO_CUDA = ("no cuda gpus are available", "no cuda-capable device", "cudaerrornodevice", "unable to initialize backend 'cuda'")


def _gpu_hint(exc: BaseException) -> None:
    """CH119: a call that asked for no GPU, on a machine that has them, is
    shown none (the peer sets CUDA_VISIBLE_DEVICES empty only where GPUs
    exist), and CUDA's error never says why: add the reason and the fix."""
    if os.environ.get("CUDA_VISIBLE_DEVICES") != "" or not any(p in str(exc).lower() for p in _NO_CUDA):
        return
    exc.add_note(
        "CH119: this call asked for no GPU, so CUDA shows it none, although this machine has GPUs; "
        "ask for one with @ch.remote(gpus=1) or fn.options(gpus=1)"
    )


def python_mismatch(e: Exception) -> RuntimeError | None:
    """Why a call's function would not load, when the program runs another
    Python than this worker (a remote peer's workers run the peer's own):
    functions defined in the program travel as bytecode for its version."""
    theirs = os.environ.get("CHIMAERA_DRIVER_PYTHON", "")
    ours = f"{sys.version_info.major}.{sys.version_info.minor}"
    if not theirs or theirs == ours:
        return None
    return RuntimeError(
        f"CH115: the call's function did not load ({type(e).__name__}: {e}). The program runs Python {theirs} "
        f"but this worker runs Python {ours}, and functions defined in the program travel as Python {theirs} "
        f"bytecode, which loads only on the same version. Run the program with Python {ours} (for example "
        f"`uv run --python {ours}`), or put the function in a module the workers can import."
    )


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
        # The call whose own code runs on the main thread, if any: the only
        # one a signal can interrupt (see interrupt).
        self.main_call: _context.TaskContext | None = None
        # exit_actor() ran in this worker's actor: later calls fail at once.
        self.exited = False
        self.stdout: _Stdout | None = None  # set by main()
        self.running: dict[bytes, _context.TaskContext] = {}
        self.inbox: queue.SimpleQueue[tuple[Any, ...] | None] = queue.SimpleQueue()
        self.fetcher: concurrent.futures.ThreadPoolExecutor | None = None  # see fetch_parts
        self.peers, self.peers_at = 1, -PEERS_EVERY  # live peers, and when counted (see splits)

    # ---- inputs -----------------------------------------------------------

    def load(self, digest: bytes) -> Any:
        fn = self.code.get(digest)
        if fn is None:
            with open(os.path.join(self.code_dir, digest.hex()), "rb") as f:
                data = f.read()
            try:
                fn = _serde.load_code(data)
            except Exception as e:
                mismatch = python_mismatch(e)
                if mismatch is None:
                    raise
                raise mismatch from e
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

    def part(self, outcome: tuple[Any, ...], index: int, count: int) -> Any:
        """Value `index` of a prerequisite that returned `count` values,
        decoding only that value when it was stored element by element."""
        if outcome[0] != "ok":
            raise RuntimeError(f"prerequisite has no value: {outcome[:4]}")
        _, inline, run, obj, _size, _oob = outcome
        rdir = _serde.run_dir(self.object_dir, run)
        if inline is not None:
            v = _serde.loads_part(inline, index, count, rdir)
        else:
            from ._runtime import read_object

            path = _serde.object_path(self.object_dir, run, obj)
            if not os.path.exists(path):
                read_object(self.object_dir, run, obj)  # fetches it from the peer that holds it
            try:
                v = _serde.loads_part(_serde._map(path), index, count, rdir)
            except FileNotFoundError as e:  # a part stored on its own, by a peer that is gone
                from ._errors import DataLost

                lost = f"value {index} of {obj.hex()} (run {run.hex()}) is no longer stored on any live peer"
                raise DataLost(lost) from e
        if isinstance(v, _future.ItemFailed):
            raise _future.from_outcome(v.err)
        return v

    def fetch_parts(self, wanted: list[_future.PartDep], deps: list[tuple[Any, ...]]) -> None:
        """Bring the parts these placeholders read into this peer's store, several
        at once: a shuffle's gather reads one part of every scatter, which one
        after another costs a round trip each. A part that cannot be fetched
        here fails when it is read (see part)."""
        missing = []
        for x in wanted:
            outcome = deps[x.dep]
            if outcome[0] != "ok" or outcome[1] is not None:
                continue
            run, obj = outcome[2], outcome[3]
            try:
                with open(_serde.object_path(self.object_dir, run, obj), "rb") as f:
                    head = f.read(_serde.table_size(x.count))  # the table alone, not the values
            except FileNotFoundError:
                continue
            part = _serde.part_id(head, x.index, x.count)
            if part is not None and not os.path.exists(_serde.object_path(self.object_dir, run, part)):
                missing.append((run, part))
        if len(missing) < 2 or _serde.localize_hook is None:
            return
        if self.fetcher is None:
            self.fetcher = concurrent.futures.ThreadPoolExecutor(PART_FETCHES, thread_name_prefix="chimaera-parts")
        localize = _serde.localize_hook

        def one(m: tuple[bytes, bytes]) -> None:
            try:
                localize(*m)
            except FileNotFoundError:
                pass

        list(self.fetcher.map(one, set(missing)))

    def call_args(self, args_ref: tuple[Any, ...], deps: list[tuple[Any, ...]]) -> tuple[tuple[Any, ...], dict[str, Any]]:
        frame_args, frame_kwargs = self.value(args_ref)
        resolved: dict[int, Any] = {}

        def one(x: Any) -> Any:
            if isinstance(x, _future.PartDep):
                if x.dep in resolved:  # the whole value is needed elsewhere in this call
                    return _future.pick(resolved[x.dep], x.index, x.count)
                return self.part(deps[x.dep], x.index, x.count)
            if isinstance(x, Dep):
                if x.index not in resolved:
                    resolved[x.index] = self.value(deps[x.index])
                return resolved[x.index]
            return x

        placeholder = (Dep, _future.PartDep)

        def sub(x: Any, f: Any = one) -> Any:
            # One level inside list, tuple and dict values, as build_call puts them.
            kind = type(x)
            if deps and (kind is list or kind is tuple) and any(isinstance(e, placeholder) for e in x):
                return kind(f(e) for e in x)
            if deps and kind is dict and any(isinstance(v, placeholder) for v in x.values()):
                return {k: f(v) for k, v in x.items()}
            return f(x)

        if len(deps) > 1:
            wanted: list[_future.PartDep] = []

            def note(x: Any) -> None:
                if isinstance(x, _future.PartDep):
                    wanted.append(x)

            for a in (*frame_args, *frame_kwargs.values()):
                sub(a, note)
            self.fetch_parts(wanted, deps)
        return tuple(sub(a) for a in frame_args), {k: sub(v) for k, v in frame_kwargs.items()}

    # ---- outputs ----------------------------------------------------------

    def ok(self, key: bytes, attempt: int, value: Any) -> tuple[Any, ...]:
        run = key[:16]
        return self.ok_object(run, child_id(run, [key[16:], attempt.to_bytes(4, "little")]), value)

    def splits(self, count: int, size: int) -> bool:
        """Whether a result of `count` values and `size` bytes is stored as parts
        (see worth_splitting), given the live peers counted at most
        PEERS_EVERY ago."""
        now = time.monotonic()
        if now - self.peers_at >= PEERS_EVERY:
            self.peers_at = now
            from ._runtime import Runtime

            try:
                self.peers = max(1, sum(1 for n in Runtime.get().client.nodes() if n["alive"]))
            except (ConnectionError, RuntimeError):
                pass  # the peer could not say; the last count stands (1: never split)
        return worth_splitting(count, size, self.peers)

    def ok_object(self, run: bytes, obj: bytes, value: Any) -> tuple[Any, ...]:
        """Store `value` as object `obj` of `run` (inline when small). Arena
        arrays in it are linked, not copied (see _objects); their blobs carry
        the run's prefix, so they live as long as the result itself, and the
        outcome names them (with their sizes): the peer places calls by them,
        fetches them before a call reads them, and frees them with the result
        (the frame alone may be a few inline bytes)."""
        rdir = _serde.run_dir(self.object_dir, run)
        blobs: list[tuple[bytes, int]] = []

        def claim(view: memoryview) -> tuple[bytes, int] | None:
            got = _objects.claim(view, rdir, run[:4]) if view.nbytes >= _objects.ARENA_MIN else None
            if got is not None:
                try:
                    blobs.append((got[0], os.stat(os.path.join(rdir, got[0].hex())).st_size))
                except OSError:
                    blobs.append((got[0], view.nbytes))
            return got

        if type(value) is tuple and 2 <= len(value) <= SEQ_MAX:
            # Possibly several values (returns=N): each readable on its own.
            encoded = [_serde.dumps_parts(v, external=claim) for v in value]
            ids = None
            total = sum(n for _, n, _ in encoded)
            if total > self.inline_limit and self.splits(len(encoded), total):
                # Large values in files of their own, written before the file that
                # names them: a consumer of one value fetches only that one.
                ids = [
                    child_id(obj, [i.to_bytes(4, "little")]) if n >= PART_FILE_MIN else None
                    for i, (_, n, _) in enumerate(encoded)
                ]
                for part, (p, _, _) in zip(ids, encoded):
                    if part is not None:
                        _serde.write_file(_serde.object_path(self.object_dir, run, part), p)
            parts, size, oob = _serde.pack_seq(encoded, ids)
            if ids is not None and any(ids):  # never inline: the peer frees and finds parts through this file
                _serde.write_file(_serde.object_path(self.object_dir, run, obj), parts)
                return ("ok", run, obj, size, None, oob, blobs)
        else:
            parts, size, oob = _serde.dumps_parts(value, external=claim)
        if size <= self.inline_limit:
            data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)
            return ("ok", run, obj, size, data, oob, blobs)
        _serde.write_file(_serde.object_path(self.object_dir, run, obj), parts)
        return ("ok", run, obj, size, None, oob, blobs)

    @staticmethod
    def err(exc: BaseException) -> tuple[Any, ...]:
        _gpu_hint(exc)
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
        seen = self.stdout.seen if self.stdout is not None else 0
        retryable, fingerprint, state = False, b"", b""
        on_main = threading.current_thread() is threading.main_thread()
        try:
            if on_main:
                self.main_call = ctx
            try:
                if self.exited:  # exit_actor() ran; the close that ends this process is on its way
                    raise ActorDied("the actor ended itself with ch.context().exit_actor()")
                result = self.invoke(msg, args_ref, deps)
            finally:
                if on_main:
                    self.main_call = None
            if isinstance(result, _future.Future) and result._put is None:
                # Returning a future forwards it: this call resolves to that
                # call's outcome, and this worker is free meanwhile. A ch.put
                # handle is no call to forward to: it is stored as the value.
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
            if self.stdout is not None and len(self.running) == 1:
                self.stdout.drain(seen)
            _context.reset(token)
            self.running.pop(key, None)
        self.trimmer((time.time_ns() - entry) / 1e9)
        self.conn.send_done(key, attempt, outcome, retryable, fingerprint, entry, time.time_ns(), state)
        if ctx.exiting and not self.exited:
            # After this call's outcome went out: closing the actor ends this process.
            from ._runtime import Runtime

            self.exited = True
            Runtime.get().client.close_actor(ctx.actor)

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
            # The driver streams a call only for a generator function (see
            # RemoteFunction.remote); for any other, nobody reads the items.
            if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
                return self.stream(msg, result)
            name = getattr(fn, "__qualname__", repr(fn))
            raise TypeError(
                f"{name} returned a generator, which cannot be sent back as one value: make it a generator "
                "function (use `yield`) to stream its items, or return a list"
            )
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
                    t = threading.Timer(CANCEL_INTERRUPT_S, self.interrupt, (ctx,))
                    t.daemon = True
                    t.start()
            elif m[0] == "credit":
                with self.credit_cond:
                    self.credit[m[1]] = max(self.credit.get(m[1], 0), m[2])
                    self.credit_cond.notify_all()
            elif m[0] in ("execute", "restore"):
                # In order: a restore lands between the calls it separates.
                self.inbox.put(m)

    def interrupt(self, ctx: _context.TaskContext) -> None:
        """A cancelled call that has not stopped by itself: interrupt it with
        SIGUSR2 if its code runs on the main thread (a sleep or a Python loop
        then raises ch.Cancelled). Anything else is killed by the peer a
        little later, unless it is an actor's."""
        if self.main_call is ctx:
            signal.pthread_kill(threading.main_thread().ident, signal.SIGUSR2)

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
# How long a cancelled call has to stop by itself before it is interrupted.
CANCEL_INTERRUPT_S = 2.0


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
    def owner() -> _context.TaskContext | None:
        running = list(ex.running.values())
        return running[0] if len(running) == 1 else None

    ex.stdout = _Stdout(conn, owner)
    log_out = open(ex.stdout.log, "w", buffering=1, closefd=False)  # noqa: SIM115 -- lives as long as the process
    sys.stdout = _TaskOutput(conn, "stdout", log_out, owner)
    sys.stderr = _TaskOutput(conn, "stderr", sys.__stderr__, owner)
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

    previous = signal.getsignal(signal.SIGUSR2)

    def on_interrupt(signum: int, frame: Any) -> None:
        call = ex.main_call
        if call is not None and call._cancelled.is_set():
            from ._errors import Cancelled

            raise Cancelled("cancelled while running")
        if callable(previous):
            previous(signum, frame)

    signal.signal(signal.SIGUSR2, on_interrupt)

    def localize(run: bytes, obj: bytes) -> None:
        from ._runtime import Runtime

        Runtime.get().client.localize(run, obj)

    _serde.localize_hook = localize
    ex.serve()


if __name__ == "__main__":
    main()
