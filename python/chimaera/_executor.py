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
import ctypes
import gc
import inspect
import io
import logging
import os
import pickle
import queue
import select
import signal
import sys
import sysconfig
import threading
import time
import traceback
from typing import Any

import cloudpickle

from . import _batch, _direct, _context, _future, _graph, _leases, _objects, _project, _serde, _statecodec, _workflow
from ._errors import ActorDied, NonRetryable
from . import _native
from ._native import ExecConn, blake3_digest, child_id
from ._serde import Dep

# CPU this thread's runtime helpers spent (parallel reads and writes of large
# values): reported with each call so the peer doesn't learn it as the
# function's cores. None with an older extension, whose send_done takes no
# helper_cpu_ns either.
_helper_cpu_ns = getattr(_native, "helper_cpu_ns", None)

CODE_CACHE = 256
# How long a restore waits for its class's code, which its peer is fetching.
CODE_FETCH_WAIT_S = 60.0
# Threads that start and finish an actor's async calls (their awaiting holds none).
ASYNC_STEP_THREADS = 4
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
        while True:
            select.select([self.r], [], [])
            # Counted while still in the pipe, so a drain that finds neither
            # unread bytes nor new counted ones has nothing in flight.
            n = _unread(self.r)
            self.seen += n
            data = os.read(self.r, n or 65536)
            if not data:
                return
            self.seen += max(0, len(data) - n)
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

    def drain(self, seen_before: int, wait: float = 1.0) -> None:
        """Before a call ends: forward what it wrote that is still in the pipe
        (only if it wrote anything, so quiet calls pay nothing). The pipe is
        read before the count: the pump counts bytes before it takes them,
        so a byte is always in one or the other."""
        if not _unread(self.r) and self.seen == seen_before:
            return
        self.drained.clear()
        os.write(1, self._DRAIN)
        self.drained.wait(wait)


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


def _env_hint(exc: BaseException) -> None:
    """CH122: a call failing on ``os.environ[name]`` for a variable the
    program has (a run ships only the ones it names) is told how to pass it;
    the bare KeyError says only the name."""
    if not isinstance(exc, KeyError) or len(exc.args) != 1 or not isinstance(exc.args[0], str):
        return
    name, tb = exc.args[0], exc.__traceback__
    while tb is not None and tb.tb_next is not None:
        tb = tb.tb_next
    if name in os.environ or tb is None or tb.tb_frame.f_locals.get("self") is not os.environ:
        return  # not raised by os.environ (whose module is frozen: no file to match)
    if blake3_digest(name.encode()).hex()[:8] not in os.environ.get("CHIMAERA_DRIVER_ENV", "").split(","):
        return
    exc.add_note(
        f"CH122: {name} is set where the program runs, but calls see only the environment variables it passes: "
        f"ch.init(env={{{name!r}: os.environ[{name!r}]}}), or CHIMAERA_ENV_PASSTHROUGH={name} when starting it"
    )


def _path_hint(exc: BaseException) -> None:
    """CH123: a call that ran on another machine than the program and failed
    on a missing file is told so: the program's files are not copied to the
    peers (its project's code is), which "works on my machine" hides."""
    if not isinstance(exc, FileNotFoundError) or not exc.filename:
        return
    from ._runtime import machine_digest

    theirs = os.environ.get("CHIMAERA_DRIVER_MACHINE", "")
    if not theirs or theirs == machine_digest():
        return
    import socket

    exc.add_note(
        f"CH123: this call ran on {socket.gethostname()}, not on the machine the program runs on, and that "
        "machine's files are not copied here. Read the file where the program runs and pass its contents (large "
        "ones go through ch.put), or keep it on storage every machine sees (a shared mount, object storage)"
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
        # The actor's concurrency, from its constructor: a call through a
        # handle rebuilt without the class's options (pickled, by key) carries
        # none, and sizing lanes by it ran a concurrency=4 actor one at a time.
        self.actor_concurrency = 0
        self.actor_loop: asyncio.AbstractEventLoop | None = None
        self.loop_lock = threading.Lock()
        # Generator calls: how far each consumer has read (from ExecCredit).
        self.credit: dict[bytes, int] = {}
        self.credit_cond = threading.Condition()
        self.lanes: dict[str, concurrent.futures.ThreadPoolExecutor] = {}
        self.async_pool: concurrent.futures.ThreadPoolExecutor | None = None  # see async_steps
        self.batchers: dict[str, _batch.Batcher | None] = {}  # @ch.method(batch=...) methods
        # The actor class's lane limits, computed once: scanning the class costs
        # more than the rest of a small call (see lane).
        self.lane_limits: dict[type, dict[str, int]] = {}
        # Per thread: `on_block`, which starts a batch's next call when the
        # current one waits (see run_batch), and `guest` (see read_loop).
        self.local = threading.local()
        # The call whose own code runs on the main thread, if any: the only
        # one a signal can interrupt (see interrupt).
        self.main_call: _context.TaskContext | None = None
        # True while the main thread runs a call or a restore. An execute that
        # arrives meanwhile is a guest: the peer sends one only while every
        # call here waits on futures, and it runs on a thread of its own.
        self.main_busy = False
        # Calls waiting on futures (keys), each with whether its wait started its
        # batch's next call and the keys named by each of its waits still going
        # on (see blocked in main), and guests' threads while their code runs
        # (the ones a cancel can interrupt, see interrupt).
        self.waiting: dict[bytes, tuple[bool, list[list[bytes]]]] = {}
        self.waiting_lock = threading.Lock()
        self.guests: dict[bytes, int] = {}
        self.guest_lock = threading.Lock()
        self.code_lock = threading.Lock()  # a guest may load code beside its host
        # exit_actor() ran in this worker's actor: later calls fail at once.
        self.exited = False
        # A restore whose state would not load (a failing State(migrate=)):
        # the error and its traceback, raised by every call until one loads.
        self.restore_error: tuple[BaseException, Any] | None = None
        self.terminating = False  # on_terminate ran: the peer is closing this worker
        self.stdout: _Stdout | None = None  # set by main()
        self.running: dict[bytes, _context.TaskContext] = {}
        self.inbox: queue.SimpleQueue[tuple[Any, ...] | None] = queue.SimpleQueue()
        self.fetcher: concurrent.futures.ThreadPoolExecutor | None = None  # see fetch_parts
        self.peers, self.peers_at = 1, -PEERS_EVERY  # live peers, and when counted (see splits)

    # ---- inputs -----------------------------------------------------------

    def load(self, digest: bytes) -> Any:
        with self.code_lock:
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

    def load_fetched(self, digest: bytes) -> Any:
        """`load` code that the peer may still be fetching (a restore's class:
        see Core::send_restore), waiting for it up to CODE_FETCH_WAIT_S."""
        deadline = time.monotonic() + CODE_FETCH_WAIT_S
        while True:
            try:
                return self.load(digest)
            except FileNotFoundError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    def value(self, outcome: tuple[Any, ...]) -> Any:
        if outcome[0] != "ok":
            raise RuntimeError(f"prerequisite has no value: {outcome[:4]}")
        _, inline, run, obj, _size, _oob = outcome
        if inline is not None:
            rdir = _serde.run_dir(self.object_dir, run)
            # A large bytes value is a frame of a few bytes naming its blob:
            # one read onto the heap (see Runtime.read_value). Decoded through
            # a mapping, ten calls reading a 256 MiB value at once spent their
            # time faulting new arena pages and then unmapping them.
            fast = _serde._plain_bytes(inline, -1, rdir)
            return fast if fast is not None else _serde.loads(inline, rdir)
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
                # Fetched from the peer that holds it, not decoded: only one value is read.
                read_object(self.object_dir, run, obj, decode=False)
            try:
                v = _serde.read_part(path, index, count, rdir)
            except FileNotFoundError as e:  # a part stored on its own, by a peer that is gone
                from ._errors import DataLost

                lost = f"value {index} of {obj.hex()} (run {run.hex()}) is no longer stored on any live peer"
                raise DataLost(lost) from e
        if isinstance(v, _future.ItemFailed):
            raise _future.from_outcome(v.err)
        return v

    def fetch_parts(self, wanted: list[Any], deps: list[tuple[Any, ...]]) -> None:
        """Bring the values these placeholders read into this peer's store,
        several at once: a shuffle's gather reads one part of every scatter (and
        a merge every input), which one after another costs a round trip each.
        A value that cannot be fetched here fails when it is read (see part)."""
        if _serde.localize_hook is None:
            return
        results: dict[tuple[bytes, bytes], list[_future.PartDep]] = {}
        for x in wanted:
            outcome = deps[x.dep if isinstance(x, _future.PartDep) else x.index]
            if outcome[0] == "ok" and outcome[1] is None:
                xs = results.setdefault((outcome[2], outcome[3]), [])
                if isinstance(x, _future.PartDep):
                    xs.append(x)
        # First the results held elsewhere (a gather's scatters ran on other
        # peers), then the parts that those stored apart.
        self.localize_all([k for k in results if not os.path.exists(_serde.object_path(self.object_dir, *k))])
        missing = []
        for (run, obj), xs in results.items():
            for x in xs:  # none: read whole
                try:  # the header and that value's entry alone, in any layout
                    part = _serde.part_of(_serde.object_path(self.object_dir, run, obj), x.index, x.count)
                except FileNotFoundError:
                    break  # fails when read (see part)
                if part is not None and not os.path.exists(_serde.object_path(self.object_dir, run, part)):
                    missing.append((run, part))
        self.localize_all(missing)

    def localize_all(self, objects: list[tuple[bytes, bytes]]) -> None:
        """Fetch these (run, object) into this peer's store, several at once;
        one that cannot be fetched fails when it is read."""
        objects = list(dict.fromkeys(objects))
        if len(objects) < 2 or _serde.localize_hook is None:
            return
        if self.fetcher is None:
            self.fetcher = concurrent.futures.ThreadPoolExecutor(PART_FETCHES, thread_name_prefix="chimaera-parts")
        localize = _serde.localize_hook

        def one(m: tuple[bytes, bytes]) -> None:
            try:
                localize(*m)
            except FileNotFoundError:
                pass

        list(self.fetcher.map(one, objects))

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
            wanted: list[Any] = []

            def note(x: Any) -> None:
                if isinstance(x, placeholder):
                    wanted.append(x)

            for a in (*frame_args, *frame_kwargs.values()):
                sub(a, note)
            self.fetch_parts(wanted, deps)
        return tuple(sub(a) for a in frame_args), {k: sub(v) for k, v in frame_kwargs.items()}

    # ---- outputs ----------------------------------------------------------

    def ok(self, key: bytes, attempt: int, value: Any, keep_local: bool = False) -> tuple[Any, ...]:
        run = key[:16]
        return self.ok_object(run, child_id(run, [key[16:], attempt.to_bytes(4, "little")]), value, keep_local)

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

    def ok_object(self, run: bytes, obj: bytes, value: Any, keep_local: bool = False) -> tuple[Any, ...]:
        """Store `value` as object `obj` of `run` (inline when small). Arena
        arrays in it are linked, not copied (see _objects); their blobs carry
        the run's prefix, so they live as long as the result itself, and the
        outcome names them (with their sizes): the peer places calls by them,
        fetches them before a call reads them, and frees them with the result
        (the frame alone may be a few inline bytes). Blobs are named from
        `obj` in order, so running the call again (lineage, the same attempt)
        writes the same ids the frame of the first run names; they keep the
        run's prefix (`obj` has it). With `keep_local` (a durability class
        other than the default) the peer makes no background copy of them."""
        rdir = _serde.run_dir(self.object_dir, run)
        blobs: list[tuple[bytes, int]] = []
        tried = 0

        def claim(view: memoryview) -> tuple[bytes, int] | None:
            nonlocal tried
            if view.nbytes < _objects.ARENA_MIN:
                return None
            name = child_id(obj, [b"blob", tried.to_bytes(4, "little")])
            tried += 1
            got = _objects.claim(view, rdir, blob=name, keep_local=keep_local)
            if got is not None:
                try:
                    blobs.append((got[0], os.stat(os.path.join(rdir, got[0].hex())).st_size))
                except OSError:
                    blobs.append((got[0], view.nbytes))
            return got

        sparse = sorted(value.present) if type(value) is _serde.Sparse else None
        if sparse is not None or type(value) is tuple and 2 <= len(value) <= SEQ_MAX:
            # Possibly several values (returns=N): each readable on its own.
            # Of a Sparse, only the values it holds (see _serde.SPARSE_MAGIC).
            encoded, weights = [], []
            for v in value if sparse is None else [value.present[i] for i in sparse]:
                linked = len(blobs)
                encoded.append(_serde.dumps_parts(v, external=claim))
                # Its frame and the arena buffers it links (blobs of their own).
                weights.append(encoded[-1][1] + sum(n for _, n in blobs[linked:]))
            ids = None
            total = sum(weights)
            if total > self.inline_limit and self.splits(len(encoded), total):
                # Large values in files of their own, written before the file that
                # names them: a consumer of one value fetches only that one.
                ids = [
                    child_id(obj, [i.to_bytes(4, "little")]) if w >= PART_FILE_MIN else None
                    for i, w in enumerate(weights)
                ]
                for part, (p, _, _) in zip(ids, encoded):
                    if part is not None:
                        self.write(run, part, p, keep_local)
            parts, size, oob = _serde.pack_seq(encoded, ids, sparse, len(value))
            if ids is not None and any(ids):  # never inline: the peer frees and finds parts through this file
                self.write(run, obj, parts, keep_local)
                return ("ok", run, obj, size, None, oob, blobs)
        else:
            parts, size, oob = _serde.dumps_parts(value, external=claim)
        if size <= self.inline_limit:
            data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)
            return ("ok", run, obj, size, data, oob, blobs)
        self.write(run, obj, parts, keep_local)
        return ("ok", run, obj, size, None, oob, blobs)

    def write(self, run: bytes, obj: bytes, parts: list[Any], keep_local: bool) -> None:
        path = _serde.object_path(self.object_dir, run, obj)
        if keep_local:
            _objects.mark_local(path)
        _serde.write_file(path, parts)

    @staticmethod
    def err(exc: BaseException) -> tuple[Any, ...]:
        _gpu_hint(exc)
        _env_hint(exc)
        _path_hint(exc)
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

    def task_context(self, msg: dict[str, Any], item: tuple[Any, ...]) -> _context.TaskContext:
        key, attempt, _, _, item_index = item
        return _context.TaskContext(
            run=key[:16],
            task=key[16:],
            attempt=attempt,
            item_index=item_index,
            policy=msg["policy"],
            deadline_unix_ms=msg["deadline_unix_ms"],
            actor=msg["actor"],
        )

    def run_one(self, msg: dict[str, Any], item: tuple[Any, ...]) -> None:
        key, _, args_ref, deps, _ = item
        ctx = self.task_context(msg, item)
        self.running[key] = ctx
        token = _context.set_task(ctx)
        helper0 = _helper_cpu_ns() if _helper_cpu_ns is not None else None
        entry = time.time_ns()
        seen = self.stdout.seen if self.stdout is not None else 0
        on_main = threading.current_thread() is threading.main_thread()
        guest = not on_main and getattr(self.local, "guest", False)
        result: Any = None
        error: BaseException | None = None
        try:
            if on_main:
                self.main_call = ctx
            elif guest:
                with self.guest_lock:
                    self.guests[key] = threading.get_ident()
            try:
                if self.exited:  # exit_actor() ran; the close that ends this process is on its way
                    raise ActorDied("the actor ended itself with ch.context().exit_actor()")
                result = self.invoke(msg, args_ref, deps)
            finally:
                if on_main:
                    self.main_call = None
                elif guest:
                    self.end_guest(key)
        except BaseException as exc:  # user code may raise anything, including SystemExit
            error = exc  # a guest's cancel interrupt lands here even if it fired in end_guest
        self.finish(msg, item, ctx, entry, seen, result, error, token, helper0)
        if self.terminating and on_main:
            raise SystemExit(0)  # its outcome is out: carry on exiting, as on_terminate began

    def end_guest(self, key: bytes) -> None:
        """A guest's code has returned: take back an interrupt not yet raised,
        so none can land while its outcome is sent."""
        with self.guest_lock:
            ident = self.guests.pop(key, None)
            if ident is not None:
                _async_raise(ident, None)

    def start_async(self, msg: dict[str, Any], item: tuple[Any, ...]) -> None:
        """An async actor method in a lane: its arguments are read here (they
        may be fetched), its coroutine runs on the actor's loop holding no
        thread while it awaits, and its outcome is sent from a lane thread. A
        thread held per awaiting call meant thousands of threads for an I/O
        actor given concurrency in the thousands."""
        key, _, args_ref, deps, _ = item
        ctx = self.task_context(msg, item)
        self.running[key] = ctx
        token = _context.set_task(ctx)
        entry = time.time_ns()
        seen = self.stdout.seen if self.stdout is not None else 0
        try:
            if self.exited:
                raise ActorDied("the actor ended itself with ch.context().exit_actor()")
            args, kwargs = self.call_args(args_ref, deps)
            coro = getattr(self.instance, msg["method"])(*args, **kwargs)
        except BaseException as exc:
            self.finish(msg, item, ctx, entry, seen, None, exc, token)
            return
        steps = self.async_steps()

        async def run() -> None:
            # The task runs in a copy of this thread's context: the call's own.
            result: Any = None
            error: BaseException | None = None
            try:
                result = await coro
            except BaseException as exc:
                error = exc
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(steps, self.finish, msg, item, ctx, entry, seen, result, error, None)

        asyncio.run_coroutine_threadsafe(run(), self._actor_loop()).add_done_callback(_report_failure)
        _context.reset(token)

    def finish(
        self,
        msg: dict[str, Any],
        item: tuple[Any, ...],
        ctx: _context.TaskContext,
        entry: int,
        seen: int,
        result: Any,
        error: BaseException | None,
        token: Any,
        helper0: int | None = None,
    ) -> None:
        """Encode and send a call's outcome (its value or its error).
        `helper0` is this thread's helper CPU when the call began (run on this
        thread from start to end): what the helpers spent since is sent."""
        key, attempt = item[0], item[1]
        retryable, fingerprint, state = False, b"", b""
        try:
            if error is None:
                try:
                    if isinstance(result, _future.Future) and result._put is None:
                        # Returning a future forwards it: this call resolves to that
                        # call's outcome, and this worker is free meanwhile. A ch.put
                        # handle is no call to forward to: it is stored as the value.
                        result._escaped = True  # the shard now pins it for the run
                        result._rt.client.flush()
                        outcome = ("forward", result.key)
                    else:
                        outcome = self.ok(key, attempt, result, bool(msg["policy"].get("durability")))
                        if msg.get("durable"):
                            state = _durable_state(self.instance)
                except BaseException as exc:  # an unpicklable value, a failing durable state
                    error = exc
            if isinstance(error, SystemExit) and self.terminating:
                # on_terminate stopped it: the actor was closed, not a failure of the call
                error = ActorDied("the actor was closed while this call ran")
            if error is not None:
                outcome = self.err(error)
                retryable = _matches(error, msg["retry_exception_types"]) and not isinstance(error, NonRetryable)
                if retryable:
                    frames = "|".join(f"{f.filename}:{f.lineno}" for f in traceback.extract_tb(error.__traceback__))
                    fingerprint = blake3_digest(f"{outcome[2]}|{outcome[3]}|{frames}".encode())
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            if self.stdout is not None and not self.active(besides=key):
                # Beside other calls, a line forwarded once this one ends would
                # go to them: wait longer for it under load.
                self.stdout.drain(seen, 5.0 if len(self.running) > 1 else 1.0)
            if token is not None:
                _context.reset(token)
            self.running.pop(key, None)
            self.waiting.pop(key, None)
        self.trimmer((time.time_ns() - entry) / 1e9)
        if "direct" in msg:  # straight back to the caller, who records it with the peer
            msg["direct"].done(key, outcome, entry, msg["cpu0"])
        else:
            done = (key, attempt, outcome, retryable, fingerprint, entry, time.time_ns(), state)
            if helper0 is not None and _helper_cpu_ns is not None:
                self.conn.send_done(*done, _helper_cpu_ns() - helper0)
            else:
                self.conn.send_done(*done)
        if ctx.exiting and not self.exited:
            # After this call's outcome went out: closing the actor ends this process.
            from ._runtime import Runtime

            self.exited = True
            Runtime.get().client.close_actor(ctx.actor)

    def shut_down(self) -> None:
        """``close(drain=True)``'s last call: run the actor's ``__exit__`` if
        it has one, then end the actor once this call's outcome is out."""
        ctx = _context.current_task()
        try:
            hook = getattr(type(self.instance), "__exit__", None)
            if hook is not None:
                hook(self.instance, None, None, None)
        finally:
            if ctx is not None:
                ctx.exiting = True

    def invoke(self, msg: dict[str, Any], args_ref: tuple[Any, ...], deps: list[tuple[Any, ...]]) -> Any:
        kind = msg["kind"]
        args, kwargs = self.call_args(args_ref, deps)
        if kind == CALL_ACTOR_CTOR:
            _leases.own_run = getattr(_context.current_task(), "run", None)  # holds what other runs pass it
            cls = self.load(msg["fn_digest"])
            self.actor_concurrency = int(msg["policy"].get("concurrency") or 1)
            self.instance = cls(*args, **kwargs)
            _direct.listen(self, msg)  # callers on this machine may now call it directly
            return None
        if kind == CALL_ACTOR_METHOD:
            if self.instance is None:
                if self.restore_error is not None:
                    exc, tb = self.restore_error
                    raise exc.with_traceback(tb)
                raise RuntimeError("actor method called before its constructor ran")
            if msg["method"] == _context.SHUTDOWN_METHOD:
                return self.shut_down()
            if msg["method"] == _context.GRAPH_METHOD:
                return _graph.serve(self, *args)
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
                fn = _workflow.definition(fn, ctx.attempt)  # a replay: its history's version, or a compatible one
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
        of the consumer, in items and in stored bytes. The call's result is
        the number of items."""
        from ._runtime import STREAM_WINDOW

        ctx = _context.current_task()
        assert ctx is not None
        key = ctx.run + ctx.task
        window = int(msg["policy"].get("max_in_flight") or STREAM_WINDOW)
        # A consumer that credits by bytes sends its byte window. 0 comes from
        # a driver older than byte credits, which credits by items only:
        # waiting on bytes would wait for a credit it never sends.
        window_bytes = int(msg["policy"].get("max_in_flight_bytes") or 0)
        sent = 0
        # Stored sizes of the items sent that the consumer has not credited,
        # oldest first, and their sum. An item larger than the whole window
        # still goes, alone.
        ahead: collections.deque[int] = collections.deque()
        ahead_bytes = 0

        def put(item: Any) -> bool:
            nonlocal sent, ahead_bytes
            with self.credit_cond:
                while True:
                    credit = self.credit.get(key, 0)
                    while len(ahead) > max(0, sent - credit):
                        ahead_bytes -= ahead.popleft()
                    if sent < credit + window and (not window_bytes or ahead_bytes < window_bytes or not ahead):
                        break
                    if ctx._cancelled.is_set():
                        return False
                    self.credit_cond.wait(0.5)
            index = sent
            run = key[:16]
            obj = child_id(run, [key[16:], ctx.attempt.to_bytes(4, "little"), b"item", index.to_bytes(8, "little")])
            outcome = self.ok_object(run, obj, item)
            self.conn.send_yield(key, ctx.attempt, index, outcome)
            ahead.append(outcome[3])  # the stored size the consumer counts too (Stream._deliver)
            ahead_bytes += outcome[3]
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

    def async_steps(self) -> concurrent.futures.ThreadPoolExecutor:
        """The few threads that start and finish async actor calls."""
        with self.loop_lock:
            if self.async_pool is None:
                self.async_pool = concurrent.futures.ThreadPoolExecutor(ASYNC_STEP_THREADS, thread_name_prefix="actor-async")
            return self.async_pool

    def _actor_loop(self) -> asyncio.AbstractEventLoop:
        # Calls in several lanes arrive at once: create the one loop under a lock,
        # or two threads could each make one and split the actor's coroutines.
        with self.loop_lock:
            if self.actor_loop is None:
                loop = asyncio.new_event_loop()
                thread = threading.Thread(target=loop.run_forever, name="actor-loop", daemon=True)
                thread.start()
                threading.Thread(target=_watch_loop, args=(loop, thread), name="actor-loop-watch", daemon=True).start()
                self.actor_loop = loop
            return self.actor_loop

    def handle(self, msg: dict[str, Any]) -> None:
        if "direct" in msg and _direct.starting(self, msg):
            return
        # A remote driver's project modules, before any of its code loads.
        _project.ensure(self.object_dir, msg["items"][0][0][:16], os.path.join(os.path.dirname(self.code_dir), "projects"))
        batcher = self.batcher(msg["method"]) if msg["kind"] == CALL_ACTOR_METHOD else None
        if batcher is not None:
            for item in msg["items"]:
                batcher.submit(msg, item)
            return
        lane = self.lane(msg) if msg["kind"] == CALL_ACTOR_METHOD else None
        if lane is not None:
            if inspect.iscoroutinefunction(getattr(type(self.instance), msg["method"], None)):
                # Starting and finishing an async call is brief, and the peer
                # sends at most `concurrency` at once: a few threads do it (a
                # lane's pool grows toward its size under a burst).
                lane, run = self.async_steps(), self.start_async
            else:
                run = self.run_one
            for item in msg["items"]:
                lane.submit(run, msg, item).add_done_callback(_report_failure)
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
            self.local.on_block = on_block
            try:
                while (item := take()) is not None:
                    self.run_one(msg, item)
            finally:
                self.local.on_block = None

        def on_block() -> bool:
            """Start the next call on a helper thread; False if none is left."""
            with lock:
                if not pending:
                    return False
            t = threading.Thread(target=work, name="batch-item", daemon=True)
            helpers.append(t)
            t.start()
            return True

        work()
        for t in helpers:  # the list only grows while items still run
            t.join()

    def active(self, besides: bytes | None = None) -> list[_context.TaskContext]:
        """Running calls that are not waiting on futures, other than `besides`."""
        return [c for k, c in list(self.running.items()) if k != besides and k not in self.waiting]

    def guest(self, msg: dict[str, Any]) -> None:
        """Run a guest's Execute on this thread (see read_loop)."""
        self.local.guest = True
        try:
            self.handle(msg)
        except BaseException:
            # As on the main thread, a failure outside any call ends the
            # process; the peer then retries or fails every call it ran.
            traceback.print_exc(file=sys.__stderr__)
            os._exit(1)

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

        total = max(self.actor_concurrency, int(msg["policy"].get("concurrency") or 1))
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
        """The peer's messages, on their own thread: calls go to the inbox in
        order, cancels and credit act at once. When the connection ends the
        executor stops, and leaves soon after if the peer is gone."""
        while True:
            m = self.conn.recv()
            if m is None or m[0] == "shutdown":
                self.inbox.put(None)
                if m is None:
                    # The peer is gone (killed, crashed): nothing this process
                    # runs can report back, and its calls are retried
                    # elsewhere. A call still sleeping or looping would keep
                    # it alive for good, so leave after a moment either way.
                    t = threading.Timer(PEER_GONE_EXIT_S, os._exit, (1,))
                    t.daemon = True
                    t.start()
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
            elif (
                m[0] == "execute"
                and self.main_busy
                and self.instance is None
                and m[1]["kind"] not in (CALL_ACTOR_CTOR, CALL_ACTOR_METHOD)
            ):
                # A guest: every call here waits on futures (the peer sends one
                # only after this executor said so, which happens after
                # main_busy is set), so it runs beside them on its own thread.
                # Started from here, it does not inherit the host's context.
                # Never an actor's call: a restarted actor's executor has no
                # instance while its main thread restores one, and the method
                # that follows the Restore must wait for it in the inbox.
                threading.Thread(target=self.guest, args=(m[1],), name="guest", daemon=True).start()
            elif m[0] in ("execute", "restore"):
                # In order: a restore lands between the calls it separates.
                self.inbox.put(m)

    def interrupt(self, ctx: _context.TaskContext) -> None:
        """A cancelled call that has not stopped by itself: interrupt it with
        SIGUSR2 if its code runs on the main thread (a sleep or a Python loop
        then raises ch.Cancelled). Anything else is killed by the peer a
        little later, unless it is an actor's. A guest's code is interrupted
        with an exception raised in its thread (at its next Python step: a
        sleep or C code it is in runs to the end)."""
        if self.main_call is ctx:
            signal.pthread_kill(threading.main_thread().ident, signal.SIGUSR2)
            return
        with self.guest_lock:
            ident = self.guests.get(ctx.run + ctx.task)
            if ident is not None:
                from ._errors import Cancelled

                _async_raise(ident, Cancelled)

    def serve(self) -> None:
        threading.Thread(target=self.read_loop, name="executor-reader", daemon=True).start()
        while True:
            m = self.inbox.get()
            if m is None:
                return
            self.main_busy = True
            try:
                if m[0] == "restore":
                    self.restore(m[1])
                else:
                    self.handle(m[1])
            finally:
                self.main_busy = False

    def restore(self, msg: dict[str, Any]) -> None:
        """Make the actor instance equal to its committed state, without
        running ``__init__``, as an instance of the class its record names:
        bound to a newer class, state an older one wrote goes through that
        class's ``State(migrate=)``. If it will not load, every call fails
        with the error until a restore succeeds."""
        try:
            cls = self.load_fetched(msg["fn_digest"])
        except BaseException as exc:  # noqa: BLE001 - the actor's calls report it
            exc.add_note("raised loading the class to restore a durable actor's state with")
            self.instance, self.restore_error = None, (exc, exc.__traceback__)
            return
        blob = msg["state"]
        if not blob:
            if self.instance is None or type(self.instance) is cls:
                return
            blob = _durable_state(self.instance)  # bound to a newer class before any commit: its live state
        try:
            state = _statecodec.restore(cls, blob)
            obj = cls.__new__(cls)
            if getattr(type(obj), "__setstate__", None) is not None:
                obj.__setstate__(state)
            elif state is not None:
                obj.__dict__.update(state)
        except BaseException as exc:  # noqa: BLE001 - the actor's calls report it
            exc.add_note(f"raised restoring the durable state of {cls.__qualname__} (State(migrate=) or __setstate__)")
            self.instance, self.restore_error = None, (exc, exc.__traceback__)
            return
        self.instance, self.restore_error = obj, None


_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
# How long a cancelled call has to stop by itself before it is interrupted.
CANCEL_INTERRUPT_S = float(os.environ.get("CHIMAERA_CANCEL_INTERRUPT_SECONDS") or 2.0)  # the peer checked it at its start
# How long an executor outlives its peer's connection (time to exit by itself).
PEER_GONE_EXIT_S = 2.0


# A step of an async method that holds the actor's event loop this long warns.
LOOP_STALL_S = 1.0
_NOT_USER = tuple(
    os.path.dirname(m.__file__) + os.sep for m in (asyncio, concurrent.futures) if m.__file__
) + (threading.__file__, _PACKAGE_DIR + os.sep)


def _watch_loop(loop: asyncio.AbstractEventLoop, thread: threading.Thread) -> None:
    """Say, once per method, when a step of an async method blocks its actor's
    event loop: every other call on the actor waits behind it (blocking I/O or
    CPU work in an ``async def``; Ray says nothing)."""
    warned: set[str] = set()
    while thread.is_alive():
        ran = threading.Event()
        try:
            loop.call_soon_threadsafe(ran.set)
        except RuntimeError:
            return  # the loop is closed
        if ran.wait(LOOP_STALL_S):
            time.sleep(0.25)
            continue
        # One step holding the loop, not a loop busy with many short ones: the
        # same coroutine frame is running a moment later.
        first = _loop_culprit(sys._current_frames().get(thread.ident or 0))
        if ran.wait(0.2):
            continue
        method, where, frame = _loop_culprit(sys._current_frames().get(thread.ident or 0))
        if method and frame == first[2] and method not in warned:
            warned.add(method)
            sys.stderr.write(
                f"chimaera: {method} has held its actor's event loop for over {LOOP_STALL_S:.0f} s at {where}, and no "
                "other call on the actor runs meanwhile. Await non-blocking calls, move blocking work off the loop "
                "(await asyncio.to_thread(fn, ...)), or make the method a plain def (it then runs on a thread)\n"
            )
        ran.wait()


def _loop_culprit(frame: Any) -> tuple[str, str, int]:
    """The async method running on the loop's thread, its current line, and
    the identity of its frame."""
    outer = None
    while frame is not None:
        if not os.path.abspath(frame.f_code.co_filename).startswith(_NOT_USER):
            outer = frame  # the outermost user frame is the method the loop is running
        frame = frame.f_back
    if outer is None:
        return "", "", 0
    method = getattr(outer.f_code, "co_qualname", outer.f_code.co_name)
    return method, f"{outer.f_code.co_filename}:{outer.f_lineno}", id(outer)


def _async_raise(ident: int, exc: type[BaseException] | None) -> None:
    """Raise `exc` in thread `ident` at its next Python step; None takes back
    one not yet raised. (A signal reaches only the main thread.)"""
    ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(ident), ctypes.py_object(exc) if exc is not None else None)


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
    by default), pickled under its class's state version (_statecodec)."""
    return _statecodec.dumps(instance)


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
    ex.executor_id = a.id
    def owner() -> _context.TaskContext | None:
        """The call output without a call of its own (a thread it started, a
        subprocess, C code) belongs to: the one running, else the one running
        that is not waiting on futures (a guest beside its waiting host)."""
        running = list(ex.running.values())
        if len(running) == 1:
            return running[0]
        active = ex.active()
        return active[0] if len(active) == 1 else None

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
        """A wait of `ctx`'s call starts or ends (naming the same `waiting`
        keys both times). Coroutines or threads of one call may wait at once
        (two task groups gathered): the call is blocked from the first wait
        to the end of the last, on the keys they name between them."""
        key = ctx.run + ctx.task
        with ex.waiting_lock:
            helper, waits = ex.waiting.get(key, (False, []))
            if is_blocked:
                if not waits:
                    on_block = getattr(ex.local, "on_block", None)
                    helper = on_block is not None and on_block()
                waits.append(waiting)
            elif waiting in waits:
                waits.remove(waiting)
            if not waits:
                ex.waiting.pop(key, None)
                conn.send_blocked(key, False, [], False)
                return
            ex.waiting[key] = (helper, waits)
            # Offer to host a guest only while every wait is on futures (polls
            # of a store or queue name none) by a plain call: not an actor's,
            # not a gang rank, and not one whose batch already goes on beside it.
            host = all(waits) and ex.instance is None and ctx.gang is None and not helper
            conn.send_blocked(key, True, list(dict.fromkeys(k for w in waits for k in w)), host)

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

    def on_terminate(signum: int, frame: Any) -> None:
        # The peer is closing this worker (its actor was closed, or its run
        # ended): exit as a program does, so the actor's __del__ and atexit
        # handlers run (Ray's kill skips both). The peer stops it outright if
        # this takes longer than its grace period.
        ex.instance = None
        ex.terminating = True
        gc.collect()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, on_terminate)

    def localize(run: bytes, obj: bytes) -> None:
        from ._runtime import Runtime

        Runtime.get().client.localize(run, obj)

    _serde.localize_hook = localize
    ex.serve()


if __name__ == "__main__":
    main()
