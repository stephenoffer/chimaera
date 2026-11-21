"""Executor process: ``python -m chimaera._executor --socket PATH --id N``.

Started by a peer with the run's interpreter, working directory and import
path. It never runs the user's main script: callables arrive as code blobs.
One reader thread receives instructions; calls run on the main thread (or on
a small pool for actor methods declared with ``concurrency > 1``).
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import inspect
import io
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

from . import _context, _future, _serde
from ._native import ExecConn, blake3_digest, child_id
from ._serde import Dep

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
        w = conn.welcome
        self.object_dir: str = w["object_dir"]
        self.code_dir: str = w["code_dir"]
        self.inline_limit: int = w["inline_limit"]
        self.code: dict[bytes, Any] = {}
        self.instance: Any = None
        self.actor_loop: asyncio.AbstractEventLoop | None = None
        self.pool: concurrent.futures.ThreadPoolExecutor | None = None
        self.running: dict[bytes, _context.TaskContext] = {}
        self.inbox: queue.Queue[tuple[Any, ...] | None] = queue.Queue()

    # ---- inputs -----------------------------------------------------------

    def load(self, digest: bytes) -> Any:
        fn = self.code.get(digest)
        if fn is None:
            with open(os.path.join(self.code_dir, digest.hex()), "rb") as f:
                fn = _serde.load_code(f.read())
            self.code[digest] = fn
        return fn

    def value(self, outcome: tuple[Any, ...]) -> Any:
        if outcome[0] != "ok":
            raise RuntimeError(f"prerequisite has no value: {outcome[:4]}")
        _, inline, run, obj, _size, _oob = outcome
        if inline is not None:
            return _serde.loads(inline)
        from ._runtime import read_object

        return read_object(self.object_dir, run, obj)

    def call_args(self, args_ref: tuple[Any, ...], deps: list[tuple[Any, ...]]) -> tuple[tuple[Any, ...], dict[str, Any]]:
        frame_args, frame_kwargs = self.value(args_ref)
        resolved: dict[int, Any] = {}

        def sub(x: Any) -> Any:
            if isinstance(x, Dep):
                if x.index not in resolved:
                    resolved[x.index] = self.value(deps[x.index])
                return resolved[x.index]
            return x

        return tuple(sub(a) for a in frame_args), {k: sub(v) for k, v in frame_kwargs.items()}

    # ---- outputs ----------------------------------------------------------

    def ok(self, key: bytes, attempt: int, value: Any) -> tuple[Any, ...]:
        parts, size, oob = _serde.dumps_parts(value)
        run = key[:16]
        obj = child_id(run, [key[16:], attempt.to_bytes(4, "little")])
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
        retryable, fingerprint = False, b""
        try:
            outcome = self.ok(key, attempt, self.invoke(msg, args_ref, deps))
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
        self.conn.send_done(key, attempt, outcome, retryable, fingerprint, entry, time.time_ns())

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
            if inspect.isawaitable(result):
                return asyncio.run_coroutine_threadsafe(_awaitable(result), self._actor_loop()).result()
            return result
        fn = self.load(msg["fn_digest"])
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            return asyncio.run(_awaitable(result))
        if inspect.isgenerator(result):
            raise TypeError(
                "generator functions cannot be called remotely yet; return a list, or use fn.map over the inputs"
            )
        return result

    def _actor_loop(self) -> asyncio.AbstractEventLoop:
        if self.actor_loop is None:
            loop = asyncio.new_event_loop()
            threading.Thread(target=loop.run_forever, name="actor-loop", daemon=True).start()
            self.actor_loop = loop
        return self.actor_loop

    def handle(self, msg: dict[str, Any]) -> None:
        concurrency = int(msg["policy"].get("concurrency") or 1)
        if msg["kind"] == CALL_ACTOR_METHOD and concurrency > 1:
            # Overlapping calls: the peer sends up to `concurrency` at once;
            # they start in arrival order and may finish in any order.
            if self.pool is None:
                self.pool = concurrent.futures.ThreadPoolExecutor(concurrency, thread_name_prefix="actor-lane")
            for item in msg["items"]:
                self.pool.submit(self.run_one, msg, item).add_done_callback(_report_failure)
            return
        for item in msg["items"]:
            self.run_one(msg, item)

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
            elif m[0] == "execute":
                self.inbox.put(m)

    def serve(self) -> None:
        threading.Thread(target=self.read_loop, name="executor-reader", daemon=True).start()
        while True:
            m = self.inbox.get()
            if m is None:
                return
            self.handle(m[1])


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

    def blocked(ctx: _context.TaskContext, is_blocked: bool, waiting: list[bytes]) -> None:
        conn.send_blocked(ctx.run + ctx.task, is_blocked, waiting)

    _future._executor_hook = blocked
    ex.serve()


if __name__ == "__main__":
    main()
