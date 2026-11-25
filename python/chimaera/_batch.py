"""Batched actor methods: ``@ch.method(batch=ch.Batch(max_size=32, max_wait="5ms"))``.

The method is written for a list and called for one item:

    @ch.method(batch=ch.Batch(max_size=32, max_wait="5ms"))
    def embed(self, texts: list[str]) -> list[list[float]]: ...

    model.embed.remote("hello")          # Future[list[float]]

The actor's executor collects calls until it has ``max_size`` of them or the
first has waited ``max_wait``, calls the method once with their arguments in
arrival order, and gives each call its element of the returned list. If the
method raises, every call in the batch fails with that error (and retries on
its own terms). The peer keeps up to two batches of calls in flight, so the
next batch fills while one runs, and with ``max_bytes`` no more than that
many bytes of their arguments at once, so a batch never holds more either.
Every call taken into a batch gets its outcome sent, whatever happens to the
others: each is what lets the peer send the next call.
"""

from __future__ import annotations

import asyncio
import inspect
import queue
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any

from . import _context, _units
from ._native import blake3_digest


@dataclass(frozen=True)
class Batch:
    """How to batch a method's calls: at most `max_size` per call of the
    method, waiting at most `max_wait` (seconds, or "5ms") after the first.
    `max_bytes` (an int, or "32MiB") bounds the arguments of the calls its
    actor holds at once, the batch running and the next filling: a call that
    would go past it waits at its peer (one larger than it goes alone)."""

    max_size: int = 32
    max_wait: float | str = 0.005
    max_bytes: int | str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.max_size, bool) or not isinstance(self.max_size, int) or self.max_size < 1:
            raise ValueError(f"Batch(max_size=...) must be a positive int, got {self.max_size!r}")
        object.__setattr__(self, "max_wait", _units.parse_duration(self.max_wait, "Batch(max_wait=...)"))
        if self.max_bytes is not None:
            size = _units.parse_bytes(self.max_bytes, "Batch(max_bytes=...)")
            if size < 1:
                raise ValueError(f"Batch(max_bytes=...) must be positive, got {self.max_bytes!r}")
            object.__setattr__(self, "max_bytes", size)

    @property
    def lane(self) -> int:
        """Calls the peer may send at once: one batch running, the next filling."""
        return 2 * self.max_size


class Batcher:
    """Collects one method's calls in an actor's executor and runs them in batches."""

    def __init__(self, executor: Any, method: str, batch: Batch) -> None:
        self.ex, self.method, self.batch = executor, method, batch
        self.inbox: queue.SimpleQueue[tuple[dict[str, Any], tuple[Any, ...]]] = queue.SimpleQueue()
        threading.Thread(target=self._loop, name=f"batch-{method}", daemon=True).start()

    def submit(self, msg: dict[str, Any], item: tuple[Any, ...]) -> None:
        self.inbox.put((msg, item))

    def _loop(self) -> None:
        max_size, max_wait = self.batch.max_size, float(self.batch.max_wait)
        while True:
            calls = [self.inbox.get()]
            deadline = time.monotonic() + max_wait
            while len(calls) < max_size:
                left = deadline - time.monotonic()
                try:
                    calls.append(self.inbox.get(timeout=left) if left > 0 else self.inbox.get_nowait())
                except queue.Empty:
                    break
            try:
                self._run(calls)
            except BaseException:  # noqa: BLE001 - keep batching; each call already got its outcome
                traceback.print_exc()

    def _run(self, calls: list[tuple[dict[str, Any], tuple[Any, ...]]]) -> None:
        from ._executor import _durable_state, _matches

        ex = self.ex
        entry = time.time_ns()
        unsent = {item[0]: (msg, item) for msg, item in calls}

        def finish(msg: dict[str, Any], item: tuple[Any, ...], result: Any = None, exc: BaseException | None = None) -> None:
            key, attempt = item[0], item[1]
            if unsent.pop(key, None) is None:
                return  # already sent
            retryable, fingerprint, state = False, b"", b""
            if exc is None:
                try:
                    outcome = ex.ok(key, attempt, result, bool(msg["policy"].get("durability")))
                    if msg.get("durable"):
                        state = _durable_state(ex.instance)
                except BaseException as e:  # noqa: BLE001 - e.g. an unpicklable result: this call fails, not the batch
                    exc = e
            if exc is not None:
                outcome = ex.err(exc)
                retryable = _matches(exc, msg["retry_exception_types"])
                if retryable:
                    frames = "|".join(f"{f.filename}:{f.lineno}" for f in traceback.extract_tb(exc.__traceback__))
                    fingerprint = blake3_digest(f"{outcome[2]}|{outcome[3]}|{frames}".encode())
            ex.running.pop(key, None)
            try:
                ex.conn.send_done(key, attempt, outcome, retryable, fingerprint, entry, time.time_ns(), state)
            except BaseException as e:  # noqa: BLE001 - this outcome would not go: send its error, go on
                traceback.print_exc()
                try:
                    ex.conn.send_done(key, attempt, ex.err(e), False, b"", entry, time.time_ns(), b"")
                except BaseException:  # noqa: BLE001 - the peer is gone: it fails or retries the calls itself
                    traceback.print_exc()

        try:
            self._call(calls, finish)
        finally:
            # Whatever stopped the batch, every call it took gets an outcome.
            stopped = RuntimeError(f"{self.method}: the batch stopped before this call's outcome was sent")
            for msg, item in list(unsent.values()):
                finish(msg, item, exc=stopped)

    def _call(self, calls: list[tuple[dict[str, Any], tuple[Any, ...]]], finish: Any) -> None:
        """Read the calls' arguments, call the method once, and finish each call."""
        ex = self.ex
        live: list[tuple[dict[str, Any], tuple[Any, ...], Any]] = []
        values: list[Any] = []
        for msg, item in calls:
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
            ex.running[key] = ctx
            try:
                args, kwargs = ex.call_args(args_ref, deps)
                if kwargs or len(args) != 1:
                    raise TypeError(f"{self.method} is batched: call it with one argument, {self.method}.remote(item)")
            except BaseException as exc:  # noqa: BLE001 - reported to this call's caller
                finish(msg, item, exc=exc)
                continue
            live.append((msg, item, ctx))
            values.append(args[0])
        if not live:
            return
        # Calls the method makes belong to the batch's first call.
        token = _context.set_task(live[0][2])
        try:
            result = getattr(ex.instance, self.method)(values)
            if inspect.isawaitable(result):
                result = asyncio.run_coroutine_threadsafe(_await(result), ex._actor_loop()).result()
            results = list(result)
            if len(results) != len(values):
                raise ValueError(f"{self.method} returned {len(results)} results for a batch of {len(values)} calls")
        except BaseException as exc:  # noqa: BLE001 - every call in the batch gets the error
            for msg, item, _ in live:
                finish(msg, item, exc=exc)
        else:
            for (msg, item, _), r in zip(live, results, strict=True):
                finish(msg, item, result=r)
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            _context.reset(token)


async def _await(awaitable: Any) -> Any:
    return await awaitable
