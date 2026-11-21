"""``ray.util.multiprocessing.Pool``: the standard library's ``multiprocessing.Pool``
API over a pool of Chimaera actors (one per process, as Ray's is).

``initializer(*initargs)`` runs once in each worker. Work goes out in chunks
(``chunksize``, or about four chunks per worker), and ``map``/``imap`` keep
input order while ``imap_unordered`` yields as results arrive.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable, Iterable, Iterator
from typing import Any

import chimaera as ch

from .._unsupported import unsupported


@ch.remote
class _Worker:
    def __init__(self, initializer: Callable[..., Any] | None, initargs: tuple[Any, ...]) -> None:
        if initializer is not None:
            initializer(*initargs)

    def run(self, fn: Callable[..., Any], chunk: list[tuple[Any, ...]], kwds: dict[str, Any]) -> list[Any]:
        return [fn(*args, **kwds) for args in chunk]


class AsyncResult:
    """A pending result, as ``multiprocessing.pool.AsyncResult``."""

    def __init__(self, compute: Callable[[], Any], callback: Any = None, error_callback: Any = None) -> None:
        self._done = threading.Event()
        self._value: Any = None
        self._error: BaseException | None = None

        def run() -> None:
            try:
                self._value = compute()
            except BaseException as e:  # noqa: BLE001 - delivered through get()
                self._error = e
            try:
                # Callbacks run before the result is ready, as in multiprocessing.
                if self._error is None and callback is not None:
                    callback(self._value)
                elif self._error is not None and error_callback is not None:
                    error_callback(self._error)
            finally:
                self._done.set()

        threading.Thread(target=run, daemon=True).start()

    def get(self, timeout: float | None = None) -> Any:
        if not self._done.wait(timeout):
            import multiprocessing

            raise multiprocessing.TimeoutError()
        if self._error is not None:
            raise self._error
        return self._value

    def wait(self, timeout: float | None = None) -> None:
        self._done.wait(timeout)

    def ready(self) -> bool:
        return self._done.is_set()

    def successful(self) -> bool:
        if not self.ready():
            raise ValueError("the result is not ready")
        return self._error is None


class Pool:
    """``Pool(processes=None, initializer=None, initargs=())``: `processes`
    workers (default: the cluster's CPUs)."""

    def __init__(
        self,
        processes: int | None = None,
        initializer: Callable[..., Any] | None = None,
        initargs: Iterable[Any] = (),
        maxtasksperchild: int | None = None,
        context: Any = None,
        ray_address: str | None = None,
        ray_remote_args: dict[str, Any] | None = None,
    ) -> None:
        if maxtasksperchild is not None:
            raise unsupported("Pool(maxtasksperchild=...)", why="workers are long-lived actors")
        if ray_address is not None:
            raise unsupported("Pool(ray_address=...)", use="ray.init(address) before creating the pool")
        n = processes or int(ch.cluster().resources()["cpus"]) or os.cpu_count() or 1
        if n < 1:
            raise ValueError("Number of processes must be at least 1")
        opts = {"replicas": n}
        if ray_remote_args and "num_cpus" in ray_remote_args:
            opts["cpus"] = float(ray_remote_args["num_cpus"])
        self._workers = _Worker.options(**opts).pool(initializer, tuple(initargs))
        self._n = n
        self._closed = False

    def _chunks(self, items: list[tuple[Any, ...]], chunksize: int | None) -> list[list[tuple[Any, ...]]]:
        size = chunksize or max(1, -(-len(items) // (4 * self._n)))
        return [items[i : i + size] for i in range(0, len(items), size)]

    def _submit(self, fn: Callable[..., Any], arglists: Iterable[tuple[Any, ...]], chunksize: int | None, kwds=None):
        if self._closed:
            raise ValueError("Pool not running")
        return [self._workers.run.remote(fn, c, kwds or {}) for c in self._chunks(list(arglists), chunksize)]

    # ---- the multiprocessing.Pool API ---------------------------------------------

    def apply(self, func: Callable[..., Any], args: tuple[Any, ...] = (), kwds: dict[str, Any] | None = None) -> Any:
        return self.apply_async(func, args, kwds).get()

    def apply_async(self, func, args=(), kwds=None, callback=None, error_callback=None) -> AsyncResult:
        [f] = self._submit(func, [tuple(args)], 1, kwds)
        return AsyncResult(lambda: f.result()[0], callback, error_callback)

    def map(self, func: Callable[[Any], Any], iterable: Iterable[Any], chunksize: int | None = None) -> list[Any]:
        return self.map_async(func, iterable, chunksize).get()

    def map_async(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> AsyncResult:
        futures = self._submit(func, [(x,) for x in iterable], chunksize)
        return AsyncResult(lambda: [y for f in futures for y in f.result()], callback, error_callback)

    def starmap(self, func: Callable[..., Any], iterable: Iterable[Iterable[Any]], chunksize: int | None = None) -> list[Any]:
        return self.starmap_async(func, iterable, chunksize).get()

    def starmap_async(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> AsyncResult:
        futures = self._submit(func, [tuple(a) for a in iterable], chunksize)
        return AsyncResult(lambda: [y for f in futures for y in f.result()], callback, error_callback)

    def imap(self, func: Callable[[Any], Any], iterable: Iterable[Any], chunksize: int = 1) -> Iterator[Any]:
        for f in self._submit(func, [(x,) for x in iterable], chunksize):
            yield from f.result()

    def imap_unordered(self, func: Callable[[Any], Any], iterable: Iterable[Any], chunksize: int = 1) -> Iterator[Any]:
        for f in ch.as_completed(self._submit(func, [(x,) for x in iterable], chunksize)):
            yield from f.result()

    def close(self) -> None:
        """No more work; what was submitted still finishes."""
        self._closed = True

    def terminate(self) -> None:
        self._closed = True
        self._workers.close()

    def join(self) -> None:
        if not self._closed:
            raise ValueError("Pool is still running")
        self._workers.close()

    def __enter__(self) -> Pool:
        return self

    def __exit__(self, *exc: object) -> None:
        self.terminate()
