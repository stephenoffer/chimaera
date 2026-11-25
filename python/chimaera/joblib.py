"""joblib on Chimaera, as Ray's ``ray.util.joblib``.

Importing this module registers the ``"chimaera"`` backend::

    import chimaera.joblib
    from joblib import Parallel, delayed, parallel_config

    with parallel_config(backend="chimaera"):
        squares = Parallel()(delayed(pow)(i, 2) for i in range(1000))

Each batch of a ``Parallel`` loop is one Chimaera call, and joblib sizes
batches by how long they take, so many small tasks share a call. ``n_jobs``
sets how many batches run at once (default and ``-1``: the CPUs of every live
peer, ``-2`` one fewer, and so on); the rest wait here, and Chimaera places
each on whichever peer has room. If a task raises, joblib raises that error
and the batches still running are cancelled.
"""

from __future__ import annotations

import concurrent.futures
import functools
import queue
import threading
from collections import deque
from collections.abc import Callable
from typing import Any

try:
    from joblib._parallel_backends import AutoBatchingMixin, ParallelBackendBase
    from joblib.parallel import register_parallel_backend
except ImportError as e:  # pragma: no cover - depends on the environment
    raise ImportError("chimaera.joblib needs joblib: pip install joblib") from e

import chimaera as ch

__all__ = ["ChimaeraBackend"]


@ch.remote
def _run_batch(batch: Callable[[], list[Any]]) -> list[Any]:
    """Run one joblib batch (a ``BatchedCalls``) and return its results."""
    return batch()


class ChimaeraBackend(AutoBatchingMixin, ParallelBackendBase):
    """A joblib backend that runs each batch as a Chimaera call."""

    default_n_jobs = -1
    supports_retrieve_callback = True

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._pending: set[ch.Future[Any]] = set()
        # Batches past n_jobs wait here, unsent: joblib dispatches up to
        # pre_dispatch (2 * n_jobs) of them, and a roomy cluster would run all.
        self._waiting: deque[tuple[Callable[[], list[Any]], Callable[[Any], Any] | None]] = deque()
        self._running = 0  # batches sent and not yet settled
        self._lock = threading.Lock()
        # joblib dispatches its next batch from the completion callback. A
        # future's callbacks run on the thread that delivers outcomes, which
        # must never wait on a submission, so they run on this thread instead.
        self._callbacks: queue.SimpleQueue[tuple[Callable[[Any], Any], Any] | None] | None = None

    def effective_n_jobs(self, n_jobs: int | None) -> int:
        if n_jobs == 0:
            raise ValueError("n_jobs == 0 in Parallel has no meaning")
        if n_jobs is None:
            return 1
        if n_jobs < 0:
            cpus = sum(n.cpus for n in ch.runs.cluster().nodes() if n.alive and not n.draining)
            return max(int(cpus) + 1 + n_jobs, 1)
        return n_jobs

    def configure(self, n_jobs: int = 1, parallel: Any = None, **kwargs: Any) -> int:
        self.parallel = parallel
        if self._callbacks is None:
            self._callbacks = queue.SimpleQueue()
            threading.Thread(target=self._deliver, args=(self._callbacks,), name="joblib-callbacks", daemon=True).start()
        self._n_jobs = self.effective_n_jobs(n_jobs)
        return self._n_jobs

    def _deliver(self, callbacks: queue.SimpleQueue[tuple[Callable[[Any], Any], Any] | None]) -> None:
        while (item := callbacks.get()) is not None:
            callback, future = item
            callback(future)

    def submit(
        self, func: Callable[[], list[Any]], callback: Callable[[Any], Any] | None = None
    ) -> ch.Future[Any] | None:
        """Send the batch, or keep it until one of the n_jobs running settles.
        joblib reads results only through `callback` (supports_retrieve_callback),
        so a kept batch returns None."""
        with self._lock:
            if self._running >= self._n_jobs:
                self._waiting.append((func, callback))
                return None
            self._running += 1
        return self._send(func, callback)

    def _send(self, func: Callable[[], list[Any]], callback: Callable[[Any], Any] | None) -> Any:
        callbacks = self._callbacks
        assert callbacks is not None, "configure() runs before submit()"
        settled = functools.partial(self._settled, callback)
        try:
            future = _run_batch.remote(func)
        except Exception as e:  # noqa: BLE001 - joblib hears of it as of a batch that failed
            # Raised on the callback thread, which sends the next batches too,
            # it would end that thread, and Parallel would wait forever.
            failed: concurrent.futures.Future[Any] = concurrent.futures.Future()
            failed.set_exception(e)
            callbacks.put((settled, failed))
            return failed
        with self._lock:
            self._pending.add(future)
        future.add_done_callback(lambda f: callbacks.put((settled, f)))
        return future

    def _settled(self, callback: Callable[[Any], Any] | None, future: Any) -> None:
        """A batch settled (on the callback thread): its slot goes to the next
        waiting batch, then joblib hears of it."""
        with self._lock:
            self._pending.discard(future)
            following = self._waiting.popleft() if self._waiting else None
            if following is None:
                self._running -= 1
        if following is not None:
            self._send(*following)
        if callback is not None:
            callback(future)

    def retrieve_result_callback(self, future: Any) -> list[Any]:  # a ch.Future, or one _send failed
        return future.result()

    def abort_everything(self, ensure_ready: bool = True) -> None:
        """Cancel the batches still running, and drop those waiting."""
        with self._lock:
            pending, self._pending = self._pending, set()
            self._waiting.clear()
        for future in pending:
            future.cancel()

    def terminate(self) -> None:
        self.abort_everything(ensure_ready=False)
        if self._callbacks is not None:
            self._callbacks.put(None)
            self._callbacks = None


register_parallel_backend("chimaera", ChimaeraBackend)
