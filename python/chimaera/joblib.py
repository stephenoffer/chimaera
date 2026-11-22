"""joblib on Chimaera, as Ray's ``ray.util.joblib``.

Importing this module registers the ``"chimaera"`` backend::

    import chimaera.joblib
    from joblib import Parallel, delayed, parallel_config

    with parallel_config(backend="chimaera"):
        squares = Parallel()(delayed(pow)(i, 2) for i in range(1000))

Each batch of a ``Parallel`` loop is one Chimaera call, and joblib sizes
batches by how long they take, so many small tasks share a call. ``n_jobs``
sets how many batches are in flight at once (default and ``-1``: this
machine's CPU count); Chimaera places them on whichever peers have room. If a
task raises, joblib raises that error and the batches still running are
cancelled.
"""

from __future__ import annotations

import os
import queue
import threading
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
        self._lock = threading.Lock()
        # joblib dispatches its next batch from the completion callback. A
        # future's callbacks run on the thread that delivers outcomes, which
        # must never wait on a submission, so they run on this thread instead.
        self._callbacks: queue.SimpleQueue[tuple[Callable[[Any], Any], ch.Future[Any]] | None] | None = None

    def effective_n_jobs(self, n_jobs: int | None) -> int:
        if n_jobs == 0:
            raise ValueError("n_jobs == 0 in Parallel has no meaning")
        if n_jobs is None:
            return 1
        if n_jobs < 0:
            return max((os.cpu_count() or 1) + 1 + n_jobs, 1)
        return n_jobs

    def configure(self, n_jobs: int = 1, parallel: Any = None, **kwargs: Any) -> int:
        self.parallel = parallel
        if self._callbacks is None:
            self._callbacks = queue.SimpleQueue()
            threading.Thread(target=self._deliver, args=(self._callbacks,), name="joblib-callbacks", daemon=True).start()
        return self.effective_n_jobs(n_jobs)

    def _deliver(self, callbacks: queue.SimpleQueue[tuple[Callable[[Any], Any], ch.Future[Any]] | None]) -> None:
        while (item := callbacks.get()) is not None:
            callback, future = item
            callback(future)

    def submit(self, func: Callable[[], list[Any]], callback: Callable[[Any], Any] | None = None) -> ch.Future[Any]:
        future = _run_batch.remote(func)
        with self._lock:
            self._pending.add(future)
        future.add_done_callback(self._settled)
        if callback is not None:
            callbacks = self._callbacks
            assert callbacks is not None, "configure() runs before submit()"
            future.add_done_callback(lambda f: callbacks.put((callback, f)))
        return future

    def _settled(self, future: ch.Future[Any]) -> None:
        with self._lock:
            self._pending.discard(future)

    def retrieve_result_callback(self, future: ch.Future[Any]) -> list[Any]:
        return future.result()

    def abort_everything(self, ensure_ready: bool = True) -> None:
        """Cancel the batches still running."""
        with self._lock:
            pending, self._pending = self._pending, set()
        for future in pending:
            future.cancel()

    def terminate(self) -> None:
        self.abort_everything(ensure_ready=False)
        if self._callbacks is not None:
            self._callbacks.put(None)
            self._callbacks = None


register_parallel_backend("chimaera", ChimaeraBackend)
