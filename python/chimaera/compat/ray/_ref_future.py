"""``ObjectRef.future()``: a ``concurrent.futures.Future`` of a ref's value,
as Ray gives, for ``concurrent.futures.wait`` and ``asyncio.wrap_future``
(which ``ObjectRef.as_future()`` is).

The shim hands out Chimaera's own futures as Ray's ObjectRefs (and its
PutRef and ItemRef for puts and generator items); `attach` gives those types
the method. The value is read through ``ray.get`` (so errors are Ray's), on a
small pool once the ref is done, not on the runtime thread that completes
futures, where a large value's read would hold up every other future.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from typing import Any, Callable

_pool: concurrent.futures.ThreadPoolExecutor | None = None
_lock = threading.Lock()


def _readers() -> concurrent.futures.ThreadPoolExecutor:
    global _pool
    with _lock:
        if _pool is None:
            _pool = concurrent.futures.ThreadPoolExecutor(4, thread_name_prefix="chimaera-ray-future")
        return _pool


def attach(get_one: Callable[[Any, float | None], Any], types: tuple[type, ...]) -> None:
    """Give each of `types` a ``future()`` that settles with ``get_one(ref, None)``, and ``as_future()``."""

    def future(self: Any) -> concurrent.futures.Future[Any]:
        """A ``concurrent.futures.Future`` of this ref's value (Ray's ``ObjectRef.future()``)."""
        out: concurrent.futures.Future[Any] = concurrent.futures.Future()

        def settle() -> None:
            try:
                out.set_result(get_one(self, None))
            except BaseException as e:  # noqa: BLE001 -- the caller sees it through `out`
                out.set_exception(e)

        when_done = getattr(self, "add_done_callback", None)
        if when_done is None:  # a put or a generator item: resolved already
            _readers().submit(settle)
        else:
            when_done(lambda _: _readers().submit(settle))
        return out

    def as_future(self: Any) -> asyncio.Future[Any]:
        """An ``asyncio.Future`` of this ref's value (Ray's ``ObjectRef.as_future()``)."""
        return asyncio.wrap_future(future(self))

    for t in types:
        setattr(t, "future", future)  # noqa: B010 -- a method added to types the shim does not define
        setattr(t, "as_future", as_future)  # noqa: B010
