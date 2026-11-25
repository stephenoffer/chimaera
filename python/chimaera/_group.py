"""``ch.TaskGroup``: a block that owns the calls added to it."""

from __future__ import annotations

from types import TracebackType
from typing import Any, TypeVar

from ._completions import Completions
from ._future import Future, _Part, start_pending, waiting

T = TypeVar("T")


class TaskGroup:
    """A block that owns the calls you add to it, as ``asyncio.TaskGroup``
    owns its tasks. Leaving the block (``with`` or ``async with``) waits for
    every call added, and there the first call to fail cancels those still
    running. Failures are looked for only then: unlike ``asyncio.TaskGroup``,
    a call that fails while the block's code still runs stops neither that
    code nor the other calls. The errors of the calls that failed by
    themselves (not those cancelled, nor those that never ran because a call
    of the group they take as an argument failed), and the block's own error
    if it raised one, are then raised together as an ``ExceptionGroup``.
    Calls that finished keep their results.

    Inside a call, the wait (either form) lends the call's CPU as
    ``result()`` does, so groups nested in calls never hold a worker while
    they wait."""

    def __init__(self) -> None:
        self._futures: list[Future[Any]] = []
        self._keys: set[bytes] = set()  # the calls', once the block ends
        self._errors: list[BaseException] = []
        self._state = "new"  # then "open", then "exiting"
        self._cancelled = False

    def add(self, future: Future[T]) -> Future[T]:
        """Own `future`'s call: leaving the block waits for it, and a failure
        of another call cancels it. Returns `future`."""
        if not isinstance(future, Future):
            raise TypeError(f"TaskGroup.add takes a future, not {type(future).__name__}")
        if self._state != "open":
            raise RuntimeError("TaskGroup.add works inside the group's block only")
        self._futures.append(future)
        return future

    def __enter__(self) -> TaskGroup:
        if self._state != "new":
            raise RuntimeError("a TaskGroup is used for one block")
        self._state = "open"
        return self

    def __exit__(self, et: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None) -> None:
        if not self._exiting(exc):
            return
        finished = self._completions()
        try:
            with waiting([f.key for f in self._futures]):
                for _ in self._futures:
                    self._finished(finished.take(None))
        except BaseException:  # interrupted while waiting: the calls stop too
            self._cancel()
            raise
        finally:
            finished.close()
        self._raise()

    async def __aenter__(self) -> TaskGroup:
        return self.__enter__()

    async def __aexit__(
        self, et: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if not self._exiting(exc):
            return
        finished = self._completions()
        try:
            with waiting([f.key for f in self._futures]):  # an async call's group lends its CPU too
                for _ in self._futures:
                    self._finished(await finished.take_async(None))
        except BaseException:  # cancelled while waiting: the calls stop too
            self._cancel()
            raise
        finally:
            finished.close()
        self._raise()

    def _exiting(self, exc: BaseException | None) -> bool:
        """Starts leaving the block; False if the calls are not waited for
        (Ctrl-C, an exit, a cancelled coroutine): they are cancelled."""
        self._state = "exiting"
        if exc is not None:
            self._cancel()
            if not isinstance(exc, Exception):
                return False
            self._errors.append(exc)
        return True

    def _completions(self) -> Completions:
        self._keys = {f.key for f in self._futures}
        if self._futures:
            start_pending()
            self._futures[0]._rt.client.flush()
        return Completions(self._futures)

    def _finished(self, f: Future[Any]) -> None:
        error = _failure(f, self._keys)
        if error is not None:
            self._errors.append(error)
            self._cancel()

    def _cancel(self) -> None:
        if not self._cancelled:
            self._cancelled = True
            for f in self._futures:
                if not f.done():  # a finished call keeps its outcome
                    f.cancel()

    def _raise(self) -> None:
        if self._errors:
            errors, self._errors = self._errors, []
            raise BaseExceptionGroup(f"{len(errors)} error{'s' * (len(errors) != 1)} in a TaskGroup", errors) from None


def _failure(f: Future[Any], group: set[bytes]) -> BaseException | None:
    """The error `f`'s call raised by itself: None if it succeeded, was
    cancelled (by its group, or by its owner), or never ran because a call of
    its `group` (their keys) that it took as an argument failed or was
    cancelled: that call reports its own error. A deadline or a budget it ran
    out of is a failure of its own."""
    call = f._parent if isinstance(f, _Part) else f
    out = call._outcome
    if out is not None and out[0] == "err" and out[1] == "cancelled":
        return None
    if out is not None and out[0] == "err" and out[1] == "causal" and out[6] in group:
        call._seen = True  # the error its argument's call raises answers for it
        return None
    if f is call and call._exc is None and out is not None and out[0] == "ok":
        return None  # its value is not fetched here
    return f.exception(0)  # a part of a call that succeeded fails only if its own value is an error
