"""Values held across runs. A ``ch.put`` handle unpickled where another run
is current (a second driver, a detached run, an actor of another run) holds
the value's files: they outlive the run that stored them, which would
otherwise delete them when it ends, until the handle is collected or this
process's session ends. The peer keeps these leases in its replicated shard
state, so a value held this way is reclaimed after its last holder lets go.

A held handle passed on keeps its lease while the copy may still be read: as
a function's argument (``Value``) until the call's outcome; as an actor's
argument, or pickled again (inside ``ch.handles``, nested deeper, returned),
until the session ends, as a run's own handle passed on keeps its value for
the run."""

from __future__ import annotations

import warnings
import weakref
from typing import Any

from . import _context
from ._errors import DataLost
from ._future import Future, _resolved_future

# An actor's process: the run that created it. The actor may keep a handle a
# call of another run passed it after that run ends.
own_run: bytes | None = None

# Each held handle's release, run when the handle is collected.
_releases: weakref.WeakKeyDictionary[Future[Any], Any] = weakref.WeakKeyDictionary()


def unpickle(
    run: bytes, object_id: bytes, inline: bytes | None, size: int, files: tuple[bytes, ...], durability: str
) -> Future[Any]:
    """Unpickle a ``ch.put`` handle (``Future.__reduce__``). The handle names
    the files the value is stored in, so holding them reads none of them."""
    f: Future[Any] = _resolved_future(run, object_id, inline, size, files, durability)
    if not files:
        return f  # the value travels in the handle
    rt = f._rt
    ctx = _context.current_task()
    if run == (own_run or (ctx.run if ctx is not None else rt.run)):
        return f  # its own run's value lives as long as the run
    try:
        rt.client.borrow(run, list(files))
    except FileNotFoundError as e:
        f._exc = DataLost(f"the ch.put value {object_id.hex()} of run {run.hex()} is gone: {e}")
        return f
    except (ConnectionError, ValueError, RuntimeError) as e:
        # The handle still reads the value while its run lasts.
        warnings.warn(f"the ch.put value {object_id.hex()} is not held past its run's end: {e}", stacklevel=2)
        return f
    release = weakref.finalize(f, rt.client.borrow, run, list(files), True)
    release.atexit = False  # the session's end lets go too
    _releases[f] = release
    return f


def keep(f: Future[Any]) -> None:
    """`f` is pickled again, and the copy may outlive it: its lease, if it
    has one, stays until this process's session ends."""
    release = _releases.pop(f, None)
    if release is not None:
        release.detach()


class Value:
    """Another run's ``ch.put`` value passed to a call: it unpickles to the
    value, read from that run's files (the caller holds them until the call's
    outcome, see ``_remote.build_call``)."""

    __slots__ = ("put",)

    def __init__(self, put: tuple[Any, ...]) -> None:
        self.put = put

    def __reduce__(self) -> tuple[Any, ...]:
        return (value, self.put)


def value(
    run: bytes, object_id: bytes, inline: bytes | None, size: int, files: tuple[bytes, ...], durability: str
) -> Any:
    return _resolved_future(run, object_id, inline, size, files, durability).result()
