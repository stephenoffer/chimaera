"""``ch.runs.object_locations``: which peers store finished values; and
``ch.runs.objects``: what each peer's store holds."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from typing import Any

from . import _serde
from ._future import Future


def object_locations(futures: Iterable[Future[Any]]) -> dict[Future[Any], dict[str, Any]]:
    """For each future: ``{"peers": [peer ids, hex], "size": bytes, "inline": bool}``.

    A call's result names the peer it was stored at, with a retained value's
    copies, and a ``ch.put`` value made here this peer. A value held inline
    in its handle has no peers (``inline`` is true). A ``ch.put`` made inside
    a call on another peer is not located: its size, no peers (no record
    names its writer, and finding it would mean asking every peer). Copies
    made in the background (``CHIMAERA_OBJECT_REPLICAS``) and copies other
    peers fetched are not listed either. A failed or unfinished call has no
    peers and no size. Peer ids are those of ``ch.runs.cluster().nodes()``.
    """
    out: dict[Future[Any], dict[str, Any]] = {}
    for f in futures:
        got = f._rt.client.object_locations(f.key) if f.done() else None
        if got is None and f._put is not None:
            got = _put_files_here(f)
        if got is not None:
            peers, size, inline = got
            out[f] = {"peers": list(peers), "size": size, "inline": inline}
        elif f._put is not None:  # a put this peer does not store: (run, object, inline, size, ...)
            out[f] = {"peers": [], "size": f._put[3], "inline": f._put[2] is not None}
        else:
            out[f] = {"peers": [], "size": 0, "inline": False}
    return out


def _put_files_here(f: Future[Any]) -> tuple[list[str], int, bool] | None:
    """A put made here whose frame travels in its handle but names files (on
    Linux, a large buffer linked from the arena): this peer's while those files
    are in its store, with their bytes counted. None for anything else."""
    run, _, inline, _, files = f._put[:5]
    if not files:
        return None
    rt = f._rt
    try:
        stored = sum(os.path.getsize(_serde.object_path(rt.object_dir, run, fid)) for fid in files)
    except OSError:
        return None  # not (or no longer) in this peer's store
    return [rt.client.info["peer"].hex()], (len(inline) if inline is not None else 0) + stored, False


def objects() -> list[dict[str, Any]]:
    """What every live peer's object store holds, as ``chimaera objects
    --cluster --json`` shows it: one dict per peer, the connected peer's
    first (its ``addr`` is ``"this peer"``). Each gives the objects and bytes
    ``in_memory`` and ``on_disk`` (spilled, or copies of other peers' values),
    the same per run (``runs``), the ``largest`` ten with where each is, the
    arenas of live processes and the pre-zeroed reserve. A peer that could
    not answer has an ``error`` instead."""
    from ._runtime import Runtime

    return [json.loads(s) for s in Runtime.get().client.objects()]
