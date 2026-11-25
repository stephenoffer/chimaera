"""``ch.runs.object_locations``: which peers store finished values."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

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
        if got is not None:
            peers, size, inline = got
            out[f] = {"peers": list(peers), "size": size, "inline": inline}
        elif f._put is not None:  # a put this peer does not store: (run, object, inline, size, ...)
            out[f] = {"peers": [], "size": f._put[3], "inline": f._put[2] is not None}
        else:
            out[f] = {"peers": [], "size": 0, "inline": False}
    return out
