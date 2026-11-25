"""``ray.experimental.get_object_locations`` on Chimaera."""

from __future__ import annotations

import time
from typing import Any

import chimaera as ch

from ._remote import ItemRef, PutRef
from .exceptions import GetTimeoutError


def get_object_locations(object_refs: list[Any], timeout_ms: int = -1) -> dict[Any, dict[str, Any]]:
    """Where each ref's value is stored, as Ray gives it: ``{ref: {"node_ids":
    [...], "object_size": n}}``, node ids being ``ray.nodes()``'s ``NodeID``.

    A task's or actor's result names the node it was stored at, and a
    ``put`` made here this node. A small value held inline in its ref has no
    node, and neither has a ``put`` made inside a task on another node (its
    size is given). Copies made in the background are not listed (Ray does
    not list spilled copies either). Waits for unfinished refs up to
    ``timeout_ms`` (-1: as long as they take), then raises GetTimeoutError.
    """
    refs = list(object_refs)
    futures = {r: (r._ref if isinstance(r, PutRef) else r) for r in refs if not isinstance(r, ItemRef)}
    pending = [f for f in futures.values() if not f.done()]
    if pending:
        deadline = None if timeout_ms < 0 else time.monotonic() + timeout_ms / 1000
        _, waiting = ch.wait(pending, timeout=None if deadline is None else max(0.0, deadline - time.monotonic()))
        if waiting:
            raise GetTimeoutError(f"Get timed out: {len(waiting)} object(s) not ready after {timeout_ms} ms.")
    found = ch.runs.object_locations(list(futures.values()))
    out: dict[Any, dict[str, Any]] = {}
    for r in refs:
        loc = found.get(futures[r], {"peers": [], "size": 0}) if r in futures else {"peers": [], "size": 0}
        out[r] = {"node_ids": loc["peers"], "object_size": loc["size"]}
    return out
