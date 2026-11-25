"""The peer's resource ledger (Peer.Hold): holds taken all or none, renewed or
expiring, and launch tokens a peer honours only for the hold they name."""

from __future__ import annotations

import os
import time
from typing import Any

import pytest

import chimaera as ch
from chimaera._ledger import holds_on
from chimaera._remote import CALL_FUNCTION, submit_call
from chimaera._runtime import Runtime


def _hold(owner: str, generation: int, slots: list, ttl_s: float = 30.0, renew: bool = False) -> tuple[bool, str]:
    held, why, _, _ = Runtime.get().client.hold(b"", owner, generation, slots, int(ttl_s * 1000), renew)
    return held, why


def _mine(owner: str) -> list[str]:
    return [line for line in holds_on() if line.startswith(owner + " ")]


def _owner(name: str) -> str:
    return f"default/test-{os.getpid()}-{time.monotonic_ns()}/{name}"


@ch.remote(max_attempts=1)
def where_it_ran():
    return ch.context().node


def _launch(token: tuple[str, int, int]) -> ch.Future:
    """`where_it_ran` started with a hold's launch token."""
    eff = where_it_ran.options(cpus=0.25).policy()
    return submit_call(Runtime.get(), where_it_ran._fn, (), {}, kind=CALL_FUNCTION, eff=eff, policy_extra={"launch": token})


def _this_peer() -> Any:
    return next(n for n in ch.runs.cluster().nodes() if n.this_peer)


def test_a_hold_is_every_slot_or_none_until_released():
    a, b = _owner("a"), _owner("b")
    assert _hold(a, 1, [(0.25, 0.0, "")] * 2) == (True, "")
    [line] = _mine(a)
    assert line.startswith(f"{a} generation 1: 2 of 2 slot(s) untaken (0.5 CPUs, 0 device(s)); expires in")
    # The whole peer would fit an idle peer, but a holds part of it: b gets nothing, not the slot that fits.
    held, why = _hold(b, 1, [(0.25, 0.0, ""), (_this_peer().cpus - 0.25, 0.0, "")])
    assert not held and "free" in why and _mine(b) == [], why
    held, why = _hold(b, 1, [(0.25, 0.0, ""), (0.0, 1.0, "rocm")])
    assert (held, why) == (False, "needs 1 rocm device(s); this peer has 0") and _mine(b) == []
    _hold(a, 1, [])  # release
    assert _mine(a) == []


def test_a_hold_nobody_renews_expires_and_a_renewed_one_stays():
    gone, kept = _owner("gone"), _owner("kept")
    assert _hold(gone, 1, [(0.25, 0.0, "")], ttl_s=1.0)[0]
    assert _hold(kept, 1, [(0.25, 0.0, "")], ttl_s=1.0)[0]
    for _ in range(4):
        time.sleep(0.5)
        assert _hold(kept, 1, [(0.25, 0.0, "")], ttl_s=1.0, renew=True)[0]
    assert _mine(gone) == [] and len(_mine(kept)) == 1
    _hold(kept, 1, [])
    held, why = _hold(gone, 1, [(0.25, 0.0, "")], ttl_s=1.0, renew=True)
    assert not held and "nothing here to renew" in why, "a renewal never takes a new hold"


def test_a_launch_token_starts_once_and_only_for_its_generation():
    owner = _owner("gang")
    assert _hold(owner, 1, [(0.25, 0.0, "")])[0]
    assert _hold(owner, 2, [(0.25, 0.0, "")])[0], "a newer generation replaces the older one's hold"
    with pytest.raises(ch.InfrastructureFailure, match="is stale: generation 2 holds here"):
        _launch((owner, 1, 0)).result(timeout=60)
    assert _launch((owner, 2, 0)).result(timeout=60) == _this_peer().peer
    assert _mine(owner)[0].startswith(f"{owner} generation 2: 0 of 1 slot(s) untaken")
    with pytest.raises(ch.InfrastructureFailure, match="slot 0 was taken already"):
        _launch((owner, 2, 0)).result(timeout=60)
    assert not _hold(owner, 1, [(0.25, 0.0, "")])[0], "an older generation cannot come back"
    _hold(owner, 2, [])
    with pytest.raises(ch.InfrastructureFailure, match="holds nothing for it"):
        _launch((owner, 2, 0)).result(timeout=60)


def test_a_token_works_only_for_its_own_tenants_calls():
    theirs = f"other-tenant/test-{os.getpid()}/gang"
    assert _hold(theirs, 1, [(0.25, 0.0, "")])[0]  # this driver's credential is the cluster's own
    with pytest.raises(ch.InfrastructureFailure, match="is not for the calls of tenant default"):
        _launch((theirs, 1, 0)).result(timeout=60)
    assert _mine(theirs)[0].startswith(f"{theirs} generation 1: 1 of 1 slot(s) untaken"), "still theirs"
    _hold(theirs, 1, [])
