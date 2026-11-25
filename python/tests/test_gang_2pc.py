"""A gang generation is decided once, in the replicated store, with its ranks'
call keys: a supervisor's retry carries on with the generation it decided,
each rank starts once, and the ranks of a generation given up leave its
start barrier at once."""

import os
import signal
import subprocess
import tempfile
import time
import uuid

import pytest

import chimaera as ch
from chimaera import _gang
from chimaera._native import vshard_of
from test_cluster import Cluster, lines


def test_a_generation_is_decided_once_and_replaces_only_a_lost_predecessors_marker():
    gang = uuid.uuid4().hex[:16]
    store = ch.coord.Store(f"gang/{gang}")
    assert _gang._record(store, 1, ["p0", "p1"], 30.0, keys=[b"a" * 32, b"b" * 32]) is None
    other = _gang._record(store, 1, ["p2", "p3"], 30.0, keys=[b"c" * 32, b"d" * 32])
    assert other is not None and other["ranks"] == ["p0", "p1"], "the second writer adopts the first decision"
    assert other["keys"] == [(b"a" * 32).hex(), (b"b" * 32).hex()]
    _gang._abandon(store, gang, 1, "a rank gave up")  # its marker sits in generation 2's slot
    assert _gang._record(store, 2, ["p4"], 30.0) is None
    assert store.get("2/record")["ranks"] == ["p4"]


def test_rank_keys_stay_with_their_supervisor_and_differ_by_generation():
    run, task = os.urandom(16), os.urandom(16)
    keys = {_gang._rank_key(run, task, g, r) for g in (1, 2) for r in range(4)}
    assert len(keys) == 8
    assert _gang._rank_key(run, task, 1, 0) == _gang._rank_key(run, task, 1, 0), "the same in a retry"
    assert all(k[:16] == run and vshard_of(k[16:]) == vshard_of(task) for k in keys)


@ch.remote(max_attempts=1)
def meet(gang, world):
    return _gang._rendezvous(gang, 1, 0, world)


def test_ranks_of_a_given_up_generation_leave_the_start_barrier_at_once():
    gang = uuid.uuid4().hex[:16]
    store = ch.coord.Store(f"gang/{gang}")
    _gang._record(store, 1, [None, None], 300.0)  # rank 1 never comes
    waiting = meet.remote(gang, 2)
    deadline = time.monotonic() + 60
    while store.get("1/at/0") is None and time.monotonic() < deadline:
        time.sleep(0.05)
    t = time.monotonic()
    _gang._abandon(store, gang, 1, "a rank was lost")
    with pytest.raises(ch.dist.GangFailed, match="a rank was lost"):
        waiting.result(timeout=60)
    assert time.monotonic() - t < 10, "not after the 300 s rendezvous deadline"


def test_a_supervisor_lost_with_its_peer_carries_on_with_the_generation_it_decided():
    c = Cluster(3, labels={i: f"slot=s{i}" for i in range(3)})
    marks = tempfile.mkdtemp(prefix="chg", dir="/tmp")
    ran, go = os.path.join(marks, "ran"), os.path.join(marks, "go")
    try:
        out = c.run(
            0,
            f"""
import os, time
import chimaera as ch

@ch.remote
def work(x):
    with open({ran!r}, "a") as f:
        f.write(os.environ["RANK"] + " " + os.environ["CHIMAERA_GANG_GENERATION"] + "\\n")
    while not os.path.exists({go!r}):
        time.sleep(0.05)
    return int(os.environ["RANK"]) * 10 + x

run = ch.init(detach=True)
g = work.options(ranks=2, placement=ch.dist.Placement(require={{"slot": "s1"}})).gang(1)
deadline = time.monotonic() + 120
while (not os.path.exists({ran!r}) or len(open({ran!r}).readlines()) < 2) and time.monotonic() < deadline:
    time.sleep(0.05)
print("RUN", run)
print("GANG", g.key.hex())
""",
        )
        run_id, gang_key = lines(out, "RUN")[0], lines(out, "GANG")[0]
        # The supervisor's machine is lost: its peer (where its driver's peer
        # is) and that peer's workers, which a restarted peer would otherwise
        # find still running.
        found = subprocess.run(["pgrep", "-P", str(c.procs[0].pid)], capture_output=True, text=True)
        c.signal(0, signal.SIGKILL)
        for pid in map(int, found.stdout.split()):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        c.start(0)
        c.wait_ready([0])
        # The supervisor's first attempt counts as lost, and its retry has had
        # time to end the ranks had it started its generation over: only then
        # may they finish.
        out = c.run(
            1,
            f"""
import time
import chimaera as ch
from chimaera._runtime import Runtime
ch.runs.attach({run_id!r})
deadline = time.monotonic() + 120
while not Runtime.get().client.explain(bytes.fromhex({gang_key!r}))["failures"] and time.monotonic() < deadline:
    time.sleep(0.1)
print("LOST", Runtime.get().client.explain(bytes.fromhex({gang_key!r}))["failures"])
""",
        )
        assert "retired" in lines(out, "LOST")[0], out.stdout + out.stderr[-2000:]  # its first attempt
        time.sleep(5)
        open(go, "w").close()
        out = c.run(
            1,
            f"""
import chimaera as ch
run = ch.runs.attach({run_id!r})
run.wait(timeout=180)
print("GANG", [v for k, v, e in run.outcomes() if {gang_key!r}.endswith(k) and e is None])
""",
            timeout=240,
        )
        assert lines(out, "GANG") == ["[[1, 11]]"], out.stdout + out.stderr[-3000:]
        assert sorted(open(ran).read().split("\n")[:-1]) == ["0 1", "1 1"], "each rank ran once, in generation 1"
    finally:
        c.close()
