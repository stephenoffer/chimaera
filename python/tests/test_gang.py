"""Gangs: fn.options(ranks=n).gang(...) — rendezvous env, collectives, failure as one."""

from __future__ import annotations

import json
import os
import pickle
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types

import pytest

import chimaera as ch


@ch.remote(cpus=0.5)
def where_am_i(tag):
    g = ch.context().gang
    env = {k: os.environ[k] for k in ("RANK", "WORLD_SIZE", "LOCAL_RANK", "LOCAL_WORLD_SIZE", "MASTER_ADDR")}
    return tag, g.rank, g.size, g.generation, env, int(os.environ["MASTER_PORT"]) > 0


def test_every_rank_gets_the_rendezvous_env_and_results_come_in_rank_order():
    results = where_am_i.options(ranks=3).gang("x").result(timeout=120)
    assert [r[1] for r in results] == [0, 1, 2]
    for tag, rank, size, generation, env, has_port in results:
        assert (tag, size, generation, has_port) == ("x", 3, 1, True)
        assert env["RANK"] == str(rank) and env["WORLD_SIZE"] == "3"
        assert env["LOCAL_RANK"] == str(rank) and env["LOCAL_WORLD_SIZE"] == "3"  # one peer: all local
    assert len({r[4]["MASTER_ADDR"] for r in results}) == 1


@ch.remote(cpus=0.5)
def no_rank_env(_):
    return os.environ.get("RANK"), ch.context().gang


def test_a_reused_worker_does_not_keep_a_ranks_env():
    where_am_i.options(ranks=2).gang("y").result(timeout=120)
    assert all(r == (None, None) for r in no_rank_env.map(range(8)))


@ch.remote(cpus=0.5)
def collectives():
    g = ch.context().gang
    everyone = g.all_gather(g.rank * 10)
    word = g.broadcast("hello" if g.rank == 1 else None, src=1)
    g.barrier()
    return everyone, word


def test_small_object_collectives_without_torch():
    assert collectives.options(ranks=3).gang().result(timeout=120) == [([0, 10, 20], "hello")] * 3


@ch.remote(cpus=0.5)
def lose_a_rank_once():
    g = ch.context().gang
    if g.generation == 1 and g.rank == 1:
        os._exit(1)  # this rank's worker dies
    g.barrier()  # rank 0 of generation 1 would wait here forever for rank 1
    return g.rank, g.generation


def test_a_lost_rank_restarts_the_whole_gang_as_a_new_generation():
    gang = lose_a_rank_once.options(ranks=2).gang()
    assert gang.result(timeout=180) == [(0, 2), (1, 2)]
    assert gang.generation == 2


@ch.remote(cpus=0.5)
def rank_two_raises():
    if ch.context().gang.rank == 2:
        raise ValueError("bad shard")
    ch.context().gang.barrier()


def test_a_rank_error_fails_the_gang_with_it():
    with pytest.raises(ValueError, match="bad shard"):
        rank_two_raises.options(ranks=3).gang().result(timeout=120)


@ch.remote(cpus=0.5, max_attempts=2)
def always_lost():
    os._exit(1)


def test_a_gang_that_keeps_losing_ranks_gives_up_after_max_attempts():
    gang = always_lost.options(ranks=2).gang()
    with pytest.raises(ch.errors.GangFailed, match="generation 2 lost a rank"):
        gang.result(timeout=180)


def test_gang_needs_ranks():
    with pytest.raises(TypeError, match="ranks=n"):
        where_am_i.gang("z")


@ch.remote(cpus=0.5)
class Shard:
    def __init__(self, scale):
        g = ch.context().gang  # the rank is known while the object is built
        self.rank, self.size, self.scale = g.rank, g.size, scale
        self.env_rank = os.environ["RANK"]

    def describe(self):
        return self.rank, self.size, self.env_rank, ch.context().gang.generation

    def total(self, x):
        g = ch.context().gang
        return sum(g.all_gather(x * self.scale * (self.rank + 1)))


def test_an_actor_gang_builds_ranks_together_and_fans_calls_out():
    with Shard.options(ranks=3).gang(10) as g:
        assert len(g) == 3
        assert g.describe.remote().result(timeout=120) == [(r, 3, str(r), 1) for r in range(3)]
        assert g.total.remote(1).result(timeout=120) == [60, 60, 60]  # 10 * (1 + 2 + 3) on every rank
        assert g[2].describe.remote().result(timeout=60)[0] == 2
        g.resize(2)
        assert g.describe.remote().result(timeout=120) == [(0, 2, "0", 2), (1, 2, "1", 2)]
    # Closed after: an actor left holding CPUs keeps a gang that needs the whole peer from being admitted.
    with Shard.options(ranks=1).gang(1) as one, pytest.raises(AttributeError, match="no method 'nope'"):
        one.nope


def test_an_actor_gang_that_fails_to_build_holds_nothing(monkeypatch):
    from chimaera import _remote
    from chimaera._ledger import holds_on

    real, built = _remote._create_actor, []

    def create(*args, **kwargs):
        if built:
            raise RuntimeError("rank 1's host could not be created")
        built.append(real(*args, **kwargs))
        return built[0]

    monkeypatch.setattr(_remote, "_create_actor", create)
    with pytest.raises(RuntimeError, match="rank 1's host"):
        Shard.options(ranks=2).gang(1)
    assert [h for h in holds_on() if "/gang/" in h] == [], "rank 1's slot is free, and rank 0's host ended"
    assert isinstance(built[0].call.remote("describe", (), {}).exception(timeout=60), ch.ActorDied)


def _peer_cpus() -> float:
    return sum(n.cpus for n in ch.runs.cluster().nodes() if n.alive)


def test_an_elastic_gang_takes_as_many_ranks_as_the_peers_hold():
    shape = _peer_cpus() / 3  # three of these fit
    gang = where_am_i.options(ranks=ch.auto(min=1, max=8), cpus=shape).gang("e")
    assert [r[1] for r in gang.result(timeout=120)] == [0, 1, 2]
    assert gang.size == 3 and "hold 3 of this shape" in gang.decisions[0], gang.decisions
    started = where_am_i.options(ranks=ch.auto(2, min=1, max=8), cpus=shape).gang("e")
    assert len(started.result(timeout=120)) == 2, "the first generation starts at the initial size"


@pytest.mark.parametrize(
    "bad, match",
    [
        (ch.auto(min=1), "needs max="),
        (ch.auto(min=3, max=2), "min <= initial <= max"),
        (ch.auto(5, min=1, max=4), "min <= initial <= max"),
        (ch.auto(min=0, max=4), r"ranks \(ch.auto min\): expected an integer >= 1"),
    ],
)
def test_elastic_ranks_are_checked(bad, match):
    with pytest.raises(ValueError, match=match):
        where_am_i.options(ranks=bad).gang("x")


def _die_unless(path):
    if not os.path.exists(path):
        open(path, "w").close()
        os._exit(1)  # this rank's worker dies while loading its arguments
    return path


class DiesOnLoad:
    """An argument that kills the first worker to unpickle it: before the rendezvous."""

    def __init__(self, path):
        self.path = path

    def __reduce__(self):
        return (_die_unless, (self.path,))


@ch.remote(cpus=0.5)
def meet(_arg):
    g = ch.context().gang
    g.barrier()
    return g.rank, g.generation


def test_a_rank_dying_before_the_rendezvous_fails_that_generation(tmp_path):
    import time

    start = time.monotonic()
    gang = meet.options(ranks=2).gang(DiesOnLoad(str(tmp_path / "died")))
    assert gang.result(timeout=120) == [(0, 2), (1, 2)]
    assert time.monotonic() - start < 60, "the other rank must not wait out the rendezvous timeout"


@ch.remote(cpus=0.5)
def sums():
    g = ch.context().gang
    return g.all_reduce(g.rank + 1), g.all_reduce([g.rank], op=lambda a, b: a + b)


def test_all_reduce_combines_every_ranks_value():
    assert sums.options(ranks=3).gang().result(timeout=120) == [(6, [0, 1, 2])] * 3


@ch.remote(cpus=0.5)
def gather_arrays(mib):
    import numpy as np

    from chimaera import _serde
    from chimaera._runtime import Runtime
    from chimaera._store import Store

    g = ch.context().gang
    everyone = g.all_gather(np.full(mib << 18, g.rank, dtype=np.float32))  # `mib` MiB from each rank
    handle = Store(f"gang/{g.id}").get(f"{g.generation}/c1/all_gather/{g.rank}")
    run_dir = _serde.run_dir(Runtime.get().object_dir, handle.put[0])
    kept = [f.hex() for f in handle.put[4] if os.path.exists(os.path.join(run_dir, f.hex()))]
    seed = g.broadcast(np.arange(1 << 19, dtype=np.float32) if g.rank == 2 else None, src=2)  # 2 MiB
    total = g.all_reduce(np.ones(1 << 19, dtype=np.float32))
    small = g.all_gather(g.rank)
    seen = [(a.nbytes, float(a.min()), float(a.max())) for a in everyone]
    return g.id, seen, float(seed[-1]), float(total.max()), small, len(handle.put[4]), kept


def test_a_large_all_gather_stays_out_of_the_store_and_comes_back_in_rank_order():
    out = gather_arrays.options(ranks=4).gang(50).result(timeout=180)
    for _, seen, seed, total, small, files, kept in out:
        assert seen == [(50 << 20, float(r), float(r)) for r in range(4)]
        assert (seed, total, small) == ((1 << 19) - 1, 4.0, [0, 1, 2, 3])
        assert files and kept == [], "each sender frees its value once every rank has read it"
    from chimaera._gang import _Stored
    from chimaera._store import Store

    store = Store(f"gang/{out[0][0]}")
    for r in range(4):
        handle = store.get(f"1/c1/all_gather/{r}")
        assert type(handle) is _Stored and len(pickle.dumps(handle)) < 1024, "the store holds a handle, not the array"
        assert store.get(f"1/c4/all_gather/{r}") == r, "a small value is the store's own"


GATHER_ACROSS_PEERS = """
import numpy as np
import chimaera as ch

@ch.remote(cpus=1.5)
def gather():
    g = ch.context().gang
    everyone = g.all_gather(np.full(8 << 18, g.rank, dtype=np.float32))  # 8 MiB from each rank
    return ch.context().node, [(float(a.min()), float(a.max())) for a in everyone]

out = gather.options(ranks=4).gang().result(timeout=120)
print("RESULT", len({node for node, _ in out}), all(seen == [(r, r) for r in range(4)] for _, seen in out), flush=True)
"""


def test_a_large_all_gather_reads_each_value_from_the_peer_of_the_rank_that_sent_it():
    from test_cluster import Cluster

    cluster = Cluster(2)  # 4 CPUs each: two ranks per peer
    try:
        out, err = cluster.driver(0, GATHER_ACROSS_PEERS).communicate(timeout=240)
        assert "RESULT 2 True" in out, (out, err[-3000:])
    finally:
        cluster.close()


@ch.remote(cpus=0.5)
def diverge(how):
    import time

    g = ch.context().gang
    if how in ("late", "timeout") and g.rank == 1:
        time.sleep(600)  # rank 1 never reaches the barrier the others wait in
    if how == "paths":
        return g.all_gather(1) if g.rank == 0 else g.broadcast(1)  # two collectives as #1: both wait
    g.barrier(timeout=3 if how == "timeout" else None)


def _until(gang, text):
    import time

    deadline = time.monotonic() + 60
    while text not in (summary := gang.explain()["summary"]) and time.monotonic() < deadline:
        time.sleep(0.2)
    return summary


def test_explain_names_the_rank_that_holds_a_gang_up():
    late = diverge.options(ranks=3).gang("late")
    try:
        summary = _until(late, "wait in #1")
        assert summary.startswith("rank 1 last entered #0 (rendezvous") and "ranks 0, 2 wait in #1 (barrier)" in summary
        ranks = late.explain()["ranks"]
        assert [r["collective"] for r in ranks] == [1, 0, 1] and ranks[0]["op"] == "barrier"
    finally:
        late.cancel()
    paths = diverge.options(ranks=2).gang("paths")
    try:
        summary = _until(paths, "different collectives")
        assert summary == "the ranks call different collectives as #1: rank 0 all_gather, rank 1 broadcast"
        with pytest.raises(TimeoutError, match="different collectives"):
            paths.result(timeout=2)  # they wait for each other rather than mix their data
    finally:
        paths.cancel()


def test_a_collective_timeout_names_the_rank_it_waited_for():
    with pytest.raises(TimeoutError, match=r"barrier #1 .* timed out: rank 1 last entered #0 \(rendezvous"):
        diverge.options(ranks=2).gang("timeout").result(timeout=120)


@ch.remote(cpus=0.5)
def stall_once(how):
    g = ch.context().gang
    if g.generation == 1 and g.rank == 1:
        if how == "stop":
            os.kill(os.getpid(), signal.SIGSTOP)  # this rank's process stops before the collective: it never ends
        else:
            time.sleep(600)
    g.barrier()  # no timeout= of its own: the gang's collective_timeout bounds the wait
    return g.rank, g.generation


def test_a_rank_stopped_before_a_collective_reforms_the_gang_within_its_collective_timeout():
    start = time.monotonic()
    gang = stall_once.options(ranks=3, collective_timeout=3).gang("stop")
    assert gang.result(timeout=120) == [(0, 2), (1, 2), (2, 2)]
    assert gang.generation == 2
    assert time.monotonic() - start < 60, "the others gave generation 1 up at the deadline and it reformed"


STALLED = r"generation 1 is lost: barrier #1 of .* waited 2 s \(collective_timeout\): rank 1 last entered #0 \(rendezvous"


def test_a_collective_that_outlasts_its_timeout_on_the_last_attempt_fails_the_gang_naming_the_rank():
    start = time.monotonic()
    with pytest.raises(ch.errors.GangFailed, match=STALLED):
        stall_once.options(ranks=3, collective_timeout=2, max_attempts=1).gang("sleep").result(timeout=120)
    assert time.monotonic() - start < 60


@ch.remote(cpus=0.5)
class Stepper:
    def step(self, late):
        g = ch.context().gang
        if g.rank == 1:
            time.sleep(late)  # past the others' collective_timeout
        g.barrier()
        return g.rank, g.generation


def test_an_actor_gang_whose_collective_stalls_fails_every_rank_with_one_error_until_restart():
    with Stepper.options(ranks=3, collective_timeout=2).gang() as g:
        start = time.monotonic()
        calls = [g[r].step.remote(5) for r in range(3)]
        errors = []
        for f in calls:
            with pytest.raises(ch.errors.GangFailed, match=STALLED) as failed:
                f.result(timeout=60)
            errors.append(str(failed.value))
        assert time.monotonic() - start < 30
        assert len(set(errors)) == 1, f"one reason on every rank, the late one's too: {errors}"
        g.restart()
        assert g.step.remote(0).result(timeout=60) == [(0, 2), (1, 2), (2, 2)]


@ch.remote(cpus=0.5)
class Late:
    def enter(self, late, other):
        g = ch.context().gang
        time.sleep(late[g.rank])
        start = time.monotonic()
        try:
            g.all_gather(g.rank) if other and g.rank == 2 else g.barrier()
        except ch.errors.GangFailed:
            return time.monotonic() - start
        return None


@pytest.mark.parametrize("other", [False, True], ids=["same", "other"])
def test_a_rank_already_waiting_raises_soon_after_another_gives_the_generation_up(other):
    # Rank 1 is stuck; rank 0 gives generation 1 up at 8 s. Rank 2 came in at 5 s, to
    # the same collective or (`other`) to a different one: it must not wait its own 8 s.
    with Late.options(ranks=3, collective_timeout=8).gang() as g:
        waited = g.enter.remote([0, 10, 5], other).result(timeout=90)
    assert 7.5 < waited[0] < 12 and waited[1] < 1, waited
    assert waited[2] < 6, f"rank 2 raised {waited[2]:.1f} s after it came in, not about 3 s"


@pytest.mark.parametrize("explicit", [False, True], ids=["collective_timeout", "timeout"])
def test_a_rank_that_arrives_after_a_collective_was_given_up_raises_the_same_error(explicit):
    from chimaera._gang import Group, _record
    from chimaera._store import Store

    gang = os.urandom(8).hex()
    _record(Store(f"gang/{gang}"), 1, [None, None], 60.0, collective=1.0)
    env = {"WORLD_SIZE": "2", "LOCAL_RANK": "0", "LOCAL_WORLD_SIZE": "1", "CHIMAERA_GANG": gang}
    env.update(CHIMAERA_GANG_GENERATION="1", MASTER_ADDR="127.0.0.1", MASTER_PORT="0")
    first, late = Group({**env, "RANK": "0"}), Group({**env, "RANK": "1"})
    error = TimeoutError if explicit else ch.errors.GangFailed
    with pytest.raises(error, match=r"barrier #1 of <Group rank 0/2 generation 1>") as gave_up:
        first.barrier(timeout=1 if explicit else None)
    # Rank 1 passed the collective's start just before rank 0 gave up: its arrival must not complete it.
    late._seq = 1
    with pytest.raises(error) as refused:
        late._wait(Store(f"gang/{gang}"), "1/c1/barrier", "barrier", 30 if explicit else None)
    assert str(refused.value) == str(gave_up.value)


@ch.remote(cpus=0.5)
class Member:
    def total(self, handle, x):
        g = handle.join()
        assert handle.join() is g, "one rendezvous per member"
        return g.rank, g.size, g.generation, g.all_reduce(x)

    def outsider(self, handle):
        return handle.join()


def test_actors_made_a_group_meet_and_reduce():
    members = [Member.remote() for _ in range(3)]
    handle = ch.dist.group(*members, name=f"t-{os.getpid()}")
    futures = [m.total.remote(handle, 10 * (i + 1)) for i, m in enumerate(members)]
    assert [f.result(timeout=120) for f in futures] == [(r, 3, handle.generation, 60) for r in range(3)]
    assert handle.explain()["summary"] == "every rank is in collective #1 (all_reduce); none is behind"
    # The same key again is a new generation, here with two of them in another order.
    with pytest.raises(ValueError, match="collective_timeout: expected a positive number"):
        ch.dist.group(members[2], members[0], name=handle.key, collective_timeout=0)
    again = ch.dist.group(members[2], members[0], name=handle.key, collective_timeout=30)
    assert again.generation == handle.generation + 1
    from chimaera._store import Store

    assert Store(f"gang/group-{again.key}").get(f"{again.generation}/record")["collective_timeout"] == 30.0
    pair = [members[2].total.remote(again, 1), members[0].total.remote(again, 2)]
    assert [f.result(timeout=120) for f in pair] == [(0, 2, again.generation, 3), (1, 2, again.generation, 3)]
    other = Member.remote()
    with pytest.raises(ValueError, match="inside a method of one of the group's actors"):
        other.outsider.remote(again).result(timeout=60)
    with pytest.raises(ValueError, match="inside a method"):
        again.join()  # the driver is not a member
    with pytest.raises(TypeError, match="takes actors"):
        ch.dist.group(where_am_i)
    for m in [*members, other]:
        m.close()


@ch.remote(cpus=0.25)
def grid():
    g = ch.context().gang
    m = g.mesh(dp=2, tp=4)
    return g.rank, m["tp"].rank, m["tp"].ranks, m["tp"].all_gather(g.rank), m["dp"].ranks, m["dp"].all_gather(g.rank)


def test_a_mesh_splits_the_ranks_into_axis_groups():
    out = grid.options(ranks=8).gang().result(timeout=120)
    for rank, tp_rank, tp_ranks, tp_seen, dp_ranks, dp_seen in out:
        row = [rank - rank % 4 + k for k in range(4)]  # tp varies fastest
        assert (tp_rank, tp_ranks, tp_seen) == (rank % 4, row, row)
        assert dp_ranks == dp_seen == [rank % 4, rank % 4 + 4]


@ch.remote(cpus=0.5)
def bad_mesh():
    g = ch.context().gang
    with pytest.raises(ValueError, match="multiply to 3, not 2"):
        g.mesh(dp=3)
    with pytest.raises(RuntimeError, match="init_device_mesh"):
        g.mesh(dp=1, tp=2)["tp"].torch()
    return "ok"


def test_a_mesh_must_cover_the_group_and_its_axes_have_no_torch_group():
    assert bad_mesh.options(ranks=2).gang().result(timeout=120) == ["ok", "ok"]


@ch.remote(cpus=0.25)
class AxisPerCall:
    def step(self, i, large):
        import numpy as np

        g = ch.context().gang
        tp = g.mesh(dp=2, tp=2)["tp"]  # the axis is built again on every call
        if g.rank % 2:
            time.sleep(0.5)  # the odd rank of each pair comes in late
        mine = i * 10 + g.rank
        value = np.full(1 << 18, mine, dtype=np.float32) if large else mine  # 1 MiB goes through the object plane
        return [int(np.asarray(v).ravel()[0]) for v in tp.all_gather(value)]


@pytest.mark.parametrize("large", [False, True], ids=["inline", "object-plane"])
def test_a_mesh_axis_built_again_on_each_call_keeps_its_collectives_apart(large):
    with AxisPerCall.options(ranks=4).gang() as g:
        for i in range(3):
            pairs = [[i * 10 + r - r % 2, i * 10 + r - r % 2 + 1] for r in range(4)]
            assert g.step.remote(i, large).result(timeout=60) == pairs, f"call {i}: each pair's values of this call"


def test_a_gang_is_a_future_like_any_call():
    gang = sums.options(ranks=2).gang()
    assert isinstance(gang, ch.Future)
    done, pending = ch.wait([gang], timeout=120)
    assert done == [gang] and not pending

    @ch.remote
    def first_of(results):
        return results[0]

    assert first_of.remote(sums.options(ranks=2).gang()).result(timeout=120) == (3, [0, 1])

    import asyncio

    async def main():
        return await sums.options(ranks=2).gang()

    assert asyncio.run(main()) == [(3, [0, 1])] * 2


# ---- admission all or none, the rendezvous record, stale ranks ----------------------

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FIXTURE_GPUS = os.path.join(REPO, "tests", "fixtures", "topology", "dgx-h100")  # eight H100s, as files


def on_a_fresh_peer(code: str, env: dict[str, str] | None = None) -> str:
    """Run `code` against a new local peer of its own (stopped after), so no
    other test's work holds its CPUs."""
    from conftest import BIN, run_script  # here: executors import this module for its remote functions

    home = tempfile.mkdtemp(prefix="chg", dir="/tmp")
    try:
        out = run_script(code, env={"CHIMAERA_HOME": home, **(env or {})}, home=home, timeout=400)
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
        shutil.rmtree(home, ignore_errors=True)
    assert out.returncode == 0, out.stderr[-3000:]
    return out.stdout


def test_two_gangs_that_each_need_the_whole_peer_run_one_after_the_other():
    out = on_a_fresh_peer(
        """
import json, os, time
import chimaera as ch
from chimaera._ledger import holds_on

cores = ch._runtime.Runtime.get().info["usable_cores"]

@ch.remote
def span(tag):
    g = ch.context().gang
    start = time.time()
    devices = [d["id"] for d in json.loads(os.environ.get("CHIMAERA_DEVICES") or "[]")]
    g.barrier()
    time.sleep(2.0)
    g.barrier()
    return tag, start, time.time(), devices

def race(what, **opts):
    a, b = (span.options(ranks=8, **opts).gang(tag) for tag in "ab")
    deadline, waits = time.time() + 30, ""
    while not waits and time.time() < deadline:  # the gang not admitted says why
        waits = next((s for s in (g.explain()["summary"] for g in (a, b)) if s.startswith("waiting")), "")
        time.sleep(0.1)
    ranks = [a.result(timeout=240), b.result(timeout=240)]
    (_, first_end), (second_start, _) = sorted((min(r[1] for r in rs), max(r[2] for r in rs)) for rs in ranks)
    devices = [len({d for r in rs for d in r[3]}) for rs in ranks]
    print(what, "APART", first_end <= second_start, "DEVICES", devices, "HOLDS", holds_on())
    print(what, "WAITED", waits)

race("GPUS", gpus=1, cpus=0.25)   # eight devices: one gang has all of them
race("CPUS", cpus=cores / 8)      # every CPU: one gang has all of them
""",
        env={"CHIMAERA_DEVICE_ROOT": FIXTURE_GPUS},
    )
    assert "GPUS APART True DEVICES [8, 8] HOLDS []" in out, out
    assert "CPUS APART True DEVICES [0, 0] HOLDS []" in out, out
    assert re.search(r"GPUS WAITED waiting for every rank's slot to be free: peer \w+: needs 1 accelerator", out), out
    assert re.search(r"CPUS WAITED waiting for every rank's slot to be free: peer \w+: needs [\d.]+ CPUs", out), out


def test_held_devices_go_to_no_other_call_or_actor_and_their_waits_say_so():
    out = on_a_fresh_peer(
        """
import json, os, time
import chimaera as ch

@ch.remote(gpus=1)
def device():
    return json.loads(os.environ["CHIMAERA_DEVICES"])[0]["id"]

@ch.remote(gpus=1)
class Holder:
    def device(self):
        return json.loads(os.environ["CHIMAERA_DEVICES"])[0]["id"]

res = ch.dist.reserve([{"cpus": 0.5, "gpus": 8}]).ready(timeout=60)
call, actor = device.remote(), Holder.remote()
method = actor.device.remote()
time.sleep(2)
print("CALL", call.explain()["pending_reason"])
print("ACTOR", method.explain()["pending_reason"])
res.release()
print("THEN", bool(call.result(timeout=60)), bool(method.result(timeout=60)))
""",
        env={"CHIMAERA_DEVICE_ROOT": FIXTURE_GPUS},
    )
    assert "CALL queued at position 0: needs 1 accelerator(s); 0 of 8 free" in out, out
    assert re.search(r"ACTOR .*needs 1 accelerator\(s\); 0 of 8 free", out), out
    assert "THEN True True" in out, out


def test_a_rank_starts_on_its_slot_while_a_call_too_big_to_start_waits_ahead_of_it():
    out = on_a_fresh_peer(
        """
import time
import chimaera as ch

cores = ch._runtime.Runtime.get().info["usable_cores"]

@ch.remote
def rank():
    return "rank"

@ch.remote
def whole_peer():
    return "whole peer"

res = ch.dist.reserve([{"cpus": 1.5}]).ready(timeout=60)  # the gang waits for these
gang = rank.options(ranks=1, cpus=cores - 1, rendezvous_timeout=20, max_attempts=1).gang()
deadline = time.time() + 60
while not gang.explain()["summary"].startswith("waiting") and time.time() < deadline:
    time.sleep(0.1)
big = whole_peer.options(cpus=cores).remote()  # queued first; it starts once the peer is idle
time.sleep(1)
res.release()  # the gang is admitted: its rank queues after big, on a slot big could not use
print("RANK", gang.result(timeout=120), "BIG", big.result(timeout=120))
"""
    )
    assert "RANK ['rank'] BIG whole peer" in out, out


def test_a_gang_that_could_never_fit_names_the_first_rank_that_does_not():
    shape = _peer_cpus() * 0.4  # two fit on the peers; a third never does
    with pytest.raises(ch.Infeasible, match=r"rank 2 of 3 fits on no live peer beside the 2 before it"):
        where_am_i.options(ranks=3, cpus=shape).gang("big").result(timeout=60)


def test_a_gang_needing_devices_no_peer_has_is_infeasible_instead_of_waiting():
    out = on_a_fresh_peer(
        """
import chimaera as ch

@ch.remote(gpus=1)
def rank():
    return 1

for device in ("rocm", "cuda:A100"):  # the peer reports eight devices, all of them H100s
    try:
        rank.options(ranks=2, device=device).gang().result(timeout=60)
    except ch.Infeasible as e:
        print("INFEASIBLE", e)
""",
        env={"CHIMAERA_DEVICE_ROOT": FIXTURE_GPUS},
    )
    head = r"INFEASIBLE rank 0 of 2 fits on no live peer \(cpus=1, gpus=1, device='{}' each; strategy 'pack'\); "
    head += r"peer \w+: "
    assert re.search(head.format("rocm") + r"needs 2 rocm device\(s\); this peer has 0", out), out
    assert re.search(head.format("cuda:A100") + r"needs 2 cuda:A100 device\(s\); this peer has 0", out), out


def test_a_gang_sizing_itself_counts_a_fractional_gpu_as_the_whole_device_it_gets():
    out = on_a_fresh_peer(
        """
import json, os
import chimaera as ch

@ch.remote(gpus=0.5, cpus=0.25)
def device():
    return json.loads(os.environ["CHIMAERA_DEVICES"])[0]["id"]

gang = device.options(ranks=ch.auto(max=16)).gang()  # eight devices: eight ranks, not sixteen half ones
ids = gang.result(timeout=240)
print("RANKS", len(ids), "DISTINCT", len(set(ids)))
print("DECISION", gang.decisions[0])
""",
        env={"CHIMAERA_DEVICE_ROOT": FIXTURE_GPUS},
    )
    assert "RANKS 8 DISTINCT 8" in out, out
    assert "DECISION generation 1: 8 ranks: the live peers hold 8 of this shape" in out, out


def test_each_rank_sees_its_own_device_as_index_0_and_the_rank_table_names_it():
    out = on_a_fresh_peer(
        """
import json, os
import chimaera as ch

@ch.remote(gpus=1, cpus=0.25)
def where():
    g = ch.context().gang
    mine = json.loads(os.environ["CHIMAERA_DEVICES"])
    return g.rank, g.local_rank, g.local_size, [d["index"] for d in mine], [d["id"] for d in mine], g.table()

rows = where.options(ranks=2).gang().result(timeout=240)
print("LOCAL", [r[1:3] for r in rows], "VISIBLE", [r[3] for r in rows], "DISTINCT", len({r[4][0] for r in rows}))
print("TABLES AGREE", rows[0][5] == rows[1][5])
table = [(t["rank"], t["local_rank"], t["visible"], t["devices"]) for t in rows[0][5]]
print("TABLE MATCHES", table == [(r[0], r[1], r[3], r[4]) for r in rows])
""",
        env={"CHIMAERA_DEVICE_ROOT": FIXTURE_GPUS},
    )
    assert "LOCAL [(0, 2), (1, 2)] VISIBLE [[0], [0]] DISTINCT 2" in out, out
    assert "TABLES AGREE True" in out, out
    assert "TABLE MATCHES True" in out, out


def test_a_rank_reads_the_store_as_often_however_large_its_gang(monkeypatch):
    """The rendezvous costs each rank a fixed number of reads: at 1,024 ranks,
    reading every rank's peer from every rank was a million reads."""
    from chimaera import _context, _gang, _store
    from chimaera._errors import VersionConflict

    data: dict[str, tuple[object, int]] = {}
    barriers: dict[str, threading.Barrier] = {}
    lock, here = threading.Lock(), threading.local()

    class CountingStore:
        def __init__(self, namespace: str) -> None:
            self.ns = namespace

        def _read(self) -> None:
            here.reads += 1

        def get(self, key, default=None):
            self._read()
            return data.get(f"{self.ns}/{key}", (default, 0))[0]

        def get_versioned(self, key, default=None):
            self._read()
            return data.get(f"{self.ns}/{key}", (default, 0))

        def put(self, key, value, *, if_version=None, ttl=None, fence=None):
            with lock:
                version = data.get(f"{self.ns}/{key}", (None, 0))[1]
                if if_version is not None and if_version != version:
                    raise VersionConflict(key, version)
                data[f"{self.ns}/{key}"] = (value, version + 1)

        def scan(self, prefix=""):
            self._read()
            full = f"{self.ns}/{prefix}"
            with lock:
                return sorted((k[len(self.ns) + 1 :], v) for k, (v, _) in data.items() if k.startswith(full))

        def barrier(self, key, parties):
            with lock:
                b = barriers.setdefault(f"{self.ns}/{key}", threading.Barrier(parties))
            return types.SimpleNamespace(wait=b.wait)

    monkeypatch.setattr(_store, "Store", CountingStore)
    monkeypatch.setattr(_context, "context", lambda: types.SimpleNamespace(node=here.node))
    reads_by_world = {}
    for world in (2, 4, 8, 16):
        gang = f"count-{world}"
        _gang._record(CountingStore(f"gang/{gang}"), 1, [None] * world, timeout=30)
        envs, reads = [None] * world, [0] * world

        def rank(r: int) -> None:
            here.node, here.reads = f"peer{r % 2}", 0  # two peers, alternating
            envs[r] = _gang._rendezvous(gang, 1, r, world)
            reads[r] = here.reads

        threads = [threading.Thread(target=rank, args=(r,)) for r in range(world)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        _gang._hand_off(gang, 1)
        assert [(e["LOCAL_RANK"], e["LOCAL_WORLD_SIZE"]) for e in envs] == [
            (str(r // 2), str(world // 2)) for r in range(world)
        ]
        reads_by_world[world] = max(reads)
    assert len(set(reads_by_world.values())) == 1, reads_by_world


def test_a_gang_the_roomiest_peer_refuses_is_admitted_where_devices_are_free():
    from test_cluster import Cluster

    gpus = {"CHIMAERA_DEVICE_ROOT": FIXTURE_GPUS}
    cluster = Cluster(2, env={0: gpus, 1: gpus})  # 4 CPUs and eight H100s each
    try:
        out = cluster.run(
            0,
            """
import time
import chimaera as ch

def live():
    return {n.peer: n for n in ch.runs.cluster().nodes() if n.alive}

deadline = time.time() + 60
while len(live()) < 2 and time.time() < deadline:
    time.sleep(0.2)
here, there = sorted(live(), key=lambda p: not live()[p].this_peer)

@ch.remote(cpus=1)
class Busy:
    def node(self):
        return ch.context().node

# The other peer has fewer CPUs free; this one's GPUs are all reserved. Reports
# show CPUs, not which devices are free, so a plan by them picks this peer first.
busy = Busy.options(placement=ch.dist.Placement(require={"node": there})).remote()
assert busy.node.remote().result(timeout=60) == there
while live()[there].cpu_used < 1 and time.time() < deadline + 60:
    time.sleep(0.2)
res = ch.dist.reserve([{"cpus": 0.25, "gpus": 8}]).ready(timeout=60)
print("RESERVED HERE", res._nodes == [here])

@ch.remote(gpus=1, cpus=0.25)
def rank():
    return ch.context().node

nodes = rank.options(ranks=8).gang().result(timeout=60)
print("RANKS THERE", set(nodes) == {there})
res.release()
""",
        )
        assert out.returncode == 0, out.stderr[-3000:]
        assert "RESERVED HERE True" in out.stdout and "RANKS THERE True" in out.stdout, out.stdout
    finally:
        cluster.close()


def _hang_once(path):
    try:
        os.close(os.open(path, os.O_CREAT | os.O_EXCL))
    except FileExistsError:
        return path
    time.sleep(600)  # the first rank to load its arguments never reaches the rendezvous
    return path


class HangsOnLoad:
    def __init__(self, path):
        self.path = path

    def __reduce__(self):
        return (_hang_once, (self.path,))


def test_a_rank_that_misses_the_rendezvous_deadline_is_named_with_its_stage(tmp_path):
    start = time.monotonic()
    gang = meet.options(ranks=3, rendezvous_timeout=5, max_attempts=1).gang(HangsOnLoad(str(tmp_path / "hung")))
    with pytest.raises(ch.errors.GangFailed) as failed:
        gang.result(timeout=120)
    message = str(failed.value)  # the supervisor's deadline, or the waiting ranks', names it
    assert "within 5 s" in message, message
    named = re.findall(r"rank (\d) \(peer [0-9a-f]+\): started, not at the rendezvous yet", message)
    assert len(named) == 1, message
    assert time.monotonic() - start < 60, "the deadline, not the 300 s default, ended it"


@ch.remote(cpus=0.5)
def remember_then_lose(path):
    g = ch.context().gang
    if g.generation == 1 and g.rank == 0:
        with open(path, "w") as f:
            json.dump({k: os.environ[k] for k in os.environ if k in ENV_KEYS}, f)
    elif g.generation == 1:
        os._exit(1)  # generation 1 is lost; its rank 0 lives on until cancelled
    g.barrier()
    return g.generation


ENV_KEYS = ("RANK", "WORLD_SIZE", "LOCAL_RANK", "LOCAL_WORLD_SIZE", "MASTER_ADDR", "MASTER_PORT", "CHIMAERA_GANG")
ENV_KEYS += ("CHIMAERA_GANG_GENERATION",)


@ch.remote
def as_a_stale_rank(env):
    from chimaera._gang import Group

    g, out = Group(env), []
    for step in (g.barrier, lambda: g.all_gather(1), g.torch):
        try:
            step()
            out.append("joined")
        except ch.errors.GangFailed as e:
            out.append(str(e))
    return out


def test_a_rank_of_a_replaced_generation_cannot_join_its_successor(tmp_path):
    path = str(tmp_path / "generation-1.json")
    assert remember_then_lose.options(ranks=2).gang(path).result(timeout=180) == [2, 2]
    with open(path) as f:
        env = json.load(f)
    stale = f"gang {env['CHIMAERA_GANG']}: generation 1 was replaced by generation 2; this rank is stale"
    assert as_a_stale_rank.remote(env).result(timeout=60) == [stale] * 3


def _stand_in_torch():
    """A stand-in torch.distributed (torch is not installed here) whose
    TCPStore binds, listens on a socket handed to it, and connects for real,
    as torch's does; returns its undo."""
    torch, dist = types.ModuleType("torch"), types.ModuleType("torch.distributed")
    group: dict = {}

    class TCPStore:
        def __init__(self, host, port, world_size=None, is_master=False, timeout=None, **kw):
            master_listen_fd = kw.get("master_listen_fd")
            self.port, self.handed = port, master_listen_fd is not None
            if is_master and self.handed:
                self.sock = socket.socket(fileno=master_listen_fd)
                assert self.sock.getsockname()[1] == port
            elif is_master:
                self.sock = socket.socket()
                self.sock.bind(("", port))  # OSError "Address already in use" when it is taken
                self.sock.listen()
            else:
                self.sock = socket.create_connection((host, port), timeout=30)

    def init_process_group(backend=None, init_method=None, rank=-1, world_size=-1, store=None, timeout=None):
        if store is None:  # tcp://addr:port, as torch: rank 0 binds the port
            host, port = init_method.removeprefix("tcp://").rsplit(":", 1)
            store = TCPStore(host, int(port), world_size, rank == 0)
        group["store"], group["timeout"] = store, timeout

    class DistError(RuntimeError):  # torch's backend, network and store errors
        pass

    class DistBackendError(DistError):
        pass

    def destroy_process_group():
        store = group.pop("store", None)
        if store is not None:
            store[1].sock.close()

    dist.TCPStore, dist.PrefixStore = TCPStore, lambda prefix, store: (prefix, store)
    dist.DistError, dist.DistBackendError = DistError, DistBackendError
    dist.init_process_group, dist.destroy_process_group = init_process_group, destroy_process_group
    dist.is_initialized, dist.group = (lambda: "store" in group), group
    torch.distributed = dist
    sys.modules["torch"], sys.modules["torch.distributed"] = torch, dist

    def undo():
        destroy_process_group()
        sys.modules.pop("torch.distributed", None)
        sys.modules.pop("torch", None)

    return undo


@ch.remote(cpus=0.5)
def torch_store_of_a_gang(squat):
    from chimaera._store import Store

    g = ch.context().gang
    committed, squatter = g.master[1], socket.socket()
    if g.rank == 0 and squat:
        squatter.bind(("", committed))  # something else takes the port between rendezvous and torch
        squatter.listen()
    g.barrier()
    undo = _stand_in_torch()
    try:
        prefix, store = g.torch().group["store"]
        g.barrier()  # as torch's own init does: rank 0 serves until every rank connected
        endpoint = Store(f"gang/{g.id}").get(f"{g.generation}/record")["endpoint"]
        served = (g.master[1], int(os.environ["MASTER_PORT"]), store.port)
        return committed, *served, list(endpoint[1:]), prefix, store.handed
    finally:
        undo()
        squatter.close()


@pytest.mark.parametrize("squat", [False, True])
def test_torch_store_serves_on_a_socket_rank_0_bound_at_a_new_endpoint_if_its_port_was_taken(squat):
    out = torch_store_of_a_gang.options(ranks=3).gang(squat).result(timeout=120)
    committed = {o[0] for o in out}
    served = {o[1:4] for o in out}
    assert len(committed) == 1 and len(served) == 1, out
    port = served.pop()[0]
    assert (port not in committed) == squat, "another port only when something took the committed one"
    assert all(o[4] == [port, 2, True] for o in out), "the record's second endpoint, serving"
    assert all(o[5].endswith("/1") for o in out), "torch's keys are generation 1's"
    assert [o[6] for o in out] == [True, False, False], "torch listens on the socket rank 0 bound: none to steal"


@ch.remote(cpus=0.5)
def torch_times_out_once():
    g = ch.context().gang
    undo = _stand_in_torch()
    try:
        dist = g.torch()
        g.barrier()  # rank 0 serves torch's store until every rank connected
        if g.generation == 1 and g.rank == 1:
            raise dist.DistBackendError("Watchdog caught collective operation timeout")  # as torch's, at its timeout
        return g.generation, dist.group["timeout"].total_seconds()
    finally:
        undo()


def test_a_torch_collective_that_times_out_reforms_the_gang_and_torch_gets_the_collective_timeout():
    gang = torch_times_out_once.options(ranks=3, collective_timeout=7).gang()
    assert gang.result(timeout=120) == [(2, 7.0)] * 3


@ch.remote(cpus=0.5)
class TorchRank:
    def __init__(self):
        _stand_in_torch()  # this rank's own process: it stays for the actor's life

    def setup(self):
        g = ch.context().gang
        g.torch()
        g.barrier()  # rank 0 serves torch's store until every rank connected
        return g.generation

    def step(self):
        g = ch.context().gang
        if g.rank == 1:
            raise sys.modules["torch.distributed"].DistBackendError("Watchdog caught collective operation timeout")
        g.barrier()  # the others wait here: well short of collective_timeout, they see the generation lost
        return g.rank


def test_a_torch_error_out_of_an_actor_gang_method_loses_the_generation_until_restart():
    lost = r"generation 1 is lost: rank 1: torch.distributed raised DistBackendError: Watchdog caught"
    with TorchRank.options(ranks=3, collective_timeout=60).gang() as g:
        assert g.setup.remote().result(timeout=60) == [1, 1, 1]
        start = time.monotonic()
        errors = []
        for f in [g[r].step.remote() for r in range(3)]:
            with pytest.raises(ch.errors.GangFailed, match=lost) as failed:
                f.result(timeout=60)
            errors.append(str(failed.value))
        assert time.monotonic() - start < 30, "the waiting ranks raised soon after rank 1, not at collective_timeout"
        assert len(set(errors)) == 1, f"one reason on every rank: {errors}"
        g.restart()
        assert g.setup.remote().result(timeout=60) == [2, 2, 2]


@ch.remote(cpus=0.5)
def torch_set_up_fails_once(how):
    g = ch.context().gang
    undo = _stand_in_torch()
    dist = sys.modules["torch.distributed"]
    try:
        if g.generation == 1 and how == "late" and g.rank == 0:
            time.sleep(600)  # rank 0 never starts torch's store: the others wait for it in torch()
        if g.generation == 1 and how == "init" and g.rank == 1:

            def refused(*args, **kwargs):
                raise dist.DistError("The client socket has timed out after 3s")  # as torch's TCPStore

            dist.init_process_group = refused
        g.torch()
        g.barrier()
        return g.generation
    finally:
        undo()


@pytest.mark.parametrize("how", ["late", "init"])
def test_a_rank_stuck_or_refused_setting_up_torch_reforms_the_gang_within_its_collective_timeout(how):
    start = time.monotonic()
    gang = torch_set_up_fails_once.options(ranks=3, collective_timeout=3, rendezvous_timeout=150).gang(how)
    assert gang.result(timeout=120) == [2, 2, 2]
    assert time.monotonic() - start < 60, "given up at the collective_timeout, not the rendezvous_timeout"


def _ddp_fit(torch, model, x, y, steps):
    """`steps` of SGD on the mean squared error of `model` over x, y."""
    opt = torch.optim.SGD(model.parameters(), lr=0.1)
    for _ in range(steps):
        opt.zero_grad()
        torch.nn.functional.mse_loss(model(x), y).backward()
        opt.step()


def _ddp_data(torch):
    gen = torch.Generator().manual_seed(1)
    x = torch.randn(8, 3, generator=gen, dtype=torch.float64)
    return x, x @ torch.tensor([[1.0], [-2.0], [0.5]], dtype=torch.float64) + 0.3


@ch.remote(cpus=0.5)
def ddp_rank(steps):
    import torch
    from torch.nn.parallel import DistributedDataParallel

    g = ch.context().gang
    dist = g.torch()
    torch.manual_seed(0)
    model = torch.nn.Linear(3, 1).double()
    x, y = _ddp_data(torch)
    half = slice(g.rank * 4, (g.rank + 1) * 4)  # each rank its half of the batch; DDP averages the gradients
    _ddp_fit(torch, DistributedDataParallel(model), x[half], y[half], steps)
    total = torch.tensor([float(g.rank + 1)], dtype=torch.float64)
    dist.all_reduce(total)
    return ch.dist.device(), g.backend, total.item(), [v for p in model.parameters() for v in p.detach().flatten().tolist()]


def test_a_ddp_model_on_two_gloo_ranks_trains_as_it_does_in_one_process():
    torch = pytest.importorskip("torch")
    ranks = ddp_rank.options(ranks=2).gang(20).result(timeout=240)
    torch.manual_seed(0)
    model = torch.nn.Linear(3, 1).double()
    _ddp_fit(torch, model, *_ddp_data(torch), 20)  # the whole batch, in this process
    alone = [v for p in model.parameters() for v in p.detach().flatten().tolist()]
    assert [r[:3] for r in ranks] == [("cpu", "gloo", 3.0)] * 2, ranks
    assert ranks[0][3] == ranks[1][3], "the ranks' weights diverged"
    assert max(abs(a - b) for a, b in zip(ranks[0][3], alone, strict=True)) < 1e-9, (ranks[0][3], alone)
