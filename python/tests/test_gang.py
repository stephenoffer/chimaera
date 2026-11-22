"""Gangs: fn.options(ranks=n).gang(...) — rendezvous env, collectives, failure as one."""

from __future__ import annotations

import os

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
    with pytest.raises(AttributeError, match="no method 'nope'"):
        Shard.options(ranks=1).gang(1).nope


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
        (ch.auto(min=0, max=4), "min must be an integer >= 1"),
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
    again = ch.dist.group(members[2], members[0], name=handle.key)
    assert again.generation == handle.generation + 1
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
