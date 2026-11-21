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
    with pytest.raises(ch.GangFailed, match="generation 2 lost a rank"):
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
