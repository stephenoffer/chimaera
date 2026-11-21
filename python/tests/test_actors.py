"""Actor semantics: ordering, concurrency, failures, identity."""

from __future__ import annotations

import asyncio
import random
import time

import pytest

import chimaera as ch


@ch.remote
class Log:
    def __init__(self):
        self.items = []

    def append(self, i, delay=0.0):
        if delay:
            time.sleep(delay)
        self.items.append(i)
        return i

    def get(self):
        return list(self.items)

    def boom(self):
        raise RuntimeError("method failed")


@ch.remote(concurrency=4)
class Overlapping:
    def __init__(self):
        self.items = []
        self.active = 0
        self.max_active = 0

    def append(self, i, delay=0.0):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        time.sleep(delay)
        self.items.append(i)
        self.active -= 1
        return i

    def get(self):
        return list(self.items), self.max_active


@ch.remote
class AsyncCounter:
    def __init__(self):
        self.active = 0
        self.max_active = 0
        self.order = []

    async def work(self, i):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.002)
        self.order.append(i)
        self.active -= 1
        return i

    def stats(self):
        return self.max_active, list(self.order)


def test_calls_through_one_handle_run_in_submission_order():
    log = Log.remote()
    rng = random.Random(7)
    futs = [log.append.remote(i, rng.choice([0, 0, 0, 0.001])) for i in range(1000)]
    ch.gather(*futs, timeout=120)
    assert log.get.remote().result(timeout=30) == list(range(1000))


def test_concurrency_control_shows_overlap_and_reordering():
    # The control proves the ordering test above can detect reordering.
    actor = Overlapping.remote()
    futs = [actor.append.remote(i, 0.05 if i % 2 == 0 else 0.001) for i in range(40)]
    ch.gather(*futs, timeout=120)
    items, max_active = actor.get.remote().result(timeout=30)
    assert max_active > 1, "concurrency=4 should overlap calls"
    assert sorted(items) == list(range(40)) and items != list(range(40)), "overlapping calls finish out of order"


def test_async_methods_do_not_overlap_by_default():
    c = AsyncCounter.remote()
    ch.gather(*(c.work.remote(i) for i in range(50)), timeout=60)
    max_active, order = c.stats.remote().result(timeout=30)
    assert max_active == 1, "writing async def must not change the actor's concurrency"
    assert order == list(range(50))


def test_failed_method_leaves_the_actor_usable_and_its_state_intact():
    log = Log.remote()
    log.append.remote(1).result(timeout=30)
    with pytest.raises(RuntimeError, match="method failed"):
        log.boom.remote().result(timeout=30)
    log.append.remote(2).result(timeout=30)
    assert log.get.remote().result(timeout=30) == [1, 2]


def test_constructor_failure_fails_every_method_call_with_its_error():
    @ch.remote
    class Broken:
        def __init__(self):
            raise OSError("cannot load model")

        def ping(self):
            return "pong"

    b = Broken.remote()
    with pytest.raises(OSError, match="cannot load model"):
        b.ping.remote().result(timeout=30)


def test_futures_flow_into_actor_calls_without_driver_fetch():
    @ch.remote
    def produce(x):
        return x * 10

    log = Log.remote()
    assert log.append.remote(produce.remote(4)).result(timeout=30) == 40


def test_keyed_actor_is_get_or_create():
    name = f"registry-{time.time_ns()}"
    a = Log.options(key=name).remote()
    a.append.remote("x").result(timeout=30)
    b = Log.options(key=name).remote()
    assert b.id == a.id
    assert b.get.remote().result(timeout=30) == ["x"]


def test_pool_spreads_independent_calls():
    @ch.remote
    class Worker:
        def __init__(self):
            import os

            self.pid = os.getpid()

        def whoami(self, _):
            time.sleep(0.02)
            return self.pid

    pool = Worker.options(replicas=3).pool()
    pids = set(pool.whoami.map(range(30)))
    assert len(pool) == 3 and len(pids) == 3


def test_closing_an_actor_ends_it():
    @ch.remote(cpus=1)
    class Holder:
        def ping(self):
            return "pong"

    h = Holder.remote()
    assert h.ping.remote().result(timeout=30) == "pong"
    h.close()
    with pytest.raises(ch.ActorDied):
        h.ping.remote().result(timeout=30)


def test_many_idle_actors_do_not_starve_tasks():
    # Actors that did not ask for CPU hold none: tasks still run.
    n = int(ch._runtime.Runtime.get().info["usable_cores"]) + 4
    actors = [Log.remote() for _ in range(n)]
    ch.gather(*(a.append.remote(1) for a in actors), timeout=60)

    @ch.remote
    def plain(x):
        return x

    assert ch.gather(*(plain.remote(i) for i in range(20)), timeout=60) == list(range(20))
    for a in actors:
        a.close()
