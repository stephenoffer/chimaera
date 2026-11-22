"""Pools: least-loaded calls, route(key), resize, rolling upgrade, shared pools, ch.auto."""

from __future__ import annotations

import threading
import time
import uuid

import pytest

import chimaera as ch


@ch.remote
class Model:
    def __init__(self, version="v1"):
        self.version = version

    def who(self, _=None):
        return ch.context().actor_id

    def predict(self, x, delay=0.0):
        time.sleep(delay)
        return (self.version, x)


def test_route_is_stable_and_resizing_moves_few_keys():
    with Model.options(replicas=4).pool() as pool:
        keys = [f"user-{i}" for i in range(200)]

        def owners():
            calls = {k: pool.route(k).who.remote() for k in keys}
            return {k: f.result(timeout=60) for k, f in calls.items()}

        before = owners()
        assert before == owners()
        assert len(set(before.values())) == 4
        pool.resize(5)
        assert len(pool) == 5
        after = owners()
        moved = sum(before[k] != after[k] for k in keys)
        # Rendezvous hashing: adding a fifth replica moves about a fifth of the keys.
        assert 0 < moved < len(keys) * 0.4, moved


def test_resize_shrinks_without_dropping_calls_in_flight():
    with Model.options(replicas=4).pool() as pool:
        slow = [pool.predict.remote(i, delay=0.5) for i in range(8)]
        pool.resize(1)
        assert len(pool) == 1
        assert [f.result(timeout=60) for f in slow] == [("v1", i) for i in range(8)]
        assert pool.predict.remote(99).result(timeout=60) == ("v1", 99)
        pool.resize(3)
        assert len({pool.who.remote().result(timeout=60) for _ in range(30)}) >= 2


def test_rolling_upgrade_drops_no_calls():
    with Model.options(replicas=3).pool("v1") as pool:
        results: list = []
        errors: list = []
        stop = threading.Event()

        def traffic():
            i = 0
            while not stop.is_set():
                try:
                    results.append(pool.predict.remote(i, delay=0.02).result(timeout=60))
                except Exception as e:  # noqa: BLE001 - any failure is the bug under test
                    errors.append(e)
                i += 1

        senders = [threading.Thread(target=traffic) for _ in range(4)]
        for t in senders:
            t.start()
        time.sleep(0.5)
        pool.upgrade(Model, "v2", surge=1)
        time.sleep(0.5)
        stop.set()
        for t in senders:
            t.join(timeout=60)
        assert errors == []
        versions = [v for v, _ in results]
        assert "v1" in versions and versions[-1] == "v2"
        assert {pool.predict.remote(0).result(timeout=60)[0] for _ in range(20)} == {"v2"}
        assert len(pool) == 3


@ch.remote
def ask_shared(pool, n):
    return sorted({pool.who.remote().result(timeout=60) for _ in range(n)})


def test_a_keyed_pool_is_shared_with_calls_and_other_handles():
    key = f"shared-{uuid.uuid4().hex[:8]}"
    pool = Model.options(name=key, replicas=2).pool()
    try:
        mine = sorted({pool.who.remote().result(timeout=60) for _ in range(20)})
        assert len(mine) == 2
        # A task given the pool reaches the same replicas, and so does a second handle.
        assert ask_shared.remote(pool, 20).result(timeout=60) == mine
        again = Model.options(name=key).pool()
        assert sorted({again.who.remote().result(timeout=60) for _ in range(20)}) == mine
        # Growing it through one handle shows up in the other.
        pool.resize(3)
        time.sleep(1.2)  # handles re-read membership at most once a second
        assert len(again) == 3
    finally:
        pool.close()


def test_auto_replicas_grow_under_load():
    pool = Model.options(replicas=ch.auto(1, max=4)).pool()
    try:
        assert len(pool) == 1
        futures = [pool.predict.remote(i, delay=0.2) for i in range(60)]
        deadline = time.monotonic() + 30
        while len(pool) < 4 and time.monotonic() < deadline:
            time.sleep(0.1)
        assert len(pool) == 4, pool.decisions
        assert pool.decisions and pool.decisions[0].startswith("grow 1 -> 2")
        assert [f.result(timeout=120)[1] for f in futures] == list(range(60))
    finally:
        pool.close()


def test_a_routed_call_holds_its_replica_through_a_resize():
    @ch.remote
    class Slow:
        def nap(self, seconds):
            time.sleep(seconds)
            return "done"

    pool = Slow.options(replicas=2).pool()
    try:
        newest = pool._replicas[1]  # the one a shrink retires
        key = next(k for k in range(1000) if pool._replica_for(str(k).encode()) is newest)
        f = pool.route(key).nap.remote(2.0)
        time.sleep(0.3)
        pool.resize(1)
        assert f.result(timeout=60) == "done", "the replica waited for its routed call"
    finally:
        pool.close()


def test_a_method_the_pool_hides_is_named():
    @ch.remote
    class Sized:
        def resize(self, n):
            return n

    with pytest.warns(UserWarning, match="Sized defines resize, which a pool handle reserves"):
        pool = Sized.options(replicas=1).pool()
    pool.close()
