"""Actor semantics: ordering, concurrency, failures, identity."""

from __future__ import annotations

import asyncio
import random
import subprocess
import tempfile
import time

import pytest

import chimaera as ch
from conftest import BIN, run_script


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
    ch.get(futs, timeout=120)
    assert log.get.remote().result(timeout=30) == list(range(1000))


def test_concurrency_control_shows_overlap_and_reordering():
    # The control proves the ordering test above can detect reordering.
    actor = Overlapping.remote()
    futs = [actor.append.remote(i, 0.05 if i % 2 == 0 else 0.001) for i in range(40)]
    ch.get(futs, timeout=120)
    items, max_active = actor.get.remote().result(timeout=30)
    assert max_active > 1, "concurrency=4 should overlap calls"
    assert sorted(items) == list(range(40)) and items != list(range(40)), "overlapping calls finish out of order"


def test_async_methods_do_not_overlap_by_default():
    c = AsyncCounter.remote()
    ch.get((c.work.remote(i) for i in range(50)), timeout=60)
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
    a = Log.options(name=name).remote()
    a.append.remote("x").result(timeout=30)
    b = Log.options(name=name).remote()
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
    ch.get((a.append.remote(1) for a in actors), timeout=60)

    @ch.remote
    def plain(x):
        return x

    assert ch.get((plain.remote(i) for i in range(20)), timeout=60) == list(range(20))
    for a in actors:
        a.close()


def test_a_call_to_an_actor_closed_before_it_started_fails_instead_of_waiting():
    home = tempfile.mkdtemp(prefix="cha", dir="/tmp")  # a fresh peer: every CPU is free
    try:
        out = run_script(
        """
        import chimaera as ch

        cores = int(ch._runtime.Runtime.get().info["usable_cores"])

        @ch.remote(cpus=cores)
        class Big:
            def ping(self):
                return 1

        holder = Big.remote()        # takes every CPU
        waiting = Big.remote()       # waits for CPUs
        call = waiting.ping.remote()
        waiting.close()
        try:
            call.result(timeout=30)
        except TimeoutError:
            print("HUNG")
        except Exception as e:
            print("FAILED", e)
        """,
            home=home,
        )
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
    assert "FAILED the actor ended before it started" in out.stdout, out.stdout + out.stderr


@ch.remote
class Named:
    def ping(self):
        return "pong"


def test_named_actors_are_listed_by_name_and_prefix():
    import uuid

    tag = uuid.uuid4().hex[:8]
    made = [Named.options(name=f"list-{tag}/{n}").remote() for n in ("b", "a")]
    assert [m.ping.remote().result(timeout=60) for m in made] == ["pong", "pong"]
    listed = ch.runs.actors(f"list-{tag}/")
    assert [a.name for a in listed] == [f"list-{tag}/a", f"list-{tag}/b"], listed
    assert all(a.state in ("Alive", "Pending") and len(a.id) == 32 for a in listed)
    assert ch.actor(listed[0].name).ping.remote().result(timeout=60) == "pong"
    for m in made:
        m.close()
    deadline = time.monotonic() + 30
    while ch.runs.actors(f"list-{tag}/") and time.monotonic() < deadline:
        time.sleep(0.2)
    assert ch.runs.actors(f"list-{tag}/") == [], "closed actors are dead, and the dead are not listed"


@ch.remote
class Quitter:
    def ping(self):
        return "pong"

    def quit(self):
        ch.context().exit_actor()


def test_an_actor_can_end_itself():
    q = Quitter.remote()
    assert q.ping.remote().result(timeout=60) == "pong"
    with pytest.raises(ch.ActorDied, match="exit_actor"):
        q.quit.remote().result(timeout=60)
    with pytest.raises(ch.ActorDied):
        q.ping.remote().result(timeout=60)  # closed: not restarted


def test_exit_actor_outside_an_actor_says_where_it_belongs():
    @ch.remote
    def not_an_actor():
        ch.context().exit_actor()

    with pytest.raises(RuntimeError, match="inside one of the actor's methods"):
        not_an_actor.remote().result(timeout=60)

