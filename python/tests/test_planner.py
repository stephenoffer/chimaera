"""The capacity planner (chimaera._planner): pure, so no peer is needed."""

from __future__ import annotations

from types import SimpleNamespace

from chimaera._cluster import Node
from chimaera._planner import Forecast, Pool, Startup, Target, _simulate, arrival_rate, plan, read
from chimaera.provisioner import Memory, Rules, decide

GiB = 1 << 30


def node(i=0, cpus=8.0, memory=32 * GiB, shapes=(), releases=(), gangs=(), age=0.0, starting=False):
    """A node as ch.runs.cluster().nodes() gives it, with its peer's demand report."""
    queued = sum(s.get("queued", 0) for s in shapes)
    demand = {"shapes": list(shapes), "releases": list(releases), "gangs": list(gangs), "started_total": 0}
    return SimpleNamespace(
        peer=f"p{i}",
        address=f"chimaera-{i}.peers:7700",
        cpus=cpus,
        memory_bytes=memory,
        alive=True,
        cpu_used=sum(c for _, c in releases),
        queued=queued,
        draining=False,
        actors=0,
        object_bytes=0,
        starting=starting,
        load_age_s=age,
        demand=demand,
    )


def calls(n, wall, cpus=1.0, memory=0):
    return {"cpus": cpus, "memory": memory, "queued": n, "wall_s": n * wall, "cold": 0, "actors": 0}


POOL = Pool(peer_cpus=8.0, peer_memory=32 * GiB, replicas=2, min=1, max=16)


def golden(wall):
    """Two eight-core peers, 48 one-core calls queued, nothing running."""
    return [node(0, shapes=[calls(24, wall)]), node(1, shapes=[calls(24, wall)])]


def test_a_backlog_that_clears_before_a_new_peer_could_start_holds():
    p = plan(golden(1.0), POOL, Target(start_s=None, finish_s=90), startup_s=60)
    assert (p.want, p.action) == (2, "hold") and p.finish_s == 3 and p.missed is None
    # Today's rule, on the same snapshot once the backlog has lasted --up-after:
    # 48 calls / 8 CPUs per peer = 6 more peers, none of which would run a call.
    assert decide(golden(1.0), 2, Rules(min=1, max=8, up_after=30), Memory(busy_since=0), now=30)[0] == 8


def test_a_finish_target_no_size_can_meet_says_so_and_asks_for_the_closest():
    p = plan(golden(60.0), POOL, Target(start_s=None, finish_s=90), startup_s=60)
    # The first 16 calls end at 60 s, when new peers are up at the earliest:
    # the other 32 cannot end before 120 s however many there are.
    assert (p.want, p.action) == (4, "grow") and p.finish_s == 120
    assert "90 s" in p.missed and "120 s" in p.missed
    assert "startup 60 s" in p.binding


def test_a_start_target_buys_the_fewest_peers_that_meet_it():
    p = plan(golden(60.0), POOL, Target(start_s=65), startup_s=60)
    assert (p.want, p.action) == (4, "grow") and p.latest_start_s == 60 and p.missed is None
    assert p.runner_up[0] == 3 and "120 s" in p.runner_up[1]  # 8 calls would still wait for the first wave


def test_calls_no_peer_fits_are_left_out_and_named():
    big = calls(5, 10.0, memory=64 * GiB)
    p = plan([node(0, shapes=[big, calls(4, 1.0)])], POOL, Target(), startup_s=60)
    assert (p.want, p.action) == (2, "hold")
    assert p.inputs["excluded"]["too_big"] == ["5 calls of 1 CPUs and 64 GiB: no peer fits them"]


def test_calls_only_a_bigger_existing_peer_fits_never_buy_pool_peers():
    nodes = [node(0, cpus=16.0, memory=64 * GiB, shapes=[calls(10, 20.0, cpus=12.0)])]
    p = plan(nodes, Pool(8.0, 32 * GiB, replicas=1, min=1, max=8), Target(start_s=60), startup_s=60)
    assert (p.want, p.action) == (1, "hold")
    assert p.missed and p.binding == "no number of new peers starts the queued calls sooner"


def test_peers_already_asked_for_count_as_supply():
    pool = Pool(8.0, 32 * GiB, replicas=4, min=1, max=16, pending=(30.0, 30.0))
    p = plan(golden(60.0), pool, Target(start_s=65), startup_s=60)
    assert (p.want, p.action) == (4, "hold") and p.latest_start_s == 60  # 16 start at 30 s, the rest at 60 s


def test_only_confirmed_demand_grows_the_pool():
    fresh = plan(golden(60.0), POOL, Target(start_s=65), startup_s=60)
    stale = [node(0), node(1, shapes=[calls(48, 60.0)], age=30.0)]
    p = plan(stale, POOL, Target(start_s=65), startup_s=60)
    assert fresh.want == 4 and (p.want, p.action) == (2, "hold")
    assert p.inputs["unconfirmed"] == [{"peer": "p1", "why": "load report 30 s old", "queued": 48}]
    starting = plan([node(0), node(1, shapes=[calls(48, 60.0)], starting=True)], POOL, Target(start_s=65), 60)
    assert starting.want == 2 and starting.inputs["unconfirmed"][0]["why"] == "starting"


def test_an_older_peer_without_a_demand_report_is_named_not_counted():
    old = Node("p0", "a:1", 8.0, 32 * GiB, [], True, 8.0, 50, False, {})
    p = plan([old], Pool(8.0, 32 * GiB, replicas=1, min=1, max=8), Target(), startup_s=60)
    assert (p.want, p.action) == (1, "hold")
    assert p.inputs["unconfirmed"] == [{"peer": "p0", "why": "no demand report (an older peer)", "queued": 50}]


def test_a_gang_waiting_on_several_peers_counts_once():
    gang = {"owner": "g1", "generation": 1, "bundles": [{"cpus": 4.0, "memory": 0}] * 4, "age_s": 3.0}
    pool = Pool(8.0, 32 * GiB, replicas=1, min=1, max=8)
    p = plan([node(0, gangs=[gang]), node(1, cpus=0.0, gangs=[gang])], pool, Target(start_s=90), startup_s=60)
    # 16 CPUs: 8 on the peer there is and 8 on one new peer (32 if counted twice).
    assert (p.want, p.action) == (2, "grow") and p.inputs["gangs"] == 1
    # A newer generation of the same gang replaces the older.
    smaller = dict(gang, generation=2, bundles=[{"cpus": 4.0, "memory": 0}] * 2)
    p = plan([node(0, gangs=[gang]), node(1, cpus=0.0, gangs=[smaller])], pool, Target(start_s=90), startup_s=60)
    assert (p.want, p.action) == (1, "hold")


def test_the_pool_maximum_caps_growth_and_is_named():
    pool = Pool(8.0, 32 * GiB, replicas=2, min=1, max=3)
    p = plan(golden(60.0), pool, Target(start_s=65), startup_s=60)
    assert (p.want, p.action) == (3, "grow") and p.binding == "pool max 3 peers" and "65 s" in p.missed


def test_a_pool_below_its_minimum_grows_back_to_it():
    p = plan([node(0)], Pool(8.0, 32 * GiB, replicas=1, min=3, max=8), Target(), startup_s=60)
    assert (p.want, p.action) == (3, "grow") and p.reason.startswith("grow to the pool minimum of 3 peers")


def test_no_peer_is_bought_that_would_mostly_be_starting():
    # 88 calls of 7 s on one peer: the last starts at 70 s. New peers are up
    # at 60 s, so a 30 s target cannot be met, and each would run 7 s of work.
    nodes = [node(0, shapes=[calls(88, 7.0)])]
    p = plan(nodes, Pool(8.0, 32 * GiB, replicas=1, min=1, max=8), Target(start_s=30), startup_s=60)
    assert (p.want, p.action) == (1, "hold") and "30 s" in p.missed
    assert p.binding.startswith("a peer more would run only 7 s of work")


def test_sustained_arrivals_grow_the_pool_and_a_light_load_does_not():
    empty = [node(0)]
    pool = Pool(8.0, 32 * GiB, replicas=1, min=1, max=16)
    assert plan(empty, pool, Target(), startup_s=60, arrivals_core_per_s=4.0).want == 1
    heavy = plan(empty, pool, Target(), startup_s=60, arrivals_core_per_s=32.0)
    assert heavy.action == "grow" and 4 <= heavy.want <= 6, heavy.reason


def test_arrival_rate_reads_one_burst_as_little_sustained_load():
    def snap(queued_core_s, started_core_s):
        n = node(0, shapes=[calls(int(queued_core_s), 1.0)] if queued_core_s else [])
        n.demand["started_core_s_total"] = started_core_s
        return n

    burst = [(0.0, [snap(0, 0)]), (5.0, [snap(400, 0)]), (10.0, [snap(360, 40)])]
    assert arrival_rate(burst) == 400 / 300  # the 300 s window: one burst, not 13 core-s per second ahead
    steady = [(t, [snap(0, 32.0 * t)]) for t in range(0, 301, 5)]
    assert abs(arrival_rate(steady) - 32.0) < 1e-9


def test_arrivals_count_calls_that_moved_to_a_new_peer_and_a_restarted_peers_starts():
    def snap(peer, queued, started, inc=1):
        n = node(int(peer[1:]), shapes=[calls(queued, 1.0)] if queued else [])
        n.demand["started_core_s_total"] = started
        n.incarnation = inc
        return n

    # 100 queued, then 100 started while half the queue moved to a new peer:
    # 100 core-seconds became runnable, not 50.
    moved = [(0.0, [snap("p0", 100, 0)]), (30.0, [snap("p0", 50, 100), snap("p1", 50, 0)])]
    assert arrival_rate(moved) == 100 / 300
    # A restarted peer counts its starts from zero again, not as negative work.
    restarted = [(0.0, [snap("p0", 10, 500, inc=1)]), (30.0, [snap("p0", 10, 20, inc=2)])]
    assert arrival_rate(restarted) == 20 / 300


def test_a_peer_later_than_expected_is_expected_later_still():
    s = Startup((), 60.0)
    assert s.remaining(10.0) == 50.0
    assert s.remaining(100.0) == 50.0 and s.remaining(200.0) == 100.0  # half as long again as so far


def test_more_peers_never_start_a_call_later():
    # Mixed sizes take the per-peer path; the planner's binary search relies on this.
    mixed = [calls(30, 7.0, cpus=2.0), calls(40, 3.0, cpus=0.5), calls(25, 11.0)]
    pool = Pool(8.0, 32 * GiB, replicas=1, min=1, max=20)
    cs, supply, _ = read([node(0, shapes=mixed, releases=[(16.0, 4.0)])], pool)
    waits, late = [], []
    for k in range(20):
        run = _simulate(supply + [(8.0, 32 * GiB, 45.0, [])] * k, cs, 0.25, Target(start_s=20))
        waits.append(run.wait)
        late.append(run.late)
    assert all(b <= a + 1e-9 for a, b in zip(waits, waits[1:])), waits
    assert all(b <= a + 1e-9 for a, b in zip(late, late[1:])), late


def test_the_same_inputs_give_the_same_plan():
    args = (golden(60.0), POOL, Target(start_s=65), 60.0)
    assert plan(*args) == plan(*args)


def test_startup_is_the_90th_percentile_once_there_are_three_samples():
    assert Startup((), 45.0).seconds() == 45.0
    assert Startup((30.0, 50.0), 45.0).seconds() == 45.0
    assert Startup(tuple(float(s) for s in range(10, 110, 10))).seconds() == 90.0


def test_a_forecast_counts_peers_asked_for_and_learns_how_long_they_take():
    f = Forecast(Target(start_s=60), startup_s=60)
    nodes = golden(60.0)
    assert f.look(nodes, 2, 1, 16, now=0.0).want == 4
    f.grew(2, now=0.0)
    # Asked for and not up yet: they count as arriving, so nothing more is asked for.
    p = f.look(nodes, 4, 1, 16, now=10.0)
    assert (p.want, p.action) == (4, "hold"), p.reason
    assert p.inputs["pending"] == [50.0, 50.0]
    # Up at 90 s: a startup sample each; after three, the 90th percentile replaces the default.
    up = nodes + [node(2), node(3)]
    f.look(up, 4, 1, 16, now=90.0)
    assert f.samples == (90.0, 90.0) and f.startup.seconds() == 60
    f.grew(1, now=100.0)
    f.look(up, 5, 1, 16, now=101.0)
    f.look(up + [node(4)], 5, 1, 16, now=220.0)
    assert f.samples == (90.0, 90.0, 120.0) and f.startup.seconds() == 120
    # A shrink by hand drops the newest wait, and a restarted provisioner counts unknown waits from now.
    g = Forecast(Target(start_s=60), startup_s=60)
    assert g.look(nodes, 3, 1, 16, now=5.0).inputs["pending"] == [60.0]
