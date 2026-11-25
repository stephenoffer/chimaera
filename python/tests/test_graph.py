"""Compiled actor graphs (ch.dist.graph): each actor runs its part of every
invocation itself, values go straight from actor to actor, and nothing is
submitted per invocation."""

import asyncio
import os
import shutil
import signal
import socket
import tempfile
import threading
import time
import types

import pytest

import chimaera as ch
from chimaera import _graph
from test_cluster import Cluster, lines


@ch.remote
class Stage:
    def __init__(self, name):
        self.name, self.calls = name, 0

    def step(self, x):
        self.calls += 1
        return x + [self.name]

    def pair(self, a, b):
        return (a, b)

    def plus(self, x, n):
        return x + n

    def fail_on(self, x, bad):
        if x == bad:
            raise ValueError(f"bad input {x}")
        return x * 10

    def slow(self, x, delay):
        time.sleep(delay)
        return x

    async def later(self, x):
        await asyncio.sleep(0.001)
        return x + 1

    def count(self):
        return self.calls

    def pid(self):
        return os.getpid()


def test_a_pipeline_compiles_to_the_results_of_its_calls_and_submits_none_per_invocation():
    a, b = Stage.remote("a"), Stage.remote("b")

    @ch.dist.graph
    def pipe(x):
        return b.step.remote(a.step.remote(x))

    assert pipe.remote([0]).result(timeout=60) == [0, "a", "b"]
    run = ch.runs.current_run()
    with pipe.compile(max_in_flight=4) as g:
        before = len(run.tasks())
        futs = [g.remote([i]) for i in range(200)]
        assert [f.result(timeout=60) for f in futs] == [[i, "a", "b"] for i in range(200)]
        assert len(run.tasks()) - before < 10, "200 invocations should submit no calls"
        assert ".step(in0)" in g.explain() and ".step(n0)" in g.explain()
    # Closed: the actors take ordinary calls again.
    assert a.count.remote().result(timeout=60) == 201


def test_fan_out_and_in_keyword_arguments_two_outputs_and_one_actor_twice():
    a, b, c = Stage.remote("a"), Stage.remote("b"), Stage.remote("c")

    @ch.dist.graph
    def diamond(x, y):
        left = a.step.remote(x)
        both = c.pair.remote(left, b=b.step.remote(y))
        return both, a.step.remote(left)  # a again, on its own first result

    with diamond.compile() as g:
        for i in range(20):
            assert g.remote([i], [-i]).result(timeout=60) == (([i, "a"], [-i, "b"]), [i, "a", "a"])
        with pytest.raises(TypeError, match="takes 2 arguments"):
            g.remote([1])


def test_an_error_fails_only_its_own_invocation_and_async_methods_run():
    a, b = Stage.remote("a"), Stage.remote("b")

    @ch.dist.graph
    def g(x):
        return b.later.remote(b.plus.remote(a.fail_on.remote(x, 3), 1))

    with g.compile() as cg:
        futs = [cg.remote(i) for i in range(6)]
        for i, f in enumerate(futs):
            if i == 3:
                with pytest.raises(ValueError, match="bad input 3"):
                    f.result(timeout=60)
            else:
                assert f.result(timeout=60) == i * 10 + 2


def test_max_in_flight_holds_back_the_next_invocation():
    a = Stage.remote("a")

    @ch.dist.graph
    def g(x):
        return a.slow.remote(x, 0.5)

    with g.compile(max_in_flight=2) as cg:
        first, _ = cg.remote(1), cg.remote(2)
        started = threading.Event()
        threading.Thread(target=lambda: (cg.remote(3), started.set()), daemon=True).start()
        assert not started.wait(0.3), "a third invocation waits for one of the two to finish"
        assert first.result(timeout=60) == 1
        assert started.wait(30)


def test_a_dead_actor_breaks_the_graph_at_once_and_frees_the_others():
    a, b = Stage.remote("a"), Stage.remote("b")
    pid = b.pid.remote().result(timeout=60)

    @ch.dist.graph
    def g(x):
        return b.slow.remote(a.step.remote(x), 60)

    cg = g.compile()
    f = cg.remote([1])
    time.sleep(0.5)
    os.kill(pid, signal.SIGKILL)
    t = time.monotonic()
    with pytest.raises(ch.errors.ActorDied):
        f.result(timeout=30)
    assert time.monotonic() - t < 10, "the driver hears of the death from its link, not a timeout"
    with pytest.raises(ch.errors.ActorDied):
        cg.remote([2])
    cg.close()
    assert a.count.remote().result(timeout=60) == 1, "the actor still alive is free again"


def test_a_connection_that_never_answers_the_key_holds_up_no_other(monkeypatch):
    # Before each of the driver's links, a socket that connects to the same
    # listener and says nothing: the challenge waiting on it must not keep
    # the real link out.
    a, b = Stage.remote("a"), Stage.remote("b")
    silent, real = [], _graph.Client

    def a_silent_one_first(address, authkey=None):
        s = socket.socket(socket.AF_UNIX if isinstance(address, str) else socket.AF_INET)
        s.connect(address)
        silent.append(s)
        return real(address, authkey=authkey)

    monkeypatch.setattr(_graph, "Client", a_silent_one_first)

    @ch.dist.graph
    def pipe(x):
        return b.step.remote(a.step.remote(x))

    got: list = []

    def run():
        with pipe.compile() as g:
            got.append(g.remote([1]).result(timeout=30))

    threading.Thread(target=run, daemon=True).start()
    deadline = time.monotonic() + 45
    while not got and time.monotonic() < deadline:
        time.sleep(0.05)
    assert got == [[1, "a", "b"]], "a connection that never answered held up the graph's setup"
    assert len(silent) == 2
    for s in silent:
        s.close()


def test_connections_past_the_handshake_cap_are_closed_at_once(monkeypatch):
    # Each connection proving the key holds a thread for up to HANDSHAKE_S:
    # past the cap, one is closed before it is challenged.
    monkeypatch.setattr(_graph, "HANDSHAKES", 2)
    home = tempfile.mkdtemp(prefix="chg", dir="/tmp")
    welcome = {"peer_socket": os.path.join(home, "peer.sock")}
    executor = types.SimpleNamespace(conn=types.SimpleNamespace(welcome=welcome))
    server = _graph._Server(executor, os.urandom(16), os.urandom(32), tcp=False)
    socks = []
    try:
        for _ in range(3):
            s = socket.socket(socket.AF_UNIX)
            s.connect(server.info["unix"])
            s.settimeout(5)
            socks.append(s)
            time.sleep(0.2)  # accepted in turn
        assert socks[0].recv(1) != b"", "a connection within the cap is challenged"
        assert socks[2].recv(1) == b"", "one past the cap is closed, not challenged"
    finally:
        server.close()
        for s in socks:
            s.close()
        shutil.rmtree(home, ignore_errors=True)


def test_graph_values_inside_containers_or_given_to_functions_are_refused():
    a = Stage.remote("a")

    @ch.remote
    def double(x):
        return x * 2

    @ch.dist.graph
    def nested(x):
        return a.step.remote([x])

    @ch.dist.graph
    def to_function(x):
        return a.step.remote(double.remote(x))

    for g in (nested, to_function):
        with pytest.raises(TypeError, match="direct arguments"):
            g.compile()
    assert a.count.remote().result(timeout=60) == 0, "a refused graph leaves its actor free"


def test_links_between_hosts_need_cross_host_and_then_carry_the_values():
    # Peers told apart as two hosts: the driver and s0 on h0, s1 on h1.
    c = Cluster(
        2,
        labels={i: f"slot=s{i}" for i in range(2)},
        env={i: {"CHIMAERA_GRAPH_HOST": f"h{i}"} for i in range(2)},
    )
    try:
        out = c.run(
            0,
            """
import os
os.environ["CHIMAERA_GRAPH_HOST"] = "h0"
import chimaera as ch

@ch.remote
class Stage:
    def step(self, x):
        return x + 1

s0 = Stage.options(placement=ch.dist.Placement(require={"slot": "s0"})).remote()
s1 = Stage.options(placement=ch.dist.Placement(require={"slot": "s1"})).remote()

@ch.dist.graph
def g(x):
    return s1.step.remote(s0.step.remote(x))

try:
    g.compile()
except ValueError as e:
    print("REFUSED", "cross_host=True" in str(e))
with g.compile(cross_host=True) as cg:
    print("SUM", sum(cg.remote(i).result(timeout=60) for i in range(50)))
    print("PLAN", "on h1" in cg.explain())
""",
        )
        assert lines(out, "REFUSED") == ["True"]
        assert lines(out, "SUM") == [str(sum(i + 2 for i in range(50)))]
        assert lines(out, "PLAN") == ["True"]
    finally:
        c.close()
