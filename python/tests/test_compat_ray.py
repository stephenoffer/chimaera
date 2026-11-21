"""chimaera.compat.ray: Ray Core programs on Chimaera.

Option translation is checked without a runtime; the rest runs against the
session's local peer. ``test_corpus_*`` runs whole Ray programs and compares
their output with what real Ray 2.60 printed (recorded by compat_corpus/record.py).
"""

from __future__ import annotations

import ast
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import warnings

import pytest

import chimaera as ch
import chimaera.compat.ray as ray
from chimaera.compat.ray import _options
from chimaera.compat.ray.exceptions import translate

from conftest import BIN, HOME, REPO

SHIM = pathlib.Path(REPO, "python", "chimaera", "compat", "ray")
CORPUS = pathlib.Path(__file__).parent / "compat_corpus"


# ---- translation (no runtime) -------------------------------------------------


def test_task_defaults_match_rays_retry_budget():
    assert _options.translate({}, _options.TASK) == {"max_attempts": 4}
    assert _options.translate({"max_retries": 0}, _options.TASK)["max_attempts"] == 1
    assert _options.translate({"max_retries": 2}, _options.TASK)["max_attempts"] == 3


def test_actor_defaults_never_retry_methods():
    assert _options.translate({}, _options.ACTOR) == {"max_attempts": 1}
    out = _options.translate({"max_task_retries": 2, "max_restarts": 3}, _options.ACTOR)
    assert out == {"max_attempts": 3, "restart": "fresh", "max_restarts": 3}


def test_resource_options_translate():
    out = _options.translate({"num_cpus": 0, "memory": 1 << 20}, _options.TASK)
    assert out["cpus"] == 0.01 and out["memory"] == 1 << 20
    assert _options.translate({"retry_exceptions": True}, _options.TASK)["retry_exceptions"] == (Exception,)
    assert _options.translate({"retry_exceptions": [KeyError]}, _options.TASK)["retry_exceptions"] == (KeyError,)


@pytest.mark.parametrize(
    ("opts", "names"),
    [
        ({"num_returns": "streaming"}, "generator"),
        ({"accelerator_type": "Ascend910B"}, "no device plugin"),
        ({"runtime_env": {"pip": ["x"]}}, "ch.Env"),
        ({"label_selector": {"zone": "!a"}}, "Placement"),
    ],
)
def test_unmapped_options_name_the_alternative(opts, names):
    with pytest.raises(ray.Unsupported, match=names):
        _options.translate(opts, _options.TASK)


def test_p2_options_map_onto_chimaera():
    assert _options.translate({"num_returns": 3}, _options.TASK)["returns"] == 3
    assert _options.translate({"concurrency_groups": {"io": 2}}, _options.ACTOR)["groups"] == {"io": 2}


def test_node_affinity_and_labels_become_placement():
    from chimaera.compat.ray.util.scheduling_strategies import (
        In,
        NodeAffinitySchedulingStrategy,
        NodeLabelSchedulingStrategy,
        NotIn,
    )

    hard = _options.translate({"scheduling_strategy": NodeAffinitySchedulingStrategy("ab12", soft=False)}, _options.TASK)
    assert hard["placement"] == ch.Placement(require={"node": "ab12"})
    soft = _options.translate({"scheduling_strategy": NodeAffinitySchedulingStrategy("ab12", soft=True)}, _options.TASK)
    assert soft["placement"] == ch.Placement(prefer=("node=ab12",))
    both = _options.translate(
        {
            "label_selector": {"zone": "a"},
            "scheduling_strategy": NodeLabelSchedulingStrategy(hard={"rack": In("r7")}, soft={"disk": In("ssd")}),
        },
        _options.TASK,
    )
    assert both["placement"] == ch.Placement(require={"zone": "a", "rack": "r7"}, prefer=("disk=ssd",))
    with pytest.raises(ray.Unsupported, match="exact label values"):
        _options.translate({"scheduling_strategy": NodeLabelSchedulingStrategy(hard={"rack": NotIn("r7")})}, _options.TASK)


@ray.remote
def node_of_call():
    return ray.get_runtime_context().get_node_id()


def test_nodes_resources_and_node_affinity_on_a_live_peer():
    from chimaera.compat.ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    ray.init(ignore_reinit_error=True)
    try:
        [node] = ray.nodes()
        assert node["Alive"] and node["Resources"]["CPU"] >= 1 and node["Labels"]["ray.io/node_id"] == node["NodeID"]
        assert ray.get_runtime_context().get_node_id() == node["NodeID"]
        assert ray.cluster_resources()["CPU"] == node["Resources"]["CPU"]
        assert 0 <= ray.available_resources()["CPU"] <= node["Resources"]["CPU"]
        pinned = node_of_call.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node["NodeID"], soft=False))
        assert ray.get(pinned.remote(), timeout=60) == node["NodeID"]
        # Ray leaves such a task pending forever; Chimaera says why it can't run.
        nowhere = node_of_call.options(scheduling_strategy=NodeAffinitySchedulingStrategy("00" * 16, soft=False))
        with pytest.raises(ch.Infeasible, match="no live peer has labels node=0+"):
            ray.get(nowhere.remote(), timeout=60)
    finally:
        ray.shutdown()


def test_ray_accelerator_types_become_device_selectors():
    cases = {
        "A100": "cuda:A100",
        "A100-80G": "cuda:A100",
        "H100": "cuda:H100",
        "RTX-PRO-6000": "cuda:RTX PRO 6000",
        "TPU-V4": "tpu:v4",
        "TPU-V5LITEPOD": "tpu:v5e",
        "AMD-Instinct-MI300A": "rocm:MI300A",
        "AMD-Instinct-MI250X-MI250": "rocm:MI250",
        "aws-neuron-core": "neuron",
        "Intel-GAUDI": "hpu",
        "Intel-GPU-Max-1550": "xpu",
        "M3-Pro": "mps:M3 Pro",
        "Apple-Silicon": "mps",
    }
    for ray_name, device in cases.items():
        assert _options.translate({"accelerator_type": ray_name}, _options.TASK)["device"] == device, ray_name
        ch.remote(device=device)(lambda: None)  # a selector Chimaera accepts


@ray.remote
def gpu_ids():
    return ray.get_gpu_ids(), ray.get_runtime_context().get_accelerator_ids()


@pytest.mark.skipif(not (sys.platform == "darwin" and os.uname().machine == "arm64"), reason="needs an Apple GPU")
def test_accelerator_type_and_gpu_ids_on_this_mac():
    import platform

    brand = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout
    model = brand.strip().removeprefix("Apple ").replace(" ", "-")  # e.g. M3-Pro, Ray's own name
    ray.init(ignore_reinit_error=True)
    try:
        assert ray.get(gpu_ids.remote(), timeout=60) == ([], {})  # no GPU asked for
        ids, accel = ray.get(gpu_ids.options(accelerator_type=model).remote(), timeout=60)
        assert ids == [0] and accel == {"GPU": ["0"]}, (platform.machine(), model)
        with pytest.raises(ch.Infeasible, match="cuda"):
            ray.get(gpu_ids.options(accelerator_type="A100").remote(), timeout=60)
    finally:
        ray.shutdown()


def test_unknown_option_is_a_value_error_as_in_ray():
    with pytest.raises(ValueError, match="Invalid option keyword bogus"):
        _options.translate({"bogus": 1}, _options.TASK)
    with pytest.raises(ValueError, match="for actors"):
        _options.translate({"max_retries": 1}, _options.ACTOR)


def test_accepted_but_ignored_options_warn_once(monkeypatch):
    monkeypatch.setattr("chimaera.compat.ray._unsupported._warned", set())
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        _options.translate({"max_calls": 5}, _options.TASK)
        _options.translate({"max_calls": 5}, _options.TASK)
    assert [str(w.message).split(":")[0] for w in seen] == ["CHR001"]
    assert issubclass(seen[0].category, ray.ChimaeraCompatWarning)


def test_named_actor_key_is_scoped_to_the_namespace():
    assert _options.actor_key({}, lambda: "ns") is None
    assert _options.actor_key({"name": "a", "namespace": "team", "get_if_exists": True}, lambda: "ns") == "team/a"
    assert _options.actor_key({"name": "a", "get_if_exists": True}, lambda: "ns") == "ns/a"


def test_unknown_attributes_say_what_to_use_and_keep_hasattr_working():
    assert not hasattr(ray, "timeline")
    with pytest.raises(ray.Unsupported, match="chimaera explain"):
        ray.timeline  # noqa: B018
    with pytest.raises(ray.Unsupported, match="Ray Core only"):
        ray.data  # noqa: B018


def test_task_errors_are_dual_typed():
    cause = ValueError("bad")
    cause.add_note("remote traceback (attempt 1):\n...")
    err = translate(cause)
    assert isinstance(err, ray.exceptions.RayTaskError) and isinstance(err, ValueError)
    assert type(err).__name__ == "RayTaskError(ValueError)"
    assert err.args == ("bad",) and err.cause is cause
    assert err.__notes__ == cause.__notes__  # copied once, not duplicated
    assert isinstance(translate(ch.Cancelled("x")), ray.exceptions.TaskCancelledError)
    assert isinstance(translate(ch.ActorDied("x")), ray.exceptions.ActorDiedError)
    assert isinstance(translate(ch.WorkerCrashed("x")), ray.exceptions.WorkerCrashedError)
    assert translate(ch.Infeasible("no")) is None  # no Ray equivalent: Chimaera's error stands


def test_shim_uses_only_the_public_chimaera_api():
    """The shim must survive internal refactors: no ``chimaera._*`` imports."""
    offenders = []
    for path in SHIM.rglob("*.py"):
        package = "chimaera.compat.ray" + "".join(f".{p}" for p in path.relative_to(SHIM).parent.parts)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if node.level:
                    parent = package.split(".")[: len(package.split(".")) - node.level + 1]
                    base = ".".join(parent + ([base] if base else []))
                names = [base] + [f"{base}.{a.name}" for a in node.names] if base == "chimaera" else [base]
            else:
                continue
            for name in names:
                parts = name.split(".")
                if parts[0] == "chimaera" and not name.startswith("chimaera.compat.ray"):
                    if any(p.startswith("_") for p in parts[1:]):
                        offenders.append(f"{path.name}:{node.lineno} {name}")
    assert offenders == []


# ---- against the local peer ------------------------------------------------------


@ray.remote
def square(x):
    return x * x


@ray.remote
def total(arr):
    return int(sum(arr))


@ray.remote
def fail(msg):
    raise ValueError(msg)


@ray.remote
def nap(seconds):
    time.sleep(seconds)
    return seconds


@ray.remote
class Counter:
    def __init__(self, start=0):
        self.n = start

    def incr(self, by=1):
        self.n += by
        return self.n

    def add_all(self, values):
        self.n += sum(values)
        return self.n


@ray.remote
class Signal:
    def __init__(self):
        import asyncio

        self.event = asyncio.Event()

    async def send(self):
        self.event.set()

    async def wait(self):
        await self.event.wait()
        return "released"


def test_remote_get_and_dependencies():
    assert ray.get([square.remote(i) for i in range(5)]) == [0, 1, 4, 9, 16]
    assert ray.get(square.remote(square.remote(3))) == 81
    with pytest.raises(TypeError, match="cannot be called directly"):
        square(3)


def test_put_references_arrive_as_values_at_top_level():
    ref = ray.put(list(range(1000)))
    assert isinstance(ref, ray.ObjectRef)
    assert ray.get(total.remote(ref)) == sum(range(1000))
    assert ray.get(total.remote([ray.get(r) for r in [ref]][0])) == sum(range(1000))
    c = Counter.remote()
    assert ray.get(c.add_all.remote(ref)) == sum(range(1000))


def test_task_error_is_catchable_as_both_types():
    with pytest.raises(ray.exceptions.RayTaskError) as info:
        ray.get(fail.remote("boom"))
    assert isinstance(info.value, ValueError) and "boom" in str(info.value)
    with pytest.raises(ValueError):
        ray.get(square.remote(fail.remote("upstream")))


def test_get_timeout_and_cancel():
    slow = nap.remote(30)
    with pytest.raises(ray.exceptions.GetTimeoutError) as info:
        ray.get(slow, timeout=0.2)
    assert isinstance(info.value, TimeoutError)
    ray.cancel(slow, force=True)
    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(slow, timeout=30)


@ray.remote
def quick():
    return 0


def test_wait_keeps_input_order_and_honors_num_returns():
    # The slow call is a different function from the quick ones, so the peer
    # never batches it with them (a batch would finish all three together).
    refs = [nap.remote(1.5), quick.remote(), quick.remote()]
    ready, rest = ray.wait(refs, num_returns=2, timeout=10)
    assert ready == [refs[1], refs[2]] and rest == [refs[0]]
    ready, rest = ray.wait([nap.remote(5)], timeout=0)
    assert ready == [] and len(rest) == 1
    with pytest.raises(ValueError, match="unique"):
        ray.wait([refs[0], refs[0]])
    assert ray.wait([]) == ([], [])


def test_named_actor_is_shared_and_kill_ends_it():
    a = Counter.options(name="shared", get_if_exists=True).remote()
    b = Counter.options(name="shared", get_if_exists=True).remote()
    ray.get(a.incr.remote(5))
    assert ray.get(b.incr.remote(1)) == 6
    ray.kill(a)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.incr.remote(), timeout=30)


def test_async_actor_gets_rays_default_concurrency():
    s = Signal.remote()
    waiting = s.wait.remote()
    assert ray.wait([waiting], timeout=0.3)[0] == []
    ray.get(s.send.remote(), timeout=30)  # with concurrency 1 this would queue behind wait()
    assert ray.get(waiting, timeout=30) == "released"


def test_actor_pool_orders_and_reuses_actors():
    pool = ray.util.ActorPool([Counter.remote() for _ in range(2)])
    # Two actors, four calls: each actor runs two, in submission order.
    assert list(pool.map(lambda a, v: a.incr.remote(v), [1, 1, 1, 1])) == [1, 1, 2, 2]
    assert sorted(pool.map_unordered(lambda a, v: a.incr.remote(v), [10, 10])) == [12, 12]
    pool = ray.util.ActorPool([Counter.remote(100), Counter.remote(200)])
    pool.submit(lambda a, v: a.incr.remote(v), 1)
    pool.submit(lambda a, v: a.incr.remote(v), 1)
    pool.submit(lambda a, v: a.incr.remote(v), 1)  # waits for a free actor
    assert not pool.has_free()
    got = sorted([pool.get_next(), pool.get_next(), pool.get_next()])
    assert got in ([101, 102, 201], [101, 201, 202])
    assert not pool.has_next() and pool.has_free()
    with pytest.raises(StopIteration):
        pool.get_next()


def test_runtime_context_reports_the_run():
    assert ray.get_runtime_context().get_job_id() == ch.current_run().id
    assert ray.get_runtime_context().get_task_id() is None


def test_init_rejects_what_does_not_map():
    with pytest.raises(ray.Unsupported, match="Ray Client"):
        ray.init("ray://head:10001")
    with pytest.raises(ray.Unsupported, match="local_mode"):
        ray.init(local_mode=True)
    with pytest.raises(TypeError, match="unexpected keyword"):
        ray.init(not_an_option=1)


# ---- whole programs ---------------------------------------------------------------


def _corpus() -> list[pathlib.Path]:
    return sorted(CORPUS.glob("ex_*.py"))


@pytest.mark.parametrize("program", _corpus(), ids=lambda p: p.stem)
def test_corpus_matches_ray(program):
    expected = program.with_suffix(".expected")
    assert expected.exists(), f"record {expected.name} with compat_corpus/record.py"
    env = dict(os.environ, CHIMAERA_HOME=HOME, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
    proc = subprocess.run(
        [sys.executable, "-m", "chimaera.compat.ray", str(program)],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=tempfile.mkdtemp(prefix="chr", dir="/tmp"),
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    assert proc.stdout == expected.read_text(), proc.stderr[-3000:]


def test_corpus_matches_ray_on_a_three_peer_cluster():
    """The same programs, driven from one peer of a formed three-peer cluster."""
    from test_cluster import Cluster

    cluster = Cluster(3)
    try:
        differ = {}
        for program in _corpus():
            env = dict(os.environ, CHIMAERA_HOME=cluster.dir(0), CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
            proc = subprocess.run(
                [sys.executable, "-m", "chimaera.compat.ray", str(program)],
                capture_output=True,
                text=True,
                timeout=180,
                env=env,
                cwd=tempfile.mkdtemp(prefix="chr", dir="/tmp"),
            )
            if proc.returncode != 0 or proc.stdout != program.with_suffix(".expected").read_text():
                differ[program.stem] = proc.stderr[-1500:] or proc.stdout[-1500:]
        assert not differ, differ
    finally:
        cluster.close()


def test_script_can_import_its_sibling_modules(tmp_path):
    """`python -m chimaera.compat.ray app.py` puts app.py's folder first on sys.path, as `python app.py` does."""
    (tmp_path / "helper.py").write_text("def double(x):\n    return 2 * x\n")
    (tmp_path / "app.py").write_text(
        "import ray\nfrom helper import double\n\n@ray.remote\ndef f(x):\n    return double(x)\n\nprint(ray.get(f.remote(21)))\n"
    )
    env = dict(os.environ, CHIMAERA_HOME=HOME, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
    cwd = tempfile.mkdtemp(prefix="chr", dir="/tmp")  # not the script's folder
    proc = subprocess.run(
        [sys.executable, "-m", "chimaera.compat.ray", str(tmp_path / "app.py")],
        capture_output=True, text=True, timeout=120, env=env, cwd=cwd,
    )
    assert proc.stdout.strip() == "42", proc.stderr[-2000:]


def test_installed_submodules_are_the_shim_itself():
    code = (
        "import chimaera.compat.ray as shim; shim.install()\n"
        "import ray, ray.exceptions, ray.util.scheduling_strategies as s\n"
        "assert ray is shim and ray.exceptions is shim.exceptions\n"
        "assert s.NodeAffinitySchedulingStrategy is shim.util.scheduling_strategies.NodeAffinitySchedulingStrategy\n"
        "print('ok')\n"
    )
    env = dict(os.environ, PYTHONPATH=os.path.join(REPO, "python"))
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=60)
    assert out.stdout.strip() == "ok", out.stderr


def test_threads_can_share_the_shim():
    results = []

    def work():
        results.append(ray.get(square.remote(7)))

    threads = [threading.Thread(target=work) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert results == [49] * 4


_SEED = []


def _remember(x):
    _SEED.append(x)


def _seeded(x):
    return x * 10 + _SEED[0]  # set by the initializer, once per worker


def _boom(x):
    if x == 3:
        raise ValueError("three")
    return x


def _add(a, b):
    return a + b


def test_multiprocessing_pool_matches_the_standard_library():
    from chimaera.compat.ray.util.multiprocessing import Pool

    ray.init(ignore_reinit_error=True)
    try:
        with Pool(processes=3, initializer=_remember, initargs=(7,)) as pool:
            assert pool.map(_seeded, range(20)) == [x * 10 + 7 for x in range(20)]
            assert list(pool.imap(_seeded, range(5))) == [7, 17, 27, 37, 47]
            assert sorted(pool.imap_unordered(_seeded, range(5), chunksize=2)) == [7, 17, 27, 37, 47]
            assert pool.starmap(_add, [(1, 2), (3, 4)]) == [3, 7]
            assert pool.apply(_add, (5, 6)) == 11
            got, errors = [], []
            ok = pool.apply_async(_add, (1, 1), callback=got.append)
            bad = pool.map_async(_boom, range(5), error_callback=errors.append)
            assert ok.get(timeout=60) == 2
            with pytest.raises(ValueError, match="three"):
                bad.get(timeout=60)
            ok.wait(5)
            bad.wait(5)
            assert ok.successful() and not bad.successful()
            assert got == [2] and isinstance(errors[0], ValueError)
            pool.close()
            with pytest.raises(ValueError, match="not running"):
                pool.map(_seeded, [1])
            pool.join()
    finally:
        ray.shutdown()


@ray.remote
def pg_where():
    return ray.get_runtime_context().get_node_id()


@ray.remote
class PgCounter:
    def __init__(self):
        self.n = 0

    def add(self):
        self.n += 1
        return self.n


def test_placement_groups_run_tasks_and_actors_in_their_bundles():
    from chimaera.compat.ray.util.placement_group import get_placement_group, placement_group, remove_placement_group
    from chimaera.compat.ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    ray.init(ignore_reinit_error=True)
    try:
        pg = placement_group([{"CPU": 1}, {"CPU": 1}], strategy="PACK", name="pg-test")
        assert ray.get(pg.ready(), timeout=60) and pg.wait(10) and pg.bundle_count == 2
        assert get_placement_group("pg-test") is pg
        [node] = [n["NodeID"] for n in ray.nodes()]
        in_bundle = PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=0)
        assert ray.get(pg_where.options(scheduling_strategy=in_bundle).remote(), timeout=60) == node
        counter = PgCounter.options(scheduling_strategy=PlacementGroupSchedulingStrategy(pg, 1)).remote()
        assert ray.get([counter.add.remote() for _ in range(3)], timeout=60) == [1, 2, 3]
        anywhere = PlacementGroupSchedulingStrategy(pg, -1)
        assert ray.get([pg_where.options(scheduling_strategy=anywhere).remote() for _ in range(4)], timeout=60) == [node] * 4
        remove_placement_group(pg)
        with pytest.raises(ValueError, match="Failed to look up"):
            get_placement_group("pg-test")
        with pytest.raises(ValueError, match="Invalid placement group strategy"):
            placement_group([{"CPU": 1}], strategy="TIGHT")
    finally:
        ray.shutdown()


@ray.remote
class Collective:
    def declared(self, name, value):
        import numpy as np

        from chimaera.compat.ray.util import collective as col

        x = np.full(3, float(value))
        col.allreduce(x, group_name=name)  # joins the declared group on first use
        top = np.full(3, float(value))
        col.allreduce(top, group_name=name, op=col.ReduceOp.MAX)
        word = np.array([col.get_rank(name) * 10.0])
        col.broadcast(word, src_rank=1, group_name=name)
        parts = [np.zeros(1) for _ in range(col.get_collective_group_size(name))]
        col.allgather(parts, np.array([float(value)]), group_name=name)
        col.barrier(name)
        return col.get_rank(name), x.tolist(), top.tolist(), word.tolist(), [p.tolist() for p in parts]

    def joins(self, world, rank, name, value):
        import numpy as np

        from chimaera.compat.ray.util import collective as col

        col.init_collective_group(world, rank, backend="gloo", group_name=name)
        x = np.array([float(value)])
        col.reduce(x, dst_rank=0, group_name=name)
        return col.get_rank(name), col.is_group_initialized(name), x.tolist()


def test_collective_groups_reduce_in_place_like_rays():
    from chimaera.compat.ray.util import collective as col

    actors = [Collective.remote() for _ in range(3)]
    name = f"g{os.getpid()}-{time.monotonic_ns()}"
    col.create_collective_group(actors, 3, [2, 0, 1], backend="gloo", group_name=name)
    with pytest.raises(RuntimeError, match="twice"):
        col.create_collective_group(actors, 3, [0, 1, 2], group_name=name)
    out = ray.get([a.declared.remote(name, v) for a, v in zip(actors, (1, 2, 4))])
    # Ranks follow `ranks`: actor 0 is rank 2. Sums and maxima are on every rank, in place.
    assert [o[0] for o in out] == [2, 0, 1]
    assert all(o[1] == [7.0] * 3 and o[2] == [4.0] * 3 and o[3] == [10.0] for o in out)
    assert all(o[4] == [[2.0], [4.0], [1.0]] for o in out), "gathered in rank order"
    joined = ray.get([a.joins.remote(3, r, name + "-init", r + 1) for r, a in enumerate(actors)])
    assert joined == [(0, True, [6.0]), (1, True, [2.0]), (2, True, [3.0])], "only the destination gets the sum"
    with pytest.raises(NotImplementedError, match="send is not supported"):
        col.send  # noqa: B018
    for a in actors:
        ray.kill(a)


@ray.remote
def one():
    return 1


@ray.remote
def ref_kinds(xs, named=None):
    return [isinstance(x, ray.ObjectRef) for x in xs], ray.get(xs), sorted(named or {})


@ray.remote
class RefKinds:
    def __init__(self, xs):
        self.were_refs = [isinstance(x, ray.ObjectRef) for x in xs]

    def check(self, xs):
        return self.were_refs, [isinstance(x, ray.ObjectRef) for x in xs]


def test_refs_inside_a_list_arrive_as_refs_as_in_ray():
    refs = [one.remote(), ray.put(2)]  # a task's result and a put
    assert ray.get(ref_kinds.remote(refs, named={"a": refs[0]})) == ([True, True], [1, 2], ["a"])
    a = RefKinds.remote(tuple(refs))
    assert ray.get(a.check.remote(refs)) == ([True, True], [True, True])
    ray.kill(a)
