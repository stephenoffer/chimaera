"""Ray side of the evals. Each workload runs in a fresh process."""

from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import time

import numpy as np
import ray

import common
from common import (
    ACTOR_CALLS, CHAINS, CORES, DEPTH, LARGE_BYTES, LARGE_READERS, LATENCY_CALLS, MAP_ITEMS, NESTED_CHILDREN,
    NESTED_PARENTS, RAY_BATCH, SCALING_TASK_S, SCALING_TASKS, TASKS, Timer, busy, summary_us,
)


# Ray workers do not have bench/evals on sys.path; ship the helpers by value.
ray.cloudpickle.register_pickle_by_value(common)


@ray.remote
def noop(x):
    return x


@ray.remote
def double(x):
    return x * 2


@ray.remote
def double_batch(xs):
    return [x * 2 for x in xs]


@ray.remote
def add1(x):
    return x + 1


@ray.remote
def burn(s):
    return busy(s)


@ray.remote
def total(a):
    return float(a.sum())


@ray.remote
class Log:
    def __init__(self):
        self.items = []

    def append(self, i):
        self.items.append(i)
        return i

    def get(self):
        return self.items


@ray.remote
def child(x):
    return x


@ray.remote
def parent(n):
    return sum(ray.get([child.remote(i) for i in range(n)]))


def w_latency():
    ray.get([noop.remote(i) for i in range(100)])
    lat = []
    for i in range(LATENCY_CALLS):
        t = time.perf_counter()
        ray.get(noop.remote(i))
        lat.append(time.perf_counter() - t)
    return summary_us(lat)


def w_tasks():
    with Timer() as t:
        out = ray.get([noop.remote(i) for i in range(TASKS)])
    assert out[-1] == TASKS - 1
    return {"tasks_per_s": round(TASKS / t.s), "seconds": round(t.s, 3)}


def w_map_small():
    with Timer() as naive:
        out = ray.get([double.remote(i) for i in range(MAP_ITEMS)])
    assert out[-1] == 2 * (MAP_ITEMS - 1)
    with Timer() as tuned:
        chunks = [list(range(i, min(i + RAY_BATCH, MAP_ITEMS))) for i in range(0, MAP_ITEMS, RAY_BATCH)]
        out = [y for part in ray.get([double_batch.remote(c) for c in chunks]) for y in part]
    assert len(out) == MAP_ITEMS
    return {"naive_items_per_s": round(MAP_ITEMS / naive.s), "hand_batched_items_per_s": round(MAP_ITEMS / tuned.s)}


def w_dag_chains():
    with Timer() as t:
        heads = []
        for c in range(CHAINS):
            ref = add1.remote(c)
            for _ in range(DEPTH - 1):
                ref = add1.remote(ref)
            heads.append(ref)
        out = ray.get(heads)
    assert out[0] == DEPTH
    return {"seconds": round(t.s, 3), "steps_per_s": round(CHAINS * DEPTH / t.s)}


def w_cpu_scaling():
    ray.get([noop.remote(i) for i in range(CORES * 2)])
    with Timer() as t:
        ray.get([burn.remote(SCALING_TASK_S) for _ in range(SCALING_TASKS)])
    ideal = SCALING_TASKS * SCALING_TASK_S / CORES
    return {"seconds": round(t.s, 3), "efficiency": round(ideal / t.s, 3)}


def w_large_object():
    a = np.ones(LARGE_BYTES // 8)
    with Timer() as t:
        ref = ray.put(a)
        out = ray.get([total.remote(ref) for _ in range(LARGE_READERS)])
    assert all(x == len(a) for x in out)
    return {"seconds": round(t.s, 3)}


def w_actor_calls():
    log = Log.remote()
    with Timer() as t:
        ray.get([log.append.remote(i) for i in range(ACTOR_CALLS)])
    ordered = ray.get(log.get.remote()) == list(range(ACTOR_CALLS))
    return {"calls_per_s": round(ACTOR_CALLS / t.s), "in_submission_order": ordered}


def w_nested():
    with Timer() as t:
        out = ray.get([parent.remote(NESTED_CHILDREN) for _ in range(NESTED_PARENTS)], timeout=300)
    assert out[0] == sum(range(NESTED_CHILDREN))
    return {"seconds": round(t.s, 3)}


@ray.remote
def die_once(marker):
    if not os.path.exists(marker):
        open(marker, "w").close()
        os.kill(os.getpid(), signal.SIGKILL)
    return "recovered"


@ray.remote
def charge(path):
    with open(path, "a") as f:
        f.write("charged\n")
    os.kill(os.getpid(), signal.SIGKILL)


def w_worker_kill():
    marker = os.path.join(tempfile.mkdtemp(), "m")
    with Timer() as t:
        out = ray.get(die_once.remote(marker), timeout=120)
    return {"result": out, "seconds_to_recover": round(t.s, 3)}


def w_external_effect():
    res = {}
    for label, opts in (("default", {}), ("max_retries_0", {"max_retries": 0})):
        path = os.path.join(tempfile.mkdtemp(), "ledger")
        try:
            ray.get(charge.options(**opts).remote(path), timeout=120)
            outcome = "returned"
        except Exception as e:
            outcome = type(e).__name__
        res[label] = {"outcome": outcome, "side_effects": open(path).read().count("charged")}
    return res


@ray.remote(memory=1 << 30)
def hog():
    data = b"\x01" * (2 << 30)
    time.sleep(1)
    return len(data)


def w_memory_limit():
    try:
        out = ray.get(hog.remote(), timeout=180)
        return {"outcome": "completed", "limit_enforced": False, "detail": f"allocated {out >> 20} MiB with memory=1 GiB"}
    except Exception as e:
        return {"outcome": type(e).__name__, "limit_enforced": True}


def w_accounting():
    ref = burn.remote(0.5)
    ray.get(ref)
    # Ray's state API lists tasks but has no per-task CPU or memory measurement.
    from ray.util import state

    # The cluster holds every earlier workload's tasks; take a page of them.
    t = list(state.list_tasks(filters=[("name", "=", "burn")], detail=True, limit=10, raise_on_missing_output=False))
    fields = sorted(vars(t[0])) if t else []  # TaskState is a dataclass
    has_cpu = any("cpu" in f.lower() and "num" not in f.lower() for f in fields)
    return {"per_task_cpu_seconds": "unavailable" if not has_cpu else "present", "state_api_fields": len(fields)}


def w_infeasible():
    with Timer() as t:
        try:
            ray.get(noop.options(num_cpus=CORES * 2).remote(1), timeout=20)
            outcome = "completed"
        except Exception as e:
            outcome = type(e).__name__
    return {"outcome": outcome, "seconds_to_answer": round(t.s, 2)}


WORKLOADS = {k[2:]: v for k, v in globals().items() if k.startswith("w_")}

if __name__ == "__main__":
    name = sys.argv[1]
    if os.environ.get("RAY_ADDRESS"):
        ray.init(log_to_driver=False)  # join the node's running Ray cluster (Anyscale)
    else:
        ray.init(log_to_driver=False, include_dashboard=False)
    try:
        result = WORKLOADS[name]()
        print(json.dumps({"runtime": "ray", "version": ray.__version__, "workload": name, "result": result}))
    finally:
        ray.shutdown()
