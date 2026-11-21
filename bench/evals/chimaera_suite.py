"""Chimaera side of the evals. Each workload runs in a fresh process."""

from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import time

import numpy as np

import chimaera as ch
from common import (
    ACTOR_CALLS, CHAINS, CORES, DEPTH, LARGE_BYTES, LARGE_READERS, LATENCY_CALLS, MAP_ITEMS, NESTED_CHILDREN,
    NESTED_PARENTS, SCALING_TASK_S, SCALING_TASKS, TASKS, Timer, busy, summary_us,
)


@ch.remote
def noop(x):
    return x


@ch.remote
def double(x):
    return x * 2


@ch.remote
def add1(x):
    return x + 1


@ch.remote
def burn(s):
    return busy(s)


@ch.remote
def total(a):
    return float(a.sum())


@ch.remote
class Log:
    def __init__(self):
        self.items = []

    def append(self, i):
        self.items.append(i)
        return i

    def get(self):
        return self.items


@ch.remote
def child(x):
    return x


@ch.remote
def parent(n):
    return sum(f.result() for f in [child.remote(i) for i in range(n)])


def w_latency():
    ch.gather(*(noop.remote(i) for i in range(100)))
    lat = []
    for i in range(LATENCY_CALLS):
        t = time.perf_counter()
        noop.remote(i).result()
        lat.append(time.perf_counter() - t)
    return summary_us(lat)


def w_tasks():
    with Timer() as t:
        out = ch.gather(*[noop.remote(i) for i in range(TASKS)])
    assert out[-1] == TASKS - 1
    return {"tasks_per_s": round(TASKS / t.s), "seconds": round(t.s, 3)}


def w_map_small():
    with Timer() as naive:
        out = list(double.map(range(MAP_ITEMS)))
    assert out[-1] == 2 * (MAP_ITEMS - 1)
    # Chimaera has no hand-batched variant: the runtime batches fast items itself.
    return {"naive_items_per_s": round(MAP_ITEMS / naive.s)}


def w_dag_chains():
    with Timer() as t:
        heads = []
        for c in range(CHAINS):
            f = add1.remote(c)
            for _ in range(DEPTH - 1):
                f = add1.remote(f)
            heads.append(f)
        out = ch.gather(*heads)
    assert out[0] == DEPTH
    return {"seconds": round(t.s, 3), "steps_per_s": round(CHAINS * DEPTH / t.s)}


def w_cpu_scaling():
    ch.gather(*(noop.remote(i) for i in range(CORES * 2)))
    with Timer() as t:
        ch.gather(*(burn.remote(SCALING_TASK_S) for _ in range(SCALING_TASKS)))
    ideal = SCALING_TASKS * SCALING_TASK_S / CORES
    return {"seconds": round(t.s, 3), "efficiency": round(ideal / t.s, 3)}


def w_large_object():
    a = np.ones(LARGE_BYTES // 8)
    with Timer() as t:
        ref = ch.put(a)
        out = ch.gather(*(total.remote(ref) for _ in range(LARGE_READERS)))
    assert all(x == len(a) for x in out)
    return {"seconds": round(t.s, 3)}


def w_actor_calls():
    log = Log.remote()
    with Timer() as t:
        ch.gather(*[log.append.remote(i) for i in range(ACTOR_CALLS)])
    ordered = log.get.remote().result() == list(range(ACTOR_CALLS))
    return {"calls_per_s": round(ACTOR_CALLS / t.s), "in_submission_order": ordered}


def w_nested():
    with Timer() as t:
        out = ch.gather(*[parent.remote(NESTED_CHILDREN) for _ in range(NESTED_PARENTS)], timeout=300)
    assert out[0] == sum(range(NESTED_CHILDREN))
    return {"seconds": round(t.s, 3)}


@ch.remote
def die_once(marker):
    if not os.path.exists(marker):
        open(marker, "w").close()
        os.kill(os.getpid(), signal.SIGKILL)
    return "recovered"


@ch.remote(effects="external")
def charge(path):
    with open(path, "a") as f:
        f.write("charged\n")
    os.kill(os.getpid(), signal.SIGKILL)


def w_worker_kill():
    marker = os.path.join(tempfile.mkdtemp(), "m")
    with Timer() as t:
        out = die_once.remote(marker).result(timeout=120)
    return {"result": out, "seconds_to_recover": round(t.s, 3)}


def w_external_effect():
    path = os.path.join(tempfile.mkdtemp(), "ledger")
    try:
        charge.remote(path).result(timeout=120)
        outcome = "returned"
    except Exception as e:
        outcome = type(e).__name__
    return {"default": {"outcome": outcome, "side_effects": open(path).read().count("charged")}}


@ch.remote(memory_limit="1 GiB")
def hog():
    data = b"\x01" * (2 << 30)
    time.sleep(1)
    return len(data)


def w_memory_limit():
    try:
        out = hog.remote().result(timeout=180)
        return {"outcome": "completed", "limit_enforced": False, "detail": f"allocated {out >> 20} MiB"}
    except ch.OutOfMemory as e:
        return {"outcome": "OutOfMemory", "limit_enforced": True, "detail": str(e)[:160]}


def w_accounting():
    f = burn.remote(0.5)
    f.result()
    m = f.explain()["attempts"][0]["measures"]
    cpu, scope, method = m["cpu_seconds"]
    cores, _, _ = m["mean_cores"]
    return {"per_task_cpu_seconds": round(cpu, 3), "mean_cores": round(cores, 3), "scope": scope, "method": method}


def w_infeasible():
    with Timer() as t:
        try:
            noop.options(cpus=CORES * 2).remote(1).result(timeout=20)
            outcome = "completed"
        except Exception as e:
            outcome = type(e).__name__
    return {"outcome": outcome, "seconds_to_answer": round(t.s, 2)}


WORKLOADS = {k[2:]: v for k, v in globals().items() if k.startswith("w_")}

if __name__ == "__main__":
    name = sys.argv[1]
    result = WORKLOADS[name]()
    print(json.dumps({"runtime": "chimaera", "version": "0.1.0", "workload": name, "result": result}))
