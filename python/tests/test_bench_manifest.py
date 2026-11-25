"""The benchmark harness's records (bench/evals): every run carries a manifest
naming the code, build, runtime versions and durability class it ran with,
and the raw samples behind its medians; a comparison without a runtime's
version or a durability class is refused; paired runs go in a seeded random
order; both shuffle variants produce the same output digest; and every report
row states its unit of work and how Chimaera committed it."""

from __future__ import annotations

import copy
import glob
import json
import os
import random
import re
import subprocess
import sys

import pytest

from conftest import BIN, HOME, REPO, run_script

sys.path.insert(0, os.path.join(REPO, "bench", "evals"))
import manifest  # noqa: E402
import multi_node  # noqa: E402

DURABLE = {"requested": "machine", "effective": "machine", "raft_sync": ["fsync"], "degraded": []}


def result(metric: str, samples: list[float], **extra) -> dict:
    return {"metric": metric, "samples": samples, "unit": "calls/s: one call", "reps": len(samples), **extra}


def comparison() -> dict:
    return {
        "manifest": {
            "source": {"digest": "0" * 16},
            "build": {"profile": "release", "rustc": "rustc 1.94.1", "rustflags": ""},
            "versions": {"chimaera": "chimaera 0.1.0 (peer level 1, wire 1, build x)", "ray": "2.60.0", "python": {}},
            "durability": DURABLE,
        },
        "results": [
            {"runtime": "ray", "workload": "tasks", "result": result("tasks_per_s", [7000, 7100, 6900])},
            {
                "runtime": "chimaera",
                "workload": "tasks",
                "durability": DURABLE,
                "result": result("tasks_per_s", [13000, 13500, 12900], ops=600, raft_entries=20, executions=4),
            },
            {
                "runtime": "chimaera",
                "workload": "tasks",
                "variant": {"CHIMAERA_RAFT_SYNC": "none"},
                "durability": {**DURABLE, "requested": "process", "effective": "process", "raft_sync": ["none"]},
                "result": result("tasks_per_s", [17000, 17500], ops=400, raft_entries=10, executions=3),
            },
        ],
    }


def test_a_comparison_without_a_runtime_version_or_durability_class_is_refused():
    doc = comparison()
    manifest.validate(doc)
    assert "| tasks |" in manifest.report(doc, ["tasks"], "t", [])

    no_ray = copy.deepcopy(doc)
    del no_ray["manifest"]["versions"]["ray"]
    with pytest.raises(manifest.ManifestError, match="ray version"):
        manifest.report(no_ray, ["tasks"], "t", [])

    no_peer = copy.deepcopy(doc)
    no_peer["manifest"]["versions"]["chimaera"] = ""
    with pytest.raises(manifest.ManifestError, match="chimaera version"):
        manifest.validate(no_peer)

    no_class = copy.deepcopy(doc)
    del no_class["results"][2]["durability"]
    with pytest.raises(manifest.ManifestError, match=r"durability class for chimaera tasks \(CHIMAERA_RAFT_SYNC"):
        manifest.report(no_class, ["tasks"], "t", [])
    no_class["results"][1]["durability"] = {}
    with pytest.raises(manifest.ManifestError, match="durability class for chimaera tasks;"):
        manifest.validate(no_class)


def test_the_interval_brackets_the_median_and_widens_with_spread_between_runs():
    assert manifest.ci([[5.0, 5.0, 5.0]]) == [5.0, 5.0]
    steady = manifest.ci([[10.0, 10.1, 9.9], [10.0, 10.1, 9.9]])
    apart = manifest.ci([[10.0, 10.1, 9.9], [20.0, 20.1, 19.9]])
    assert steady[1] - steady[0] < 0.5 < apart[1] - apart[0], (steady, apart)

    rows = manifest.table(comparison(), ["tasks"])
    plain, variant = rows
    assert variant["variant"] == "CHIMAERA_RAFT_SYNC=none"
    s = plain["chimaera"]
    assert s["ci95"][0] <= s["median"] == 13000 <= s["ci95"][1]
    assert s["samples"] == 3 and s["runs"] == 1 and s["unit"] == "calls/s: one call"
    assert s["raft_entries_per_op"] == pytest.approx(20 / 600)
    assert "ray" not in variant  # a variant is Chimaera's alone


def test_the_durability_class_is_read_once_every_peer_gives_what_it_asks_for():
    formed = {"durability": DURABLE}
    # A peer that has not learned a group's voters yet reports the group as process.
    forming = {"durability": {**DURABLE, "effective": "process", "degraded": ["group 1 gives process: no voters known"]}}
    readings = iter([[formed, forming], [formed, formed]])
    assert manifest.settled_durability(lambda: next(readings))["effective"] == "machine"
    # A class below what was asked that does not clear is what the peers give.
    assert manifest.settled_durability(lambda: [formed, forming], timeout=0.5)["effective"] == "process"


def test_every_workload_states_its_unit():
    assert set(multi_node.UNITS) == set(multi_node.WORKLOADS)
    assert multi_node.CHIMAERA_ONLY <= set(multi_node.CONTROL_ENV)


def test_the_manifest_names_the_running_peer_its_build_and_durability_class():
    out = run_script("import chimaera as ch\nprint(ch.get([ch.remote(lambda: 1).remote()]))")
    assert out.returncode == 0, out.stderr[-2000:]
    binary = manifest.running_binary(HOME)
    assert binary and os.path.samefile(binary, BIN)
    durability = manifest.durability([manifest.status(binary, HOME)])
    assert durability["effective"] == "process" and durability["raft_sync"] == [], durability  # a local peer
    m = manifest.collect(binary, {"chimaera": sys.executable})
    assert re.fullmatch(r"chimaera \S+ \(peer level \d+, wire \d+, build \S+\)", m["versions"]["chimaera"])
    assert m["versions"]["python"]["chimaera"] == ".".join(map(str, sys.version_info[:3]))
    parts = os.path.realpath(BIN).split(os.sep)  # target/release or target/debug (the dev profile), else unknown
    assert m["build"]["profile"] == ("release" if "release" in parts else "dev" if "debug" in parts else "unknown")
    assert re.fullmatch(r"[0-9a-f]{16}", m["source"]["digest"])
    if os.path.exists(os.path.join(REPO, ".git")):
        assert re.fullmatch(r"[0-9a-f]{40}", m["source"]["commit"]) and m["source"]["diff"]
    else:  # a copy without git (the Linux lane, a cluster job): the digest alone names the code
        assert "commit" not in m["source"]


def test_paired_rounds_report_units_digests_intervals_and_commits(tmp_path):
    names = ["tasks", "tasks_serial", "actors", "actors_peer", "shuffle", "shuffle_unfused"]
    env = dict(os.environ, EVAL_SCALE="0.01", EVAL_MIN_SECONDS="0", PYTHONPATH=os.path.join(REPO, "python"))
    p = subprocess.run(
        [sys.executable, os.path.join(REPO, "bench", "evals", "local_cluster.py"), "--rounds", "2", "--seed", "7",
         "--only", ",".join(names), "--out", str(tmp_path)],
        capture_output=True, text=True, timeout=280, env=env, stdin=subprocess.DEVNULL,
    )
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr[-3000:]
    doc = json.load(open(glob.glob(str(tmp_path / "*.json"))[0]))
    m = doc["manifest"]
    assert m["durability"]["effective"] == "machine" and m["durability"]["raft_sync"] == ["fsync"], m["durability"]
    assert m["order"]["seed"] == 7 and m["order"]["scale"] == 0.01 and m["topology"]["nodes"] == 3

    # The seeded order: each round's workloads in a fresh random order.
    rng = random.Random(7)
    expected = []
    for _ in range(2):
        for w in rng.sample(names, len(names)):
            rng.sample(["chimaera"], 1)
            expected.append(w)
    assert [r["workload"] for r in doc["results"]] == expected

    for r in doc["results"]:
        assert "result" in r, r
        assert len(r["result"]["samples"]) >= 3 and r["result"]["unit"] == multi_node.UNITS[r["workload"]]
        assert r["durability"]["effective"] == "machine"
    rows = {row["workload"]: row["chimaera"] for row in manifest.table(doc, names)}
    # Fused and unfused shuffles agree, run after run.
    assert len(rows["shuffle"]["digests"]) == 1 and rows["shuffle"]["digests"] == {
        d: 6 for d in rows["shuffle_unfused"]["digests"]
    }, (rows["shuffle"], rows["shuffle_unfused"])
    # The per-call control commits each call; the burst coalesces them.
    serial, burst = rows["tasks_serial"]["raft_entries_per_op"], rows["tasks"]["raft_entries_per_op"]
    assert serial >= 1 and serial > 10 * burst, (serial, burst)
    assert rows["actors_peer"]["direct_share"] == 0
    for s in rows.values():
        assert s["ci95"][0] <= s["median"] <= s["ci95"][1] and s["runs"] == 2

    report = open(glob.glob(str(tmp_path / "*.md"))[0]).read()
    table = [ln for ln in report.splitlines() if ln.startswith("| ") and not ln.startswith(("| Workload", "|---"))]
    assert len(table) == len(names), report
    for ln in table:
        workload, unit, _, chimaera, commits = (c.strip() for c in ln.strip("|").split("|"))
        assert unit == multi_node.UNITS[workload], ln
        assert re.match(r"[\d,.]+ \[[\d,.]+, [\d,.]+\], \d+ samples in 2 runs", chimaera), ln
        assert re.search(r"raft entries per (unit|run)", commits), ln
    assert "Durability: requested machine, effective machine, raft_sync fsync." in report
