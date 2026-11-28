"""The release's qualification manifest (bench/qualification): each claim the
README makes of speed, scale and fault tolerance, each row of its status table
and each measured section of docs/qualification.md has an entry with a tier, a
replay command and either raw artifacts whose digests match or the reason it
is not traced yet. A claim missing from claims.toml, a changed figure or a
changed artifact stops the manifest; a lane's passing record on the same
source traces the claims it backs; and the signature verifies, and stops
verifying once the manifest changes."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib

import pytest

from conftest import REPO

sys.path.insert(0, os.path.join(REPO, "bench", "qualification"))
import build_manifest as qm  # noqa: E402

BUILD = os.path.join(REPO, "bench", "qualification", "build_manifest.py")


def claims() -> list[dict]:
    with open(os.path.join(REPO, "bench", "qualification", "claims.toml"), "rb") as f:
        return tomllib.load(f)["claim"]


def docs() -> tuple[str, str]:
    with open(os.path.join(REPO, "README.md")) as f, open(os.path.join(REPO, "docs", "qualification.md")) as g:
        return f.read(), g.read()


def checkout(root) -> None:
    """The files a build reads from a checkout: the version and the artifacts claims.toml names."""
    os.makedirs(root, exist_ok=True)
    shutil.copy(os.path.join(REPO, "pyproject.toml"), root / "pyproject.toml")
    for c in claims():
        for a in c.get("artifacts", []):
            os.makedirs(root / os.path.dirname(a["path"]), exist_ok=True)
            shutil.copy(os.path.join(REPO, a["path"]), root / a["path"])


def test_every_claim_of_the_readme_and_the_record_is_listed_with_what_backs_it(tmp_path):
    readme, qualification = docs()
    doc = qm.build(claims(), readme, qualification, lane_dir=str(tmp_path / "no-lanes"))
    by_id = {c["id"]: c for c in doc["claims"]}
    quoted = {q for c in doc["claims"] for q in c["quotes"]}
    status = readme.split("\n## Status\n", 1)[1].split("\n## ", 1)[0]
    rows = [ln for ln in status.splitlines() if ln.startswith("| ") and not ln.startswith("| Area |")]
    assert len(rows) >= 10 and set(rows) <= quoted
    assert "No head node to babysit." in quoted  # the header is read
    assert "| A machine killed under one-second tasks | 18.5 s | 9.6 s |" in by_id["machine-loss-64-nodes"]["quotes"]
    # What you get: retries, at most once and OOM kills avoided are fault-tolerance claims too.
    assert "A crashed worker's calls get retried; your own exceptions don't, unless you list them." in quoted
    assert any("starts at most once" in q for q in by_id["failures"]["quotes"])
    assert any("OOM-killed" in q for q in by_id["memory-admission"]["quotes"])
    measured = {s for s, body in qm.sections(qualification).items() if qm.tables(body)}
    assert measured <= {s for c in doc["claims"] for s in c["sections"]}
    for c in doc["claims"]:
        assert c["kind"] in qm.KINDS and c["tier"] in qm.TIERS and c["replay"], c
        assert c["artifacts"] or c["pending"], c
        for a in c["artifacts"]:
            with open(os.path.join(REPO, a["path"]), "rb") as f:
                assert hashlib.sha256(f.read()).hexdigest() == a["sha256"], a
    # Traced: the container's memory traps are the results kept beside the harness.
    assert "pending" not in by_id["memory-traps"]
    assert [a["path"] for a in by_id["memory-traps"]["artifacts"]] == [
        "bench/evals/gotchas/results/results.json", "bench/evals/gotchas/results/report.md",
    ]
    # Not traced: the cloud rounds' raw results were never kept, and the manifest says so.
    assert "not kept" in by_id["ray-64-nodes"]["pending"] and not by_id["ray-64-nodes"]["artifacts"]
    assert {"claim": "ray-64-nodes", "why": by_id["ray-64-nodes"]["pending"]} in doc["pending"]
    assert any(p.get("gate", "").startswith("Killing 1 of 3 voters") and p["result"] == "fail (local)"
               for p in doc["pending"])
    limits = {(x["claim"], x["value"]) for x in doc["limits"]}
    assert {("cluster-scale", 64), ("gangs-and-nccl", 4), ("fleet-emulation", 262144)} <= limits
    # No lane ran here, so no limit is traced, and each says why, as its claim does.
    assert all(x["pending"] and x["pending"] == by_id[x["claim"]]["pending"] for x in doc["limits"])
    assert doc["release"]["version"] and re.fullmatch(r"[0-9a-f]{16}", doc["release"]["source"]["digest"])


def test_an_unlisted_claim_a_changed_figure_or_a_changed_artifact_is_refused(tmp_path):
    readme, qualification = docs()
    lane_dir = str(tmp_path / "no-lanes")
    kill = "Kill a machine mid-run and its calls finish somewhere else."
    new = readme.replace(kill, kill + " It survives 5 machines lost at once.")
    with pytest.raises(SystemExit, match="a README claim no entry of claims.toml covers: 'It survives 5 machines"):
        qm.build(claims(), new, qualification, lane_dir=lane_dir)
    changed = readme.replace("| 4,502/s | 11,235/s |", "| 4,502/s | 21,235/s |")
    with pytest.raises(SystemExit, match=r"ray-cluster-2026-11-24: quote no longer in README.md: '\| Independent no-op tasks"):
        qm.build(claims(), changed, qualification, lane_dir=lane_dir)
    extra = qualification + "\n## Ray comparison (cluster, 128 nodes)\n\n| Workload | Ray |\n|---|---|\n| tasks | 1 |\n"
    with pytest.raises(SystemExit, match=r"no entry covers: 'Ray comparison \(cluster, 128 nodes\)'"):
        qm.build(claims(), readme, extra, lane_dir=lane_dir)

    # The same artifacts in another checkout, one of them edited.
    root = tmp_path / "root"
    for c in claims():
        for a in c.get("artifacts", []):
            os.makedirs(root / os.path.dirname(a["path"]), exist_ok=True)
            shutil.copy(os.path.join(REPO, a["path"]), root / a["path"])
    shutil.copy(os.path.join(REPO, "pyproject.toml"), root / "pyproject.toml")
    qm.build(claims(), readme, qualification, root=str(root), lane_dir=lane_dir)
    with open(root / "bench/evals/gotchas/results/report.md", "a") as f:
        f.write("| mem_heavy | chimaera | 1.0 | 0 | 0 | 12/12 | [] |\n")
    with pytest.raises(SystemExit, match="memory-traps: artifact bench/evals/gotchas/results/report.md has sha256"):
        qm.build(claims(), readme, qualification, root=str(root), lane_dir=lane_dir)


def test_a_lane_that_passed_on_the_same_source_traces_the_claims_it_backs(tmp_path):
    readme, qualification = docs()
    root = tmp_path / "root"
    lanes, run = root / "lanes", str(root / "lanes" / "100")
    checkout(root)
    # A test of the release, outside the source digest (what builds the peer and the package, and the evals).
    test = root / "python" / "tests" / "test_streams.py"
    test.parent.mkdir(parents=True)
    test.write_text("def test_a_stream(): pass\n")

    def git(*args: str) -> None:
        subprocess.run(["git", "-C", str(root), "-c", "user.name=test", "-c", "user.email=test@example.com",
                        "-c", "commit.gpgsign=false", *args], check=True, capture_output=True, timeout=60)

    git("init", "-q")
    git("add", "-A")
    git("commit", "-q", "-m", "release")
    assert qm.record_lane("quick", "local", [sys.executable, "-c", "print('ran')"], run, str(root)) == 0
    assert qm.record_lane("native", "linux", [sys.executable, "-c", "raise SystemExit(3)"], run, str(root)) == 3
    with open(os.path.join(run, "quick.log")) as f:
        assert f.read() == "ran\n"

    def build() -> tuple[dict, dict]:
        doc = qm.build(claims(), readme, qualification, root=str(root), lane_dir=str(lanes))
        return doc, {c["id"]: c for c in doc["claims"]}

    doc, by_id = build()
    log = {"path": "lanes/100/quick.log", "sha256": hashlib.sha256(b"ran\n").hexdigest(), "bytes": 4}
    assert "pending" not in by_id["streams"] and by_id["streams"]["artifacts"] == [log]
    # A claim needs every lane it names: the Linux lane failed.
    assert by_id["measurements"]["pending"] == "no passing record on this source of lane native"
    assert doc["passed_tiers"] == ["local"]
    assert {(x["lane"], x["passed"], x["current"]) for x in doc["lanes"]} == {
        ("quick", True, True), ("native", False, True)}

    # A later run of the lane on another source (a nightly of a later commit, collected beside it) leaves the
    # record of this source standing; the listing shows a lane's latest run only when none ran on this source.
    shutil.copytree(run, lanes / "200")
    with open(lanes / "200" / "quick.lane.json") as f:
        rec = json.load(f)
    rec["source"]["digest"], rec["started"] = "0" * 16, "2099-01-01T00:00:00Z"
    with open(lanes / "200" / "quick.lane.json", "w") as f:
        json.dump(rec, f)
    doc, by_id = build()
    assert "pending" not in by_id["streams"] and by_id["streams"]["artifacts"] == [log]
    assert [x["current"] for x in doc["lanes"] if x["lane"] == "quick"] == [True]

    # The same digest with another test suite traces nothing, uncommitted or committed.
    digest = qm.run_manifest.source(str(root))["digest"]
    test.write_text("def test_a_stream(): assert False\n")
    assert qm.run_manifest.source(str(root))["digest"] == digest
    for step in ["uncommitted", "committed"]:
        if step == "committed":
            git("commit", "-q", "-am", "a failing test")
        doc, by_id = build()
        assert by_id["streams"]["pending"] == "no passing record on this source of lane quick", step
        assert doc["passed_tiers"] == [] and not any(x["current"] for x in doc["lanes"]), step
        assert {x["lane"]: x["started"] for x in doc["lanes"]}["quick"] == rec["started"], step


def test_a_cloud_jobs_printed_results_come_back_as_its_ci_lanes_artifacts(tmp_path, monkeypatch, capfd):
    # capfd, read to the end: the job logs below, followed to stdout, must not reach the log of a
    # lane that runs this test, even with -s (a fixture's unread output goes to the real stdout).
    readme, qualification = docs()
    root, ci = tmp_path / "root", str(tmp_path / "ci")
    checkout(root)
    # The GPU check's output, incompressible so that it takes several chunks.
    checks = tmp_path / "checks.jsonl"
    checks.write_bytes(os.urandom(9000).hex().encode() + b"\n" + b'{"check": "nccl all-reduce", "ok": true}\n')
    p = subprocess.run([sys.executable, BUILD, "emit", str(checks)], capture_output=True, text=True, timeout=60)
    lines = p.stdout.splitlines()
    assert p.returncode == 0 and len(lines) >= 3, p.stdout + p.stderr
    # As `anyscale job submit --wait` streams the job's log: each line after a prefix, one twice.
    job_log = tmp_path / "job.log"
    job_log.write_text("".join(f"(job pid=7) {ln}\n" for ln in ["nvidia-smi ...", *lines, lines[1]]))
    cat = [sys.executable, "-c", "import sys; sys.stdout.write(open(sys.argv[1]).read())"]
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    assert qm.record_lane("quick", "local", [sys.executable, "-c", "pass"], ci, str(root)) == 0
    assert qm.record_lane("gpu-check", "gpu", [*cat, str(job_log)], ci, str(root), expect=["checks.jsonl"]) == 0
    with open(os.path.join(ci, "gpu-check", "checks.jsonl"), "rb") as f:
        assert f.read() == checks.read_bytes()

    # Collected as `gh run download 123 -p 'lane-*' -D dist/qualification/lanes/123` lays them out.
    lanes = root / "dist" / "qualification" / "lanes"
    shutil.copytree(ci, lanes / "123" / "lane-gpu-check")
    doc = qm.build(claims(), readme, qualification, root=str(root), lane_dir=str(lanes))
    nccl = {c["id"]: c for c in doc["claims"]}["gangs-and-nccl"]
    at = "dist/qualification/lanes/123/lane-gpu-check/"
    assert "pending" not in nccl
    assert [a["path"] for a in nccl["artifacts"]] == [at + "quick.log", at + "gpu-check.log", at + "gpu-check/checks.jsonl"]
    assert nccl["artifacts"][-1]["sha256"] == hashlib.sha256(checks.read_bytes()).hexdigest()
    assert {x["lane"]: x["run"] for x in doc["lanes"]} == dict.fromkeys(
        ["quick", "gpu-check"], "https://github.com/owner/repo/actions/runs/123")
    assert doc["passed_tiers"] == ["gpu", "local"]
    assert "pending" not in {x["claim"]: x for x in doc["limits"]}["gangs-and-nccl"]
    with open(lanes / "123" / "lane-gpu-check" / "gpu-check" / "checks.jsonl", "ab") as f:
        f.write(b'{"check": "added later", "ok": true}\n')
    with pytest.raises(SystemExit, match=f"lane gpu-check: artifact {at}gpu-check/checks.jsonl changed since its record"):
        qm.build(claims(), readme, qualification, root=str(root), lane_dir=str(lanes))

    # A chunk lost from the log, one garbled on the way, or the file never printed (a job that died before
    # emit) fails the lane and writes no file.
    for name, kept, why in [
        ("lost", [lines[0], *lines[2:]], f"checks.jsonl: chunk 2 of {len(lines)} is not in the log"),
        ("garbled", [lines[0][:-8] + "AAAAAAAA", *lines[1:]], "checks.jsonl: not rebuilt from the log"),
        ("absent", ["nvidia-smi ..."], "checks.jsonl: the job printed no such file into the log"),
    ]:
        job_log.write_text("".join(f"{ln}\n" for ln in kept))
        lane_dir = str(tmp_path / name)
        assert qm.record_lane("gpu-check", "gpu", [*cat, str(job_log)], lane_dir, str(root), ["checks.jsonl"]) == 1
        assert f"lane gpu-check: {why}" in capfd.readouterr().out
        with open(os.path.join(lane_dir, "gpu-check.lane.json")) as f:
            rec = json.load(f)
        assert rec["exit"] == 0 and not rec["passed"] and len(rec["artifacts"]) == 1
        assert not os.path.exists(os.path.join(lane_dir, "gpu-check", "checks.jsonl"))

    # A log cannot plant a lane record, which the build would read as another lane's: emit will not print one,
    # and a lane whose log holds one fails without writing it.
    forged = tmp_path / "x.lane.json"
    forged.write_text(json.dumps({"lane": "gpu-check", "tier": "gpu", "command": [], "exit": 0, "passed": True,
                                  "started": "2099-01-01T00:00:00Z", "seconds": 1,
                                  "source": qm.run_manifest.source(str(root)), "artifacts": []}))
    p = subprocess.run([sys.executable, BUILD, "emit", str(forged)], capture_output=True, text=True, timeout=60)
    assert p.returncode == 1 and "not *.lane.json" in p.stderr, p.stdout + p.stderr
    printed = base64.b64encode(gzip.compress(forged.read_bytes())).decode()
    job_log.write_text(f"(job pid=7) chimaera-artifact x.lane.json 1/1 {printed}\n")
    lane_dir = str(root / "forged")
    assert qm.record_lane("quick", "local", [*cat, str(job_log)], lane_dir, str(root)) == 1
    assert "lane quick: x.lane.json: a lane record's name, which a log cannot write" in capfd.readouterr().out
    assert not os.path.exists(os.path.join(lane_dir, "quick", "x.lane.json"))
    doc = qm.build(claims(), readme, qualification, root=str(root), lane_dir=lane_dir)
    assert [x["lane"] for x in doc["lanes"]] == ["quick"] and doc["passed_tiers"] == []
    assert "pending" in {c["id"]: c for c in doc["claims"]}["gangs-and-nccl"]


def test_the_signature_verifies_and_stops_verifying_once_the_manifest_changes(tmp_path):
    def key(name: str) -> str:
        path = str(tmp_path / name)
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", name, "-f", path], check=True)
        return path

    release, other = key("release"), key("other")
    signers = tmp_path / "allowed_signers"
    with open(release + ".pub") as f:
        signers.write_text(f'release namespaces="{qm.NAMESPACE}" {f.read()}')
    out = str(tmp_path / "manifest.json")

    def run(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, BUILD, *args], capture_output=True, text=True, timeout=120)

    p = run("build", "--sign", release, "--out", out)
    assert p.returncode == 0 and "; signed" in p.stdout, p.stdout + p.stderr
    p = run("verify", "--signers", str(signers), out)
    assert p.returncode == 0 and "signature verifies; 0 artifacts changed, 0 missing" in p.stdout, p.stdout + p.stderr
    # Where the artifacts are not, the verdict names each one and fails, unless the reader allows it.
    with open(out) as f:
        doc = json.load(f)
    named = {a["path"] for c in doc["claims"] + doc["lanes"] for a in c["artifacts"]}
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    p = run("verify", "--signers", str(signers), "--root", str(elsewhere), out)
    assert p.returncode == 1 and f"0 artifacts changed, {len(named)} missing" in p.stdout, p.stdout + p.stderr
    assert {ln.rsplit(": ", 1)[1] for ln in p.stdout.splitlines() if ln.startswith("missing under")} == named
    assert "bench/evals/gotchas/results/results.json" in named
    p = run("verify", "--signers", str(signers), "--root", str(elsewhere), "--allow-missing", out)
    assert p.returncode == 0 and f"0 artifacts changed, {len(named)} missing" in p.stdout, p.stdout + p.stderr

    with open(out) as f:
        doc = json.load(f)
    doc["claims"][0]["pending"] = ""  # claim a cloud round as traced
    with open(out, "w") as f:
        json.dump(doc, f, indent=2)
    p = run("verify", "--signers", str(signers), out)
    assert p.returncode == 1 and "the signature does not verify" in p.stderr, p.stdout + p.stderr

    # Signed by a key the signers file does not name, in place of the earlier signature.
    p = run("build", "--sign", other, "--out", out)
    assert p.returncode == 0, p.stderr
    p = run("verify", "--signers", str(signers), out)
    assert p.returncode == 1 and "the signature does not verify" in p.stderr, p.stdout + p.stderr
