"""A release's qualification manifest: each speed, scale and fault-tolerance
claim the README advertises or docs/qualification.md publishes, with its tier,
its raw artifacts (path and sha256) and the command that replays it; the
lanes that ran on this source and whether they passed; the gates still
pending; and the limits tested. It is signed with an SSH key (ssh-keygen -Y),
so a reader can check who issued it and that nothing in it changed.

    python bench/qualification/build_manifest.py lane quick -- make check regress
    python bench/qualification/build_manifest.py build --sign ~/.ssh/release_ed25519
    python bench/qualification/build_manifest.py verify --signers allowed_signers
    python bench/qualification/build_manifest.py emit results.json   # inside a cloud job

`build` refuses a README claim or a measured section of docs/qualification.md
that no entry of claims.toml covers, a quote or section no longer in its
document, an artifact whose digest changed, and an entry with neither
artifacts, a lane nor a reason it is pending. A claim backed by a lane is
traced when that lane's latest record on this very source passed: the same
source digest and, in a checkout, the same commit with nothing uncommitted. A
cloud job's storage is out of a lane's reach, so the job prints its raw
results into its log (`emit`), and the lane that submitted it rebuilds them
from its own log as artifacts of its record (`lane --expect FILE` fails the
lane when one is not there).
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import tomllib
import zlib
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "bench", "evals"))
import manifest as run_manifest  # noqa: E402  (the source digest every run records)

OUT = os.path.join(REPO, "dist", "qualification")
NAMESPACE = "chimaera-qualification"
KINDS = {"speed", "scale", "fault", "function"}
TIERS = {"local", "linux", "launchers", "cluster", "gpu", "emulation"}
# Where the README claims speed, scale and fault tolerance: its header (no
# heading), the comparison with Ray, How it works, What you get and the status table.
README_SECTIONS = {
    "", "How it stacks up against Ray", "How it works", "What you get", "Calls sized for you",
    "Failures you can reason about", "A record of everything", "Runs where your machines are", "Status",
}
# A sentence there that names a figure, a comparison, a scale, a loss or a place it runs.
CLAIMED = re.compile(
    r"(?<![A-Za-z])\d|\b(faster|slower|wins?|kill(ed)?|lose|lost|fail(s|ed|over)?|survives?|replicated|retried"
    r"|crash(es|ed)?|dies|died|(at most|exactly) once|outlives?|restarts?|oom|kubernetes|slurm|ssh"
    r"|head node|machines?|cores?|peers?|nodes?|scal(e|es|ing))\b",
    re.I,
)
PENDING_GATE = re.compile(r"unverified|fail|pending|in progress", re.I)
# A line of a file a job printed into its log (emit): name, chunk i of n, base64 of the gzipped file.
EMITTED = re.compile(r"chimaera-artifact (\w[\w.-]*) (\d+)/(\d+) ([A-Za-z0-9+/=]+)")
CHUNK = 4096


def _visible(line: str) -> str:
    """What a reader sees of a markdown or HTML line: an image's alt text,
    a link's text, no tags."""
    alts = re.findall(r'alt="([^"]*)"', line)
    if alts:
        return " ".join(alts)
    line = re.sub(r"<[^>]+>", " ", line)
    return re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", line).strip()


def readme_claims(text: str) -> list[str]:
    """Each claim of README_SECTIONS: every table row, and every sentence (an
    image's alt text included) that CLAIMED matches, outside code blocks."""
    out: list[str] = []
    para: list[str] = []
    section, fenced = "", False

    def flush() -> None:
        joined = re.sub(r"\s+", " ", " ".join(para)).strip()
        out.extend(s for s in re.split(r"(?<=[.!?])\s+(?=[A-Z`])", joined) if s and CLAIMED.search(s))
        para.clear()

    lines = text.splitlines()
    for i, raw in enumerate(lines):
        if raw.startswith("```"):
            fenced = not fenced
            flush()
        elif fenced:
            continue
        elif raw.startswith("#"):
            flush()
            section = raw.lstrip("#").strip()
        elif section not in README_SECTIONS:
            continue
        elif raw.startswith("|"):
            flush()
            separator = re.fullmatch(r"\|[-:| ]+\|", raw.strip())
            header = i + 1 < len(lines) and re.fullmatch(r"\|[-:| ]+\|", lines[i + 1].strip())
            if not separator and not header:
                out.append(raw.strip())
        elif not raw.strip():
            flush()
        elif _visible(raw):
            para.append(_visible(raw))
    flush()
    return out


def sections(text: str) -> dict[str, str]:
    """docs/qualification.md's headings, each with its body."""
    out: dict[str, str] = {}
    name = ""
    for line in text.splitlines():
        if line.startswith("#"):
            name = line.lstrip("#").strip()
            out[name] = ""
        elif name:
            out[name] += line + "\n"
    return out


def tables(body: str) -> list[list[list[str]]]:
    """The markdown tables in `body`, each a list of rows of cells (header first, no separator)."""
    out: list[list[list[str]]] = []
    run: list[list[str]] = []
    for line in body.splitlines() + [""]:
        if line.startswith("|"):
            if not re.fullmatch(r"\|[-:| ]+\|", line.strip()):
                run.append([c.strip() for c in line.strip().strip("|").split("|")])
        elif run:
            out.append(run)
            run = []
    return out


def gates(text: str) -> list[dict[str, str]]:
    """Every row of the gate tables (| Gate | Result | Evidence |)."""
    return [
        {"section": name, "gate": row[0], "result": row[1]}
        for name, body in sections(text).items()
        for table in tables(body)
        if table[0] == ["Gate", "Result", "Evidence"]
        for row in table[1:]
    ]


def artifact(path: str, root: str = REPO) -> dict[str, Any]:
    full = os.path.join(root, path)
    with open(full, "rb") as f:
        data = f.read()
    return {"path": path, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def same_source(rec: dict[str, Any], source: dict[str, str]) -> bool:
    """Whether a lane record ran on `source`: the same digest and, when the
    build knows its commit, the same commit with nothing uncommitted on either
    side, since the digest leaves out the tests, deploy/ (the launchers'
    images) and CI."""
    ran = rec["source"]
    return ran["digest"] == source["digest"] and (
        "commit" not in source or ran.get("commit") == source["commit"] and ran.get("diff") == source["diff"] == "none"
    )


def lanes(
    directory: str, source: dict[str, str], root: str = REPO
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """The lane records under `directory` (`gh run download` puts each CI
    artifact in a folder of its own), by lane, keeping a lane's latest run on
    `source`, or its latest run when none ran there, with their artifacts
    named by their path from `root`; and each artifact missing or changed
    since its record."""
    out: dict[str, dict[str, Any]] = {}
    problems = []
    for d, _, files in sorted(os.walk(directory)):
        for name in sorted(f for f in files if f.endswith(".lane.json")):
            with open(os.path.join(d, name)) as f:
                rec = json.load(f)
            arts = []
            for a in rec["artifacts"]:
                path = os.path.relpath(os.path.join(d, a["path"]), root)
                try:
                    got = artifact(path, root)
                except OSError as e:
                    problems.append(f"lane {rec['lane']}: artifact {path}: {e.strerror}")
                    continue
                if got["sha256"] != a["sha256"]:
                    problems.append(f"lane {rec['lane']}: artifact {path} changed since its record")
                arts.append(got)
            rec["artifacts"] = arts
            kept = out.get(rec["lane"])
            if not kept or (same_source(rec, source), rec["started"]) > (same_source(kept, source), kept["started"]):
                out[rec["lane"]] = rec
    return out, problems


def coverage(claims: list[dict[str, Any]], readme: str, qualification: str) -> list[str]:
    """Why the claims do not cover the documents: a README claim or a measured
    section (one with a table) no entry names, or a quote or section an entry
    names that is no longer there."""
    units = readme_claims(readme)
    heads = sections(qualification)
    quoted = {q for c in claims for q in c.get("quotes", [])}
    named = {s for c in claims for s in c.get("sections", [])}
    measured = [s for s, body in heads.items() if tables(body)]
    return (
        [f"a README claim no entry of claims.toml covers: {u!r}" for u in units if u not in quoted]
        + [f"a section of docs/qualification.md no entry covers: {s!r}" for s in measured if s not in named]
        + [f"{c['id']}: quote no longer in README.md: {q!r}" for c in claims for q in c.get("quotes", [])
           if q not in units]
        + [f"{c['id']}: no section {s!r} in docs/qualification.md" for c in claims for s in c.get("sections", [])
           if s not in heads]
    )


def entry(
    c: dict[str, Any], records: dict[str, dict[str, Any]], source: dict[str, str], root: str
) -> tuple[dict[str, Any], list[str]]:
    """One claim of the manifest, and what is wrong with its entry. It is
    traced when it has artifacts and no reason it is pending: its own, or a
    lane it names without a passing record on this source."""
    cid = c.get("id", "?")
    if c.get("kind") not in KINDS or c.get("tier") not in TIERS or not c.get("replay"):
        return {}, [f"{cid}: needs a kind in {sorted(KINDS)}, a tier in {sorted(TIERS)} and a replay command"]
    problems, arts = [], []
    for a in c.get("artifacts", []):
        try:
            got = artifact(a["path"], root)
        except OSError as e:
            problems.append(f"{cid}: artifact {a['path']}: {e.strerror}")
            continue
        if got["sha256"] != a["sha256"]:
            problems.append(f"{cid}: artifact {a['path']} has sha256 {got['sha256']}, claims.toml {a['sha256']}")
        arts.append(got)
    unrun = []
    for name in c.get("lanes", []):
        rec = records.get(name)
        if rec and rec["passed"] and same_source(rec, source):
            arts.extend(rec["artifacts"])
        else:
            unrun.append(name)
    out: dict[str, Any] = {k: c[k] for k in ("id", "kind", "tier", "replay", "lanes", "limit") if k in c}
    out.update(quotes=c.get("quotes", []), sections=c.get("sections", []), artifacts=arts)
    why = c.get("pending") or (f"no passing record on this source of lane {', '.join(unrun)}" if unrun else "")
    if why or not arts:
        out["pending"] = why or ""
        if not why:
            problems.append(f"{cid}: neither artifacts, a lane nor a reason it is pending")
    return out, problems


def build(claims: list[dict[str, Any]], readme: str, qualification: str, root: str = REPO,
          lane_dir: str = os.path.join(OUT, "lanes")) -> dict[str, Any]:
    """The manifest of the source in `root`; raises SystemExit with every
    reason it cannot be issued."""
    problems = coverage(claims, readme, qualification)
    source = run_manifest.source(root)
    records, wrong = lanes(lane_dir, source, root)
    problems += wrong
    out = []
    for c in claims:
        e, wrong = entry(c, records, source, root)
        problems += wrong
        out.append(e)
    if problems:
        raise SystemExit("qualification manifest refused:\n  " + "\n  ".join(problems))
    with open(os.path.join(root, "pyproject.toml"), "rb") as f:
        version = tomllib.load(f)["project"]["version"]
    current = {r["lane"] for r in records.values() if same_source(r, source)}
    return {
        "release": {"version": version, "source": source, "built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
        "passed_tiers": sorted({r["tier"] for r in records.values() if r["lane"] in current and r["passed"]}),
        "lanes": [{k: r[k] for k in ("lane", "tier", "command", "passed", "started", "seconds", "run", "artifacts")
                   if k in r} | {"current": r["lane"] in current} for r in records.values()],
        "claims": out,
        "pending": [{"claim": e["id"], "why": e["pending"]} for e in out if "pending" in e]
        + [g for g in gates(qualification) if PENDING_GATE.search(g["result"])],
        # A limit whose claim is not traced says why, as its claim does.
        "limits": [{**e["limit"], "tier": e["tier"], "claim": e["id"]} | {k: e[k] for k in ("pending",) if k in e}
                   for e in out if "limit" in e],
    }


def record_lane(
    name: str, tier: str, cmd: list[str], lane_dir: str = os.path.join(OUT, "lanes"), root: str = REPO,
    expect: list[str] | None = None,
) -> int:
    """Run one lane's command and record its outcome, the source it ran on, the
    CI run it was part of, its log and the files a job it ran printed into that
    log (emit): the raw artifacts of the claims it backs, named from the
    record's folder. A printed file that cannot be rebuilt, or an `expect`ed
    one the log does not hold, fails the lane."""
    os.makedirs(lane_dir, exist_ok=True)
    log = os.path.join(lane_dir, f"{name}.log")
    t0 = time.time()
    # Into a file, followed here, not a pipe: a process the lane leaves running cannot hold a file open.
    with open(log, "wb") as f, open(log, "rb") as follow:
        proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        while proc.poll() is None:
            sys.stdout.buffer.write(follow.read())
            sys.stdout.flush()
            time.sleep(0.5)
        sys.stdout.buffer.write(follow.read())
        code = proc.returncode
    files, broken = rebuild(log, os.path.join(lane_dir, name))
    rebuilt = {os.path.basename(p) for p in files}
    broken += [f"{x}: the job printed no such file into the log" for x in expect or [] if x not in rebuilt]
    for why in broken:
        print(f"lane {name}: {why}")
    passed = code == 0 and not broken
    rec = {
        "lane": name, "tier": tier, "command": cmd, "exit": code, "passed": passed,
        "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t0)), "seconds": round(time.time() - t0),
        "source": run_manifest.source(root),
        "artifacts": [artifact(os.path.relpath(p, lane_dir), lane_dir) for p in [log, *files]],
    }
    if os.environ.get("GITHUB_RUN_ID"):  # a CI job: the run whose page and uploads hold this record
        rec["run"] = "{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}".format_map(os.environ)
    with open(os.path.join(lane_dir, f"{name}.lane.json"), "w") as f:
        json.dump(rec, f, indent=2)
    print(f"lane {name}: {'passed' if passed else f'failed (exit {code})'}")
    return code or int(not passed)


def rebuildable(name: str) -> bool:
    """A name rebuild() writes: letters, digits, _ . - (nothing outside the
    lane's folder), and not a lane record's, which lanes() would read as one."""
    return bool(re.fullmatch(r"\w[\w.-]*", name)) and not name.endswith(".lane.json")


def emit(paths: list[str]) -> None:
    """Print each file, in lines that rebuild() turns back into it, into this
    job's log, which the lane that submitted the job keeps."""
    for path in paths:
        name = os.path.basename(path)
        if not rebuildable(name):
            raise SystemExit(f"emit: {name!r} is not a name a lane rebuilds (letters, digits, _ . -, not *.lane.json)")
        with open(path, "rb") as f:
            text = base64.b64encode(gzip.compress(f.read())).decode()
        n = -(-len(text) // CHUNK)
        for i in range(n):
            print(f"chimaera-artifact {name} {i + 1}/{n} {text[i * CHUNK:(i + 1) * CHUNK]}")
    sys.stdout.flush()


def rebuild(log: str, directory: str) -> tuple[list[str], list[str]]:
    """The files a job printed into `log` (emit), written into `directory`, and
    why any could not be rebuilt. A chunk may come after a prefix or twice; a
    file with a chunk missing or garbled (gzip checks its CRC) is not written."""
    chunks: dict[str, dict[int, str]] = {}
    counts: dict[str, int] = {}
    with open(log, errors="replace") as f:
        for line in f:
            for name, i, n, part in EMITTED.findall(line):
                chunks.setdefault(name, {})[int(i)] = part
                counts[name] = int(n)
    files, broken = [], []
    for name, parts in chunks.items():
        if not rebuildable(name):
            broken.append(f"{name}: a lane record's name, which a log cannot write")
            continue
        try:
            data = gzip.decompress(base64.b64decode("".join(parts[i] for i in range(1, counts[name] + 1))))
        except KeyError as e:
            broken.append(f"{name}: chunk {e} of {counts[name]} is not in the log")
            continue
        except (ValueError, OSError, EOFError, zlib.error) as e:
            broken.append(f"{name}: not rebuilt from the log ({type(e).__name__}: {e})")
            continue
        os.makedirs(directory, exist_ok=True)
        files.append(os.path.join(directory, name))
        with open(files[-1], "wb") as f:
            f.write(data)
    return files, broken


def sign(path: str, key: str) -> None:
    """Write `path`.sig, an SSH signature over the manifest."""
    subprocess.run(["ssh-keygen", "-Y", "sign", "-q", "-f", key, "-n", NAMESPACE, path], check=True)


def verify(path: str, signers: str, root: str = REPO) -> tuple[list[str], list[str]]:
    """Check the signature against an allowed_signers file, then re-hash every
    artifact the manifest names under `root`. Returns the artifacts whose
    digest changed and those not there; raises SystemExit when the signature
    does not verify."""
    sig = path + ".sig"
    who = subprocess.run(["ssh-keygen", "-Y", "find-principals", "-s", sig, "-f", signers],
                         capture_output=True, text=True, check=False)
    principal = who.stdout.split("\n")[0].strip()
    with open(path, "rb") as f:
        p = subprocess.run(["ssh-keygen", "-Y", "verify", "-f", signers, "-I", principal or "unknown", "-n", NAMESPACE,
                            "-s", sig], stdin=f, capture_output=True, text=True, check=False)
    if p.returncode != 0:
        raise SystemExit(f"{path}: the signature does not verify: {(p.stderr or p.stdout or who.stderr).strip()}")
    with open(path) as f:
        doc = json.load(f)
    named = {a["path"]: a["sha256"] for c in doc["claims"] for a in c["artifacts"]}
    named.update((a["path"], a["sha256"]) for lane in doc["lanes"] for a in lane["artifacts"])
    missing = [p for p in named if not os.path.exists(os.path.join(root, p))]
    changed = [p for p in named if p not in missing and artifact(p, root)["sha256"] != named[p]]
    return changed, missing


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    lp = sub.add_parser("lane", help="run a lane's command (after --) and record its outcome")
    lp.add_argument("name")
    lp.add_argument("--tier", default="local", choices=sorted(TIERS))
    lp.add_argument("--expect", action="append", metavar="FILE",
                    help="a file the job must print into the log (emit); the lane fails without it")
    bp = sub.add_parser("build", help="write the manifest, signed with --sign")
    bp.add_argument("--sign", help="an SSH private key (ssh-keygen -Y sign)")
    bp.add_argument("--out", default=os.path.join(OUT, "manifest.json"))
    vp = sub.add_parser("verify", help="check a manifest's signature and re-hash the artifacts it names")
    vp.add_argument("--signers", required=True, help="an allowed_signers file naming the release's key")
    vp.add_argument("--root", default=REPO,
                    help="where the artifacts are: a checkout of the release with its qualification.tar.gz unpacked")
    vp.add_argument("--allow-missing", action="store_true", help="pass though artifacts are missing under --root")
    vp.add_argument("manifest", nargs="?", default=os.path.join(OUT, "manifest.json"))
    ep = sub.add_parser("emit", help="print files into a cloud job's log, for the lane that submitted it")
    ep.add_argument("files", nargs="+")
    argv = sys.argv[1:]
    split = argv.index("--") if "--" in argv else len(argv)
    a = ap.parse_args(argv[:split])
    if a.cmd == "lane":
        if not argv[split + 1:]:
            ap.error("lane: give the lane's command after --")
        sys.exit(record_lane(a.name, a.tier, argv[split + 1:], expect=a.expect))
    if a.cmd == "emit":
        emit(a.files)
        return
    if a.cmd == "verify":
        changed, missing = verify(a.manifest, a.signers, a.root)
        for path in changed:
            print(f"changed since the release: {path}")
        for path in missing:
            print(f"missing under {a.root}: {path}")
        print(f"{a.manifest}: signature verifies; {len(changed)} artifacts changed, {len(missing)} missing")
        sys.exit(1 if changed or (missing and not a.allow_missing) else 0)
    with open(os.path.join(HERE, "claims.toml"), "rb") as f:
        claims = tomllib.load(f)["claim"]
    with open(os.path.join(REPO, "README.md")) as f, open(os.path.join(REPO, "docs", "qualification.md")) as g:
        doc = build(claims, f.read(), g.read())
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    # An earlier signature is not this manifest's (and ssh-keygen keeps an existing one).
    if os.path.exists(a.out + ".sig"):
        os.remove(a.out + ".sig")
    with open(a.out, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    if a.sign:
        sign(a.out, a.sign)
    traced = sum(1 for c in doc["claims"] if "pending" not in c)
    print(f"{a.out}: {traced} of {len(doc['claims'])} claims traced to raw artifacts, {len(doc['pending'])} pending, "
          f"passed tiers {', '.join(doc['passed_tiers']) or 'none'}" + ("; signed" if a.sign else "; not signed"))


if __name__ == "__main__":
    main()
