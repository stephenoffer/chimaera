"""``python -m chimaera.provisioner``: grow and shrink a cluster's pool of peers.

    CHIMAERA_ADDRESS=host:7710 CHIMAERA_TOKEN=... \\
        python -m chimaera.provisioner --k8s-statefulset chimaera --min 1 --max 8

It is an ordinary remote driver: it reads ``ch.cluster().nodes()`` and keeps
no state of its own, so you can restart it at any time, and losing it stops
scaling, never work. It sits outside the peers, so user code never runs next
to the credentials that scale the cluster. Each change is printed as one
JSON line saying what it saw.

- **Grow** when calls have been queued for ``--up-after`` seconds: by as many
  peers as the queue needs at the current peers' size, at least one.
- **Shrink** by one when the cluster has been mostly idle (the CPU in use
  would fit on one peer fewer, at most half full) for ``--down-after``
  seconds. Kubernetes removes the highest pod, whose SIGTERM drains it.
- **Hold** while peers it asked for are still starting, for up to
  ``--start-grace`` seconds; peers still not up then (no room for their pods)
  count as unavailable: it stops growing and may shrink them away.

Kubernetes is driven through the API server with the pod's service account
when it runs in the cluster (``chimaera k8s render --provisioner MAX``), else
with ``kubectl`` (your current context, or ``--namespace``). Inside a
``slurm-launch`` job, ``--slurm`` grows the cluster with spare one-node jobs
instead (see ``SlurmSpares``); ``--sky`` grows it with one-node SkyPilot
clusters on any cloud (see ``SkySpares``).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shlex
import ssl
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any

import chimaera as ch


@dataclass
class Rules:
    min: int = 1
    max: int = 8
    up_after: float = 30.0
    down_after: float = 300.0
    # Peers asked for that are not up after this long count as unavailable
    # (no room for their pods): stop waiting for them.
    start_grace: float = 300.0


@dataclass
class Memory:
    """What the provisioner remembers between looks: since when it has been busy or idle."""

    busy_since: float | None = None
    idle_since: float | None = None
    starting_since: float | None = None
    decisions: list[dict[str, Any]] = field(default_factory=list)


def decide(nodes: list[Any], replicas: int, rules: Rules, memory: Memory, now: float) -> tuple[int, str]:
    """The replica count to ask for, and why. `nodes` are ``ch.cluster().nodes()``."""
    live = [n for n in nodes if n.alive]
    stuck = False
    if len(live) < replicas:
        memory.starting_since = memory.starting_since if memory.starting_since is not None else now
        if now - memory.starting_since < rules.start_grace:
            memory.busy_since = memory.idle_since = None
            return replicas, f"waiting: {len(live)} of {replicas} peers are up"
        stuck = True  # the missing peers cannot start (no room): decide from the live ones
    else:
        memory.starting_since = None
    queued = sum(n.queued for n in live)
    cpus = sum(n.cpus for n in live)
    used = sum(n.cpu_used for n in live)
    per_peer = cpus / max(1, len(live))
    if queued > 0:
        memory.idle_since = None
        memory.busy_since = memory.busy_since if memory.busy_since is not None else now
        waited = now - memory.busy_since
        if stuck:
            return replicas, f"busy, but {replicas - len(live)} peers asked for have not started: not growing"
        if waited >= rules.up_after and replicas < rules.max:
            want = min(rules.max, replicas + max(1, math.ceil(queued / max(per_peer, 1.0))))
            memory.busy_since = None
            return want, f"grow: {queued} calls queued for {waited:.0f} s on {len(live)} peers"
        return replicas, f"busy: {queued} calls queued for {waited:.0f} s"
    memory.busy_since = None
    if replicas > rules.min and used <= 0.5 * per_peer * (replicas - 1):
        memory.idle_since = memory.idle_since if memory.idle_since is not None else now
        quiet = now - memory.idle_since
        if quiet >= rules.down_after:
            memory.idle_since = None
            return replicas - 1, f"shrink: {used:.1f} of {cpus:.0f} CPUs in use for {quiet:.0f} s"
        return replicas, f"quiet for {quiet:.0f} s"
    memory.idle_since = None
    return replicas, "steady"


SERVICE_ACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount"


class StatefulSet:
    """A Kubernetes StatefulSet's replica count. Inside a pod with a service
    account (``chimaera k8s render --provisioner``) it calls the API server
    directly; elsewhere it runs kubectl with your current context."""

    def __init__(self, name: str, namespace: str | None = None, sa_dir: str = SERVICE_ACCOUNT) -> None:
        self.name, self.namespace, self.sa_dir = name, namespace, sa_dir
        self.in_cluster = os.path.exists(os.path.join(sa_dir, "token")) and "KUBERNETES_SERVICE_HOST" in os.environ

    def _kubectl(self, *args: str) -> str:
        ns = ["--namespace", self.namespace] if self.namespace else []
        return subprocess.run(["kubectl", *ns, *args], check=True, capture_output=True, text=True).stdout

    def _api(self, method: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        with open(os.path.join(self.sa_dir, "token")) as f:
            token = f.read().strip()
        ns = self.namespace
        if ns is None:
            with open(os.path.join(self.sa_dir, "namespace")) as f:
                ns = f.read().strip()
        host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        url = f"https://{host}:{port}/apis/apps/v1/namespaces/{ns}/statefulsets/{self.name}/scale"
        req = urllib.request.Request(
            url,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/merge-patch+json"},
        )
        tls = ssl.create_default_context(cafile=os.path.join(self.sa_dir, "ca.crt"))
        with urllib.request.urlopen(req, context=tls, timeout=10) as r:
            return json.load(r)

    def replicas(self) -> int:
        if self.in_cluster:
            return int(self._api("GET")["spec"].get("replicas", 0))
        return int(self._kubectl("get", "statefulset", self.name, "-o", "jsonpath={.spec.replicas}") or 0)

    def scale(self, n: int) -> None:
        if self.in_cluster:
            self._api("PATCH", {"spec": {"replicas": n}})
        else:
            self._kubectl("scale", "statefulset", self.name, f"--replicas={n}")


def _seeds() -> tuple[str, str]:
    """The live peers' addresses (seeds for a joining peer) and their port."""
    live = sorted(n.address for n in ch.cluster().nodes() if n.alive and n.address)
    if not live:
        raise RuntimeError("no live peer has a network address (a local peer?): spares have nothing to join")
    return ",".join(live), live[0].rsplit(":", 1)[1]


class SlurmSpares:
    """A cluster that started in a Slurm job (``slurm-launch``), grown by spare
    one-node jobs whose peers join it. The pool is the original job's nodes
    plus the spares; growing submits spares (``sbatch``), shrinking cancels the
    newest (``scancel``: its peers get SIGTERM and drain). Spares join with the
    cluster's token, so the job must run with ``CHIMAERA_CLUSTER_TOKEN`` set."""

    NAME = "chimaera-spare"

    def __init__(self, base: int, partition: str | None = None) -> None:
        if not os.environ.get("CHIMAERA_CLUSTER_TOKEN"):
            raise SystemExit("--slurm needs CHIMAERA_CLUSTER_TOKEN in the job's environment: spares join with it")
        self.base, self.partition = base, partition

    @staticmethod
    def _run(*args: str) -> str:
        return subprocess.run(args, check=True, capture_output=True, text=True).stdout

    def _spares(self) -> list[str]:
        return sorted(self._run("squeue", "-h", "-n", self.NAME, "-o", "%i").split(), key=int)

    def replicas(self) -> int:
        return self.base + len(self._spares())

    def scale(self, n: int) -> None:
        spares, want = self._spares(), max(0, n - self.base)
        for _ in range(want - len(spares)):
            self._submit()
        for job in spares[want:]:  # the newest go first
            self._run("scancel", job)

    def _submit(self) -> None:
        seeds, port = _seeds()
        peer = (
            f'exec chimaera --dir "${{TMPDIR:-/tmp}}/{self.NAME}-$SLURM_JOB_ID" peer start --provider static '
            f'--listen 0.0.0.0:{port} --advertise "$(hostname):{port}" --seeds {seeds}'
        )
        args = ["sbatch", "--parsable", "--job-name", self.NAME, "--nodes=1", "--ntasks-per-node=1", "--exclusive"]
        if self.partition:
            args += ["--partition", self.partition]
        self._run(*args, "--wrap", f"srun sh -c '{peer}'")


class SkySpares:
    """A cluster grown by one-node SkyPilot clusters whose peers join it.
    Growing runs ``sky launch`` with ``launch_args`` (where and on what, e.g.
    ``--infra aws --gpus H100:8 --use-spot --image-id ...``; the node needs
    ``chimaera`` on its PATH). Shrinking drains the newest spare's peer
    (SIGUSR1 over ``ssh NAME``, the host alias SkyPilot writes), waits out the
    drain, then runs ``sky down``. Both run in the background, so a slow cloud
    never stalls the loop. The spares' names live in the cluster's store, so a
    restarted provisioner knows them. Spares join with
    ``CHIMAERA_CLUSTER_TOKEN``, which must be set where this runs.
    shortcut: unverified against a real SkyPilot install here."""

    PREFIX = "chimaera-spare"

    def __init__(self, base: int, launch_args: list[str], drain_s: float = 35.0) -> None:
        if not os.environ.get("CHIMAERA_CLUSTER_TOKEN"):
            raise SystemExit("--sky needs CHIMAERA_CLUSTER_TOKEN set: spares join the cluster with it")
        self.base, self.launch_args, self.drain_s = base, launch_args, drain_s
        self._store = ch.Store("provisioner/sky")
        self._children: list[subprocess.Popen[bytes]] = []

    def _names(self) -> list[str]:
        return list(self._store.get("spares") or [])

    def _spawn(self, args: list[str]) -> None:
        self._children = [c for c in self._children if c.poll() is None]
        self._children.append(subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))

    def replicas(self) -> int:
        return self.base + len(self._names())

    def scale(self, n: int) -> None:
        names, want = self._names(), max(0, n - self.base)
        while len(names) < want:
            seeds, port = _seeds()
            peer = (
                f"chimaera peer start --provider static --listen 0.0.0.0:{port} "
                f'--advertise "$(hostname -I | cut -d" " -f1):{port}" --seeds {seeds}'
            )
            name = f"{self.PREFIX}-{max((int(x.rsplit('-', 1)[1]) for x in names), default=0) + 1}"
            names.append(name)
            self._store.put("spares", names)  # first: a launch that never comes up is shrunk away like any stuck peer
            launch = ["sky", "launch", "-c", name, "-y", "-d", "--env", "CHIMAERA_CLUSTER_TOKEN"]
            self._spawn([*launch, *self.launch_args, peer])
        while len(names) > want:
            name = names.pop()
            self._store.put("spares", names)
            q = shlex.quote(name)
            self._spawn(["sh", "-c", f"ssh {q} pkill -USR1 -x chimaera; sleep {self.drain_s:g}; sky down -y {q}"])


def run(pool: Any, rules: Rules, interval: float, out: Any = sys.stdout) -> None:
    memory = Memory()
    view = ch.cluster()
    while True:
        try:
            nodes, replicas = view.nodes(), pool.replicas()
        except Exception as e:  # noqa: BLE001 - a missed look is retried; scaling never acts on stale data
            print(json.dumps({"at": time.time(), "error": str(e)}), file=out, flush=True)
            time.sleep(interval)
            continue
        want, why = decide(nodes, replicas, rules, memory, time.monotonic())
        if want != replicas:
            pool.scale(want)
            record = {"at": time.time(), "from": replicas, "to": want, "why": why}
            memory.decisions.append(record)
            print(json.dumps(record), file=out, flush=True)
        time.sleep(interval)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m chimaera.provisioner", description=__doc__.split("\n\n")[0])
    where = p.add_mutually_exclusive_group(required=True)
    where.add_argument("--k8s-statefulset", metavar="NAME", help="the StatefulSet of the peers")
    where.add_argument("--slurm", action="store_true", help="grow with spare Slurm jobs (inside a slurm-launch job)")
    where.add_argument(
        "--sky",
        metavar="LAUNCH_ARGS",
        help='grow with one-node SkyPilot clusters: sky launch args, e.g. "--infra aws --use-spot"',
    )
    p.add_argument("--base", type=int, default=1, help="with --sky: peers that are not spares")
    p.add_argument("--partition", help="Slurm partition for spare jobs")
    p.add_argument("--namespace")
    p.add_argument("--cluster", help="a profile or chimaera://host:port (default: $CHIMAERA_ADDRESS)")
    p.add_argument("--min", type=int, default=1)
    p.add_argument("--max", type=int, default=8)
    p.add_argument("--up-after", type=float, default=30.0, metavar="SECONDS")
    p.add_argument("--down-after", type=float, default=300.0, metavar="SECONDS")
    p.add_argument("--start-grace", type=float, default=300.0, metavar="SECONDS")
    p.add_argument("--interval", type=float, default=5.0, metavar="SECONDS")
    a = p.parse_args(argv)
    if not 1 <= a.min <= a.max:
        p.error("need 1 <= --min <= --max")
    ch.init(cluster=a.cluster)
    if a.slurm:
        pool: Any = SlurmSpares(int(os.environ.get("SLURM_JOB_NUM_NODES", "1")), a.partition)
    elif a.sky is not None:
        pool = SkySpares(a.base, shlex.split(a.sky))
    else:
        pool = StatefulSet(a.k8s_statefulset, a.namespace)
    run(pool, Rules(a.min, a.max, a.up_after, a.down_after, a.start_grace), a.interval)


if __name__ == "__main__":
    main()
