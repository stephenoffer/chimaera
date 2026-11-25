"""``python -m chimaera.provisioner``: grow and shrink a cluster's pool of peers.

    CHIMAERA_ADDRESS=host:7710 CHIMAERA_TOKEN=... \\
        python -m chimaera.provisioner --k8s-statefulset chimaera --min 1 --max 8

It is an ordinary remote driver: it reads ``ch.runs.cluster().nodes()`` and keeps
no state of its own, so you can restart it at any time, and losing it stops
scaling, never work. It sits outside the peers, so user code never runs next
to the credentials that scale the cluster. Each change is printed as one
JSON line saying what it saw.

- **Grow** when calls have been queued for ``--up-after`` seconds: by as many
  peers as the queue needs at the current peers' size, at least one.
- **Shrink** by one when the cluster has been mostly idle (the CPU in use
  would fit on one peer fewer, at most half full) for ``--down-after``
  seconds, and the unit that would go (the highest pod, the newest spare) is
  known to hold nothing: its peer has no calls, no actors and no stored
  values (its drain hands its group seats on, not those), or the provider
  says it never started. When neither can be shown (no peer from it has
  joined, yet it has started; its peer stopped answering) it holds, and
  prints why. Kubernetes removes the highest pod, whose SIGTERM drains it.
- **Hold** while peers it asked for are still starting, for up to
  ``--start-grace`` seconds; peers still not up then (no room for their pods)
  count as unavailable: it stops growing and may shrink them away.

With ``--planner forecast``, growth is decided instead from what each peer
reports queued and how long those calls take (``chimaera._planner``): it asks
for the fewest new peers that start every queued call within
``--target-start`` seconds (and end them within ``--target-finish``), counting
the peers it asked for that are still starting, and none when the backlog
clears before a new peer could start. Shrinking is today's rule, never while
peers it asked for are starting. One provisioner per pool acts (a lease in
the cluster's store); another waits to take over. ``--dry-run`` changes
nothing and ``--explain`` prints every plan.

Kubernetes is driven through the API server with the pod's service account
when it runs in the cluster (``chimaera k8s render --provisioner MAX``), else
with ``kubectl`` (your current context, or ``--namespace``). Inside a
``slurm-launch`` job, ``--slurm`` grows the cluster with spare one-node jobs
instead (see ``SlurmSpares``); ``--sky`` grows it with one-node SkyPilot
clusters on any cloud (see ``SkySpares``).
"""

from __future__ import annotations

import argparse
import getpass
import json
import math
import os
import shlex
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from typing import Any, NamedTuple

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
    held: str | None = None  # why the last shrink it wanted did not happen
    forecast: Any = None  # --planner forecast: the planner's state (chimaera._planner.Forecast)
    plan: Any = None  # its last plan


class Victim(NamedTuple):
    """The unit a shrink removes, as its pool resolved it. ``joined``: its
    peer, `node`, is a live member. ``never_joined``: the provider shows it
    never started, so nothing on it can be lost. ``unknown``: neither can be
    shown (`why`), and a shrink holds."""

    state: str
    target: str  # what the provider removes: a pod, a Slurm job, a SkyPilot cluster
    why: str = ""
    node: Any = None  # the live member, when joined
    uid: str = ""  # the provider's identity for it, when it has one


UNRESOLVED = Victim("unknown", "", "the pool did not say which unit a shrink removes")


def blocked(victim: Victim) -> str | None:
    """Why removing `victim` now could lose work, or None."""
    if victim.state == "never_joined":
        return None
    if victim.state == "joined":
        holding = busy(victim.node)
        return holding and f"{victim.node.address}, the peer a shrink removes, holds {holding}"
    return f"cannot tell what removing {victim.target or 'a unit'} would lose: {victim.why}"


def same(a: Victim, b: Victim) -> bool:
    """`a` and `b` are one resolution of one unit (checked again just before removing it)."""
    peer = lambda v: v.node.peer if v.node is not None else None  # noqa: E731
    return (a.state, a.target, a.uid, peer(a)) == (b.state, b.target, b.uid, peer(b))


def busy(victim: Any) -> str | None:
    """Why removing the peer `victim` (a ``Node``) now would lose work, or None."""
    held = [
        f"{n} {what}"
        for n, what in (
            (victim.cpu_used and f"{victim.cpu_used:g}", "CPUs in use"),
            (victim.queued, "queued calls"),
            (victim.actors, "actors"),
            (victim.object_bytes, "bytes of stored values"),
        )
        if n
    ]
    return ", ".join(held) or None


def decide(
    nodes: list[Any],
    replicas: int,
    rules: Rules,
    memory: Memory,
    now: float,
    victim: Victim | Callable[[], Victim] = UNRESOLVED,
) -> tuple[int, str]:
    """The replica count to ask for, and why. `nodes` are ``ch.runs.cluster().nodes()``;
    `victim` is the unit a shrink would remove (or how to find it, asked only
    when a shrink is due). Without one, nothing shrinks."""
    memory.held = None
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
        if quiet < rules.down_after:
            return replicas, f"quiet for {quiet:.0f} s"
        memory.held = blocked(victim() if callable(victim) else victim)
        if memory.held:
            return replicas, f"quiet for {quiet:.0f} s, but {memory.held}"
        memory.idle_since = None
        return replicas - 1, f"shrink: {used:.1f} of {cpus:.0f} CPUs in use for {quiet:.0f} s"
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
        # The size last read: a change is sent only on top of it, so it fails
        # rather than undo someone else's change of the size.
        self._seen: int | None = None

    def _kubectl(self, *args: str) -> str:
        ns = ["--namespace", self.namespace] if self.namespace else []
        return subprocess.run(["kubectl", *ns, *args], check=True, capture_output=True, text=True).stdout

    def _api(self, method: str, body: Any = None, core: str = "") -> dict[str, Any]:
        """The StatefulSet's scale, or with `core` (``pods/NAME``) that object in its namespace."""
        with open(os.path.join(self.sa_dir, "token")) as f:
            token = f.read().strip()
        ns = self.namespace
        if ns is None:
            with open(os.path.join(self.sa_dir, "namespace")) as f:
                ns = f.read().strip()
        host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        path = f"/api/v1/namespaces/{ns}/{core}" if core else f"/apis/apps/v1/namespaces/{ns}/statefulsets/{self.name}/scale"
        req = urllib.request.Request(
            f"https://{host}:{port}{path}",
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json-patch+json"},
        )
        tls = ssl.create_default_context(cafile=os.path.join(self.sa_dir, "ca.crt"))
        with urllib.request.urlopen(req, context=tls, timeout=10) as r:
            return json.load(r)

    def pod(self, name: str) -> dict[str, Any] | None:
        """Pod `name`'s object, or None when there is no such pod."""
        if self.in_cluster:
            try:
                return self._api("GET", core=f"pods/{name}")
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return None
                raise
        out = self._kubectl("get", "pod", name, "--ignore-not-found", "-o", "json")
        return json.loads(out) if out.strip() else None

    def victim(self, nodes: list[Any], replicas: int) -> Victim:
        """The highest pod, the one a smaller StatefulSet loses. Its peer is
        the member whose ``host`` label (the pod's hostname) is the pod's name."""
        pod = f"{self.name}-{replicas - 1}"
        mine = [n for n in nodes if n.labels.get("host") == pod or n.address.startswith(f"{pod}.")]
        live = [n for n in mine if n.alive]
        if len(live) == 1:
            return Victim("joined", pod, node=live[0])
        if live:
            return Victim("unknown", pod, f"{len(live)} live peers say they run in {pod}")
        if mine:
            return Victim("unknown", pod, f"its peer {mine[0].peer[:12]} stopped answering")
        obj = self.pod(pod)
        if obj is None:
            return Victim("never_joined", pod, "there is no such pod")
        status, uid = obj.get("status") or {}, (obj.get("metadata") or {}).get("uid", "")
        phase = status.get("phase", "")
        started = [
            c.get("name", "")
            for c in status.get("containerStatuses") or []
            if c.get("restartCount") or c.get("lastState") or {"running", "terminated"} & set(c.get("state") or {})
        ]
        if phase == "Pending" and not started:
            return Victim("never_joined", pod, "it is Pending and none of its containers has started", uid=uid)
        return Victim("unknown", pod, f"it is {phase or 'in no phase'} but no peer from it has joined", uid=uid)

    def replicas(self) -> int:
        if self.in_cluster:
            n = int(self._api("GET")["spec"].get("replicas", 0))
        else:
            n = int(self._kubectl("get", "statefulset", self.name, "-o", "jsonpath={.spec.replicas}") or 0)
        self._seen = n
        return n

    def scale(self, n: int) -> None:
        """Ask for `n` replicas, on top of the size last read: if another writer
        changed the size since, the change fails (422) and the next look starts
        over. Only the size is compared, not the object's version: the
        controller's status updates change that all the time."""
        seen = self._seen if self._seen is not None else self.replicas()
        if self.in_cluster:
            path = "/spec/replicas"
            self._api("PATCH", [{"op": "test", "path": path, "value": seen}, {"op": "replace", "path": path, "value": n}])
        else:
            self._kubectl("scale", "statefulset", self.name, f"--current-replicas={seen}", f"--replicas={n}")

    def shrink(self, victim: Victim) -> str | None:
        """Remove the highest pod if it is still `victim` and still holds nothing; else why not.
        shortcut: a pod that starts between this check and the change is
        sent SIGTERM before it can hold work; revisit if a peer can take work
        within one API round trip of starting."""
        replicas = self.replicas()
        now = self.victim(ch.runs.cluster().nodes(), replicas)
        if not same(now, victim):
            return f"{victim.target} changed before the shrink: {now.state} ({now.why or 'live'})"
        held = blocked(now)
        if held:
            return held
        self.scale(replicas - 1)
        return None


def _seeds() -> tuple[str, str]:
    """The live peers' addresses (seeds for a joining peer) and their port."""
    live = sorted(n.address for n in ch.runs.cluster().nodes() if n.alive and n.address)
    if not live:
        raise RuntimeError("no live peer has a network address (a local peer?): spares have nothing to join")
    return ",".join(live), live[0].rsplit(":", 1)[1]


class SlurmSpares:
    """A cluster that started in a Slurm job (``slurm-launch``), grown by spare
    one-node jobs whose peers join it. The pool is the original job's nodes
    plus the spares; growing submits spares (``sbatch``), shrinking cancels the
    newest (``scancel``: its peers get SIGTERM and drain). Spares join with the
    cluster's token, so the job must run with ``CHIMAERA_CLUSTER_TOKEN`` set.

    A spare is this cluster's when it is this user's job named for the
    cluster's own job (``chimaera-spare-<SLURM_JOB_ID>``), so two clusters
    sharing an account and partition never count or cancel each other's. Its
    peer carries the label ``slurm.job=<its job id>``, which is how a shrink
    knows which peer it would stop."""

    NAME = "chimaera-spare"

    def __init__(self, base: int, partition: str | None = None) -> None:
        if not os.environ.get("CHIMAERA_CLUSTER_TOKEN"):
            raise SystemExit("--slurm needs CHIMAERA_CLUSTER_TOKEN in the job's environment: spares join with it")
        if not os.environ.get("SLURM_JOB_ID"):
            raise SystemExit("--slurm runs inside the cluster's own job (slurm-launch): SLURM_JOB_ID is not set")
        self.base, self.partition = base, partition
        self.job = f"{self.NAME}-{os.environ['SLURM_JOB_ID']}"

    @staticmethod
    def _run(*args: str) -> str:
        return subprocess.run(args, check=True, capture_output=True, text=True).stdout

    def _spares(self) -> list[tuple[str, str]]:
        """This cluster's spare jobs, oldest first: (job id, state)."""
        out = self._run("squeue", "-h", "-u", getpass.getuser(), "-n", self.job, "-o", "%i %T")
        jobs = [line.split() for line in out.splitlines() if len(line.split()) >= 2]
        return sorted(((j[0], j[1]) for j in jobs), key=lambda j: int(j[0]))

    def replicas(self) -> int:
        return self.base + len(self._spares())

    def victim(self, nodes: list[Any], replicas: int) -> Victim:
        """The newest spare job: the one a shrink cancels."""
        spares = self._spares()
        if not spares:
            return Victim("unknown", "", "no spare jobs: the cluster's own job's nodes are not removed")
        job, state = spares[-1]
        if state == "PENDING":
            return Victim("never_joined", job, "the job is pending: it has not started", uid=job)
        mine = [n for n in nodes if n.labels.get("slurm.job") == job]
        live = [n for n in mine if n.alive]
        if len(live) == 1:
            return Victim("joined", job, node=live[0], uid=job)
        if live:
            return Victim("unknown", job, f"{len(live)} live peers say they run in job {job}", uid=job)
        if mine:
            return Victim("unknown", job, f"its peer {mine[0].peer[:12]} stopped answering", uid=job)
        return Victim("unknown", job, f"the job is {state.lower()} but its peer has not joined", uid=job)

    def scale(self, n: int) -> None:
        for _ in range(max(0, n - self.base) - len(self._spares())):
            self._submit()

    def shrink(self, victim: Victim) -> str | None:
        """Cancel `victim`'s job if it is still the newest spare and holds nothing; else why not."""
        now = self.victim(ch.runs.cluster().nodes(), self.replicas())
        if not same(now, victim):
            return f"job {victim.target} changed before the shrink: {now.state} ({now.why or 'live'})"
        held = blocked(now)
        if held:
            return held
        self._run("scancel", now.target)
        return None

    def _submit(self) -> None:
        seeds, port = _seeds()
        peer = (
            'export CHIMAERA_LABELS="${CHIMAERA_LABELS:+$CHIMAERA_LABELS,}slurm.job=$SLURM_JOB_ID"; '
            f'exec chimaera --dir "${{TMPDIR:-/tmp}}/{self.NAME}-$SLURM_JOB_ID" peer start --provider static '
            f'--listen 0.0.0.0:{port} --advertise "$(hostname):{port}" --seeds {seeds}'
        )
        args = ["sbatch", "--parsable", "--job-name", self.job, "--nodes=1", "--ntasks-per-node=1", "--exclusive"]
        if self.partition:
            args += ["--partition", self.partition]
        self._run(*args, "--wrap", f"srun sh -c '{peer}'")


# Objects on a huge-page tmpfs: a spare's peer finds one and keeps its objects
# there (objplane.rs). Writing three times its memory limit in 16 MB objects
# then took 3.2-3.3 s against 5.5-5.8 s on /dev/shm's 4 KiB pages
# (bench/objects/RESULTS.md). Mounted only where sudo needs no password, at
# most half the RAM as /dev/shm is (pages are taken only as written); any
# failure leaves the peer on /dev/shm.
HUGE_OBJECTS = (
    "sudo -n mkdir -p /mnt/chimaera-objects && "
    "{ mountpoint -q /mnt/chimaera-objects || "
    "sudo -n mount -t tmpfs -o huge=within_size,size=50%,mode=1777 tmpfs /mnt/chimaera-objects; } || true"
)


class SkySpares:
    """A cluster grown by one-node SkyPilot clusters whose peers join it.
    Growing runs ``sky launch`` with ``launch_args`` (where and on what, e.g.
    ``--infra aws --gpus H100:8 --use-spot --image-id ...``; the node needs
    ``chimaera`` on its PATH). Shrinking drains the newest spare's peer
    (SIGUSR1 over ``ssh NAME``, the host alias SkyPilot writes), waits out the
    drain, then runs ``sky down``. Both run in the background, so a slow cloud
    never stalls the loop. The spares' names live in the cluster's store, so a
    restarted provisioner knows them. Spares join with
    ``CHIMAERA_CLUSTER_TOKEN``, which must be set where this runs (it goes to
    SkyPilot as a secret, kept out of its logs). With ``CHIMAERA_TLS_CA`` (the
    cluster's ``chimaera cluster init`` directory) here, each spare gets a
    certificate of its own, issued here: the CA's key never leaves this
    machine. shortcut: unverified against a real SkyPilot install here."""

    PREFIX = "chimaera-spare"

    def __init__(self, base: int, launch_args: list[str], drain_s: float = 35.0) -> None:
        if not os.environ.get("CHIMAERA_CLUSTER_TOKEN"):
            raise SystemExit("--sky needs CHIMAERA_CLUSTER_TOKEN set: spares join the cluster with it")
        self.base, self.launch_args, self.drain_s = base, launch_args, drain_s
        self.ca = os.environ.get("CHIMAERA_TLS_CA") or None
        if self.ca and not os.path.exists(os.path.join(self.ca, "ca.key")):
            raise SystemExit(f"CHIMAERA_TLS_CA={self.ca} has no ca.key: name the `chimaera cluster init` directory")
        self._work = tempfile.mkdtemp(prefix="chimaera-sky-")
        self._store = ch.coord.Store("provisioner/sky")
        self._children: list[subprocess.Popen[bytes]] = []
        self._launching: dict[str, subprocess.Popen[bytes]] = {}  # spare name -> its `sky launch`

    def _task(self, name: str, peer: str) -> str:
        """A SkyPilot task file for spare `name`: its own certificate, issued
        here from the cluster CA, uploaded with the CA certificate (never the
        CA's key); its peer starts with them."""
        from ._runtime import _peer_binary

        certs = os.path.join(self._work, name)
        shutil.rmtree(certs, ignore_errors=True)
        subprocess.run(
            [_peer_binary(), "cluster", "cert", "--ca", self.ca, "--name", name, "--out", certs],
            check=True,
            capture_output=True,
        )
        remote = "~/.chimaera/spare-tls"
        mounts = "".join(f"  {remote}/{f}: {os.path.join(certs, f)}\n" for f in ("ca.pem", "peer.pem", "peer.key"))
        path = os.path.join(self._work, f"{name}.yaml")
        with open(path, "w") as f:
            f.write(
                f"name: {name}\nfile_mounts:\n{mounts}setup: chmod 600 {remote}/peer.key\n"
                f"run: {peer} --tls-dir {remote}\n"
            )
        return path

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
            name = f"{self.PREFIX}-{max((int(x.rsplit('-', 1)[1]) for x in names), default=0) + 1}"
            # The label is how a shrink finds this spare's peer among the members.
            peer = (
                f'{HUGE_OBJECTS}; export CHIMAERA_LABELS="${{CHIMAERA_LABELS:+$CHIMAERA_LABELS,}}chimaera.spare={name}"; '
                f"chimaera peer start --provider static --listen 0.0.0.0:{port} "
                f'--advertise "$(hostname -I | cut -d" " -f1):{port}" --seeds {seeds}'
            )
            names.append(name)
            self._store.put("spares", names)  # first: a launch that never comes up is shrunk away like any stuck peer
            launch = ["sky", "launch", "-c", name, "-y", "-d", "--secret", "CHIMAERA_CLUSTER_TOKEN"]
            task = self._task(name, peer) if self.ca else peer
            self._spawn([*launch, *self.launch_args, task])
            self._launching[name] = self._children[-1]

    def _sky_has(self, name: str) -> bool | None:
        """Whether SkyPilot has a running (not stopped) cluster `name`; None when it cannot say.
        shortcut: reads `sky status`'s table, unverified against a real install."""
        r = subprocess.run(["sky", "status", name], capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            return None
        rows = [line.split() for line in r.stdout.splitlines() if line.split()[:1] == [name]]
        return any("STOPPED" not in row for row in rows)

    def victim(self, nodes: list[Any], replicas: int) -> Victim:
        """The newest spare: the one a shrink tears down. Its peer carries the
        label ``chimaera.spare=<name>``."""
        names = self._names()
        if not names:
            return Victim("unknown", "", "no spares: the peers that are not spares are not removed")
        name = names[-1]
        mine = [n for n in nodes if n.labels.get("chimaera.spare") == name]
        live = [n for n in mine if n.alive]
        if len(live) == 1:
            return Victim("joined", name, node=live[0])
        if live:
            return Victim("unknown", name, f"{len(live)} live peers say they are spare {name}")
        if mine:
            return Victim("unknown", name, f"its peer {mine[0].peer[:12]} stopped answering")
        launching = self._launching.get(name)
        if launching is not None and launching.poll() is None:
            return Victim("unknown", name, "its sky launch is still running")
        has = self._sky_has(name)
        if has is None:
            return Victim("unknown", name, "sky status failed")
        if has:
            return Victim("unknown", name, "SkyPilot runs it but its peer has not joined")
        return Victim("never_joined", name, "SkyPilot has no running cluster by that name")

    def shrink(self, victim: Victim) -> str | None:
        """Tear down `victim` if it is still the newest spare and holds nothing
        (draining its peer first when it has one); else why not."""
        now = self.victim(ch.runs.cluster().nodes(), self.replicas())
        if not same(now, victim):
            return f"spare {victim.target} changed before the shrink: {now.state} ({now.why or 'live'})"
        held = blocked(now)
        if held:
            return held
        names = self._names()
        names.remove(now.target)
        self._store.put("spares", names)
        q = shlex.quote(now.target)
        drain = f"ssh {q} pkill -USR1 -x chimaera; sleep {self.drain_s:g}; " if now.state == "joined" else ""
        # `sky down` also for one that never joined: a cluster it could not see is torn down, never left running.
        self._spawn(["sh", "-c", f"{drain}sky down -y {q}"])
        return None


def step(pool: Any, rules: Rules, memory: Memory, now: float) -> dict[str, Any] | None:
    """One look and the change it calls for: what to print, if anything. A
    shrink removes only the unit the pool named, checked again just before."""
    nodes, replicas = ch.runs.cluster().nodes(), pool.replicas()
    victim = cache(lambda: pool.victim(nodes, replicas))  # asked only when a shrink is due, then reused
    was = memory.held
    want, why = decide(nodes, replicas, rules, memory, now, victim)
    if memory.forecast is not None:
        want, why = forecast(memory, nodes, replicas, rules, now, want, why)
    removes = None
    if want < replicas:
        memory.held = pool.shrink(victim())
        if memory.held:
            want, why = replicas, f"shrink stopped: {memory.held}"
        else:
            removes = {"unit": victim().target, "as": victim().state}
    elif want > replicas:
        pool.scale(want)
        if memory.forecast is not None:
            memory.forecast.grew(want - replicas, now)
    live = [n for n in nodes if n.alive]
    peers = {"asked": replicas, "up": len(live), "usable": sum(not n.draining for n in live)}
    if want != replicas:
        return {"at": time.time(), "from": replicas, "to": want, "why": why, "peers": peers} | (
            {"removes": removes} if removes else {}
        )
    if memory.held and memory.held != was:  # each new reason a shrink is held, once
        return {"at": time.time(), "holding": replicas, "why": why, "peers": peers}
    return None


def forecast(
    memory: Memory, nodes: list[Any], replicas: int, rules: Rules, now: float, want: int, why: str
) -> tuple[int, str]:
    """--planner forecast: the planner decides growth; today's rule may still
    shrink (`want`, `why` from decide()), but not while peers it asked for
    are starting."""
    plan = memory.plan = memory.forecast.look(nodes, replicas, rules.min, rules.max, now)
    if plan.want > replicas:
        return plan.want, plan.reason
    if want < replicas and not memory.forecast.asked:
        return want, why
    return replicas, plan.reason


def plan_record(plan: Any) -> dict[str, Any]:
    """What a printed line (and the store, for explain()) says of a plan."""
    out: dict[str, Any] = {"want": plan.want, "action": plan.action, "reason": plan.reason}
    out.update({k: v for k in ("missed", "binding") if (v := getattr(plan, k))})
    if plan.runner_up:
        out["runner_up"] = {"peers": plan.runner_up[0], "why": plan.runner_up[1]}
    if plan.inputs.get("unconfirmed"):
        out["left_out"] = plan.inputs["unconfirmed"]
    return out


class Preview:
    """--dry-run: the pool as it is, read but never changed."""

    def __init__(self, pool: Any) -> None:
        self.pool = pool

    def replicas(self) -> int:
        return self.pool.replicas()

    def victim(self, nodes: list[Any], replicas: int) -> Any:
        return self.pool.victim(nodes, replicas)

    def scale(self, n: int) -> None:
        pass

    def shrink(self, victim: Any) -> str | None:
        return "--dry-run: nothing is removed"


class Owner:
    """With --planner forecast, one provisioner per pool acts: it holds a lease
    in the cluster's store (another waits to take over) and keeps there what
    it learned of startup times, and its latest plan, which explain() shows
    for calls still waiting. Writes are fenced: one that lost the lease writes
    nothing."""

    NAMESPACE = "chimaera/capacity"

    def __init__(self, pool_id: str, act: bool = True) -> None:
        """`act`: hold the lease and write (else, for --dry-run, only read)."""
        self.pool_id = pool_id
        self.store = ch.coord.Store(self.NAMESPACE)
        self.lease = self.store.lock(f"owner:{pool_id}", ttl="30s") if act else None
        self.kept: list[float] | None = None  # startup times as the store has them, once read
        self.shown: tuple[Any, ...] | None = None

    def hold(self, memory: Memory | None = None) -> None:
        """Hold the lease, waiting for it if another provisioner has it; on the
        first look, start from the startup times kept in the store. Called
        inside the run loop, so a store not answering yet (peers still
        starting) is retried at the next look, not fatal."""
        if self.lease is not None and not self.lease.valid:
            self.lease.acquire()
        if self.kept is None:
            self.kept = self.samples()
            if memory is not None and memory.forecast is not None:
                memory.forecast.remember(self.kept)

    def samples(self) -> list[float]:
        return list(self.store.get(f"startup:{self.pool_id}") or [])

    def publish(self, memory: Memory, replicas: int) -> None:
        f, plan = memory.forecast, memory.plan
        if plan is None or self.lease is None or not self.lease.valid:
            return
        if list(f.samples) != self.kept:
            self.store.put(f"startup:{self.pool_id}", list(f.samples), fence=self.lease)
            self.kept = list(f.samples)
        said = (replicas, plan.want, plan.action, plan.reason)
        if said != self.shown:
            record = {"pool": self.pool_id, "at": time.time(), "replicas": replicas, **plan_record(plan)}
            self.store.put(f"plan:{self.pool_id}", record, fence=self.lease)
            self.shown = said


def run(
    pool: Any,
    rules: Rules,
    interval: float,
    out: Any = sys.stdout,
    memory: Memory | None = None,
    owner: Owner | None = None,
    explain: bool = False,
) -> None:
    memory = memory or Memory()
    while True:
        try:
            if owner is not None:
                owner.hold(memory)
            record = step(pool, rules, memory, time.monotonic())
            if owner is not None:
                owner.publish(memory, pool.replicas())
        except Exception as e:  # noqa: BLE001 - a failed look or change is retried; nothing acts on stale data
            record = {"at": time.time(), "error": str(e)}
        if memory.plan is not None and (record or explain):
            record = (record or {"at": time.time()}) | {"plan": plan_record(memory.plan)}
        if record and isinstance(pool, Preview):
            record["dry_run"] = True
        if record:
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
    p.add_argument(
        "--planner",
        choices=("queue", "forecast"),
        default="queue",
        help="how growth is decided: queue (the backlog lasted --up-after) or forecast (queued work and its run time)",
    )
    p.add_argument(
        "--target-start", type=float, default=60.0, metavar="SECONDS", help="forecast: start every queued call within"
    )
    p.add_argument("--target-finish", type=float, metavar="SECONDS", help="forecast: and finish it within")
    p.add_argument(
        "--startup-estimate",
        type=float,
        default=60.0,
        metavar="SECONDS",
        help="forecast: a new peer's startup until three are seen (a slow one, not a typical one)",
    )
    p.add_argument(
        "--peer-cpus", type=float, metavar="CPUS", help="forecast: a new peer's CPUs (default: as the first it adds)"
    )
    p.add_argument("--peer-memory", metavar="BYTES", help='forecast: and its memory, e.g. "32 GiB" (with --peer-cpus)')
    p.add_argument("--dry-run", action="store_true", help="change nothing: print what would be asked for")
    p.add_argument("--explain", action="store_true", help="print every look's plan, not only changes")
    a = p.parse_args(argv)
    if not 1 <= a.min <= a.max:
        p.error("need 1 <= --min <= --max")
    if a.planner == "queue" and (a.explain or a.target_finish is not None or a.peer_cpus is not None):
        p.error("--explain, --target-finish and --peer-cpus need --planner forecast")
    if (a.peer_cpus is None) != (a.peer_memory is None):
        p.error("--peer-cpus and --peer-memory go together")
    ch.init(cluster=a.cluster)
    if a.slurm:
        pool: Any = SlurmSpares(int(os.environ.get("SLURM_JOB_NUM_NODES", "1")), a.partition)
    elif a.sky is not None:
        pool = SkySpares(a.base, shlex.split(a.sky))
    else:
        pool = StatefulSet(a.k8s_statefulset, a.namespace)
    memory, owner = Memory(), None
    if a.planner == "forecast":
        from ._planner import Forecast, Target

        pool_id = f"k8s/{a.k8s_statefulset}" if a.k8s_statefulset else "slurm" if a.slurm else "sky"
        owner = Owner(pool_id, act=not a.dry_run)  # reads what it kept at its first look, in run()
        target = Target(start_s=a.target_start, finish_s=a.target_finish)
        from ._units import parse_bytes

        shape = (a.peer_cpus, parse_bytes(a.peer_memory, "--peer-memory")) if a.peer_cpus is not None else None
        memory.forecast = Forecast(target, a.startup_estimate, shape=shape)
    if a.dry_run:
        pool = Preview(pool)
    rules = Rules(a.min, a.max, a.up_after, a.down_after, a.start_grace)
    run(pool, rules, a.interval, memory=memory, owner=owner, explain=a.explain)


if __name__ == "__main__":
    main()
