# Run on a cluster

The same script runs on your laptop and on many machines. This guide takes
you from the local peer to a cluster of your own: starting the peers,
pointing a script at them, getting your code there, and keeping it running
while machines come and go.

## Start on your laptop

There is nothing to set up. The first call starts a peer for you:

```python run
import os
import chimaera as ch

@ch.remote
def where() -> str:
    return ch.context().node

run = ch.init()                      # connects now; the first .remote() would too
node = where.remote().result()
assert node in {n.peer for n in ch.runs.cluster().nodes()}
```

That local peer keeps its data in `~/.chimaera/local` (or `$CHIMAERA_HOME`)
and its log in `peer.log` there. It is started with `--idle-timeout 600`, so
it exits after ten minutes with no connected script and no work. Later
scripts reuse it while it runs. To stop it now:

```bash
chimaera stop
```

`chimaera stop` and Ctrl-C stop a peer without draining. If you rebuild or
upgrade Chimaera while a local peer is running, your next script warns with
CH111 until you stop the old peer
([observability.md](observability.md#understand-the-chnnn-warnings)).

## Start a cluster

Every machine runs the same `chimaera` binary, and there is no head node. A
cluster needs a shared token and, unless the network is trusted, TLS
certificates. Make them once:

```bash
chimaera cluster init --out ca                            # ca/ca.pem, ca/ca.key, ca/token
chimaera cluster cert --ca ca --name gpu1 --out tls-gpu1  # one per machine
```

Copy `ca/token` and that machine's `tls-<name>` directory to each machine,
then start a peer on each:

```bash
chimaera peer start \
  --listen 10.0.0.1:7700 \
  --seeds 10.0.0.1:7700,10.0.0.2:7700,10.0.0.3:7700 \
  --token-file token \
  --tls-dir tls-gpu1 \
  --client-listen 10.0.0.1:7710
```

| Flag | What it does |
|---|---|
| `--listen` | Address this peer listens on for other peers. Without it the peer runs alone |
| `--seeds` | The founders' addresses. A peer whose own address is in the list is a founder; any other joins as a spare |
| `--token-file` | The cluster token. Also read from `CHIMAERA_CLUSTER_TOKEN`. Required unless `--insecure` |
| `--tls-dir` | This peer's `ca.pem`, `peer.pem` and `peer.key`, from `chimaera cluster cert` |
| `--client-listen` | Also accept scripts from other machines here. They must present the token, and use TLS when `--tls-dir` is set |
| `--cluster` | Cluster name (default `chimaera`); peers of different clusters refuse each other |
| `--cpus` | CPUs to offer (default: what this process is allowed to use) |

`--advertise`, `--bootstrap-expect`, `--data-groups` and the rest are in
[../cli.md](../cli.md#chimaera-peer-start).

Founders hold the cluster's Raft groups from the start; spares add capacity
and take a voter's place when one is lost. Use three founders or more: with
fewer, a group loses its majority when one machine goes down. Give every
peer the same `--seeds`.

Without TLS, `chimaera cluster token > token` makes a token on its own (it
must be at least 16 characters). `--insecure` lets peers join without any
token; use it only on a network you trust.

Here is a full cluster on one machine: three founders and a spare, each on
its own ports and data directory (`--dir`), with TLS:

```bash
chimaera cluster init --out ca
for i in 0 1 2 3; do chimaera cluster cert --ca ca --name p$i --out tls$i; done
SEEDS=127.0.0.1:17701,127.0.0.1:17702,127.0.0.1:17703
for i in 0 1 2 3; do
  chimaera --dir "$PWD/p$i" peer start --listen 127.0.0.1:$((17701 + i)) --seeds $SEEDS \
    --token-file ca/token --tls-dir tls$i --client-listen 127.0.0.1:$((17711 + i)) --cpus 2 \
    > p$i.log 2>&1 &
done
```

Give `--dir` an absolute path. With a relative one, the peer's workers,
which start in your script's directory, can't find the peer, and calls hang.

The spare (`p3`) logs that it is ready and holds no groups:

```text
INFO chimaera_peer::cluster: cluster peer ready node=6222675771281290418 addr=127.0.0.1:17704 hosted=[]
```

## Point a script at the cluster

A script picks its cluster from the first of these that is set:

1. `ch.init(cluster="lab")`: a profile name, or `tcp://host:port`
2. `CHIMAERA_CLUSTER`: the same, from the environment
3. `CHIMAERA_ADDRESS`: one or more `host:port` client addresses, comma-separated, with the token in `CHIMAERA_TOKEN` or `CHIMAERA_TOKEN_FILE` and the CA in `CHIMAERA_CA`
4. `default` in the profile file
5. the local peer

Profiles live in `~/.config/chimaera/clusters.toml` (`CHIMAERA_CLUSTERS`
names another file):

```toml
default = "lab"

[clusters.lab]
peers = ["10.0.0.1:7710", "10.0.0.2:7710", "10.0.0.3:7710"]   # the peers' --client-listen
```

`peers` lists client addresses, tried in order until one answers. The
profile's token is `~/.chimaera/clusters/lab/token` and its CA is
`~/.chimaera/clusters/lab/ca.pem`; the keys `token_file` and `ca` name other
files. Keep credentials out of the profile itself. A `CHIMAERA_TOKEN` or
`CHIMAERA_TOKEN_FILE` in the environment overrides the profile's token.
`default = "local"`, or `cluster="local"`, means the local peer. For the
cluster on one machine above, `peers` is `["127.0.0.1:17711", "127.0.0.1:17712",
"127.0.0.1:17713"]`, and `ca/token` and `ca/ca.pem` are copied to
`~/.chimaera/clusters/lab/`. Then `ch.init(cluster="lab")` in a script
connects to it.

`ch.init(cluster=...)` must come before the script's first call. To switch,
end the run with `ch.shutdown()` and call `ch.init` again. An unknown name
fails with the profiles the file does have:

```python run
import tempfile

profiles = os.path.join(tempfile.mkdtemp(), "clusters.toml")
with open(profiles, "w") as f:
    f.write('default = "local"\n\n[clusters.lab]\npeers = ["10.0.0.1:7710"]\n')
os.environ["CHIMAERA_CLUSTERS"] = profiles

ch.shutdown()                        # end this run so ch.init can choose again
try:
    ch.init(cluster="staging")
except ch.ChimaeraError as e:
    assert "no cluster 'staging'" in str(e) and "(profiles: lab)" in str(e)
run = ch.init(cluster="local")       # back on the local peer
```

Other ways a connection fails:

| What you see | Why |
|---|---|
| `ChimaeraError: cannot reach cluster 'lab': ...` | No address in `peers` answered |
| `RuntimeError: missing or wrong cluster token or driver credential` | No token was found, or it is wrong |
| `RuntimeError: h2 protocol error: http2 error` | The peers use TLS and the script has no CA (`ca.pem` or `CHIMAERA_CA`) |
| warning CH116 | The cluster runs a different Chimaera version than your script |

The CLI picks a cluster the same way, with one difference: `--dir` or
`CHIMAERA_HOME` means the local peer, unless you also pass `--cluster`:

```bash
chimaera status --cluster lab
```

```text
peer 000000002b7d (incarnation 1) · 0.00/2.00 CPU in use · 4 executors (0 busy) · 0 queued · 0 running · 0 sessions · 0 object bytes
  ...
  automatic: raft sync fsync (fsync p50 71 µs: fast)
  peers:
    127.0.0.1:17704 · 0.0/2.0 CPU in use · 0 queued
    127.0.0.1:17702 · 0.0/2.0 CPU in use · 0 queued · leads 0, 1, 2, 3, 4, 5, 6, 7, 8
    127.0.0.1:17703 · 0.0/2.0 CPU in use · 0 queued
    127.0.0.1:17701 (this peer) · 0.0/2.0 CPU in use · 0 queued
  ...
```

## Run a script against the cluster

Take a small project of two files, `helper.py` and `app.py`:

```python
# helper.py
def square(x: int) -> int:
    return x * x
```

```python
# app.py: needs a cluster profile named "lab" (or CHIMAERA_CLUSTER); not run here
import chimaera as ch
from helper import square

@ch.remote
def sq(x: int) -> int:
    return square(x)

print("run", ch.init())
print("total", sum(ch.get([sq.remote(i) for i in range(20)])))
print("live peers", sum(n.alive for n in ch.runs.cluster().nodes()))
```

`chimaera run` runs a program here with `CHIMAERA_CLUSTER` set, so your
script stays on your machine and its calls run on the cluster. `helper.py`
reaches the workers because the script ships its project (see below):

```bash
chimaera run --cluster lab -- python app.py
```

```text
run 000001994bf57c4e7d950c570c16c8e7
total 2470
live peers 4
```

The run ends when the program exits. `--detach` keeps it going after the
program exits, so its calls finish and you can read them later, but only
for a program that doesn't call `ch.init()` itself: an explicit `ch.init()`
uses its own `detach=False`. Such a program asks with `ch.init(detach=True)`.

`chimaera submit` runs the program itself on the cluster and returns once the
cluster holds it:

```text
$ chimaera submit --cluster lab -- python app.py
00000158597987ff150ede66097dafc5
$ chimaera logs 00000158597987ff150ede66097dafc5 --cluster lab
00000158161c stdout| run 0000036bbfad7594c6fa317401a32a25
00000158161c stdout| total 2470
00000158161c stdout| live peers 4
$ chimaera runs --cluster lab
RUN                               STATE       AGE  COMMAND
0000036bbfad7594c6fa317401a32a25  finished     2s  app.py
00000158597987ff150ede66097dafc5  active       3s  .../chimaera/_submit.py -- python app.py  (detached)
000001994bf57c4e7d950c570c16c8e7  finished     4s  app.py
$ chimaera cancel 00000158597987ff150ede66097dafc5 --cluster lab
run 00000158597987ff150ede66097dafc5 cancelled
```

What `submit` does:

- It ships the current directory as the run's project, and starts the program on a peer as the one call of a detached run. It prints that run's id.
- It runs the program at most once. If the peer running it is lost, the call fails instead of starting the program again.
- `python` or `python3` at the start of the command becomes the peer's own interpreter.
- The program's output is the run's log: `chimaera logs RUN --cluster lab`. The program's own `ch.init()` starts a second run, which `chimaera runs` lists separately.
- The submitted run stays `active` after the program exits, until you end it with `chimaera cancel RUN --cluster lab`.
- It runs `python3 -m chimaera._submit` on your machine first, so that `python3` needs Chimaera installed (`CHIMAERA_PYTHON` names another interpreter).

## Get your code and settings to the workers

A script connected to a cluster ships its project: the `sys.path`
directories that are not the interpreter's, a virtualenv's, or Chimaera's own
(and never your home directory or `/`). They go as one archive named by its
content, so an unchanged project uploads once. Each peer unpacks it once and
puts it on the workers' `sys.path`. Skipped are `.git`, `__pycache__`,
virtualenvs, `node_modules`, `build`, `dist`, `target` and other caches. The
archive may hold at most 100 MiB (`CHIMAERA_PROJECT_MAX_BYTES`); past that,
`ch.init` fails and names the largest files.

Third-party packages are a separate matter. When the project has a
`uv.lock`, each peer installs the locked packages once with
`uv sync --frozen` and its own Python, and workers put them ahead of the
peer's own; `uv` must be on the peers' `PATH`. An install slower than
`CHIMAERA_ENV_BUILD_TIMEOUT` seconds (set on the peers; default 900) fails
the call that needed it. Without a lock, the packages must already be
installed on every peer.

On another machine, workers run the peer's Python (`CHIMAERA_PYTHON`, else
`python3`), which needs Chimaera installed. Functions defined in your script
travel as bytecode, so that Python must be the same minor version as
yours; otherwise the call fails with CH115.

Environment variables don't follow your script unless you say so, even on
the local peer. A run carries `LANG`, `LC_ALL`, `LC_CTYPE`, `TZ` and
`PYTHONHASHSEED`, whatever `ch.init(env=...)` adds, and the variables whose
names match `CHIMAERA_ENV_PASSTHROUGH` (comma-separated, `*` wildcards
allowed). Everything else a worker sees comes from its peer's environment:

```python run
os.environ["APP_STAGE"] = "test"
os.environ["CHIMAERA_ENV_PASSTHROUGH"] = "APP_*"
os.environ["API_KEY"] = "stays-here"

ch.shutdown()
ch.init(env={"MODEL_SIZE": "small"})

@ch.remote
def settings() -> dict:
    return {k: os.environ.get(k) for k in ("APP_STAGE", "MODEL_SIZE", "API_KEY")}

assert settings.remote().result() == {"APP_STAGE": "test", "MODEL_SIZE": "small", "API_KEY": None}
```

Never ship secrets this way: a run's variables are stored with the run record,
which is replicated across peers. Give the peers their secrets in their own
environment (a Kubernetes Secret, the Slurm job's environment). Workers never
see the cluster token: the peer removes `CHIMAERA_CLUSTER_TOKEN`,
`CHIMAERA_TOKEN` and `CHIMAERA_TOKEN_FILE` before it starts them.

## Share a cluster between teams

Every run belongs to a tenant: `ch.init(tenant="team-a")`, else
`CHIMAERA_TENANT`, else the tenant of the script's credential, else
`"default"`. A script holding the cluster token can do anything, including
stop peers. Give each team a driver credential instead:

```bash
chimaera cluster credential --tenant team-a --token-file ca/token > team-a.cred
CHIMAERA_ADDRESS=10.0.0.1:7710 CHIMAERA_TOKEN_FILE=team-a.cred CHIMAERA_CA=ca/ca.pem python app.py
```

A credential can also be a profile's `token_file`. Over the client port, a
script or CLI command presenting it reaches only that tenant's runs, actors,
store, objects and logs. Another tenant's run looks like a missing one:

```text
$ chimaera runs        # with the team-a credential in CHIMAERA_TOKEN_FILE
RUN                               STATE       AGE  COMMAND
000001a35d3c588a7711a36f76bae090  finished     0s  app.py  [tenant team-a]
$ chimaera inspect 000001994bf57c4e7d950c570c16c8e7
chimaera: tenant team-a has no run 000001994bf57c4e7d950c570c16c8e7
```

A credential can't stop a peer. Changing the cluster token revokes every
credential. It is not a sandbox: calls run as the peer's OS user and can
read what the peer can, so it guards teams that trust each other against
mistakes, not against each other. Per-tenant CPU quotas
(`CHIMAERA_TENANT_QUOTAS`, `CHIMAERA_TENANT_SHARES`) and the full security
model are in [../deploy.md](../deploy.md#tenants-and-quotas).

## Take a peer out of service

Send a peer SIGTERM (or SIGUSR1) to drain it. It stops taking new work,
other peers stop sending it any, and it waits up to `CHIMAERA_DRAIN_SECONDS`
(default 30) for its queued and running calls. Meanwhile it hands the Raft
groups it leads to other voters, gives up its voter seats to live peers, and
copies the values it stores to other peers. The spare above, drained:

```text
INFO chimaera_peer::runtime: SIGTERM: draining for up to 30s
INFO chimaera_peer::runtime: drained: this peer holds no group seat another peer could take
INFO chimaera_peer::runtime: drained: no calls left
INFO chimaera_peer::runtime: drained: values handed to other peers copied=0 left=0
```

Actors it hosts are not moved: they restart elsewhere, as on a lost peer, and
that counts against their restart limit. Kubernetes sends SIGTERM; under
Slurm use SIGUSR1, which `srun` forwards. On cloud VMs,
`CHIMAERA_PREEMPTION_NOTICES=gce|aws|azure` drains the peer when the
provider announces a preemption ([../deploy.md](../deploy.md#draining-and-preemption)).

## When a peer dies

The other peers notice, stop sending it work, and retire it. A group keeps
working as long as a majority of its voters is alive. Calls it was running
are retried elsewhere within their `max_attempts`, and actors restart under
their restart policy. With the spare drained, one of the three founders above
was killed with `kill -9`. The cluster kept serving on the two peers left:
`app.py` ran again, and `chimaera status` showed both missing peers as
`unreachable`:

```text
$ chimaera run --cluster lab -- python app.py
run 000003875257ce029e171429efc39dd3
total 2470
live peers 2
$ chimaera status --cluster lab
  ...
  peers:
    127.0.0.1:17704 · 0.0/2.0 CPU in use · 0 queued · unreachable
    127.0.0.1:17702 · 0.0/2.0 CPU in use · 0 queued · leads 0, 1, 2, 3, 4, 5, 6, 7, 8
    127.0.0.1:17703 · 0.0/2.0 CPU in use · 0 queued · unreachable
    127.0.0.1:17701 (this peer) · 0.0/2.0 CPU in use · 0 queued
```

Started again with the same `--dir`, the peer rejoined as a new incarnation.
What a script sees when a call or a value is lost is in
[failures.md](failures.md). If a group's other voters are gone for good,
`chimaera cluster recover` rebuilds it around a surviving peer
([../deploy.md](../deploy.md#providers)).

## Start peers with your platform's tools

You rarely type `peer start` by hand on more than a few machines.
`chimaera peer start --provider auto` (the default) reads Kubernetes, Slurm,
PBS, LSF and SkyPilot environments and works out the founders and its own
address. Each platform has a launcher:

| Platform | Command | Details |
|---|---|---|
| Kubernetes | `chimaera k8s render --image IMG \| kubectl apply -f -` | [Kubernetes](../deploy.md#kubernetes) |
| Slurm | `chimaera slurm-launch -- python app.py`, inside an allocation | [Slurm](../deploy.md#slurm) |
| Machines you reach by SSH | `chimaera up lab` and `chimaera down lab`, with `provider = "ssh"` and `hosts` in the profile | [SSH and plain VMs](../deploy.md#ssh-and-plain-vms) |
| Clouds | a SkyPilot task that runs `chimaera peer start --provider sky` | [Clouds](../deploy.md#clouds) |

[../deploy.md](../deploy.md) also says which of these have been tested where.
Every flag is in [../cli.md](../cli.md), and every environment variable in
[../configuration.md](../configuration.md).

Next: watch what the cluster is doing with [observability.md](observability.md).
