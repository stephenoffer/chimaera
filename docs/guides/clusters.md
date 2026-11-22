# Run on a cluster

Your laptop script runs on a cluster unchanged. This guide shows you how to
start one, point scripts at it, ship your code and keep work running while
machines come and go.

## Start on your laptop

Nothing to set up. Your first call starts a peer:

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

Later scripts reuse that peer while it runs. To stop it now:

```bash
chimaera stop
```

<details>
<summary>More: where it keeps data, when it exits by itself, upgrades</summary>

That peer keeps its data and `peer.log` in `~/.chimaera/local` (or
`$CHIMAERA_HOME`). Started with `--idle-timeout 600`, it exits after ten
minutes with no connected script and no work.

`chimaera stop` and Ctrl-C stop a peer without draining. After you rebuild
or upgrade Chimaera, scripts warn with CH111 until you stop the old peer
([observability.md](observability.md#understand-the-chnnn-warnings)).

</details>

## Start a cluster

There is no head node: every machine runs the same `chimaera` binary. Make
a shared token once, and TLS certificates unless you trust the network:

```bash
chimaera cluster init --out ca                            # ca/ca.pem, ca/ca.key, ca/token
chimaera cluster cert --ca ca --name gpu1 --out tls-gpu1  # one per machine
```

Copy `ca/token` and the machine's `tls-<name>` directory to each machine,
and start its peer:

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

<details>
<summary>More: how many founders you need, other flags, tokens without TLS</summary>

Founders hold the Raft groups from the start; spares add capacity and
replace lost voters. Use at least three founders, or one machine going down
costs a group its majority. Give every peer identical `--seeds`.

`--advertise`, `--bootstrap-expect`, `--data-groups` and the rest are in
[../cli.md](../cli.md#chimaera-peer-start).

Without TLS, `chimaera cluster token > token` makes a token by itself; a
token must be at least 16 characters. `--insecure` lets peers join with no
token, so use it only on a network you trust.

</details>

<details>
<summary>More: try a four-peer cluster on one machine</summary>

Start three founders and a spare with TLS, each on separate ports and a
separate data directory (`--dir`):

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

Give `--dir` an absolute path. Workers start in your script's directory,
so with a relative one they can't find their peer and calls hang.

The spare (`p3`) is ready and holds no groups:

```text
INFO chimaera_peer::cluster: cluster peer ready node=6222675771281290418 addr=127.0.0.1:17704 hosted=[]
```

</details>

## Point a script at the cluster

Profiles in `~/.config/chimaera/clusters.toml` name your clusters, and
`ch.init(cluster="lab")` picks one:

```toml
default = "lab"

[clusters.lab]
peers = ["10.0.0.1:7710", "10.0.0.2:7710", "10.0.0.3:7710"]   # the peers' --client-listen
```

<details>
<summary>More: which setting wins, where the token and CA go, switching, connection errors</summary>

A script picks its cluster from the first of these that is set:

1. `ch.init(cluster="lab")`: a profile name, or `tcp://host:port`
2. `CHIMAERA_CLUSTER`: the same, from the environment
3. `CHIMAERA_ADDRESS`: one or more `host:port` client addresses, comma-separated, with the token in `CHIMAERA_TOKEN` or `CHIMAERA_TOKEN_FILE` and the CA in `CHIMAERA_CA`
4. `default` in the profile file
5. the local peer

The CLI chooses the same way, except that `--dir` or `CHIMAERA_HOME`
means the local peer unless you also pass `--cluster`.

`CHIMAERA_CLUSTERS` names another profile file. `peers` lists client
addresses, tried in order until one answers. The token comes from
`~/.chimaera/clusters/lab/token` and the CA from
`~/.chimaera/clusters/lab/ca.pem`. `ch.init(cluster=...)` must come before
your script's first call.

The keys `token_file` and `ca` name other files. Keep credentials out of
the profile itself. A `CHIMAERA_TOKEN` or `CHIMAERA_TOKEN_FILE` in the
environment overrides the profile's token. `default = "local"`, or
`cluster="local"`, means the local peer.

For the one-machine cluster above, copy `ca/token` and `ca/ca.pem` to
`~/.chimaera/clusters/lab/` and set `peers` to this:
`["127.0.0.1:17711", "127.0.0.1:17712", "127.0.0.1:17713"]`.
Then `ch.init(cluster="lab")` connects to it.

To switch, call `ch.shutdown()` and then `ch.init` again. An unknown name
fails and lists the profiles the file has:

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

</details>

The CLI takes `--cluster` too:

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

Take a two-file project, `helper.py` and `app.py`:

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

`chimaera run` starts your program here, and only its calls go out:

```bash
chimaera run --cluster lab -- python app.py
```

```text
run 000001994bf57c4e7d950c570c16c8e7
total 2470
live peers 4
```

<details>
<summary>More: how the program finds the cluster, keeping a run going with --detach</summary>

`chimaera run` starts your program with `CHIMAERA_CLUSTER` set.
`helper.py` reaches the workers with the project (see below).

The run ends when the program exits. `--detach` keeps it going so its
calls finish and you can read them later. A program that calls
`ch.init()` itself ignores the flag, since an explicit `ch.init()` uses
its own `detach=False`; ask with `ch.init(detach=True)` there.

</details>

To run the whole program remotely too, use `chimaera submit`:

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

<details>
<summary>More: at-most-once starts, logs, ending a submitted run</summary>

`submit` returns as soon as the cluster holds the program. It ships the
current directory as the project of a detached run, starts the program on
one of the peers as that run's one call, and prints the run's id.

- The program runs at most once. If its peer is lost, the call fails rather than start it again.
- A leading `python` or `python3` becomes the peer's own interpreter.
- Its output is the run's log: `chimaera logs RUN --cluster lab`. Its own `ch.init()` starts a second run, which `chimaera runs` lists separately.
- The run stays `active` after its program exits, until `chimaera cancel RUN --cluster lab` ends it.
- `submit` first runs `python3 -m chimaera._submit` on your machine, so that `python3` needs Chimaera installed (`CHIMAERA_PYTHON` names another interpreter).

</details>

## Get your code and settings to the workers

Your script uploads its project, and with a `uv.lock` each peer also
installs your third-party packages.

<details>
<summary>More: what counts as the project, its size limit, package installs, Python versions</summary>

The project is the `sys.path` directories that aren't the interpreter's, a
virtualenv's or Chimaera's own, and never your home directory or `/`. An
unchanged project uploads once. Without a `uv.lock`, every peer must have
your third-party packages already.

The project goes as one archive named by its content, and each peer
unpacks it once onto the workers' `sys.path`. The archive skips `.git`,
`__pycache__`, virtualenvs, `node_modules`, `build`, `dist`, `target` and
other caches. It may hold at most 100 MiB (`CHIMAERA_PROJECT_MAX_BYTES`);
past that, `ch.init` fails and names the largest files.

Packages from a `uv.lock` go in once per peer, with `uv sync --frozen` and
that peer's Python, and workers put them ahead of what that Python
already has. `uv`
must be on the peers' `PATH`, and an install slower than
`CHIMAERA_ENV_BUILD_TIMEOUT` seconds (set on the peers; default 900) fails
the call that needed it.

On another machine, workers run the peer's Python (`CHIMAERA_PYTHON`, else
`python3`), which needs Chimaera installed. Functions defined in your
script travel as bytecode, so that Python must match your minor version,
or the call fails with CH115.

</details>

Environment variables stay behind unless you send them with
`ch.init(env=...)` or name them in `CHIMAERA_ENV_PASSTHROUGH`:

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

Never ship secrets this way: a run's variables are stored with its record,
which is replicated across peers.

<details>
<summary>More: where secrets go, variables every run carries, passthrough patterns, what workers never see</summary>

Put secrets in the peers' own environment (a Kubernetes Secret, the Slurm
job's environment). Workers take everything else from their peer's
environment, and a variable stays behind even when everything runs on
your laptop.

A run also carries `LANG`, `LC_ALL`, `LC_CTYPE`, `TZ` and `PYTHONHASHSEED`.
`CHIMAERA_ENV_PASSTHROUGH` is comma-separated, with `*` wildcards allowed.

Workers never see the token: the peer removes
`CHIMAERA_CLUSTER_TOKEN`, `CHIMAERA_TOKEN` and `CHIMAERA_TOKEN_FILE` before
starting them.

</details>

## Share a cluster between teams

The cluster token can do anything, including stop peers. Give each team a
driver credential instead:

```bash
chimaera cluster credential --tenant team-a --token-file ca/token > team-a.cred
CHIMAERA_ADDRESS=10.0.0.1:7710 CHIMAERA_TOKEN_FILE=team-a.cred CHIMAERA_CA=ca/ca.pem python app.py
```

A credential reaches only its tenant's runs, actors, store, objects and
logs. Another tenant's run looks missing:

```text
$ chimaera runs        # with the team-a credential in CHIMAERA_TOKEN_FILE
RUN                               STATE       AGE  COMMAND
000001a35d3c588a7711a36f76bae090  finished     0s  app.py  [tenant team-a]
$ chimaera inspect 000001994bf57c4e7d950c570c16c8e7
chimaera: tenant team-a has no run 000001994bf57c4e7d950c570c16c8e7
```

<details>
<summary>More: why a credential is not a sandbox, picking a tenant, revoking credentials, quotas</summary>

The limit holds for a script or CLI command presenting the credential over
the client port.

A credential is not a sandbox. Calls run as the peer's OS user and can read
whatever it can, so credentials guard teams that trust each other against
mistakes, not against each other.

Credentials also work as a profile's `token_file`. Every run belongs to a
tenant: `ch.init(tenant="team-a")`, else `CHIMAERA_TENANT`, else the tenant
of the script's credential, else `"default"`.

Credentials can't stop peers, and changing the cluster token revokes them
all. Per-tenant CPU quotas (`CHIMAERA_TENANT_QUOTAS`,
`CHIMAERA_TENANT_SHARES`) and the full security model are in
[../deploy.md](../deploy.md#tenants-and-quotas).

</details>

## Take a peer out of service

Send a peer SIGTERM (or SIGUSR1) to drain it. The spare above, drained:

```text
INFO chimaera_peer::runtime: SIGTERM: draining for up to 30s
INFO chimaera_peer::runtime: drained: this peer holds no group seat another peer could take
INFO chimaera_peer::runtime: drained: no calls left
INFO chimaera_peer::runtime: drained: values handed to other peers copied=0 left=0
```

<details>
<summary>More: what a drain waits for and hands over, Slurm, actors, cloud preemption</summary>

A draining peer stops taking new work, waits up to
`CHIMAERA_DRAIN_SECONDS` (default 30) for its queued and running calls,
and meanwhile hands what it holds to other peers.

Kubernetes sends SIGTERM. Under Slurm, use SIGUSR1, which `srun` forwards.
Actors don't move with a drain: they restart elsewhere as on a lost peer,
which counts against their restart limit.

Other peers stop sending a draining peer any work. It hands the Raft groups
it leads to other voters, gives its voter seats to live peers and copies
its stored values elsewhere.

On cloud VMs, `CHIMAERA_PREEMPTION_NOTICES=gce|aws|azure` drains the peer
when the provider announces a preemption
([../deploy.md](../deploy.md#draining-and-preemption)).

</details>

## When a peer dies

A group keeps working while a majority of its voters is alive. Here the
spare was drained, then a founder killed with `kill -9`. The two peers left
kept serving: `app.py` ran again, and `chimaera status` showed both missing
peers as `unreachable`:

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

<details>
<summary>More: what happens to its calls and actors, bringing a peer back, rebuilding a group</summary>

The survivors notice the dead peer, stop sending it work and retire it.
Its calls are retried elsewhere within their `max_attempts`, and its
actors restart under their restart policy.

Started again with its old `--dir`, the killed peer rejoined as a new
incarnation. If a group's other voters are gone for good,
`chimaera cluster recover` rebuilds it around one surviving peer
([../deploy.md](../deploy.md#providers)).

</details>

[failures.md](failures.md) covers what a script sees when a call or a
value is lost.

## Start peers with your platform's tools

Past a few machines, you won't type `peer start` by hand.
`chimaera peer start --provider auto` (the default) reads Kubernetes, Slurm,
PBS, LSF and SkyPilot environments to find the founders and its own address.

<details>
<summary>More: the launcher for Kubernetes, Slurm, SSH and clouds (table), and where each was tested</summary>

Each platform has a launcher:

| Platform | Command | Details |
|---|---|---|
| Kubernetes | `chimaera k8s render --image IMG \| kubectl apply -f -` | [Kubernetes](../deploy.md#kubernetes) |
| Slurm | `chimaera slurm-launch -- python app.py`, inside an allocation | [Slurm](../deploy.md#slurm) |
| Machines you reach by SSH | `chimaera up lab` and `chimaera down lab`, with `provider = "ssh"` and `hosts` in the profile | [SSH and plain VMs](../deploy.md#ssh-and-plain-vms) |
| Clouds | a SkyPilot task that runs `chimaera peer start --provider sky` | [Clouds](../deploy.md#clouds) |

[The deployment guide](../deploy.md) also says which of these have been tested where.

</details>

Every flag is in [../cli.md](../cli.md), and every environment variable in
[../configuration.md](../configuration.md).

Next: watch what your cluster is doing with [observability.md](observability.md).
