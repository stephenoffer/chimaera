# Run on a cluster

Outgrown your laptop? Your script runs on a cluster unchanged. Here's how
to start one, point scripts at it, ship your code and keep work running
while machines come and go.

## Start on your laptop

There's nothing to set up. Your first call starts a peer for you:

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

Later scripts reuse that peer while it's running. Want it stopped now?

```bash
chimaera stop
```

<details>
<summary>Data directory and idle exit</summary>

That peer keeps its data and `peer.log` in `~/.chimaera/local` (or
`$CHIMAERA_HOME`). It's started with `--idle-timeout 600`, so it exits
after ten minutes with no connected script and no work.

`chimaera stop` and Ctrl-C stop the peer without draining it. Rebuilt or
upgraded Chimaera? Scripts warn with CH111 until you stop the old one
([observability.md](observability.md#understand-the-chnnn-warnings)).

</details>

## Start a cluster

There's no head node. Every machine runs the same `chimaera` binary. Make a
shared token once, plus TLS certificates unless you trust the network:

```bash
chimaera cluster init --out ca                            # ca/ca.pem, ca/ca.key, ca/token
chimaera cluster cert --ca ca --name gpu1 --out tls-gpu1  # one per machine
```

Copy `ca/token` and the machine's `tls-<name>` directory to each machine,
then start its peer:

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
<summary>How many founders you need</summary>

Founders hold the Raft groups from the start. Spares add capacity and
replace lost voters. Use at least three founders. With fewer, one machine
going down costs a group its majority. Give every peer identical `--seeds`.

You'll find `--advertise`, `--bootstrap-expect`, `--data-groups` and the
rest in [../cli.md](../cli.md#chimaera-peer-start).

Skipping TLS? `chimaera cluster token > token` makes a token on its own.
Any token must be at least 16 characters. `--insecure` lets peers join with
no token at all, so use it only on a network you trust.

</details>

<details>
<summary>Four peers on one machine</summary>

Start three founders and a spare with TLS, each with its own ports and data
directory (`--dir`):

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

Give `--dir` an absolute path. Workers start in your script's directory, so
with a relative one they can't find their peer, and calls hang.

The spare (`p3`) comes up ready, holding no groups:

```text
INFO chimaera_peer::cluster: cluster peer ready node=6222675771281290418 addr=127.0.0.1:17704 hosted=[]
```

</details>

## Point a script at the cluster

Name your clusters with profiles in `~/.config/chimaera/clusters.toml`,
then pick one with `ch.init(cluster="lab")`:

```toml
default = "lab"

[clusters.lab]
peers = ["10.0.0.1:7710", "10.0.0.2:7710", "10.0.0.3:7710"]   # the peers' --client-listen
```

<details>
<summary>Cluster choice and connection errors</summary>

A script picks its cluster from the first of these that's set:

1. `ch.init(cluster="lab")`: a profile name, or `tcp://host:port`
2. `CHIMAERA_CLUSTER`: the same, from the environment
3. `CHIMAERA_ADDRESS`: one or more `host:port` client addresses, comma-separated, with the token in `CHIMAERA_TOKEN` or `CHIMAERA_TOKEN_FILE` and the CA in `CHIMAERA_CA`
4. `default` in the profile file
5. the local peer

The CLI chooses the same way, except that `--dir` or `CHIMAERA_HOME`
means the local peer unless you also pass `--cluster`.

`CHIMAERA_CLUSTERS` names another profile file. `peers` lists client
addresses, tried in order until one answers. The lab profile's token comes
from `~/.chimaera/clusters/lab/token`, and its CA from
`~/.chimaera/clusters/lab/ca.pem`. `ch.init(cluster=...)` has to come
before your script's first call.

The keys `token_file` and `ca` name other files. Keep the credentials
themselves out of the profile. A `CHIMAERA_TOKEN` or `CHIMAERA_TOKEN_FILE`
in the environment overrides the profile's token. `default = "local"`, or
`cluster="local"`, means the local peer.

Trying the one-machine cluster above? Copy `ca/token` and `ca/ca.pem` to
`~/.chimaera/clusters/lab/` and set `peers` to
`["127.0.0.1:17711", "127.0.0.1:17712", "127.0.0.1:17713"]`.
Then `ch.init(cluster="lab")` connects to it.

To switch clusters, call `ch.shutdown()`, then `ch.init` again. Get the
name wrong and it fails, listing the profiles the file does have:

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

Other ways a connection can fail:

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
  durability: commits survive a machine loss (asked for: machine)
  ...
  automatic: raft sync fsync (the default): commits take p50 266 µs, p99 2850 µs, 1.2 entries each; the oldest entry waits p99 5004 µs
  peers:
    127.0.0.1:17704 · 0.0/2.0 CPU in use · 0 queued
    127.0.0.1:17702 · 0.0/2.0 CPU in use · 0 queued · leads 0, 1, 2, 3, 4, 5, 6, 7, 8
    127.0.0.1:17703 · 0.0/2.0 CPU in use · 0 queued
    127.0.0.1:17701 (this peer) · 0.0/2.0 CPU in use · 0 queued
  ...
```

## Run a script against the cluster

Say you have a two-file project, `helper.py` and `app.py`:

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

`chimaera run` starts your program right here. Only its calls go out:

```bash
chimaera run --cluster lab -- python app.py
```

```text
run 000001994bf57c4e7d950c570c16c8e7
total 2470
live peers 4
```

<details>
<summary>Keeping a run going</summary>

`chimaera run` starts your program with `CHIMAERA_CLUSTER` set.
`helper.py` gets to the workers as part of your project (next section).

The run ends when the program exits. `--detach` keeps it going, so the
calls finish and you can read them later. One catch: a program that calls
`ch.init()` itself ignores the flag, because an explicit `ch.init()`
defaults to `detach=False`. Pass `ch.init(detach=True)` there instead.

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

Your script uploads its project. Got a `uv.lock`? Then each peer also
installs your third-party packages.

<details>
<summary>Project limits and packages</summary>

What counts as the project? The `sys.path` directories that aren't the
interpreter's, a virtualenv's or Chimaera's own, and never your home
directory or `/`. It travels as one archive named by its content and
uploads once until it changes. Each peer unpacks it once onto the workers'
`sys.path`. The archive skips `.git`, `__pycache__`, virtualenvs,
`node_modules`, `build`, `dist`, `target` and other caches. It can hold at
most 100 MiB (`CHIMAERA_PROJECT_MAX_BYTES`). Past that, `ch.init` fails and
names the largest files.

Without a `uv.lock`, every peer needs your third-party packages
already. With one, they go in once per peer, with `uv sync --frozen` and
that peer's Python, and workers put them ahead of what that Python already
has. `uv` has to be on the peers' `PATH`, and an install slower than
`CHIMAERA_ENV_BUILD_TIMEOUT` seconds (set on the peers; default 900) fails
the call that needed it.

On another machine, workers run the peer's Python (`CHIMAERA_PYTHON`, else
`python3`), which needs Chimaera installed. Functions defined in your
script travel as bytecode, so that Python has to match your minor version,
or the call fails with CH115.

</details>

Environment variables don't come along unless you send them with
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

Never ship secrets this way, though. A run's variables are stored with its
record, which is replicated across peers.

<details>
<summary>Secrets and passed-through variables</summary>

Put secrets in the peers' own environment instead (a Kubernetes Secret, the
Slurm job's environment). Workers take everything else from their peer's
environment. A variable stays behind even when everything runs on your
laptop.

A run also carries `LANG`, `LC_ALL`, `LC_CTYPE`, `TZ` and `PYTHONHASHSEED`.
`CHIMAERA_ENV_PASSTHROUGH` is comma-separated, with `*` wildcards allowed.

Workers never see a token: the peer removes
`CHIMAERA_CLUSTER_TOKEN`, `CHIMAERA_TOKEN` and `CHIMAERA_TOKEN_FILE` before
starting them.

</details>

## Share a cluster between teams

The cluster token can do anything, including stop peers. That's a lot to
hand out, so give each team a driver credential instead:

```bash
chimaera cluster credential --tenant team-a --token-file ca/token > team-a.cred
CHIMAERA_ADDRESS=10.0.0.1:7710 CHIMAERA_TOKEN_FILE=team-a.cred CHIMAERA_CA=ca/ca.pem python app.py
```

A credential reaches only its tenant's runs, actors, store, objects and
logs. With it, another tenant's run just looks missing:

```text
$ chimaera runs        # with the team-a credential in CHIMAERA_TOKEN_FILE
RUN                               STATE       AGE  COMMAND
000001a35d3c588a7711a36f76bae090  finished     0s  app.py  [tenant team-a]
$ chimaera inspect 000001994bf57c4e7d950c570c16c8e7
chimaera: tenant team-a has no run 000001994bf57c4e7d950c570c16c8e7
```

<details>
<summary>Why a credential isn't a sandbox</summary>

The limit holds for a script or CLI command presenting the credential over
the client port.

But a credential isn't a sandbox. Calls run as the peer's OS user and can
read whatever it can. So credentials protect teams that trust each other
from mistakes, not from each other.

Credentials also work as a profile's `token_file`. Every run belongs to a
tenant: `ch.init(tenant="team-a")`, else `CHIMAERA_TENANT`, else the tenant
of the script's credential, else `"default"`.

Credentials can't stop peers, and changing the cluster token revokes them
all. Per-tenant CPU quotas (`CHIMAERA_TENANT_QUOTAS`,
`CHIMAERA_TENANT_SHARES`) and the full security model are in
[../deploy.md](../deploy.md#tenants-and-quotas).

</details>

## Take a peer out of service

To drain a peer, send it SIGTERM (or SIGUSR1). Here's the spare from above,
drained:

```text
INFO chimaera_peer::runtime: SIGTERM: draining for up to 30s
INFO chimaera_peer::runtime: drained: this peer holds no group seat another peer could take
INFO chimaera_peer::runtime: drained: no calls left
INFO chimaera_peer::runtime: drained: values handed to other peers copied=0 left=0
```

<details>
<summary>What a drain does</summary>

A draining peer stops taking new work, and the other peers stop sending it
any. It waits up to `CHIMAERA_DRAIN_SECONDS` (default 30) for its queued and
running calls. Meanwhile, it hands the Raft groups it leads to other
voters, gives its voter seats to live peers and copies its stored values
elsewhere.

Kubernetes sends SIGTERM for you. Under Slurm, use SIGUSR1, which `srun`
forwards. Note that actors don't move with a drain. They restart elsewhere,
as they would on a lost peer, and that counts against their restart limit.

On cloud VMs, `CHIMAERA_PREEMPTION_NOTICES=gce|aws|azure` drains the peer
when the provider announces a preemption
([../deploy.md](../deploy.md#draining-and-preemption)).

</details>

## When a peer dies

A group keeps working as long as a majority of its voters is alive. Here
we drained the spare, then killed a founder with `kill -9`. The two peers
left kept serving: `app.py` ran again, and `chimaera status` showed both
missing peers as `unreachable`:

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
<summary>After a peer is killed</summary>

The survivors notice the dead peer, stop sending it work and retire it.
Its calls are retried elsewhere within their `max_attempts`, and its
actors restart under their restart policy.

Restarted with its old `--dir`, the killed peer rejoined as a new
incarnation. And if a group's other voters are gone for good,
`chimaera cluster recover` rebuilds it around one surviving peer
([../deploy.md](../deploy.md#providers)).

</details>

[failures.md](failures.md) covers what a script sees when a call or a
value is lost.

## Start peers with your platform's tools

Past a few machines, you won't want to type `peer start` by hand, and you
don't have to. `chimaera peer start --provider auto` (the default) reads
Kubernetes, Slurm, PBS, LSF and SkyPilot environments to find the founders
and its own address.

<details>
<summary>Launchers by platform</summary>

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
