# CLI reference

Every command and flag of `chimaera`, the one binary that is both peer and
command-line tool. Summaries and flag descriptions come from
`chimaera --help` and `chimaera <command> --help` of
`chimaera 0.1.0 (peer level 1)` (`chimaera --version`). Where the help is
empty or silent, the table says what the code does.

For a walkthrough, see [guides/clusters.md](guides/clusters.md) and
[guides/observability.md](guides/observability.md).

## Global options

```text
chimaera [OPTIONS] <COMMAND>
```

| Flag | Description |
|---|---|
| `--dir <DIR>` | Data directory of the local peer (default: `$CHIMAERA_HOME` or `~/.chimaera/local`). Every command accepts it |
| `-h, --help` | Print help |
| `-V, --version` | Print version: the release and its peer level, e.g. `chimaera 0.1.0 (peer level 1)` |

Installing the Python wheel puts `chimaera` on your `PATH`, and that
command runs the binary installed with the package. A failing command prints
`chimaera: <message>` to stderr and exits with status 1.

## Which peer a command asks

`status`, `runs`, `actors`, `cancel`, `logs`, `inspect` and `explain` take
`--cluster <CLUSTER>`: a profile name from ~/.config/chimaera/clusters.toml,
or tcp://host:port. Their tables below leave that flag out. To pick the peer,
the first of these that applies wins:

1. `--cluster`.
2. `--dir` or `CHIMAERA_HOME`: whichever peer uses that directory.
3. As for a driver: `CHIMAERA_CLUSTER`, then `CHIMAERA_ADDRESS` (with
   `CHIMAERA_TOKEN` or `CHIMAERA_TOKEN_FILE`, and `CHIMAERA_CA`), then the
   profile file's `default`.
4. The local peer.

[guides/clusters.md](guides/clusters.md#point-a-script-at-the-cluster)
describes profiles.

`objects`, `profiles`, `stop`, `doctor` and `cluster recover` always use this
machine's data directory. `objects --cluster` is a plain switch with another
meaning: every live peer's store.

## Run programs

### `chimaera run`

Run a program against the local peer, or with --cluster against a cluster
(`chimaera run --cluster lab -- python app.py`).

```text
chimaera run [OPTIONS] <COMMAND>...
```

| Flag | Description |
|---|---|
| `<COMMAND>...` | The program and its arguments, after `--` |
| `--detach` | Keep the program's run going after the program exits |
| `--cluster <CLUSTER>` | A cluster profile or tcp://host:port, passed to the program as $CHIMAERA_CLUSTER |

`chimaera run` exits with your program's exit status. It sets
`CHIMAERA_DETACH` for it (`1` with `--detach`, else `0`), plus `CHIMAERA_CLUSTER`
or, without `--cluster`, `CHIMAERA_HOME` pointing at the peer's directory.

A program that calls `ch.init()` itself ignores `--detach`, since `detach`
defaults to `False`. Use `ch.init(detach=True)` there.

### `chimaera submit`

Run a program remotely. One peer runs it as the only call of a
detached run, with this directory shipped as its project. `submit` prints
the run's id and returns (`chimaera logs RUN`, `chimaera cancel RUN`).

```text
chimaera submit [OPTIONS] <COMMAND>...
```

| Flag | Description |
|---|---|
| `<COMMAND>...` | The program and its arguments, after `--` |
| `--cluster <CLUSTER>` | A cluster profile or tcp://host:port (default: as for a driver) |

It runs at most once. A leading `python` or `python3` in the
command becomes the peer's own interpreter. If no peer has taken it
within 60 seconds, `submit` fails. The run stays active after your
program exits, until `chimaera cancel RUN` ends it.

`submit` itself runs `$CHIMAERA_PYTHON -m chimaera._submit` (default
`python3`) on this machine, so that interpreter needs Chimaera installed.

## Inspect

### `chimaera status`

Peer status: executors, queue, CPU in use, why work is waiting.

```text
chimaera status [OPTIONS]
```

| Flag | Description |
|---|---|
| `--json` | Print the status as JSON |

On a clustered peer, the output adds every peer (unreachable ones marked) and
each Raft group this peer holds, with its leader and voters.

### `chimaera runs`

The cluster's runs, newest first: state, age and the command that started
each.

```text
chimaera runs [OPTIONS]
```

| Flag | Description |
|---|---|
| `--limit <LIMIT>` | Show at most this many (0: all) [default: 20] |
| `--json` | Print the runs as JSON |

### `chimaera actors`

The cluster's live named actors: tenant, name, state.

```text
chimaera actors [OPTIONS]
```

| Flag | Description |
|---|---|
| `--prefix <PREFIX>` | Only names starting with this [default: ""] |
| `--json` | Print the actors as JSON |

### `chimaera inspect`

Summary of a run: tasks by state.

```text
chimaera inspect [OPTIONS] <RUN>
```

| Flag | Description |
|---|---|
| `<RUN>` | The run id, 32 hex digits |
| `--json` | Print the run's state, whether it is detached, and its counts as JSON |

### `chimaera explain`

What happened to one task of a run (attempts, measurements, waiting reason),
or the tuning decisions the peer made.

```text
chimaera explain [OPTIONS] <RUN>
```

| Flag | Description |
|---|---|
| `<RUN>` | The run id, 32 hex digits |
| `--task <TASK>` | Task id (hex) to explain: the second half of a future's `id` |
| `--decisions` | Show tuning decisions |
| `--json` | Print the attempts, decisions, waiting reason and task state as JSON |

### `chimaera logs`

What a run's calls printed on this peer, kept after the driver has gone.

```text
chimaera logs [OPTIONS] <RUN>
```

| Flag | Description |
|---|---|
| `<RUN>` | The run id, 32 hex digits |
| `-f, --follow` | Keep waiting for more output (Ctrl-C to stop) |

Each line reads `<task> <stream>| <text>`, where `<task>` is the first 12 hex
digits of the call's task id.

### `chimaera profiles`

What the peer has learned about each function: calls, time, cores, memory,
and the reservation the next call gets.

```text
chimaera profiles [OPTIONS]
```

| Flag | Description |
|---|---|
| `--fn <NAME>` | Only functions whose name contains this |
| `--json` | Print the profiles as JSON |

It reads `profiles.pb` from the peer's directory. The peer rewrites that file
every 30 seconds while profiles change, and when it stops.

### `chimaera objects`

What this peer's object store holds: in memory, on disk (spilled or
replicas), per run, the largest objects, NumPy arenas and the reserve.

```text
chimaera objects [OPTIONS]
```

| Flag | Description |
|---|---|
| `--json` | Print the inventory as JSON |
| `--cluster` | Every live peer's store, not only this one's |

### `chimaera doctor`

Check what this host supports (cgroups, shared memory, Python).

```text
chimaera doctor [OPTIONS]
```

No flags beyond `--dir`. It prints the OS, CPUs and memory, NUMA nodes and
GPUs, whether isolated accounting works, and the data directory's fsync
latency.

## Control

### `chimaera cancel`

Cancel a run: its unfinished calls are cancelled and its actors stopped.

```text
chimaera cancel [OPTIONS] [RUNS]...
```

| Flag | Description |
|---|---|
| `[RUNS]...` | Runs to cancel, ids as `chimaera runs` prints them |
| `--match <PATTERN>` | Instead of ids: the active runs whose command contains this text |
| `--yes` | With `--match`, cancel them. Without it, `--match` only lists what would be cancelled |

### `chimaera stop`

Stop the local peer.

```text
chimaera stop [OPTIONS]
```

No flags beyond `--dir`. It stops the peer without draining; send SIGTERM
(or SIGUSR1) to drain first.

## Peers and clusters

### `chimaera peer start`

Run a peer in the foreground.

```text
chimaera peer start [OPTIONS]
```

| Flag | Description |
|---|---|
| `--provider <PROVIDER>` | Where the founders and this peer's address come from: auto, local, static (the flags), k8s, slurm, hpc (PBS/LSF) or sky. `auto` treats --listen as static and --idle-timeout as local, else detects the platform from its environment. Explicit flags win over discovered values [default: auto] [possible values: auto, local, static, k8s, slurm, hpc, sky] |
| `--client-port <CLIENT_PORT>` | Under k8s, slurm, hpc and sky: accept drivers on other machines on this port (default: $CHIMAERA_CLIENT_PORT, else the peer port + 10) |
| `--cpus <CPUS>` | CPU capacity to offer (default: this process's allocation) |
| `--idle-timeout <IDLE_TIMEOUT>` | Exit after this many seconds with no sessions and no work |
| `--listen <LISTEN>` | Join a cluster: address to listen on for other peers (host:port). Without it the peer runs alone |
| `--advertise <ADVERTISE>` | Address other peers dial to reach this one (default: --listen) |
| `--seeds <SEEDS>` | Founding peers' addresses, comma-separated. A peer whose address is listed is a founder; others join as spares |
| `--bootstrap-expect <BOOTSTRAP_EXPECT>` | Number of founders; must match --seeds (a guard against typos) |
| `--cluster <CLUSTER>` | Cluster name; peers of different clusters refuse each other [default: chimaera] |
| `--data-groups <DATA_GROUPS>` | Shard groups the vshards are split across (fixed at formation) [default: 8] |
| `--token-file <TOKEN_FILE>` | File holding the cluster's shared token (`chimaera cluster token`). Also read from $CHIMAERA_CLUSTER_TOKEN. Required unless --insecure |
| `--insecure` | Let peers join without a token (only on a network you trust) |
| `--tls-dir <TLS_DIR>` | Directory with this peer's mutual-TLS files (`chimaera cluster cert`) |
| `--client-listen <CLIENT_LISTEN>` | Also accept drivers on other machines at this address (host:port). They must present the cluster token, and use TLS when --tls-dir is set |

The code refuses:

- `--seeds` without `--listen`, or the reverse (static provider).
- `--client-port` under local and static providers; use
  `--client-listen` there.
- A token shorter than 16 characters.

Give `--dir` an absolute path. SIGTERM and SIGUSR1 drain a running peer for up
to `CHIMAERA_DRAIN_SECONDS`; Ctrl-C stops it at once. The environment variables
that tune peers are in [configuration.md](configuration.md).

### `chimaera cluster token`

Print a new random cluster token.

```text
chimaera cluster token [OPTIONS]
```

The token is 64 hex digits from `/dev/urandom`.

### `chimaera cluster init`

Create a cluster CA and token in OUT (ca.pem, ca.key, token).

```text
chimaera cluster init [OPTIONS] --out <OUT>
```

| Flag | Description |
|---|---|
| `--out <OUT>` | Directory to create; it receives `ca.pem`, `ca.key` and `token` |

### `chimaera cluster cert`

Issue a peer certificate from the CA in --ca into OUT, the directory that
peer's --tls-dir names (ca.pem, peer.pem, peer.key).

```text
chimaera cluster cert [OPTIONS] --ca <CA> --name <NAME> --out <OUT>
```

| Flag | Description |
|---|---|
| `--ca <CA>` | The directory `chimaera cluster init` created |
| `--name <NAME>` | A name for the peer (its host name, say); recorded in the certificate |
| `--out <OUT>` | Directory to write `ca.pem`, `peer.pem` and `peer.key` to |

### `chimaera cluster credential`

Print a driver credential for TENANT. Drivers presenting it (as
$CHIMAERA_TOKEN, or a profile's token file) reach only that tenant's runs,
actors, store and logs, and cannot stop peers. Changing the cluster token
revokes every credential.

```text
chimaera cluster credential [OPTIONS] --tenant <TENANT>
```

| Flag | Description |
|---|---|
| `--tenant <TENANT>` | The tenant: 1 to 64 letters, digits, `-` or `_` |
| `--token-file <TOKEN_FILE>` | The cluster token's file (default: $CHIMAERA_CLUSTER_TOKEN) |

`CHIMAERA_TOKEN_FILE`, naming a file that holds the credential, works too.

### `chimaera cluster recover`

Rebuild group GROUP around this stopped peer when the group's other voters
are gone for good. This peer becomes the group's only voter and keeps the
commits it holds; commits it never received are lost. Once it runs, the
cluster adds voters back. Use the surviving replica that holds the most of
the group's log.

```text
chimaera cluster recover [OPTIONS] --group <GROUP>
```

| Flag | Description |
|---|---|
| `--group <GROUP>` | The group number, as `chimaera status` lists it |
| `--force-new-quorum` | Required: confirms that the group's other voters will not return |

Run it against a stopped peer's `--dir`, then start that peer.

## Launchers

[deploy.md](deploy.md) covers each platform in depth.

### `chimaera k8s render`

Print the Services, StatefulSet and PodDisruptionBudget of a cluster
(`chimaera k8s render --image IMG | kubectl apply -f -`).

```text
chimaera k8s render [OPTIONS] --image <IMAGE>
```

| Flag | Description |
|---|---|
| `--name <NAME>` | StatefulSet name; its pods are NAME-0, NAME-1, ... [default: chimaera] |
| `--namespace <NAMESPACE>` | Namespace for every object (default: the one `kubectl apply` uses) |
| `--image <IMAGE>` | Image with `chimaera` on PATH and the chimaera wheel installed (deploy/images/Dockerfile), running as uid 1000 |
| `--replicas <REPLICAS>` | Number of pods [default: 3] |
| `--founders <FOUNDERS>` | Pods 0..FOUNDERS-1 found the cluster (default: min(3, replicas)) |
| `--port <PORT>` | The peer port [default: 7700] |
| `--client-port <CLIENT_PORT>` | Port of the client Service, for drivers outside the pods [default: 7710] |
| `--cpus <CPUS>` | CPU per pod, request and limit (the peer offers its limit), e.g. 4 or 500m |
| `--memory <MEMORY>` | Memory per pod, request and limit, e.g. 8Gi |
| `--storage <STORAGE>` | Size of each pod's data volume (Raft logs, code, results) [default: 10Gi] |
| `--token-secret <TOKEN_SECRET>` | Secret holding the cluster token under the key `token` (default: NAME-token) |
| `--provisioner <MAX>` | Also run the provisioner in the cluster, growing the pool from --replicas up to MAX peers under load (a Deployment whose Role may only read this StatefulSet and change its scale). Needs --namespace |

With fewer than 3 founders it warns on stderr: losing one peer can then
halt the cluster's groups until it returns.

### `chimaera slurm-launch`

Inside a Slurm allocation: start a peer on every node, run COMMAND on this
one against its peer, then stop the peers.

```text
chimaera slurm-launch [OPTIONS] <COMMAND>...
```

| Flag | Description |
|---|---|
| `<COMMAND>...` | The program and its arguments, after `--` |
| `--port <PORT>` | Peer port (default: 7700 + SLURM_JOB_ID % 1000) |

It fails outside an allocation (`SLURM_JOB_ID` unset). The data directory
defaults to `$TMPDIR/chimaera-$SLURM_JOB_ID` (`/tmp` when `TMPDIR` is unset)
and must be node-local. The job makes its own cluster token unless
`CHIMAERA_CLUSTER_TOKEN` is set. The client port is the peer's port + 1000
unless `CHIMAERA_CLIENT_PORT` is set.

### `chimaera up`

Start the peers of an SSH cluster profile in clusters.toml.

```text
chimaera up [OPTIONS] <NAME>
```

| Flag | Description |
|---|---|
| `<NAME>` | A profile with `provider = "ssh"` |

The profile lists `hosts` (SSH destinations) and `peers` (client
addresses). Optional keys: `dir` (default `~/.chimaera/peers/NAME`),
`port` (7700), `client_port` (port + 10), `ssh_user`, `remote_bin` and
`token_file` (default `~/.chimaera/clusters/NAME/token`). `up` uses the
system `ssh` and `scp`, so `~/.ssh/config` applies.

### `chimaera down`

Stop the peers of an SSH cluster profile.

```text
chimaera down [OPTIONS] <NAME>
```

| Flag | Description |
|---|---|
| `<NAME>` | A profile with `provider = "ssh"` |

The peers' data directories are kept, so the next `up` resumes the same
cluster.

## Related: the provisioner

Autoscaling is a Python module rather than a `chimaera` subcommand. From
`python -m chimaera.provisioner --help`:

```text
usage: python -m chimaera.provisioner [-h] (--k8s-statefulset NAME | --slurm |
                                      --sky LAUNCH_ARGS) [--base BASE]
                                      [--partition PARTITION]
                                      [--namespace NAMESPACE]
                                      [--cluster CLUSTER] [--min MIN]
                                      [--max MAX] [--up-after SECONDS]
                                      [--down-after SECONDS]
                                      [--start-grace SECONDS]
                                      [--interval SECONDS]
```

| Flag | Description |
|---|---|
| `--k8s-statefulset NAME` | the StatefulSet of the peers |
| `--slurm` | grow with spare Slurm jobs (inside a slurm-launch job) |
| `--sky LAUNCH_ARGS` | grow with one-node SkyPilot clusters: sky launch args, e.g. "--infra aws --use-spot" |
| `--base BASE` | with --sky: peers that are not spares |
| `--partition PARTITION` | Slurm partition for spare jobs |
| `--cluster CLUSTER` | a profile or chimaera://host:port (default: $CHIMAERA_ADDRESS) |
| `--namespace NAMESPACE` | Kubernetes namespace of the StatefulSet (no help text) |
| `--min MIN` | Fewest peers (no help text; default 1) |
| `--max MAX` | Most peers (no help text; default 8) |
| `--up-after SECONDS` | Grow after calls have been queued this long (no help text; default 30) |
| `--down-after SECONDS` | Shrink after the cluster has been mostly idle this long (no help text; default 300) |
| `--start-grace SECONDS` | No help text; default 300 |
| `--interval SECONDS` | No help text; default 5 |

[deploy.md](deploy.md#autoscaling-kubernetes) explains when it grows and
shrinks the pool.
