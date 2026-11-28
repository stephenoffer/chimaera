# Environment variables

> *The knob you need should exist. The knob you do not need should not have been invented.*

Every `CHIMAERA_*` environment variable the code reads: who reads it, its
default and what it does. Most programs need none of them. Names and defaults
come from the code itself, in the Python package (`python/chimaera/`) and the
Rust crates (`crates/`).

"Read by" names the process that looks at the variable:

- driver: your script, when it connects (`ch.init` or its first call)
- CLI: a `chimaera` command such as `status` or `logs`
- peer: `chimaera peer start`, including the local peer a script starts
- worker: the Python process that runs calls

To tune a local peer, set the variable in your script's environment before
the peer starts. It inherits only `PATH`, `HOME`, `USER`, `LOGNAME`,
`SHELL`, `TMPDIR`, `TZ`, `LANG`, `LC_*` and every `CHIMAERA_*` variable from
the script. Once the peer runs, stop it (`chimaera stop`) for a change to
apply.

Workers see their peer's environment plus what the run ships
([guides/clusters.md](guides/clusters.md#get-your-code-and-settings-to-the-workers)).

## Connecting to a cluster

| Variable | Read by | Default | What it does |
|---|---|---|---|
| `CHIMAERA_CLUSTER` | driver, CLI | unset | The cluster to use: a profile name from the profile file, `tcp://host:port`, or `local`. `ch.init(cluster=...)` and the CLI's `--cluster` take precedence. `chimaera run --cluster` sets it for the program |
| `CHIMAERA_ADDRESS` | driver, CLI | unset | Client addresses to connect to when `CHIMAERA_CLUSTER` is unset: one or more `host:port`, comma-separated, tried in order. Pair it with `CHIMAERA_TOKEN` or `CHIMAERA_TOKEN_FILE`, and `CHIMAERA_CA` |
| `CHIMAERA_TOKEN` | driver, CLI | unset | The cluster token or a driver credential, as text. Overrides a profile's token. Removed from workers' environment |
| `CHIMAERA_TOKEN_FILE` | driver, CLI | unset | A file holding the token or credential; used when `CHIMAERA_TOKEN` is unset. Removed from workers' environment |
| `CHIMAERA_CA` | driver, CLI | unset | The cluster CA (`ca.pem`) for peers that use TLS, with `CHIMAERA_ADDRESS` or a `tcp://` address. A profile uses `~/.chimaera/clusters/NAME/ca.pem` or its `ca` key |
| `CHIMAERA_CLUSTERS` | driver, CLI | `~/.config/chimaera/clusters.toml` | Path of the profile file. `chimaera up` and `down` read it too |
| `CHIMAERA_TENANT` | driver, worker | unset | The tenant that owns the script's run, when `ch.init(tenant=...)` is not given. Without either: the driver credential's tenant, else `default`. Each run ships its tenant to its workers in this variable |

CLI commands use these only when neither `--dir` nor `CHIMAERA_HOME` is set,
or when you pass `--cluster` ([cli.md](cli.md#which-peer-a-command-asks)).

## The local peer

| Variable | Read by | Default | What it does |
|---|---|---|---|
| `CHIMAERA_HOME` | driver, CLI, peer | `~/.chimaera/local` | Data directory of the local peer: its socket, log, Raft state, objects and profiles. With a remote cluster the driver keeps its cache of fetched values under it. When set, CLI commands go to that local peer unless given `--cluster` |
| `CHIMAERA_BIN` | driver | unset | The `chimaera` binary to start a local peer from. Otherwise: the one installed with the package, a source checkout's `target/release` or `target/debug` build, then `PATH` |
| `CHIMAERA_DETACH` | driver | unset (`0`) | `1` makes the run detached, as `ch.init(detach=True)` does. `chimaera run` sets it to `1` with `--detach`, else `0`. Ignored when the script calls `ch.init()` itself, whose `detach` defaults to `False` |

## Your script's run

| Variable | Read by | Default | What it does |
|---|---|---|---|
| `CHIMAERA_ENV_PASSTHROUGH` | driver | unset | More variables for the run to ship to its workers, on top of `LANG`, `LC_ALL`, `LC_CTYPE`, `TZ` and `PYTHONHASHSEED`: comma-separated names or `fnmatch` patterns (`APP_*`). Names starting with `CHIMAERA_` never match. Don't list secrets: the run's variables are stored with the run |
| `CHIMAERA_PROJECT_MAX_BYTES` | driver | `104857600` (100 MiB) | Largest project archive a remote run ships. Past it, `ch.init` fails and names the largest files |
| `CHIMAERA_PROJECT_INCLUDE` | driver | unset | Files or directories a remote run ships even though git ignores them, separated by `:` (`;` on Windows), relative to the working directory. A directory git ignores as a whole otherwise ships nothing (warning CH126) |
| `CHIMAERA_PROJECT_GITIGNORE` | driver | `1` | `0` ships every file of the project's directories, ignored by git or not |
| `CHIMAERA_MAX_PENDING_BYTES` | driver | unset: half the peer's memory, at least 256 MiB | Bytes of unfinished calls' arguments the driver may hold before `.remote()` waits for room (warning CH110). It also waits at 1,000,000 unfinished calls |
| `CHIMAERA_SPREAD_RATE` | driver | `2000` | Calls per second a driver must submit before its calls spread over several Raft groups. Below it they stay in the run's own group |
| `CHIMAERA_STALL_NOTICE_S` | driver, worker | `60` | Seconds a wait on a call that has not started may last before the reason is printed to stderr, once |
| `CHIMAERA_DIRECT` | driver | `1` | `0` sends every actor call through the peer. By default a driver's call to an actor on its local peer goes straight to the actor's worker, which answers it, and the peer records the call just after: about half the round trip. That holds for an actor that runs one call at a time and for one with `concurrency` or groups, whose worker runs the call in its lanes; one process has at most 16,384 calls in flight this way to one such actor, and its further calls go through the peer until some finish. So does a call made inside a call on this machine, unless it is to the caller's own actor. Calls to durable actors or with a deadline, retries or a generator always go through the peer. So do calls that can't safely run again: `effects="external"`, or `deterministic=False` (a second run may give another answer), including those of actors and functions placed `within=` a reservation and of gang ranks. A call sent straight to the worker runs before the peer has a record of it, so a retry of its caller can run it again. As for every option, the call's own setting wins over its method's, and its method's over its actor's. A handle passed to a call keeps its actor's `effects` and `deterministic`. One found with `ch.actor(name)` doesn't know them, so an actor made with `deterministic=False` takes all of its calls through the peer, even one that says `deterministic=True`. If the connection to the worker breaks, a call that began never runs again: the worker sends what became of each call, and one that hadn't begun goes through the peer, still in order (or ends `ch.Cancelled` if you cancelled it). If the worker died, a call that had begun raises `ch.ActorDied`, as through the peer. If the worker is alive but can't send back what became of a call, the call raises `ch.OutcomeUnknown`. Once the worker's instance may no longer be the actor's, calls still waiting at that worker never begin there: they, and the process's later calls to the actor, go through the peer, which runs them on the actor's current instance, in order, or fails them with `ch.ActorDied`. That is once the actor was closed from any process, the worker's peer died, or the peer went 3 seconds without hearing from most of its cluster (paused, stalled or cut off), sooner than the 6 it takes for a peer to be declared dead and its actors started again elsewhere. A peer that finds its actor closed or started again elsewhere stops its old worker. While 16,384 of a process's calls to one concurrent actor are in flight this way, its next calls go through the peer |
| `CHIMAERA_NATIVE_SCATTER` | worker | `1` | `0` makes `reduce_by_key`'s scatter calls split pairs with the Python loop instead of the native one, which makes the same buckets. For comparing the two; set it on the peers |

## Resources and memory

> *A setting you change is respected. A setting you do not change is measured.*

| Variable | Read by | Default | What it does |
|---|---|---|---|
| `CHIMAERA_MEMORY_POOL` | peer | usable memory minus the peer's reserve | Memory the peer grants to calls and, when objects live in shared memory, to objects. A size such as `8GiB`, `512MiB` or `16GB`; anything else stops the peer from starting. It also caps the bytes of objects kept in memory (otherwise 90% of the object filesystem) |
| `CHIMAERA_TENANT_QUOTAS` | peer | unset | CPUs each tenant's running calls may hold on this peer: `team-a=8,team-b=2.5`. A malformed value stops the peer. See [deploy.md](deploy.md#tenants-and-quotas) |
| `CHIMAERA_TENANT_SHARES` | peer | unset | CPUs each tenant may hold across the cluster, same format. Set it on every peer |
| `CHIMAERA_DEVICE_ROOT` | peer | `/` | Directory the peer reads its accelerators from, as if it were `/`: a captured host tree such as `tests/fixtures/topology/dgx-h100` gives a peer eight H100s to test GPU placement without them |
| `CHIMAERA_LABELS` | peer | unset | Labels for placement, `zone=us-east1-b,rack=r7`, on top of `node`, `host`, `os` and `arch` |
| `CHIMAERA_PYTHON` | peer, CLI | `python3` | Fallback interpreter for workers: the peer starts them with the script's own interpreter when that path exists on this machine, else with this one. `chimaera submit` runs `-m chimaera._submit` with it |
| `CHIMAERA_HOST_WHILE_WAITING` | peer | `1` | `0` gives every call that waits on calls it started a worker process of its own. By default, while every call in a worker waits, the peer may run another call of the same run there, on a thread of its own, so nested waits need about one process per CPU ([guides/tasks.md](guides/tasks.md#wait-for-a-result)) |
| `CHIMAERA_NUMA_BIND` | peer | `1` | `0` leaves workers unbound. On Linux with more than one NUMA node, each new worker whose thread count fits one node runs on that node's CPUs and prefers its memory: the node of its GPUs, otherwise the node with the most CPUs not taken by workers already there. A worker the kernel will not bind runs unbound |
| `CHIMAERA_ENV_BUILD_TIMEOUT` | worker | `900` | Seconds a peer may spend installing a project's `uv.lock` packages before the call that needed them fails. Set it on the peers |

## Objects and spilling

[objects.md](objects.md) and [guides/data.md](guides/data.md) explain the
object store these tune.

| Variable | Read by | Default | What it does |
|---|---|---|---|
| `CHIMAERA_OBJECT_DIR` | peer | unset | Directory under which the peer keeps its object store. Otherwise: a huge-page tmpfs if one is mounted, else `/dev/shm` when it has room (Linux), else `objects/` in the data directory |
| `CHIMAERA_SPILL_DIR` | peer | `spill/` in the data directory | Where objects in memory move under pressure: directories separated by `:`, used in turn (local disks or a network mount). Used only when the store is in memory and the directory is on another filesystem |
| `CHIMAERA_SPILL_URL` | peer | unset | Object storage for spilled objects: `s3://bucket/prefix`, `gs://...`, `az://...` or `file:///path`. Needs a peer built with the `object-store-spill` feature. Without it, or if the store can't be opened, the peer logs an error and spills to local disk. Used only when the store is in memory |
| `CHIMAERA_SPILL_THRESHOLD` | peer | `0.8` | Share of the memory pool past which objects in memory move to disk. Values outside 0.05 to 1.0 are ignored |
| `CHIMAERA_SPILL_THREADS` | peer | `4` | Objects copied to disk at once while spilling |
| `CHIMAERA_OBJECT_REPLICAS` | peer | `1` | Copies of each new object kept on the next peers, so losing one peer loses no data. `0` turns copies off. Linux only |
| `CHIMAERA_ARENA` | driver, worker | `1` | `0` turns off the shared-memory arena that NumPy arrays and large values are written into. The arena works only on Linux, with the store in memory |
| `CHIMAERA_CUDA_IPC` | driver, worker | `1` | `0` stops `ch.put` of a CUDA tensor from sharing it with readers on the same machine through CUDA IPC (Linux only). Readers then use the host copy |

## Logging and metrics

| Variable | Read by | Default | What it does |
|---|---|---|---|
| `CHIMAERA_LOG` | peer, CLI | `chimaera=info,chimaera_peer=info,warn` | The peer's log filter, in the syntax of the `tracing` crate's `EnvFilter` (like `RUST_LOG`). The log goes to stderr; a local peer's goes to `peer.log` in its data directory |
| `CHIMAERA_LOG_FORMAT` | driver | `text` | How the driver prints its calls' output: `text` is `(function pid=N) line`; `json` is one object per line with `time`, `task`, `function`, `pid`, `stream` and `line`, for log collectors. Any other value fails `ch.init` |
| `CHIMAERA_DEDUP_LOGS` | peer | `1` | `0` prints every worker's copy of a line. Otherwise a line several of a run's workers print within 5 s reaches the driver once, then as `<line> [repeated Nx across workers]`; `chimaera logs` keeps every copy |
| `CHIMAERA_METRICS_LISTEN` | peer | unset (off) | Serve Prometheus metrics at `GET /metrics` on this address, such as `0.0.0.0:9700`. No token is asked, so bind it privately. A bad address or a port in use stops the peer from starting. See [guides/observability.md](guides/observability.md#scrape-metrics-with-prometheus) |

## Cluster peers

| Variable | Read by | Default | What it does |
|---|---|---|---|
| `CHIMAERA_CLUSTER_TOKEN` | peer, CLI, provisioner | unset | The cluster token, when `peer start` or `cluster credential` has no `--token-file`. `slurm-launch` shares it with its peers (else it makes a token for the job). `python -m chimaera.provisioner --slurm` or `--sky` requires it. Removed from workers' environment |
| `CHIMAERA_CANCEL_INTERRUPT_SECONDS` | peer | `2` | How long a cancelled call has to stop by itself before its worker interrupts it, raising `ch.Cancelled` in its main thread. A value that is not a positive number stops the peer from starting |
| `CHIMAERA_CANCEL_KILL_SECONDS` | peer | `5` | How long after its cancel a call that still runs loses its worker process (never an actor's). Must be longer than the interrupt, or the peer does not start |
| `CHIMAERA_DRAIN_SECONDS` | peer | `30` | How long SIGTERM or SIGUSR1 (or a preemption notice) waits for queued and running calls before the peer stops |
| `CHIMAERA_PREEMPTION_NOTICES` | peer | unset (off) | `gce`, `aws` or `azure`: poll the VM's metadata server every 5 s and drain when the VM is about to be preempted or stopped. A bad value stops the peer from starting |
| `CHIMAERA_RAFT_SYNC` | peer | `fsync` | How cluster peers write Raft logs. `fsync` syncs every commit to disk before acknowledging it. `durable` does too, except an attached run's per-call commits: its calls' submissions, grants and results, unless the call has `effects="external"`, is a durable actor's call, a durable workflow or one of its steps, is a gang rank or reservation host, or returns a `retained` value. Those are acknowledged once a majority of the group's voters hold them in memory (`replicated-memory`): they survive the peers' processes dying and the restart or loss of machines holding fewer than half of a group's voters, but the last of them are lost if machines holding a majority of a group's voters restart together. Everything else is kept as with `fsync`: detached runs, durable actors and workflows, `external` calls, `retained` results, coordination, leases, quotas and membership. `full` also flushes the drive cache; `none` keeps every commit in the page cache only (a commit survives the peers' processes dying, not their machines). A `durable` or `none` peer whose machine restarted without a clean stop rejoins as a new member while the other voters still lead its groups. A slow disk never changes the setting: commits batch more and `chimaera status` reports degraded service. `chimaera status` shows the durability asked for and the one each group's voters give, with an attached run's calls (`calls`) next to everything else. Voter replacements and moves skip peers that sync less than the leader asks; a `durable` peer may take any seat |
| `CHIMAERA_RAFT_MAX_COMMIT_DELAY_US` | peer | `2000` | The most, in microseconds, a cluster peer holds a Raft log commit open for more entries to share its fsync. It holds one only while each of its last 8 commits took a millisecond or more and entries keep arriving together, for a quarter of the fastest of them; `0` commits whatever is queued at once |
| `CHIMAERA_RAFT_SNAPSHOT_BYTES` | peer | `67108864` (64 MiB) | Minimum Raft log, in bytes, between snapshots |
| `CHIMAERA_MESH_RATE` | peer | unset (unpaced) | Bytes per second (`120MB`) or bits (`1gbit`) the peer sends other peers object bytes and Raft snapshots at, set a little under its link's capacity. Up to it, the traffic classes share by weight while more than one wants more: a call's or driver's read 8, a shuffle's gather 4, recovery (a drain's hand-off, a snapshot for a lagging replica) 2, checkpoint copies (background replicas, retained puts) 1; a class alone takes all of it. Raft messages are never held but count against it, so heartbeats go round quickly. `chimaera status` shows bytes and waits per class and heartbeat round trips against their 100 ms budget |
| `CHIMAERA_REBALANCE_SECONDS` | peer | `60` | How long membership must be steady before voters and leaders move to spread the groups |
| `CHIMAERA_RUN_SPREAD` | peer | `16` | How many vshards, each in another group, a run's calls may spread over. Clamped to 1 to 64; `1` keeps every run in one |
| `CHIMAERA_MACHINE_ID` | peer | the machine's boot id | Identity of the machine. Peers with the same id take objects straight from each other's stores. Tests set it so peers on one machine act as separate machines |
| `CHIMAERA_PORT` | peer | `7700`; under Slurm `7700 + SLURM_JOB_ID % 1000` | Peer port under the `k8s`, `slurm`, `hpc` and `sky` providers. `slurm-launch --port` sets it |
| `CHIMAERA_CLIENT_PORT` | peer | peer port + 10 | Client port under the `k8s`, `slurm`, `hpc` and `sky` providers, when `--client-port` and `--client-listen` are not given. `slurm-launch` sets it to the peer port + 1000 unless it is set |
| `CHIMAERA_K8S_SERVICE` | peer | none (required under `k8s`) | Headless Service the StatefulSet's pods are reached through. `chimaera k8s render` sets it |
| `CHIMAERA_K8S_NAMESPACE` | peer | `POD_NAMESPACE` | Namespace of the pods under `k8s`; one of the two must be set |
| `CHIMAERA_FOUNDERS` | peer | `3` | Under `k8s`, pods `0` to `N-1` found the cluster |
| `CHIMAERA_POOL` | peer | the StatefulSet's name | Under `k8s`, the pool name the peer logs at start |
| `CHIMAERA_FLEET` | peer | unset | Other clusters (cells) this one may send work to when it is full: `name=host:port`, comma-separated. See [federation.md](federation.md) |
| `CHIMAERA_FLEET_TOKEN_FILE` | peer | none (required with a fleet) | Token the other cells accept |
| `CHIMAERA_FLEET_DIRECTORY` | peer | unset | A directory cell that lists the fleet's cells |
| `CHIMAERA_CELL` | peer | none (required with a fleet) | This cell's name, as the other cells list it. `name@host:port` also lists it in the directory at that client endpoint |

## Variables the runtime sets

The peer and the driver set these for workers and for programs that
`chimaera run` or `submit` starts. Read the public ones inside a call if you
need them. Never set them yourself.

| Variable | Set by, for | What it holds |
|---|---|---|
| `CHIMAERA_DEVICES` | peer, for each worker | JSON list of the call's granted devices; `ch.dist.devices()` reads it. Public |
| `CHIMAERA_GANG`, `CHIMAERA_GANG_GENERATION` | Chimaera, for each gang rank | The gang's id and its generation, next to `RANK`, `WORLD_SIZE` and `MASTER_ADDR`. Public; see [guides/gangs.md](guides/gangs.md) |
| `CHIMAERA_TENANT` | driver, for the run's workers | The run's tenant; calls use it as their default tenant |
| `CHIMAERA_EXECUTOR` | peer, for each worker | `1`: this process is a worker. Internal |
| `CHIMAERA_PEER_SOCKET` | peer, for each worker | The peer's socket; a worker, and a program it starts, connect there instead of starting a peer. Internal |
| `CHIMAERA_PROJECT` | driver, for the run's workers | Digest of the shipped project archive. Internal |
| `CHIMAERA_CODE_FINGERPRINT` | driver, for the run's workers | Digest of the script's own source files; only runs with the same digest share warm workers. Internal |
| `CHIMAERA_DRIVER_PYTHON` | driver, for the run's workers | The script's Python version, so a worker can explain CH115. Internal |
| `CHIMAERA_LOG_LEVEL` | driver, for the run's workers | The script's logging level; workers without logging handlers log at it. Internal |
| `CHIMAERA_SHIP_PROJECT` | `chimaera submit`, for the program | `1`: ship the project even to the local peer, since calls may run elsewhere. Internal |

## Test only

| Variable | Read by | What it does |
|---|---|---|
| `CHIMAERA_TEST_CRASH_DIR`, `CHIMAERA_TEST_CRASH_MODE` | `chimaera-raft` tests | Drive the crash-recovery test's child process. Not read outside the test suite |
| `CHIMAERA_TOPOLOGY_ROOT` | peer | A directory holding another machine's `/proc/self/status` and `/sys/devices/system/{cpu,node}` files at those paths: the peer takes that machine's CPUs, cores, caches and NUMA nodes as its own. Tests and qualification runs use it to emulate a host |
