# Chimaera

Chimaera runs Python functions and classes across many processes and machines.
You put `@ch.remote` on a function or a class, call `.remote()` to run it through
the runtime, and pass the futures it returns into other calls. The runtime is
written in Rust and has no head node: every peer runs the same agent, and the
state that matters (which calls exist, which finished, what they returned) is
replicated with Raft instead of living in one process.

```python
import chimaera as ch

@ch.remote
def word_count(text: str) -> int:
    return len(text.split())

@ch.remote
def add(left: int, right: int) -> int:
    return left + right

word_count("blue sky")                          # 2: an ordinary local call
total = add.remote(word_count.remote("blue sky"), word_count.remote("rain today"))
total.result()                                  # 4
```

`add` starts only after both `word_count` calls finish, and their results go
straight to it; the driver never fetches them.

## Status

This is early software. What works today and what does not:

| Area | State |
|---|---|
| Functions, futures as dependencies, `map`, actors, pools | Working, on one machine and across peers |
| Retries, `OutcomeUnknown` for external effects, memory limits, cancellation, deadlines | Working |
| Per-call measurements (`future.explain()`, `chimaera explain`) | Working; per-attempt cgroup readings on Linux |
| Restarting the peer mid-run and reattaching with `ch.runs.attach(run_id)` | Working |
| Several peers: replicated shard groups, placement across peers, objects fetched between peers, failover, dead voters replaced | Working on localhost clusters and on 4 cloud machines (Ray comparison in [qualification](docs/qualification.md)); per-call throughput across machines still trails Ray |
| Peer authentication: cluster token, mutual TLS (opt-in, `--tls-dir`) | Working on localhost clusters |
| Tenants: driver credentials bound to a tenant (`chimaera cluster credential`), per-tenant CPU quotas | Working on localhost clusters; a credential scopes the API and is not a sandbox (docs/deploy.md) |
| Operating a cluster: `chimaera status` (peers, groups, leaders), `runs`, `logs`, `cancel`, `inspect` with `--cluster`, Prometheus metrics | Working |
| Durable actors (`state=ch.durable.State()`): committed state, idempotent command ids, restart from the committed state | Working |
| Learned reservations, memory-aware admission, large values stored once ([details](#what-chimaera-does-for-you)) | Working on one machine; placement across peers weighs input locality and memory |
| Streaming generator calls (`ch.typing.Stream`, bounded ahead of the consumer, replayed after a worker dies) | Working |
| Durable workflows (`workflow="durable"`): replay answered from the run's record, divergence detected | Working |
| Gangs (`ch.dist.group`, `fn.options(ranks=n).gang()`): rendezvous env, collectives on small objects, device meshes, elastic size, restart as a new generation | Working on one machine and localhost clusters; GPU process groups are not tested here |
| Shuffles (reduce by key across partitions) | Not yet: `ch.data.partitions` splits data, nothing regroups it |

The design and the plan are in [docs/design.md](docs/design.md). What has been
measured so far is in [docs/qualification.md](docs/qualification.md).

## Install from source

You need Rust (the toolchain in `rust-toolchain.toml` installs itself through
rustup), `protoc`, and Python 3.11 or newer.

```bash
cargo build --release -p chimaera-peer          # the `chimaera` binary
python -m venv .venv && . .venv/bin/activate
pip install maturin && maturin develop --release
```

To install on machines without Rust, build a wheel once: `make wheel` puts
one in `dist/` with the peer binary inside, and `pip install dist/chimaera-*.whl`
gives `import chimaera` and the `chimaera` command (same OS and CPU
architecture as the build). `make wheel-smoke` checks it in a fresh venv.

The first `.remote()` call starts a local peer in the background
(`~/.chimaera/local`) if one is not running. It stops after ten idle minutes.

## Run a cluster

Make a cluster CA and token once, and a certificate per peer:

```bash
chimaera cluster init --out ca/                      # ca.pem, ca.key, token
chimaera cluster cert --ca ca/ --name host1 --out host1-tls/
```

Start one peer per machine. The founders are the addresses in `--seeds`; a
peer started later with the same seeds joins as a spare that runs work and
replaces a founder that dies.

```bash
chimaera --dir /var/lib/chimaera peer start \
    --listen 10.0.0.1:7700 --seeds 10.0.0.1:7700,10.0.0.2:7700,10.0.0.3:7700 \
    --token-file ca/token --tls-dir host1-tls/
```

Peers refuse calls without the token and connections from certificates
another CA signed. Give drivers a credential for their tenant rather than the
token (`chimaera cluster credential --tenant T`, see docs/deploy.md). `--insecure` skips the token; leaving out `--tls-dir`
sends peer traffic in plain text.

Point a program at the peer on its machine with `CHIMAERA_HOME=/var/lib/chimaera`.
Work spreads over every peer; any peer can attach to any run. A peer that
stops answering is declared dead after its suspicion timeout (at least six
seconds) and retired, and its work runs elsewhere.
`SIGTERM` drains a peer: it takes no new work, hands the groups it leads,
its voter seats and the values it stores to other peers, and stops once its
running calls finish (up to `CHIMAERA_DRAIN_SECONDS`, default 30).

Programs on a peer's machine reach it through a local socket only it and its
user can open; programs elsewhere connect to a peer's client port
(`--client-listen`) with the token or a driver credential.

A peer that dies takes the values it stored with it, except where they were
copied. On Linux each value is copied to one other peer about a second after
it is written. A call whose input was lost with every peer that held it runs
the call that made it again (a function or map item without external
effects, inputs first), so a pipeline goes on. Not remade: values your
program stored (`ch.put`, large arguments), actor results, and a result your
script reads directly; losing one of those fails the read.

## A short tour

### Futures

`.remote()` returns a `Future`. Call `.result(timeout=...)` or
`await` it. A wait that times out raises `TimeoutError` and leaves the call
running; `.cancel()` stops it and tells you whether cancellation won.

### Maps

`fn.map(items)` returns a stream of results, in order by default
(`ordered=False` gives completion order). Only a bounded number of items are in
flight at once, so a slow consumer slows the producer instead of filling
memory. Fast items are batched automatically; you do not size batches by hand.

```python
for n in word_count.map(["a", "a b", "a b c"]):
    print(n)
```

### Actors

The decorator works on a class too. Calls through one handle run
in the order you made them, and a method that raises leaves the actor usable.

```python
@ch.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self, amount: int) -> int:
        self.value += amount
        return self.value

counter = Counter.remote()
counter.increment.remote(add.remote(1, 2)).result()   # 3
```

`Counter.options(name="counter/main").remote()` gets or creates a named actor.
`Counter.options(replicas=4).pool()` makes independent replicas; `pool.increment.map(items)`
spreads calls across them.

### Options

Set them on the decorator, on one call with `.options(...)`, or
for a block with `with ch.defaults(...)`. The closest setting wins, and a typo
fails where you wrote it.

```python
@ch.remote(cpus=2, memory="1 GiB")
def heavy(x): ...

heavy.options(cpus=1).remote(5)
with ch.defaults(deadline="30s"):
    heavy.remote(6)
```

### Failures

A plain function runs up to three times (two retries) if its
worker or machine dies; your own exceptions are never retried unless you list
them in `retry_exceptions`. Errors come back with their original type and the
remote traceback attached. A function that touches the outside world can say
so:

```python
@ch.remote(effects="external")
def charge(card: str): ...
```

It then starts at most once. If its worker dies after it started, you get
`ch.OutcomeUnknown` instead of a silent second charge.

### Explaining a call

`future.explain()` shows its state, why it is
waiting, and for each attempt the CPU time, mean cores and peak memory, each
labeled with how and over what scope it was measured. A request that no
machine can satisfy fails at once with `ch.Infeasible` and the shape it asked
for.

## What Chimaera does for you

Code written the obvious way should not need tuning. Each row is a trap Ray
users hit and what
Chimaera does instead, with no option to set.

| Ray trap | What Chimaera does |
|---|---|
| The same large argument passed to many calls is copied into the object store every time | Arguments of 1 MiB or more are stored once by content: a 16 MiB array passed to 50 calls is written once |
| Arrays read from the object store are read-only (`assignment destination is read-only`) | Arrays arrive writable and zero-copy; a write stays in its own call |
| `num_cpus` and `memory` are guesses nobody checks; too many memory-heavy tasks start together and get OOM-killed | Each function's private memory peak is measured; after three calls later calls reserve it (scaled with input size when memory tracks input), and calls wait for memory instead of overcommitting it. A function not yet measured runs a few calls at a time (full width where the pool has 4 GiB free per call) until its first call finishes |
| OOM kills retry the same mistake | A call killed for memory is retried with 1.5x the memory it reached, and waits until that fits; if no peer's pool can fit it, it fails at once with `ch.OutOfMemory` |
| A task that mostly waits on I/O holds a whole core | After ten calls the runtime tries a quarter core on half of them and keeps it only if it is faster per core |
| A leaking worker grows until the node OOMs, unless you find `max_calls` | A worker whose memory between calls keeps rising is replaced, and the decision is recorded. On Linux, memory the allocator kept after frees is handed back first, so it is not mistaken for a leak |
| Library thread pools oversubscribe or stick at one thread | OpenMP, BLAS, Polars and Numba pools are sized to the call's CPU grant; core dumps are off; glibc arenas are capped on Linux |
| A module edited between runs is served stale by a warm worker | Runs whose source files differ never share workers |
| The driver and the cluster run different versions, and it shows up as odd failures | A program that would start a different (rebuilt or upgraded) peer binary than the one serving it warns once and says how to switch (CH111) |
| Submitting millions of calls in a loop fills memory | `.remote()` waits for room once unfinished calls hold half the peer's memory in arguments, and says so (CH110) |
| A task that cannot be scheduled hangs with the reason in a log on another machine | `.result()` prints why after a minute (needs memory, needs CPU, waiting for an executor) |
| An actor whose method waits on I/O runs its calls one at a time, and nothing says why they queue | A call queued at an actor says how many are ahead; when that method is learned to mostly wait, it suggests `@ch.method(concurrency=N)` |
| A full disk shows up as a bare `No space left on device` deep in a worker, leaving partial files | The error names the disk, its free space and what to do; the partial file is removed |
| `logging` inside tasks goes nowhere | Task log records reach the driver at the driver's log level |
| The object store's directory fills the disk with what finished jobs left behind | A run's stored arguments and `ch.put` values are deleted when the run ends; its results are kept for a day so `ch.runs.attach` can read them |
| In a container, the object store and memory monitor are sized from host memory, or from the wrong cgroup | The memory pool and CPU capacity come from the tightest limit on the process's cgroup and every cgroup above it |
| Everything learned is lost with the cluster | Profiles are saved by each peer, reloaded on restart, and shared between peers on the same hardware |

What was learned and why is in `chimaera explain RUN --decisions`; each
attempt's reservation and where it came from is in `future.explain()`.
Explicit `cpus=` and `memory=` always win, `ch.auto(min=, max=)` bounds what is
learned, and `adaptive="observe"` (records only) or `"frozen"` turn learning
off. Under host memory pressure on Linux the peer stops the attempt furthest
over its reservation (`bench/evals/gotchas`, `runaway` trap); that path is not
exercised by the test suite.

## The `chimaera` command

```bash
chimaera status                          # CPU in use, queue, why work is waiting, peers, groups and their leaders
chimaera runs                            # the cluster's runs, newest first (ids, state, command)
chimaera logs RUN [--follow]             # what a run's calls printed, from every peer
chimaera cancel RUN                      # cancel a run's unfinished calls
chimaera inspect RUN                     # a run's calls by state
chimaera explain RUN --task TASK         # one call: attempts, measurements, waiting reason
chimaera explain RUN --decisions         # what the runtime tuned, and why
chimaera profiles [--fn NAME]            # what was learned per function, and what its next call reserves
chimaera doctor                          # what this host supports
chimaera stop                            # stop the local peer
```

`status`, `runs`, `logs`, `cancel`, `inspect` and `explain` ask the local peer,
or a cluster with `--cluster NAME` (a profile in
`~/.config/chimaera/clusters.toml`) or `--cluster tcp://host:port`. Without
the flag they ask what a script here would use: `$CHIMAERA_CLUSTER`,
`$CHIMAERA_ADDRESS`, then the profile file's `default`. `--dir` or
`$CHIMAERA_HOME` always means that local peer. `chimaera run --cluster NAME --
python app.py` runs a program against a cluster.

## Repository layout

```
proto/                 wire and log schemas
crates/chimaera-core   the replicated shard state machine, ids, clocks, controllers (no IO)
crates/chimaera-raft   many Raft groups per peer on openraft and redb
crates/chimaera-peer   the peer: scheduling, executors, objects, accounting, CLI
crates/chimaera-py     the Python extension module
python/chimaera        the Python package
bench/evals            Ray vs Chimaera comparisons (one node, multi-node)
docs/                  design, Ray lessons, requirements, qualification
```

## Tests

```bash
make check            # fmt, clippy, Rust tests, Python tests
make linux-pytest     # the Python tests in a Linux container, with cgroup accounting
make proptest-deep    # 20,000 random histories through the shard state machine
```

More: [getting started](docs/getting-started.md) ·
[design](docs/design.md) · [lessons from Ray](docs/ray-lessons.md) · [Ray gotchas and Chimaera](docs/ray-gotchas.md) ·
[requirements](docs/requirements.md).

## License

Apache-2.0
