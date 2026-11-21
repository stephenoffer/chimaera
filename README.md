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
| Functions, futures as dependencies, `map`, actors, pools | Working, on one machine |
| Retries, `OutcomeUnknown` for external effects, memory limits, cancellation, deadlines | Working |
| Per-call measurements (`future.explain()`, `chimaera explain`) | Working; per-attempt cgroup readings on Linux |
| Restarting the peer mid-run and reattaching with `ch.attach(run_id)` | Working |
| Several peers: replicated shard groups, placement across peers, objects fetched between peers, failover, dead voters replaced | Working on localhost clusters; not yet run across real machines |
| Peer authentication (tokens, mTLS) | Not yet: run clusters only on a trusted network |
| Streaming generators, durable actors and workflows, GPU gangs, shuffles | Not yet |

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

The first `.remote()` call starts a local peer in the background
(`~/.chimaera/local`) if one is not running. It stops after ten idle minutes.

## Run a cluster

Start one peer per machine. The founders are the addresses in `--seeds`; a
peer started later with the same seeds joins as a spare that runs work and
replaces a founder that dies.

```bash
chimaera --dir /var/lib/chimaera peer start \
    --listen 10.0.0.1:7100 --seeds 10.0.0.1:7100,10.0.0.2:7100,10.0.0.3:7100
```

Point a program at the peer on its machine with `CHIMAERA_HOME=/var/lib/chimaera`.
Work spreads over every peer; any peer can attach to any run. A peer that
stops answering for three seconds is retired and its work runs elsewhere.
`SIGTERM` drains a peer: it takes no new work and stops once its running
calls finish (up to `CHIMAERA_DRAIN_SECONDS`, default 30).

Peers do not authenticate each other yet, so keep the listen port on a
trusted network. Results stored on a peer that dies are lost with it; their
tasks are not re-run to rebuild them.

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

`Counter.options(key="counter/main").remote()` gets or creates a named actor.
`Counter.options(replicas=4).pool()` makes independent replicas; `pool.increment.map(items)`
spreads calls across them.

### Options

Set them on the decorator, on one call with `.options(...)`, or
for a block with `with ch.options(...)`. The closest setting wins, and a typo
fails where you wrote it.

```python
@ch.remote(cpus=2, memory="1 GiB")
def heavy(x): ...

heavy.options(cpus=1).remote(5)
with ch.options(deadline="30s"):
    heavy.remote(6)
```

### Failures

A plain function is retried up to three times if its
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

## The `chimaera` command

```bash
chimaera status                          # CPU in use, queue, why work is waiting
chimaera inspect RUN                     # a run's calls by state
chimaera explain RUN --task TASK         # one call: attempts, measurements, waiting reason
chimaera explain RUN --decisions         # what the runtime tuned, and why
chimaera doctor                          # what this host supports
chimaera stop                            # stop the local peer
```

## Repository layout

```
proto/                 wire and log schemas
crates/chimaera-core   the replicated shard state machine, ids, clocks, controllers (no IO)
crates/chimaera-raft   many Raft groups per peer on openraft and redb
crates/chimaera-peer   the peer: scheduling, executors, objects, accounting, CLI
crates/chimaera-py     the Python extension module
python/chimaera        the Python package
bench/evals            Ray vs Chimaera single-node comparison
docs/                  design, Ray lessons, requirements, qualification
```

## Tests

```bash
make check            # fmt, clippy, Rust tests, Python tests
make linux-pytest     # the Python tests in a Linux container, with cgroup accounting
make proptest-deep    # 20,000 random histories through the shard state machine
```

More: [getting started](docs/getting-started.md) ·
[design](docs/design.md) · [lessons from Ray](docs/ray-lessons.md) ·
[requirements](docs/requirements.md).

## License

Apache-2.0
