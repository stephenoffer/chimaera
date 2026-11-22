<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <img src="docs/assets/logo.svg" alt="Chimaera" width="112" height="112">
  </picture>
</p>

<h1 align="center">Chimaera</h1>

<p align="center">
  <strong>Distributed Python with one decorator.</strong><br>
  A Rust runtime with no head node that learns what your code needs.
</p>

<p align="center">
  <a href="docs/getting-started.md">Get started</a> ·
  <a href="docs/README.md">Guides</a> ·
  <a href="docs/guides/from-ray.md">Coming from Ray</a> ·
  <a href="docs/api.md">API</a> ·
  <a href="docs/qualification.md">Benchmarks</a>
</p>

---

```python
import chimaera as ch

@ch.remote
def word_count(text: str) -> int:
    return len(text.split())

@ch.remote
def add(left: int, right: int) -> int:
    return left + right

total = add.remote(word_count.remote("blue sky"), word_count.remote("rain today"))
print(total.result())                       # 4

for n in word_count.map(["a", "a b", "a b c"]):
    print(n)                                # 1, 2, 3
```

That's the API you need on day one. `add` starts when both counts are done,
and their results travel worker to worker without passing through your script.
On a laptop, the first `.remote()` starts a local peer for you. On a cluster,
the same file runs unchanged.

## What you get

### No head node

Every machine runs the same `chimaera` peer, and none of them is in charge.
Which calls exist, which finished and what they returned is replicated with
Raft, so there's no head process to size, babysit or lose. Kill a machine
mid-run and its calls run elsewhere. In a 64-machine test under a load of
one-second tasks, every task still returned exactly once, in 9.6 s. Ray took
18.5 s.

### It sizes your calls for you

No more guessing `num_cpus` or `memory`. After three calls the peer reserves
the memory peak it measured, so later calls wait for room instead of getting
OOM-killed, and a call killed for memory retries with 1.5x what it reached.
Tiny calls batch themselves. `future.explain()` shows what was reserved and
why, and a `cpus=` or `memory=` you set always wins.

### Failures you can reason about

A crashed worker's calls are retried; your own exceptions aren't, unless you
list them. A function that charges a card says `effects="external"` and
starts at most once. If its worker dies mid-call, you get
`ch.OutcomeUnknown`, not a second charge. Runs outlive your script, too:
reattach from another process with `ch.runs.attach(run_id)`.

### Fast where it counts

64 × c7i.4xlarge machines (1,024 vCPU), the same machine type for both
runtimes, median of at least 45 s of repetitions:

| Workload | Ray | Chimaera |
|---|---:|---:|
| Independent no-op tasks | 5,188/s | 14,203/s |
| Map over tiny items, no hand batching | 5,171/s | 774,908/s |
| 32 MiB array passed straight to 2,048 calls | 3.51 s | 0.71 s |
| Nested tasks (4 parents per CPU, 8 children each) | 1.29 s | 0.82 s |
| A machine killed under one-second tasks | 18.5 s | 9.6 s |

Ray's nested and machine-loss figures come from a separate run, because the
first one hit its time limit. Ray is still faster at shuffles (6x at 64
machines) and at calls on a value that was just made. All of it, with method
and caveats, is in [qualification.md](docs/qualification.md).

### Runs where your machines are

One binary covers a laptop, a few SSH hosts, Kubernetes and Slurm:
`chimaera up` over SSH, `chimaera k8s render --image IMG | kubectl apply -f -`,
or `chimaera slurm-launch` inside an allocation. Then
`chimaera run --cluster NAME -- python app.py` runs your script against it.
See [clusters](docs/guides/clusters.md) and [deploy.md](docs/deploy.md).

## Install

Chimaera isn't on PyPI yet. Build it from source with Rust (the toolchain in
`rust-toolchain.toml` installs itself through rustup), `protoc` and
Python 3.11 or newer:

```bash
git clone https://github.com/stephenoffer/chimaera && cd chimaera
cargo build --release -p chimaera-peer          # the `chimaera` binary
python -m venv .venv && . .venv/bin/activate
pip install maturin && maturin develop --release
```

For machines without Rust, `make wheel` builds a wheel with the peer binary
inside it (same OS and CPU architecture as the build machine).

## Status

Chimaera is early software. This is what works today:

| Area | State |
|---|---|
| Functions, futures as dependencies, `map`, actors, pools | Working, on one machine and across peers |
| Retries, `ch.OutcomeUnknown` for external effects, memory limits, canceling calls, deadlines | Working |
| Learned CPU and memory reservations, memory-aware admission, large values stored once | Working; across peers, placement weighs input locality and memory |
| Per-call measurements (`future.explain()`, `chimaera explain`) | Working; per-attempt cgroup readings on Linux |
| Reattaching to a run after the driver or the peer restarts (`ch.runs.attach`) | Working |
| Clusters: replicated shard groups, failover, dead voters replaced | Working on localhost clusters and on up to 64 cloud machines |
| Cluster tokens, mutual TLS, tenants with CPU quotas | Working on localhost clusters |
| Durable actors (`ch.durable.State`) and durable workflows | Working |
| Streaming generators (`ch.typing.Stream`) | Working |
| Gangs and collectives (`ch.dist.group`, `.gang()`) | Working on CPU; NCCL checked on one node of 4 NVIDIA A10Gs |
| Shuffles (`reduce_by_key`) | Working; Ray is faster today |
| Kubernetes, Slurm and SSH launchers | Working in container tests (kind, slurmd, sshd); real clouds unverified |

## Learn more

- [Getting started](docs/getting-started.md): install, a first program, a ten-minute tour
- [Guides](docs/README.md): tasks, actors, failures, resources and GPUs, data, gangs, clusters, observability
- [Coming from Ray](docs/guides/from-ray.md): the mapping, the differences that bite, and running Ray code as is
- [API reference](docs/api.md), [CLI](docs/cli.md), [configuration](docs/configuration.md)
- [Design](docs/design.md) and [architecture](docs/architecture.md), for how it works inside

## Development

```bash
make check            # fmt, clippy, Rust tests, Python tests
make linux-pytest     # the Python tests in a Linux container, with cgroup accounting
```

`crates/` holds the Rust runtime (`chimaera-core` is the replicated state
machine, `chimaera-raft` runs many Raft groups per peer, `chimaera-peer` is
the agent and CLI, `chimaera-py` the extension module), `python/chimaera`
the Python package, and `bench/` the comparisons against Ray.

## License

Apache-2.0
