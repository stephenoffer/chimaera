# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Chimaera runs Python functions and classes across processes and machines (`@ch.remote` plus futures) on a Rust
runtime with no head node. Every peer runs the same agent, and the state that matters is replicated with Raft in
shard groups. README.md has the status table and a tour; docs/architecture.md (D1–D9) and docs/design.md have the
design.

## Commands

```bash
make check                                   # the gate: fmt-check, clippy -D warnings, cargo test, pytest
cargo test -p chimaera-core invariants_hold  # one Rust test, by name filter
make proptest-deep                           # 20,000 shard state-machine histories (default run: 512)

cargo build -p chimaera-peer                          # the peer binary the Python tests start
VIRTUAL_ENV=$PWD/.venv .venv/bin/maturin develop -q   # rebuild chimaera._native (only after crates/chimaera-py or proto changes)
.venv/bin/python -m pytest python/tests/test_values.py -q -k released   # one Python test
.venv/bin/python -m pytest python/tests -q -m "not slow"                # skip the minute-plus tests

make linux-check      # fmt, clippy, cargo test in the deploy/dev Docker image
make linux-pytest     # Python suite on Linux with cgroup accounting
make kind-test        # launcher gates (also slurm-test, ssh-test): real clusters in Docker
make wheel-smoke      # wheel with the peer binary inside, installed into a fresh venv
```

- Python tests start their own peer from `$CHIMAERA_BIN`, which defaults to `target/debug/chimaera`
  (python/tests/conftest.py). Each pytest session gets its own peer home in `/tmp/cht*`.
- Every test has a 300 s timeout. A stuck wait fails with a traceback instead of hanging.
- Some docs are tests. test_docs.py runs every ```` ```python run ```` block in docs/api.md, deploy.md,
  compat-ray.md, getting-started.md and docs/guides/*.md. test_readme_status.py checks that the `ch.` names in README's status table resolve. test_typing.py
  runs mypy, with the `chimaera.mypy` plugin, on python/tests/typing/api_types.py. Update these when the public API
  changes.
- Formatting: rustfmt with max_width 120, toolchain pinned in rust-toolchain.toml (1.94.1).

## Architecture

- **proto/chimaera/v1**: every message that crosses a process boundary or is stored in a Raft log. Peers of adjacent
  versions must decode each other's data, so add fields; never renumber or reuse them.
- **chimaera-core**: pure, deterministic logic: the shard state machine (src/shard), ids, HLC clock, SWIM membership,
  resource estimators, device and provider topology. It does no IO, reads no clock and draws no randomness, so every
  replica and the simulator agree. Anything that needs time or IO belongs in the peer.
- **chimaera-raft**: many shard groups per peer, on one log database and one state-machine database (redb) with group
  commit. Everything else uses `Group::propose` and `Group::read`; openraft stays internal to this crate.
- **chimaera-peer**: the `chimaera` binary (agent and CLI). `ingress.rs` is the client socket and submissions,
  `core.rs` scheduling and dispatch, `executor.rs` the supervised Python executors, `mesh.rs` peer-to-peer RPC and
  object fetch, `stores.rs`/`objplane.rs` objects and the shared-memory arena, `budget.rs`/`accounting.rs` memory
  admission and cgroup readings, `launch/` the k8s, Slurm and SSH launchers.
- **chimaera-py**: `chimaera._native`. `Client` talks to the local peer over its Unix socket and batches
  submissions; `ExecConn` is an executor's link to its peer. Rust owns every wire format; Python passes plain values.
- **python/chimaera**: the package. `_remote.py` (the decorator and options), `_future.py`, `_runtime.py` (finds or
  starts the local peer), `_executor.py` (the executor process), `_serde.py`/`_values.py` (serialization,
  content-addressed blobs, arena links), `compat/ray` (a Ray Core shim). The public submodules are coord, dist, data,
  runs, durable, errors and typing.
- **chimaera-fleet**: an emulator that runs whole cells of real code to size fleets of up to 262,144 peers
  (bench/scale).

A call goes from `.remote()` through `_native.Client` to the peer's ingress, through its shard group's Raft log, to
an executor, and its result is published back through the log. docs/architecture.md §3 describes today's per-call
commits and the journal (D2) that takes them off the hot path. The first `.remote()` outside tests starts a local
peer in `~/.chimaera/local`, which stops after ten idle minutes.
