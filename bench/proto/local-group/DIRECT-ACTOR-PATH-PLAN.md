# Build plan: the direct actor path

Read [DIRECT-ACTOR-PATH.md](DIRECT-ACTOR-PATH.md) first: it is the design and the measured floor. This is the
plan for building it.

## Where things stand (2026-11-22)

**Already in the tree** (`bench/proto/local-group/RESULTS.md` and `INLINE-APPLY.md` have the evidence):
- A local peer's group applies commands without a Raft log (`crates/chimaera-raft/src/local.rs`).
- An attached run's per-call commands go to an in-memory overlay, written durably within 500 ms. They are applied
  in the proposing task when the applier thread is idle.
- The caller of an attached run's call gets its outcome before the publication and before the peer's accounting
  (`core.rs` `on_exec_done`, `notify_early`).

**Measured on the live tree,** release build, interleaved with Ray 2.60, load 14–21:

| p50 round trip | Chimaera | Ray |
|---|---:|---:|
| Task call | 192.5 µs | 378 µs |
| Actor call | 190.5 µs | 145 µs |

**The direct hop's floor**, with Chimaera's real executor handoff: 15–22 µs.

**Target:** local actor calls at or under Ray's, about 70–90 µs expected.

## What it touches

| Area | Files |
|---|---|
| Native client | `crates/chimaera-py/src/lib.rs` |
| Driver futures and handles | `python/chimaera/_future.py`, `_remote.py`, `_runtime.py` |
| Executor | `python/chimaera/_executor.py` |
| Ingress, core, group | `crates/chimaera-peer/src/{ingress,core}.rs`, `crates/chimaera-raft` |
| Protocol | `proto/chimaera/v1/{executor,client}.proto` |
| Parity and design docs, benchmarks | `docs/parity.md`, `docs/architecture.md`, `bench/` |

## Steps

Each step is built and tested in a scratch copy of the tree first. It is then applied behind the peer setting
`direct_actor_calls` (off by default), so the tree stays green between steps.

### 1. Executor: direct socket and journal batches (`_executor.py`, `executor.proto`)

**Direct socket.** An actor executor (a process actor with concurrency 1 in version 1) listens on
`<peer dir>/direct/<executor id>.sock`, mode 0600.
- It accepts `DirectCall` frames carrying the call key, the handle's sequence number, the method, and args (inline
  or an object reference).
- The reader thread hands each call to the main thread through the existing inbox. Keep the handoff: cooperative
  cancellation depends on the reader staying free.
- The main thread replies with `DirectDone`.

**Journal batches.** Every 1 ms or 64 calls, the executor sends the peer one record per call over its existing
connection: the call key, spec digest, start and end times, rusage delta, and outcome summary or inline value.
- This needs a new `ExecToPeer` variant.
- Records go out after the reply, never on the call's path.

**Tests:** per-handle order across concurrent handles; the journal batch carries every call; a malformed frame
closes only that connection.

### 2. Peer: journal apply, socket in the placement event (`core.rs`, `ingress.rs`, `client.proto`, `local.rs`)

**Journal apply.** The core turns each journal record into the overlay commands today's path would have made:
submit with the grant to this peer, then publish. Use the same call key, so `explain`, `inspect`, `chimaera runs`,
accounting, learning and released-value tracking all see direct calls, about 1–2 ms late.

**Placement event.** When an actor is placed on this peer, the session event gains the direct socket path and a
per-actor token. The token is issued only to sessions whose tenant owns the actor.

**Tests:**
- `explain()` on a direct call shows its attempt within a few milliseconds.
- A session from another tenant gets no socket path.

### 3. Native client: direct connections, futures, fallback (`lib.rs`, `_future.py`, `_remote.py`)

**Connections.** Open the direct connection on a handle's first method call, when the actor is local and eligible.

**Eligible calls:** not a durable actor; not an `effects="external"` method; not in a detached run; not a generator
method; no deadline.

**Futures.**
- A client-owned socket-reader thread resolves the futures. It must never block on Python.
- Keep the dispatcher thread out of this path.

**The hard case: a direct call's future passed as an argument to another call.** Two options:
- (a) The client sends the value inline if the future is already resolved. Otherwise it waits for resolution
  before submitting the dependent call.
- (b) The client waits until the call's journal record is applied.

Start with (a). It keeps the shard's view consistent because the dependent carries the value, not a reference to a
task the shard has not seen yet.

**Fallback.** When the connection breaks (executor death, actor restart or migration), the client resubmits every
unanswered call through the peer with the same call keys. The peer's dedupe stops a call that finished from running
twice. Only `effects` none or idempotent calls take this path, so a rerun is allowed.

**Cancellation.** Send a direct cancel frame and the normal peer cancel. The peer escalates to SIGUSR2 at 2 s and a
kill at 5 s, as today.

**Tests:**
- Kill the executor in the middle of a burst: every call completes exactly once.
- A direct future passed into another call gives the right result.
- Durable actors, external effects and detached runs never use the direct path (test-only counter).
- Cancel works for running and queued direct calls.

## Gates before turning `direct_actor_calls` on by default

- `cargo test -p chimaera-raft -p chimaera-peer --lib` and `cargo clippy ... -D warnings` are clean.
- The full Python suite on the live tree passes, with failures compared against a baseline built from the same
  snapshot.
- Ray's actor microbenchmark rows (`bench/microbench`: sync, async, concurrent, 1:n, n:n) are at or above Ray's,
  interleaved with Ray on the same machine.
- Round trips use `rtt.py`, which is easy to rebuild: 3,000 sync calls after 300 warm-up, p50 and p99.

## Testing notes

- **A/B builds:** build both sides from one snapshot, at the same time, so a stale baseline can't look like a
  regression.
- **Binaries on macOS:** never `cp` over an existing binary in place. A process started from it can hang
  unkillably. Run `rm -f` first.
- **Crash tests:** don't use `abort()` in test children. macOS's crash reporter can stall and leave them stuck
  exiting for hours. Use `std::process::exit` with a distinctive code and a parent-side timeout.
- **Load:** use paired, alternating rounds, and wait for a quiet machine before timing.
- **Race tests:** a race window of a microsecond needs a test-only delay hook, or the test cannot fail; see
  `events_keep_proposal_order_across_queued_and_inline_commands` in `local.rs`.

## Status 2026-11-22: applied, on by default

It was built and tested in a separate copy of the tree, then applied (`direct_actor_calls`, on by default;
`CHIMAERA_DIRECT=0` turns it off in a driver). The shape differs from the plan above in three ways, all found while
building:

- **The driver records each call, not the executor.** When the outcome arrives, the driver submits the call with
  `TaskSpec.ran` set (new field 24). The shard stores it as done in the submission's own commit (`submit`, the same
  path as a failed dependency), and the peer files the executor's timings as the attempt and learns from it. Releases
  travel the same session after the record, so a large result is never freed early. The executor needs no new
  message to the peer.
- **In-flight calls are announced, not journaled.** As it sends a call, the driver also submits a copy with
  `TaskSpec.direct` set (field 25). Ingress gives it to the core's in-memory `direct::Inflight` and never to a shard.
  That gives `explain()` "running" or "queued at its actor behind N call(s)" (with the concurrency hint), lets the
  deadlock message name the waiting method, and is dropped on the call's Done event.
- **Concurrent actors go direct too.** The executor runs direct calls through `handle()`, so lanes, groups and async
  methods apply, using the actor's own concurrency from its constructor message, never the caller's.

**Files.** New: `crates/chimaera-peer/src/direct.rs`, `python/chimaera/_direct.py`, `python/tests/test_direct.py`.
Hooks:
- `proto/chimaera/v1/shard.proto` (`Ran`, fields 24 and 25); `chimaera-core` shard `submit` plus a scenario test.
- `chimaera-peer`: `lib.rs` (module), `core.rs` (in-flight map, `learn_direct`, explain, deadlock name,
  `queue_reason`), `ingress.rs` (notices and attempts).
- `chimaera-py/src/lib.rs`: `CallSpec.ran` and `CallSpec.direct`. Needs `maturin develop` when applied (shared .so).
- `_executor.py`: listen after the constructor, `starting`, reply in `finish`, `executor_id`.
- `_remote.py` (`take` in `submit_call`, `forget` in `close`), `_runtime.py` (`submit(send=)`, flush before
  release), `_future.py` (cancel).

**A bug found on the way, also on the live tree:** closing an actor while one of its calls runs made that call
raise `RemoteError: SystemExit: 0` instead of `ActorDied` (early notify forwards the executor's own outcome), and
the worker kept serving with no instance. Fix in `_executor.py`: `terminating`, map the interrupted call to
`ActorDied`, then re-raise `SystemExit`. Test: `test_a_call_running_when_its_actor_is_closed_raises_actor_died`,
which fails on the live tree every time.

**Evidence so far (debug builds, load 20–110):**
- Round trip at load ~110, same binary, direct on and off alternating: p50 121–125 µs against 296–360 µs.
- `test_direct.py`: 16 tests (order across paths, queued cancel, executor death, errors, large results, a direct
  result passed on, calls that must stay on the peer, concurrency kept, explain, close, socket mode).
- Mutation checks, each caught for the intended reason: no peer-busy check, no drain, no resubmit after a broken
  connection, deadline calls going direct, queued cancel ignored, no record; in the shard, deps pinned and `ran`
  stored twice.
- Rust: `cargo test -p chimaera-core -p chimaera-peer --lib` and clippy `-D warnings` clean; rustfmt clean on the
  touched lines.
- Full Python suite with the direct path on: the second run is in progress; the first found five failures, all fixed
  above (close and kill, CH121 count, explain and learning, deadlock name).

**Left from before it was applied:** the second full-suite run; release builds and the interleaved comparison with Ray
(`bench/microbench/run.py --chimaera-bin ... --chimaera-path ...`, actor rows); review of the hooks in the files
they touch.

**Also not done:** the parity.md §6 "partly today" items: a typed out-of-disk error, worker-log de-duplication,
configurable cancel timeouts, per-state Prometheus series and backlog age.
