# Local call path: no Raft log, no durable commit per call (2026-11-21)

The first steps of architecture.md D2 on the real runtime, applied to the shared tree with the user's approval, plus
two executor fixes found by profiling. A local peer's group no longer writes a Raft log, and an ordinary call's
commands no longer wait for a database commit at all.

## What changed

**Local peer without a Raft log** (`crates/chimaera-raft/src/local.rs`). A single-member group gains nothing from a
replicated log. Before, openraft wrote each command to the log database and then to the state-machine database:
two redb write transactions per command, and a local call makes two commands.
- **Startup:** the peer starts its group through Raft as before, so a log tail left by an older peer is replayed. It
  then waits until every committed entry is applied, shuts Raft down, and hands the group to an applier thread.
- **Ordinary calls:** a batch of only an attached run's call commands goes to an in-memory overlay above the
  database, with no write transaction. Those commands are submit, grant, publish, release, subscribe and dependency
  notice.
- **Everything else** is written together with the overlay in one `Durability::Immediate` commit. That covers run,
  actor and store commands, external effects, and every command of a detached run, which must resume after a crash.
  The commit survives the peer process dying.
- **Bounded loss:** the overlay is also written durably when it is 500 ms old, holds 65,536 entries, when the
  applier goes idle, or when the group shuts down.
- **What a crash can lose:** every database commit is durable, so the overlay is exactly what a crash may lose: at
  most 500 ms of an attached run's calls. Their driver loses its connection in the same crash and does not reconnect
  by itself.
- **Reads:** `Group::read` sees the overlay over the database.
- **Snapshots:** a local peer writes no Raft snapshots any more.
- **Unchanged:** cluster mode.

**Early outcome** (`core.rs`, `on_exec_done`, D2's direct path, stage 1). On a local peer, for an attached run, when
no other attempt of the call exists, the caller gets the outcome as soon as the executor returns. The publication
still follows through the group and is the record. `on_done` then finds no watchers left, so the caller is notified
exactly once. Detached runs and cluster peers still wait for the publication.

**Smaller fixes on the same path:**
- **Executor lane limits:** `python/chimaera/_executor.py` computed an actor's lane limits on every method call. That
  meant `dir()` plus about 30 `inspect.getattr_static` calls, nearly half the executor's per-call handling time
  under the profiler. They are now computed once per class. The executor's inbox is a `queue.SimpleQueue`.
- **Memory readings:** `budget::private_and_held` reads a finished process's memory once instead of twice. On Linux
  that is one `/proc/<pid>/status` read instead of two.

The diff:
- `crates/chimaera-raft/src/local.rs` is new.
- `lib.rs`: `Group.local`, `RaftHost::without_log`, and `Group::read` routed through the overlay.
- `state_machine.rs`: shared helpers.
- One line in `crates/chimaera-peer/src/runtime.rs`.
- `crates/chimaera-peer/src/budget.rs` and `core.rs` (`on_exec_done`).
- `python/chimaera/_executor.py`.

## Checks

**Rust: `cargo test -p chimaera-raft`.** 9 pass, and 3 measurements are ignored.
- **`the_overlay_reads_like_one_map`:** 20,000 random puts, deletes, write-outs and reads through the overlay over a
  real redb file. Every `get` and scan must match `MemKv`, with other groups' keys on both sides of the range.
- **`applies_without_a_log_and_survives_restarts_from_either_mode`:**
  - Writes through the Raft log, restarts without it, and the writes are there.
  - Applies 30 concurrent proposals, each with its own reply and its events.
  - Restarts cleanly, and everything is still there.
- **`a_detached_runs_calls_survive_the_process_dying_at_once`:** a child process submits 40 calls of a detached run
  and aborts. The parent finds all 40.
- **`an_attached_runs_calls_are_durable_within_the_checkpoint_window`:** the same with an attached run, idle for
  three checkpoint periods before aborting.

**Can the tests fail?** Each was run against a deliberate bug, and each bug failed its test:

| Deliberate bug | Test that failed |
|---|---|
| A merge tie that prefers the database over the overlay | the randomized overlay test |
| A shadowed database key that is not skipped | the randomized overlay test |
| Detached runs' calls treated as ordinary | the detached crash test |
| No idle write-out | the attached crash test, and the clean-restart test |
| Non-durable commits (step 1) | the step-1 crash test, with 0 of 40 surviving |

**A test changed on purpose.** The step-1 crash test expected an attached run's calls to survive an immediate abort.
D2 relaxes exactly that, so it became the two crash tests above.

**Other checks:**
- `cargo clippy -p chimaera-peer -p chimaera-raft --all-targets -- -D warnings` is clean, and so is `cargo fmt`.
- `cargo test -p chimaera-peer --lib`: 64 pass.
- **Python suite:** a fresh debug build of the live tree, with `test_docs.py` excluded. 298 passed, 5 skipped, 0
  failed. It includes the test that SIGKILLs the peer during a detached run of 10,000 tasks and checks every result
  arrives exactly once.

## Measurements

**Setup:** an M3 Pro laptop shared with other sessions. Builds are release. Round trips are one call through the
Python API on a fresh local peer, 3,000 calls after 300 warm-up, p50.

**The change on its own:** two binaries from one snapshot of the live tree, identical except for the one line that
turns the local group on. Same Python package, three alternating rounds, load 23–30:

| | Local group off | Local group on |
|---|---:|---:|
| Sync task | 595–640 µs | 247–296 µs |
| Sync actor call | 641–654 µs | 276–296 µs |

**Against Ray 2.60**, same laptop:

| | Chimaera (local group on) | Ray, interleaved at load 31–38 | Ray, at load 6–20 |
|---|---:|---:|---:|
| Sync task | 241–1084 µs (median 273) | 391–1987 µs (median 443) | 386–391 µs |
| Sync actor call | 257–472 µs (median 313) | 163–771 µs (median 185) | 166–181 µs |

- A local task call is now faster than Ray's.
- A local actor call is about 1.6–1.7× slower, down from about 3.5×.

**Early outcome on its own:** two release binaries from one snapshot, with and without the early outcome, three
alternating rounds, load 26–32:

| | Without | With |
|---|---:|---:|
| Sync task | 230–239 µs | 199–202 µs |
| Sync actor call | 247–261 µs | 219.5–220 µs |

A local actor call is now about 1.25× Ray's (163–185 µs at a similar load). A local task call is about half of
Ray's.

**Where an actor call's time goes** (timestamps in the driver, peer and executor; p50 of 1,499 calls; load 25–28):

| Stage | p50 |
|---|---:|
| Driver's `.remote()` (Python) | 34 µs |
| To the peer's ingress | 14 µs |
| Submit through the group, until the core handles the grant | 35 µs |
| Grant to Execute sent | 5 µs |
| Execute to the executor's reader thread | 21 µs |
| Reader thread to main thread | 15 µs |
| The method | 16 µs |
| Done back to the core | 5 µs |
| Accounting before the caller is told | 12 µs |
| Notify to the driver's `result()` | 33 µs |

- **The accounting now comes after the caller is told** (`on_exec_done`). In paired rounds, actor calls were faster
  in all six: 210.5→199.3 and 200.9→190.5 µs in the two calm rounds.
- **Removing the two forwarding tasks didn't help.** The forwarders carry raft and executor events into the core
  loop. Over eight alternating runs each, the median actor call was 204 µs with them and 215 µs without, so that
  change was not applied.

**One proposal through the group** (`measure_propose_with_and_without_the_log`):

| | p50 | p99 |
|---|---:|---:|
| Raft log | 347 µs | 1,117 µs |
| Attached call, overlay | 22 µs | 128 µs |

Most of the 22 µs is the hop to the applier thread and back.

**The executor fix on its own:** same binary, Python package with and without the lane cache, three alternating
rounds, load 18–22. The actor round trip went from 326–541 µs to 288–298 µs. Task calls don't use lanes and were
unchanged.

**Where the actor call's time goes now** (about 280 µs):
- Two group proposals of about 22 µs each.
- The executor: about 46 µs of CPU per call.
- The peer's core loop: about 34 µs per call.
- The gRPC session: about 15 µs.
- The driver's `.remote()`: about 15–20 µs.
- About six thread wake-ups across three processes.

Ray sends an actor call from caller to actor process with no agent in between. Matching it needs the design's direct
actor path, not more trimming of this one.

## Found on the way

**A peer filled the disk.** A peer without this change writes a full Raft snapshot every 50,000 log entries and never
deletes old ones. A benchmark run here reached 5.1 GB of them and filled the disk at 07:37. The peer then refused
writes and hung. Local peers no longer write snapshots; cluster peers still have the bug.

**Testing hazards in a shared tree:**
- **Mutation checks:** one of my mutation checks was running in the live tree when the runtime session committed it.
  The deliberately broken line went into `881a26d`, and the runtime session fixed it in `b6c71a5`. Mutation checks
  belong in a scratch copy.
- **Binaries:** on macOS, a peer binary copied over in place can hang processes started from it. Delete the file
  first.
- **Stale builds:** compare binaries built from one snapshot. A released-values test looked like a regression from
  this change until both sides were built from the same tree.
