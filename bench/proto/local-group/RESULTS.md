# Local peer without a Raft log (2026-11-21)

`local-group.patch` is the first step of architecture.md D2 on the real runtime: a local peer's single group stops
writing a Raft log. It is not applied. The files it touches belong to the runtime session, which has not yet agreed
to the change.

Apply with `git apply bench/proto/local-group/local-group.patch`. It applied cleanly to the live tree at 11:05, and
the result type-checks.

## What it changes

A single-member group gains nothing from a replicated log. Today openraft writes each command twice:
- to the log database;
- then to the state-machine database.

That is two redb write transactions per command, and a local call makes two commands.

With the patch:
- **Startup:** the local peer starts its group through Raft as before, so a log tail left by an older peer is
  replayed. It then waits until every committed entry is applied, shuts Raft down, and hands the group to an applier
  thread (`crates/chimaera-raft/src/local.rs`).
- **Applying:** the applier applies each batch of concurrent proposals in one transaction.
- **Durability:** each commit is `Durability::Immediate` on the local peer's no-sync backend. It survives the peer
  process dying, which is the guarantee the log gave.
- **Replies and events:** these go out exactly as they did from `StateMachine::apply`.
- **Cluster mode:** unchanged.
- **Snapshots:** the log no longer grows, so a local peer never writes Raft snapshots. Snapshots are never deleted
  (the bug below), and one benchmark run here produced 5.9 GB of them.

The diff:
- **`crates/chimaera-raft/src/lib.rs`:**
  - `Group` gains a `local` applier.
  - `propose`, `linearizable`, `is_leader` and `view` route to it.
  - `RaftHost::without_log` makes the switch.
- **`crates/chimaera-raft/src/state_machine.rs`:** `apply_cmd` is extracted so both paths apply commands the same
  way.
- **`crates/chimaera-peer/src/runtime.rs`:** one line in `run_local`.
- **`crates/chimaera-raft/src/local.rs`:** new, with the applier and its tests.

## Checks

**Rust: `cargo test -p chimaera-raft`.** 7 pass, and the 2 measurements are ignored. The new tests:
- **`applies_without_a_log_and_survives_restarts_from_either_mode`:**
  - Writes through the Raft log, then restarts without it, and the writes are there.
  - Applies 30 concurrent proposals; each gets its own reply, and their events arrive.
  - Restarts again, and everything is still there. A resubmitted call comes back as a duplicate.
- **`replies_survive_the_process_dying`:** a child process applies 40 commands, then aborts with no shutdown. The
  parent reopens the directory and finds all 40.
  - This test can fail. With `Durability::None` it fails: 0 of 40 survive.
  - The first test cannot catch that mutant, because redb persists everything on a clean shutdown.
- **Lint:** `cargo fmt --check` and `cargo clippy --tests` are clean on the changed files.

**Python suite:** run against a peer built with and without the patch, from the same frozen copy of the tree.
- Both: 258 passed, 4 skipped.
- No test changed outcome.
- `test_docs.py` was left out.

## Measurements

The setup:
- **Machine:** M3 Pro laptop, macOS, shared with other sessions.
- **Binaries:** release builds of the same source, with and without the patch.

**Commit path alone** (`measure_propose_with_and_without_the_log`, release build, load about 12):

| | Sequential propose p50 | p99 | Concurrent proposals |
|---|---:|---:|---:|
| Raft log | 346 µs | 3,106 µs | 95,028/s |
| No log | 132 µs | 479 µs | 98,972/s |

**Round trip of one call** through the Python API on a fresh local peer:
- 3,000 calls after 300 warm-up.
- Three alternating runs per binary.
- Load 13–14 throughout.

| | Without the patch, p50 (p99) | With the patch, p50 (p99) |
|---|---:|---:|
| Sync task | 483–499 µs (1.3–3.5 ms) | 295–301 µs (0.46–0.93 ms) |
| Sync actor call | 514–531 µs (0.81–1.5 ms) | 330–337 µs (0.99–1.66 ms) |

Every run with the patch beat every run without it, by about 190 µs per call.

**Ray's microbenchmark rows** (bench/microbench, two alternating rounds, load 6–23):
- Sync tasks rose from 1,254/s to 1,842/s, ahead in both rounds.
- The pipelined rows moved by −33% to +7%:

  | Row | Change |
  |---|---:|
  | Async tasks | +7% |
  | 1:1 actor calls, async | +5% |
  | 1:1 actor calls, concurrent | −33% |
  | n:n actor calls | −24% |

  These rows aren't bound by the commit path, and the load varied 4× between runs, so they don't separate the two
  binaries.

## What it means for parity

**What changes:** a local actor call's round trip falls from about 520 µs to about 335 µs.
- Ray's is 167–177 µs on this machine.
- The gap to Ray shrinks from about 3× to about 2×.

**What is left:**
- Two Immediate commits per call (~130 µs each).
- The executor and driver stages.

The next step in D2 removes the commits for ordinary calls: apply them in memory and journal them. The prototype in
bench/proto/hotpath shows that path at 17.8 µs.

## Found on the way

**A baseline peer filled the disk.** It writes a full Raft snapshot every 50,000 log entries and never deletes old
ones (`chimaera-raft/src/lib.rs:63`, `state_machine.rs`).
- A benchmark run here reached 5.1 GB of snapshots, on a disk with about 6 GB free.
- The disk filled at 07:37. The peer's group then refused writes ("No space left on device") and the benchmark hung
  until it was killed.
- With this patch a local peer stops adding to the log, so it writes no snapshots. Cluster peers still have the bug.
