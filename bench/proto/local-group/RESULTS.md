# Local peer without a Raft log (2026-11-21)

The first two steps of architecture.md D2 on the real runtime: a local peer's single group no longer writes a Raft
log, and an ordinary call's commands no longer wait for a durable commit. Applied to the shared tree on 2026-11-21
with the user's approval. Not committed by this session.

## What changed

A single-member group gains nothing from a replicated log. Before, openraft wrote each command to the log database
and then to the state-machine database: two redb write transactions per command, and a local call makes two
commands.

**Step 1: no log.**
- **Startup:** the local peer starts its group through Raft as before, so a log tail left by an older peer is
  replayed. It then waits until every committed entry is applied, shuts Raft down, and hands the group to an applier
  thread (`crates/chimaera-raft/src/local.rs`).
- **Applying:** the applier applies each batch of concurrent proposals in one transaction.
- **Replies and events:** these go out as they did from `StateMachine::apply`.
- **Snapshots:** the log no longer grows, so a local peer writes no Raft snapshots. Snapshots are never deleted, and
  one benchmark run here piled up 5.9 GB of them.

**Step 2: durability only where it is needed.** "Consensus for lifetimes, not for calls."
- **What stays durable:** a batch commits `Durability::Immediate` (it survives the peer process dying) when it holds
  anything but an ordinary call's commands. That covers:
  - run, actor and store commands, and external effects;
  - every command of a detached run, which must resume after a crash.
- **What doesn't wait:** a batch of only an attached run's call commands commits non-durably. Those commands are
  submit, grant, publish, release, subscribe and dependency notice.
- **Why that is safe:** redb makes durability a prefix, so the next durable commit persists it too. That comes at
  most 500 ms later: the applier checkpoints when idle, and forces the next batch durable after 500 ms.
- **What a crash can lose:** at most that window of an attached run's calls. Their driver loses its connection to
  the peer in the same crash and does not reconnect on its own.
- **Unchanged:** cluster mode.

The diff:
- **`crates/chimaera-raft/src/lib.rs`:**
  - `Group` gains a `local` applier.
  - `propose`, `linearizable`, `is_leader` and `view` route to it.
  - `RaftHost::without_log` makes the switch.
- **`crates/chimaera-raft/src/state_machine.rs`:**
  - `apply_cmd` is shared by both paths.
  - `run_detached` is a read helper.
  - `CHECKPOINT_EVERY` is now `pub(crate)`.
- **`crates/chimaera-peer/src/runtime.rs`:** one line in `run_local`.
- **`crates/chimaera-raft/src/local.rs`:** new, with the applier and its tests.

## Checks

**Rust: `cargo test -p chimaera-raft`.** 8 pass, and 3 measurements are ignored. The new tests:
- **`applies_without_a_log_and_survives_restarts_from_either_mode`:**
  - Writes through the Raft log, restarts without it, and the writes are there.
  - Applies 30 concurrent proposals; each gets its own reply, and their events arrive.
  - Restarts again, and everything is still there.
- **`a_detached_runs_calls_survive_the_process_dying_at_once`:** a child process submits 40 calls of a detached run,
  then aborts. The parent finds all 40.
- **`an_attached_runs_calls_are_durable_within_the_checkpoint_window`:** the same with an attached run, sitting idle
  for three checkpoint periods before aborting. All 40 are there.

**Can the tests fail?** Each was run against a deliberate bug:
- Making commits non-durable (step 1) failed the crash test, with 0 of 40 surviving.
- Treating detached runs' calls as ordinary failed the detached test.
- Dropping the idle checkpoint failed the attached test.

**Step 2 changed a test on purpose.** The step-1 crash test expected an attached run's calls to survive an
immediate abort. Step 2 relaxes exactly that, as D2 intends, so the test became the two above.

**Lint:** `cargo fmt --check` is clean. `cargo clippy` is clean on these files.

**Python suite:** a debug peer built from the live tree, with `test_docs.py` excluded.

| Peer | Passed | Skipped | Failed |
|---|---:|---:|---:|
| Without the change | 268 | 5 | 1 |
| Step 1 | 267 | 5 | 2 |
| Step 2 | 269 | 5 | 0 |

The failures:
- **Without the change:** `test_cluster::test_an_unsynced_peer_whose_machine_rebooted_rejoins_as_a_new_member`. It is
  cluster mode, which this change doesn't touch. It passed in the step-2 run.
- **Step 1:** that same cluster test, plus `test_locality::test_readers_spread_to_other_peers_once_the_holder_is_busy`.
  The locality test is a multi-peer timing assertion, and it passed on reruns with both binaries.

The step-2 suite includes `test_recovery::test_peer_killed_mid_run_resumes_and_reattach_sees_every_result_once`:
SIGKILL the peer during a detached run of 10,000 tasks, restart, and see every result exactly once.

## Measurements

**Setup:** an M3 Pro laptop shared with other sessions. Each comparison ran release builds alternately, three runs
each. Round trips are for one call through the Python API on a fresh local peer, 3,000 calls after 300 warm-up.

**Commit path alone** (`measure_propose_with_and_without_the_log`, release build):

| | Sequential propose p50 | p99 | Concurrent |
|---|---:|---:|---:|
| Raft log | 139–346 µs | 367–3,106 µs | 95k–143k/s |
| No log, durable commit | 132 µs | 479 µs | 99k/s |
| No log, non-durable commit | 31 µs | 42 µs | 124k/s |

The two Raft-log runs were at different machine loads, hence the ranges.

**Round trips:**

| | Sync task p50 | Sync actor call p50 | Load |
|---|---:|---:|---:|
| Before the change (07:15 tree) | 483–499 µs | 514–531 µs | 13–14 |
| Step 1 (07:15 tree) | 295–301 µs | 330–337 µs | 13–14 |
| Before the change (live tree, 11:47) | 499–525 µs | 530–624 µs | 14–17 |
| Steps 1 and 2 (live tree) | 257–260 µs | 285–319 µs | 14–17 |

**The result:** a local call's round trip is about half what it was.
- Ray's actor round trip on this machine is 167–177 µs.
- Chimaera's local actor call was about 3× slower than Ray's. It is now about 1.7× slower.

**What is left is not commits.** At about 30 µs each, the two commits are about 60 µs of a ~260 µs round trip. The
rest is:
- the driver's `.remote()` (about 22 µs);
- the peer reading the task back after the grant;
- admission and rusage syscalls;
- about three Python thread handoffs;
- four process crossings.

bench/proto/hotpath shows that a lean version of this path takes 17.8 µs.

## Found on the way

**A peer filled the disk.** A peer without this change writes a full Raft snapshot every 50,000 log entries and
never deletes old ones (`chimaera-raft/src/lib.rs:63`, `state_machine.rs`).
- A benchmark run here reached 5.1 GB of snapshots on a disk with about 6 GB free.
- The disk filled at 07:37. The group then refused writes ("No space left on device") and the run hung until killed.
- Local peers no longer write snapshots. Cluster peers still have the bug.
