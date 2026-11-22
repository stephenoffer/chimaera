# Design: apply ordinary calls inline in the local group (proposal, not applied)

**For:** chimaera-runtime-design-review, which owns `crates/chimaera-raft`.
**From:** chimaera-8f, with the user's approval to propose it.
**Scope:** only `crates/chimaera-raft/src/local.rs`, plus tests. Cluster groups are unchanged.

## Why

On a local peer, a synchronous actor call takes 190–200 µs p50, against Ray's 163–185 µs on the same laptop.
Timestamps in the driver, peer and executor put 35 µs of it between ingress receiving the submit and the core
handling its grant (RESULTS.md, "Where an actor call's time goes").

Almost none of those 35 µs is work. The submit is an attached run's call, so it only touches the in-memory overlay.
The time goes on three hops:
- from the ingress task to the applier thread, over a std channel and a thread wake;
- back to ingress, over a oneshot;
- from the applier thread to the core, via `blocking_send` on the events channel.

## Change

`Group::propose` on a local group applies a command in the calling task, with no applier hop, when all of these
hold:
1. **The command is overlay-eligible:** the same rule as today (`needs_durability` is false). That means submit,
   grant, publish, release, subscribe or dependency notice, for runs known not to be detached.
2. **The overlay is young and small:** the same `full` test the applier uses.
3. **The applier is idle:** a shared counter of proposals queued for it is zero. The counter is checked while
   holding the overlay's write lock.

Otherwise the command goes to the applier exactly as today. Anything durable never runs inline, including detached
runs, actors, store writes, external starts and overlay write-outs.

## Ordering

These invariants should be reviewed. Each is enforced as follows:

- **A proposer's commands apply in the order it proposed them.**
  - If an earlier command went to the applier, the counter is non-zero until the applier has applied it *and* sent
    its events. A later command from the same proposer therefore queues behind it.
  - If the earlier command ran inline, it finished before `propose` returned.
- **Events reach the core in apply order for causally related commands.** This is the subtle one. Today the
  applier replies first and sends events after. Suppose a proposer gets the reply to a durable command (say
  `ActorCreate`) and immediately submits a call inline. Its events could then overtake the `ActorCreate` events still
  waiting on the applier thread. To stop that, the applier decrements the counter only after its events are sent.
  So "counter is zero" means every earlier batch is applied, replied to and has had its events sent.
- **Unrelated concurrent proposers** may interleave, as they can today through the channel.
- **An inline apply's events** are sent after the overlay lock is released, with `try_send`. If the channel is full,
  they fall back to `send().await`, so no lock is held across an await.

The overlay, the detached-run cache and a `ShardState` move into one struct behind the existing `RwLock`. The
applier thread and inline callers take the same write lock. Readers (`Group::read`) are unchanged.

## Durability

There is no change. Inline applies touch only the overlay, which already holds exactly what a crash may lose. The
applier still writes the overlay out durably at 500 ms, at 65,536 entries, when idle, and with every durable batch.

## Tests

All existing local tests still pass: the randomized overlay test, the restart test and both crash tests. New ones:
- **The inline path is taken** for an attached run's submit: a test-only counter.
- **Per-proposer order** across a mix of durable and inline commands from several tasks: each task's commands apply
  in its own order.
- **Event order:** a durable command's events precede the events of an inline command proposed after its reply.
- **Each test can fail:** each is run against a planted bug in a scratch copy only:
  - decrementing the counter before the events are sent;
  - skipping the counter check.

## Status: built and measured in a scratch copy, not applied

`inline-apply.patch`, next to this note, is the change with its tests. `git apply --check` passes on the live tree.

**Raft tests:** all 16 pass, including two new ones.
- `an_attached_runs_calls_apply_in_the_calling_task` checks the inline path is taken. It uses a per-applier counter,
  so tests running in parallel can't disturb it.
- `events_keep_proposal_order_across_queued_and_inline_commands` runs 50 rounds of a detached run's call followed
  by an attached run's call, and checks every pair's events arrive in order.
  - The race window is about a microsecond, so a test-only per-applier pause between a batch's replies and its
    events widens it.
  - Without the pause, neither planted bug was caught.
  - With it, both are: releasing the counter before the events are sent, and skipping the counter check.

**Python suite** (scratch snapshot, inline build): 4 failures, none caused by inline apply.
- `test_chained_stages_still_flow_between_workers` fails on the baseline build from the same snapshot too.
- `test_split_values_reach_their_calls` passes on both builds when rerun.
- The quorum and elastic-gang tests are cluster tests, where inline apply doesn't run.

**Measured:** release builds from one snapshot, six paired rounds at load 10–15.

| p50 round trip, median of 6 runs | Baseline | Inline apply |
|---|---:|---:|
| Actor call | 192.5 µs | 183.3 µs |
| Task call | 188 µs | 187 µs |

Actor calls were faster in four of six rounds, equal in one and slower in one.

## Expected effect (before measuring)

About 20–25 µs off each local submit, so local actor calls of about 170–180 µs, level with Ray. It will be measured
by an A/B from one snapshot before anything is applied.
