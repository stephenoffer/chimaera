# Design: the direct actor path on a local peer (proposal; floor measured, not built)

**Touches:** ingress, the core and the group; the native client (`crates/chimaera-py`), `_runtime.py` and
`_future.py`; the executor (`_executor.py`).
**Scope:** same-machine actor calls first. Cluster and remote drivers keep today's path.

## Why

Interleaved with Ray 2.60 on the same laptop, a local task call takes 192.5 µs against Ray's 378 µs, but a local
actor call takes 190.5 µs against Ray's 145 µs. Ray sends an actor call from the caller straight to the actor's
process. Chimaera's goes through the peer agent twice, and the timestamps show where that costs (RESULTS.md, "Where
an actor call's time goes"):

| Peer-side hop on today's path | p50 |
|---|---:|
| Driver to ingress (gRPC session) | 14 µs |
| Submit through the group to the core's grant | ~25 µs after inline apply |
| Grant to Execute sent | 5 µs |
| Execute to the executor's reader | 21 µs |
| Done back to the core | 5 µs |
| Core to the driver's `result()` (session stream and dispatcher thread) | 33 µs |

Together that is about 100 µs. A direct path keeps two hops: driver to executor and executor to driver, each one
Unix-socket write. Expected round trip: about 95–110 µs, below Ray's 145 µs.

## Measured floor (prototype, 2026-11-22)

**What the prototype does:** in a scratch copy only, the actor's executor listens on a socket. Its reader thread
hands each direct call to the main thread through the executor's real inbox, and the main thread runs the method and
replies on the connection. The driver calls through a plain socket.

**What it leaves out:** the API layer, futures, journaling and failure handling.

**Result:** three runs on a live-tree release peer, load 19–21.

| p50 round trip | Through the peer (today) | Direct to the executor |
|---|---:|---:|
| Run 1 | 209.1 µs | 15.0 µs |
| Run 2 | 363.5 µs | 17.0 µs |
| Run 3 | 365.2 µs | 22.0 µs |

**What that means:** the direct hop costs about 20 µs. A full direct path adds the driver's `.remote()` (about
34 µs today, which can be trimmed) and resolving the future from the client's socket reader (about 10–15 µs).
Journaling runs off the call's path. That puts the full path at about 70–90 µs, under Ray's 145 µs.

## Shape

1. **Each actor executor gets a direct socket.** On a local peer, an actor executor also listens on
   `<peer dir>/direct/<executor id>.sock`, mode 0600 and owned by the peer's OS user. That is the same trust boundary
   as `exec.sock`.
2. **The driver learns the socket.** It comes with the actor's placement. The peer already tells the session when
   an actor is placed; the event gains the socket path. The native client connects lazily on the handle's first
   method call.
3. **Calls go straight to the executor.** A `DirectCall` frame carries:
   - the call key, from the deterministic id scheme;
   - the handle's sequence number;
   - the method;
   - inline args, or an object reference as today.

   The executor's reader thread hands calls to the main thread in sequence order per connection, as Execute
   messages are today. Lanes and `concurrency` apply unchanged.
4. **Results come straight back** as a `DirectDone` frame (the outcome, inline when small) on the same connection.
   The native client resolves the future from its socket reader, which is a thread of the client. So the dispatcher
   thread is not on this path.
5. **The peer still records every call, off the call's path.** The executor batches journal records to the peer
   over its existing connection, every 1 ms or 64 calls:
   - the call key and spec digest;
   - the start and end times;
   - its own rusage deltas;
   - the outcome summary.

   The peer applies them to the local group's overlay as a submit-and-publish pair, so `explain`, `inspect`,
   `chimaera runs`, accounting and learning keep working, about 1–2 ms behind the call.

## What does not take the direct path

These keep today's path through the peer:
- **Durable actors.** Their reply must wait for the committed state.
- **`effects="external"` methods.** `Started` must be durable before user code runs.
- **Detached runs.** Every command must survive the peer dying.
- **Generator methods and calls with deadlines.** Version 1 only, to keep it small.
- **Calls to actors on other peers.**
- **Cancels.** A cancel is sent to both the executor (direct frame) and the peer (today's path). The peer still
  escalates to SIGUSR2 at 2 s and a kill at 5 s.

## Failure and ordering

- **Per-handle order.** Sequence numbers are per handle, and the executor runs a connection's calls in sequence
  order. A handle that has used the direct path stays on it until the connection breaks.
- **Executor death.** The driver's connection closes. The client resubmits every unanswered call through the peer,
  with the same call keys. The peer restarts the actor under its restart policy, and its dedupe by call key stops a
  call that did finish from running again.
  - A call that ran but whose `DirectDone` was lost reruns only if the journal record didn't reach the peer either.
    Only `effects` none or idempotent calls take the direct path, so that is allowed.
- **Peer death.** The driver loses its session, as it does today.
- **Actor restart or migration.** The peer tells the session the new socket, or none. The client drains the old
  connection and switches.
- **Backpressure.** The executor stops reading its direct socket when it has `max_in_flight` calls queued. The
  driver's `.remote()` then waits on the socket write, which bounds memory on both sides.

## Security

- The direct socket lives in the peer's directory, mode 0600. Only processes running as the peer's OS user can
  connect, which matches `exec.sock` and the client socket.
- With per-tenant credentials, the peer only gives the socket path to a session whose tenant owns the actor. The
  executor checks a per-actor token, issued with the path, on connect.

## Tests (planned)

- **Order:** many handles call one actor concurrently; each handle's calls run in order.
- **Executor killed mid-burst:** every call completes exactly once, through the peer, after the restart.
- **Journal:** `explain()` on a direct call shows its attempt, measurements and outcome within a few milliseconds.
- **Fallback:** durable actors, external effects and detached runs never use the direct path, checked with a
  test-only counter.
- **Security:** a session without the actor's token can't connect.
- **Microbenchmarks:** Ray's actor rows (sync, async, concurrent, 1:n, n:n), same-snapshot A/B, then interleaved
  with Ray.

## Plan

1. Build it in a scratch copy, measure, and run the suites.
2. Show the owners the diff and numbers.
3. Apply in this order: executor (direct socket, journal batches), then peer (journal apply, socket path in the
   placement event), then native client (direct connection, resubmit on failure).

Each step is behind a peer setting (`direct_actor_calls`), off until all three are in, so the tree stays green
between steps.
