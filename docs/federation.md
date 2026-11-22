# Federation: cells that lend each other capacity

Status: first slice built and tested (M5); see the gate at the end. This adds to
[architecture.md §2 and §6](architecture.md); where the two differ, this file
describes what is being built now.

## What a cell is, and what crosses between cells

A **cell** is an ordinary Chimaera cluster: its own membership, Raft groups,
vshard map, object stores and drivers. Nothing in a cell's correctness depends
on another cell. A **fleet** is a set of cells that know each other's client
endpoints and may run each other's calls.

Two things cross a cell boundary, and nothing else:

1. **A summary**, every 500 ms: each cell's free CPUs (less the calls
   waiting in its queues) and its total CPUs, read by the others from its
   `Status` reply. No per-call traffic is sent for this. (shortcut: every peer
   polls every cell. A large fleet would read summaries through per-cell
   ambassadors.)
2. **A delegated call.** When a cell is full, a peer holding a granted
   attempt may run that attempt in another cell instead of locally. It does so
   as an ordinary authenticated client of the other cell.

## Delegation

The owning cell's shard never learns that a call left the cell. From its point
of view, peer P was granted attempt *n* of call X and later published an
outcome or reported a failure, exactly as for local work. P does the rest:

1. **When.** X is queued on P and cannot start: P has no free CPU, and no
   peer in P's cell has room either, according to the load reports spillback
   already uses. X must also have waited at least `DELEGATE_AFTER` (300 ms).
   Another cell must report free CPUs for it. The peers of one cell delegate
   without talking to each other, so each counts on an equal share
   (1 / its cell's live peers) of another cell. Within that share it counts:
   - the cell's reported room, less what it sent since that report, so a
     burst cannot flood a cell between reports;
   - and never more than that share of the cell's CPUs, less its own calls
     still running there, because reports lag.
2. **What.** Only calls whose whole meaning travels with them:
   - plain function calls, not actor calls or generators;
   - no dependencies; the large values it reads (buffers passed to it, and
     `ch.put` values) must be on the delegating peer, at most 256 MiB in all.
     Each is uploaded to the shadow run once, with `PutObject`, which checks
     the bytes against a checksum (a `ch.put` id is not its content);
   - no devices, labels, placement predicates or budget;
   - `effects="none"`, so running it twice is harmless.

   Everything else stays in its cell.
3. **How.** P connects to the other cell's client endpoint with the fleet
   token, creates a *shadow run* there, ships the function's code (`PutCode`)
   and submits X's spec under the shadow run. The shadow run gets the owning
   run's interpreter, working directory and import path. P then watches X's
   outcome, fetching large results with `GetObject` (a result stored in parts
   comes with its parts), and publishes it in its own cell as the attempt's
   outcome.
4. **Failure.** If the other cell is unreachable, loses the call, or fails it
   for an infrastructure reason, X goes back to P's own queue, and P marks
   that cell down for 30 s. P was granted X and X has no effects, so running it
   here is safe and costs no attempt. If P itself dies, the shard retries X as
   usual. A user exception is the call's result and is published as such.
5. **Fencing.** The outcome is published through the same fenced path as an
   executor's, keyed by task, attempt and peer. If P was retired meanwhile, the
   publish is refused and the home cell's retry wins. The shadow run is not
   detached and is tied to P's session, so the other cell cancels its calls 10 s
   after P disappears.

Why this shape:
- It needs no cross-cell Raft, no shared vshard space and no shard changes.
- A lost cell costs only the attempts delegated to it, and those are retried
  where they came from. So "kill cell B and cell A keeps its throughput"
  follows from the existing retry path.
- The price: delegated calls pay a client round trip and ship their arguments,
  which is why only self-contained calls are eligible.

## Configuration

```
CHIMAERA_CELL=a                                   # this cell's name (also the peer label cell=a)
CHIMAERA_FLEET="b=10.0.1.5:7710,c=10.0.2.5:7710"  # other cells' client endpoints
CHIMAERA_FLEET_TOKEN_FILE=/etc/chimaera/fleet     # the token the other cells accept
```

Each cell's client endpoint must accept the fleet token. Its peers get it as
their cluster token, or a cell's operator gives the fleet that cell's token.
A peer without `CHIMAERA_FLEET` never delegates.

## The cell directory

Instead of, or alongside, the static `CHIMAERA_FLEET` list, cells can find
each other through a directory cell:

```
CHIMAERA_FLEET_DIRECTORY=10.0.1.5:7710   # a client endpoint of the directory cell
CHIMAERA_CELL=a@10.0.0.5:7710            # this cell's name and the endpoint others reach it at
```

- **Registering:** every 2 seconds each peer renews its cell's entry, with a 10 s TTL, in the `fleet/cells` store namespace of the directory cell. That cell's Raft groups replicate it, so it survives the loss of a minority of the directory cell's peers.
- **Reading:** each peer reads the list the same way. A cell that stops renewing drops out within 10 s.
- **Losing the directory:** if the directory cell is lost entirely, peers keep the list they last read. Discovery stops; delegation doesn't.
- **Tested:** `test_cells_find_each_other_through_a_directory`.

## Not in this slice

- **Global names and tenant envelopes** in the directory (only the cell registry is there).
- **Cross-cell vshard seal/adopt** (moving ownership of a range between cells).
- **Calls with dependencies**, which need fetching an object from the home
  cell by id (large arguments and `ch.put` values the call names do travel).
- **Actor placement across cells.**

## Gate for this slice

`python/tests/test_fleet.py` runs two cells of three 4-CPU peers on one
machine:
1. 48 two-second calls in a run of A, which alone needs about 8 s. Some run in
   B, and the run takes under 6.5 s.
2. Every peer of B is killed mid-run. All 48 calls return, each exactly once,
   within A's own time plus one retry of what B held.
3. B's peers restart from their directories, and B serves a new run.

More tests run 48 calls that each read the same 4 MiB array, passed directly
or as a `ch.put` value: some run in B, which got the array once, and every call
reads all of it. Another returns three values per call, stored in parts in B
(see `docs/api.md`, multiple returns): every value comes back,
whole and one at a time.

It passes. Against a build without delegation it fails at step 1 ("no call ran
in cell B", 4.21 s).

A cell that loses the majority of its peers tells its clients so:
`ch.errors.QuorumLost`, naming the group, once a submission has waited out the 60 s
patience. When its peers come back, calls go through again
(`python/tests/test_quorum.py`).

Two flooding bugs were found and fixed while making it pass:
- B's reported room ignored calls queued in B;
- each of A's peers counted all of B's room as its own.

Not covered here:
- calls with dependencies, which stay home;
- a restore from a Raft snapshot, rather than from each peer's directory.
