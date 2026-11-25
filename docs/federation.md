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

1. **Capacity summaries**, every 500 ms, between one peer of each cell and
   four other cells (see [Summaries](#summaries)): a cell's free CPUs (less
   the calls waiting in its queues), its total CPUs, and a bounded digest of
   what it heard of other cells. No per-call traffic is sent for this.
2. **A delegated call.** When a cell is full, a peer holding a granted
   attempt may run that attempt in another cell instead of locally. It does so
   as an ordinary authenticated client of the other cell.

## Summaries

One peer of each cell, its **summary publisher**, talks to the other cells for
it, so the traffic between cells does not grow with their peers.

- **Election.** The publisher holds a lease in its cell's own store (3 s)
  and writes the cell's view there every round, renewing the lease just
  before each write. The cell's other peers read the view from a replica of
  that store, their own or another peer's, which adds nothing to the Raft
  log. A peer that has not seen the view change for the lease's time, plus up
  to as long again (it varies by peer), takes the lease. Its fencing token is
  higher, and the store refuses a view written under an older one. The new
  publisher reads the view through the log first, so it carries on from the
  last one's view rather than a replica's older copy.
- **Stalls.** A request for another cell's summary waits at most 1 s. A
  cell that accepts connections and never answers slows the rounds, but the
  publisher keeps its lease. The directory is read apart from the rounds
  (see [The cell directory](#the-cell-directory)), so it never holds one up.
- **Neighbors.** Every round the publisher asks four other cells for their
  capacity (`Status` with `fleet`). With every cell the fleet knows ordered by
  name, those are the cells 1, b, b² and b³ places after its own, where b⁴ is
  at least the number of cells. Each cell asks four and is asked by four,
  however many cells there are. That is why a fleet needs each cell's name
  (`CHIMAERA_CELL`), and why every cell must list the others by the names
  they give themselves. The directory takes care of this.
- **Reports.** A cell answers with its own report and the reports it heard.
  Its own report holds its free and total CPUs, its region, and a number that
  grows with each report. CPUs are the only resource class a peer reports,
  since only CPU calls are delegated; the digest groups by class all the
  same. A cell passes on at most 64 heard reports, at most 8
  per region and resource class, the roomiest first. The publisher keeps
  each cell's newest report and counts its age in rounds. It drops a report
  more than 4 rounds old: by then the room has likely gone to other cells'
  calls. Each report also carries its cell's id, the node id of that cell's
  first publisher, kept in the stored view. A cell's own report comes back
  to it under the names other cells use for it, and the id is how the cell
  recognizes and drops it.
- **Choosing a cell.** A call goes only to a cell its own cell lists (in
  `CHIMAERA_FLEET` or the directory), at the endpoint listed there: reports
  name other cells too, but not where to reach them. It goes to a cell in its
  own cell's region (the peers' `region` label) when one there has room, and
  else to any cell with room. The pick is random, weighted by room, so cells
  that heard the same reports spread their calls instead of all choosing the
  roomiest.
- **Metrics.** `fleet_summaries_served_total` and `store_requests_total` count
  the requests a peer served, and `fleet_directory_listings_total` the times
  it listed the directory whole. `fleet_cells`, `fleet_publishing` and
  `fleet_epoch` show what its view holds.
- **Tested.** `test_one_peer_of_a_cell_asks_the_others_however_many_it_has`
  runs a cell of 2 peers, then one of 8. Each asks another cell for 2
  summaries a second. When every peer asked every 500 ms, the same test
  measured 4 and 16. After the 8-peer cell's publisher is killed, another
  peer takes over with a higher epoch, and the rate stays at 2.
  `test_a_publisher_keeps_its_lease_while_other_cells_do_not_answer` points
  a cell's directory and only neighbor at a socket that never answers: the
  publisher keeps one epoch.
  `crates/chimaera-core/tests/fleet_routing.rs` runs the exchange at 196 and
  512 cells, in four regions with CPUs and GPUs, against an oracle that routes
  with every cell's true free capacity. Hot spots move every 30 rounds. 1.0%
  of the calls sent found no room at 196 cells and 1.3% at 512, and calls
  waited 0.0012 and 0.0015 rounds on average; the oracle's calls never
  waited. Choosing the roomiest cell instead sent 11% and 16% where the room
  had gone, and calls waited about 20 times as long.

## Delegation

The owning cell's shard never learns that a call left the cell. From its point
of view, peer P was granted attempt *n* of call X and later published an
outcome or reported a failure, exactly as for local work. P does the rest:

1. **When.** X is queued on P and cannot start: P has no free CPU, and no
   peer in P's cell has room either, according to the load reports spillback
   already uses. X must also have waited at least `DELEGATE_AFTER` (300 ms).
   P's cell's view must hold a report of another cell with free CPUs for it,
   and P picks among those as [Summaries](#summaries) describes. The peers of one cell delegate
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
   - the default `durability`: a result of another class is made in its
     cell, which keeps the copies (or none) its class asks for;
   - `effects="none"`, so running it twice is harmless.

   Everything else stays in its cell.
3. **How.** P connects to the other cell's client endpoint with the fleet
   token, creates a *shadow run* there, ships the function's code (`PutCode`)
   and submits X's spec under the shadow run. The shadow run gets the owning
   run's interpreter, working directory and import path. P then watches X's
   outcome, fetching large results with `GetObject` (a result stored in parts
   comes with its parts, and a result's buffers stored apart from it, its
   `blobs`, come too), and publishes it in its own cell as the attempt's
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
CHIMAERA_CELL=a                                   # this cell's name, as the other cells list it
CHIMAERA_FLEET="b=10.0.1.5:7710,c=10.0.2.5:7710"  # other cells' client endpoints
CHIMAERA_FLEET_TOKEN_FILE=/etc/chimaera/fleet     # the token the other cells accept
CHIMAERA_LABELS=region=eu-west                    # this cell's region: calls go to cells there first
```

Each cell's client endpoint must accept the fleet token. Its peers get it as
their cluster token, or a cell's operator gives the fleet that cell's token.
A peer without `CHIMAERA_FLEET` or a directory never delegates. A fleet needs
`CHIMAERA_CELL`: a peer without it starts outside any fleet and logs why.

## The cell directory

Instead of, or alongside, the static `CHIMAERA_FLEET` list, cells can find
each other through a directory cell:

```
CHIMAERA_FLEET_DIRECTORY=10.0.1.5:7710   # a client endpoint of the directory cell
CHIMAERA_CELL=a@10.0.0.5:7710            # this cell's name and the endpoint others reach it at
```

- **Registering:** every 2 seconds a cell's publisher renews the cell's entry (when `CHIMAERA_CELL` gives an endpoint), with a 10 s TTL, in the `fleet/cells` store namespace of the directory cell. That cell's Raft groups replicate it, so it survives the loss of a minority of the directory cell's peers.
- **Reading:** alongside the renewal, the publisher reads the namespace's changes since the revision its cell's view holds, and keeps the list in the view. It lists the namespace whole only when the view holds no revision, or the directory's change log has moved past it. A new publisher, after a failover or a restart of the whole cell, carries on from the stored revision. A cell whose entry the directory has not shown renewed for 10 s drops out.
- **A slow directory:** the publisher talks to the directory in a task of its own, and each round takes the list as that task last read it. A directory that takes longer than 2 s to answer is asked again as soon as it has answered. Each request waits up to 5 s. When one fails, what the pass read before it stays read: the next pass carries on from there, a whole listing from its next page. A directory that answers in under 5 s is read however slow it is; one that takes longer is treated as lost.
- **Load:** about one request a second per cell, however many peers it has, and never more than two at a time.
- **Losing the directory:** if the directory cell is lost or stops answering, cells keep the list they last read, and the publisher logs a warning, then a line when the directory answers again. Discovery stops until then; delegation doesn't.
- **Tested:** `test_cells_find_each_other_through_a_directory`. A finds B through the directory. Then all four of A's peers restart at once, at a new endpoint. Every peer of A comes to know B again and B knows A at its new endpoint. Over the 15 s after the restart the directory serves fewer than 3 requests a second; when every peer of both cells renewed and listed every 2 s, the same test measured 7.1. Neither cell lists the directory whole again: A's new publisher carries on from the revision in A's stored view. `test_a_cell_learns_the_fleet_through_a_directory_that_answers_slowly` puts 1.5 s before every reply the directory sends A: A registers, learns of B and keeps one publisher. Unit tests run the publisher's reads against the store's own state machine: a listing over two pages, then only changes, a cell that stops renewing, a change log trimmed past the stored revision, a directory that went back, and passes cut off by a failed request, which the next pass carries on from.

## Not in this slice

- **Global names and tenant envelopes** in the directory (only the cell registry is there).
- **Cross-cell vshard seal/adopt** (moving ownership of a range between cells).
- **Calls with dependencies**, which need fetching an object from the home
  cell by id (large arguments and `ch.put` values the call names do travel).
- **Actor placement across cells.**

## Gate for this slice

`python/tests/test_fleet.py` runs two cells of three 4-CPU peers on one
machine:
1. 36 three-second calls in a run of A, which alone can never take under 9 s
   (three rounds on its 12 CPUs). Some run in B, and the run takes under 90%
   of that. (It was 48 two-second calls under 6.5 s, which failed on a 4-core
   Linux container: there B's fresh workers started too slowly to matter.)
2. Every peer of B is killed mid-run. All 48 calls return, each exactly once,
   within A's own time plus one retry of what B held.
3. B's peers restart from their directories, and B serves a new run.

More tests run 48 calls that each read the same 4 MiB array, passed directly
or as a `ch.put` value: some run in B, which got the array once, and every call
reads all of it. Another returns three values per call, stored in parts in B
(see `docs/api.md`, multiple returns): every value comes back,
whole and one at a time. A last one returns an 8 MiB array per call, which on
Linux is linked from B's arena as a buffer of its own; every array arrives
whole (Linux run pending; it passes on macOS, where there is no arena).

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
