# Call-path prototype vs Ray (2026-11-21)

This prototype tests one claim from architecture.md §3 (D2): with the call journal replicated to two backups off
the call's path, an actor call can be several times faster than Ray's. It is not Chimaera. It has no `@ch.remote`
layer, futures, placement, admission, accounting or failover. Read its numbers as the cost of the transport plus the
journal, which is the floor the real path is built on.

## What runs

- **`hotpath agent`** (Rust, standard library only):
  - Appends each call to an in-memory journal and forwards it at once to the actor's executor.
  - Appends each result and forwards it to the driver.
  - A replicator thread sends the journal to two `hotpath backup` processes every 1 ms or 64 KiB and waits for both
    acks. Each backup applies the batch to a table of open calls.
  - An `external` call waits until its record is safe on both backups before it runs.
- **Executors and drivers** are Python, one process each:
  - Calls and results are pickled, and each call is its own socket write.
  - Small results come back inline.
  - The driver's waiting thread reads the socket itself; there is no dispatcher thread.
- **The benchmarks** use Ray's `timeit` method (1 s warm-up, then the mean of four 2 s windows) with a 3 s pause.
  They are the rows of `ray/_private/ray_perf.py` that measure the actor path:
  - `1:1 actor calls sync` and `async`.
  - `n:n actor calls async`: 4 clients, each making 5,000 calls round-robin over 6 actors.
  - Round-trip latency over 20,000 synchronous calls.
- **`ray_side.py`** runs the same rows through Ray 2.60.0 with the same code as `ray_perf.py`.

The setup:
- **Machine:** M3 Pro laptop, 12 cores, macOS 14.5, shared with other sessions. The 1-minute load average was
  8–20 during counted runs.
- **Python:** 3.12.13 for both.
- **Runs:** three of each, alternating Ray and the prototype. Cells show the median, with the range in parentheses.
- **Prototype source:** the runs above predate the unsafe-window bound; see "With the bound" below.
- **Data:** every value is in `results-2026-11-21.json`.

## Results

| Benchmark | Ray 2.60 | Prototype | Prototype ÷ Ray |
|---|---:|---:|---:|
| 1:1 actor calls sync | 5,839/s (5,690–5,980) | 50,390/s (32,978–59,217) | 8.6× |
| 1:1 actor calls async | 19,143/s (18,697–21,075) | 592,910/s (578,890–658,478) | 31× |
| n:n actor calls async | 26,436/s (24,118–28,672) | 1,629,758/s (1,616,285–1,737,237) | 62× |
| 1:1 actor calls sync, `effects="external"` | — | 27,477/s (27,139–29,623) | — |

| Round trip | p50 | p99 |
|---|---:|---:|
| Ray | 167 µs (165–181) | 558 µs (525–936) |
| Prototype, through the agent | 17.8 µs (14.5–19.0) | 27.9 µs (25.2–35.0) |
| Prototype, `effects="external"` | 32.6 µs (32.1–32.7) | 92.6 µs (42.0–93.6) |
| Driver straight to the executor, no agent | 9.2 µs (7.7–12.5) | 59.5 µs (17.6–66.9) |

The journal over all three runs:
- **Calls:** 65 million.
- **Size:** 98 bytes per call per backup. The capacity model assumes 256.
- **Batches:** about 300,000 per run.
- **Unsafe window** (a batch's first record until both acks):
  - p50: 24 µs.
  - p99: 1.5 ms.
  - Worst case: 198–258 ms, during the n:n burst at 1.6M calls/s.

## What this shows

- **The journal costs the call almost nothing.** The replicator never sits between a call and its result. Journaling
  to two backups did not stop the prototype from beating Ray by 8.6× on synchronous calls and 31–62× with many calls
  in flight.
- **The agent hop is cheap.** Going through the agent instead of straight to the executor adds about 9 µs at p50.
  The design keeps the agent in the path: it owns the call and its journal. Cutting the hop is not needed to meet
  the 150 µs target.
- **Waiting for safety costs one extra round trip.** `effects="external"` adds about 15 µs at p50 on one machine.
  Across a network it adds one in-cell round trip, which is the 50–200 µs in the design.
- **The headroom is for the layers left out.** Chimaera's driver spends 21–25 µs per `.remote()` today, measured in
  bench/microbench. The executor's thread handoffs, accounting and futures add more. A full call can grow several
  times larger than the prototype's round trip and still come in under Ray's 167 µs.
- **The design needs a bound on the unsafe window.** Nothing slows submissions when the backups fall behind, so a
  1.6M calls/s burst stretched the worst-case window to about 250 ms. The window is how much replayable work a
  successor must resubmit after owner loss. architecture.md D2 now bounds unsafe journal bytes per owner, with
  submissions waiting at the bound.

## With the bound

The agent now makes submissions wait while buffered plus in-flight journal bytes reach 8 MiB (`MAX_UNSAFE`,
src/main.rs sha256 `90b499ad1ec5b62b…`). Two more runs at load average 6–8:

| | Run 1 | Run 2 |
|---|---:|---:|
| 1:1 actor calls sync | 52,016/s | 59,422/s |
| 1:1 actor calls async | 280,948/s | 619,128/s |
| n:n actor calls async | 1,856,653/s | 1,888,169/s |
| Round trip p50 / p99 | 18.3 / 27.0 µs | 14.9 / 25.3 µs |
| Unsafe window p99 / worst | 1.8 / 112 ms | 1.5 / 88 ms |

- **The worst case fell from 198–258 ms to 88–112 ms, and throughput did not drop.**
- **The window matches the bound.** It is about twice the bound divided by what the backups apply per second:
  - One batch waits behind the one in flight.
  - The backups apply about 180 MB/s here.
  - A smaller bound gives a proportionally smaller window.
- **The async outlier.** Run 1's 281k/s async rate is about half of every other run. That is consistent with the
  noise on this shared machine, not with the bound: the n:n row, which hits the bound hardest, was faster than
  before.

## Not covered

- Linux, more than one machine, and real network round trips to backups.
- Large arguments.
- Owner failover (D3), placement, and the real shard state machine. Backups apply into a hash map.

## Reproduce

```
CARGO_TARGET_DIR=/tmp/hp cargo build --release
python hotpath.py bench --bin /tmp/hp/release/hotpath --json proto.json
RAY_ADDRESS=local python ray_side.py --json ray.json
```
