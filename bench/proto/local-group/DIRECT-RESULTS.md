# Direct actor path: first measurements (2026-11-22)

These measure the build described in DIRECT-ACTOR-PATH-PLAN.md ("Status"), before it was applied to the tree. Both
runtimes ran on the same interpreter (`.venv-bench`, Python 3.12) with release builds, on one laptop with a load
average between 8 and 36.

## Round trip, one actor call at a time

3,000 sync calls after 300 warm-up, rounds alternating; Chimaera's direct path on and off (`CHIMAERA_DIRECT=0`) on
the same binary. Load 10–25.

| p50 (p99) | Direct path | Through the peer | Ray 2.60 |
|---|---:|---:|---:|
| Round 1 | 117.6 µs (467) | 197.8 µs (292) | 223.9 µs (880) |
| Round 2 | 121.9 µs (332) | 234.6 µs (1,317) | 239.2 µs (952) |
| Round 3 | 195.8 µs (2,959) | 264.7 µs (2,516) | 452.0 µs (2,562) |

Ray measured slower than in the earlier interleaved run (145 µs); only the paired comparison is claimed.

## Ray's actor microbenchmark rows

`bench/microbench/run.py --rounds 2 --sleep 3` with `TESTS_TO_RUN=actor`, Chimaera's interpreter set to
`.venv-bench`. Medians of two rounds (range); raw values in `direct-microbench-2026-11-22.json`.

| metric | Ray 2.60 | Chimaera, direct | ratio | 2026-11-21, no direct path |
|---|---:|---:|---:|---:|
| 1:1 actor calls sync | 2,222 | 3,924 | 1.77x | 0.23x |
| 1:1 actor calls async | 12,413 | 12,902 | 1.04x | 0.33x |
| 1:1 actor calls concurrent | 8,411 | 11,439 | 1.36x | 0.24x |
| 1:n actor calls async | 11,974 | 8,515 | 0.71x | 0.50x |
| n:n actor calls async | 18,519 | 8,479 | 0.46x | 0.43x |
| n:n actor calls with arg async | 8,356 | 8,054 | 0.96x | 0.83x |
| 1:1 async-actor calls sync | 2,265 | 1,974 | 0.87x | 0.34x |
| 1:1 async-actor calls async | 7,039 | 6,883 | 0.98x | 0.46x |
| 1:1 async-actor calls with args async | 4,578 | 4,668 | 1.02x | 0.63x |
| 1:n async-actor calls async | 11,096 | 4,242 | 0.38x | 0.24x |
| n:n async-actor calls async | 16,768 | 3,575 | 0.21x | 0.30x |
| client: 1:1 actor calls sync | 1,069 | 772 | 0.72x | 0.60x |
| client: 1:1 actor calls async | 2,071 | 10,548 | 5.09x | |
| client: 1:1 actor calls concurrent | 2,548 | 9,121 | 3.58x | |

The 2026-11-21 column is from `bench/microbench/RESULTS.md` (another day, another load), so compare it loosely.

## What is still short, and why

- **n:n rows:** their clients are tasks, and calls made inside a call still take the peer path (`eligible` requires
  a driver). Letting executors call local actors directly is the next step; it needs the ordering check to work
  without the driver's `_unfinished` table.
- **1:n rows:** one driver feeding many actors. The driver's own work per call (building the call, the in-flight
  notice and the record, two coalesced submissions) is the likely limit; not profiled yet.
- **Async-actor 1:n / n:n:** the same two causes, with Ray's 1,000-wide concurrency.
