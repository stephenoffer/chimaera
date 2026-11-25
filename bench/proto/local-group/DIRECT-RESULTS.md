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

## Later the same day: applied, and measured on Linux

The direct path is now in the runtime (on by default). Measured on a 32-core Linux workspace (Anyscale
m6id.8xlarge) built from the current tree, empty calls, best of 3×400:

| | Chimaera | Ray |
|---|---:|---:|
| Actor round trip, direct path | 212 µs | 426 µs |
| Actor round trip, path off | 446 µs | |
| Plain task round trip | 444 µs | 805 µs |
| Plain task pipeline `f.remote(f.remote())` | 866 µs | 1,714 µs |
| Same-actor pipeline | 760 µs (899 off) | 835 µs |
| Two-actor pipeline `cons.consume.remote(prod.emit.remote())` | 1.87 ms | 0.79 ms |

The two-actor pipeline was slow because a call whose input is another call's future took the peer path and waited
for the input's record. Calls whose inputs are futures of this process now queue on their handle and go direct
once the inputs land (a failed input still goes through the peer, which raises the causal error). On this laptop
at load ~370, same binary, alternating: 2,117 / 1,923 µs p50 before, 1,197 / 1,104 µs after. The Linux number has
not been re-measured.

## Calls from inside tasks, mailbox limits (2026-11-23, applied)

- **Calls made inside tasks and actors** to other actors on the same machine go direct too. An actor's calls to
  itself stay on the peer, whose deadlock check needs to see them. Each handle now tracks its own latest call
  through the peer, so the order rule works inside calls as well as in drivers.
- **Methods with a mailbox limit** (`@ch.method(mailbox=ch.Mailbox(...))`) and handles whose class is not known
  here take the peer path, which keeps the limits.
- **Concurrent actors** promise no order, so a call through the peer no longer waits for the handle's direct calls.
- **Sockets** are found from the peer socket's directory, as executors listen there (a long home moves it to /tmp).
- **Found on the way (peer path, not fixed here):** a handle with the actor's class but no options (pickled, or
  found by key) runs a `concurrency=4` actor's calls one at a time through the peer, 2.03 s against 0.51 s for
  4 × `nap(0.5)`; the direct path avoids it because the executor uses the constructor's concurrency.

Not measured yet: the one-to-many and many-to-many rows with calls from tasks going direct (needs a release build;
the shared tree did not build for part of the day).
