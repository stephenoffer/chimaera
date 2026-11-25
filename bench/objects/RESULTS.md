# Objects: Chimaera vs Ray 2.59 (2026-11-21)

Measured on Anyscale. Reproduce with `bench/objects/run.py` (one node) and
`bench/objects/cluster.py` (peers on every node). The design behind these
numbers is in [docs/objects.md](../../docs/objects.md).

## Setup

- **Machines:** 3× m6id.8xlarge (32 vCPU, 123 GiB, local NVMe, 12.5 Gbit/s),
  Linux 6.8, Python 3.11, NumPy 2.1.3, torch 2.15 (CPU), pyarrow 23.
- **Ray:** 2.59.0 from Anyscale's image, as Anyscale runs it: the
  workspace's own Ray cluster, with the object store and spilling Anyscale
  configures.
- **"Ray on huge pages":** a private Ray head whose plasma directory is the
  same huge-page tmpfs Chimaera uses (`ray start --plasma-directory`), for an
  apples-to-apples check.
- **Chimaera:** this tree. Objects live either on `/dev/shm` (4 KiB pages,
  the default on these hosts) or on a `huge=within_size` tmpfs mounted with
  one command (see docs/objects.md), which Chimaera detects by itself.
- **Method:**
  - Medians over repetitions: up to 200 repetitions for 1 MB and under, 4
    at 1 GB.
  - `get` is timed inside the consumer process. `touch` (reading every
    page) is reported separately.
  - `return` and `pipeline` subtract the same call carrying `None`, so they
    measure the object path, not the call path.
  - The speedup is Ray's time over Chimaera's.
- **Data types:**
  - bytes, NumPy float32, torch float32, a pyarrow table (int, float and
    string columns), a pandas DataFrame (same columns, strings as objects);
  - a dict of str→float, a list of ints, a str;
  - a nested dict holding 8 arrays.
- **Sizes:** 1 KB to 1 GB. The Python-object types stop at 256 MB.
- **Raw rows:** `results/*.jsonl`, one file per table below. Render any
  of them with `python bench/objects/run.py --report <dir>`, after copying
  the file to `<dir>/results.jsonl`.
- **Which code:** the rows for bytes, torch and Arrow at 16 MB–1 GB were
  rerun after the pre-zeroed reserve was capped at 64 MiB files (it had
  distorted the memory pool's accounting); the other rows come from the same
  build without that cap, which does not affect them (no allocation is timed
  there). The `bytes` rows for `put`, put+get on huge pages and many 1 MB
  objects were rerun on 2026-11-22 with Python's raw allocator routed to the
  arena (`results/raw-arena-*.jsonl`); the Ray rows are the earlier ones,
  from the same cluster and Ray build. The bench makes each value after the
  runtime starts, so its large `bytes` live in the arena, as NumPy arrays
  do in the NumPy rows.

## Where it is 50x or more, and where it cannot be

The design point (objects as files in shared memory, NumPy arrays born
there, no hashing, warm pages, replicas) changes the cost of the object
plane, not the cost of turning Python objects into bytes. The result:

| what | best measured speedup | why |
|---|---:|---|
| `put` of a NumPy array, 256 MB–1 GB | **134–1,010x** | zero copy: a hard link, constant ~40 µs at any size |
| `put` of nested arrays, 256 MB–1 GB | **49–168x** | same, for each array inside |
| put, then get in another process, NumPy 1 GB | **152–240x** | the reader maps the same pages |
| `return` from a process, NumPy 1 GB | **53–138x** | the result is linked, not copied |
| torch tensors, 16 MB–1 GB on huge pages (put, get, return, pipeline) | **10–64x** | Ray pickles tensors through a slow path |
| `get` of a torch tensor, 256 MB–1 GB | **2,000–8,000x** | Ray copies on get; Chimaera maps |
| `put` of Python `bytes`, 256 MB–1 GB | **104–2,368x** | large `bytes` live in the arena too (Python's raw allocator), so they are linked |
| `put` of a 1 KB value | 11–30x | inline in the handle |
| `get` of a 1–64 KB value on another node | **17–137x** | inline in the handle, no transfer at all |
| first get of a spilled 256 MB object | **360–1,080x** | restore is a mapping of the disk file |
| writing 3x the memory limit (spill): 256 MB objects, or any size on huge pages | 1.2–2.2x | the spiller keeps memory under its line, so writes seldom wait for the disk |
| recovery when a node dies holding a value | **90–70,000x**, and values Ray loses survive | replicas on the next peer (Ray: 12–14 s of detection, then a re-run) |
| `bytes` get, str up to 16 MB, dicts, lists, pandas, 1 MB+ | 0.5–1.7x | bound by pickling or by the one copy both must make |
| `str` put of 256 MB | 1.9x (4 KiB pages), 3.8x (huge pages) | encoded to UTF-8 once into an arena block, then linked |
| Arrow tables, 256 MB–1 GB | 6–9x on huge pages, about 1.2x on 4 KiB pages | Arrow buffers are copied (no zero-copy allocator for Arrow) |
| one object to one other node, 1 MB–1 GB | 1.1–4x | network-bound: both near the NIC's rate; torch avoids Ray's slow path |

Geometric means over every type and size, same host:

| scenario | `/dev/shm` | huge pages |
|---|---:|---:|
| put | 4.2x | 5.6x |
| get (in another process) | 1.9x | 2.3x |
| return | 1.7x | 1.7x |
| pipeline (process → process) | 1.2x | 3.0x |
| many small objects | 2.3x | 2.4x |

The means are far from 50x. Half the types are pickle-bound or
copy-bound, where neither runtime can do much better than the other. The
small-size `return` and `pipeline` rows are dominated by the call path, not
objects; there Chimaera's base call is 1.9 ms against Ray's 0.37 ms.

## What Chimaera is not faster for, and why

Re-measured on 2026-11-24 at e012f2b on the same workspace, both runtimes
the same day (Ray 2.59 as Anyscale runs it), two runs each: `/dev/shm` with
4 KiB pages, medians. Every case where Chimaera is slower than Ray, or
barely faster:

| data, pattern | vs Ray | why | what helps |
|---|---:|---|---|
| writing 3x the memory limit (spill), 16 MB objects on `/dev/shm` | 0.82–0.89x (5.5–5.8 s against 4.8–4.9 s) | Each new array is allocated in shared memory, on pages the kernel allocates and zeroes, while Ray reuses its heap's pages for each new array and copies it into its store on several threads: making and filling each 16 MB array took 2.8 ms against Ray's 1.2 ms. Measured on 2026-11-24 (4 GiB of 64 MiB objects into `/dev/shm`, two runs): fresh tmpfs pages 2.6 s (1.7 GB/s), a heap array copied into warm shared memory as Ray does 1.5 s (2.9 GB/s), and pages already in a tmpfs file, mapped with `MAP_POPULATE`, 0.95 s (4.5 GB/s). Those faults cost the most on `/dev/shm`, with shared-memory huge pages off (`shmem_enabled=never`, the usual default): on a huge-page tmpfs the same writes are 1.4x faster than Ray's, and 256 MB objects are faster on `/dev/shm` too. 8 or 16 spill threads did no better than 4. | A huge-page tmpfs (`mount -t tmpfs -o huge=within_size`; the peer uses one it finds, and nodes Chimaera provisions mount one), a larger memory limit, or reusing arrays. Not built: handing a spilled object's pages to the next array, which would give it pages already faulted in, but is safe only once no process still maps the old object, and only within one run. |
| first touch of a value's pages just after `get` | pandas 16 MiB 0.5x (693 µs against 312), Arrow 1 MiB 0.6x (72–82 µs against 45) | A value read in place is a mapping of its file, whose pages fault in when first touched. `get` and the touch together are faster than Ray's: pandas 16 MiB 37.3 ms against 41.4–42.9, Arrow 1 MiB 166–179 µs against 200. | - |
| pandas with object columns, get of 1 MiB | 1.0x (1.73–1.75 ms against 1.66–1.68) | Unpickling the frame's structure is most of the time in both (put is 2.0–2.1x, return and pipeline 1.3–1.5x). | NumPy or Arrow columns, or `category` dtype. |
| Arrow tables, `return` and `pipeline` of 1 MiB | 0.9–1.4x | Arrow buffers are copied once into shared memory: the zero-copy allocator covers NumPy. From 16 MiB 1.2–2.1x; put and get are 1.2–1.9x. | - |
| 1 MiB `str` | 0.9–1.7x over four runs (put 425–541 µs against 486–749) | Encoding to and decoding from UTF-8 is most of the time in both, and Ray's own time varies most. From 16 MiB, put, return and pipeline are 1.9–2.4x. | - |
| `str` get of 16 MiB; many 1 MB values read in one call | 1.1x (2.85–2.91 ms against 3.02–3.09; 475–482 µs against 503–514 each) | Decoding the UTF-8 into a new `str`, and one copy per value, are the cost in both. | Batch small values into one array. |

Fixed on 2026-11-24 (in e012f2b): small results through `return` and
`pipeline`, and 1 MiB `str`.

- With the direct actor path (`CHIMAERA_DIRECT`, docs/configuration.md), a
  driver's call to a one-at-a-time actor on its local peer goes straight to
  the actor's worker, which answers it, and the peer records the call just
  after. A call whose inputs are futures of the same process now waits for
  them and then goes the same way; the peer no longer steps over deleted
  keys looking for a finished call's dependents, and no longer sends the
  caller an outcome it already has.
- A large `str` is encoded on the heap from 1 to 8 MiB and written with one
  copy, not into a new arena block whose pages fault in one at a time; and
  a process takes pre-zeroed files from the page reserve out of one listing
  of it, not a listing (2,000 files here) for each.

| at e012f2b, two runs | `put` | `return` | `pipeline` |
|---|---:|---:|---:|
| empty call | - | 219–227 µs against 371–391 (**1.7x**) | 423–426 µs against 801–812 (**1.9x**) |
| `bytes`, 1 MiB | **7.0–7.1x** | **2.6–2.9x** | **2.1–2.5x** |
| list, 1 MiB | **2.8–2.9x** | **1.8x** | **1.6–1.9x** |
| `str`, 1 MiB | 0.9–1.7x (above) | 1.0–1.5x | 1.1–1.7x |

Before, at e03a965: the empty `pipeline` 0.4x (1.84–1.87 ms against 0.75–0.81)
and a 1 MiB `str` through `return` 0.9–1.0x.

Fixed on 2026-11-22: plain dicts and lists. A top-level dict, list or tuple
of at least 1024 items made of `str`, `bytes`, `int` (to 64 bits), `float`,
`bool` and `None`, with no container reachable twice, is encoded by a native
codec (`crates/chimaera-py/src/plain.rs`) in one pass that checks and writes
at once, and decoded natively; anything else is pickled as before. It skips
the memo-free pickle's separate safety scan, which was a fifth of the time:

| same day, 4 KiB pages | Ray | Chimaera before | Chimaera now |
|---|---:|---:|---:|
| dict put, 1 / 16 / 256 MiB | 0.94 / 11.6 / 268 ms | 1.49 / 16.3 / 274 ms (0.63–0.98x) | 0.62 / 9.3 / 174 ms (**1.5x / 1.2x / 1.5x**) |
| list put, 1 / 16 / 256 MiB | 1.15 / 15.6 / 287 ms | 0.61 / 12.5 / 204 ms | 0.35 / 7.5 / 120 ms (**3.3x / 2.1x / 2.4x**) |
| dict get, 1 / 16 / 256 MiB | 4.4 / 96 / 2,397 ms | 2.0 / 50 / 1,524 ms | 1.8 / 49 / 1,459 ms (**2.4x / 2.0x / 1.6x**) |
| list pipeline, 256 MiB | 659 ms | 546 ms | 457 ms (**1.44x**) |
| dict return, 16 / 256 MiB | 139 / 3,317 ms | 83 / 1,978 ms | 49 / 1,600 ms (**2.9x / 2.1x**) |

The same codec takes object arrays of plain values, such as a pandas
column of strings, with each repeated string object written once and read
back as one object, as pickle's memo would (a low-cardinality column relies
on that for its memory):

| same day, 4 KiB pages | Ray | Chimaera before | Chimaera now |
|---|---:|---:|---:|
| pandas put, 16 / 256 MiB | 126 / 3,511 ms | 103 / 3,114 ms | 90 / 2,023 ms (**1.4x / 1.7x**) |
| pandas get, 16 / 256 MiB | 39 / 836 ms | 47 / 802 ms (0.83x / 1.04x) | 36 / 740 ms (**1.08x / 1.13x**) |
| pandas return, 16 / 256 MiB | 193 / 4,734 ms | 167 / 4,323 ms | 139 / 2,946 ms (**1.4x / 1.6x**) |
| pandas pipeline, 16 / 256 MiB | 192 / 4,715 ms | 158 / 4,218 ms | 143 / 2,932 ms (**1.3x / 1.6x**) |

Fixed on 2026-11-22: large `bytes` and `str`. New arena blocks of 8 MiB or
more are now populated on several threads as they are handed out, so the
code that fills them (a `str` encode, a read into a new `bytes`, a NumPy
copy) no longer faults their tmpfs pages in one at a time on its own thread:

| same day, 4 KiB pages | Ray | Chimaera before | Chimaera now |
|---|---:|---:|---:|
| `bytes` get, 256 MiB | 140 ms | 178 ms (0.79x) | 77 ms (**1.81x**) |
| `bytes` return, 256 MiB | 157 ms | 207 ms (0.76x) | 113 ms (**1.39x**) |
| `bytes` pipeline, 256 MiB | 172 ms | 213 ms (0.81x) | 114 ms (**1.51x**) |
| `str` put, 16 / 256 MiB | 12.2 / 303 ms | 11.9 / 174 ms | 6.4 / 87 ms (**1.9x / 3.5x**) |
| `str` return, 16 / 256 MiB | 16.6 / 489 ms | 17.1 / 365 ms | 10.3 / 201 ms (**1.6x / 2.4x**) |
| pandas put, return, pipeline, 256 MiB | 3.5–4.7 s | 3.6–4.9 s (0.97–1.0x) | 3.2–4.4 s (**1.08–1.11x**) |

A parallel read of a large `bytes` value into heap memory with huge pages
was tried as well and dropped: no faster, and at 1 GiB slower (453 → 655 ms),
most likely from the kernel compacting memory for the huge pages.

Everything else, from 1 KB values to 1 GB arrays and tensors, on one host or
across hosts, and spilled reads and recovery, is faster than Ray. Often it is
far faster. The per-type tables below were measured before the populate
change; for `bytes` and `str` of 16 MiB and up, the table above is current.

## Same host: put (driver)

`/dev/shm` (4 KiB pages), Ray as Anyscale runs it:

| type | 1K | 64K | 1M | 16M | 256M | 1G |
|---|---:|---:|---:|---:|---:|---:|
| numpy | 268 µs → 20 µs (**13x**) | 266 µs → 25 µs (**11x**) | 392 µs → 38 µs (**10x**) | 609 µs → 39 µs (**16x**) | 5.27 ms → 39 µs (**134x**) | 19.2 ms → 39 µs (**493x**) |
| torch | 348 µs → 31 µs (**11x**) | 368 µs → 44 µs (8.3x) | 604 µs → 399 µs (1.5x) | 26.2 ms → 2.57 ms (**10x**) | 288 ms → 48 ms (6.0x) | 1.16 s → 234 ms (5.0x) |
| nested | 314 µs → 39 µs (8.1x) | 352 µs → 47 µs (7.5x) | 422 µs → 645 µs (0.7x) | 1.90 ms → 137 µs (**14x**) | 6.47 ms → 132 µs (**49x**) | 21.0 ms → 137 µs (**154x**) |
| arrow | 417 µs → 61 µs (6.8x) | 440 µs → 70 µs (6.3x) | 647 µs → 555 µs (1.2x) | 2.71 ms → 3.43 ms (0.8x) | 35.7 ms → 30.8 ms (1.2x) | 140 ms → 115 ms (1.2x) |
| bytes | 167 µs → 6.4 µs (**26x**) | 182 µs → 25 µs (7.3x) | 348 µs → 49 µs (7.1x) | 1.27 ms → 51 µs (**25x**) | 14.9 ms → 53 µs (**281x**) | 131 ms → 55 µs (**2,368x**) |
| pydict | 161 µs → 7.7 µs (**21x**) | 254 µs → 53 µs (4.8x) | 938 µs → 942 µs (1.0x) | 11.0 ms → 13.8 ms (0.8x) | 264 ms → 271 ms (1.0x) | - |
| pylist | 169 µs → 7.0 µs (**24x**) | 270 µs → 46 µs (5.9x) | 1.16 ms → 608 µs (1.9x) | 15.0 ms → 10.4 ms (1.4x) | 285 ms → 201 ms (1.4x) | - |
| str | 191 µs → 6.4 µs (**30x**) | 195 µs → 15 µs (**13x**) | 502 µs → 609 µs (0.8x) | 11.3 ms → 6.80 ms (1.7x) | 292 ms → 349 ms (0.8x) | - |
| pandas | 402 µs → 60 µs (6.7x) | 562 µs → 183 µs (3.1x) | 3.08 ms → 3.19 ms (1.0x) | 108 ms → 122 ms (0.9x) | 3.15 s → 3.25 s (1.0x) | - |

## Both runtimes on the same huge-page tmpfs

Ray's plasma store and Chimaera's objects share one `huge=within_size`
tmpfs, so neither runtime has the page-size advantage.

| put | 1M | 16M | 256M | 1G |
|---|---:|---:|---:|---:|
| numpy | 376 µs → 36 µs (**10x**) | 1.81 ms → 36 µs (**50x**) | 11.0 ms → 38 µs (**288x**) | 41.2 ms → 41 µs (**1,010x**) |
| torch | 641 µs → 369 µs (1.7x) | 27.7 ms → 608 µs (**46x**) | 300 ms → 10.6 ms (**28x**) | 1.21 s → 38.2 ms (**32x**) |
| nested | 434 µs → 358 µs (1.2x) | 1.89 ms → 121 µs (**16x**) | 6.72 ms → 126 µs (**53x**) | 21.3 ms → 127 µs (**167x**) |
| arrow | 651 µs → 430 µs (1.5x) | 2.74 ms → 1.85 ms (1.5x) | 37.2 ms → 5.91 ms (6.3x) | 154 ms → 19.6 ms (7.9x) |
| bytes | 318 µs → 49 µs (6.5x) | 543 µs → 51 µs (**11x**) | 5.40 ms → 52 µs (**104x**) | 19.8 ms → 54 µs (**363x**) |

| put, then get in another process | 1M | 16M | 256M | 1G |
|---|---:|---:|---:|---:|
| numpy | 468 µs → 128 µs (3.7x) | 711 µs → 113 µs (6.3x) | 5.43 ms → 127 µs (**43x**) | 21.0 ms → 138 µs (**152x**) |
| torch | 1.01 ms → 465 µs (2.2x) | 19.6 ms → 726 µs (**27x**) | 576 ms → 10.1 ms (**57x**) | 2.27 s → 35.6 ms (**64x**) |
| nested | 528 µs → 473 µs (1.1x) | 2.08 ms → 290 µs (7.2x) | 6.76 ms → 296 µs (**23x**) | 21.6 ms → 299 µs (**72x**) |
| arrow | 1.05 ms → 576 µs (1.8x) | 4.11 ms → 1.93 ms (2.1x) | 50.4 ms → 5.68 ms (8.9x) | 148 ms → 18.7 ms (7.9x) |
| bytes | 476 µs → 219 µs (2.2x) | 2.04 ms → 1.04 ms (2.0x) | 136 ms → 79.3 ms (1.7x) | 542 ms → 310 ms (1.7x) |

| return (producer process → driver) | 1M | 16M | 256M | 1G |
|---|---:|---:|---:|---:|
| numpy | 521 µs → 124 µs (4.2x) | 758 µs → 167 µs (4.5x) | 6.05 ms → 267 µs (**23x**) | 20.7 ms → 389 µs (**53x**) |
| torch | 1.10 ms → 526 µs (2.1x) | 33.1 ms → 1.90 ms (**17x**) | 601 ms → 12.3 ms (**49x**) | 2.35 s → 42.6 ms (**55x**) |
| nested | 604 µs → 587 µs (1.0x) | 2.13 ms → 338 µs (6.3x) | 6.91 ms → 392 µs (**18x**) | 21.6 ms → 437 µs (**49x**) |
| bytes | 500 µs → 656 µs (0.8x) | 2.06 ms → 4.07 ms (0.5x) | 145 ms → 172 ms (0.8x) | 591 ms → 645 ms (0.9x) |

| pipeline (process → process) | 1M | 16M | 256M | 1G |
|---|---:|---:|---:|---:|
| numpy | 653 µs → 138 µs (4.7x) | 933 µs → 127 µs (7.3x) | 7.10 ms → 1.02 ms (7.0x) | 23.3 ms → 3.50 ms (6.7x) |
| torch | 1.58 ms → 841 µs (1.9x) | 23.9 ms → 2.41 ms (9.9x) | 586 ms → 14.4 ms (**41x**) | 2.37 s → 49.3 ms (**48x**) |
| nested | 721 µs → 683 µs (1.1x) | 2.39 ms → 418 µs (5.7x) | 8.06 ms → 1.30 ms (6.2x) | 25.1 ms → 3.74 ms (6.7x) |
| bytes | 654 µs → 686 µs (1.0x) | 2.26 ms → 3.96 ms (0.6x) | 149 ms → 165 ms (0.9x) | 593 ms → 664 ms (0.9x) |

The pipeline consumer reads every page of the value. That read is the
same work in both runtimes, which is why NumPy pipelines stay near 7x while
the put and return paths reach hundreds.

## Many small objects (10,000 per size; 2,048 at 1 MB)

| size | put per object | get per object (batched, in another process) |
|---|---:|---:|
| 1 KB | 149 µs → 8.0 µs (**19x**) | 15.5 µs → 5.8 µs (2.7x) |
| 64 KB | 175 µs → 53 µs (3.3x) | 45.9 µs → 38.6 µs (1.2x) |
| 1 MB | 502 µs → 41 µs (**12x**) | 515 µs → 522 µs (1.0x) |

## Spill and restore

Measured on 2026-11-24 on one workspace node, both runtimes in one
session, two runs each (the ranges below). Each runtime gets 8 GiB of
object memory: Ray a local cluster of its own started with
`object_store_memory`, Chimaera `CHIMAERA_MEMORY_POOL`. On Anyscale, unset
`RAY_OVERRIDE_RESOURCES` for such a cluster: it sets the node's 41 GB as
the object store, Ray then never spills in this test, and its restore looks
7 to 50 times faster. Each runtime writes 24 GiB of distinct NumPy objects,
then reads every object back in another process and checks it. Both spill
to the same NVMe disk. Ray's log shows 49 GB spilled in every run: the
write, then the restore making room.

Chimaera's spiller goes on until memory is under its line, measuring after
each round. When it ran one round a second, writers reached the hard cap
and each put then wrote its object to disk before returning: the 16 MB and
256 MB writes on `/dev/shm` took 7.3–7.5 s and 9.3 s, against 5.9 s for
both with it (one build of each from the same tree, two runs).

Objects on `/dev/shm` (4 KiB pages, the default on these hosts):

| object size | measure | Ray | Chimaera | speedup |
|---|---|---:|---:|---:|
| 16 MB | write 24 GiB | 4.79–4.86 s | 5.48–5.82 s | 0.82–0.89x |
| 16 MB | restore and check 24 GiB | 7.80–8.12 s | 1.46–1.49 s | **5.3–5.4x** |
| 256 MB | write 24 GiB | 7.86–7.92 s | 5.86–6.38 s | 1.23–1.35x |
| 256 MB | restore and check 24 GiB | 3.35–3.83 s | 1.26 s | **2.7–3.0x** |

Both on a `huge=within_size` tmpfs (Ray's plasma directory there, and
Chimaera's objects, which it finds by itself):

| object size | measure | Ray | Chimaera | speedup |
|---|---|---:|---:|---:|
| 16 MB | write 24 GiB | 4.50–4.53 s | 3.22–3.29 s | 1.38–1.40x |
| 16 MB | restore and check 24 GiB | 4.59–4.63 s | 1.22–1.26 s | **3.6–3.8x** |
| 256 MB | write 24 GiB | 8.10–8.18 s | 3.68–5.56 s | 1.47–2.20x |
| 256 MB | restore and check 24 GiB | 3.58–4.28 s | 0.95 s | **3.8–4.5x** |

The first get of the oldest (spilled) object, timed on its own in that
day's earlier runs (the spiller does not change reads): 0.10–0.28 ms for
Chimaera at both sizes, against Ray's 0.19–0.23 ms at 16 MB and 45–118 ms
at 256 MB. In every run both runtimes read back all 1,536 and all 96
objects correctly. Chimaera still writes 16 MB objects on `/dev/shm`
slower; "What Chimaera is not faster for" above says why.

Spilling to S3 (`CHIMAERA_SPILL_URL`, 2026-11-24, one workspace node, a 64
MiB pool): 2,000 `put`s of 64 KiB NumPy arrays went up as 2 packs, not 2,000
objects, like Ray's fusing up to `min_spilling_size`. Every value read back
correct, each with one ranged read of its pack. Freeing the values while the
run went on deleted both packs, and so did ending the run with them still
held: nothing was left in the bucket or in the peer's restore cache.

## CUDA tensors across machines

Two g5.2xlarge nodes (one A10G each, plain TCP: no EFA), a CUDA tensor put
on one and read by a call granted a GPU on the other (best of 3, including
the call):

| read on the other machine's GPU | host copy (the default here) | NCCL (`CHIMAERA_CUDA_NET=1`) | NIXL (`CHIMAERA_CUDA_NET=nixl`, 2026-11-24) |
|---|---:|---:|---:|
| 256 MiB | 345 ms | 470 ms | 824 ms |
| 1 GiB | 1,184 ms | 1,852 ms | 3,032 ms |

The NIXL run used UCX over the same TCP network (no EFA here); the same day
NCCL measured 505 ms and 1,873 ms and the host copy 323 ms and 1,233 ms.

Both arrive on the reader's granted GPU (`cuda:0`), and a reader without a
GPU gets a CPU tensor. On TCP, NCCL's socket transport is the slower one
whatever its socket count (4 to 16 measured alike), and NIXL slower still,
so both are off unless `CHIMAERA_CUDA_NET` asks for one. On an RDMA network
(InfiniBand, EFA) they skip both the host copy's fetch and its copy up to
the GPU; that was not measured here.

## Recovery: a node dies holding values

Re-measured on 2026-11-24, both on one laptop (macOS; Ray 2.60 with
`cluster_utils`, Chimaera at 28c0965 as three peers), two runs each. A
1 MiB value is made on node B, or by an actor, then that node or actor is
killed and the driver reads the value:

| value | Ray | Chimaera | Chimaera, no replicas |
|---|---|---|---|
| task result on the killed node | rebuilt from lineage after 13.3–16.5 s | **read in 0.6–4.3 ms** | **rebuilt in 26–27 ms** |
| `put` made by a task on the killed node | `OwnerDiedError` after 1.0 s: lost | **read in 0.3–3.4 ms** | `DataLost` |
| an actor's result on the killed node | `ObjectReconstructionFailedError`: lost | **read in 0.2–3.7 ms** | `DataLost` |
| `put` owned by an actor that was killed (its node lives) | `OwnerDiedError` after 1.0 s: lost | **read in 0.7–4.2 ms** | **read in 6.7–7.3 ms** (the value is its run's, not the actor's) |

On Linux, at e012f2b, the same four read back in 0.8–3.2 ms with replicas,
and without them the task result was rebuilt in 37.6 ms.

The measurements below are from 2026-11-21.

Three nodes on one host (Ray `cluster_utils`; three Chimaera peers). A value
is made on node B, B is killed 3 s later, and the driver gets the value.
The 3 s mattered for replicas when this was measured: Chimaera then copied
each object to the next peer about a second after it was written (now at
once, see below). A task result lost before its copy is rebuilt from
lineage (the second table).

| value | Ray | Chimaera |
|---|---|---|
| task result, 1 MB | rebuilt from lineage after 13.7 s | 0.22–3.7 ms (replica) |
| task result, 256 MB | rebuilt after 12.4 s | 0.18–143 ms |
| `put` from a task on B, 1 MB | `OwnerDiedError` after 12.1 s: lost | 0.19–2.6 ms |
| `put` from a task on B, 256 MB | `OwnerDiedError` after 12.1 s: lost | 0.17–139 ms |

Ray's time is the GCS noticing the node is gone. Chimaera's reader asks the
dead peer and is refused at once. The low end of each range is a replica that
happens to be on the reader's own peer, so the read is a mapping. The high
end is two separate runs where the replica sat on the third peer and had to
be fetched (256 MB in about 140 ms). Either way it is 90x to 70,000x faster
than Ray's rebuild, and Ray cannot recover the `put` values at all.

With no replica at all (`CHIMAERA_OBJECT_REPLICAS=0`, and the holder killed
the moment the call finished, before any copy could be made), the driver's
read runs the call again on its own peer, under the same attempt, so the
value comes back under the ids it was read by:

| task result, holder killed at once, no replicas | Ray (lineage, above) | Chimaera (lineage) |
|---|---:|---:|
| `bytes`, 1 MiB | 13.7 s | 33.7 ms |
| NumPy, 64 MiB | - | 47.5 ms |
| NumPy, 512 MiB | - | 147 ms |

The time includes running the call again. A `put` value lost before its
replica exists stays lost (`DataLost`, which says why).
Since 2026-11-22 each object is copied as soon as it is written
(`CHIMAERA_REPLICA_DELAY_MS`, now 0 by default; it was a fixed 1 s), at the
cost of a transfer even for objects a reader on another peer would have
copied anyway. With the holder of a 64 MiB put killed 0.3 s after the put,
the value was lost in 2 of 2 runs with the 1 s delay and read back from its
replica in 2 of 2 runs at 0.

## Across nodes

One Chimaera peer per node on the same 3 machines as Ray's cluster. The
driver puts a value on the head node; a consumer on another node gets it
("get"), or a producer on the head hands it to a consumer on another node
("pipeline").

| get | 1K | 64K | 1M | 16M | 256M | 1G |
|---|---:|---:|---:|---:|---:|---:|
| numpy | 1.07 ms → 16.6 µs (**64x**) | 1.11 ms → 21.1 µs (**53x**) | 2.28 → 2.04 ms (1.1x) | 19.5 → 16.6 ms (1.2x) | 223 → 185 ms (1.2x) | 880 → 706 ms (1.2x) |
| bytes | 1.13 ms → 8.3 µs (**137x**) | 1.06 ms → 15.1 µs (**70x**) | 2.39 → 1.88 ms (1.3x) | 20.7 → 19.0 ms (1.1x) | 352 → 326 ms (1.1x) | 1.39 → 1.27 s (1.1x) |
| torch | 1.09 ms → 23 µs (**47x**) | 1.43 ms → 33 µs (**44x**) | 2.62 → 1.72 ms (1.5x) | 22.8 → 17.1 ms (1.3x) | 492 → 186 ms (2.6x) | 1.94 → 0.71 s (2.7x) |
| arrow | 1.23 ms → 44 µs (**28x**) | 1.36 ms → 52 µs (**26x**) | 2.50 → 1.83 ms (1.4x) | 19.4 → 17.9 ms (1.1x) | 257 → 231 ms (1.1x) | 1.01 → 0.84 s (1.2x) |
| pydict | 991 µs → 10.9 µs (**91x**) | 1.42 ms → 85 µs (**17x**) | 5.53 → 3.29 ms (1.7x) | 94.0 → 52.6 ms (1.8x) | 2.35 → 1.41 s (1.7x) | - |
| nested | 1.07 ms → 28 µs (**38x**) | 1.31 ms → 34 µs (**38x**) | 2.31 → 1.71 ms (1.4x) | 19.4 → 18.1 ms (1.1x) | 223 → 284 ms (0.8x) | 875 → 791 ms (1.1x) |

| pipeline | 1M | 16M | 256M | 1G |
|---|---:|---:|---:|---:|
| numpy | 3.35 → 2.27 ms (1.5x) | 22.5 → 19.0 ms (1.2x) | 236 → 198 ms (1.2x) | 942 → 760 ms (1.2x) |
| bytes | 3.14 → 2.13 ms (1.5x) | 22.7 → 21.3 ms (1.1x) | 366 → 347 ms (1.1x) | 1.46 → 1.36 s (1.1x) |
| torch | 3.83 → 2.09 ms (1.8x) | 37.4 → 20.2 ms (1.9x) | 807 → 209 ms (3.9x) | 3.18 → 0.80 s (4.0x) |
| arrow | 3.74 → 2.29 ms (1.6x) | 23.9 → 21.4 ms (1.1x) | 295 → 257 ms (1.1x) | 1.24 → 0.91 s (1.4x) |
| pydict | 15.4 → 12.9 ms (1.2x) | 257 → 211 ms (1.2x) | 4.95 → 3.96 s (1.2x) | - |
| nested | 3.02 → 2.16 ms (1.4x) | 22.7 → 20.0 ms (1.1x) | 232 → 288 ms (0.8x) | 899 → 838 ms (1.1x) |

What this shows:
- **Small values** travel inside the handle, so there is nothing to
  transfer.
- **From 1 MB up**, Chimaera now matches or beats Ray (1.1–4x). A 1 GB get
  runs at about 1.4 GB/s, which is the NIC's rate.
- **How it got there:** this took four changes, which together took a 1 GB
  get from 3.4 s to 0.71 s and a 1 MB get from 3.8 ms to 2.0 ms:
  - fixed 8 MiB HTTP/2 windows (hyper's adaptive window had capped each
    stream at about 250 MB/s);
  - four TCP connections per peer;
  - parallel ranges from 1 MiB;
  - reading, hashing and writing each chunk on blocking threads, so they
    overlap the network instead of queueing behind it on the async workers.

## Sharing on one machine (2026-11-22)

**GPU.** One NVIDIA A10G (Anyscale g5.2xlarge, torch 2.15, CUDA 12). The
driver puts a CUDA tensor; a call granted the same GPU reads it, sums a slice
and writes to it (the write stays in the call: the driver's tensor is
checked unchanged). End-to-end call time, median of 5 after a warm-up:

| tensor | read from the owner's GPU memory (CUDA IPC) | read from the host copy (`CHIMAERA_CUDA_IPC=0`) | speedup |
|---|---:|---:|---:|
| 256 MiB float32 | 2.7 ms | 54.8 ms | **20x** |
| 1 GiB float32 | 6.2 ms | 194.7 ms | **31x** |

The worker's counter of direct reads confirms which path ran. Ray was not
measured on this machine.

**CPU, across peers.** Three peers on one machine: a value one peer needs
from another is now a hard link of the holder's file (same inode, checked by
`test_locality.py::test_a_peer_on_the_same_machine_takes_an_object_from_the_holders_store`)
instead of a stream over loopback gRPC. Not timed.

## Usability

| check | Ray | Chimaera |
|---|---|---|
| modify an array right after `get` | `ValueError: assignment destination is read-only` | works (copy-on-write) |
| shared memory comes back after dropping a 256 MB `put` | yes | yes |
| put and get a 2 GiB object | yes | yes |
| configure object store size before starting | yes (`object_store_memory`) | yes (`CHIMAERA_MEMORY_POOL`; default 90% of the object filesystem) |
| choose the spill disk | yes (`object_spilling_config`) | yes (`CHIMAERA_SPILL_DIR`) |
| see what the store holds | `ray memory` | `chimaera objects` |
| copy finished values here while waiting | `ray.wait(fetch_local=True)` | `ch.wait(fetch_local=True)` |
| a `put` value survives the death of the node that made it | no (`OwnerDiedError`) | yes (replica) |
| torch tensors take a fast path | no (1.2 s to put 1 GB) | yes (38 ms on huge pages) |

The full feature comparison with Ray's object store is in
[docs/objects.md](../../docs/objects.md#compared-with-rays-object-store).

## What is not done

- **Writing under memory pressure** trails Ray, about 0.5x. The cause is the
  cost of the user's fresh arrays in tmpfs (see the spill section).
- **Copied types:** Arrow buffers and torch tensors from torch's own
  allocator are copied once into shared memory. Large Python `bytes`,
  `bytearray`, `str` and lists are now allocated in the arena (stored by
  link); on 4 KiB tmpfs pages that makes reading a 256 MB+ `bytes` or `str`
  about 15% slower than before (huge pages: faster).
- **Not re-measured:** the `bytes` rows for `return` and `pipeline` above
  predate the raw-allocator arena.
- **Large `str`** now travels out of band (`results/str-oob-*.jsonl`, one
  run): put of 256 MB 291.8 ms → 153 ms on 4 KiB pages (1.9x Ray; it was
  0.7x) and 77 ms on huge pages; put+get of 256 MB 1.5x (4 KiB) and 2.1x
  (huge). At 1–16 MB it is 0.7–1.3x, and one 16 MB get on huge pages
  measured 6.0 ms against 2.8 ms in the previous run (0.5x Ray); that run
  was not repeated, so whether it is noise is not known.
- **The call path** is no longer behind: an actor call is 1.7x faster than
  Ray and one actor's result passed to another 1.9x (fixed above). The
  per-type `return` and `pipeline` rows below were measured before the
  direct path, so their small sizes show the old call path.
