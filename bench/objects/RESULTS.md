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
| first get of a spilled 256 MB object | **400x** | restore is a mapping of the disk file |
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

Every measured case where Chimaera is slower than Ray, or under 2x faster:

| data, pattern | vs Ray | why | what helps |
|---|---:|---|---|
| Python `bytes`, get of 256 MB–1 GB on 4 KiB pages | 0.8x (1.2–1.3x up to 16 MB; 1.5–1.7x on huge pages) | Both runtimes copy into a new `bytes` object: Python's `bytes` cannot point at shared memory. The new object is now allocated in the arena, and first-touch faults on 4 KiB tmpfs pages cost more than on the heap. | Huge pages, or `bytearray`, NumPy or Arrow buffers, which are read in place. `CHIMAERA_ARENA=0` restores heap allocation (and copied puts). |
| dicts, lists, pandas object columns, 1 MB+; `str` up to 16 MB | 0.5–1.8x | Pickling and unpickling (building Python objects one by one) is the whole cost in both runtimes. Plain dicts and lists already skip pickle's memo here (up to 6.9x less pickling time). A top-level `str` is encoded once and linked: from 256 MB it is 1.5–3.8x faster than Ray. | Columnar data (NumPy, Arrow) instead of Python objects. |
| writing 3x the memory limit (spill) | 0.5–0.7x | In this loop each new array is allocated in shared memory, where first-touch page faults cost more than on Ray's heap. The reserve covers only part of it. | Larger memory limit, or reusing arrays. |
| many 1 MB objects, get | 1.0x | One file and one copy per object, against Ray's copy into reused memory (puts are 12x faster: linked). | Batch small values into one array. |
| Arrow tables on 4 KiB pages | about 1.2x | Arrow buffers are copied; the zero-copy allocator covers NumPy only. | Huge pages (6–9x). |
| small values through `return` and `pipeline` | 0.1–1x | The call path, not objects: an actor call costs about 1.9 ms in Chimaera against 0.37 ms in Ray. | Outside the object store (the runtime's call journal). |

Everything else, from 1 KB values to 1 GB arrays and tensors, on one host or
across hosts, and spilled reads and recovery, is faster than Ray. Often it is
far faster.

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

Each runtime gets 8 GiB of object memory: Ray `--object-store-memory`,
Chimaera `CHIMAERA_MEMORY_POOL`. Each writes 24 GiB of distinct NumPy
objects, then reads every object back in another process and checks it.
Both spill to the same NVMe disk.

| object size | measure | Ray | Chimaera | speedup |
|---|---|---:|---:|---:|
| 16 MB | write 24 GiB | 5.0 s (5.1 GB/s) | 7.5 s (3.4 GB/s) | 0.67x |
| 16 MB | restore and check 24 GiB | 7.75 s (3.3 GB/s) | 1.66 s (15.5 GB/s) | **4.7x** |
| 16 MB | first get of the oldest object | 0.19 ms | 0.13 ms | 1.5x |
| 256 MB | write 24 GiB | 8.1 s (3.2 GB/s) | 15.3 s (1.7 GB/s) | 0.52x |
| 256 MB | restore and check 24 GiB | 3.57 s (7.2 GB/s) | 1.34 s (19.3 GB/s) | **2.7x** |
| 256 MB | first get of the oldest object | 45.5 ms | 0.11 ms | **400x** |

Both runtimes read back every object correctly. Chimaera's write rate is
still the slower one; the next section explains why.

## Recovery: a node dies holding values

Three nodes on one host (Ray `cluster_utils`; three Chimaera peers). A value
is made on node B, B is killed 3 s later, and the driver gets the value.
The 3 s matter: Chimaera copies each object to the next peer about a second
after it is written, and a node that dies sooner can lose it.

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
- **The call path** (1.9 ms per actor call against Ray's 0.37 ms) is not part
  of this work; it hides the object path for small values in `return` and
  `pipeline`.
