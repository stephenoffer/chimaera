# Objects: how values move, spill and survive

This is how Chimaera stores the values that calls pass and return, and that
`ch.put` stores. It covers moving them between processes on one host and
across hosts, spilling them to disk, and keeping them when a peer dies.
Measurements against Ray are in [bench/objects/RESULTS.md](../bench/objects/RESULTS.md).

## The model

An object is a file. On Linux the object directory is in memory (tmpfs), so
every process on the host maps the same pages. Nothing is copied to give a
value to another process, and the kernel frees a value's pages when its
last name and its last mapping are gone. One object directory serves the
whole host. In order of preference, it is:

1. `CHIMAERA_OBJECT_DIR`, when set;
2. a tmpfs mounted with huge pages (`huge=always` or `huge=within_size`),
   detected automatically;
3. `/dev/shm`, when it has at least 2 GiB;
4. the peer's own directory on disk.

Huge pages matter because, at the sizes objects reach, the cost is page
faults, not copying. On the m6id.8xlarge benchmark machines:

| operation | 4 KiB pages | huge pages |
|---|---:|---:|
| map 1 GiB of existing pages | 32 ms | 0.28 ms |
| read-fault 1 GiB | 30 ms | 2.8 ms |
| remap 1 GiB copy-on-write | 15.6 ms | about 0 |

For comparison, a memcpy on the same machine runs at 13.6 GB/s on one
thread and 73 GB/s on sixteen.

To get huge pages on a host, mount one tmpfs; the peer finds it:

```
sudo mkdir -p /mnt/chimaera-shm
sudo mount -t tmpfs -o huge=within_size,size=50%,mode=1777 tmpfs /mnt/chimaera-shm
```

## Writing a value

`ch.put(value)`, a call's return value and a call's large arguments are all
encoded the same way (`_serde.dumps_parts`). The result is a protocol-5
pickle plus out-of-band buffers. Each buffer then takes the cheapest path
that is still exact:

- **Arena values: linked, never copied.** In every Chimaera process
  (drivers and executors), allocations of 1 MiB or more come from files in
  `<objects>/.arena/<pid>/` (`objects.rs`). That covers NumPy's allocator
  and Python's own raw allocator, so large `bytes`, `bytearray`, `str` and
  lists live there too. Storing such a value hard-links its file under the
  object's name. It then remaps the owner's pages copy-on-write, so the
  owner's later writes never reach the stored object. The cost no longer
  depends on the value's size. A second store of a value its owner has not
  changed since links the same file again: `PAGEMAP_SCAN` checks the pages
  wholly inside the value, and the partial pages at its ends are compared
  with the file (a `bytes` object's header, which its reference count
  rewrites, shares the first page with its data). pandas numeric columns,
  NumPy-backed torch tensors and nested structures of arrays take this path
  too. A growing arena block (a `bytearray` being extended) grows its file
  and remaps in place, with no copy, until it is stored.
- **Other buffers: copied once, in parallel, onto warm pages.** The native
  writer fills the object file on up to 16 threads. It takes its pages from
  a reserve of pre-zeroed files that the peer keeps topped up in the
  background, so the copy does not also pay for faults. The reserve follows
  demand: a size class (powers of two from 256 KiB to 1 GiB, each file a
  little larger than its power so a value of exactly that size fits) is
  filled once writers have missed it twice within 10 s, by eight filler
  threads. Unused files are dropped after 60 s, or 10 s from 64 MiB up.
  The budget is the lesser of 2 GiB and 1/16 of the memory limit, because
  the pool counts reserve pages as memory in use. Back-to-back puts of
  large `bytes` use files faster than they are refilled, so the later ones
  fault their pages in. A full tmpfs is an `ENOSPC` error, never a
  `SIGBUS`.
- **Top-level `bytes`** go out of band, which avoids pickle's copy into its
  stream: from the arena they are linked, otherwise copied once.
- **Small values** (up to 256 KiB for `ch.put`, 64 KiB for results) travel
  inside the handle; no file is written.
- **Large plain containers** (dicts, lists and tuples of str, int, float,
  bytes, bool and None, with no container reachable twice) are pickled
  without the memo. The memo is most of the cost: a dict of four million
  strings pickles 6.9x faster without it, and the result is exact because
  nothing is shared.

`ch.put` gives each value a fresh id; nothing is hashed. Call arguments are
still stored by content (so the same array passed to a thousand calls is
stored once), except arena arrays, which are linked.

## Reading a value

Reading maps the object's file copy-on-write. Arrays come back **writable**,
and a write stays private to the reader. Ray returns read-only arrays, so
`arr[0] = 1` raises there. A read costs about the same at 1 KB and at 1 GB
(roughly 80 µs for a 1 GiB array), because pages fault in as they are
touched.

## Across hosts

A reader on another host fetches the object's file from the peer that holds
it. Objects of 1 MiB or more come over eight parallel ranges spread across
four TCP connections, in chunks of at most 4 MiB, each chunk checked with
blake3. On both ends a blocking thread does the file work and the hashing
(`pread` on the sender, `pwrite` on the receiver), so it overlaps the
network. A reader that names an object without knowing its size first asks
for one byte, which returns the size. HTTP/2 flow-control windows are fixed
for bulk transfer: 8 MiB per stream and 32 MiB per connection.

`ch.wait(fs, fetch_local=True)` also copies each finished value (its file
and any blobs it names) to this host before returning, as Ray's
`fetch_local` does. Waiting never moves data otherwise.

## Memory, spill and restore

- **The limit.** Objects may use `CHIMAERA_MEMORY_POOL` bytes of memory,
  or 90% of the object filesystem when that is unset (the `.plane` file
  tells writers). The count covers only this peer's objects: the peer
  publishes it each second in `.usage`, and each writer adds what it wrote
  since.
- **Background spill.** From 80% of the pool, the peer moves the oldest
  objects to disk, four at a time, without blocking anyone.
- **Spill at birth.** Writers keep going to memory until the limit plus a
  quarter. Past that hard cap, a new object is written straight to the
  spill directory on disk, and its name in the object directory becomes a
  symlink to it. Nothing is evicted to make room, and nothing is moved
  twice.
- **Restore is a mapping.** A spilled object is read by mapping its disk
  file through the symlink. The page cache serves it, or the disk does on a
  miss. There is no restore step and no copy back into memory.
- **Where it spills.** The spill directory is `spill/` in the peer's
  directory. `CHIMAERA_SPILL_DIR` names others instead, colon-separated
  (bigger or faster disks, or a network mount, like Ray's
  `object_spilling_config` directories), each used as
  `<dir>/chimaera-<peer>`. Spilled objects go to each directory in turn.
  Spilling only happens when objects are in memory, and only to spill
  directories on another filesystem. `CHIMAERA_SPILL_THRESHOLD` (default
  0.8) and `CHIMAERA_SPILL_THREADS` (default 4) are Ray's
  `object_spilling_threshold` and `max_io_workers`.
- **Seeing what is stored.** `chimaera objects` (or `--json`) lists what this
  peer's object store holds in memory and on disk, the arenas of live
  processes, the reserve, each run's share and the largest objects.
  `--cluster` asks every live peer (over the mesh `Inventory` call) and
  lists each one's store.
- **Lifetime.** A `ch.put` whose handle is dropped without having escaped
  (pickled, or passed by reference to a call) is deleted at once. Everything
  else lives until its run ends, as before.

## Fault tolerance

- **Replicas.** Every object a peer writes is copied to its ring successor,
  the next live peer by node id. The successor pulls the bytes into its
  spill directory on disk behind the same symlink layout, so a replica
  costs no memory. Copies fetched from other peers and spilled objects are
  not replicated again.
- **When the copy is made.** The copy starts 1 s after the write and is
  skipped if another peer has fetched the object by then: that peer already
  holds a copy, and a replica would only compete with it for bandwidth. So
  an object is safe about one second plus one transfer after it is written.
  At most eight copies are in flight from one peer; the rest wait their
  turn. The writer waits for the successor to confirm each copy
  (`ReplicateRequest.wait`), so a failed copy is logged as a warning and
  counted (`replication_failures_total` in `/metrics`), not lost silently.
  `CHIMAERA_OBJECT_REPLICAS=0` turns replicas off.
- **Finding a copy.** A reader whose holder is gone tries the holders'
  successors, then a few other live peers (at most eight peers in all),
  before reporting `DataLost`.
- **Dead processes.** A process that dies leaves its arena behind; the peer
  sweeps it every 10 s. Stored objects keep their own links, so sweeping
  never loses one.

## Settings

| variable | default | effect |
|---|---|---|
| `CHIMAERA_OBJECT_DIR` | auto (see above) | where objects live |
| `CHIMAERA_MEMORY_POOL` | 90% of the object filesystem | objects in memory before new ones go to disk |
| `CHIMAERA_ARENA` | `1` | `0`: NumPy arrays and large Python values stay on the heap, and are copied when stored |
| `CHIMAERA_SPILL_DIR` | the peer's `spill/` | where objects go past the memory limit: one directory or several, colon-separated |
| `CHIMAERA_SPILL_THRESHOLD` | `0.8` | share of the memory pool past which the oldest objects move to disk |
| `CHIMAERA_SPILL_THREADS` | `4` | objects copied to disk at once |
| `CHIMAERA_OBJECT_REPLICAS` | `1` | `0`: no replicas |

## Compared with Ray's object store

| Ray | Chimaera |
|---|---|
| `ray.put`, `ray.get`, refs nested in values | `ch.put`, `ch.get` / `future.result()`, `ch.handles()` to pass handles inside values |
| `ray.wait(..., fetch_local=True)` | `ch.wait(..., fetch_local=True)` |
| zero-copy NumPy reads, read-only | zero-copy reads, writable (copy-on-write); zero-copy *writes* for NumPy arrays and large `bytes` too |
| pickle 5 out-of-band buffers, `register_serializer` | the same, `ext.register_serializer` |
| `object_store_memory` | `CHIMAERA_MEMORY_POOL` |
| huge pages (`_plasma_directory` plus a flag) | a mounted huge-page tmpfs is found and used; `CHIMAERA_OBJECT_DIR` |
| spill to local disk, `object_spilling_config` directories (one or several) | `spill/`, or the directories in `CHIMAERA_SPILL_DIR`, each in turn; past the hard cap, new objects are written to disk directly |
| `object_spilling_threshold`, `max_io_workers` | `CHIMAERA_SPILL_THRESHOLD`, `CHIMAERA_SPILL_THREADS` |
| restore: a copy back into the store | a mapping of the disk file: no copy |
| `ObjectStoreFullError` after the fallback allocator fills | no store-full error: memory, then disk; a full disk is an `OSError` naming it |
| `ray memory` | `chimaera objects`, `chimaera objects --cluster` (every live peer) |
| store metrics in the dashboard | Prometheus `object_bytes`, `memory_used_bytes`, `replication_failures_total` |
| chunked pull between nodes | parallel ranges over four connections, every chunk checked with blake3 (Ray checks nothing) |
| lineage reconstruction of task outputs | replicas on the next peer, for task outputs *and* `put` values; lineage is in progress (review plan P5.2) |
| `OwnerDiedError` when a `put`'s owner dies | the value is read from its replica |
| ref counting, `free` | a `put` handle dropped without escaping is freed at once; a released value is deleted on every peer that fetched it; the rest go when their run ends |

Not matched yet:

- Spilling to S3 through `smart_open` (Ray marks it as not for production).
  A network filesystem works as a spill directory today; object storage is
  M3.
- `min_spilling_size`: Ray packs small objects into larger spill files;
  Chimaera spills one file per object.
- Device tensors moved between GPUs without the object store (Ray Direct
  Transport, `_tensor_transport`): not part of either object store; P5 of
  the layered-API plan.

## Limits

- The arena covers NumPy (with pandas numeric columns and torch tensors
  made from NumPy) and Python's raw allocator (large `bytes`, `bytearray`,
  `str`, lists). Tensors from torch's own allocator, Arrow buffers, and
  values made before `ch.init` or while `tracemalloc` traces are copied
  once (in parallel, onto warm pages).
- `str`, dicts of strings and object columns are pickled. Pickling bounds
  their speed in both runtimes.
- Before a fork, the parent's arena mappings become copy-on-write, so the
  child gets the usual private copy (Python object headers in the arena
  included). The parent's later writes to those blocks then copy pages, as
  after any fork.
- Replicas cost one extra network transfer per object that no other peer
  read. A peer that dies within about a second of writing an object can
  lose that object.
