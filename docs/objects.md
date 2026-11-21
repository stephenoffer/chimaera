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

- **NumPy arrays in the arena: linked, never copied.** In every Chimaera
  process (drivers and executors), NumPy allocations of 1 MiB or more come
  from files in `<objects>/.arena/<pid>/` (`objects.rs`). Storing such an
  array hard-links its file under the object's name. It then remaps the
  owner's pages copy-on-write, so the owner's later writes never reach the
  stored object. The cost no longer depends on the array's size. A second
  store of an array its owner has not written since links the same file
  again; `PAGEMAP_SCAN` tells whether the owner wrote. pandas numeric
  columns, NumPy-backed torch tensors and nested structures of arrays take
  this path too.
- **Other buffers: copied once, in parallel, onto warm pages.** The native
  writer fills the object file on up to 16 threads. It takes its pages from
  a reserve of pre-zeroed files that the peer keeps topped up in the
  background, so the copy does not also pay for faults. The reserve follows
  demand within a budget, the lesser of 2 GiB and 1/16 of the memory limit.
  A full tmpfs is an `ENOSPC` error, never a `SIGBUS`.
- **Top-level `bytes`** go out of band, which avoids pickle's copy into its
  stream.
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

## Memory, spill and restore

- **The limit.** Objects may fill the object filesystem up to
  `CHIMAERA_MEMORY_POOL`, or 90% of it when that is unset (the `.plane`
  file tells writers).
- **Spill at birth.** Past the limit, a new object is written straight to
  the spill directory on disk, and its name in the object directory becomes
  a symlink to it. Nothing blocks, nothing is evicted to make room, and
  nothing is moved twice. The peer also moves the oldest objects out when
  memory passes 80% of its pool.
- **Restore is a mapping.** A spilled object is read by mapping its disk
  file through the symlink. The page cache serves it, or the disk does on a
  miss. There is no restore step and no copy back into memory.
- **Lifetime.** A `ch.put` whose handle is dropped without having escaped
  (pickled, or passed by reference to a call) is deleted at once. Everything
  else lives until its run ends, as before.

## Fault tolerance

- **Replicas.** Every object a peer writes is copied to its ring successor,
  the next live peer by node id. The successor pulls the bytes into its
  spill directory on disk behind the same symlink layout, so a replica
  costs no memory. Copies fetched from other peers and spilled objects are
  not replicated again. The copy follows the write by one transfer, about a
  second for 1 GiB on these hosts. `CHIMAERA_OBJECT_REPLICAS=0` turns
  replicas off.
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
| `CHIMAERA_ARENA` | `1` | `0`: NumPy arrays stay on the heap, and are copied when stored |
| `CHIMAERA_OBJECT_REPLICAS` | `1` | `0`: no replicas |

## Limits

- The arena covers NumPy, which includes pandas numeric columns and torch
  tensors made from NumPy. Tensors from torch's own allocator, Arrow
  buffers and Python `bytes` are copied once (in parallel, onto warm pages).
- `str`, dicts of strings and object columns are pickled. Pickling bounds
  their speed in both runtimes.
- A process that forks keeps private copies of its arena arrays in the
  child. A write by the parent after the fork can show through in the
  child, on pages the child has not written yet.
- Replicas cost one extra network transfer per object. A peer that dies
  within about a transfer time of writing an object can lose that object.
