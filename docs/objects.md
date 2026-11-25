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

Nodes Chimaera launches itself (the SkyPilot spares of
`chimaera.provisioner`) mount one at `/mnt/chimaera-objects` before their
peer starts, wherever `sudo` needs no password. Writing three times the
memory limit in 16 MB objects then took 3.2–3.3 s, against 5.5–5.8 s on
`/dev/shm` (bench/objects/RESULTS.md). A pod cannot mount one itself:
`emptyDir` takes no mount options. On Kubernetes, mount it on the nodes (a
privileged DaemonSet, or the node image) and give the pod a `hostPath`
volume there, named in `CHIMAERA_OBJECT_DIR`, in place of the `emptyDir`
at `/dev/shm` in deploy/k8s/example.yaml.

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
  and remaps in place, with no copy, until it is stored. A new block of
  8 MiB or more is populated on several threads as it is handed out, so the
  code that fills it (a `str` encode, a read into a new `bytes`, a NumPy
  copy) does not fault its pages in one 4 KiB page at a time.
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
- **Top-level `str`** of 1 MiB or more is encoded to UTF-8 once and goes out
  of band the same way (the encoded bytes are an arena block, so they are
  linked), instead of being encoded into pickle's stream and copied again.
  Reading decodes it once, as pickle would.
- **Small values** (up to 4 KiB for `ch.put`, 64 KiB for results) travel
  inside the handle; no file is written. A larger put is one stored file:
  passing its handle to calls copies no bytes, and a read maps or copies it
  once.
- **Large plain containers** (dicts, lists and tuples of str, int, float,
  bytes, bool and None, with no container reachable twice) are pickled
  without the memo. The memo is most of the cost: a dict of four million
  strings pickles 6.9x faster without it, and the result is exact because
  nothing is shared.

`ch.put` gives each value a fresh id; nothing is hashed. Call arguments are
still stored by content (so the same array passed to a thousand calls is
stored once), except arena arrays, which are linked. A driver connected to a
cluster over TCP stores by content too, and uploads each buffer straight
from the array or bytes object it serialized: chunks of 1 MiB are copied
out only as the connection takes them (four at most waiting per upload, and
32 across all of a process's uploads), so a `ch.put` of a large array does
not need room for another copy of it: 128 MiB raised a driver's peak memory
by 33-36 MiB, against 386 MiB before. The peer checks the bytes against their
id, so a buffer changed by another thread while it was sent is refused,
never stored under the id.

## Reading a value

Reading maps the object's file copy-on-write. Arrays come back **writable**,
and a write stays private to the reader. Ray returns read-only arrays, so
`arr[0] = 1` raises there. A read costs about the same at 1 KB and at 1 GB
(roughly 80 µs for a 1 GiB array), because pages fault in as they are
touched. On macOS a copy-on-write mapping costs more to set up (20 to 30 ms
for 512 MiB, against 2 ms read-only), so large reads on a laptop are slower
than on Linux.

Three kinds of value are read rather than mapped:
- a small object file, read whole: up to 1 MiB on Linux and 16 MiB on macOS,
  where a first mapping costs about 2.5 ms;
- a large `bytes` value, read with one `pread` from its file or linked blob
  into the new `bytes` object (it must be copied anyway, and copying out of a
  fresh mapping would pay a page fault per page as well);
- one value of a stored tuple (a call with `returns=N`), which reads the
  tuple's table and that value's bytes only (a shuffle's gather wants one
  bucket of each scatter's file).

## Across hosts

A reader on another host fetches the object's file from the peer that holds
it. Objects of 1 MiB or more come over eight parallel ranges spread across
four TCP connections, in chunks of at most 4 MiB, each chunk checked with
blake3. A range whose stream is lost partway is asked for again from its
first unchecked byte, once every other range has finished, for at most two
more rounds while each round checks some bytes; only then does the reader
fall back to one stream of the whole object, and then to another holder.
On both ends a blocking thread does the file work and the hashing
(`pread` on the sender, `pwrite` on the receiver), so it overlaps the
network. A reader that names an object without knowing its size first asks
for one byte, which returns the size. HTTP/2 flow-control windows are fixed
for bulk transfer: 8 MiB per stream and 32 MiB per connection.

**Peers on the same machine** take an object straight from the holder's
store instead of streaming it: each peer advertises its machine (the boot
id) and its object directory, and when the holder shares this machine and
its file is visible here, the fetch is a hard link (no bytes move, when the
two stores share a filesystem) or a kernel copy. Anything else (another
machine, a store this peer cannot see or read, a size that does not match)
falls back to the network path. Such a copy is counted apart from network
fetches (`object_bytes_linked_total` against `object_bytes_fetched_total` in
`/metrics`, and "on this machine" in `chimaera status`), and it does not
teach the peer's estimate of the holder's network rate: a hard link of a
gigabyte takes microseconds. Processes under one peer always shared pages
directly: they map the same object file.

**CUDA tensors.** A tensor on a GPU is stored as a host copy (readable
anywhere, and replicated like any object). A `ch.put` of a CUDA tensor of
1 MiB or more also carries a CUDA IPC handle: a process on the same machine
that can see that GPU copies the tensor device to device from the owner's
memory, without the host round trip, onto its own device (a write stays in
the reader). The putting process keeps the tensor alive while the put's
future lives, and for its whole life once the future is passed on, so the
memory a handle names is never freed while a reader may open it. Readers
on other machines, readers that cannot see that GPU, and the owner itself
use the host copy. `CHIMAERA_CUDA_IPC=0` turns the handle off.

Across machines, with `CHIMAERA_CUDA_NET=1`, a put CUDA tensor is also
offered for NCCL (`chimaera._gpunet`): a call granted a GPU on another
machine asks the putting process, and the two send it GPU to GPU through a
two-rank NCCL group kept for that pair. The host copy is then stored as a
blob of its own, replicated as usual and fetched only when that fails (the
putting process gone, no NCCL), so a dead holder costs speed, never the
value. A reader without a GPU still gets a CPU tensor. It is off by default:
these transfers are neither authenticated nor encrypted (the token in each
value is the only capability), so turn it on only on a trusted cluster
network. It is meant for RDMA networks (InfiniBand, EFA): on plain TCP,
NCCL's socket transport was slower than the host copy (1 GiB in 2.5 s
against 1.8 s between two 10 Gb/s g5 nodes, with 4 to 16 sockets alike).

`CHIMAERA_CUDA_NET=nixl` sends such tensors with NIXL instead, Ray Direct
Transport's other option: the putting process registers the tensor's memory
and tells the reader where it is, and the reader reads it one-sided (UCX,
or LIBFABRIC where an EFA device is present), with no group to set up. Both
sides need the `nixl` package (`pip install nixl nixl-cu12`); a holder
without it leaves the reader the host copy. On plain TCP it was the slowest
of the three (1 GiB in 3.0 s, against 1.9 s over NCCL and 1.2 s for the host
copy, between two g5 nodes), so it is for RDMA networks. Between two EFA
nodes (p4d.24xlarge) in Anyscale's standard image no NIXL read completed.
NCCL goes GPU to GPU over EFA once torch is a CUDA 12 build, as the image's
aws-ofi-nccl plugin needs (1 GiB in 1.04 s against 1.26 s for the host
copy; bench/objects/RESULTS.md). A read that fails once it
has begun falls back after a few seconds, with a warning, and that
process reads the same holder's host copies from then on.

`ch.wait(fs, fetch_local=True)` also copies each finished value (its file
and any blobs it names) to this host before returning, as Ray's
`fetch_local` does. Waiting never moves data otherwise.

## Memory, spill and restore

- **The limit.** Objects may use `CHIMAERA_MEMORY_POOL` bytes of memory,
  or 90% of the object filesystem when that is unset (the `.plane` file
  tells writers). The count covers only this peer's objects: the peer
  publishes it each second (and after each round of spilling) in `.usage`,
  and each writer adds what it wrote since.
- **Background spill.** From 80% of the pool, the peer moves the oldest
  objects to disk, four at a time, without blocking anyone. It measures
  again after each round and goes on until they are under that line, so
  fast writers seldom reach the hard cap below.
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
  `<dir>/chimaera-<peer>`. Spilled objects go to each directory in turn,
  skipping one without room for the object or whose copy failed in the
  last minute (it is tried after the others). An object's bytes are
  reserved on the disk (`posix_fallocate`) before it is copied, so a full
  disk fails before anything is written; a failed copy leaves nothing in
  that directory, the object stays in memory, and the next directory is
  tried. Writers spilling at birth do the same. Spilling only happens when
  objects are in memory, and only to spill directories on another
  filesystem. `CHIMAERA_TEST_SMALL_SPILL_DIR` runs the full-disk test
  (`cargo test -p chimaera-peer a_spill_to_a_full_disk`) against a real
  small filesystem: on Linux `docker run --tmpfs /small:size=4m` with the
  variable set to `/small`; on macOS a disk image, `hdiutil create -size 6m
  -fs HFS+ small.dmg && hdiutil attach -nobrowse -mountpoint <dir>
  small.dmg`. `CHIMAERA_SPILL_THRESHOLD` (default
  0.8) and `CHIMAERA_SPILL_THREADS` (default 4) are Ray's
  `object_spilling_threshold` and `max_io_workers`.
- **Seeing what is stored.** `chimaera objects` (or `--json`) lists what this
  peer's object store holds in memory and on disk, the arenas of live
  processes, the reserve, each run's share and the largest objects.
  `--cluster` asks every live peer (over the mesh `Inventory` call) and
  lists each one's store.
- **Lifetime.** A `ch.put` whose handle is dropped without having escaped
  (pickled, or passed by reference to a call) is deleted at once. Everything
  else lives until its run ends, as before, unless another run holds it.
- **Handles held by other runs.** A `ch.put` handle unpickled where another
  run is current (a second program, a detached run, an actor of another
  run) holds the value: the peer records a lease on its files in the run's
  replicated shard state, so they stay when their run ends. The value goes
  once its last holder lets go: the handle is collected, the holder's
  session ends, or its peer restarts or is lost. A held handle passed to a
  function as an argument (the call reads the value) keeps its lease until
  the call's outcome; passed to an actor, or pickled again (inside
  `ch.handles`, deeper in an argument, or returned), until the session
  ends. A run whose files are held does not expire. A handle
  unpickled after its run ended and its value was deleted raises `DataLost`
  when read.

## Fault tolerance

- **Replicas.** Every object a peer writes is copied to another peer in
  the background. The copy goes to another failure domain first: a peer in
  another `zone`, then another `rack` (both from `CHIMAERA_LABELS`), then
  on another machine, then the next live peer by node id. The receiver
  pulls the bytes into its spill directory on disk behind the same symlink
  layout, so a replica costs no memory. Copies fetched from other peers and
  spilled objects are not replicated again.
- **When the copy is made.** The copy starts as soon as the replicator sees
  the object (`CHIMAERA_REPLICA_DELAY_MS`, default 0, delays it), so an
  object is safe one transfer after it is written. A delayed copy is
  skipped if another peer fetched the object meanwhile: that peer already
  holds a copy. At most eight copies are in flight from one peer; the rest
  wait their turn. The writer waits for the receiver to confirm each copy
  (`ReplicateRequest.wait`), so a failed copy is logged as a warning and
  counted (`replication_failures_total` in `/metrics`), not lost silently.
  `CHIMAERA_OBJECT_REPLICAS=N` keeps N copies, each in a domain of its own
  where there are enough (default 1; a peer that fetched the object counts
  as one); 0 turns background copies off. Linux sees new objects through
  inotify; other systems list the store every 250 ms.
- **Durability classes.** `durability=` on a call (or `.options()`) or on
  `ch.put` says what the value survives, and a result's record keeps the
  class it was published with:

  | class | copies | when every copy is lost |
  |---|---|---|
  | default | a background copy, as above | a call's result is made again by running its call |
  | `"ephemeral"` | none | `DataLost`, saying it was ephemeral |
  | `"replayable"` | none | made again by running its call (functions and map items without external effects, not declared `deterministic=False`) |
  | `"retained"` | made before the call succeeds (or `ch.put` returns), `CHIMAERA_OBJECT_REPLICAS` of them, at least one; the result's record names them | made again by running its call, where it can |

  A CUDA tensor `ch.put` with `"ephemeral"` keeps no host copy either: it
  is read from the putting process's GPU memory, by processes on its
  machine that see that GPU while that process lives (a call granted another
  GPU does not), or GPU to GPU with `CHIMAERA_CUDA_NET`. Any other reader
  gets `DataLost` naming the class and why.

  A writer that wants no background copy leaves an empty
  `<object>.tmp-local` file beside the object before writing it, which the
  replicator takes away. A retained result whose copies fail is an
  infrastructure failure of that attempt (it is retried); on a peer that
  runs alone it fails as `ch.Infeasible`, as does a retained `ch.put`.
- **Finding a copy.** A reader whose holder is gone tries the peers the
  holder's copies go to, in the order above, then a few other live peers
  (at most eight peers in all), before reporting `DataLost`.
- **Draining.** A peer that drains (stopped with SIGTERM, or scaled down)
  first copies every object it holds to the first peer its copies go to
  (another failure domain first; never a draining peer), where readers look
  once it is gone. It waits for each copy, eight at a time, within the
  drain's grace, and logs how many it copied and how many it left
  (`objplane::hand_off`, called from the drain once running calls finish).
- **Results returned as handles.** A `ch.put` made inside a call is named
  like the call's results, so when the run ends and its other values are
  deleted, a result that is a handle to that put still reads.
- **Dead processes.** A process that dies leaves its arena behind; the peer
  sweeps it every 10 s. Stored objects keep their own links, so sweeping
  never loses one.
- **Dead peers.** A peer's object directory in a shared root (`/dev/shm`, a
  huge-page tmpfs) names its owner (`.owner`: pid and data directory). Every
  peer sweeps its neighbours every 10 s: a dead peer whose data directory is
  gone loses its whole object directory; one that may restart (its data
  directory is still there) keeps its objects and loses only its pre-zeroed
  reserve.

## Settings

A variable set to an empty value (`CHIMAERA_MEMORY_POOL=` in a compose file or pod spec) takes its default.

| variable | default | effect |
|---|---|---|
| `CHIMAERA_OBJECT_DIR` | auto (see above) | where objects live |
| `CHIMAERA_MEMORY_POOL` | 90% of the object filesystem | objects in memory before new ones go to disk |
| `CHIMAERA_ARENA` | `1` | `0`: NumPy arrays and large Python values stay on the heap, and are copied when stored |
| `CHIMAERA_SPILL_DIR` | the peer's `spill/` | where objects go past the memory limit: one directory or several, colon-separated |
| `CHIMAERA_SPILL_THRESHOLD` | `0.8` | share of the memory pool past which the oldest objects move to disk |
| `CHIMAERA_SPILL_THREADS` | `4` | objects copied to disk at once |
| `CHIMAERA_SPILL_URL` | unset | spill to object storage (`s3://bucket/prefix`, `gs://`, `az://`, `file://`) instead of disk; needs a peer built with the `object-store-spill` cargo feature (off by default) |
| `CHIMAERA_MACHINE_ID` | the boot id | peers with the same id take objects from each other's stores (tests set one per peer to stand for separate machines) |
| `CHIMAERA_CUDA_IPC` | `1` | `0`: a put CUDA tensor carries no IPC handle; same-machine readers use the host copy |
| `CHIMAERA_CUDA_NET` | unset (off) | `1`: a put CUDA tensor read on another machine goes GPU to GPU over NCCL; `nixl`: with NIXL's one-sided reads (needs the `nixl` package on both sides). Unauthenticated and unencrypted, so for trusted cluster networks only; on plain TCP both are slower than the host copy |
| `CHIMAERA_OBJECT_REPLICAS` | `1` | copies of each object on other peers; `0`: none |
| `CHIMAERA_REPLICA_DELAY_MS` | `0` | how long a new object waits before it is copied to another peer. At `0` a `put` value is protected as soon as it is written. A wait (say `1000`) lets a reader on another peer make that copy instead, saving a transfer for objects read elsewhere soon after they are written, but a peer that dies within the wait takes the object with it |

## Compared with Ray's object store

| Ray | Chimaera |
|---|---|
| `ray.put`, `ray.get`, refs nested in values | `ch.put`, `ch.get` / `future.result()`, `ch.handles()` to pass handles inside values |
| `ray.wait(..., fetch_local=True)` | `ch.wait(..., fetch_local=True)` |
| zero-copy NumPy reads, read-only | zero-copy reads, writable (copy-on-write); zero-copy *writes* for NumPy arrays and large `bytes` too |
| pickle 5 out-of-band buffers, `register_serializer` | the same; `ext.register_reducer` (Chimaera's values only, lambdas allowed) or `ext.register_serializer` |
| `object_store_memory` | `CHIMAERA_MEMORY_POOL` |
| huge pages (`_plasma_directory` plus a flag) | a mounted huge-page tmpfs is found and used; `CHIMAERA_OBJECT_DIR` |
| spill to local disk, `object_spilling_config` directories (one or several) | `spill/`, or the directories in `CHIMAERA_SPILL_DIR`, each in turn; past the hard cap, new objects are written to disk directly. On disk each object keeps a file of its own (Ray fuses small ones there too), so a restore is a mapping of it and a freed object gives its space back at once |
| spill to S3 through `smart_open` (marked not for production) | `CHIMAERA_SPILL_URL`: S3, GCS, Azure or `file://`, uploaded in 16 MiB parts several at a time. Objects under 4 MiB go up together, a run's at a time, packed into objects of up to 64 MiB (Ray's `min_spilling_size`): 2,000 objects of 64 KiB are 2 uploads, not 2,000. A spilled object is downloaded on its first read (or when another peer fetches it), a packed one with one ranged read of its pack. Its remote copy is deleted when it is freed (a pack, when the last of its objects is) or with its run. Credentials and region come from the usual `AWS_*`/`GOOGLE_*`/`AZURE_*` variables or the machine's role. An S3 bucket's region is looked up when none is set |
| `object_spilling_threshold`, `max_io_workers` | `CHIMAERA_SPILL_THRESHOLD`, `CHIMAERA_SPILL_THREADS` |
| restore: a copy back into the store | a mapping of the disk file: no copy |
| `ObjectStoreFullError` after the fallback allocator fills | no store-full error: memory, then disk; a full disk is an `OSError` naming it |
| `ray memory` | `chimaera objects`, `chimaera objects --cluster` (every live peer) |
| store metrics in the dashboard | Prometheus `object_bytes`, `memory_used_bytes`, `replication_failures_total` |
| chunked pull between nodes | parallel ranges over four connections, every chunk checked with blake3 (Ray checks nothing) |
| lineage reconstruction of task outputs | both: replicas on another peer (another failure domain first), for task outputs *and* `put` values, with a durability class per value (above). When every copy of a call's result is lost, the call runs again on the reader's peer, under the same attempt, so it writes the same object and blob ids. That covers a call reading it as an input, and a program reading it, local or remote. Only functions and map items without external effects run again, and not one declared `deterministic=False`, which could return other bytes under the old id; the chain is rebuilt depth first, three levels at most. A lost `put` value, argument or actor result fails with `DataLost`, which says why it cannot be remade |
| `OwnerDiedError` when a `put`'s owner dies | the value is read from its replica |
| ref counting, `free` | a `put` handle dropped without escaping is freed at once; a released value is deleted on every peer that fetched it; the rest go when their run ends, except values another run's handle holds, which go when the last such handle does |

Not matched yet: nothing in Ray's object store.
`ray.experimental.get_object_locations` is `ch.runs.object_locations`: the
peer each value is stored at, with its size. Copies made in the background
are not listed, as no peer keeps a record of where every copy is, and Ray
lists no spilled copies either.

Ray Direct Transport's NIXL option is `CHIMAERA_CUDA_NET=nixl` (see CUDA
tensors above); its gloo option, for CPU tensors, is not needed here, since
a CPU tensor is a shared-memory object read in place on one machine and
fetched in parallel ranges across machines.

## Limits

- The arena covers NumPy (with pandas numeric columns and torch tensors
  made from NumPy) and Python's raw allocator (large `bytes`, `bytearray`,
  `str`, lists). Tensors from torch's own allocator, Arrow buffers, and
  values made before `ch.init` or while `tracemalloc` traces are copied
  once (in parallel, onto warm pages).
- Large top-level dicts, lists and tuples of plain data (str, bytes, int to
  64 bits, float, bool, None; no container reachable twice) go through a
  native codec, faster than pickle both ways, and so do object arrays of
  plain values (a pandas column of strings), with repeated strings kept as
  one object. Nested containers and other types are pickled, which bounds
  their speed in both runtimes. The codec does not share repeated strings the way pickle's memo
  does: a plain list holding one string object a million times reads back as
  a million strings (pickle already did this for such lists, which skipped
  the memo when they were plain).
- Before a fork, the parent's arena mappings become copy-on-write, so the
  child gets the usual private copy (Python object headers in the arena
  included). The parent's later writes to those blocks then copy pages, as
  after any fork.
- Replicas cost one extra network transfer per object. Each object is
  copied as soon as it is written, so the window in which a dead peer takes
  it along is the copy itself (64 MiB in well under 0.3 s on one host);
  `CHIMAERA_REPLICA_DELAY_MS` trades that window for fewer transfers.
  Losing a peer together with every peer holding a copy (one by default:
  `CHIMAERA_OBJECT_REPLICAS`) loses the object too. A call's
  result lost that way is made again by running its call (see the table
  above); a `put` value is not.
  The replicator watches the store with inotify on Linux, and lists it
  every 250 ms elsewhere (macOS), so an object written there may wait up to
  that long for its copy.
