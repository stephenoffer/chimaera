# Qualification record

What each milestone gate showed, how it was measured, and what remains
unverified. Labels: **local** (this machine or the dev container),
**cluster** (an Anyscale node), **emulation**, **unverified**.

Machines: macOS 14 (Apple Silicon, 12 cores, 36 GiB) and the dev container on
the colima VM (Linux 6.8 aarch64, 4 vCPU, 6 GiB, cgroup v2 delegated with
`--privileged --cgroupns=private`).

## M0 Foundations (commit fc34c1c)

| Gate | Result | Evidence |
|---|---|---|
| Decompression bombs and corrupt frames are rejected | pass (local) | `chimaera-proto` frame tests: a 64 MiB zstd bomb under a 1 MiB cap, a lz4 frame declaring 4 GiB, a flipped checksum byte |
| Topology reports the allocation, not the host | pass (local, Linux) | container with `--cpus 1.5 --memory 2g`: quota 1.5, memory 2 GiB from `memory.max` |
| HLC monotonic | pass (local) | proptests in `chimaera-core::hlc` |
| Shard invariants hold under random histories | pass (local) | 20,000 histories: single immutable winner, fencing of retired incarnations and revoked attempts, cancel/publish exclusive, external start consumed once, dependencies satisfied once, command dedup, replica digests equal |
| Mutants are caught | pass (local) | removing fencing, outcome immutability or command dedup each fails the property test for the intended reason |

## M1 Local runtime (in progress)

| Gate | Result | Evidence |
|---|---|---|
| Design doc examples print 2, 4, 5 | pass (local, macOS and Linux) | `test_api::test_design_doc_examples_print_2_4_5` |
| `fn(x)` never starts the runtime | pass (local) | `test_local_call_never_starts_the_runtime` |
| User kwargs named like options reach the function | pass (local) | `test_user_kwargs_named_like_options_reach_the_function` |
| A dependent of a failure never runs | pass (local) | side-channel file stays absent; the cause's type is re-raised with a "prerequisite" note |
| Actor FIFO over 1,000 calls; control shows reordering | pass (local) | `test_actors`: FIFO holds; `concurrency=4` overlaps and reorders |
| `async def` methods do not overlap by default | pass (local) | max observed concurrency 1 over 50 calls |
| Map pulls at most the window; `close()` runs the source's `finally` | pass (local) | window 8: at most 9 items pulled |
| Cancel racing commit reports the winner | pass (local) | `test_cancel_races_commit_and_reports_the_winner` |
| Script without a `__main__` guard prints once | pass (local) | fresh-process test |
| Killed executor is retried as attempt 2 | pass (local) | `test_executor_killed_mid_call_is_retried_on_a_new_attempt` |
| User exception runs exactly once | pass (local) | counter file has one line |
| External effect lost after start is `OutcomeUnknown`, never replayed | pass (local) | ledger has one line |
| Peer killed during a 10,000-call run; restart; attach sees every result once | pass (local) | SIGKILL at ~30% done; after restart and `ch.attach`: 10,000 results, 10,000 distinct |
| Busy loop measures 0.95–1.05 cores; a sleep under 0.02 | pass (local, macOS and Linux) | `test_accounting` (process scope, rusage) |
| Isolated attempt reports its own peak; exceeding the limit is an OOM kill | pass (local, Linux) | `memory_peak_bytes` ≥ 64 MiB with scope `attempt`, method `cgroup`; a 300 MiB allocation under `memory_limit="100 MiB"` raises `OutOfMemory` with the peak |
| `cpus=64` is infeasible with the shape | pass (local) | rejected at submit with requested vs offered |
| Thread pools follow the grant | pass (local) | `OMP_NUM_THREADS` and `OPENBLAS_NUM_THREADS` equal `cpus=2` |
| Prefetch replay adopts 2 and 4, reverts 8, clamps to 2 | pass (local) | `chimaera-core::adapt` replay reproduces the doc's 40.00/73.99/99.65/99.65 batches/s and the decisions |
| p99 grant-to-handoff ≤ 250 µs | pass on macOS (local), Linux pending | macOS release build: p50 18 µs, p99 34 µs over 10,000 dispatches at about half load |

| Generator calls stream in order; the generator stays a window ahead; an error arrives after the items before it; `close()` stops it; a killed worker replays without duplicates | pass (local) | `test_streams.py`: window 8 keeps the producer within 11 items of a consumer that took 2; SIGKILL after 3 items, all 20 arrive once in order; a cluster test streams on the consumer's peer |

Open in M1: the mypy plugin, the Linux dispatch measurement.

## M2 Multi-peer (in progress)

Localhost clusters of real peer processes (`python/tests/test_cluster.py`),
macOS. Each peer offers 4 CPUs; 4 data groups of 3 voters plus the directory.

| Gate | Result | Evidence |
|---|---|---|
| Work spreads over peers; values move between peers; a driver reads values produced elsewhere | pass (local) | 90 calls land on all 3 peers; 9 × 2 MiB inputs from ≥2 peers reach one call; repeated 5× after fixing a stale-load bias |
| Actors live on any peer and keep per-handle order | pass (local) | 6 actors × 200 calls, order exact, ≥2 hosts |
| A peer killed mid-run loses no result | pass (local) | 240 calls, peer SIGKILLed after 1.5 s; 8 of 8 repeats after fixing re-placement of refused grants |
| Ingress and the run's group leader killed; a second client attaches through another peer | pass (local) | detached run of 300 calls, all 300 results exactly once, read through another peer |
| A paused peer is retired and rejoins without duplicating results | pass (local) | SIGSTOP for 6 s, then SIGCONT; 400 results exact; retirement logged |
| A dead voter is replaced, so a second loss is survivable | pass (local) | 3 founders + 1 spare: kill one founder, wait for replacement, kill a second; calls still complete. Control: with the membership change disabled the same test times out |
| A peer without the cluster token is refused | pass (local) | the joiner's log shows `missing or wrong cluster token`; it never serves |
| Mutual TLS: the cluster runs over TLS; a peer from another CA is refused | pass (local) | 60 calls across peers over TLS; the foreign peer's handshake fails with `invalid peer certificate: BadSignature` and it never serves |
| Kill ingress and shard leader during a 100k-item map, resume p99 < 10 s | unverified | the localhost test uses 300 calls; the 100k-item map and resume percentiles are not measured yet |
| Killing 1 of 3 voters keeps admission at ≥ 80% throughput | fail (local) | 3 founders + 1 spare, release build, 5,000 no-op calls per sample, a founder SIGKILLed at 20 s: median 8,629 calls/s before, 6,213 (72%) in the 20 s after, 5,071 (59%) after the replacement (one peer's executors fewer, shared laptop; a first run gave 11,986 / 5,588 / 4,121). Found and fixed on the way: a peer hosting no replica of a group kept the plan's voter list and spent a 1 s connect timeout on the dead voter for every result watch |
| Simulation invariants over 10k seeds | unverified | no turmoil harness yet |
| Durable actor: a repeated command returns its first reply; the same id with other arguments is `CommandConflict` | pass (local) | `test_durable.py`: deposits 5 then 2 (state 7); repeating op-1 returns 5 and state stays 7; op-1 with 9 raises `CommandConflict` |
| Durable actor: a call that raises commits nothing; a killed process resumes from the committed state | pass (local) | state unchanged after a raising call; after SIGKILL of the actor's process the next call sees the committed total, on a new pid, without `__init__` |
| Durable actor survives the loss of its peer | pass (local) | 3 peers: the host peer SIGKILLed; `ch.actor(key)` from another peer reads total 45 on a new host (4 of 4 runs, after fixing code blobs that lived only on the ingress peer) |
| Durable actor killed after commit and before its reply | unverified as timed | the retry path it needs (same command id answered from the stored reply) is covered above; the kill is not timed between commit and publish |
| Workflow killed mid-way replays without repeating finished steps; recorded values repeat; a different call on replay diverges | pass (local) | `test_workflows.py`: worker SIGKILLed after step 2 of 4, each step ran once, result 60; `now`/`random` identical across the two runs; a raw `random.random()` argument raises `WorkflowDivergence` |

| Spillback hand-back refused by the shard keeps the call | pass (local) | `core::tests::only_an_accepted_give_back_moves_the_work_away`: only Accepted or Existing move the work away; Stale, Rejected, NotFound and a lost answer run it where it is. Spillback and peer-kill cluster tests pass |
| Autoscaling on Kubernetes grows on queued work and shrinks when quiet (P6) | pass (local, kind) | `deploy/k8s/autoscale-test.sh` (layered-api session): the in-cluster provisioner grew 1→3 on "37 calls queued for 6 s"; spillback moved queued calls to the new pod (40 results, 22/18 over two pods); shrank 3→2→1 at 40 s quiet periods. A pod that can't be scheduled is given up after `--start-grace` |
| Autoscaling on Slurm (P6) | pass (local, dockerized Slurm) | `deploy/slurm/slurm-autoscale-test.sh` (layered-api session): a 1-node job grew to 3 ("14 calls queued for 4 s"), 16 results spread 6/5/5, then 3→2→1 after 20 s quiet periods with all spare jobs cancelled |

Three local peers, release build, 20,000 no-op calls after a warmup
(`ch.gather`), Raft fsync on (`DiskDurability::Os`), other sessions building on
the same laptop. Counting the commands each group applied showed results
committed nearly one by one: about 22,000 calls cost 754 `SubmitTasks`, 675
`Grant` and 5,972 `Publish` entries. Each peer now keeps at most 2 publish
proposals in flight per vshard and batches the rest (`PUBLISH_IN_FLIGHT`):

| | Before | After |
|---|---:|---:|
| No-op calls/s, 3 peers | 4,300–6,800 (one run 9,400) | 11,700–13,200 |
| Actor calls/s, 8 actors over 3 peers | 7,475 (local3 run) | 12,000–14,100 |

Ray 2.60 on the same laptop (local3 run above): 8,739 calls/s and 15,894 actor
calls/s. Without fsync, before batching, the same calls reached 9,900–11,400/s:
the fsync on every commit set the pace. The Raft layer alone commits
18,000–29,000 entries/s with 64 proposals in flight, 1 or 3 voters, with or
without fsync (`measure_concurrent_commit_throughput`, ignored test).

Known limits: a value stored only on a peer that dies is lost (`DataLost`);
its task is not re-run to rebuild it. Peers on real separate machines have not
been tested.

## Ray comparison (cluster, single node)

One Anyscale node, c7i.16xlarge (64 vCPU, 124 GiB, Linux 6.8 x86_64), Ray
2.59.0 from the image against Chimaera built from source on the same node,
Python 3.11, full workload sizes (`EVAL_SCALE=1`). Job
`prodjob_db31rrxdh7at2a428g9vcvvcmb`, one run per workload: these are single
samples, not distributions. Multi-node scalability is unverified until the
multi-peer gates run.

| Workload | Ray 2.59 | Chimaera | Note |
|---|---|---|---|
| Round trip, p50 / p99 | 3.17 / 4.21 ms | 3.47 / 4.85 ms | Ray ahead; Chimaera is 0.58 ms p50 on macOS, the Linux gap is not yet explained |
| Independent tasks | 6,712/s | 7,861/s | |
| Map of tiny items, naive | 7,316/s | 10,231/s | Ray with hand batching: 732,542/s |
| Dependent chains | 547 steps/s | 4,868 steps/s | |
| CPU scaling efficiency (64 cores) | 0.33 | 0.82 | |
| Large object to many readers | 2.73 s | 0.66 s | |
| Actor calls through one handle | 5,091/s | 1,884/s | Ray ahead |
| Nested tasks | 3.55 s | 7.23 s | Ray ahead |
| Worker killed mid-call | recovered in 1.11 s | recovered in 0.016 s | |
| External effect, worker killed after start | 4 side effects (1 with `max_retries=0`) | 1 side effect, `OutcomeUnknown` | |
| 2 GiB allocated under a 1 GiB limit | completed (not enforced) | `OutOfMemory` with peak | |
| Per-call CPU measurement | not available (harness error in this run, fixed since) | 0.5 CPU-s, 1.00 cores, process scope | |

Earlier runs on the same node type were invalid. The first had the local peer
fsync every raft commit (3.5 ms per fsync on that disk, three commits per
call: 24 ms round trips). The second failed to start the peer because of a
socket-directory check. Both are fixed; `chimaera doctor` now prints the
disk's fsync latency.

Dev-container numbers (4 vCPU, a tenth of the sizes) are smoke runs only.

## Ray comparison (cluster, 4 nodes)

Four Anyscale nodes, c7i.4xlarge (16 vCPU each, 64 in all), Ray from the
image against one Chimaera peer per node started through Ray
(`bench/evals/multi_node.py`, `deploy/anyscale/eval-4node.job.yaml`), full
sizes. Job `prodjob_khqvpua1iltd1zzu58dnna5vew`, working tree diff
`c2efc4b541130ba9` on top of `22cbcd2`. One run per workload: single samples.

| Workload | Ray | Chimaera | Better |
|---|---:|---:|---|
| Independent no-op tasks | 6,596/s | 401/s | Ray, 16x |
| CPU scaling efficiency (64 CPUs) | 0.671 | 0.619 | Ray |
| Map of tiny items, naive | 6,274/s | 62,282/s | Chimaera, 10x (Ray with hand batching: 5.18M/s) |
| 512 MiB object to 128 readers | 3.385 s (2 nodes) | 2.768 s (1 node) | Chimaera |
| 32 MiB array passed straight to 128 calls | 3.719 s | 1.695 s | Chimaera |
| 10 calls on a 256 MiB value just made | 1.463 s | 0.459 s | Chimaera |
| Actor calls, 32 actors on 4 hosts | 10,265/s | 457/s | Ray, 22x |
| Nested tasks, 256 parents | 3.583 s | 2.866 s | Chimaera |
| Shuffle, 3.2M pairs | 1.642 s | 1.231 s | Chimaera |
| A node killed under 256 one-second tasks | 18.457 s | 7.035 s | Chimaera |

Per-call throughput collapsed across machines while it was near parity on
one laptop. Cause, found afterwards: the peers' TCP servers accepted
connections without `TCP_NODELAY` (tonic's builder flag does not reach a
listener the caller supplies), so raft replies waited out delayed ACKs, about
160 ms per 64-call submission commit, and the ingress commits one session's
submissions one at a time. Both are fixed (accepted sockets set nodelay; the
client keeps 2 submissions in flight and coalesces up to 4,096 calls
meanwhile). Rerun with both fixes (job `prodjob_3vat62mbqmvbbwb3rcwch8sydb`,
tree diff `e6e2bad21fbdee8d`), same machines and sizes:

| Workload | Ray | Chimaera | Better |
|---|---:|---:|---|
| Independent no-op tasks | 7,035/s | 2,675/s | Ray, 2.6x |
| CPU scaling efficiency (64 CPUs) | 0.667 | 0.660 | parity |
| Map of tiny items, naive | 7,275/s | 194,071/s | Chimaera, 27x |
| 512 MiB object to 128 readers | 3.052 s | 2.388 s (4 nodes) | Chimaera |
| 32 MiB array passed straight to 128 calls | 3.074 s | 2.229 s | Chimaera |
| 10 calls on a 256 MiB value just made | 1.097 s | 0.386 s | Chimaera |
| Actor calls, 32 actors on 4 hosts | 11,239/s | 4,688/s | Ray, 2.4x |
| Nested tasks, 256 parents | 3.318 s | 1.715 s | Chimaera |
| Shuffle, 3.2M pairs | 1.493 s | 0.620 s | Chimaera |
| A node killed under 256 one-second tasks | 8.686 s | 6.470 s | Chimaera |

Per-call throughput rose 6–10x but is still behind Ray across machines. A
3.5 ms fsync injected on three laptop peers does not reproduce the gap (they
still run 8–13k calls/s), so the next run recorded per-peer CPU and repeated
tasks and actor calls with raft fsync off (job
`prodjob_hrjyprp6y491c2sct9lmq7455f`, same machines, single samples):

| Workload | Ray | Chimaera, fsync per commit | Chimaera, replicas only |
|---|---:|---:|---:|
| Independent no-op tasks | 7,228/s | 2,681/s | 17,547/s |
| Actor calls, 32 actors on 4 hosts | 10,700/s | 3,820/s | 18,687/s |

With fsync the busiest peer used 0.3 cores: every peer was waiting on the
volume (an fsync costs about 3.5 ms there). Without it the busiest used 2.0.
Every other workload in that run favored Chimaera: CPU scaling 0.703 vs 0.675,
naive map 201,882 vs 7,294 items/s, broadcast 1.19 vs 3.00 s (4 nodes
reached), large argument 0.088 vs 3.101 s, follow-input 0.403 vs 1.177 s,
nested 2.094 vs 3.350 s, shuffle 0.655 vs 1.561 s, node loss 6.42 vs 9.38 s.

Peers now choose: fsync where it costs under a millisecond, replicas only on
slower volumes, and a peer that ran unsynced and whose machine rebooted
without a clean shutdown rejoins under a new identity
(`crates/chimaera-peer/src/durability.rs`; `test_cluster.py::
test_an_unsynced_peer_whose_machine_rebooted_rejoins_as_a_new_member`, which
also caught a starting peer being declared dead while its core was not yet
answering pings, and a dead identity kept alive by a new one answering at its
address).

## Smart defaults: learned reservations, memory, large values

Plan `mellow-finding-scone.md`. All results are **local** (macOS 14, 12 cores,
36 GiB; Linux results from the colima dev container where noted). The machine
was shared with other builds and test runs during these measurements.

| Gate | Result | Evidence |
|---|---|---|
| The same large argument is stored once | pass (local) | `test_values.py`: a 16 MiB array passed to 50 calls leaves exactly one new file, named by its content |
| Arrays arrive writable; a write stays in its call | pass (local) | `test_values.py`: copy-on-write mapping; the next call and the caller see the original |
| Old and new value encodings agree | pass (local) | without blobs the new encoder writes byte-identical objects for ten kinds of value; old and new readers read each other's output |
| Per-call argument files are released when the call ends | pass (local) | `test_values.py` |
| A finished run's stored arguments go when it ends; its results stay | pass (local) | `test_values.py`: after the script exits, its 16 MiB argument is deleted within 15 s and its 2 MiB result is still read through `ch.attach`. Fails on the previous peer, which kept both for the 24 h retention |
| `ch.put` returns a finished future read by reference | pass (local) | `test_values.py` |
| Memory is learned after three calls and survives a peer restart | pass (local) | `test_learning.py` |
| A memory kill raises later reservations, then fades as calls succeed | pass (unit) | `profile.rs`: killed at 900 MiB among 100 MiB calls: 900 MiB reserved (not 1.5× it, capped by 4× usual use), 500 after 8 successes, 400 after 16, back to 120 after 56. Before, the bound held at 1.5× until 64 more calls pushed it out, then vanished at once |
| Calls wait for memory instead of overcommitting it | pass (local) | `test_memory.py`: 10 calls of 300 MiB under a 1 GiB pool: at most 3 at once while unmeasured, at most 2 once learned |
| Nested calls do not deadlock on a small pool | pass (local) | `test_memory.py`: 6 parents × 3 children under 512 MiB |
| A leaking worker is replaced | pass (local) | `test_memory.py`: 40 MiB leaked per call; the worker changes and a `leaks` decision is recorded |
| Memory the allocator kept after frees goes back before the leak check | pass (Linux container) | `test_memory.py::test_memory_a_call_freed_goes_back_to_the_system`: 400k 1 KiB blocks with every 50th kept leave the worker at 423 MiB without `malloc_trim`, under 200 MiB with it; fails with the trim disabled |
| NumPy arrays in the Linux arena count toward memory | pass (Linux container) | Arrays allocated as tmpfs files (`.arena/<pid>`) are not private memory: each worker is charged its arena files, the plane's pre-zeroed reserve counts as object memory, and the leak check counts shared memory held past the arena's 256 MiB reuse cache. `test_memory.py` actor charge (300 MiB actor was charged 81 MiB before) and leak tests; `test_values.py` stored-once now counts files by inode, since the arena links one file under a name per use |
| A call queued at an actor whose method waits says how to overlap them | pass (local) | `test_learning.py`: after 12 calls of a 100 ms sleep, a call behind 25 others suggests `@ch.method(concurrency=N) on fetch`. Every call of a batch now counts toward its function's profile (at most 64 per batch), not only the last |
| New calls wait when the objects' disk is nearly full | pass (unit; Linux container) | `budget.rs` test; on a 700 MiB tmpfs object directory with 399 MiB free, a new call waited with "the filesystem holding objects (...) has 399 MiB free; new calls wait for room" while the running one finished |
| A full disk is named in the error | pass (local, unit) | `test_values.py::test_a_full_disk_is_named_in_the_error`; objects are now written natively (`chimaera-py/src/objects.rs`), which owns removing a partial file on failure |
| A memory or CPU limit on a parent cgroup is honored | pass (unit) | `topology.rs::chain_tests`: the pool, CPU capacity and available memory take the tightest limit from the process's cgroup up to the root (systemd slices and some runtimes leave the process's own cgroup unlimited) |
| A local peer running an older binary is reported | pass (local) | `test_defaults.py::test_a_program_warns_when_the_local_peer_runs_another_binary`: the peer records its binary (path, size, mtime) at start; a program that would start a different one warns CH111 |
| Work waiting on CPU that actors hold says so | pass (local) | `test_defaults.py::test_work_waiting_on_cpu_that_actors_hold_says_so`: actors hold no CPU by default; one given `cpus=2` on a 2-CPU peer makes a waiting call report the CPU held by 1 actor and its `cpus=` setting |
| What was learned can be listed | pass (local) | `chimaera profiles [--fn NAME] [--json]`; `test_learning.py::test_chimaera_profiles_lists_what_was_learned` |
| A call that mostly waits moves to a quarter core | pass (local) | `test_learning.py`: canary adopted; `profiles.rs` shows a computing function reverted by the latency guard. 600 calls of `sleep(0.2)`, as plain calls and as a map: 133 control and 133 canary attempts, adopted after 3 paired windows. Before queued calls kept their arm, re-drawing on each dispatch pass gave 553 canaries to 26 controls and the trial never concluded |
| Executors cap thread pools and write no core dumps | pass (local; Linux arena cap in the container) | `test_memory.py` |
| An edited module is used by the next run | pass (local) | `test_learning.py` |
| Driver backpressure, stalled-wait notice, task logs at the driver's level | pass (local) | `test_defaults.py` |
| A shared large input reaches each peer once | pass (local, 3-peer localhost cluster) | `test_locality.py`; fails with the single-flight lock removed |
| A call runs where its large input is | pass (local, cluster) | `test_locality.py`: at least 4 of 5 calls on the holder of a 64 MiB input; 10 of 10 repeats. Earlier failures were the test's own `result()` copying the input to the driver's peer, which then held it too |
| What one peer learned is used by another | pass (local, cluster) | `test_locality.py`: a peer that never ran the function reserves learned memory |
| Spill to disk keeps objects readable | pass (unit; Linux container end to end) | `stores.rs`; `test_memory.py::test_objects_past_the_pool_move_to_disk_and_stay_readable` in the dev container with objects in tmpfs: six 100 MiB values under a 600 MiB pool, some move to the spill directory and all six read back. Skipped on macOS, where objects are on disk |
| Host-pressure kill of the call furthest over its reservation | pass (Linux container, 3 GiB, 4 vCPU) | `deploy/dev/gotchas.sh`, `runaway` trap: a call whose memory grows without bound is stopped by the peer in 4.6 s with no kernel OOM kill; its retry would need 1.5× the peak it reached, more than the pool, so the caller gets an error at once, naming the peak and the pool (`ch.OutOfMemory` since `test_memory.py::test_a_call_killed_for_memory_whose_retry_cannot_fit_is_out_of_memory`; the container run returned `ch.Infeasible`). Ray 2.60 retried it until the trap's 180 s limit |
| A fresh function's first calls run at full width when memory allows | pass (local) | 16 calls of a 0.1 s busy loop never run before, on a `--cpus 4` peer, a fresh peer directory each run: CPU efficiency 0.87, 0.85, 0.79, against 0.49, 0.55, 0.55 with the old rule (a quarter of the CPUs until three calls were measured). Later runs reach 0.99 from the saved profile. The main session measured Ray 2.60 at 0.78 on the same workload in the Linux container (a different host) |
| Driver cost per `.remote()` | 27 µs → 15–17 µs per call (local) | 20,000 calls of a one-line function, and 5,000 actor method calls: options and argument signatures resolved once per function and scope instead of per call |

Memory traps against Ray 2.60, one repetition each in a Linux container capped
at 3 GiB with 4 vCPU (`deploy/dev/gotchas.sh`, release builds, 2026-11-21):

| Trap | Ray 2.60 | Chimaera |
|---|---|---|
| `mem_heavy`: 12 calls each needing 35% of memory | 3 of 12 finished, 43 kernel OOM kills, 10.8 s | 12 of 12, no OOM kill, 12.9 s |
| `leak`: 230 calls leaking memory in their worker | 230 of 230, 3 kernel OOM kills | 230 of 230, no OOM kill |
| `size_scaling`: memory proportional to input size | failed: object store spilled until the disk check refused writes (`OutOfDiskError`) | 120 of 120, 8.8 s |
| `runaway`: memory grows without bound | still retrying at the 180 s limit | stopped in 4.6 s, error returned |
| `mem_heavy_again`: the same script run a second time | 3 of 12, 44 kernel OOM kills, 13.1 s | 12 of 12, no OOM kill, 12.7 s |

Other traps against Ray 2.60 on the Mac (12 cores, 36 GiB; release peer;
median of 3 after a warmup, a fresh peer directory per run; 2026-11-21):

| Trap | Ray 2.60 | Chimaera |
|---|---|---|
| `tiny_tasks`: 100,000 one-line calls submitted in a loop | 8.7 s | 5.2 s |
| `io_sleep`: 2,000 calls that sleep 100 ms | 18.2 s (one core held per sleeping call) | 6.9 s (moved to a quarter core by the CPU trial) |
| `mutate_arg`: writing into an array argument | 0 of 20: `ValueError`, read-only array | 20 of 20 |
| `edited_module`: a module edited between two runs | 8 of 8, 2.6 s | 8 of 8, 0.5 s |
| `big_arg` at `EVAL_SCALE=0.06`: a 200 MB array passed to 30 calls | failed: `OutOfDiskError` with the disk 52% full (Ray caps its object store at 2 GiB on macOS, and its disk check refused the spill) | 30 of 30, 0.8 s |

`big_arg` ran at 6% of its full 500 calls, because at full scale Ray would
spill about 100 GB here. With a debug peer, Chimaera ran `tiny_tasks` at about
7,000 calls/s, slower than Ray; every comparison above uses release builds.

Three nodes on one machine (`bench/evals/gotchas/cluster.py`): Ray 2.60's
`ray.cluster_utils` cluster against three Chimaera peers, each node 4 CPUs in
its own zone, the driver on the first; release builds; each cell is the median
of three runs, each run the median of 3 repetitions after a warmup:

| Workload | Ray 2.60 | Chimaera |
|---|---|---|
| A 32 MiB array passed straight to 30 calls spread over the zones | 0.57 s (one run 10.3 s) | 0.23 s; stored once, fetched once per peer |
| 10 calls on a 64 MiB value made in zone a, no placement given | 0.10 s; 7–8 of 10 calls in zone a | 0.10 s; 10 of 10 in zone a |
| 30,000 one-line calls submitted in a loop | 3.9 s | 3.4 s (within run-to-run spread) |

This is not a multi-machine measurement: every node shares one host's cores,
memory and loopback network. A peer binary built earlier the same day lost 17
of 120,000 tiny calls twice: each was granted to a peer and never ran, or ran
and never finished, so the driver waited forever. With the current tree, 360,000
calls in three runs all finished. The spillback work landing in `core.rs` now
changes that path. Likely cause, not proven (that build's source is gone): a peer handing back unstarted work (`Core::requeue`) drops it from its queue before the shard accepts the hand-back and never restores it if the shard refuses, leaving a grant no peer holds. Fixed since: the work stays queued until the shard accepts the hand-back (see the M2 table).

## M6 Fleet emulation

`cargo run --release -p chimaera-fleet` (emulation): one 512-peer cell in
detail, scaled by cells, driving the real `Swim`, `ShardState::apply` and
`cell::summarize`. 5 peers killed mid-run.

| Gate | Result | Evidence |
|---|---|---|
| Appendix rows at 100,000 and 262,144 peers within 10% | pass (emulation) | every row within 1.5% (items, range grants, ledger records, heartbeats, summaries, cells) |
| Weak scaling 1k → 2k peers ≥ 1.8× | pass (emulation) | ×2.00 |
| Per-peer SWIM state ≤ 16 KiB | pass (emulation) | 12,864 bytes for a 512-peer cell, after: table rows packed to 20 bytes (`Entry`), vectors grown by an eighth instead of doubled, gossip space returned once drained, and members known from the plan no longer gossiped (gossip carries news only). It was 45,280 bytes |
| First binding limit reported | pass (emulation) | SWIM state per peer at 78.5% of its budget; a cell summary is at most 210 bytes (budget 2,048) |

Death detection at the appendix's 5 s heartbeat took 77–92 s for every live
peer to see a death (no false dead views). Real peers probe every 300 ms.
This is emulation, not a fleet.
