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
| Peer killed during a 10,000-call run; restart; attach sees every result once | pass (local) | SIGKILL at ~30% done; after restart and `ch.runs.attach`: 10,000 results, 10,000 distinct |
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
Python 3.11, full workload sizes (`EVAL_SCALE=1`). One run per workload: these are single
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
sizes. Working tree diff
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
meanwhile). Rerun with both fixes (tree diff `e6e2bad21fbdee8d`), same machines and sizes:

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
tasks and actor calls with raft fsync off (same machines, single samples):

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

## Ray comparison (cluster, 1, 4, 16 and 64 nodes, 2026-11-22)

Same method as the runs below: c7i.4xlarge machines, one Chimaera peer per
machine started through Ray, at least 60 s of repetitions per workload (45 s
at 64 nodes), median shown; node loss runs once. Built from the shared tree
at 06:46 (the commit that adds this section). The 64-node job reached its
hour before Ray's actor, nested, shuffle and node-loss passes; those Ray
figures (marked *) are from two separate runs (actors; nested, shuffle and node loss) on the same
machine type and Ray.

| Workload | 1 node Ray | 1 node Chimaera | 4 Ray | 4 Chimaera | 16 Ray | 16 Chimaera | 64 Ray | 64 Chimaera |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Independent no-op tasks (/s) | 6,432 | 13,548 | 6,763 | 16,450 | 5,805 | 15,373 | 5,188 | 14,203 |
| CPU scaling efficiency | 0.704 | 0.693 | 0.719 | 0.735 | 0.698 | 0.710 | 0.656 | 0.605 |
| Map of tiny items, naive (/s) | 6,489 | 2,031,911 | 6,723 | 905,159 | 5,676 | 820,232 | 5,171 | 774,908 |
| 512 MiB value to 2 x CPUs readers (s) | 0.010 | 0.009 | 0.025 | 0.032 | 0.106 | 0.110 | 0.407 | 0.375 |
| 32 MiB array passed straight to 2 x CPUs calls (s) | 0.058 | 0.009 | 0.214 | 0.043 | 0.928 | 0.208 | 3.507 | 0.713 |
| 10 calls on a 256 MiB value just made (s) | 0.227 | 0.310 | 0.229 | 0.270 | 0.233 | 0.262 | 0.228 | 0.376 |
| Actor calls (/s) | 7,528 | 14,073 | 8,809 | 14,433 | 7,947 | 14,339 | 9,482* | 10,973 |
| Nested tasks, 4 x CPUs parents of 8 (s) | 0.176 | 0.058 | 0.319 | 0.223 | 0.486 | 0.395 | 1.29* | 0.824 |
| Shuffle, 3.2M pairs (s) | 0.235 | 0.441 | 0.145 | 0.449 | 0.141 | 0.472 | 0.135* | 0.842 |
| A node killed under 4 x CPUs one-second tasks (s) | 8.22 | 5.62 | 18.59 | 11.62 | 18.25 | 8.23 | 18.5* | 9.58 |

Where Ray is still ahead: the shuffle at every size (1.9x on one node, about
3x at 4 and 16, 6x at 64); calls on a value just made at every size (12% to
65%); CPU scaling on one node (2%) and at 64 (8%); broadcast at 4 nodes and,
within 4%, at 16. Chimaera is ahead on everything else, including tasks on
one machine, which went from 4,787/s to 13,548/s: a finished call no longer
scans every running batch on its peer, nor sends a cancel to the executor of
each batched call that had already finished.

Run spreading, same machines and pass order, with `CHIMAERA_RUN_SPREAD=1` (off)
against the default:

| Workload | 4 off | 4 on | 16 off | 16 on | 64 off | 64 on |
|---|---:|---:|---:|---:|---:|---:|
| Tasks (/s) | 15,012 | 16,450 | 13,500 | 15,373 | 10,770 | 14,203 |
| Nested (s) | 0.216 | 0.223 | 0.898 | 0.395 | 4.328 | 0.824 |
| Shuffle (s) | 0.442 | 0.449 | 0.548 | 0.472 | 0.790 | 0.842 |
| Actor calls (/s) | 12,897 | 14,433 | 13,433 | 14,339 | timed out | 10,973 |

With spreading off, the peer leading the run's group used 2.3 to 3.1 cores
in tasks and nested at 16 and 64 nodes; with it on, the busiest used 1.0 to
2.2. The shuffle submits too slowly to spread (the rate rule keeps it in one
group), so its difference is noise.

An earlier round the same night ran a tree with two faults, both fixed
in the tree above: a peer whose core was slow to answer a ping was taken for
one shutting down and lost its voter seats under load, and a large result's
buffers were freed only with its run (follow-input filled one node's memory
and hung). Its Chimaera numbers are not used here; its Ray actor-call figure
at 64 nodes is (Ray was not affected).

## Ray comparison (cluster, 1 and 16 nodes)

c7i.4xlarge machines (16 vCPU each), Ray from the image against one Chimaera
peer per machine started through Ray (`bench/evals/multi_node.py`). Each
workload ran for at least 60 s after an uncounted warm-up; the median of the
repetitions is shown. Peers chose raft sync themselves (replicas only on this
volume: fsync p50 3.9 ms). Tree diff `4805bb8acb567ec2`
on `b6c71a5`.

| Workload | 1 node: Ray | 1 node: Chimaera | 16 nodes: Ray | 16 nodes: Chimaera |
|---|---:|---:|---:|---:|
| Independent no-op tasks | 6,285/s | 4,787/s | 5,644/s | 13,785/s |
| CPU scaling efficiency | 0.703 | 0.695 | 0.716 | 0.729 |
| Map of tiny items, naive | 6,427/s | 2,020,917/s | 5,432/s | 1,026,548/s |
| 512 MiB value to 2 x CPUs readers | 0.010 s | 0.010 s | 0.097 s | 0.079 s |
| 32 MiB array passed straight to 2 x CPUs calls | 0.054 s | 0.010 s | 0.921 s | 0.078 s |
| 10 calls on a 256 MiB value just made | 0.223 s | 0.239 s | 0.229 s | 0.237 s |
| Actor calls | 7,893/s | 13,246/s | 8,184/s | 14,183/s |
| Nested tasks (4 x CPUs parents, 8 children each) | 0.179 s | 0.063 s | 0.464 s | 0.799 s |
| Shuffle, 3.2M pairs | 0.226 s | 0.496 s | 0.137 s | 0.634 s |
| A node killed under one-second tasks | 7.95 s | 5.56 s | 21.25 s | 6.76 s |

Where Ray is ahead: tasks on one machine (1.3x), shuffle on both (2.2x and
4.6x), nested at 16 nodes (1.7x); follow-input is within 7%. At 16 nodes
Chimaera put all 128 actors on one host (Ray: 16), which these trivial
methods did not mind but CPU-heavy ones would.

Fixed since the earlier 16-node run (where broadcast took 6.39 s and the large argument 0.389 s): values a driver
uploaded now carry their holder, so placement keeps quick readers there; tuple
results are stored value by value so a split value decodes alone; a cluster
of one grants at submission; local-first grants for calls made inside tasks.
An earlier 1-node run hung on the large-argument workload (an arena leak in
/dev/shm), now fixed. Before those, a 16-node attempt filled /dev/shm before
spilling began and retired the driver's peer after a 2 s stall; both fixed
(spill on free space, 6 s suspicion floor).

## Ray comparison (cluster, 64 nodes)

64 x c7i.4xlarge (1,024 vCPU), same
method with at least 45 s of repetitions per workload, tree diff
`40e2fe4826e0cf5b` on `b6c71a5`.

| Workload | Ray | Chimaera | Better |
|---|---:|---:|---|
| Independent no-op tasks | 5,341/s | 9,579/s | Chimaera, 1.8x |
| CPU scaling efficiency (1,024 CPUs) | 0.674 | 0.633 | Ray, 6% |
| Map of tiny items, naive | 5,197/s | 1,016,057/s | Chimaera |
| 512 MiB value to 2,048 readers | 0.391 s (p90 19.6 s) | 0.365 s | Chimaera |
| 32 MiB array passed straight to 2,048 calls | 3.50 s | 0.391 s | Chimaera, 9x |
| 10 calls on a 256 MiB value just made | 0.228 s | 0.249 s | Ray, 9% |
| Actor calls, 512 actors | 9,005/s | failed: timed out creating an actor | Ray |
| Nested tasks, 4,096 parents | 1.29 s | 5.23 s | Ray, 4x |
| Shuffle, 3.2M pairs | 0.135 s | 1.149 s | Ray, 8.5x |
| A node killed under 4,096 one-second tasks | 18.5 s | 20.5 s | Ray, 10% |

Found since: actor placement let a group leader win every tie on idle peers,
and all of a run's actors share one group, so a burst landed on one host (at
16 nodes all 128 did); placements now count against their host for 10 s.
The actor-admission retry rescanned every waiting actor against the whole
membership every 100 ms, starving the core loop when hundreds waited (fixed
by the layered-api session). A call's missing inputs are now fetched
concurrently (a shuffle gather reads 64). Per-run throughput falls from
13.8k/s at 16 nodes to 9.6k/s at 64: every call of a run goes through one
raft group's leader, which the design's partitioning of runs across groups
(not built yet) would remove.

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
| A finished run's stored arguments go when it ends; its results stay | pass (local) | `test_values.py`: after the script exits, its 16 MiB argument is deleted within 15 s and its 2 MiB result is still read through `ch.runs.attach`. Fails on the previous peer, which kept both for the 24 h retention |
| `ch.put` returns a finished future read by reference | pass (local) | `test_values.py` |
| Memory is learned after three calls and survives a peer restart | pass (local) | `test_learning.py` |
| A memory kill raises later reservations, then fades as calls succeed | pass (unit) | `profile.rs`: killed at 900 MiB among 100 MiB calls: 900 MiB reserved (not 1.5× it, capped by 4× usual use), 500 after 8 successes, 400 after 16, back to 120 after 56. Before, the bound held at 1.5× until 64 more calls pushed it out, then vanished at once |
| Calls wait for memory instead of overcommitting it | pass (local) | `test_memory.py`: 10 calls of 300 MiB under a 1 GiB pool: at most 3 at once while unmeasured, at most 2 once learned |
| Nested calls do not deadlock on a small pool | pass (local) | `test_memory.py`: 6 parents × 3 children under 512 MiB |
| A leaking worker is replaced | pass (local) | `test_memory.py`: 40 MiB leaked per call; the worker changes and a `leaks` decision is recorded |
| Memory the allocator kept after frees goes back before the leak check | pass (Linux container) | `test_memory.py::test_memory_a_call_freed_goes_back_to_the_system`: 400k 1 KiB blocks with every 50th kept leave the worker at 423 MiB without `malloc_trim`, under 200 MiB with it; fails with the trim disabled |
| NumPy arrays in the Linux arena count toward memory | pass (Linux container) | Arrays allocated as tmpfs files (`.arena/<pid>`) are not private memory: each worker is charged its arena files, the plane's pre-zeroed reserve counts as object memory, and the leak check counts shared memory held past the arena's 256 MiB reuse cache. `test_memory.py` actor charge (300 MiB actor was charged 81 MiB before) and leak tests; `test_values.py` stored-once now counts files by inode, since the arena links one file under a name per use |
| A call queued at an actor whose method waits says how to overlap them | pass (local) | `test_learning.py`: after 12 calls of a 100 ms sleep, a call behind 25 others suggests `@ch.method(concurrency=N) on fetch`. Every call of a batch now counts toward its function's profile (at most 64 per batch), not only the last |
| New calls wait when the objects' disk is nearly full | pass (unit; Linux container) | `budget.rs` tests: below min(512 MiB, a tenth of the filesystem) free, new calls wait with a reason naming it; on a 700 MiB tmpfs object directory with 59 MiB free a new call waited while the running one finished |
| A `ch.put` value is freed when nothing needs it | pass (local, cluster) | Passed to calls, it used to stay pinned for the whole run on every peer that fetched it. Now it is released once its future is dropped and the calls that read it have finished (the runtime holds it until each reader's outcome): `test_values.py` (fails with the old pinning), and `test_locality.py` checks a fetching peer's copy is deleted too. A put that escaped as a handle (nested deeper, or sent to an actor) stays for the run |
| A fresh array per iteration does not pile up | pass (Linux container) | On Linux an array passed to calls is linked into the run, not hashed. Each new array was a new link kept until the run ended, so the 16-node eval's 60 s loop of fresh 32 MiB arrays filled /dev/shm and hung. A link is now dropped once its array is collected and the calls that read it have finished, here and on peers that fetched it, as a `ch.put` value is. Links passed to actors, or made inside a task, still last for the run. `test_values.py::test_a_fresh_array_per_iteration_does_not_pile_up`: 12 iterations of 4 MiB keep at most 2 stored; it fails when links last for the run. The eval's `naive_big_arg` for 60 s on one container peer with a 2 GiB /dev/shm: with links kept for the run, /dev/shm filled within 15 s and the run failed for lack of space; now 3153 reps (p50 4 ms) with /dev/shm between 88 and 129 MiB, against 1142 reps (p50 44 ms) with the arena off |
| A forked child neither shares its parent's connection nor deletes its values | pass (local; Linux container) | A child of `os.fork`, a fork-mode multiprocessing pool or a DataLoader worker shared the parent's session stream (on macOS its call even returned, but either process could take the other's events). Dropping its copy of a `ch.put` value unlinked the parent's stored files. Now any submit, wait or fetch in the child raises CH113, and finalizers in a child leave the parent's files alone. `test_values.py`: both tests fail without the after-fork hook |
| A CUDA tensor is rebuilt where its reader is | pass (stand-in torch module) | It travels as a host array and lands on the reader's granted GPU, or in a driver on its current one; a reader with no GPU gets it on the CPU with CH112. Torch's own pickling names the writer's device index, which fails in a CPU call. `test_values.py::test_a_cuda_tensor_is_rebuilt_where_its_reader_is` fails with the old fallback. Not yet run on a GPU; three checks are in `bench/evals/gpu_check.py` for the next GPU job |
| A version mismatch with a remote cluster is named | pass (local; the Python mismatch is simulated) | Workers learn the program's Python version from its run. When a function will not load on a worker running another version (remote clusters run their own Python), the error names both versions and the fix (CH115). A remote cluster on another Chimaera version warns once at `ch.init` (CH116). `test_defaults.py`, `test_cluster.py` |
| A gather that outgrows the machine warns | pass (local; Linux container) | `ch.gather` warns once (CH117) when the results it has read pass a quarter of physical memory, counting arrays' bytes because a Linux arena result's stored size is only its frame. It points at `ch.as_completed` and `fn.map`. `test_values.py` |
| A long-lived driver's argument blobs do not accumulate | pass (local; Linux container) | Blobs stored by content (arrays on macOS or without the arena, bytes, large Python objects) lasted until the run ended. In a soak, the run grew 4 MiB per wave of fresh 4 MiB arrays. Now a blob is held while its array lives or a pending call reads it, then deleted here and released on the peers that fetched it. A blob whose owner cannot be watched (bytes, lists) waits among the recently freed, up to 256 MiB, so sending the same object again still stores it once. A peer now reports a released input only after the fetch fails, because the same bytes sent again get the same id. Tests: `test_values.py::test_fresh_values_per_iteration_do_not_pile_up` (12 stored arrays when blobs last for the run) and `test_locality.py::test_bytes_sent_again_after_their_release_reach_a_peer_that_had_them` (fails with the old up-front check). Soak: 296 waves, run directory flat at 0 MiB. Because a lost result is made again by running its call, a call's inputs also stay while its future lives, and a call keeps the futures it depends on until its outcome (lineage; `test_values.py::test_a_calls_inputs_stay_while_its_result_is_held`). The runtime's event loop no longer holds the last finished future after handling it; it had kept one dropped future, and now its inputs, alive until the next event |
| A busy peer's memory stays bounded | pass (local soak, release peer) | `heap -showSizes` on a peer after 700k calls showed exactly 200,000 blocks of 3 KB plus their parts, about 700 MB: the attempt log kept for `explain()`, capped only by count (ATTEMPT_LOG_CAP). It is now also capped at 8 MiB encoded, about 25,000 recent attempts. Physical footprint after 750k tiny calls: 833 MB before, 213 MB after. Together with the runtime owner's change keeping only a released call's outcome, a 1.3M-call mixed soak keeps peer memory flat rather than about 1.2 KB per call. The modules that use `explain()` pass (accounting, api, learning, memory, defaults, locality, compat, gang) |
| Under a pod's task limit, calls wait for workers instead of failing | pass (unit; Linux container) | `pids.max` counts every thread. Starting more workers than fit failed with EAGAIN, and the calls were lost as "3 attempts lost to infrastructure failures; last: executor process exited". The peer now reads the tightest limit up its cgroup chain (`topology::pids_room`) and starts a worker only if it has room for it (4 tasks plus its threads; measured 3). Otherwise calls wait with a reason naming the limit, and `chimaera status` lists it. `topology.rs` unit test; `test_defaults.py::test_under_a_task_limit_calls_wait_for_workers_instead_of_failing` starts a peer in a cgroup with room for two workers: 2/2 pass, and 2/2 fail with the old behavior. Manual: `docker run --pids-limit=24`, 32 calls in 6.7 s |
| More lints: large Python collections, hidden GPUs, functions defined per call | pass (local; Linux container for the first) | CH118: a list, dict, set or tuple that pickles to 8 MiB or more, passed to calls or stored with `ch.put`, warns once per site that every reader unpickles a copy, unlike a NumPy array or Arrow table (`test_values.py`). CH119: a CUDA "no device" error in a call that asked for no GPU, on a machine with GPUs, gets a note saying how to ask for one (`test_defaults.py`, simulated). CH120: the same code sent as 100 distinct functions warns once to define it once; the digest cache is bounded at 1,024 functions (`test_defaults.py`). Also fixed: a `ch.put` handle returned by a call was forwarded like a call's future and never resolved; it is now stored as the value (`test_values.py`, which failed every time before) |
| A result's separately stored buffers are freed, placed by and fetched with it | pass (Linux container) | On Linux a call's large arrays are linked from the arena into files of their own, and the outcome named only the frame (often a few inline bytes). Releasing a result freed nothing: a 1-node cloud run looping over fresh 256 MiB results filled the pool and killed the peer. Placement saw no large input either, so follow-input, large arguments and broadcast slowed at 16 and 64 nodes. Outcomes now list those files (`ObjectRef.blobs`, with sizes, the writer named as holder). Release deletes them here and on peers that copied them; placement counts them; a peer fetches them before the call; output sizes are learned with them. `test_values.py::test_large_results_released_mid_run_free_their_files` (eight 32 MiB results; "8 results still stored" when release skips them). `test_locality.py` result-locality test: 3/3, flaky before |
| A worker returning large bytes or str keeps none of them | pass (unit; Linux container) | `_serde` pickled a large top-level bytes or str through a class made per call. A class sits in a reference cycle, so each value stayed alive until the next full garbage collection, which an idle worker rarely runs: on Linux an arena file per call. A loop making a fresh 256 MiB bytes result filled a 4 GiB /dev/shm within a minute; in the cloud, follow-input's median ran 12–65% behind its best reps. Now one plain class, freed when pickling ends. The same loop for 60 s: 137 reps, p50 0.335 s, p90 0.425 s, no worker restarts, /dev/shm flat. `test_values.py::test_pickling_large_bytes_or_str_leaves_nothing_holding_them` (one extra reference with the old code) and `::test_a_worker_returning_large_bytes_keeps_none_of_them` (the old code kept all 24 values, 1,536 MiB, in one worker's arena) |
| An async actor's awaiting calls hold no threads | pass (local) | An async method in a lane (actor `concurrency` > 1) used to block a lane thread on the coroutine's result, one OS thread per awaiting call. Now its arguments are read in a lane thread, the coroutine runs on the actor's event loop, and the outcome is encoded and sent from a lane thread (`Executor.start_async`, `Executor.finish`). Starting and finishing run on 4 threads, not the lane's pool, which a burst grew toward its size. 500 calls awaiting 3 s: 505 threads before; 1,500 calls: 9 threads after. `test_defaults.py::test_an_async_actors_awaiting_calls_hold_no_threads` (205 threads with the old dispatch); actors, admission, compat, gang and workflow modules pass (95) |
| In-memory objects spill before their filesystem fills | pass (Linux container) | Objects also move to disk when their tmpfs has under a fifth free, not only past 80% of the pool (/dev/shm defaults to half of RAM, smaller than the pool). 3 peers, objects on a 700 MiB tmpfs, 25 rounds of a 64 MiB `ch.put` read by 24 calls: failed within 4–17 rounds before, 25 of 25 in three runs after (arena on and off) |
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
| Quick readers of a `ch.put` value stay on its peer | pass (local, 4-peer localhost) | Put values and argument blobs carried no holder, so every group leader except the one on the driver's peer saw all peers, the holder included, missing them and placed by load alone. With 4 peers × 4 CPUs, a fresh 128 MiB put and 32 one-millisecond readers, the readers split 8/8/8/8 in every rep (0.46–0.83 s). Now, when calls are submitted, the driver's peer names itself as the holder of the inputs it stored: all 32 readers run on the holder from the second rep, in about 0.2 s. The first rep still spreads partly, because an unmeasured function's duration is assumed to be 1 s. On Linux, a put of an arena array is a small inline handle that links the array's file. That file is now listed among the call's inputs too, so it is placed by and fetched before the call, not on first read inside it. `test_locality.py::test_quick_readers_of_a_put_value_stay_where_it_is` learns the duration on one value, then reads a fresh one only the driver's peer holds. It asks for at least 20 of 24 readers on the holder and passed 3 of 3 on macOS and in the Linux container; without the stamp, 4 of 24. An earlier version read the value it had learned on. Its first calls, run before the duration was known, had copied that value to other peers, so spreading by load was then correct and the count varied. Found by the 16-node broadcast eval (6.39 s against Ray's 0.087 s); not yet rerun there |
| Calls reading one `ch.put` value do not wait for driver room | pass (local) | A stored value was counted in every pending call's argument bytes, so 12 calls on one 16 MiB put under a 48 MiB limit waited and warned CH110. It now counts only as an input. `test_defaults.py::test_calls_reading_one_stored_value_do_not_wait_for_room` fails with the old count |
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
