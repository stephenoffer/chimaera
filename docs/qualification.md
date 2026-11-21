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

Open in M1: streaming generator calls, the mypy plugin, the Linux dispatch
measurement.

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
| Kill ingress and shard leader during a 100k-item map, resume p99 < 10 s | unverified | the localhost test uses 300 calls; the 100k-item map and resume percentiles are not measured yet |
| Killing 1 of 3 voters keeps admission at ≥ 80% throughput | unverified | not measured |
| Simulation invariants over 10k seeds | unverified | no turmoil harness yet |
| Peer from the wrong CA refused (mTLS) | unverified | token auth only; mTLS not built |
| Durable actor command dedup, workflow replay | unverified | not built |

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
