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

Known limits at this gate: a value stored only on a peer that died was lost
(`DataLost`), and its task was not re-run to rebuild it; peers on real separate
machines had not been tested. Both have changed since: lost results of function
and map calls are rebuilt by running them again, and the Ray comparisons below
ran on up to 64 separate machines.

## How a comparison is recorded (from 2026-11-23)

The Ray comparison rounds below predate this: one run per workload and
runtime, in a fixed order, with medians and no raw samples. From 2026-11-23
the harnesses (`bench/evals/multi_node.py` on clusters, `local_cluster.py` on
one machine, `run_evals.py` on one node) write a manifest beside their numbers
(`bench/evals/manifest.py`):

- the code: a digest of the source files, plus the commit and a digest of the
  uncommitted diff where git is present. Cluster jobs upload the tree without
  `.git`; `python bench/evals/manifest.py` prints a checkout's digest to match.
- the peer binary's `--version`, its cargo profile and that profile's settings,
  rustc and RUSTFLAGS.
- each runtime's version and its drivers' Python.
- the machines (host, CPUs and platform of each) and the data groups.
- the durability class the peers report in `chimaera status --json`: the
  guarantee asked for, the weakest any group gives and each peer's raft sync.
  It is read once every peer gives what it asks for, or after 30 s: a peer
  reports a group as `process` until it learns the group's voters.

A report is refused when its manifest lacks the version of a runtime it
measured or the durability class of a Chimaera row. Every row keeps each timed
sample. A cell is the median over all of them with a bootstrap 95% interval
that resamples the runs, then the samples within each. Both runtimes stay up,
and each trial runs every workload once per runtime, the workloads and which
runtime goes first in a random order from a recorded seed (`EVAL_SEED` and
`EVAL_TRIALS` on clusters, `--seed` and `--rounds` on one machine).

What one unit of each throughput row is, and how Chimaera commits it:

| Row | One unit | Path |
|---|---|---|
| `tasks` | a no-op call, made in one burst with the rest, its result read | the client coalesces calls into submissions of up to 4,096 |
| `tasks_serial` | a no-op call made alone, its result read before the next | one submission per call: the per-call path, kept as a control. One call is in flight at a time, so the rate is the round trip, not throughput with coalescing off |
| `map_small` | one int doubled: in a 4M-item map (Chimaera), in 200k items run one task each (Ray, naive) or in 4M items run 1,000 per task (Ray, batched by hand) | Chimaera chunks the map's items into calls |
| `actors` | one method call on one of max(8, CPUs / 2) actors | straight to the actor's executor when it shares the driver's peer, the shard recording it after the reply; otherwise through the peer |
| `actors_peer` | as `actors`, Chimaera only | `CHIMAERA_DIRECT=0`: every call through the peer and its shard, kept as a control |

The report's last column counts the raft entries the peers' groups applied
per unit (per run for timed rows), read from every peer's status before and
after each run, and the share of actor calls that went direct. On three local
peers (debug build, `EVAL_SCALE=0.01`, local) `tasks` took 0.033 raft entries
per call and `tasks_serial` 4.0. The shuffle has two pairs: `shuffle`, each
runtime's idiomatic program with the map run inside the split (Ray's by hand,
Chimaera's `reduce_by_key` on a map nothing has read), and `shuffle_unfused`,
stage for stage, with the map's outputs stored first and read by separate
split calls. Both stop the clock with every partition in the driver, check
the output after it, and record a digest of the output for every run: in the
local run Ray and Chimaera gave the same digest for both.

The cluster `node_loss` row now also records `recovered_s` (kill to last
result, both runtimes) and `detected_s` (kill to the head peer's first
suspicion of the victim, Chimaera).

### Node loss, repeated (local)

`bench/evals/node_loss.py` starts a fresh cluster of four peers for each
experiment and SIGKILLs one peer other than the driver's: the one leading the
most shard groups (`leader`) or the fewest (`follower`). It kills when every
call has been made, when a call has started on the victim, or when half the
calls have started, under half-second tasks, durable actors and a 16-map
`reduce_by_key`. Every call attempt leaves a marker naming its call, its peer
and its start time. Times are seconds after the kill: when the driver's peer
first suspected the victim, when it retired it, when each call that had
started on the victim started again elsewhere, when the first of their results
reached the driver, and when the last result did.

2026-11-23, laptop shared with other heavy work, debug build, 3 seeds x 2
roles x 3 workloads x 3 kill points, 54 experiments. A `leader` victim led 3
to 5 of the 5 groups in 22 of its 27 experiments, 1 in one, and none in 4
(leadership had not spread from the driver's peer within 30 s); a `follower`
led none. Pooled over workloads and kill points, p50 [p90, max], as the
harness's report prints it (the report also gives p50 [min, max] for each
workload, victim and kill point):

| Victim | Detected | Retired | Lost calls started again | First of their results | Last result |
|---|---:|---:|---:|---:|---:|
| `leader` | 0.15 [0.28, 0.35] | 1.25 [1.52, 1.55] | 3.42 [4.09, 4.68], 35 calls | 3.60 [4.15, 4.59] | 4.02 [5.18, 6.05] |
| `follower` | 0.17 [0.34, 0.64] | 1.39 [1.50, 1.55] | 1.51 [2.02, 2.60], 32 calls | 1.91 [2.22, 2.72] | 2.10 [3.90, 4.21] |

Every result reached its driver exactly once in all 54. Restarting a leader's
calls took about 2 s longer than a follower's. Detection is fast here because
a killed peer's port refuses connections at once on loopback; a machine that
vanishes from a real network is noticed only through missed probes, so these
detection times are a lower bound. Repeating this across machines is
unverified. In an earlier run of the same matrix, one durable actor call of 54
experiments failed with `InfrastructureFailure` ("commit refused:
VersionConflict state version moved") after the actor's peer, which led 5
groups, was killed while the call ran. The 54 above did not reproduce it, and
it is open.

## Lanes, baselines and the release manifest (from 2026-11-24)

The gates run in lanes. The quick lane (`.github/workflows/check.yml`, on
every push to main and every pull request) is `make check` on Linux and
macOS, and `make regress` on Linux: three local release peers run eight
workloads (`bench/evals/regress.py`) at a pinned size, seed and number of
rounds. The run fails when any run failed its own output check, an output
digest differs from the baseline's, the pins or the build profile differ, or a
median is worse than the baseline's by more than its tolerance (30%, or the
baseline's own p10 to p90 spread, up to 90%). Timings compare only on one
kind of machine, so each machine keeps its own baseline in
`bench/evals/baselines`, written by `make regress-record`. None is recorded
yet; the check workflow's `record_baseline` input records one for its runner.
The slower lanes (`.github/workflows/integration.yml`, nightly) run
`make linux-check linux-pytest` in the dev image, each launcher gate in
Docker and the fleet emulator's gates (`chimaera-fleet --weak-scaling`, which
exits 1 when one fails), and, by hand, the GPU check and the 4-node
comparison as Anyscale jobs. Neither workflow has run on GitHub yet.

Each lane has a mutation proof that it fails for the reason it should:

- `test_regress.py::test_a_corrupted_result_fails_the_quick_lane` runs the
  lane on a copy of the package whose decoder returns an int value one too
  high. Every round of `tasks` fails its output check and the lane exits 1,
  while the unmodified copy records a baseline that its own run passes
  (local, debug peer). Without the lane, `local_cluster.py` exits 0 on the
  same mutant and only its report's cells show the errors.
- `make kind-mutant-test` gives kind-test an image whose `chimaera k8s render`
  mounts the peers' volume at /srv/data instead of /data, which the render's
  unit tests do not check. On 2026-11-24 (local, colima VM) kind-test passed
  on the base image and failed on the mutant, its pods never Ready, so the
  proof passed. Both ran inside the VM, because its port forwarding did not
  reach kind's API server from macOS that day.

A release's qualification manifest (`make qualification
QUALIFICATION_KEY=<ssh key>`, `bench/qualification/build_manifest.py`) lists
each claim of speed, scale and fault tolerance the README makes, each row of
its status table and each measured section of this record, as
`bench/qualification/claims.toml` lists them: the claim's tier, its raw
artifacts with their sha256, the command that replays it, and why it is
pending when it is not traced. The manifest also names the lanes that passed
on the release's source, the gates here that are unverified or failing, and
the limits tested (a limit whose claim is not traced says why). The build is
refused, and `test_qualification_manifest.py` fails, when a README claim or a
measured section here has no entry, a quoted figure changed, or an artifact's
digest changed.

Each CI job records its lane with `build_manifest.py lane` (the command, its
outcome, the source digest, a link to the Actions run, and the log) and
uploads the record as an artifact named `lane-*`. Before a release, collect
each run's records with `gh run download <run-id> -p 'lane-*' -D
dist/qualification/lanes/<run-id>`. A lane traces its claims only when its
latest record on the release's own source passed: the same source digest
and, since that digest leaves out the tests, `deploy/` and CI, the same
commit with nothing uncommitted on either side. So build the manifest in a
clean checkout of the commit the lanes ran on. The Anyscale jobs print their
raw results (`multi_results.json`, `multi_cost.json` and `multi_report.md`,
or the GPU check's `checks.jsonl`) into the job's log with `build_manifest.py
emit`, and the hardware lane rebuilds them from its log as files of its
record (never as a lane record) and fails when one it expects (`lane
--expect`) is not there. So a cloud round's results reach the manifest
without anyone copying them out of the cloud's storage. `make qualification
QUALIFICATION_KEY=<key> RELEASE=<tag>` signs the manifest with `ssh-keygen -Y
sign` and uploads it, its signature and `qualification.tar.gz` (the manifest
and every lane record, log and rebuilt file) to that GitHub release. A reader
unpacks the tarball in a checkout of the tag and runs `build_manifest.py
verify --signers <allowed_signers>`, which checks the signature, re-hashes
every artifact the manifest names, and fails on one that changed or is
missing (unless `--allow-missing`).

Today only the container memory traps are traced to raw artifacts kept in the
repository. The earlier cloud rounds' raw results stayed in the jobs' storage,
so their entries stay pending. A section published from a hardware lane's
round keeps that lane's rebuilt files in `bench/evals/results` and lists them
as its artifacts. No release key has been chosen yet.

### Cost per workload

From the next cluster round, `multi_node.py` records each machine's instance
type and region (EC2 instance metadata) and writes `multi_cost.json` beside
its report. For each workload and runtime it gives the instance-hours (the
wall time of its runs times the machines held), the dollars, allocated
core-hours and GPU-hours at the prices in `bench/evals/prices.json` (AWS
on-demand list prices, dated, carried in the result), and the useful
completions at one SLO for both runtimes. A repetition counts only within its
workload's bound in `bench/evals/cost.py`, set about 5x worse than the slower
runtime's 64-node median, and a failed run counts its hours and nothing else.
`python bench/evals/cost.py multi_results.json` recomputes it from the raw
results, with `--instances c7i.4xlarge=64` for a run that did not record its
machines. No round has been priced yet, because the rounds below did not keep
their raw results.

## Shuffle phases (local, 2026-11-24)

`bench/evals/shuffle_phases.py` runs the 3.2M-pair shuffle of the cluster
rounds (64 maps of 50,000 `(key, 1)` pairs into 64 partitions, the map run
inside the scatters) on three release peers on one laptop, with
`chimaera._shuffle_phases` recording each scatter's, the manifest call's and
each gather's own steps. Seconds add up over the calls, which run side by
side on the machine's 12 cores; the per-call median is beside them. Bytes
are the encoded buckets and partitions. Network is what the peers sent each
other for the shuffle, from their own counters
(`chimaera_mesh_sent_bytes_total{class="shuffle"}`), and spill what their
stores moved out of memory (`chimaera_spilled_bytes_total`, with the time it
took). The uniform keys are the benchmark's; the skewed ones are drawn from a
Zipf distribution (exponent 1.2) over the same 100,003 keys. Each is the
median of 3 runs after a warm-up, and every run's output matched the digest
of the same pairs counted serially. Neither run spilled. The seconds are
wall time, each call's own clock, not CPU time: the script records the
calls' CPU time too since this run (see the next section).

| Phase | Uniform: s | Per call | MiB | Zipf: s | Per call | MiB |
|---|---:|---:|---:|---:|---:|---:|
| plan (driver) | 0.004 | | | 0.004 | | |
| map | 0.215 | 0.003 | | 0.650 | 0.009 | |
| split | 0.278 | 0.003 | | 0.594 | 0.004 | |
| scatter encode | 0.145 | 0.001 | 17.6 | 0.079 | 0.000 | 3.7 |
| fetch | 0.826 | 0.001 | | 0.172 | 0.000 | |
| read | 0.839 | 0.007 | 17.6 | 0.416 | 0.002 | 3.7 |
| reduce | 0.743 | 0.006 | | 0.194 | 0.001 | |
| gather encode | 0.016 | 0.000 | 0.7 | 0.013 | 0.000 | 0.7 |
| final (driver) | 0.088 | | | 0.076 | | |
| network (peers) | | | 20.4 | | | 1.3 |
| spill (peers) | 0.000 | | 0 | 0.000 | | 0 |
| whole shuffle | 0.665 | | | 0.529 | | |

The uniform run's stages, in ms after the driver's start (median run;
first, median and last call):

| Boundary | First | Median | Last |
|---|---:|---:|---:|
| driver submitted the shuffle | 4.4 | | |
| scatter start | 3.5 | 84.5 | 245.4 |
| scatter split done | 9.3 | 101.0 | 250.7 |
| manifest start | 147.9 | | |
| manifest saw every scatter | 259.8 | | |
| manifest returned | 259.9 | | |
| gather start | 272.9 | 409.5 | 540.9 |
| gather fetched | 333.4 | 414.6 | 541.3 |
| gather merged | 342.9 | 438.2 | 573.6 |
| driver saw every partition done | 576.8 | | |
| driver held every partition | 664.6 | | |

The scatters take 240 ms to all start: 64 calls on 12 cores, in about five
waves. The manifest returns 9 ms after the last split, and the first gather
starts 13 ms after that. The gathers spread over 270 ms for the same reason
as the scatters, and the driver holds every partition 88 ms after the last
one is done. The skewed run is faster because its scatters fold the repeated
keys before anything is stored, which leaves 3.7 MiB of buckets against 17.6.
Its largest partition held 1,548 keys and the uniform one's 1,563. A recorded
scatter or gather encodes its result twice, once to time it, and every
recorded call writes one report, so a recorded run is slower than an
unrecorded one. The same phases on separate machines are still to do, in a
cloud round.

## Shuffle phases, larger than memory and spilling (Linux container, 2026-11-24)

Two runs of the same script in the dev image (`deploy/dev/Dockerfile`), each
in a container held to 1 GiB (`docker run --memory 1g --memory-swap 1g`, 4
CPUs), with two peers of one core each. The peer and `chimaera._native` were
built there in the dev profile (opt-level 1) without debug info: the release
profile's single-unit LTO link was killed for memory in the shared Docker VM.
It's slower than a release build, so read the shares, not the seconds. Each
artifact's manifest records the container's limit, its `memory.peak` (which
counts the page cache of the objects written, so it sits at the limit), its
OOM kills, the size of `/dev/shm` and each peer's memory pool. Both outputs
matched the digest of the same pairs counted serially, which the driver takes
a partition at a time and never holds whole, and neither container had an OOM
kill.

Larger than memory: 2,400 maps of 200,000 `(key, 1)` pairs, 480M in all, into
256 partitions over 4,000,037 keys. The scatters stored 3.16 GiB of buckets,
3.2 times the container's memory. The container's `/dev/shm` is the default
64 MiB, so the stores kept every object on disk and spilled nothing.

Spilling: 300 maps of the same pairs, 0.40 GiB of buckets, with a 2.5 GiB
`/dev/shm` (`--shm-size 2560m`), so the stores keep objects in memory, and
each peer's pool set to 320 MiB (`CHIMAERA_MEMORY_POOL`). The stores moved
211 MiB of objects to disk, in 0.42 s.

| Phase | Larger: s | CPU s | GiB | Spilling: s | CPU s | GiB |
|---|---:|---:|---:|---:|---:|---:|
| plan (driver) | 0.1 | | | 0.04 | | |
| map | 72.3 | 60.0 | | 8.5 | 7.2 | |
| split | 103.7 | 94.6 | | 12.1 | 11.3 | |
| scatter encode | 24.2 | 22.3 | 3.16 | 2.7 | 2.5 | 0.40 |
| fetch | 8.1 | 6.9 | | 0.6 | 0.5 | |
| read | 1,845.9 | 691.1 | 3.16 | 30.6 | 17.7 | 0.40 |
| reduce | 164.2 | 152.4 | | 12.8 | 12.4 | |
| gather encode | 1.0 | 0.9 | 0.03 | 0.1 | 0.1 | 0.02 |
| final (driver) | 4.6 | | | 3.0 | | |
| network (peers) | | | 0 | | | 0 |
| spill (peers) | 0.0 | | 0 | 0.42 | | 0.21 |
| whole shuffle | 1,220.7 | | | 47.5 | | |

Seconds add up over the calls; CPU seconds are the CPU the calls' threads
spent. In the larger run the scatters ran from 0.02 s to 154.7 s, the
manifest returned at 154.8 s, the gathers ran from 154.8 s to 1,204.1 s, and
the driver held every partition at 1,208.7 s. Reading the buckets is most of
the gathers' time, and most of the reading is waiting: each gather reads 2,400
buckets from disk, and the container's page cache holds less than a third
of what was stored. The peers also paused admission for memory pressure
through the run: with the page cache of the objects they wrote, the
container's memory stayed near its limit. With two peers, every stored object is copied to the other peer in
the background (the default durability), so each gather found its buckets in
its own store and the shuffle sent nothing between the peers; those copies are
the peers' checkpoint traffic, which these counters leave out. No gather grew
its process's private memory: each reused memory its executor had freed.

The larger run's size with objects in `/dev/shm` (the spilling run's settings)
did not pass: it was OOM-killed after 16 minutes, with the container's shared
memory sampled near 500 MiB and its anonymous memory near 600 MiB. That case
is open.

## The partition count on a cluster (to run)

Without `partitions=`, `reduce_by_key` makes one partition per input up to
the cores of all the live peers, not only the script's, and more when inputs
already stored would not fit a reducer's memory (docs/guides/data.md). The
check on real hosts is still to do: start 64 peers, then on the smallest
host, a 2-core one with `CHIMAERA_HOME` naming its peer, run

    .venv/bin/python bench/evals/shuffle_phases.py --join --nodes 64 \
        --metrics <each peer's CHIMAERA_METRICS_LISTEN, comma-separated> \
        --maps 4096 --partitions 0 --memory 2GiB --rounds 3 --name cluster-partitions

The script fails unless `reduce_by_key` chose as many partitions as the
cluster has cores, and a gather that grows past `--memory` fails the run.
The report gives the most any gather grew. On one machine, `--nodes 3
--cpus 4 --driver-cpus 1 --partitions 0 --memory 64MiB --maps 16` runs the
same check with a one-core peer for the script: 9 partitions over 9 cores.

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

Since 2026-11-23 cluster peers sync every commit unless `CHIMAERA_RAFT_SYNC=none` is set, slow volumes included (commits are held open up to 2 ms to batch them), so throughput numbers measured after that change are not comparable with earlier ones, including the hosting and nested-tree numbers in `docs/ray-gotchas.md` and `bench/evals/nested_tree.py`.

Holding commits open on a slow disk (laptop; the ignored test
`measure_group_commit_hold_on_a_slow_disk` in `chimaera-raft`: 16 writers in a
closed loop, every commit synced, each setting run at least twice). On the SSD
alone (2026-11-24) the test writes as a syncing peer does: each batch synced
in the raft log's write-ahead file, then put in redb unsynced. The slow disks
(2026-11-23) are the SSD with a sleep added to every sync. Those rows run
without the write-ahead file, every commit a synced redb transaction, because
the test cannot slow the write-ahead file's sync; the writer holds and times
commits the same way on both paths. A real network volume has not been
measured yet. To measure one, run the test with `TMPDIR` set to a directory
on the volume and `MEASURE_SYNC_US=0`, which takes the write-ahead path.

| Disk | Hold | Operations/s | p99 wait | Operations per commit |
|---|---|---:|---:|---:|
| SSD, write-ahead file | none | 127,600–154,800 | 0.22–0.47 ms | 8.3–8.5 |
| SSD, write-ahead file | up to 2 ms (never held) | 108,500–166,700 | 0.18–0.83 ms | 8.2–8.4 |
| SSD, +3.5 ms per sync, synced redb | none | 1,412–1,491 | 18.4–23.3 ms | 8.0 |
| SSD, +3.5 ms per sync, synced redb | up to 2 ms (held 1.4 ms) | 2,110–2,318 | 11.1–21.8 ms | 15.9 |
| SSD, +5 ms per sync, synced redb | none | 1,088–1,134 | 23.5–23.8 ms | 8.0–8.1 |
| SSD, +5 ms per sync, synced redb | up to 2 ms (held 1.9 ms) | 1,662–1,722 | 14.1–18.7 ms | 15.9 |

On the SSD alone, commits take 41–61 µs at p50, so the writer never holds a
batch and the setting changes nothing; the spread over four runs each is
other work on the laptop (load average 90–160). Holding helps only when
operations arrive together: a writer whose operations come one at a time has
nothing to wait for. The EBS run below committed 1.0 operations per commit,
so holding could not recover the cost of its syncs.

## Ray comparison (cluster, 2026-11-23): the fsync default, and an A/B on one build

A snapshot of the shared tree at 10:14 MST on 2026-11-23, same machines and
method as below (30 s windows in the A/B jobs). The tree carries a durability
change: every peer now syncs each raft commit
(`raft sync fsync (the default)` in the peers' logs), where the evening
round's peers measured their EBS volume (fsync p50 3.4 ms) and kept commits
in the page cache (`raft sync none`). Ray's control plane syncs nothing.
Rounds ran on 1, 4, 16 (out of time during Ray's passes) and 64 nodes, with A/B
jobs on 4 and 16.

With the fsync default, Chimaera fell far behind its own evening numbers:
tasks 13,588 → 1,033/s on one node and 12,096 → 2,768/s at 64; actor calls
13,040 → 7,104/s (one node) and timed out at 64; nested 0.937 → 9.31 s at 64.

The A/B runs each setting on the same build and cluster, Chimaera's default
first changed by one variable (median of 30 s, seconds unless marked):

| 4 nodes | `RAFT_SYNC=none` | default (fsync) | `OBJECT_REPLICAS=0` | `REPLICA_DELAY_MS=1000` | Ray |
|---|---:|---:|---:|---:|---:|
| Actor calls (/s) | 14,209 | 2,037 | 1,910 | 1,526 | 9,434 |
| Nested tasks | 0.209 | 2.240 | 2.330 | 2.179 | 0.295 |
| Broadcast | 0.035 | 0.107 | 0.095 | 0.096 | 0.024 |
| 10 calls on a 256 MiB value just made | 0.258 | 0.295 | 0.335 | 0.374 | 0.223 |
| Shuffle, 3.2M pairs | 0.173 | 0.299 | 0.296 | 0.293 | 0.153 |

| 16 nodes | `RAFT_SYNC=none` | default (fsync) | `OBJECT_REPLICAS=0` | `REPLICA_DELAY_MS=1000` | Ray |
|---|---:|---:|---:|---:|---:|
| Nested tasks | 0.329 | 6.412 | 4.451 | 2.934 | 0.409 |
| Broadcast | 0.117 | 0.331 | 0.423 | 0.349 | 0.106 |
| 10 calls on a 256 MiB value just made | 0.239 | 0.371 | 0.289 | 0.320 | 0.237 |
| Shuffle, 3.2M pairs | 0.171 | 0.290 | 0.294 | 0.293 | 0.145 |

- Syncing every commit costs 3-20x on these workloads on EBS; replicas,
  made at once or a second later, change little against that.
- With commits unsynced, as Ray's control plane is, Chimaera leads on actor
  calls and nested tasks at 4 and 16 nodes, is level on calls on a value
  just made at 16, and trails on broadcast (10% at 16) and the shuffle
  (13% at 4, 18% at 16: it was 2.8x and 3.4x the evening before).
- The shuffle's gain is the placement change of this round: a leader placing
  a call no longer copies every member's view (it cost a gather 1.8 ms and
  the 64-peer leader 63 copies per call), and gathers start about 40 ms after
  the last scatter instead of 120 (three local peers, per-call timeline).
- On a local disk an emulated 3.4 ms sync costs tasks only 25% (5,849 against
  7,856/s on one peer). On EBS the cost is IO count, not sync latency.
  - Two one-node jobs ran tasks at:
    - 13,486/s unsynced;
    - 1,051–1,064/s synced on the EBS-backed `/tmp`;
    - 13,440/s synced with the raft directory on tmpfs.
  - Synced on EBS, the raft log's commits took p50 7.0 ms and p99 97 ms, at 1.0 operations each.
  - The volume did 1.97 M writes in 653 s, about 3,000 per second: gp3's baseline IOPS cap.
  - Each commit costs about 21 write IOs (12.4 GB in all, about 130 KB per commit), because redb rewrites B-tree and allocator pages per transaction. At 3,000 IOPS that is about 143 commits a second.
  - A per-file count on one local peer (20,000 tasks) split the writes: the raft log did 269 syncs and 1,729 write calls, and the state machine's database did 11 syncs and 6,645 write calls (about 600 per checkpoint). Most of the IO is the state machine's.
  - The fix, both halves (one node, the fsync default):
    - the raft log goes through a synced write-ahead file;
    - the state machine's database is left unsynced, and rebuilt from snapshots and the log after an unclean machine restart.
  - Results: tasks 13,066/s (from 1,044; Ray 6,636), nested 0.12 s (Ray 0.151), actor calls 8,095/s (Ray 9,071). The volume did 1,770 writes/s during tasks, below its cap.
  - The raft log's write-ahead file alone changed nothing on one node (1,044/s).

## Ray comparison (cluster, 1, 4, 16 and 64 nodes, 2026-11-22, evening)

Same machines and method as the morning round below (c7i.4xlarge, one peer
per machine started through Ray, at least 60 s of repetitions per workload, 35 s
at 64 nodes, median shown; node loss runs once). Built from a snapshot of the
working tree at 15:42 MST that compiled, passed a local smoke run of every
workload and 478 tests on Linux. All four jobs (1, 4, 16 and 64 nodes) finished
every workload for both runtimes.

| Workload | 1 node Ray | 1 node Chimaera | 4 Ray | 4 Chimaera | 16 Ray | 16 Chimaera | 64 Ray | 64 Chimaera |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Independent no-op tasks (/s) | 6,887 | 13,588 | 6,993 | 16,987 | 6,084 | 16,105 | 5,123 | 12,096 |
| CPU scaling efficiency | 0.700 | 0.694 | 0.724 | 0.726 | 0.710 | 0.710 | 0.661 | 0.603 |
| Map of tiny items, naive (/s) | 6,558 | 1,738,476 | 6,740 | 885,102 | 5,939 | 759,987 | 5,246 | 749,701 |
| 512 MiB value to 2 x CPUs readers (s) | 0.009 | 0.008 | 0.025 | 0.030 | 0.089 | 0.110 | 0.397 | 0.397 |
| 32 MiB array passed straight to 2 x CPUs calls (s) | 0.054 | 0.008 | 0.211 | 0.043 | 0.892 | 0.194 | 3.539 | 0.737 |
| 10 calls on a 256 MiB value just made (s) | 0.212 | 0.257 | 0.223 | 0.262 | 0.225 | 0.261 | 0.227 | 0.270 |
| Actor calls (/s) | 8,538 | 13,040 | 8,845 | 12,697 | 8,510 | 12,008 | 8,872 | 9,475 |
| Nested tasks, 4 x CPUs parents of 8 (s) | 0.148 | 0.054 | 0.323 | 0.256 | 0.437 | 0.606 | 1.388 | 0.937 |
| Shuffle, 3.2M pairs (s) | 0.206 | 0.297 | 0.143 | 0.400 | 0.135 | 0.458 | 0.132 | 0.675 |
| A node killed under 4 x CPUs one-second tasks (s) | 7.77 | 5.44 | 18.43 | 9.57 | 18.26 | 6.44 | 18.55 | 9.66 |

Changed since the morning round, on the Chimaera side:
- **Shuffle:** shuffle buckets and partitions are dicts rather than lists of pairs. A gather reads only its own bucket of each scatter's file. Inputs held on other peers are fetched concurrently. The eval's clock now stops for Chimaera where it stops for Ray: before, Chimaera's included a Python sum over every pair. It went from 0.441 to 0.297 s on one node and from 0.842 to 0.675 s at 64.
- **Calls on a value just made:** a large `bytes` value is read with one `pread` instead of being copied out of a fresh mapping, which went from 0.310 to 0.257 s on one node and from 0.376 to 0.270 s at 64.
- **Actor calls at 64 nodes:** these now finish ahead of Ray (9,475/s against 8,872); the morning round ran out of time before Ray's pass.

Where Ray is still ahead:
- **Shuffle at every size:** 1.4x on one node, 2.8x at 4, 3.4x at 16 and 5.1x at 64. Timed by phase on three local peers, the scatters are on par with Ray's, and the rest is spent before the gathers are dispatched, in the shard's handling of the 4,096 dependency edges.
- **Calls on a value just made:** 16 to 21% at every size.
- **CPU scaling at 64 nodes:** 0.603 against 0.661; level at 4 and 16, and within 1% on one node.
- **Broadcast at 4 and 16 nodes:** 20 to 24%; level at 64, and Chimaera ahead on one node.
- **Nested tasks at 16 nodes:** 0.606 s against 0.437; at 16 nodes in the morning it was Chimaera 0.395 against Ray 0.486. Its spread is wide (p10 0.35 s, p90 0.72 s), so part of this is variance.

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

All results are **local** (macOS 14, 12 cores,
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
