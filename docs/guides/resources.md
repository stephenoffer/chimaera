# Resources, GPUs and automatic tuning

Tell Chimaera what a call needs, or let the peer measure it. Then see what
was reserved and why, hold capacity ahead of time, and cap what a tree of
calls may spend.

## Ask for CPUs and memory

```python run
import os
import time

import chimaera as ch

@ch.remote(cpus=2, memory="512 MiB")
def threads() -> str:
    return os.environ["OMP_NUM_THREADS"]

assert threads.remote().result() == "2"
```

The call starts only where both are free and keeps them while it runs.
That's admission, not a fence: a call that reserves 512 MiB and touches
2 GiB isn't stopped for it (`memory_limit=` below is the fence).

Fractions work: `cpus=0.25` lets four calls that mostly wait on the network
share one core. Thread pools follow the grant: `OMP_NUM_THREADS`,
`OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `VECLIB_MAXIMUM_THREADS`,
`NUMEXPR_NUM_THREADS`, `RAYON_NUM_THREADS`, `POLARS_MAX_THREADS` and
`NUMBA_NUM_THREADS` are set to the CPUs rounded up (1 for `cpus=0.25`)
unless your run's environment sets them.

Set options on the decorator, per call with `.options(...)`, or for a block
of calls with `with ch.defaults(...)`. Values are checked where you write
them, so a typo raises `ch.OptionsError` on that line.

| Option | What it does |
|---|---|
| `cpus` | CPUs to reserve; a positive number, fractions allowed. Default: 1, adjusted by measurement |
| `memory` | Bytes to reserve. Default: none until measured |
| `memory_limit` | Bytes a call may use; over it, the worker is stopped |
| `gpus` | Accelerator units of any vendor |
| `device` | `"KIND[:MODEL]"`, such as `"cuda:H100"` |
| `placement` | `ch.dist.Placement(require=..., prefer=...)` on peer labels |
| `within` | A bundle of a reservation, `res[i]` |
| `budget` | `{"cpu_seconds": ..., "gpu_seconds": ...}` for this call and every call it makes |
| `adaptive` | `"on"` (default), `"observe"` or `"frozen"` |
| `accounting` | `"auto"` (default), `"process"`, or `"isolated"` for a cgroup per attempt (Linux) |

A request no peer could ever meet raises `ch.Infeasible`, naming the
constraint, when you wait on it:

```python run
try:
    threads.options(cpus=10_000).remote().result()
except ch.Infeasible as e:
    assert "largest peer offers" in str(e)
```

## Write sizes with a unit

A size is an int of bytes or a string with a byte unit. Decimal and binary
units each mean what they say:

| Unit | Bytes |
|---|---|
| `B` | 1 |
| `KB`, `MB`, `GB`, `TB` | 10³, 10⁶, 10⁹, 10¹² |
| `KiB`, `MiB`, `GiB`, `TiB` | 2¹⁰, 2²⁰, 2³⁰, 2⁴⁰ |

`"2 GB"` is 2,000,000,000 bytes and `"2 GiB"` is 2,147,483,648, about 7%
more. Case and the space don't matter, and `"1.5 GiB"` works if it comes to
whole bytes. A bare `"2g"` or `"512m"` is refused, because Docker reads `m`
as MiB and Kubernetes as millibytes:

```python run
try:
    threads.options(memory="2g")
except ch.OptionsError as e:
    assert "not a byte size" in str(e)
```

## Cap a call's memory

`memory_limit=` is enforced:

```python run
@ch.remote(memory_limit="100 MiB")
def too_big() -> int:
    data = b"x" * (400 * 2**20)
    time.sleep(1)
    return len(data)

try:
    too_big.remote().result()
except ch.OutOfMemory as e:
    assert "exceeding its memory limit" in str(e)
```

A call over the limit is stopped and retried twice more under it, then fails
with `ch.OutOfMemory` giving the peak it reached and the limit. These
retries are counted apart from `max_attempts`.

With cgroup v2 delegation on Linux, `accounting="isolated"` runs each
attempt in a fresh worker in its own cgroup, so the limit is a kernel limit
and the CPU and memory readings are the attempt's own. Without that
delegation the call is `ch.Infeasible`.

## Let the peer measure

You usually don't need `cpus=` or `memory=`. The peer measures each
function, keyed by its code, and reserves from what it saw. An edited
function is a new one that starts from the old one's numbers:

```python run
@ch.remote
def load(mib: int) -> int:
    data = b"x" * (mib * 2**20)
    time.sleep(0.05)
    return len(data)

for _ in range(4):
    f = load.remote(64)
    f.result()

reserved = f.explain()["attempts"][-1]["reservation"]
print(reserved)  # cpus 1 (default); memory 175 MiB (learned: p95 peak 87 MiB over 3 calls of this input size × 2)
assert "learned" in reserved
```

Values you set always win over learned ones. The peer's rules:

- A function never measured runs one call per four of the peer's CPUs at a
  time (at least one, so 3 on a 12-CPU peer) until one finishes or has run
  2 s. More start only while the memory pool has 4 GiB free per call.
- After three successful calls, a call reserves the 95th-percentile private
  memory peak times a margin: 1.2 for steady peaks, 1.5 when they vary, 2
  when they vary a lot or a call was killed for memory. Calls are grouped by
  input size. Once the peak grows in a straight line with input size over
  at least 8 calls, each call's reservation is predicted from its own input.
- Calls wait for memory instead of overcommitting it. A wait over a minute
  prints why on your terminal (`chimaera: waited 60 s on call ...: needs ...`).
- A call killed for memory is retried with 1.5 times its peak once that
  fits, or fails at once with `ch.OutOfMemory` if that's more than the
  peer's whole memory pool.
- CPU grows at once when three or more calls, with a median of at least
  50 ms, use over 1.3 times their grant. It steps to the next of 0.25, 0.5,
  1, 2, 4 or 8 cores (whole cores above 8).
- CPU shrinks only after a trial. Once ten calls steadily use well under
  their grant (median at least 50 ms), every other call gets 1.5 times the
  measured use, rounded up to that ladder. The smaller grant stays only if
  it finishes more calls per reserved core without hurting tail latency. A
  function that mostly waits on I/O ends up at a quarter core.
- A plain-call worker (not an actor's) whose memory between calls keeps
  rising (more than double plus 512 MiB, or a steady climb over 20 calls) is
  replaced after its call, with the decision recorded. On Linux, freed
  allocator memory is handed back first so it isn't taken for a leak.
- Profiles are saved in the peer's home directory every 30 s, reloaded on
  restart and shared with other peers. Memory carries across hardware; CPU
  use only between peers of the same hardware class.

## Bound or turn off learning

`ch.auto(min=..., max=...)` keeps the learning but clips it. `adaptive=`
chooses whether it applies at all:

```python run
capped = load.options(memory=ch.auto(max="80 MiB")).remote(64)
capped.result()
assert "memory 80 MiB" in capped.explain()["attempts"][-1]["reservation"]

frozen = load.options(adaptive="frozen").remote(64)
frozen.result()
assert "memory not known yet" in frozen.explain()["attempts"][-1]["reservation"]
```

| `adaptive=` | Effect |
|---|---|
| `"on"` | Default. Learned CPU and memory apply |
| `"observe"` | Measure and say what would have been reserved, but reserve the defaults |
| `"frozen"` | No learning: 1 CPU and no memory reservation unless you set them |

Pass only `min=` and `max=` to `ch.auto` for `cpus` and `memory`. Today an
initial value (`cpus=ch.auto(2, max=4)`) is sent as if you had set `cpus=2`,
so the peer never adjusts it.

## See what was reserved and why

Each attempt in `future.explain()["attempts"]` has a `reservation` string,
such as `cpus 1 (default); memory 175 MiB (learned: ...)` or
`cpus 0.5 (set by the caller)`, and a `placement` string saying why it ran
where it did. For a call that hasn't started, `pending_reason` says why.

`chimaera profiles` shows what the peer has learned and what the next call
gets: the profiles file, then a line per function (`--fn` filters by name,
`--json` for scripts):

```bash
$ chimaera profiles --fn load
  __main__.load: 6 calls, 0.060 s p50, 0.09 cores on average; next call gets cpus 1 (default), memory 175 MiB (p95 peak 87 MiB over 6 calls of this input size × 2)
```

`chimaera explain RUN --decisions` lists the peer's changes with evidence
and reasons. `RUN` is a run id from `chimaera runs` or
`ch.runs.current_run()`. The list holds the peer's recent decisions about
every run's functions, not only RUN's, so on a secured cluster it needs the
cluster token. Only `--json` names the function, in `scope`.

```bash
$ chimaera explain 000003a491eb48727b1548db9633e829 --decisions
  granularity range_size 1→2 adopted · 3 calls, median 7.028 ms · calls under 50 ms are batched toward 200 ms per range
  cpu-sizing cpus 1→0.25 pending · 10 calls used 0.04 cores on average (CV 0.75) · calls mostly wait on I/O; canarying a smaller grant
```

[observability.md](observability.md) covers `explain()` and the CLI in full.

## Use GPUs and other accelerators

A call sees only the accelerators it was granted:

```python run
@ch.remote
def where() -> str:
    return ch.dist.device()

assert where.remote().result() == "cpu" and ch.dist.devices() == []
try:
    where.options(gpus=64).remote().result()
except ch.Infeasible as e:
    assert "accelerator" in str(e)
```

`gpus=N` asks for N units of any vendor; `device="KIND[:MODEL]"` narrows
it. `KIND` is `cuda`, `rocm`, `tpu`, `neuron`, `hpu`, `xpu` or `mps`, and
`MODEL` is a case-insensitive prefix of the model name, such as `H100`,
`MI300X` or `v5e`. `device=` alone means one unit. An unknown kind or a
model no peer has is `ch.Infeasible`.

The peer sets the vendor's visibility variable for the worker
(`CUDA_VISIBLE_DEVICES` with GPU UUIDs, `HIP_VISIBLE_DEVICES`,
`TPU_VISIBLE_CHIPS`, `NEURON_RT_VISIBLE_CORES`, `HABANA_VISIBLE_MODULES`,
`ZE_AFFINITY_MASK`), and a call without `gpus=` or `device=` gets an empty
list, so it sees none. Apple's GPU is the exception: `mps` can't be hidden, so there the
grant is bookkeeping only.

Inside the call, `ch.dist.device()` is the torch device string of the first
grant (`"cuda:0"`, `"xla:0"`, `"hpu"`, `"xpu:0"`, `"mps"`, or `"cpu"` with
none), with no torch import. `ch.dist.devices()` lists every grant with
`kind`, `id`, `index`, `torch`, `backend` (the `torch.distributed` backend),
`model` and `memory_bytes`. This needs a GPU host:

```python
@ch.remote(gpus=1, device="cuda:A10G")
def matmul(n: int) -> float:
    import torch
    x = torch.randn(n, n, device=ch.dist.device())   # "cuda:0": the only GPU this call sees
    return (x @ x).sum().item()

print(matmul.remote(4096).result())
```

A fractional `gpus=0.5` takes a whole device today; sharing a device between
calls is not built yet. NVIDIA discovery, grants and NCCL gangs have run on
A10G GPUs, and the test suite covers Apple's GPU. AMD, TPU, Neuron, Gaudi
and Intel follow their vendors' documented files but haven't been run on
that hardware. [../deploy.md](../deploy.md) has the per-vendor table and how
peers find devices.

## Hold capacity before work runs

Reserve capacity up front so nothing else can take it, then place work
inside it:

```python run
@ch.remote
def node_of() -> str:
    return ch.context().node

with ch.dist.reserve([{"cpus": 1}, {"cpus": 1}], strategy="pack") as res:
    assert len(res) == 2 and res[0].node == res[1].node
    assert node_of.options(within=res[0]).remote().result() == res[0].node
```

`ch.dist.reserve(bundles, strategy=)` holds one bundle per dict (`cpus`,
which defaults to 1, `gpus`, `memory`, `device`) from `ready()` until
`release()`, used or not. The `with` block calls both. A call or actor made
with `.options(within=res[i])` runs inside bundle `i`, on its peer and with
its resources.

| `strategy=` | Placement |
|---|---|
| `"pack"` (default) | One peer when it fits, else wherever |
| `"spread"` | Different peers when there are enough |
| `"strict_pack"` | One peer, or `ch.Infeasible` |
| `"strict_spread"` | One peer per bundle, or `ch.Infeasible` |

Good to know:

- `reserve()` returns at once. `ready(timeout)` waits, and on a timeout
  raises `TimeoutError` but keeps trying until you `release()`.
- A reservation holds every bundle or none, so two that want the same CPUs
  take turns instead of each holding half.
- Calls in one bundle share one process (as threads, up to the bundle's
  CPUs at once). Give each parallel worker its own bundle.
- Holds come from the peers' load reports, not one atomic step. A burst of
  reservations against stale reports can over-commit for a moment before
  one backs off.
- `strict_pack` checks free CPUs when you call `reserve()`. If no peer has
  them free right then, it raises `ch.Infeasible` even when one could later.

## Choose peers by label

```python run
peer = ch.runs.cluster().nodes()[0].peer
pinned = node_of.options(placement=ch.dist.Placement(require={"node": peer}))
assert pinned.remote().result() == peer

try:
    node_of.options(placement=ch.dist.Placement(require={"zone": "us-east1-b"})).remote().result()
except ch.Infeasible as e:
    assert "no live peer has labels zone=us-east1-b" in str(e)
```

Every peer has the labels `node` (its id), `host`, `os` and `arch`, plus any
set in its environment as `CHIMAERA_LABELS="zone=us-east1-b,rack=r7"`.
`ch.runs.cluster().nodes()` shows each peer's `labels`.

`require={...}` keeps only peers with every pair, and none left is
`ch.Infeasible`. `prefer=(...)` is a tuple of `"key=value"` strings. Each
one a peer matches makes it cheaper to choose, without ruling others out.
Keep the trailing comma in `prefer=("zone=us-east1-b",)`: a bare string is
split into characters. The `resources=` option is accepted but not used for
placement yet, so put custom capabilities in labels.

## Cap what a call tree may spend

A budget caps the CPU and GPU time of a call and every call made inside it:

```python run
@ch.remote
def spin(seconds: float) -> float:
    end = time.process_time() + seconds
    while time.process_time() < end:
        pass
    return seconds

try:
    spin.options(budget={"cpu_seconds": 1}).remote(30).result()
except ch.errors.BudgetExceeded as e:
    assert "cpu_seconds=1 allowed" in str(e)
```

`budget={"cpu_seconds": ..., "gpu_seconds": ...}` takes seconds or duration
strings (`"250ms"`, `"30s"`, `"5m"`, `"8h"`). Usage is charged about once a second while calls run, so even a
single long call is stopped. Past either limit, the call and everything it
submitted are canceled with `ch.errors.BudgetExceeded` (a `ch.Cancelled`).
The message names the budget and what was used.

CPU seconds are a call's measured CPU when it has its worker to itself, else
its CPU grant times how long it ran. GPU seconds are its GPUs times how long
it held them. Budgets don't nest: a call inside a budgeted call that sets its
own raises `ch.OptionsError`.

## Share a cluster between teams

A run belongs to a tenant (`ch.init(tenant="team-a")`, default `"default"`).
A peer started with `CHIMAERA_TENANT_QUOTAS="team-a=8,team-b=2.5"` holds
back a tenant's new calls while its running calls there already use its CPU
quota. `CHIMAERA_TENANT_SHARES` does the same across the cluster. Other
tenants' work goes ahead meanwhile, and a call larger than a whole quota is
`ch.Infeasible`. [clusters.md](clusters.md) covers setting them up.

Next: run many ranks together for distributed training in [gangs.md](gangs.md).
