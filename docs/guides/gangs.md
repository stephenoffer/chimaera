# Distributed training with gangs

Need one function on many GPUs or peers, in lockstep? That's a gang: N ranks
that start together, meet at a rendezvous, and fail and restart as one. Use
it to run PyTorch DDP across peers, pass small values between ranks without
torch, fit its size to your capacity, and find the rank holding up the rest.

## Run a function on N ranks

```python run
import os
import time

import chimaera as ch

@ch.remote(cpus=0.5)
def shard_sum(rows: int) -> int:
    g = ch.context().gang                       # this rank's view of the gang
    mine = sum(range(g.rank, rows, g.size))     # every size-th row, starting at this rank
    return g.all_reduce(mine)                   # the total, on every rank

gang = shard_sum.options(ranks=4).gang(100)
assert gang.result() == [4950] * 4
```

`fn.options(ranks=n).gang(*args)` starts n calls of `fn` with the same
arguments and returns a `Gang`: one future for all their return values, in
rank order.

<details>
<summary>Per-rank resources and retries</summary>

Wait on the `Gang`, `await` it, pass it to another call, or `cancel()` it to
stop them all.

Each rank gets the gang's `cpus`, `memory`, `memory_limit`, `gpus`,
`device`, `placement`, `priority` and `deadline` (see
[resources.md](resources.md)). `max_attempts` (default 3) counts
generations, not retries of one rank. `rendezvous_timeout` (default 300 s)
is how long the ranks of a generation have to meet once it starts, and
`collective_timeout` (default 600 s) how long a rank waits in one
collective (see [Survive a lost rank](#survive-a-lost-rank)). The gang's
supervisor is a small call (0.01 CPU) on your script's own peer.

Each generation is decided once, in the cluster's replicated store: which
peer holds each rank's slot, and each rank's call. If the supervisor itself
is lost (its worker, or its peer while the run goes on), its retry carries on
with the generation it was supervising: ranks already running keep running,
and none starts twice. Ranks of a generation that was given up leave its
rendezvous within a second rather than at `rendezvous_timeout`.

`gang()` without `ranks=` raises `TypeError`, and `.remote()` with `ranks=`
raises `ch.OptionsError`.

</details>

## Know where you are

Written for `torchrun`? Your torch and DeepSpeed code runs as is, since each
rank gets the variables it sets:

```python run
@ch.remote(cpus=0.5)
def rendezvous_env() -> dict:
    keys = ("RANK", "WORLD_SIZE", "LOCAL_RANK", "LOCAL_WORLD_SIZE", "MASTER_ADDR", "MASTER_PORT")
    return {k: os.environ[k] for k in keys}

envs = rendezvous_env.options(ranks=2).gang().result()
assert [e["RANK"] for e in envs] == ["0", "1"]
assert {e["WORLD_SIZE"] for e in envs} == {"2"}
assert envs[0]["MASTER_ADDR"] == envs[1]["MASTER_ADDR"]
```

| Variable | Value |
|---|---|
| `RANK`, `WORLD_SIZE` | This rank, and how many there are |
| `LOCAL_RANK`, `LOCAL_WORLD_SIZE` | The same among ranks on this peer |
| `MASTER_ADDR`, `MASTER_PORT` | Rank 0's host and a port it holds there until your code starts |
| `CHIMAERA_GANG`, `CHIMAERA_GANG_GENERATION` | The gang's id and its current generation |

<details>
<summary>Group fields and reused workers</summary>

In Python, `ch.context().gang` gives you this as a `Group`: `rank`, `size`,
`local_rank`, `local_size`, `generation`, `id` and `master` (an
`(addr, port)` pair). Outside a gang it's `None`. `table()` lists every
rank of the generation: its `rank`, `node`, `local_rank`, `devices` (the
ids of the GPUs it was granted) and `visible` (their indexes as its process
numbers them, so a rank with one GPU sees index 0).

If later calls reuse a worker, it gets its old environment back, so plain
calls never see a stale `RANK`.

</details>

## Exchange values without torch

Four collectives pass Python values between ranks with no torch:

```python run
@ch.remote(cpus=0.5)
def exchange() -> tuple:
    g = ch.context().gang
    names = g.all_gather(f"worker-{g.rank}")             # every rank's value, in rank order
    longest = g.all_reduce(len(names[g.rank]), op=max)   # combined with op (default: +)
    seed = g.broadcast(1234 if g.rank == 0 else None, src=0)
    g.barrier()
    return names, longest, seed

assert exchange.options(ranks=3).gang().result() == [(["worker-0", "worker-1", "worker-2"], 8, 1234)] * 3
```

<details>
<summary>What each collective returns</summary>

A value that pickles to 64 KiB or less goes through the cluster's
replicated store: configs, seeds, metrics, file lists. A bigger one, such as
a NumPy array, goes through the object plane: the sender stores it as
`ch.put` would, the store carries only its handle, and the sender frees it
once every rank has read it. A copy that a rank on another peer fetched
stays on that peer until the run ends, so a long loop of large collectives
across peers fills their object stores. Move that kind of data, and GPU
tensors, with `torch.distributed`.
Every rank has to run the same sequence of collectives.

| Collective | Returns |
|---|---|
| `all_gather(obj)` | Every rank's `obj`, in rank order |
| `all_reduce(obj, op=operator.add)` | Every rank's `obj` folded with `op`, on every rank |
| `broadcast(obj, src=0)` | Rank `src`'s `obj`, on every rank |
| `barrier()` | Nothing, once every rank has arrived |

Each takes `timeout=` in seconds. If ranks reach one point through different
collectives, they wait for each other instead of mixing data, and
`explain()` reports the mismatch (see the last section).

</details>

## Use torch.distributed

`g.torch()` sets up the default `torch.distributed` process group for this
gang and hands back the module. Here's DDP with checkpoints; it needs torch,
eight GPUs (on any number of peers) and a checkpoint path every peer can
read:

```python
import os

import chimaera as ch

@ch.remote(gpus=1, cpus=4)
def train(epochs: int, ckpt: str) -> float:
    import torch
    from torch.nn.parallel import DistributedDataParallel as DDP
    from torch.utils.data import DataLoader, TensorDataset
    from torch.utils.data.distributed import DistributedSampler

    g = ch.context().gang
    dist = g.torch()                                  # NCCL group for this generation
    device = torch.device(ch.dist.device())           # "cuda:0": the one GPU this rank sees
    torch.manual_seed(0)
    data = TensorDataset(torch.randn(8192, 512), torch.randn(8192, 1))
    sampler = DistributedSampler(data, num_replicas=g.size, rank=g.rank)
    loader = DataLoader(data, batch_size=64, sampler=sampler)

    model = torch.nn.Sequential(torch.nn.Linear(512, 256), torch.nn.ReLU(), torch.nn.Linear(256, 1)).to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3)
    start = 0
    if os.path.exists(ckpt):                          # a later generation resumes here
        state = torch.load(ckpt, map_location=device)
        model.load_state_dict(state["model"])
        opt.load_state_dict(state["opt"])
        start = state["epoch"] + 1
    ddp = DDP(model, device_ids=[device.index])

    loss = torch.zeros(())
    for epoch in range(start, epochs):
        sampler.set_epoch(epoch)
        for x, y in loader:
            loss = torch.nn.functional.mse_loss(ddp(x.to(device)), y.to(device))
            opt.zero_grad()
            loss.backward()
            opt.step()
        if g.rank == 0:
            torch.save({"model": model.state_dict(), "opt": opt.state_dict(), "epoch": epoch}, ckpt + ".tmp")
            os.replace(ckpt + ".tmp", ckpt)           # never leave a half-written checkpoint
        dist.barrier()
    return loss.item()

losses = train.options(ranks=8, max_attempts=5).gang(20, "/mnt/shared/ckpt.pt").result()
```

<details>
<summary>Backends and group lifetime</summary>

The backend follows the rank's devices: `nccl` for CUDA and ROCm, `xla` for
TPU and Neuron, `hccl` for Gaudi, `xccl` for Intel, and `gloo` on CPU or
Apple's GPU. Want another one? Pass `backend=`. A function gang's group is
torn down when the rank's call returns; a class gang's lasts as long as its
rank.

</details>

## Keep a gang alive as actors

Want ranks that stay up between steps? `Class.options(ranks=n).gang(*args)`
builds n instances together, each with the rendezvous environment set for
good in its own process, so `__init__` can call `ch.context().gang.torch()`
and load a model once:

```python run
@ch.remote(cpus=0.5)
class Worker:
    def __init__(self, base: int):
        g = ch.context().gang                 # known while the object is built
        self.rank, self.base = g.rank, base

    def step(self) -> int:
        return self.base + self.rank

    def where(self) -> tuple:
        g = ch.context().gang
        return g.rank, g.size, g.generation

with Worker.options(ranks=3).gang(100) as workers:
    assert len(workers) == 3
    assert workers.step.remote().result() == [100, 101, 102]   # every rank
    assert workers[2].step.remote().result() == 102            # one rank
    workers.resize(2)                                          # rebuilt with 2 ranks
    assert workers.where.remote().result() == [(0, 2, 2), (1, 2, 2)]
```

<details>
<summary>Lost ranks and gang methods</summary>

Lose any rank and the class gang breaks: its calls raise `ch.ActorDied`, and
nothing restarts until you call `workers.restart()`. Restore your checkpoint
in `__init__`. A collective that outlasts `collective_timeout` breaks it
too: every rank's collectives raise the same `ch.errors.GangFailed` until
you restart it.

- `workers.method.remote(...)` runs on every rank and gives one future of
  the results in rank order; `workers[i].method.remote(...)` runs on rank
  `i`.
- `workers.restart()` rebuilds the whole gang as the next generation, and
  `workers.resize(n)` does so with n ranks.
- `workers.close()`, or leaving the `with` block, ends all ranks.

</details>

[actors.md](actors.md) covers actors in general.

## Let the gang size itself

Not sure how many ranks you'll get? `ranks=ch.auto(initial, min=, max=)`
sizes each generation to how many ranks of this shape the live peers can
hold:

```python run
@ch.remote(cpus=0.5)
def world() -> int:
    return ch.context().gang.size

elastic = world.options(ranks=ch.auto(2, min=1, max=4)).gang()
assert elastic.result() == [2, 2]                    # the first generation starts at 2
print(elastic.decisions)  # ['generation 1: 2 ranks: the live peers hold 24 of this shape (min 1, max 4)']
assert elastic.decisions[0].startswith("generation 1: 2 ranks")
```

<details>
<summary>How elastic sizing works</summary>

Lose a peer and an elastic gang comes back smaller instead of waiting for a
replacement. `gang.decisions` tells you why each generation got its size;
`gang.size` and `gang.generation` tell you how it ran.

The count goes by the peers' CPUs, plus GPUs when ranks ask for them. A
fractional `gpus=0.5` counts as the whole device each rank gets, so eight
GPUs hold eight such ranks. The count doesn't look at `device=` yet: on a
cluster of mixed GPUs, give `max` no larger than the matching devices. It
counts capacity, not what's free right now. If you give `initial`, the first
generation starts there. `min` defaults to 1. `max` is required unless you
give `initial`, which then doubles as the max.

A running generation never grows, so new peers join in from the next one. An
elastic class gang resizes on `restart()`.

</details>

## Survive a lost rank

If a rank's worker or peer dies, the gang doesn't limp on: the other ranks
of that generation are canceled, and the whole gang starts over as a new
generation:

```python run
@ch.remote(cpus=0.5)
def lose_rank_once() -> tuple:
    g = ch.context().gang
    if g.generation == 1 and g.rank == 1:
        os._exit(1)                      # this rank's worker dies
    g.barrier()                          # rank 0 would wait here forever without the restart
    return g.rank, g.generation

retried = lose_rank_once.options(ranks=2).gang()
assert retried.result() == [(0, 2), (1, 2)]
assert retried.generation == 2
```

<details>
<summary>Why the restart takes 15 s</summary>

Resume from your own checkpoint; `g.generation` tells you which try this is.
After `max_attempts` generations have each lost a rank, the gang fails with
`ch.errors.GangFailed`.

Before restarting, the supervisor waits up to 15 s to learn whether the dead
rank's peer is gone, so the new generation isn't sized to fit on it. That's
why the example takes about 15 s.

`ch.errors.GangFailed` is an `InfrastructureFailure`:

```python
@ch.remote(cpus=0.5, max_attempts=2)
def always_lost():
    os._exit(1)

try:
    always_lost.options(ranks=2).gang().result()
except ch.errors.GangFailed as e:
    print(e)  # gang 4d3ac3a3f2ced558: generation 2 lost a rank: 1 attempts lost to infrastructure failures; last: executor process exited (exit status: 1)
```

</details>

A rank that stops making progress counts as lost too. A rank waits in one
collective, or in `g.torch()` for the other ranks, at most
`collective_timeout` seconds (default 600, torch's own default for NCCL), and
`g.torch()` gives torch's process group the same timeout. When a wait runs
out, the rank gives the generation up, every other rank raises the same
`ch.errors.GangFailed` (within a second if it is waiting too, else at its
next collective), and the gang starts its next generation:

```python run
@ch.remote(cpus=0.5)
def stuck_once() -> tuple:
    g = ch.context().gang
    if g.generation == 1 and g.rank == 1:
        time.sleep(60)                   # stuck: never reaches the barrier
    g.barrier()                          # rank 0 gives generation 1 up after 2 s
    return g.rank, g.generation

assert stuck_once.options(ranks=2, collective_timeout=2).gang().result() == [(0, 2), (1, 2)]
```

The deadline counts from when a rank enters the collective, so one rank's
long work before it counts against the others. If rank 0 downloads the data
for 20 minutes while the others wait in `g.barrier()`, give the gang a
`collective_timeout` longer than that, or every generation is given up at
10 minutes until `max_attempts` runs out.

A `torch.distributed` `DistError` out of a rank or out of `g.torch()`
(torch's watchdog timed a collective out, or torch lost a peer) loses the
generation the same way. A collective's own `timeout=` is different: it
raises `TimeoutError` in every rank that waits in that collective or enters
it later, which fails the gang as your error.

A rank that raises is different. That's your bug, not a lost machine, so the
gang fails at once with that rank's own exception and cancels the others:

```python run
@ch.remote(cpus=0.5)
def bad_shard() -> None:
    g = ch.context().gang
    if g.rank == 1:
        raise ValueError("bad shard")
    g.barrier()

try:
    bad_shard.options(ranks=2).gang().result()
except ValueError as e:
    assert "bad shard" in str(e)
```

<details>
<summary>When not all ranks fit</summary>

A generation starts all or none. The peers first hold every rank's CPUs and
GPUs, and only then does any rank start, each on what was held for it. A
gang that doesn't fit yet waits holding nothing, and `explain()` says why
(`waiting for every rank's slot to be free: ...`). So two gangs that each
need all eight GPUs run one after the other, never four and four. Actors
that asked for `cpus=` or `gpus=` keep them while they live, so a gang
waits for those too.

A gang that couldn't fit even on idle peers fails at once with
`ch.Infeasible`, naming the first rank that doesn't fit. Once a generation
starts, every rank has `rendezvous_timeout` seconds to reach the
rendezvous. If one doesn't, the generation counts as lost, and the error
says where each missing rank is: queued on its peer and why, or started
but not there yet.

A rank of a generation that was replaced can't join the next one: its
collectives and `g.torch()` raise `ch.errors.GangFailed`. If something
takes `MASTER_PORT` before `g.torch()` binds it, rank 0 serves torch's
store on another port, and the other ranks connect to that one.

</details>

[failures.md](failures.md) covers retries and errors for ordinary calls.

## Group actors you already have

Made your actors one by one? `ch.dist.group(*actors)` turns them into one
group, ranked in the order you pass them:

```python run
@ch.remote(cpus=0.5)
class Learner:
    def __init__(self, weight: int):
        self.weight = weight

    def average(self, handle: ch.dist.GroupHandle) -> float:
        group = handle.join()                         # this member's Group
        return group.all_reduce(self.weight) / group.size

learners = [Learner.remote(w) for w in (1, 2, 6)]
handle = ch.dist.group(*learners)
assert [f.result() for f in [l.average.remote(handle) for l in learners]] == [3.0] * 3
for l in learners:
    l.close()
```

<details>
<summary>Joining and new generations</summary>

Pass the handle to the members. Inside a member's method, `handle.join()`
returns its `Group`, with the collectives, `torch()` and `mesh()` that gang
ranks get.

Each member's first `join()` waits for all of them; later ones return that
`Group`. Changed the set of actors? Call `ch.dist.group(..., name="k")`
again with the same name to start a new generation. `join()` anywhere but
inside a member's method raises `ValueError`.

A member waits in one collective at most 600 seconds, or what you pass as
`ch.dist.group(..., collective_timeout=s)`. Past it the generation is given
up, and every member's collectives raise `ch.errors.GangFailed` until you
start a new one.

</details>

## Split ranks into a device mesh

Mixing data and tensor parallelism? `g.mesh(dp=..., tp=...)` lays the ranks
out in a grid and returns this rank's `Group` along each axis:

```python run
@ch.remote(cpus=0.25)
def grid() -> tuple:
    g = ch.context().gang
    axes = g.mesh(dp=2, tp=2)            # tp: ranks 0,1 and 2,3; dp: ranks 0,2 and 1,3
    return axes["tp"].ranks, axes["dp"].ranks, axes["tp"].all_reduce(g.rank)

out = grid.options(ranks=4).gang().result()
assert out[0] == ([0, 1], [0, 2], 1)
assert out[3] == ([2, 3], [1, 3], 5)
```

For tensors, build torch's own mesh after `g.torch()`. This one needs torch
and eight GPUs:

```python
@ch.remote(gpus=1)
def train_2d(steps: int) -> None:
    from torch.distributed.device_mesh import init_device_mesh

    g = ch.context().gang
    g.torch()
    mesh = init_device_mesh("cuda", (2, 4), mesh_dim_names=("dp", "tp"))
    tp_group = mesh["tp"].get_group()            # the ranks of g.mesh(dp=2, tp=4)["tp"].ranks
    ...

train_2d.options(ranks=8).gang(1000).result()
```

<details>
<summary>Axis collectives and sizes</summary>

An axis has `ranks` (their ranks in the whole gang) and the store
collectives, but no torch process group of its own, so its `torch()` raises
`RuntimeError`. The last axis varies fastest, as in torch's `DeviceMesh`.
The sizes must multiply to the group's size, or `mesh()` raises
`ValueError`.

</details>

## Find what a hung gang waits for

Gang stuck? `explain()` tells you which ranks it's waiting on:

```python run
@ch.remote(cpus=0.5)
def slow_rank() -> int:
    g = ch.context().gang
    if g.rank == 1:
        time.sleep(30)                   # never reaches the barrier in time
    g.barrier()
    return g.rank

hung = slow_rank.options(ranks=3).gang()
try:
    hung.result(timeout=5)
except TimeoutError as e:
    print(e)  # the gang did not finish within 5 s: rank 1 last entered #0 (rendezvous, 5 s ago); ranks 0, 2 wait in #1 (barrier)
    assert "rank 1 last entered #0 (rendezvous" in str(e)
assert hung.explain()["ranks"][1]["op"] == "rendezvous"
hung.cancel()
```

A wait timeout on `gang.result(timeout=)` and a collective's own `timeout=`
both put that one-line summary in their message:

```python run
@ch.remote(cpus=0.5)
def impatient() -> None:
    g = ch.context().gang
    if g.rank == 1:
        time.sleep(30)
    g.barrier(timeout=2)

try:
    impatient.options(ranks=3).gang().result()
except TimeoutError as e:
    assert "timed out: rank 1 last entered #0 (rendezvous" in str(e)
```

<details>
<summary>Reading explain, and NCCL hangs</summary>

`explain()` sees the rendezvous and the four store collectives. It can't see
inside `torch.distributed`.

On a gang, a class gang or a group handle, `explain()` returns the current
`generation`, a `ranks` list with each rank's last collective (its number,
`op` and when it was `entered`), and a one-line `summary` naming the ranks
that hold the others up. On a function gang, a rank that hasn't started
shows why it's waiting instead.

If ranks call different collectives at one point, the summary reads
`the ranks call different collectives as #1: rank 0 all_gather, rank 1 broadcast`.

A hang inside NCCL looks like all ranks sitting in one store collective,
with none behind. To narrow it down, drop a `g.barrier()` before a suspect
step.

</details>

[observability.md](observability.md) covers `explain()` for every kind of call.

Next: pick CPUs, memory and GPUs for each rank in
[resources.md](resources.md).
