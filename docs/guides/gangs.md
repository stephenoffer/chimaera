# Distributed training with gangs

A gang runs the same function on N ranks that start together, meet at a
rendezvous, and fail and restart as one. With one you can run PyTorch DDP
across peers, pass small values between ranks with no torch at all, size the
job to the capacity you have, and find the rank that holds the others up.

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
arguments and returns a `Gang`: a future of every rank's return value, in rank
order. It is a future like any other, so you can wait on it, `await` it, pass
it to another call, or `cancel()` it, which stops every rank. `gang()` without
`ranks=` raises `TypeError`, and `.remote()` with `ranks=` raises
`ch.OptionsError`.

Each rank gets the gang's `cpus`, `memory`, `memory_limit`, `gpus`, `device`,
`placement`, `priority` and `deadline` (see [resources.md](resources.md)).
`max_attempts` (default 3) counts generations of the whole gang, not retries
of one rank. A small supervisor call (0.01 CPU) runs the gang from your
script's own peer.

## Know where you are

Every rank runs with the variables `torchrun` sets, so torch and DeepSpeed
code written for it runs as is:

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
| `MASTER_ADDR`, `MASTER_PORT` | Rank 0's host and a free port there |
| `CHIMAERA_GANG`, `CHIMAERA_GANG_GENERATION` | The gang's id and its current generation |

`ch.context().gang` is the same information as a `Group`: `rank`, `size`,
`local_rank`, `local_size`, `generation`, `id` and `master` (an
`(addr, port)` pair). Outside a gang it is `None`. A worker reused by later
calls gets its old environment back, so plain calls never see a stale `RANK`.

## Exchange small values

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

These four collectives need no torch. They go through the cluster's
replicated store, so they suit configs, seeds, metrics and file lists:
values that pickle to at most 1 MiB each. Move tensors with
`torch.distributed`.

| Collective | Returns |
|---|---|
| `all_gather(obj)` | Every rank's `obj`, in rank order |
| `all_reduce(obj, op=operator.add)` | Every rank's `obj` folded with `op`, on every rank |
| `broadcast(obj, src=0)` | Rank `src`'s `obj`, on every rank |
| `barrier()` | Nothing, once every rank has arrived |

Each takes `timeout=` in seconds. Every rank must call the same collectives in
the same order. If two ranks reach the same point through different ones,
they wait for each other instead of mixing their data, and `explain()` says
so (see the last section).

## Use torch.distributed

`g.torch()` initializes the default `torch.distributed` process group for
this gang and returns the `torch.distributed` module. The backend comes from
the rank's devices: `nccl` for CUDA and ROCm, `xla` for TPU and Neuron, `hccl`
for Gaudi, `xccl` for Intel, and `gloo` on CPU or Apple's GPU. Pass
`backend=` to choose another. In a function gang the group is torn down
when the rank's call returns; in a class gang it lasts as long as the rank.

The sketch below is DDP with checkpoints. It needs torch, eight GPUs (on any
number of peers) and a checkpoint path every peer can read:

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

## Keep a gang alive as actors

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

`Class.options(ranks=n).gang(*args)` builds n instances together, each with
the rendezvous environment set for good in its own process, so `__init__` can
call `ch.context().gang.torch()` and load a model once. Then:

- `workers.method.remote(...)` runs on every rank and gives one future of the
  results in rank order; `workers[i].method.remote(...)` runs on rank `i`.
- A lost rank breaks the gang: its calls raise `ch.ActorDied`. Nothing
  restarts on its own.
- `workers.restart()` builds every rank again as the next generation, and
  `workers.resize(n)` does so with n ranks. Restore your checkpoint in
  `__init__`.
- `workers.close()`, or leaving the `with` block, ends every rank.

[actors.md](actors.md) covers actors in general.

## Let the gang size itself

```python run
@ch.remote(cpus=0.5)
def world() -> int:
    return ch.context().gang.size

elastic = world.options(ranks=ch.auto(2, min=1, max=4)).gang()
assert elastic.result() == [2, 2]                    # the first generation starts at 2
print(elastic.decisions)  # ['generation 1: 2 ranks: the live peers hold 24 of this shape (min 1, max 4)']
assert elastic.decisions[0].startswith("generation 1: 2 ranks")
```

`ranks=ch.auto(initial, min=, max=)` sizes each generation to how many ranks
of this shape the live peers can hold, by their CPUs, and GPUs when the ranks
ask for them. It counts capacity, not what is free right now. The first
generation starts at `initial` when you give one. `min` defaults to 1, and
`max` is required unless `initial` is given (then it is the max). A gang that
loses a peer comes back smaller instead of waiting for a replacement.

`gang.size`, `gang.generation` and `gang.decisions` say how it ran. A running
generation never grows, so new peers are used from the next generation on. An
elastic class gang re-sizes on `restart()`.

## Survive a lost rank

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

When a rank's worker or peer dies, every other rank of that generation is
cancelled and the whole gang starts again as the next generation. Resume from
your own checkpoint; `g.generation` tells you which try this is. Before the
next generation starts, the supervisor waits up to 15 s to see whether the
dead rank's peer is gone, so the next one isn't sized to fit on it. That is
why the example above takes about 15 s.

After `max_attempts` generations have each lost a rank, the gang fails with
`ch.errors.GangFailed` (an `InfrastructureFailure`):

```python
@ch.remote(cpus=0.5, max_attempts=2)
def always_lost():
    os._exit(1)

try:
    always_lost.options(ranks=2).gang().result()
except ch.errors.GangFailed as e:
    print(e)  # gang 4d3ac3a3f2ced558: generation 2 lost a rank: 1 attempts lost to infrastructure failures; last: executor process exited (exit status: 1)
```

A rank that raises is different: that is your bug, not a lost machine. The
gang fails at once with the rank's own exception and the other ranks are
cancelled:

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

Ranks are placed one at a time today, not all or nothing. A gang larger than
the free capacity starts the ranks that fit, and they wait at the rendezvous.
If every rank hasn't arrived within 300 s, that generation counts as lost and
the next one tries again. Until placement is all or nothing, size a gang to
what is free when it starts. [failures.md](failures.md) covers retries and
errors for ordinary calls.

## Group actors you already have

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

`ch.dist.group(*actors)` makes actors you created separately one group,
ranked in the order you pass them. Pass the handle to them; inside a member's
method, `handle.join()` returns its `Group`, with the same collectives,
`torch()` and `mesh()` as a gang's rank. The first `join()` in each member
waits for all of them, and later ones return the same `Group`.

Call `ch.dist.group(..., name="k")` again with the same name to start a new
generation for a changed set of actors. `join()` anywhere but inside a
member's method raises `ValueError`.

## Split ranks into a device mesh

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

`g.mesh(dp=..., tp=...)` lays the ranks out in a grid and returns this rank's
`Group` along each axis. The last axis varies fastest, as in torch's
`DeviceMesh`, and the sizes must multiply to the group's size or it raises
`ValueError`. An axis has the store collectives and `ranks` (their ranks in
the whole gang), but no torch process group of its own: its `torch()` raises
`RuntimeError`. For tensors, build torch's own mesh after `g.torch()`. This
needs torch and eight GPUs:

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

## Find what a hung gang waits for

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

`explain()` on a gang, a class gang or a group handle returns the current
`generation`, a `ranks` list with each rank's last collective (its number,
`op` and when it was `entered`), and a one-line `summary` naming the ranks
that hold the others up. On a function gang, a rank that hasn't started
shows why it is waiting instead. A wait timeout on `gang.result(timeout=)`
and a collective's own `timeout=` both put that summary in their message:

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

When ranks call different collectives at the same point, the summary reads
`the ranks call different collectives as #1: rank 0 all_gather, rank 1 broadcast`.
`explain()` sees the rendezvous and the four store collectives, not calls
inside `torch.distributed`. A hang inside NCCL shows up as every rank in the
same store collective with none behind; add a `g.barrier()` before a suspect
step to narrow it down.
[observability.md](observability.md) covers `explain()` for every kind of call.

Next: choose CPUs, memory and GPUs for each rank in [resources.md](resources.md).
