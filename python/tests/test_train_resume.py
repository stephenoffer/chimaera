"""Recoverable training, end to end: a two-rank DDP job on gloo loses a rank
mid-epoch, comes back as a new generation, restores the last committed
checkpoint and loader cursor, and ends with the weights and the sample
sequence of a run that never failed. Also the loader through a real torch
DataLoader, and even=True on uneven ranks. Needs torch and pyarrow."""

from __future__ import annotations

import json
import os
import uuid

import pytest

torch = pytest.importorskip("torch")
pq = pytest.importorskip("pyarrow.parquet")

import pyarrow as pa  # noqa: E402

import chimaera as ch  # noqa: E402


def parquet(tmp_path, rows: list[int]) -> str:
    """One file per entry of `rows`, with columns id, x and y = 3x + 1."""
    base = 0
    for i, n in enumerate(rows):
        xs = [((base + j) * 37 % 101) / 101 for j in range(n)]
        table = pa.table({"id": list(range(base, base + n)), "x": xs, "y": [3 * x + 1 for x in xs]})
        pq.write_table(table, tmp_path / f"part-{i}.parquet")
        base += n
    return str(tmp_path / "*.parquet")


@ch.remote(cpus=1)
def train(name: str, root: str, pattern: str, crash_at: int | None, logs: str):
    """20 SGD steps of y = wx + b under DDP, a checkpoint every 5; rank 1 of
    generation 1 dies after step `crash_at`. Logs each step's sample ids."""
    import torch
    import torch.nn as nn

    g = ch.context().gang
    g.torch()  # gloo: the default process group, for DDP and the loader's even check
    torch.manual_seed(0)
    model = nn.Linear(1, 1)
    ddp = nn.parallel.DistributedDataParallel(model)
    opt = torch.optim.SGD(ddp.parameters(), lr=0.1)
    loader = ch.data.loader(
        pattern, batch_size=4, num_workers=2, seed=1,
        collate=lambda rows: (torch.tensor([[r["x"]] for r in rows]), torch.tensor([[r["y"]] for r in rows]),
                              [r["id"] for r in rows]),
    )  # fmt: skip
    ckpts = ch.dist.Checkpoints(name, root)
    ckpt, step = ckpts.latest(), 0
    if ckpt:
        state = torch.load(ckpt.path("model.pt", rank=0))
        model.load_state_dict(state["model"])
        opt.load_state_dict(state["opt"])
        loader.load_state(ckpt.cursor)
        step = ckpt.step
    with open(os.path.join(logs, f"g{g.generation}-r{g.rank}.jsonl"), "w") as log:
        while step < 20:
            for x, y, ids in loader:
                step += 1
                opt.zero_grad()
                ((ddp(x) - y) ** 2).mean().backward()
                opt.step()
                log.write(json.dumps({"step": step, "ids": ids}) + "\n")
                log.flush()
                if step % 5 == 0:
                    with ckpts.save(step, cursor=loader.state()) as d:
                        if g.rank == 0:
                            torch.save({"model": model.state_dict(), "opt": opt.state_dict()}, d / "model.pt")
                if step == crash_at and g.generation == 1 and g.rank == 1:
                    os._exit(1)
                if step == 20:
                    break
    return [p.item() for p in model.parameters()], ckpt.step if ckpt else None, loader.state()["epoch"]


def steps(path) -> dict[int, list[int]]:
    with open(path) as f:
        return {e["step"]: e["ids"] for e in map(json.loads, f)}


def test_a_ddp_job_that_loses_a_rank_resumes_with_the_weights_and_samples_of_one_that_did_not(tmp_path):
    # Files of four lengths: however an epoch's shuffle pairs them, a rank's two loader workers run out at
    # different times, and the ranks hold different amounts (even=True cuts each epoch at the shorter one,
    # at least 8 batches, so steps 5 and 7 are mid-epoch).
    pattern = parquet(tmp_path, [48, 20, 32, 12])
    runs = {}
    for label, crash in (("whole", None), ("crashed", 7)):
        logs = tmp_path / label
        logs.mkdir()
        gang = train.options(ranks=2, collective_timeout=60).gang(f"t-{uuid.uuid4().hex[:8]}", str(tmp_path / "ckpt"),
                                                                  pattern, crash, str(logs))  # fmt: skip
        runs[label] = (gang.result(timeout=280), gang.generation, logs)
    (whole, g_whole, whole_logs), (crashed, g_crashed, crashed_logs) = runs["whole"], runs["crashed"]
    assert (g_whole, g_crashed) == (1, 2)
    assert [restored for _, restored, _ in crashed] == [5, 5], "generation 2 restores step 5, the last commit"
    assert all(epoch >= 1 for *_, epoch in whole), "the 20 steps cross into a later, reshuffled epoch"
    for r in (0, 1):
        reference = steps(whole_logs / f"g1-r{r}.jsonl")
        first, second = steps(crashed_logs / f"g1-r{r}.jsonl"), steps(crashed_logs / f"g2-r{r}.jsonl")
        assert sorted(second) == list(range(6, 21)), "generation 2 runs steps 6 to 20"
        assert {s: first[s] for s in (6, 7) if s in first} == {s: second[s] for s in (6, 7) if s in first}, (
            "the replayed steps take the same samples"
        )
        assert [first[s] for s in range(1, 6)] + [second[s] for s in range(6, 21)] == [
            reference[s] for s in range(1, 21)
        ], f"rank {r}: committed steps, then the replay, give the uninterrupted run's sample sequence"
    for (w, _, _), (w2, _, _) in zip(whole, crashed, strict=True):
        assert w2 == pytest.approx(w, abs=1e-6), "the same weights as the run that never failed"


def jsonl(tmp_path, sizes: list[int]) -> str:
    for i, n in enumerate(sizes):
        with open(tmp_path / f"part-{i}.jsonl", "w") as f:
            f.writelines(json.dumps({"id": f"{i}-{j}"}) + "\n" for j in range(n))
    return str(tmp_path / "*.jsonl")


def test_a_dataloader_resumed_at_any_batch_goes_on_as_if_never_stopped(tmp_path):
    pattern = jsonl(tmp_path, [5, 9, 2, 7, 3, 11, 4])  # three workers that run out at different times

    def run(state=None, take=None):
        loader = ch.data.loader(pattern, batch_size=2, num_workers=3, seed=4, collate=lambda rows: [r["id"] for r in rows])
        if state is not None:
            loader.load_state(state)
        out = []
        for batch in loader:
            out.append(batch)
            if take is not None and len(out) == take:
                return out, loader.state()
        return out, None

    whole, _ = run()
    from chimaera import _loader

    parts = ch.data.partitions(pattern)
    emulated = [b for b, _ in _loader._Stream(parts, ch.data.loader(pattern, batch_size=2, seed=4)._mine(), 3, 0,
                                              [[0, 0]] * 3, 2, lambda rows: [r["id"] for r in rows])]  # fmt: skip
    assert whole == emulated, "the DataLoader's order is the in-process round-robin's"
    for cut in (1, 2, 4, 7, len(whole) - 1):
        head, state = run(take=cut)
        rest, _ = run(state=json.loads(json.dumps(state)))
        assert head + rest == whole, f"resumed after {cut} batches"


@ch.remote(cpus=1)
def uneven(pattern: str):
    g = ch.context().gang
    g.torch()
    loader = ch.data.loader(pattern, batch_size=2, shuffle=False)
    got = [len(batch) for batch in loader]
    return len(got), loader.stopped_early, loader.epoch


def test_even_ranks_stop_together_when_one_runs_out(tmp_path):
    pattern = jsonl(tmp_path, [20, 4])  # rank 0 has ten batches, rank 1 two
    out = uneven.options(ranks=2, collective_timeout=30).gang(pattern).result(timeout=120)
    assert out == [(2, 1, 1), (2, 0, 1)], "both take two batches; rank 0 stops early, and both move to epoch 1"
