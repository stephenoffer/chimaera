"""ch.data.loader: rank and worker shares of partitioned input, epochs by seed,
and a cursor that resumes at the exact next batch. The DataLoader path and
even=True need torch: test_train_resume.py covers them."""

from __future__ import annotations

import collections
import json

import pytest

import chimaera as ch
from chimaera import _loader

SIZES = [3, 1, 4, 1, 5, 9, 2, 6]  # rows per file: uneven on purpose


def files(tmp_path, sizes=SIZES) -> str:
    for i, n in enumerate(sizes):
        with open(tmp_path / f"part-{i}.jsonl", "w") as f:
            f.writelines(json.dumps({"id": f"{i}-{j}"}) + "\n" for j in range(n))
    return str(tmp_path / "*.jsonl")


def ids(batches) -> list[str]:
    return [row["id"] for batch in batches for row in batch]


def as_rank(loader, rank: int, world: int):
    """The loader as rank `rank` of `world` (outside a gang it is rank 0 of 1)."""
    loader.rank, loader.world = rank, world
    return loader


def test_ranks_read_disjoint_shares_that_cover_every_row_each_epoch(tmp_path):
    pattern = files(tmp_path)
    for epoch in (0, 1):
        shares = []
        for r in range(3):
            loader = as_rank(ch.data.loader(pattern, batch_size=2, even=False), r, 3)
            loader.set_epoch(epoch)
            shares.append(ids(loader))
        assert sum(len(s) for s in shares) == sum(SIZES) == len(set().union(*shares)), shares


def test_a_ranks_rows_do_not_depend_on_its_worker_count_and_workers_take_turns(tmp_path):
    parts = ch.data.partitions(files(tmp_path))
    mine = list(range(len(parts)))
    seen = {}
    for workers in (1, 2, 3, 4):
        stream = _loader._Stream(parts, mine, workers, 0, [[0, 0]] * workers, 2, None)
        out = list(stream)
        seen[workers] = collections.Counter(row["id"] for batch, _ in out for row in batch)
        order = [k for _, (k, _, _) in out]
        # Round-robin: each worker's turn comes in order until it runs out.
        alive, expect = list(range(workers)), []
        counts = collections.Counter(order)
        while alive:
            for k in list(alive):
                if counts[k] == 0:
                    alive.remove(k)
                else:
                    expect.append(k)
                    counts[k] -= 1
        assert order == expect, (workers, order)
    assert all(c == seen[1] for c in seen.values())


def test_a_restored_stream_goes_on_exactly_where_the_cursor_stood_whatever_worker_was_next(tmp_path):
    parts = ch.data.partitions(files(tmp_path))
    mine, workers = list(range(len(parts))), 3
    whole = list(_loader._Stream(parts, mine, workers, 0, [[0, 0]] * workers, 2, None))
    for cut in range(len(whole) + 1):
        positions, first = [[0, 0] for _ in range(workers)], 0
        for _, (k, i, rows) in whole[:cut]:  # as Loader.__iter__ advances its cursor
            positions[k], first = [i, rows], (k + 1) % workers
        rest = list(_loader._Stream(parts, mine, workers, first, positions, 2, None))
        assert [b for b, _ in whole[:cut] + rest] == [b for b, _ in whole], f"resumed after {cut} batches"


def test_state_and_load_state_resume_mid_epoch_and_at_an_epoch_boundary(tmp_path):
    pattern = files(tmp_path)
    reference = ch.data.loader(pattern, batch_size=3, seed=7)
    epochs = [ids(reference), ids(reference)]
    assert sorted(epochs[0]) == sorted(epochs[1]) and epochs[0] != epochs[1], "each epoch reshuffles"

    loader = ch.data.loader(pattern, batch_size=3, seed=7)
    it = iter(loader)
    taken = [next(it) for _ in range(4)]
    state = json.loads(json.dumps(loader.state()))  # as a checkpoint's manifest holds it
    resumed = ch.data.loader(pattern, batch_size=3, seed=7)
    resumed.load_state(state)
    resumed.set_epoch(0)  # the restored epoch: its cursor stays
    assert ids(taken) + ids(resumed) == epochs[0]
    assert resumed.epoch == 1 and ids(resumed) == epochs[1]

    done = ch.data.loader(pattern, batch_size=3, seed=7)
    list(done)  # a whole epoch: the cursor stands at the next one's start
    after = ch.data.loader(pattern, batch_size=3, seed=7)
    after.load_state(done.state())
    assert ids(after) == epochs[1]


def test_a_cursor_from_other_input_or_another_split_is_refused(tmp_path):
    pattern = files(tmp_path)
    state = ch.data.loader(pattern, batch_size=2).state()
    with pytest.raises(ValueError, match="world=1 can't resume one with 2"):
        as_rank(ch.data.loader(pattern, batch_size=2), 0, 2).load_state(state)
    with pytest.raises(ValueError, match="batch_size=2 can't resume one with 4"):
        ch.data.loader(pattern, batch_size=4).load_state(state)
    with open(tmp_path / "part-9.jsonl", "w") as f:
        f.write(json.dumps({"id": "new"}) + "\n")
    with pytest.raises(ValueError, match="partitions="):
        ch.data.loader(pattern, batch_size=2).load_state(state)


def test_set_epoch_starts_another_epoch_from_its_beginning_and_shuffle_false_keeps_source_order(tmp_path):
    pattern = files(tmp_path)
    loader = ch.data.loader(pattern, batch_size=100, shuffle=False)
    assert ids(loader) == [f"{i}-{j}" for i, n in enumerate(SIZES) for j in range(n)]
    shuffled = ch.data.loader(pattern, batch_size=100, seed=3)
    shuffled.set_epoch(5)
    first = ids(shuffled)
    again = ch.data.loader(pattern, batch_size=100, seed=3)
    again.set_epoch(5)
    assert ids(again) == first, "the same seed and epoch give the same order"


def test_even_loaders_refuse_what_would_leave_a_rank_waiting(tmp_path):
    pattern = files(tmp_path, [2, 2])
    with pytest.raises(ValueError, match="2 partition\\(s\\) for 3 ranks"):
        iter(as_rank(ch.data.loader(pattern, batch_size=1, even=True), 0, 3)).__next__()
    with pytest.raises(ValueError, match="torch.distributed"):
        next(iter(as_rank(ch.data.loader(pattern, batch_size=1, even=True), 0, 2)))
    with pytest.raises(ValueError, match="in-memory items"):
        ch.data.loader(list(range(10)), batch_size=2, num_workers=2)
    for bad in (0, True, 1.5):
        with pytest.raises(ValueError, match="batch_size"):
            ch.data.loader(pattern, batch_size=bad)


@ch.remote(cpus=0.5)
def read_my_share(pattern: str) -> list[str]:
    loader = ch.data.loader(pattern, batch_size=2, even=False)
    return [ch.context().gang.rank, ids(loader)]


def test_in_a_gang_each_rank_reads_its_own_share(tmp_path):
    pattern = files(tmp_path)
    shares = read_my_share.options(ranks=2).gang(pattern).result(timeout=120)
    assert [r for r, _ in shares] == [0, 1]
    got = [s for _, s in shares]
    assert not set(got[0]) & set(got[1]) and len(got[0]) + len(got[1]) == sum(SIZES)


@ch.remote(cpus=0.5)
def read_with(loader) -> list:
    return [ch.context().gang.rank, ids(loader)]


def test_a_loader_made_in_the_driver_reads_each_ranks_share_inside_the_gang(tmp_path):
    loader = ch.data.loader(files(tmp_path), batch_size=2, even=False)
    shares = read_with.options(ranks=2).gang(loader).result(timeout=120)
    got = [s for _, s in shares]
    assert not set(got[0]) & set(got[1]) and len(got[0]) + len(got[1]) == sum(SIZES)
