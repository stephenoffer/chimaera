"""Feed a training loop from partitioned input: ``ch.data.loader``.

    loader = ch.data.loader("data/*.parquet", batch_size=32, columns=["id", "x", "y"],
                            num_workers=2, collate=torch.utils.data.default_collate)
    ckpt = ckpts.latest()
    if ckpt:
        loader.load_state(ckpt.cursor)
    while loader.epoch < epochs:                   # a restore may have moved it on
        for batch in loader:                       # this rank's share of this epoch
            ...
            with ckpts.save(step, cursor=loader.state()) as d:
                ...

Each epoch shuffles the partitions by seed and epoch; rank r of W takes every
W-th, and loader worker k of K every K-th of those, so ranks read disjoint
input and a rank's input does not depend on its worker count. Batches come
out in a fixed round-robin over the workers. The cursor (``state()``) holds
where each worker is, advanced only when the loop takes a batch: batches a
worker read ahead are not in it, so a restore replays exactly the batches
taken after the save.
"""

from __future__ import annotations

import hashlib
import itertools
import random
import warnings
from collections.abc import Callable, Iterator, Sequence
from typing import Any

import cloudpickle

from ._data import ItemsSource, Partition, partitions


class Loader:
    """This rank's batches of a list of partitions, epoch by epoch (see
    ``ch.data.loader``). Iterating it runs the current epoch from where it
    stands and then moves to the next; ``set_epoch`` jumps to one."""

    def __init__(
        self,
        parts: list[Partition],
        batch_size: int,
        num_workers: int,
        seed: int,
        shuffle: bool,
        collate: Callable[[list[Any]], Any] | None,
        even: bool | None,
    ) -> None:
        self.parts, self.batch_size, self.num_workers = parts, batch_size, num_workers
        self.seed, self.shuffle, self.collate, self._even = seed, shuffle, collate, even
        self._rank: int | None = None  # set to read as that rank; else the gang's, where it is used
        self._world: int | None = None
        self.workers = max(1, num_workers)
        self.digest = hashlib.sha256(repr([p.part for p in parts]).encode()).hexdigest()[:16]
        self.epoch, self.stopped_early = 0, 0  # epochs this rank ended with batches left (``even``)
        self._start()

    def __repr__(self) -> str:
        return f"<Loader rank {self.rank}/{self.world} epoch {self.epoch} of {len(self.parts)} partition(s)>"

    @property
    def rank(self) -> int:
        """This process's rank in its gang where the loader is iterated (0 outside one)."""
        return self._rank if self._rank is not None else getattr(_gang(), "rank", 0)

    @rank.setter
    def rank(self, value: int) -> None:
        self._rank = value

    @property
    def world(self) -> int:
        return self._world if self._world is not None else getattr(_gang(), "size", 1)

    @world.setter
    def world(self, value: int) -> None:
        self._world = value

    @property
    def even(self) -> bool:
        return self.world > 1 if self._even is None else self._even

    def _start(self) -> None:
        self.positions = [[0, 0] for _ in range(self.workers)]  # per worker: [partition index, rows read in it]
        self.next_worker = 0

    def set_epoch(self, epoch: int) -> None:
        """Run `epoch` next, from its start; the current epoch (one a restore
        left halfway) is left where it stands."""
        if epoch != self.epoch:
            self.epoch = epoch
            self._start()

    def state(self) -> dict[str, Any]:
        """Where this rank stands, as JSON: what ``load_state`` resumes from."""
        return {
            "epoch": self.epoch, "positions": [list(p) for p in self.positions], "next_worker": self.next_worker,
            "rank": self.rank, "world": self.world, "workers": self.workers, "seed": self.seed,
            "shuffle": self.shuffle, "batch_size": self.batch_size, "partitions": [len(self.parts), self.digest],
        }  # fmt: skip

    def load_state(self, state: dict[str, Any]) -> None:
        """Resume where ``state()`` was. ValueError if it was taken with other
        input, another rank or world, worker count, seed or batch size:
        those split the input another way."""
        mine = self.state()
        for key in ("rank", "world", "workers", "seed", "shuffle", "batch_size", "partitions"):
            if state.get(key) != mine[key]:
                raise ValueError(f"a loader state saved with {key}={state.get(key)!r} can't resume one with {mine[key]!r}")
        self.epoch, self.next_worker = state["epoch"], state["next_worker"]
        self.positions = [list(p) for p in state["positions"]]

    def _mine(self) -> list[int]:
        """This rank's partitions this epoch, in order (indexes into parts)."""
        order = list(range(len(self.parts)))
        if self.shuffle:
            random.Random(f"{self.seed}:{self.epoch}").shuffle(order)
        return order[self.rank :: self.world]

    def __iter__(self) -> Iterator[Any]:
        if self.even and self.world > 1 and len(self.parts) < self.world:
            raise ValueError(
                f"{len(self.parts)} partition(s) for {self.world} ranks: with even=True every rank needs one; "
                "cut the input finer (FileSource split_bytes=) or pass even=False"
            )
        agree = _agreement() if self.even and self.world > 1 else None
        stream = _Stream(self.parts, self._mine(), self.workers, self.next_worker, self.positions,
                         self.batch_size, self.collate)  # fmt: skip
        it: Iterator[Any] = iter(stream) if self.num_workers == 0 else stream.load(self.num_workers)
        try:
            while True:
                item = next(it, None)
                if agree is not None and not agree(item is not None, self.epoch):
                    if item is not None:  # another rank ran out: this one stops too, or a collective would hang
                        self.stopped_early += 1
                        if self.stopped_early == 1:
                            warnings.warn(
                                f"rank {self.rank} stopped epoch {self.epoch} with batches left: another rank ran "
                                "out first (even=True drops the rest of an uneven epoch)",
                                stacklevel=2,
                            )
                    break
                if item is None:
                    break
                batch, (k, i, rows) = item
                self.positions[k] = [i, rows]
                self.next_worker = (k + 1) % self.workers
                yield batch
            self.epoch += 1
            self._start()
        finally:
            it = iter(())  # drop the DataLoader's iterator: its workers stop


def _gang() -> Any:
    from ._context import context

    return getattr(context(), "gang", None)


def _agreement() -> Callable[[bool, int], bool]:
    """Whether every rank has a batch, by one MIN all_reduce through
    torch.distributed (on the default group's device), which also checks
    that every rank is on the same epoch."""
    try:
        import torch
        import torch.distributed as dist
    except ImportError:
        raise ValueError("a loader with even=True on several ranks needs torch.distributed; pass even=False") from None
    if not dist.is_initialized():
        raise ValueError("a loader with even=True needs torch.distributed initialized first (group.torch())")
    device = torch.device("cuda", torch.cuda.current_device()) if dist.get_backend() == "nccl" else torch.device("cpu")

    def agree(have: bool, epoch: int) -> bool:
        t = torch.tensor([int(have), epoch, -epoch], dtype=torch.int64, device=device)
        dist.all_reduce(t, op=dist.ReduceOp.MIN)
        low, high = int(t[1]), -int(t[2])
        if low != high:
            raise RuntimeError(f"loader ranks are on different epochs ({low} to {high}): each must run every epoch")
        return bool(t[0])

    return agree


class _Stream:
    """The batches of one rank's epoch, from its workers in round-robin.
    Logical worker k reads ``mine[k::workers]`` from its position; a
    DataLoader's worker slot j runs logical worker ``(j + first) % workers``,
    so its round-robin starts where the last one stopped."""

    def __init__(self, parts: list[Partition], mine: list[int], workers: int, first: int,
                 positions: list[list[int]], batch_size: int, collate: Callable[[list[Any]], Any] | None) -> None:  # fmt: skip
        self.parts, self.mine, self.workers, self.first = parts, mine, workers, first
        self.positions, self.batch_size, self.collate = [list(p) for p in positions], batch_size, collate

    def __iter__(self) -> Iterator[tuple[Any, tuple[int, int, int]]]:
        """Batches of all logical workers, in round-robin (no DataLoader)."""
        streams = {k: self.worker(k) for k in itertools.islice(itertools.cycle(range(self.workers)), self.first,
                                                                self.first + self.workers)}  # fmt: skip
        while streams:
            for k in list(streams):
                item = next(streams[k], None)
                if item is None:
                    del streams[k]
                else:
                    yield item

    def worker(self, k: int) -> Iterator[tuple[Any, tuple[int, int, int]]]:
        """Logical worker k's batches, each with where it stands after it:
        (k, partition index, rows read in that partition)."""
        mine = self.mine[k :: self.workers]
        start, skip = self.positions[k]
        batch: list[Any] = []
        for i in range(start, len(mine)):
            rows = skip if i == start else 0
            for row in itertools.islice(self.parts[mine[i]].rows(), rows, None):
                batch.append(row)
                rows += 1
                if len(batch) == self.batch_size:
                    yield (self.collate(batch) if self.collate else batch), (k, i, rows)
                    batch = []
        if batch:
            yield (self.collate(batch) if self.collate else batch), (k, len(mine) - 1, rows)

    def load(self, num_workers: int) -> Iterator[Any]:
        """The same batches through a torch DataLoader with `num_workers`
        processes, one per logical worker."""
        from torch.utils.data import DataLoader

        return iter(DataLoader(_dataset(self), batch_size=None, num_workers=num_workers, collate_fn=_identity))


def _identity(item: Any) -> Any:
    return item  # with batch_size=None, DataLoader would otherwise convert NumPy arrays and tuples


_DATASET: type | None = None


def _dataset(stream: _Stream) -> Any:
    """`stream` as a torch IterableDataset (a class made on first use, so
    importing chimaera does not import torch). It pickles itself with
    cloudpickle: a spawned worker unpickles with plain pickle, which can't
    rebuild a collate defined in the driver's ``__main__``."""
    global _DATASET
    if _DATASET is None:
        from torch.utils.data import IterableDataset, get_worker_info

        class Dataset(IterableDataset):  # type: ignore[type-arg]
            def __init__(self, stream: _Stream) -> None:
                self.stream = stream

            def __iter__(self) -> Iterator[Any]:
                info = get_worker_info()
                slot = 0 if info is None else info.id
                return self.stream.worker((slot + self.stream.first) % self.stream.workers)

            def __reduce__(self) -> tuple[Any, ...]:
                return (_unpickle, (cloudpickle.dumps(self.stream),))

        _DATASET = Dataset
    return _DATASET(stream)


def _unpickle(data: bytes) -> Any:
    return _dataset(cloudpickle.loads(data))


def loader(
    source: Any,
    *,
    batch_size: int,
    columns: list[str] | None = None,
    filter: Any = None,  # noqa: A002
    num_workers: int = 0,
    seed: int = 0,
    shuffle: bool = True,
    collate: Callable[[list[Any]], Any] | None = None,
    even: bool | None = None,
) -> Loader:
    """This rank's batches of `source`: a path pattern, a ``Source``, or a
    list of ``ch.data.partitions(...)`` (``columns`` and ``filter`` apply to
    the first two). Inside a gang the partitions are shared out by rank;
    outside one, this process reads them all.

    `batch_size` rows make a batch (a worker's last may be short); `collate`
    turns the list of rows into what the loop takes, in the worker
    (``torch.utils.data.default_collate`` for tensors). `num_workers` > 0
    reads through a torch DataLoader with that many processes. `seed` and
    `shuffle` decide each epoch's partition order. With `even` (the default
    on several ranks; it needs torch.distributed), every rank stops when the
    first runs out, so an uneven epoch never leaves one rank waiting in a
    collective; the rest of that epoch is not read.

    Every rank must list the same partitions in the same order, which a
    ``FileSource`` on a filesystem they share does."""
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise ValueError(f"batch_size must be an integer >= 1, got {batch_size!r}")
    if isinstance(num_workers, bool) or not isinstance(num_workers, int) or num_workers < 0:
        raise ValueError(f"num_workers must be an integer >= 0, got {num_workers!r}")
    if isinstance(source, Sequence) and not isinstance(source, str) and all(isinstance(p, Partition) for p in source):
        parts = list(source)
    else:
        parts = partitions(source, columns=columns, filter=filter)
    if num_workers and any(isinstance(p.source, ItemsSource) for p in parts):
        raise ValueError("num_workers > 0 reads in other processes, which can't read in-memory items; use files")
    return Loader(parts, batch_size, num_workers, seed, shuffle, collate, even)
