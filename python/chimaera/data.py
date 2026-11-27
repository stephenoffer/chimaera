"""Partitioned data: cut input into partitions (``partitions``), regroup
results by key between calls (``Results.reduce_by_key``), write each
partition from the worker that holds it (``Results.write``), and feed a
training loop its rank's share (``loader``)."""

from ._data import Partition, Partitioned, partitions
from ._loader import Loader, loader

__all__ = ["Loader", "Partition", "Partitioned", "loader", "partitions"]
