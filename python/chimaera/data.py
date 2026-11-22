"""Partitioned data: cut input into partitions (``partitions``), regroup
results by key between calls (``Results.reduce_by_key``) and write each
partition from the worker that holds it (``Results.write``)."""

from ._data import Partition, Partitioned, partitions

__all__ = ["Partition", "Partitioned", "partitions"]
