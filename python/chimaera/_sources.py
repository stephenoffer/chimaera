"""A map's items: its sources zipped as builtin ``map`` does, read with
``async for`` when any of them is an async iterable."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Iterator
from typing import Any


def map_source(sources: tuple[Any, ...]) -> Iterator[Any] | AsyncIterator[Any]:
    """The items of ``fn.map(*sources)``: an async iterator if any source is
    an async iterable (one with ``__aiter__`` and no ``__iter__``)."""
    if not any(_is_async(s) for s in sources):
        return iter(sources[0]) if len(sources) == 1 else zip(*sources)
    return aiter(sources[0]) if len(sources) == 1 else _azip(sources)


def _is_async(source: Any) -> bool:
    return hasattr(source, "__aiter__") and not hasattr(source, "__iter__")


async def _azip(sources: Iterable[Any]) -> AsyncIterator[tuple[Any, ...]]:
    """zip() over sources some of which are async; ends with the shortest,
    and closes the async ones however it ends."""
    its = [(aiter(s), True) if _is_async(s) else (iter(s), False) for s in sources]
    try:
        while True:
            row = []
            for it, is_async in its:
                try:
                    row.append(await anext(it) if is_async else next(it))
                except (StopIteration, StopAsyncIteration):
                    return
            yield tuple(row)
    finally:
        for it, is_async in its:
            aclose = getattr(it, "aclose", None) if is_async else None
            if aclose is not None:
                await aclose()
