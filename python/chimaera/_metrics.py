"""Progress reports from inside a call: ``ch.context().report(loss=0.3)``,
read by whoever holds the call's future: ``future.metrics()``.

    @ch.remote
    def trial(lr):
        for epoch in range(20):
            loss = train_one_epoch(lr)
            ch.context().report(epoch=epoch, loss=loss)
            if ch.context().cancelled():
                return None

    trials = [trial.remote(lr) for lr in grid]
    ...
    for f in trials:
        if (m := f.metrics()) and m[-1]["loss"] > 2 * best:
            f.cancel()                       # early stopping; its subtree stops too

Each report is one write to the replicated store, in a namespace per call so
many calls' reports spread across shards: report progress (per epoch, per
thousand steps), not every sample. A retried call starts a fresh list.
Reports expire a day after they are written.
"""

from __future__ import annotations

from typing import Any

TTL = "24h"


def _namespace(key: bytes) -> Any:
    from ._store import Store

    return Store(f"metrics/{key.hex()}")


def report(ctx: Any, metrics: dict[str, Any]) -> None:
    if not metrics:
        raise TypeError("report() takes the metrics as keywords: report(loss=0.3, epoch=2)")
    ctx._reports += 1
    _namespace(ctx.run + ctx.task).put(f"{ctx.attempt:04d}/{ctx._reports:08d}", dict(metrics), ttl=TTL)


def reports(key: bytes) -> list[dict[str, Any]]:
    """The reports of the call's latest attempt, oldest first."""
    entries = list(_namespace(key).scan())
    if not entries:
        return []
    latest = max(k.split("/", 1)[0] for k, _ in entries)
    return [v for k, v in entries if k.startswith(latest + "/")]
