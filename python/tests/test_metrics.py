"""ch.context().report() and Future.metrics(): a Tune-style sweep with early stopping."""

from __future__ import annotations

import os
import tempfile
import time

import pytest

import chimaera as ch


@ch.remote
def prefetch(marker_dir, name):
    """A helper the trial starts: it must stop when its trial is cancelled."""
    open(os.path.join(marker_dir, f"started {name}"), "w").close()
    for _ in range(300):
        if ch.context().cancelled():
            open(os.path.join(marker_dir, f"cancelled {name}"), "w").close()
            return "cancelled"
        time.sleep(0.05)
    open(os.path.join(marker_dir, f"ran out {name}"), "w").close()
    return "ran out"


@ch.remote
def trial(lr, marker_dir):
    prefetch.remote(marker_dir, f"lr={lr}")
    loss = 10.0
    for epoch in range(100):  # long enough that no trial can finish before the rung
        if ch.context().cancelled():
            return None
        loss *= 1 - lr
        ch.context().report(epoch=epoch, loss=loss)
        time.sleep(0.05)
    return loss


def test_early_stopping_cancels_losing_trials_and_their_subtrees():
    marker_dir = tempfile.mkdtemp()
    grid = [0.3, 0.28, 0.01, 0.02]
    trials = {lr: trial.remote(lr, marker_dir) for lr in grid}
    # One rung, as a successive-halving scheduler does: once every trial has
    # reported epoch 4, stop those whose loss there is over twice the best.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        rung = {lr: m[4]["loss"] for lr, f in trials.items() if len(m := f.metrics()) >= 5}
        if len(rung) == len(grid):
            break
        time.sleep(0.1)
    best = min(rung.values())
    stopped = {lr for lr, loss in rung.items() if loss > 2 * best}
    for lr in stopped:
        assert trials[lr].cancel() is True, f"trial lr={lr} finished before the rung"
    assert stopped == {0.01, 0.02}
    for lr in (0.3, 0.28):
        assert trials[lr].result(timeout=60) < 0.01
        reports = trials[lr].metrics()
        assert [r["epoch"] for r in reports] == list(range(100))
    for lr in stopped:
        with pytest.raises(ch.Cancelled):
            trials[lr].result(timeout=60)
    # The losers' helpers were cancelled with them: one already running sees
    # it and stops; one still queued (on a small machine) never starts. The
    # winners' helpers were not cancelled.
    def markers():
        return set(os.listdir(marker_dir))

    for _ in range(100):
        if all(f"started lr={lr}" not in markers() or f"cancelled lr={lr}" in markers() for lr in stopped):
            break
        time.sleep(0.1)
    for lr in stopped:
        assert f"ran out lr={lr}" not in markers()
        assert f"started lr={lr}" not in markers() or f"cancelled lr={lr}" in markers(), sorted(markers())
    assert not {f"cancelled lr={lr}" for lr in (0.3, 0.28)} & markers()


@ch.remote
class Learner:
    def fit(self, steps):
        for s in range(steps):
            ch.context().report(step=s)
        return steps


def test_actor_methods_report_too_and_reports_need_keywords():
    learner = Learner.remote()
    f = learner.fit.remote(3)
    assert f.result(timeout=60) == 3
    assert f.metrics() == [{"step": 0}, {"step": 1}, {"step": 2}]
    quiet = learner.fit.remote(0)
    assert quiet.result(timeout=60) == 0 and quiet.metrics() == []

    @ch.remote
    def empty():
        ch.context().report()

    with pytest.raises(TypeError, match="as keywords"):
        empty.remote().result(timeout=60)
