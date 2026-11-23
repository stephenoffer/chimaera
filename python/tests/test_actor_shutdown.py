"""close(drain=True): earlier calls finish, then the actor's __exit__ runs."""

import os
import tempfile
import time

import pytest

import chimaera as ch


@ch.remote
class Journal:
    def __init__(self, path: str, exit_mode: str = "write") -> None:
        self.path, self.exit_mode = path, exit_mode

    def write(self, text: str) -> None:
        time.sleep(0.05)
        with open(self.path, "a") as f:
            f.write(text + "\n")

    def __exit__(self, *exc: object) -> None:
        if self.exit_mode == "hang":
            time.sleep(60)
        if self.exit_mode == "raise":
            raise RuntimeError("could not flush")
        with open(self.path, "a") as f:
            f.write("exit\n")


def _path() -> str:
    return os.path.join(tempfile.mkdtemp(prefix="chx", dir="/tmp"), "journal")


def _lines(path: str) -> list[str]:
    return open(path).read().split() if os.path.exists(path) else []


def test_drain_finishes_earlier_calls_then_runs_exit_and_ends_the_actor():
    path = _path()
    j = Journal.remote(path)
    for i in range(3):
        j.write.remote(f"w{i}")
    j.close(drain=True)
    assert _lines(path) == ["w0", "w1", "w2", "exit"]
    with pytest.raises(ch.ActorDied):
        j.write.remote("late").result(timeout=30)


def test_close_without_drain_ends_at_once_without_exit():
    path = _path()
    j = Journal.remote(path)
    j.write.remote("w0").result(timeout=30)
    j.close()
    with pytest.raises(ch.ActorDied):  # the actor is gone, so its __exit__ can no longer run
        j.write.remote("late").result(timeout=30)
    assert "exit" not in _lines(path)


def test_an_exit_that_hangs_is_cut_off_at_the_timeout():
    j = Journal.remote(_path(), "hang")
    j.write.remote("w0").result(timeout=30)
    start = time.monotonic()
    with pytest.warns(UserWarning, match="did not finish draining"):
        j.close(drain=True, timeout=1.0)
    assert time.monotonic() - start < 10
    with pytest.raises(ch.ActorDied):
        j.write.remote("late").result(timeout=30)


def test_an_exit_that_raises_ends_the_actor_and_raises_here():
    j = Journal.remote(_path(), "raise")
    with pytest.raises(RuntimeError, match="could not flush"):
        j.close(drain=True)
    with pytest.raises(ch.ActorDied):
        j.write.remote("late").result(timeout=30)
