"""Which failures are retried, and the priority executors run at."""

import os
import tempfile
import time

import pytest

import chimaera as ch


class Permanent(ValueError, ch.errors.NonRetryable):
    pass


def _lines(path: str) -> int:
    return sum(1 for _ in open(path))


def _tmpfile(name: str) -> str:
    return os.path.join(tempfile.mkdtemp(prefix="chp", dir="/tmp"), name)


@ch.remote(retry_exceptions=(ValueError,), max_attempts=5)
def fails_until_third(path: str, permanent: bool) -> int:
    with open(path, "a") as f:
        f.write("x\n")
    n = _lines(path)
    if n < 3:
        raise (Permanent if permanent else ValueError)(f"attempt {n}")
    return n


def test_a_matching_error_is_retried():
    assert fails_until_third.remote(_tmpfile("v"), False).result(timeout=60) == 3


def test_a_non_retryable_error_runs_once_even_when_its_type_is_retried():
    path = _tmpfile("p")
    with pytest.raises(Permanent, match="attempt 1"):
        fails_until_third.remote(path, True).result(timeout=60)
    assert _lines(path) == 1
    # The rule calls the peer runs several to a batch share.
    from chimaera._executor import _matches

    class Refused(ConnectionRefusedError, ch.errors.NonRetryable):
        pass

    assert _matches(ConnectionRefusedError(), ["builtins.ConnectionError"])
    assert not _matches(Refused(), ["builtins.ConnectionError"])


def drop_twice(path: str) -> int:
    """Records each attempt's start time; the first loses its connection, the second times out."""
    with open(path, "a") as f:
        f.write(f"{time.time()}\n")
    n = _lines(path)
    if n == 1:
        raise ConnectionResetError("connection reset by peer")
    if n == 2:
        raise TimeoutError("read timed out")
    return n


drops_twice = ch.remote(drop_twice)


@ch.remote
def wrong(path: str) -> int:
    with open(path, "a") as f:
        f.write("x\n")
    raise ValueError("wrong")


@ch.remote
def nap() -> None:
    time.sleep(3)


@ch.remote
def waits_on_nap(path: str) -> None:
    with open(path, "a") as f:
        f.write("x\n")
    nap.remote().result(timeout=0.2)


@ch.remote
class Session:
    def drop_twice(self, path: str) -> int:
        return drop_twice(path)


def _waits(path: str) -> list[float]:
    starts = [float(x) for x in open(path).read().split()]
    return [round(b - a, 3) for a, b in zip(starts, starts[1:])]


def test_a_dropped_connection_is_retried_by_default_after_a_short_wait():
    # Ray retries no application exception unless told: one network blip and
    # the call (and its job) fails.
    path = _tmpfile("c")
    assert drops_twice.remote(path).result(timeout=60) == 3
    first, second = _waits(path)
    assert first >= 0.1 and second >= 0.2, f"retries started {first} s and {second} s after the failed attempts"
    paths = [_tmpfile(f"m{i}") for i in range(3)]  # map items too, however they are grouped into calls
    assert list(drops_twice.map(paths)) == [3, 3, 3]
    for p in paths:
        first, second = _waits(p)
        assert first >= 0.1 and second >= 0.2, (p, first, second)


def test_calls_that_may_not_run_again_are_not_retried_by_default():
    for f in (drops_twice.options(effects="external"), drops_twice.options(retry_exceptions=())):
        path = _tmpfile("n")
        with pytest.raises(ConnectionResetError):
            f.remote(path).result(timeout=60)
        assert _lines(path) == 1, f
        path = _tmpfile("nm")
        with pytest.raises(ConnectionResetError):
            list(f.map([path]))
        assert _lines(path) == 1, f
    path = _tmpfile("a")  # an actor's state may already have changed
    with pytest.raises(ConnectionResetError):
        Session.remote().drop_twice.remote(path).result(timeout=60)
    assert _lines(path) == 1
    path = _tmpfile("w")  # only transient errors are retried unlisted
    with pytest.raises(ValueError):
        wrong.remote(path).result(timeout=60)
    assert _lines(path) == 1
    path = _tmpfile("t")  # a wait in the call that ran out: a retry would start nap again
    with pytest.raises(TimeoutError, match="no result within"):
        waits_on_nap.remote(path).result(timeout=60)
    assert _lines(path) == 1



@ch.remote
def niceness() -> int:
    return os.getpriority(os.PRIO_PROCESS, 0)


def test_executors_run_at_a_lower_cpu_priority_than_the_driver():
    mine = os.getpriority(os.PRIO_PROCESS, 0)
    theirs = niceness.remote().result(timeout=60)
    assert theirs >= min(mine + 5, 19), (mine, theirs)
