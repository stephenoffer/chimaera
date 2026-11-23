"""Which failures are retried, and the priority executors run at."""

import os
import tempfile

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



@ch.remote
def niceness() -> int:
    return os.getpriority(os.PRIO_PROCESS, 0)


def test_executors_run_at_a_lower_cpu_priority_than_the_driver():
    mine = os.getpriority(os.PRIO_PROCESS, 0)
    theirs = niceness.remote().result(timeout=60)
    assert theirs >= min(mine + 5, 19), (mine, theirs)
