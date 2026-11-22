"""chimaera.joblib: joblib's Parallel running on Chimaera."""

import os

import pytest

pytest.importorskip("joblib")

import chimaera.joblib  # noqa: E402,F401  (registers the backend)
from joblib import Parallel, delayed, parallel_config  # noqa: E402


def square(x):
    return x * x


def three_fails(x):
    if x == 3:
        raise ValueError("three is not allowed")
    return x


def test_results_come_back_in_order_from_chimaera_workers():
    with parallel_config(backend="chimaera", n_jobs=4):
        out = Parallel()(delayed(square)(i) for i in range(200))
        pids = set(Parallel()(delayed(os.getpid)() for _ in range(8)))
    assert out == [i * i for i in range(200)]
    assert os.getpid() not in pids, "the tasks ran in this process, not on Chimaera"


def test_a_task_that_raises_raises_from_parallel():
    with parallel_config(backend="chimaera", n_jobs=2), pytest.raises(ValueError, match="three is not allowed"):
        Parallel(batch_size=1)(delayed(three_fails)(i) for i in range(20))


def test_generator_output_and_reuse_of_one_backend():
    with parallel_config(backend="chimaera", n_jobs=3):
        gen = Parallel(return_as="generator")(delayed(square)(i) for i in range(30))
        assert list(gen) == [i * i for i in range(30)]
        assert Parallel()(delayed(square)(i) for i in range(5)) == [0, 1, 4, 9, 16]
