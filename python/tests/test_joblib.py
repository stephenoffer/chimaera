"""chimaera.joblib: joblib's Parallel running on Chimaera."""

import os
import time

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


def span(_):
    start = time.time()
    time.sleep(0.5)
    return start, time.time()


def test_no_more_than_n_jobs_batches_run_at_once():
    with parallel_config(backend="chimaera", n_jobs=2):
        Parallel(batch_size=1)(delayed(span)(i) for i in range(4))  # workers started
        spans = Parallel(batch_size=1)(delayed(span)(i) for i in range(12))  # joblib sends 2 * n_jobs ahead
    edges = sorted([(s, 1) for s, _ in spans] + [(e, -1) for _, e in spans])
    running = [sum(d for _, d in edges[: i + 1]) for i in range(len(edges))]
    assert max(running) == 2, spans


def test_a_batch_that_cannot_be_sent_raises_from_parallel_instead_of_hanging():
    import threading

    import chimaera as ch

    raised = []

    def run():  # batch 7 goes out after the first ones settle, from the backend's callback thread
        with parallel_config(backend="chimaera", n_jobs=2), pytest.raises(ch.SerializationError) as e:
            Parallel(batch_size=1)(delayed(square)(threading.Lock() if i == 7 else i) for i in range(12))
        raised.append(e.value)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(60)
    assert raised, "Parallel still waits for the batch that could not be sent"


def test_negative_n_jobs_counts_the_cpus_of_every_live_peer():
    from test_cluster import Cluster

    cluster = Cluster(2, cpus={0: 3, 1: 2})
    try:
        out = cluster.run(
            0,
            """
import time
import chimaera as ch
import chimaera.joblib

deadline = time.time() + 60
while sum(n.alive for n in ch.runs.cluster().nodes()) < 2 and time.time() < deadline:
    time.sleep(0.2)
backend = chimaera.joblib.ChimaeraBackend()
print("N_JOBS", backend.effective_n_jobs(-1), backend.effective_n_jobs(-2))
""",
        )
        assert out.returncode == 0, out.stderr[-3000:]
        assert "N_JOBS 5 4" in out.stdout, out.stdout
    finally:
        cluster.close()


def test_a_seeded_grid_search_matches_loky():
    pytest.importorskip("sklearn")
    from sklearn.datasets import load_iris
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import GridSearchCV, StratifiedKFold

    x, y = load_iris(return_X_y=True)

    def search(backend):
        grid = {"C": [0.01, 0.1, 1.0, 10.0]}
        cv = StratifiedKFold(3, shuffle=True, random_state=0)
        with parallel_config(backend=backend, n_jobs=2):
            return GridSearchCV(LogisticRegression(max_iter=500), grid, cv=cv).fit(x, y)

    here, there = search("loky"), search("chimaera")
    assert there.best_params_ == here.best_params_
    for k in ("split0_test_score", "split1_test_score", "split2_test_score", "mean_test_score"):
        assert list(there.cv_results_[k]) == list(here.cv_results_[k]), k


def test_generator_output_and_reuse_of_one_backend():
    with parallel_config(backend="chimaera", n_jobs=3):
        gen = Parallel(return_as="generator")(delayed(square)(i) for i in range(30))
        assert list(gen) == [i * i for i in range(30)]
        assert Parallel()(delayed(square)(i) for i in range(5)) == [0, 1, 4, 9, 16]
