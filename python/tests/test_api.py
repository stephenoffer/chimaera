"""The core API contract, end to end through a real local peer."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

import pytest

import chimaera as ch
from conftest import run_script


@ch.remote
def word_count(text: str) -> int:
    return len(text.split())


@ch.remote
def add(left: int, right: int) -> int:
    return left + right


@ch.remote
def echo_kwargs(**kwargs):
    return kwargs


@ch.remote
def fail(msg: str):
    raise ValueError(msg)


@ch.remote
def write_marker(path: str) -> str:
    with open(path, "a") as f:
        f.write("ran\n")
    return path


@ch.remote
def sleep_then(seconds: float, value):
    time.sleep(seconds)
    return value


def test_design_doc_examples_print_2_4_5():
    r = run_script(
        """
        import chimaera as ch

        @ch.remote
        def word_count(text):
            return len(text.split())

        @ch.remote
        def add(left, right):
            return left + right

        @ch.remote
        class Counter:
            def __init__(self):
                self.value = 0
            def increment(self, amount):
                self.value += amount
                return self.value

        assert word_count("blue sky") == 2
        print(word_count.remote("blue sky").result())
        print(add.remote(word_count.remote("blue sky"), word_count.remote("rain today")).result())
        left = word_count.remote("ordinary Python")
        right = word_count.remote("across several machines")
        counter = Counter.remote()
        print(counter.increment.remote(add.remote(left, right)).result())
        """
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["2", "4", "5"]


def test_local_call_never_starts_the_runtime():
    r = run_script(
        """
        import chimaera as ch
        from chimaera._runtime import Runtime

        @ch.remote
        def f(x):
            return x + 1

        assert f(1) == 2
        assert Runtime.current() is None
        print("local only")
        """
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "local only"


def test_user_kwargs_named_like_options_reach_the_function():
    assert echo_kwargs.remote(cpus=64, memory="x", timeout=3).result() == {"cpus": 64, "memory": "x", "timeout": 3}


def test_wrong_arguments_fail_at_the_call_site():
    with pytest.raises(TypeError):
        add.remote(1)


def test_user_exception_keeps_type_message_and_remote_traceback():
    with pytest.raises(ValueError, match="bad input") as info:
        fail.remote("bad input").result()
    notes = "\n".join(getattr(info.value, "__notes__", []))
    assert "remote traceback" in notes and "raise ValueError(msg)" in notes


def test_dependent_of_a_failure_never_runs_and_raises_the_cause():
    marker = os.path.join(tempfile.mkdtemp(prefix="chm", dir="/tmp"), "dependent-ran")
    bad = fail.remote("upstream broke")

    @ch.remote
    def consume(_dep, path):
        with open(path, "a") as f:
            f.write("ran\n")

    with pytest.raises(ValueError, match="upstream broke") as info:
        consume.remote(bad, marker).result()
    assert not os.path.exists(marker), "a dependent of a failed call must not run"
    assert any("prerequisite" in n for n in getattr(info.value, "__notes__", []))


def test_infeasible_request_fails_immediately_with_the_shape():
    with pytest.raises(ch.Infeasible, match=r"cpus=64.*offers"):
        word_count.options(cpus=64).remote("x").result(timeout=10)
    with pytest.raises(ch.Infeasible, match="cuda"):
        word_count.options(device="cuda").remote("x").result(timeout=10)


def test_result_timeout_leaves_the_call_running():
    f = sleep_then.remote(0.5, "late")
    with pytest.raises(TimeoutError):
        f.result(timeout=0.05)
    assert f.result(timeout=10) == "late"


def test_cancel_races_commit_and_reports_the_winner():
    slow = sleep_then.remote(30, "never")
    assert slow.cancel() is True
    with pytest.raises(ch.Cancelled):
        slow.result(timeout=10)
    quick = word_count.remote("a b c")
    assert quick.result(timeout=10) == 3
    assert quick.cancel() is False, "cancelling a finished call keeps its result"
    assert quick.result() == 3


def test_gather_and_as_completed():
    futs = [sleep_then.remote(0.02 * (5 - i), i) for i in range(5)]
    assert ch.get(futs, timeout=30) == [0, 1, 2, 3, 4]
    futs = [sleep_then.remote(0.05 * i, i) for i in (3, 1, 2)]
    order = [f.result() for f in ch.as_completed(futs, timeout=30)]
    assert sorted(order) == [1, 2, 3]


def test_put_shares_one_copy():
    big = list(range(200_000))
    ref = ch.put(big)

    @ch.remote
    def total(xs):
        return sum(xs)

    assert ch.get((total.remote(ref) for _ in range(4)), timeout=60) == [sum(big)] * 4


def test_large_arguments_and_results_round_trip():
    np = pytest.importorskip("numpy")
    a = np.arange(3_000_000, dtype=np.float64)

    @ch.remote
    def double(x):
        return x * 2

    out = double.remote(a).result(timeout=60)
    assert out.shape == a.shape and float(out[-1]) == float(a[-1] * 2)


def test_map_ordered_unordered_and_outcomes():
    assert list(word_count.map(["a", "a b", "a b c"])) == [1, 2, 3]
    vals = sorted(sleep_then.map([0.03, 0.01, 0.02], ["x", "y", "z"], ordered=False))
    assert vals == ["x", "y", "z"]

    @ch.remote
    def maybe(i):
        if i == 2:
            raise KeyError(i)
        return i

    outs = list(maybe.map(range(5)).outcomes())
    assert [o.index for o in outs] == [0, 1, 2, 3, 4]
    assert isinstance(outs[2].error, KeyError) and outs[3].value == 3
    it = maybe.map(range(5))
    got = []
    for _ in range(5):
        try:
            got.append(next(it))
        except KeyError:
            got.append("err")
    assert got == [0, 1, "err", 3, 4], "iteration continues after a caught item error"


def test_map_pulls_at_most_the_window_and_close_runs_the_source_finally():
    pulled = []
    finished = []

    def source():
        try:
            for i in range(10_000):
                pulled.append(i)
                yield i
        finally:
            finished.append(True)

    with word_count.options(max_in_flight=8).map(str(i) for i in source()) as results:
        first = next(results)
        assert first == 1
        assert len(pulled) <= 9, f"pulled {len(pulled)} items with a window of 8"
    # Leaving the block closes the stream: no more pulls; the source's finally ran.
    time.sleep(0.05)
    assert finished == [True]
    assert len(pulled) <= 9


def test_chained_stages_pass_handles_not_values():
    stage1 = word_count.map(["a", "a b", "a b c"])
    stage2 = stage1.map(add.partial(10))
    assert list(stage2) == [11, 12, 13]


def test_context_inside_a_task():
    @ch.remote
    def info():
        c = ch.context()
        return (c.attempt, len(c.task_id), c.cpus)

    attempt, task_len, cpus = info.remote().result(timeout=30)
    assert attempt == 1 and task_len == 32 and cpus == 1.0


def test_nested_submissions_and_blocking_waits_inside_tasks():
    @ch.remote
    def fan(n):
        parts = [word_count.remote("x " * i) for i in range(n)]
        return sum(p.result() for p in parts)

    # More nested waiters than cores must not deadlock: a blocked task lends its CPU.
    outs = ch.get((fan.remote(4) for _ in range(20)), timeout=120)
    assert outs == [6] * 20


def test_await_inside_asyncio():
    async def main():
        return await add.remote(2, 3)

    assert asyncio.run(main()) == 5


def test_closure_that_cannot_be_serialized_names_the_variable():
    import threading

    lock = threading.Lock()

    @ch.remote
    def uses_lock():
        with lock:
            return 1

    with pytest.raises(ch.SerializationError, match="closure lock"):
        uses_lock.remote()


def test_script_without_main_guard_prints_once():
    r = run_script(
        """
        import chimaera as ch
        print("top level ran")

        @ch.remote
        def sq(x):
            return x * x

        print(sum(sq.map(range(20))))
        """
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.count("top level ran") == 1
    assert "2470" in r.stdout


def test_task_prints_reach_the_driver():
    r = run_script(
        """
        import chimaera as ch

        @ch.remote
        def talk():
            print("hello from a task")
            return 1

        talk.remote().result()
        """
    )
    assert r.returncode == 0, r.stderr
    assert "hello from a task" in r.stdout


def test_thread_caps_follow_the_grant():
    @ch.remote(cpus=2)
    def threads():
        return os.environ.get("OMP_NUM_THREADS"), os.environ.get("OPENBLAS_NUM_THREADS")

    assert threads.remote().result(timeout=30) == ("2", "2")


def test_explain_reports_attempts_with_measurement_scope():
    @ch.remote
    def busy():
        t = time.process_time()
        while time.process_time() - t < 0.3:
            pass
        return 1

    f = busy.remote()
    f.result(timeout=30)
    info = f.explain()
    assert info["state"] == "Done"
    a = info["attempts"][0]
    cpu, scope, _method = a["measures"]["cpu_seconds"]
    assert scope == "process" and 0.25 < cpu < 0.6
    peak_scope = a["measures"]["rss_peak_bytes"][1]
    assert peak_scope == "process_lifetime", "a lifetime peak must not be reported as the call's own"


@ch.remote
def square_of(x: int) -> int:
    return x * x


@ch.remote
def fails_with_key_error(_: int) -> int:
    raise KeyError("upstream")


@ch.remote
def received(xs):
    """What the call got: values, or the names of the types of what it got."""
    if isinstance(xs, dict):
        return {k: type(v).__name__ for k, v in xs.items()}
    return [type(x).__name__ for x in xs]


@ch.remote
def total_of(xs) -> int:
    return sum(xs.values()) if isinstance(xs, dict) else sum(xs)


def test_futures_inside_a_list_tuple_or_dict_arrive_as_values():
    fs = [square_of.remote(i) for i in range(5)]
    assert total_of.remote(fs).result(timeout=60) == 30
    assert total_of.remote(tuple(fs)).result(timeout=60) == 30
    assert total_of.remote({"a": fs[1], "b": fs[2], "c": 10}).result(timeout=60) == 15
    # Order is kept, and plain items pass through untouched.
    assert received.remote([fs[0], 7, "x"]).result(timeout=60) == ["int", "int", "str"]


def test_a_failed_future_inside_a_list_fails_the_call():
    with pytest.raises(KeyError, match="upstream"):
        total_of.remote([square_of.remote(1), fails_with_key_error.remote(0)]).result(timeout=60)


def test_handles_keeps_futures_unresolved_and_deeper_nesting_is_left_alone():
    f = square_of.remote(3)
    assert received.remote(ch.handles([f])).result(timeout=60) == ["Future"]
    assert received.remote({"k": ch.handles(f)}).result(timeout=60) == {"k": "Future"}
    assert received.remote([[f]]).result(timeout=60) == ["list"], "only one level is resolved"
