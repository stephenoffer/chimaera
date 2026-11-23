"""@ch.remote(cache=...): a result reused for the same code, arguments and environment."""

import os
import subprocess
import sys
import time
import types
import uuid

import pytest

import chimaera as ch
from chimaera import _cache
from chimaera.errors import OptionsError
from conftest import BIN, HOME, REPO

RUNS = ch.coord.Store("test-cache-runs")


def runs(tag: str) -> int:
    return RUNS.counter(tag).value()


def new_tag() -> str:
    return uuid.uuid4().hex


@ch.remote(cache=True)
def square(tag, x):
    RUNS.counter(tag).add()
    return x * x


@ch.remote(cache=True)
def size(tag, value):
    RUNS.counter(tag).add()
    return len(value)


def test_a_second_identical_call_is_a_hit_and_runs_nothing():
    tag = new_tag()
    first = square.remote(tag, 7)
    assert first.result() == 49
    assert first.explain()["cache"] == "miss"
    second = square.remote(tag, 7)
    assert second.result() == 49
    assert second.explain()["cache"] == "hit"
    assert runs(tag) == 1


def test_arguments_by_name_or_position_are_one_entry_and_other_values_are_not():
    tag = new_tag()
    assert square.remote(tag, 3).result() == 9
    assert square.remote(tag, x=3).result() == 9
    assert square.remote(tag=tag, x=3).result() == 9
    assert runs(tag) == 1
    assert square.remote(tag, 4).result() == 16
    assert square.remote(tag, 3.0).result() == 9.0  # a float is not the int it equals
    assert runs(tag) == 3


def test_dicts_are_keyed_by_content_and_tuples_and_lists_stay_apart():
    tag = new_tag()
    assert size.remote(tag, {"a": 1, "b": [1, 2]}).result() == 2
    assert size.remote(tag, {"b": [1, 2], "a": 1}).result() == 2
    assert runs(tag) == 1
    size.remote(tag, (1, 2)).result()
    size.remote(tag, [1, 2]).result()
    assert runs(tag) == 3


@ch.remote(cache=True)
def count_items(tag, value):
    RUNS.counter(tag).add()
    return len(value)


def test_an_argument_without_a_canonical_form_runs_uncached_and_says_why_once():
    tag = new_tag()
    with pytest.warns(UserWarning, match="not cached, because argument 'value' holds a set") as caught:
        futures = [count_items.remote(tag, {1, 2}) for _ in range(3)]
        assert [f.result() for f in futures] == [2, 2, 2]
    assert sum("not cached" in str(w.message) for w in caught) == 1
    assert runs(tag) == 3
    assert futures[-1].explain()["cache"].startswith("skipped: argument 'value' holds a set")


@ch.remote(cache=True)
def flaky(tag):
    n = RUNS.counter(tag).add()
    if n == 1:
        raise ValueError("the first run fails")
    return n


def test_an_error_is_not_cached():
    tag = new_tag()
    with pytest.raises(ValueError, match="the first run fails"):
        flaky.remote(tag).result()
    assert flaky.remote(tag).result() == 2
    assert flaky.remote(tag).result() == 2
    assert runs(tag) == 2


@ch.remote(cache="1s")
def stamp(tag):
    RUNS.counter(tag).add()
    return tag


def test_an_entry_expires_after_its_time_to_live():
    tag = new_tag()
    stamp.remote(tag).result()
    stamp.remote(tag).result()
    assert runs(tag) == 1
    time.sleep(1.5)
    stamp.remote(tag).result()
    assert runs(tag) == 2


@ch.remote(cache=True)
def large(tag):
    RUNS.counter(tag).add()
    return b"x" * (2 << 20)


def test_a_result_over_the_store_limit_runs_every_time():
    tag = new_tag()
    assert len(large.remote(tag).result()) == 2 << 20
    assert len(large.remote(tag).result()) == 2 << 20
    assert runs(tag) == 2


@ch.remote(cache=True)
def load(tag, n):
    RUNS.counter(tag + "load").add()
    return list(range(n))


@ch.remote(cache=True)
def total(tag, xs):
    RUNS.counter(tag + "total").add()
    return sum(xs)


@ch.remote
def plain_list():
    return [1, 2, 3]


def test_a_pipeline_of_cached_calls_hits_from_end_to_end():
    tag = new_tag()
    for _ in range(2):
        out = total.remote(tag, load.remote(tag, 10))
        assert out.result() == 45
    assert out.explain()["cache"] == "hit"
    assert (runs(tag + "load"), runs(tag + "total")) == (1, 1)


def test_a_future_of_an_uncached_call_runs_uncached():
    tag = new_tag()
    with pytest.warns(UserWarning, match="holds a future of a call that is not cached"):
        outs = [total.remote(tag, plain_list.remote()) for _ in range(2)]
    assert [o.result() for o in outs] == [6, 6]
    assert runs(tag + "total") == 2


def test_a_small_put_value_is_keyed_by_its_bytes():
    tag = new_tag()
    assert total.remote(tag, ch.put([1, 2, 3])).result() == 6
    assert total.remote(tag, ch.put([1, 2, 3])).result() == 6
    assert total.remote(tag, ch.put([1, 2, 4])).result() == 7
    assert runs(tag + "total") == 2


@ch.remote(cache=True, returns=2)
def pair(tag, x):
    RUNS.counter(tag).add()
    return x, -x


@ch.remote(cache=True, returns=3)
def short(tag):
    RUNS.counter(tag).add()
    return 1, 2


def test_a_hit_splits_and_passes_on_like_the_miss():
    tag = new_tag()
    for _ in range(2):
        a, b = pair.remote(tag, 5).split()
        assert (a.result(), b.result()) == (5, -5)
        assert total.remote(tag, [a, b]).result() == 0
    assert runs(tag) == 1
    assert runs(tag + "total") == 1


def test_a_hit_with_the_wrong_number_of_values_fails_like_the_miss():
    tag = new_tag()
    for _ in range(2):
        with pytest.raises(ValueError, match="returns=3"):
            short.remote(tag).split()[0].result()
    assert runs(tag) == 1


@ch.remote(cache=True)
async def async_square(tag, x):
    RUNS.counter(tag).add()
    return x * x


def test_an_async_function_is_cached():
    tag = new_tag()
    assert [async_square.remote(tag, 6).result() for _ in range(2)] == [36, 36]
    assert runs(tag) == 1


def test_a_map_reads_the_environment_once_and_keeps_order_across_hits_and_misses(monkeypatch):
    _cache._env.cache_clear()
    scans = []
    real = _cache._project.roots
    monkeypatch.setattr(_cache._project, "roots", lambda paths: scans.append(1) or real(paths))
    tag = new_tag()
    assert list(square.map([tag] * 1000, range(1000))) == [i * i for i in range(1000)]
    assert runs(tag) == 1000
    # The first 1,000 items hit, the last 100 run; results stay in item order.
    assert list(square.map([tag] * 1100, range(1100))) == [i * i for i in range(1100)]
    assert runs(tag) == 1100
    assert len(scans) == 1


def test_a_lookup_that_fails_runs_the_call_and_says_so_once(monkeypatch):
    def unreachable(self, key, default=None):
        raise ConnectionError("quorum lost")

    tag = new_tag()
    monkeypatch.setattr(_cache.Store, "get", unreachable)
    with pytest.warns(UserWarning, match="could not be read, so the call runs: ConnectionError") as caught:
        assert [square.remote(tag, 2).result() for _ in range(2)] == [4, 4]
    assert sum("could not be read" in str(w.message) for w in caught) == 1
    assert runs(tag) == 2


def gen():
    yield 1


@ch.remote
class Box:
    @ch.method(cache=True)
    def get(self):
        return 1


def test_cache_is_refused_where_reusing_a_result_would_change_behavior():
    with pytest.raises(OptionsError, match="effects='none'"):
        ch.remote(cache=True, effects="idempotent")(lambda: 1).remote()
    with pytest.raises(OptionsError, match="deterministic=False"):
        ch.remote(cache=True, deterministic=False)(lambda: 1).remote()
    with pytest.raises(OptionsError, match="generator"):
        ch.remote(cache=True)(gen).remote()
    with pytest.raises(OptionsError, match="not on actors"):
        ch.remote(cache=True)(type("Counter", (), {}))
    with pytest.raises(OptionsError, match="not on scopes"):
        ch.defaults(cache=True)
    with pytest.raises(OptionsError, match="cache= is for functions"):
        Box.remote().get.remote()
    with pytest.raises(OptionsError, match="time to live"):
        ch.remote(cache="0s")(lambda: 1)


def test_the_environment_digest_follows_project_sources_but_not_the_script_or_notebooks(tmp_path, monkeypatch):
    (tmp_path / "helper.py").write_text("X = 1\n")
    (tmp_path / "main.py").write_text("print(1)\n")
    (tmp_path / "notes.ipynb").write_text("{}")
    main = types.ModuleType("__main__")
    main.__file__ = str(tmp_path / "main.py")
    monkeypatch.setattr(_cache._project, "roots", lambda paths: [str(tmp_path)])  # this project alone
    monkeypatch.setitem(sys.modules, "__main__", main)

    def env() -> bytes:
        _cache._env.cache_clear()
        return _cache._env()

    try:
        base = env()
        (tmp_path / "main.py").write_text("print(2)\n")
        (tmp_path / "notes.ipynb").write_text('{"cells": []}')
        assert env() == base
        (tmp_path / "helper.py").write_text("X = 2\n")
        assert env() != base
    finally:
        _cache._env.cache_clear()


SCRIPT = """
import chimaera as ch

@ch.remote(cache=True)
def work(tag):
    ch.coord.Store("test-cache-runs").counter(tag).add()
    return tag.upper()

fut = work.remote(TAG)
print(fut.result(), fut.explain()["cache"])
"""


def test_rerunning_a_script_hits_and_editing_the_function_does_not(tmp_path):
    tag = new_tag()
    path = tmp_path / "pipeline.py"
    env = dict(os.environ, CHIMAERA_HOME=HOME, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))

    def run(source: str) -> list[str]:
        path.write_text(source)
        p = subprocess.run([sys.executable, str(path)], capture_output=True, text=True, timeout=120, env=env, cwd=tmp_path)
        assert p.returncode == 0, p.stderr
        return p.stdout.split()[:2]

    script = SCRIPT.replace("TAG", repr(tag))
    assert run(script) == [tag.upper(), "miss"]
    assert run(script) == [tag.upper(), "hit"]
    assert run(script + "print('an unrelated line')\n") == [tag.upper(), "hit"]
    assert run(script.replace("tag.upper()", "tag.upper() + ''")) == [tag.upper(), "miss"]
    assert runs(tag) == 2
