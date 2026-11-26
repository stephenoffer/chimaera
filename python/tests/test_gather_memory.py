"""CH117: a ch.get of many results warns before they outgrow what this
process can hold: a cgroup's limit and charge count, a size known before a
value is read is checked first, ch.put values count, and a value listed
twice counts once."""

from __future__ import annotations

import os
import warnings

import pytest

import chimaera as ch
from chimaera import _future
from chimaera._runtime import Runtime

KIB = 1 << 10


@ch.remote
def blob(n: int) -> bytes:
    return b"x" * n


def cgroups(tmp_path, monkeypatch, path: str, files: dict[str, str]) -> None:
    """Point the reader at a fake /proc/self/cgroup naming `path`, and a fake mount holding `files`."""
    proc = tmp_path / "cgroup"
    proc.write_text(f"12:memory:/legacy\n0::{path}\n")
    for name, text in files.items():
        f = tmp_path / "fs" / name
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(text)
    monkeypatch.setattr(_future, "PROC_CGROUP", str(proc))
    monkeypatch.setattr(_future, "CGROUP_ROOT", str(tmp_path / "fs"))


def test_the_cgroup_with_the_least_room_sets_the_limit(tmp_path, monkeypatch):
    gib = 1 << 30
    cgroups(
        tmp_path,
        monkeypatch,
        "/app/driver",
        {
            "app/driver/memory.max": f"{4 * gib}\n",  # its own limit is roomy
            "app/driver/memory.current": f"{gib}\n",
            "app/memory.max": f"{2 * gib}\n",  # its parent's has 512 MiB left
            "app/memory.current": f"{gib + gib // 2}\n",
        },
    )
    assert _future._cgroup_room() == (2 * gib, gib // 2)
    monkeypatch.setattr(_future, "GATHER_WARN_BYTES", None)
    machine = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 4
    cgroup = (gib // 4, "half of the 512 MiB left under this process's 2048 MiB cgroup limit")
    assert _future._gather_limit() == (cgroup if cgroup[0] < machine else (machine, "a quarter of this machine's memory"))


def test_without_a_cgroup_limit_the_machine_sets_it(tmp_path, monkeypatch):
    cgroups(tmp_path, monkeypatch, "/app", {"app/memory.max": "max\n", "app/memory.current": "1000\n"})
    assert _future._cgroup_room() is None
    monkeypatch.setattr(_future, "PROC_CGROUP", str(tmp_path / "missing"))  # macOS: no such file
    assert _future._cgroup_room() is None
    monkeypatch.setattr(_future, "GATHER_WARN_BYTES", None)
    assert _future._gather_limit()[1] == "a quarter of this machine's memory"


@pytest.fixture
def one_mib(monkeypatch):
    monkeypatch.setattr(_future, "GATHER_WARN_BYTES", 1 << 20)
    monkeypatch.setattr(_future, "_gather_warned", False)


def ch117(caught) -> list[str]:
    return [str(w.message) for w in caught if "CH117" in str(w.message)]


def test_results_that_have_finished_warn_before_any_is_read(one_mib, monkeypatch):
    futures = [blob.remote(512 * KIB) for _ in range(4)]
    done, pending = ch.wait(futures, timeout=60)
    assert not pending
    warned_at_first_read: list[bool] = []
    read = Runtime.read_value

    def reading(self, *args, **kwargs):
        if not warned_at_first_read:
            warned_at_first_read.append(_future._gather_warned)
        return read(self, *args, **kwargs)

    monkeypatch.setattr(Runtime, "read_value", reading)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        values = ch.get(futures, timeout=60)
    assert [len(v) for v in values] == [512 * KIB] * 4, "every value, in order"
    assert warned_at_first_read == [True], "the warning came before the first value was read"
    (message,) = ch117(caught)
    assert "4 results" in message and "past 1 MiB" in message and "as_completed" in message


def test_put_values_count_and_a_value_listed_twice_counts_once(one_mib, monkeypatch):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        big = ch.put(b"x" * (2 << 20))
        assert len(ch.get([big], timeout=60)[0]) == 2 << 20
    assert len(ch117(caught)) == 1, "a ch.put value is held like any other"

    monkeypatch.setattr(_future, "_gather_warned", False)
    one = blob.remote(512 * KIB)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        values = ch.get([one] * 8, timeout=60)
        assert len(values) == 8 and all(v is values[0] for v in values), "one value, held once"
        assert not ch117(caught), "512 KiB held, however often listed"
        ch.get([blob.remote(512 * KIB) for _ in range(3)], timeout=60)
    assert len(ch117(caught)) == 1, "three values of 512 KiB pass 1 MiB"
