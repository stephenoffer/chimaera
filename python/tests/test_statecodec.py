"""Durable actor state as committed: a versioned header, and migration from
an older version's state when a newer class restores it. No peer needed."""

from __future__ import annotations

import collections
import os
import pickle
import threading

import cloudpickle
import pytest

import chimaera as ch
from chimaera import _executor, _serde, _statecodec
from chimaera._native import blake3_digest


class Old:
    __chimaera_state__ = ch.durable.State()

    def __init__(self):
        self.entries = [3, 4]


def to_v2(state, version):
    assert version == 1
    return {"items": state["entries"], "total": sum(state["entries"])}


class New:
    __chimaera_state__ = ch.durable.State(version=2, migrate=to_v2)


class Plain:
    """Version 2 without migrate: the stored state is used as it is."""

    __chimaera_state__ = ch.durable.State(version=2)


def test_state_is_stored_under_its_class_version():
    blob = _statecodec.dumps(Old())
    assert blob.startswith(b"CHS1\x00\x00\x00\x01")
    assert _statecodec.loads(blob) == (1, {"entries": [3, 4]})


def test_a_newer_class_migrates_state_an_older_one_wrote():
    blob = _statecodec.dumps(Old())
    assert _statecodec.restore(New, blob) == {"items": [3, 4], "total": 7}
    assert _statecodec.restore(Plain, blob) == {"entries": [3, 4]}
    assert _statecodec.restore(Old, blob) == {"entries": [3, 4]}  # its own version: as stored


def test_state_from_before_versions_reads_as_version_1():
    legacy = cloudpickle.dumps({"entries": [1]}, protocol=5)  # what commits held until now
    assert legacy[:1] == pickle.PROTO
    assert _statecodec.loads(legacy) == (1, {"entries": [1]})
    assert _statecodec.restore(New, legacy) == {"items": [1], "total": 1}


def test_state_a_newer_class_wrote_is_refused():
    obj = New.__new__(New)
    obj.__dict__.update(items=[1], total=1)
    with pytest.raises(RuntimeError, match=r"state is version 2, newer than this class's State\(version=1\)"):
        _statecodec.restore(Old, _statecodec.dumps(obj))


def test_state_options_are_checked_where_written():
    with pytest.raises(ValueError, match="version"):
        ch.durable.State(version=0)
    with pytest.raises(ValueError, match="migrate"):
        ch.durable.State(version=2, migrate="to_v2")


def test_a_restore_waits_for_its_class_code_and_reports_code_that_never_comes(tmp_path, monkeypatch):
    """The peer fetches a restore's class code if it is not there yet (a newer
    class, or a restart on another peer); the executor waits for it, and an
    error loading it fails the actor's calls rather than ending the executor."""
    ex = object.__new__(_executor.Executor)  # what restore() uses of one
    ex.code_lock, ex.code, ex.code_dir = threading.Lock(), collections.OrderedDict(), str(tmp_path)
    ex.instance, ex.restore_error = None, None
    code = _serde.dump_code(New)
    digest = blake3_digest(code)

    def fetched():
        (tmp_path / "part").write_bytes(code)
        os.replace(tmp_path / "part", tmp_path / digest.hex())

    threading.Timer(0.3, fetched).start()
    ex.restore({"fn_digest": digest, "state": _statecodec.dumps(Old())})
    assert ex.restore_error is None and ex.instance.__dict__ == {"items": [3, 4], "total": 7}
    monkeypatch.setattr(_executor, "CODE_FETCH_WAIT_S", 0.2)
    ex.restore({"fn_digest": bytes(32), "state": _statecodec.dumps(Old())})
    assert ex.instance is None and isinstance(ex.restore_error[0], FileNotFoundError)
