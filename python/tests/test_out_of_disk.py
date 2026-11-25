"""A full disk is a typed error, ch.errors.OutOfDisk, which is still an OSError with ENOSPC."""

import errno

import pytest

import chimaera as ch
from chimaera import _serde
from conftest import run_script


def test_storing_a_value_on_a_full_disk_raises_out_of_disk(tmp_path, monkeypatch):
    def full_disk(path, parts):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(_serde._native, "write_object", full_disk, raising=False)
    (tmp_path / "run").mkdir()
    with pytest.raises(ch.errors.OutOfDisk, match=r"no space left on the disk holding .*run \(\d+ MiB free\)") as info:
        _serde.write_file(str(tmp_path / "run" / "object"), [b"x" * (3 << 20)])
    assert isinstance(info.value, OSError) and info.value.errno == errno.ENOSPC


def test_other_write_errors_keep_their_own_type(tmp_path, monkeypatch):
    def denied(path, parts):
        raise PermissionError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(_serde._native, "write_object", denied, raising=False)
    with pytest.raises(PermissionError) as info:
        _serde.write_file(str(tmp_path / "object"), [b"x"])
    assert not isinstance(info.value, ch.errors.OutOfDisk)


def test_a_worker_whose_disk_fills_while_storing_its_result_raises_out_of_disk_here():
    # Its own script: the fake full disk stays in that run's executor.
    r = run_script(
        """
        import errno
        import chimaera as ch

        @ch.remote
        def big_result_on_a_full_disk():
            from chimaera import _serde

            def full_disk(path, parts):
                raise OSError(errno.ENOSPC, "No space left on device")

            _serde._native.write_object = full_disk
            return b"x" * (8 << 20)

        try:
            big_result_on_a_full_disk.remote().result(timeout=60)
        except ch.errors.OutOfDisk as e:
            print("OUT_OF_DISK", e.errno, isinstance(e, OSError))
        """
    )
    assert "OUT_OF_DISK 28 True" in r.stdout, (r.stdout[-2000:], r.stderr[-2000:])
