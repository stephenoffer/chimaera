"""A remote driver's own modules reach the workers (the project archive)."""

from __future__ import annotations

import os
import tempfile

import pytest

from chimaera import _project
from test_cluster import Cluster


def test_a_remote_drivers_project_modules_import_on_the_peers():
    cluster = Cluster(1, clients=True)
    try:
        out = cluster.remote(
            0,
            """
import os, sys
here = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(here, "projlib.py"), "w") as f:
    f.write("def triple(x):\\n    return 3 * x\\n")
import projlib
import chimaera as ch

ch.init()  # packs and uploads this directory
os.rename(here, here + "-gone")  # the workers can no longer read it from here

@ch.remote
def use(x):
    return projlib.triple(x)  # projlib is imported by reference on the worker

print("RESULT", sum(use.map(range(10))))
""",
        )
        assert out.returncode == 0, out.stderr[-3000:]
        assert "RESULT 135" in out.stdout
    finally:
        cluster.close()


def test_the_archive_is_reproducible_skips_caches_and_refuses_huge_projects(monkeypatch):
    root = tempfile.mkdtemp(prefix="chp")
    os.makedirs(os.path.join(root, "pkg", "__pycache__"))
    os.makedirs(os.path.join(root, ".git"))
    for name, text in [("pkg/__init__.py", ""), ("pkg/mod.py", "X = 1\n"), ("pkg/__pycache__/mod.cpython.pyc", "junk"), (".git/HEAD", "ref")]:
        with open(os.path.join(root, name), "w") as f:
            f.write(text)
    digest, data = _project.pack([root])
    again = _project.pack([root])
    assert again == (digest, data), "same files, same bytes"
    import io
    import tarfile

    names = tarfile.open(fileobj=io.BytesIO(data)).getnames()
    assert sorted(names) == ["r0/pkg/__init__.py", "r0/pkg/mod.py"]
    assert _project.pack([os.path.dirname(os.__file__)]) is None, "the standard library is not a project"
    monkeypatch.setenv("CHIMAERA_PROJECT_MAX_BYTES", "3")
    with pytest.raises(ValueError, match=r"largest: .*mod\.py"):
        _project.pack([root])


def test_a_projects_uv_lock_is_installed_on_the_peers():
    import shutil

    if shutil.which("uv") is None:
        pytest.skip("needs uv")
    cluster = Cluster(1, clients=True)
    try:
        out = cluster.remote(
            0,
            """
import os, subprocess, sys
here = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(here, "pyproject.toml"), "w") as f:
    f.write('[project]\\nname = "demo"\\nversion = "0"\\nrequires-python = ">=3.11"\\ndependencies = ["toolz==1.0.0"]\\n')
lock = subprocess.run(["uv", "lock"], cwd=here, capture_output=True, text=True)
if lock.returncode != 0:
    print("NO-LOCK", lock.stderr[-500:])
    sys.exit(0)
import chimaera as ch

ch.init()

@ch.remote
def toolz_version(_):
    import toolz  # not installed in the peer's own Python
    return toolz.__version__

print("VERSIONS", sorted(set(toolz_version.map(range(4)))))
""",
        )
        if "NO-LOCK" in out.stdout:
            pytest.skip(f"uv lock needs PyPI: {out.stdout[-300:]}")
        assert out.returncode == 0, out.stderr[-3000:]
        assert "VERSIONS ['1.0.0']" in out.stdout
    finally:
        cluster.close()


def test_an_archive_that_would_write_outside_its_directory_is_refused(tmp_path):
    import io
    import tarfile

    def archive(add):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            add(tar)
        buf.seek(0)
        return tarfile.open(fileobj=buf)

    def plain(name):
        def add(tar):
            info = tarfile.TarInfo(name)
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))

        return add

    def link(tar):
        info = tarfile.TarInfo("r0/escape")
        info.type, info.linkname = tarfile.SYMTYPE, "/etc/passwd"
        tar.addfile(info)

    def device(tar):
        info = tarfile.TarInfo("r0/dev")
        info.type = tarfile.CHRTYPE
        tar.addfile(info)

    for bad in (plain("../outside.py"), plain("/abs.py"), plain("r0/../../x.py"), link, device):
        dest = tmp_path / "dest"
        dest.mkdir(exist_ok=True)
        with pytest.raises(ValueError, match="only plain files"):
            _project._extract(archive(bad), str(dest))
        assert list(dest.iterdir()) == []
    good = tmp_path / "good"
    good.mkdir()
    _project._extract(archive(plain("r0/pkg/mod.py")), str(good))
    assert (good / "r0" / "pkg" / "mod.py").read_text() == "x"
    assert not (tmp_path / "outside.py").exists()


def test_pythons_without_tar_filters_unpack_and_refuse_the_same(tmp_path, monkeypatch):
    import io
    import tarfile

    monkeypatch.setattr(_project, "_TAR_FILTERS", False)  # as on Python 3.11.2: no filter= argument
    digest, data = _project.pack([str(tmp_path / "missing")]) or (None, None)
    assert digest is None
    src = tmp_path / "proj"
    (src / "pkg").mkdir(parents=True)
    (src / "pkg" / "mod.py").write_text("X = 1\n")
    _, data = _project.pack([str(src)])
    dest = tmp_path / "dest"
    dest.mkdir()
    _project._extract(tarfile.open(fileobj=io.BytesIO(data)), str(dest))
    assert (dest / "r0" / "pkg" / "mod.py").read_text() == "X = 1\n"
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        info = tarfile.TarInfo("../evil.py")
        info.size = 1
        tar.addfile(info, io.BytesIO(b"x"))
    buf.seek(0)
    with pytest.raises(ValueError, match="only plain files"):
        _project._extract(tarfile.open(fileobj=buf), str(dest))
    assert not (tmp_path / "evil.py").exists()
