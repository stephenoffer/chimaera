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


def test_a_git_projects_ignored_files_stay_home():
    import io
    import shutil
    import subprocess
    import tarfile

    if shutil.which("git") is None:
        pytest.skip("needs git")
    root = tempfile.mkdtemp(prefix="chp")
    files = {
        ".gitignore": "data/\n.env\n*.ckpt\nscratch/\n",
        "pkg/__init__.py": "",
        "pkg/tracked.py": "X = 1\n",
        "pkg/new.py": "Y = 2\n",  # untracked but not ignored: still the project's code
        "pkg/model.ckpt": "weights",
        "data/big.csv": "a,b\n",
        ".env": "SECRET=1\n",
        "scratch/run.py": "print(1)\n",
    }
    for name, text in files.items():
        os.makedirs(os.path.dirname(os.path.join(root, name)), exist_ok=True)
        with open(os.path.join(root, name), "w") as f:
            f.write(text)
    subprocess.run(["git", "init", "-q", root], check=True)
    subprocess.run(["git", "-C", root, "add", ".gitignore", "pkg/__init__.py", "pkg/tracked.py"], check=True)
    _, data = _project.pack([root])
    names = sorted(tarfile.open(fileobj=io.BytesIO(data)).getnames())
    assert names == ["r0/.gitignore", "r0/pkg/__init__.py", "r0/pkg/new.py", "r0/pkg/tracked.py"]
    # A folder git ignores as a whole is still the code being run when it is the project.
    _, data = _project.pack([os.path.join(root, "scratch")])
    assert tarfile.open(fileobj=io.BytesIO(data)).getnames() == ["r0/run.py"]
    # Asked for, ignored files ship too (an extension module built in place).
    os.environ["CHIMAERA_PROJECT_GITIGNORE"] = "0"
    try:
        _, data = _project.pack([root])
    finally:
        del os.environ["CHIMAERA_PROJECT_GITIGNORE"]
    assert "r0/pkg/model.ckpt" in tarfile.open(fileobj=io.BytesIO(data)).getnames()
    # A source checkout of Chimaera on the path is never a project: the peers use their own.
    checkout = tempfile.mkdtemp(prefix="chp")
    os.makedirs(os.path.join(checkout, "chimaera"))
    open(os.path.join(checkout, "chimaera", "_runtime.py"), "w").close()
    assert _project.roots([root, checkout]) == [os.path.realpath(root)]


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


def test_an_environment_serves_only_the_interpreter_and_machine_it_was_built_for(tmp_path, monkeypatch):
    import sys
    import sysconfig

    (tmp_path / "pyproject.toml").write_text('[project]\nname = "demo"\n')
    lock = b'[[package]]\nname = "toolz"\n'
    key = lambda: _project._env_key(str(tmp_path), lock, "uv 0.11.7")  # noqa: E731
    base = key()
    assert base == key(), "the same inputs name the same environment"
    assert _project._env_key(str(tmp_path), lock, "uv 0.12.0") != base

    (tmp_path / "pyproject.toml").write_text('[project]\nname = "demo"\n[tool.uv]\ndefault-groups = ["gpu"]\n')
    assert key() != base, "pyproject.toml decides what `uv sync --frozen` installs"
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "demo"\n')
    assert key() == base

    def changed(patch):
        with monkeypatch.context() as m:
            patch(m)
            return key()

    soabi = sysconfig.get_config_var("SOABI")
    config_var = sysconfig.get_config_var
    other_abi = {"SOABI": f"{soabi}d"}
    assert changed(lambda m: m.setattr(sysconfig, "get_config_var", lambda k: other_abi.get(k, config_var(k)))) != base
    assert changed(lambda m: m.setattr(sys, "abiflags", sys.abiflags + "t")) != base
    assert changed(lambda m: m.setattr(sysconfig, "get_platform", lambda: "linux-x86_64")) != base
    assert changed(lambda m: m.setattr(_project.os, "confstr", lambda name: "glibc 2.17")) != base
    assert changed(lambda m: m.setattr(sys, "version", sys.version + " (another build)")) != base

    # The GPU driver counts only for a lock that holds GPU packages.
    driver = tmp_path / "nvidia-version"
    monkeypatch.setattr(_project, "ACCELERATOR_FILES", (str(driver),))
    gpu_lock = b'[[package]]\nname = "nvidia-cublas-cu12"\n'
    gpu = lambda: _project._env_key(str(tmp_path), gpu_lock, "uv 0.11.7")  # noqa: E731
    driver.write_text("NVRM version: NVIDIA UNIX x86_64 Kernel Module  550.54.15\n")
    before = (key(), gpu())
    driver.write_text("NVRM version: NVIDIA UNIX x86_64 Kernel Module  570.86.10\n")
    assert key() == before[0]
    assert gpu() != before[1]


def _wheel(where, name: str, version: str) -> str:
    """A minimal pure-Python wheel of package `name` (no build step, no index)."""
    import zipfile

    path = os.path.join(where, f"{name}-{version}-py3-none-any.whl")
    info = f"{name}-{version}.dist-info"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(f"{name}/__init__.py", f"WHO = {name!r}\n")
        z.writestr(f"{info}/METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        z.writestr(f"{info}/WHEEL", "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        z.writestr(f"{info}/RECORD", "")
    return os.path.basename(path)


def test_a_lock_that_pins_chimaera_does_not_install_a_second_one(tmp_path, monkeypatch):
    import shutil
    import subprocess
    import sys

    if shutil.which("uv") is None:
        pytest.skip("needs uv")
    project = tmp_path / "project"
    project.mkdir()
    ours, dep = _wheel(project, "chimaera", "9.9.9"), _wheel(project, "tinydep", "1.0")
    (project / "pyproject.toml").write_text(
        '[project]\nname = "app"\nversion = "0"\nrequires-python = ">=3.11"\n'
        'dependencies = ["chimaera", "tinydep"]\n\n'
        f'[tool.uv.sources]\nchimaera = {{ path = "{ours}" }}\ntinydep = {{ path = "{dep}" }}\n'
    )
    monkeypatch.setenv("UV_OFFLINE", "1")  # every package is a local wheel
    done = subprocess.run(["uv", "lock", "--python", sys.executable], cwd=project, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(sys, "meta_path", list(sys.meta_path))  # the env's .pth files may add finders
    _project._use_locked_env(str(project), str(tmp_path / "envs"))
    packages = sys.path[0]
    assert packages.startswith(str(tmp_path / "envs"))
    installed = os.listdir(packages)
    assert "tinydep" in installed
    assert not [n for n in installed if n.startswith("chimaera")], installed
