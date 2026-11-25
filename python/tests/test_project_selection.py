"""Which files a remote run's project ships, and how a worker lays them out:
an ignored directory ships nothing unless named, and a worker imports a
module from the same root the driver does."""

from __future__ import annotations

import io
import os
import shutil
import subprocess
import sys
import tarfile
import types
import warnings
from importlib.machinery import PathFinder

import pytest

from chimaera import _project, _serde

needs_git = pytest.mark.skipif(shutil.which("git") is None, reason="needs git")


def names(packed: tuple[bytes, bytes] | None) -> list[str]:
    assert packed is not None
    return sorted(tarfile.open(fileobj=io.BytesIO(packed[1])).getnames())


def write(root, files: dict[str, str]) -> None:
    for name, text in files.items():
        path = os.path.join(root, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(text)


@pytest.fixture
def ignored(tmp_path, monkeypatch):
    """A git work tree whose .gitignore ignores scratch/, which holds a script and a private file."""
    monkeypatch.delenv("CHIMAERA_PROJECT_INCLUDE", raising=False)
    monkeypatch.delenv("CHIMAERA_PROJECT_GITIGNORE", raising=False)
    monkeypatch.setattr(_project, "_noted", set(), raising=False)
    write(tmp_path, {".gitignore": "scratch/\n", "scratch/demo.py": "X = 1\n", "scratch/private.key": "sentinel\n"})
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    return tmp_path / "scratch"


@needs_git
def test_a_directory_git_ignores_as_a_whole_ships_nothing(ignored):
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # the program imported nothing from it: no warning
        packed = _project.pack([str(ignored)])
    assert names(packed) == [] and b"sentinel" not in packed[1]


@needs_git
def test_an_ignored_directory_the_program_imported_from_is_named_in_a_warning(ignored, monkeypatch):
    module = types.ModuleType("scratch_helpers")
    module.__file__ = str(ignored / "scratch_helpers.py")
    monkeypatch.setitem(sys.modules, "scratch_helpers", module)
    with pytest.warns(UserWarning, match=r"CH126: git ignores all of .*scratch_helpers.*CHIMAERA_PROJECT_INCLUDE"):
        assert names(_project.pack([str(ignored)])) == []
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _project.pack([str(ignored)])  # said once per process


@needs_git
def test_a_file_named_in_the_include_setting_ships_alone(ignored, monkeypatch):
    monkeypatch.setenv("CHIMAERA_PROJECT_INCLUDE", str(ignored / "demo.py"))
    assert names(_project.pack([str(ignored)])) == ["r0/demo.py"]
    monkeypatch.chdir(ignored)  # relative to the working directory, and a directory ships what is under it
    monkeypatch.setenv("CHIMAERA_PROJECT_INCLUDE", ".")
    assert names(_project.pack([str(ignored)])) == ["r0/demo.py", "r0/private.key"]


@needs_git
def test_chimaera_submit_ships_its_script_even_where_git_ignores_it(ignored, monkeypatch):
    from chimaera import _submit

    class Stop(Exception):
        pass

    def init(**kwargs):
        raise Stop(os.environ.get("CHIMAERA_PROJECT_INCLUDE"))

    monkeypatch.setenv("CHIMAERA_PROJECT_INCLUDE", "")
    monkeypatch.setattr(_submit.ch, "init", init)
    monkeypatch.chdir(ignored)
    with pytest.raises(Stop, match="^demo.py$"):
        _submit.main(["--", "python", "-u", "demo.py", "private.key"])
    assert names(_project.pack([str(ignored)])) == ["r0/demo.py"], "the script, not the files it is given"


@needs_git
def test_an_included_file_in_a_skipped_directory_ships_and_a_stray_one_is_reported(tmp_path, monkeypatch):
    write(tmp_path, {"app.py": "", "build/ext.so": "binary", "build/other.o": ""})
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    monkeypatch.setenv("CHIMAERA_PROJECT_INCLUDE", os.pathsep.join([str(tmp_path / "build" / "ext.so"), "/nowhere"]))
    with pytest.warns(UserWarning, match="names /nowhere, which does not exist"):
        assert names(_project.pack([str(tmp_path)])) == ["r0/app.py", "r0/build/ext.so"]
    monkeypatch.setenv("CHIMAERA_PROJECT_INCLUDE", os.path.dirname(str(tmp_path)))
    with pytest.warns(UserWarning, match="in none of the project's directories"):
        _project.pack([str(tmp_path)])


@needs_git
def test_nested_ignore_rules_apply_and_a_directory_outside_git_ships_everything(tmp_path, monkeypatch):
    monkeypatch.delenv("CHIMAERA_PROJECT_INCLUDE", raising=False)
    repo, plain = tmp_path / "repo", tmp_path / "plain"
    write(repo, {".gitignore": "*.ckpt\n", "pkg/.gitignore": "*.secret\n", "pkg/a.py": "", "pkg/b.secret": ""})
    write(repo, {"pkg/w.ckpt": ""})
    write(plain, {"run.py": "", "w.ckpt": ""})
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    assert names(_project.pack([str(repo)])) == ["r0/.gitignore", "r0/pkg/.gitignore", "r0/pkg/a.py"]
    assert names(_project.pack([str(plain)])) == ["r0/run.py", "r0/w.ckpt"], "no git: every file"


@needs_git
@pytest.mark.parametrize("failure", [subprocess.TimeoutExpired(["git"], 30), FileNotFoundError("git")])
def test_git_failing_in_a_work_tree_ships_nothing_rather_than_everything(ignored, monkeypatch, failure):
    def broken(*args, **kwargs):
        raise failure

    fake = types.SimpleNamespace(run=broken, TimeoutExpired=subprocess.TimeoutExpired)
    monkeypatch.setattr(_project, "subprocess", fake)
    with pytest.raises(ValueError, match="git could not list.*CHIMAERA_PROJECT_GITIGNORE=0"):
        _project.pack([str(ignored)])
    # Asked for every file, it does not ask git.
    monkeypatch.setenv("CHIMAERA_PROJECT_GITIGNORE", "0")
    assert names(_project.pack([str(ignored)])) == ["r0/demo.py", "r0/private.key"]


@needs_git
def test_a_repository_named_by_git_dir_applies_its_ignore_rules(tmp_path, monkeypatch):
    repo, tree = tmp_path / "repo.git", tmp_path / "tree"
    write(tree, {".gitignore": "*.key\n", "app.py": "", "private.key": ""})
    subprocess.run(["git", "init", "-q", "--bare", str(repo)], check=True)
    monkeypatch.setenv("GIT_DIR", str(repo))
    monkeypatch.setenv("GIT_WORK_TREE", str(tree))
    assert names(_project.pack([str(tree)])) == ["r0/.gitignore", "r0/app.py"]


@needs_git
def test_a_cache_key_still_covers_the_sources_of_an_ignored_directory(ignored, monkeypatch):
    # What ships and what decides a cached result differ: an ignored helper still changes the answer.
    assert [name for _, name, _ in _project._files(str(ignored), shipping=False)] == ["demo.py", "private.key"]

    def broken(*args, **kwargs):
        raise FileNotFoundError("git")

    fake = types.SimpleNamespace(run=broken, TimeoutExpired=subprocess.TimeoutExpired)
    monkeypatch.setattr(_project, "subprocess", fake)
    assert len(_project._files(str(ignored), shipping=False)) == 2, "git failing: every file, and no error"


def unpack(tmp_path, monkeypatch, blobs: dict[bytes, bytes], digest: bytes) -> list[str]:
    """Store `blobs` as a run's, unpack project `digest` as a worker does, and return the
    directories it put on sys.path."""
    objects, projects = str(tmp_path / "peer" / "objects"), str(tmp_path / "peer" / "projects")
    run = b"\x07" * 16
    for d, data in blobs.items():
        path = _serde.object_path(objects, run, d)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(_project, "_installed", {})
    monkeypatch.setenv(_project.ENV, digest.hex())
    before = len(sys.path)
    _project.ensure(objects, run, projects)
    return sys.path[: len(sys.path) - before]  # prepended


def eleven_roots(tmp_path) -> list[str]:
    roots = [str(tmp_path / f"src{i:02d}") for i in range(11)]
    for r in roots:
        os.makedirs(r)
    write(roots[2], {"shared_mod.py": "WHERE = 2\n"})
    write(roots[10], {"shared_mod.py": "WHERE = 10\n"})
    return roots


def test_a_worker_imports_a_module_from_the_root_the_driver_does(tmp_path, monkeypatch):
    roots = eleven_roots(tmp_path)
    driver = PathFinder.find_spec("shared_mod", roots)
    assert driver is not None and driver.origin == os.path.join(roots[2], "shared_mod.py")
    digest, archive = _project.pack(roots)
    added = unpack(tmp_path, monkeypatch, {digest: archive}, digest)
    assert [os.path.basename(d) for d in added] == ["r2", "r10"]
    worker = PathFinder.find_spec("shared_mod", added)
    assert worker is not None and open(worker.origin).read() == "WHERE = 2\n"
    # Unpacked already (another worker of the peer): the same order.
    again = unpack(tmp_path, monkeypatch, {}, digest)
    assert again == added


def test_the_first_locked_root_in_the_drivers_order_installs(tmp_path, monkeypatch):
    roots = eleven_roots(tmp_path)
    write(roots[2], {"uv.lock": "two"})
    write(roots[10], {"uv.lock": "ten"})
    used: list[str] = []
    monkeypatch.setattr(_project, "_use_locked_env", lambda project, envs: used.append(project))
    digest, archive = _project.pack(roots)
    unpack(tmp_path, monkeypatch, {digest: archive}, digest)
    assert [os.path.basename(u) for u in used] == ["r2"]


def test_an_archive_keeps_its_roots_in_order_with_gaps(tmp_path, monkeypatch):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, text in [("r0/a.py", b""), ("r10/shared_mod.py", b"WHERE = 10\n"), ("r2/shared_mod.py", b"2")]:
            info = tarfile.TarInfo(name)
            info.size = len(text)
            tar.addfile(info, io.BytesIO(text))
    archive = buf.getvalue()
    digest = _project.blake3_digest(archive)[:16]
    added = unpack(tmp_path, monkeypatch, {digest: archive}, digest)
    assert [os.path.basename(d) for d in added] == ["r0", "r2", "r10"]
