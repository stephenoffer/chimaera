"""Every test session gets its own local peer in a fresh directory."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BIN = os.environ.get("CHIMAERA_BIN") or os.path.join(REPO, "target", "debug", "chimaera")

# Short path: Unix socket paths are limited to ~104 bytes on macOS.
HOME = tempfile.mkdtemp(prefix="cht", dir="/tmp")
# Scripts run_script writes, each in a directory of its own (its working
# directory, which a detached run's executors keep using after the script
# exits), removed with the session.
SCRIPTS = tempfile.mkdtemp(prefix="chs", dir="/tmp")
os.environ["CHIMAERA_HOME"] = HOME
os.environ["CHIMAERA_BIN"] = BIN


@pytest.fixture(scope="session", autouse=True)
def peer_home():
    yield HOME
    subprocess.run([BIN, "--dir", HOME, "stop"], capture_output=True, timeout=30)
    # The peer's data (stores, raft) runs to gigabytes over a suite: keep only
    # its logs, as Cluster.close does.
    for name in os.listdir(HOME):
        if name != "logs" and os.path.isdir(os.path.join(HOME, name)):
            shutil.rmtree(os.path.join(HOME, name), ignore_errors=True)
    shutil.rmtree(SCRIPTS, ignore_errors=True)


def run_script(code: str, *, timeout: float = 120, env: dict[str, str] | None = None, home: str = HOME) -> subprocess.CompletedProcess:
    """Run `code` in a fresh interpreter against the session's peer."""
    here = tempfile.mkdtemp(dir=SCRIPTS)
    path = os.path.join(here, "script.py")
    with open(path, "w") as f:
        f.write(textwrap.dedent(code))
    e = dict(os.environ, CHIMAERA_HOME=home, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
    e.update(env or {})
    return subprocess.run([sys.executable, path], capture_output=True, text=True, timeout=timeout, env=e, cwd=here)
