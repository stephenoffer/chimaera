"""An editor's view of the API: mypy, with the chimaera.mypy plugin, checks
python/tests/typing/api_types.py (its assert_type lines and expected errors)."""

from __future__ import annotations

import os
import subprocess
import sys

from conftest import REPO


def test_the_public_api_types_check_with_mypy():
    here = os.path.join(REPO, "python", "tests", "typing")
    env = dict(os.environ, PYTHONPATH=os.path.join(REPO, "python"), MYPYPATH=os.path.join(REPO, "python"))
    out = subprocess.run(
        [sys.executable, "-m", "mypy", "--config-file", "mypy.ini", "--cache-dir", os.devnull, "api_types.py"],
        cwd=here,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert out.returncode == 0, out.stdout + out.stderr
