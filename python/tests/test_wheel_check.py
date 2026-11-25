"""A wheel whose peer binary and extension module are not one build is
refused before it is published (deploy/dev/wheel-check.py, run by `make
wheel`)."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
import zipfile

import pytest

import chimaera._native as native
from conftest import REPO

CHECK = os.path.join(REPO, "deploy", "dev", "wheel-check.py")


def _wheel(where, *, release=native.VERSION, level=native.PEER_LEVEL, wire=native.WIRE_VERSION,
           build=native.BUILD_ID, metadata=native.VERSION, executable=True, stale_module=False) -> str:  # fmt: skip
    """A wheel holding this checkout's extension module and a stand-in peer
    binary that reports the given versions."""
    path = os.path.join(where, f"chimaera-{metadata}-cp311-abi3-test.whl")
    peer = f'#!/bin/sh\necho "chimaera {release} (peer level {level}, wire {wire}, build {build})"\n'
    with zipfile.ZipFile(path, "w") as z:
        binary = zipfile.ZipInfo("chimaera/_bin/chimaera")
        binary.external_attr = (stat.S_IFREG | (0o755 if executable else 0o644)) << 16
        z.writestr(binary, peer)
        z.write(native.__file__, "chimaera/" + os.path.basename(native.__file__))
        if stale_module:  # a develop build left in the source tree, packed alongside
            z.write(native.__file__, "chimaera/_native.cpython-312-darwin.so")
        z.writestr(f"chimaera-{metadata}.dist-info/METADATA", f"Metadata-Version: 2.4\nName: chimaera\nVersion: {metadata}\n")
    return path


def _check(wheel: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, CHECK, wheel], capture_output=True, text=True, timeout=120)


def test_a_wheel_built_as_one_passes(tmp_path):
    done = _check(_wheel(tmp_path))
    assert done.returncode == 0, done.stderr
    assert "one build" in done.stdout


@pytest.mark.parametrize(
    ("mismatch", "named"),
    [
        ({"build": native.BUILD_ID + "-stale"}, "build id: peer binary"),
        ({"wire": native.WIRE_VERSION + 1}, "wire version: peer binary"),
        ({"level": native.PEER_LEVEL + 1}, "peer level: peer binary"),
        ({"release": "0.0.1"}, "release: peer binary 0.0.1"),
        ({"metadata": "9.9.9"}, "release: wheel metadata 9.9.9"),
        ({"executable": False}, "not executable"),
        ({"stale_module": True}, "must hold one extension module"),
    ],
)
def test_a_mismatched_wheel_is_refused(tmp_path, mismatch, named):
    done = _check(_wheel(tmp_path, **mismatch))
    assert done.returncode == 1, done.stdout
    assert named in done.stderr, done.stderr
    assert "refused" in done.stderr
