#!/usr/bin/env python3
"""Refuse a wheel whose two compiled halves are not one build.

A wheel carries the peer binary (chimaera/_bin/chimaera) and the extension
module (chimaera/_native). They must agree on the release, the wire version,
the peer level and the build id, and the release must be the wheel's own
version. `make wheel` runs this before it moves a wheel to dist/, so a wheel
with a stale or foreign peer never reaches users.

Usage: wheel-check.py WHEEL
"""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess
import sys
import tempfile
import zipfile

# What `chimaera --version` prints (crates/chimaera-peer/src/main.rs).
VERSION_LINE = re.compile(r"chimaera (\S+) \(peer level (\d+), wire (\d+), build (\S+)\)")


def problems(wheel: str) -> list[str]:
    with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(wheel) as z:
        names = z.namelist()
        binary = "chimaera/_bin/chimaera"
        native = [n for n in names if re.fullmatch(r"chimaera/_native\.[^/]*(so|pyd)", n)]
        meta = [n for n in names if re.fullmatch(r"chimaera-[^/]*\.dist-info/METADATA", n)]
        needed = [
            ("the peer binary " + binary, binary in names),
            ("one extension module chimaera/_native", len(native) == 1),
            ("one dist-info METADATA", len(meta) == 1),
        ]
        missing = [what for what, ok in needed if not ok]
        if missing:
            return [f"the wheel must hold {m}" for m in missing]
        if not (z.getinfo(binary).external_attr >> 16) & 0o111:
            return [f"{binary} is not executable in the wheel, so pip would install it unrunnable"]
        z.extractall(tmp, [binary, native[0]])
        release = re.search(r"^Version: (\S+)$", z.read(meta[0]).decode(), re.M)
        path = os.path.join(tmp, binary)
        os.chmod(path, 0o755)
        try:
            out = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=60).stdout.strip()
        except OSError as e:
            return [f"the peer binary does not run here: {e}"]
        peer = VERSION_LINE.fullmatch(out)
        if peer is None:
            return [f"the peer binary's --version printed {out!r}, not `chimaera VERSION (peer level N, wire N, build ID)`"]
        spec = importlib.util.spec_from_file_location("chimaera._native", os.path.join(tmp, native[0]))
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        ext = tuple(str(getattr(mod, k, "none")) for k in ("VERSION", "PEER_LEVEL", "WIRE_VERSION", "BUILD_ID"))
    found = []
    for what, in_peer, in_ext in zip(("release", "peer level", "wire version", "build id"), peer.groups(), ext):
        if in_peer != in_ext:
            found.append(f"{what}: peer binary {in_peer}, extension module {in_ext}")
    if release is None or release.group(1) != peer.group(1):
        found.append(f"release: wheel metadata {release and release.group(1)}, peer binary {peer.group(1)}")
    return found


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    wheel = sys.argv[1]
    found = problems(wheel)
    for p in found:
        print(f"wheel check: {os.path.basename(wheel)}: {p}", file=sys.stderr)
    if found:
        print("wheel check: refused; rebuild both halves together with `make wheel`", file=sys.stderr)
        return 1
    print(f"wheel check: {os.path.basename(wheel)}: peer binary and extension module are one build")
    return 0


if __name__ == "__main__":
    sys.exit(main())
