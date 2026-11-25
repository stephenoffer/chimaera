"""The `chimaera` command: runs the peer binary installed with this package."""

from __future__ import annotations

import os
import sys


def _subcommand(args: list[str]) -> str | None:
    """The subcommand `args` name, past the global `--dir DIR`."""
    rest = iter(args)
    for a in rest:
        if a == "--dir":
            next(rest, None)
        elif not a.startswith("--dir="):
            return a
    return None


def main() -> None:
    from ._runtime import _peer_binary

    path = _peer_binary(skip=sys.argv[0])
    if _subcommand(sys.argv[1:]) == "doctor":
        # The doctor checks the Python this command runs with, not python3 on PATH.
        os.environ.setdefault("CHIMAERA_PYTHON", sys.executable)
    os.execv(path, [path, *sys.argv[1:]])
