"""The `chimaera` command: runs the peer binary installed with this package."""

from __future__ import annotations

import os
import sys


def main() -> None:
    from ._runtime import _peer_binary

    path = _peer_binary(skip=sys.argv[0])
    os.execv(path, [path, *sys.argv[1:]])
