"""Run an unmodified Ray program on Chimaera: ``python -m chimaera.compat.ray app.py [args]``."""

from __future__ import annotations

import os
import runpy
import sys

from chimaera.compat.ray import install


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("usage: python -m chimaera.compat.ray SCRIPT [ARGS...]")
    install()
    sys.argv = sys.argv[1:]
    # As `python SCRIPT` does: the script's folder comes first, so its sibling modules import.
    sys.path[0] = os.path.dirname(os.path.abspath(sys.argv[0]))
    runpy.run_path(sys.argv[0], run_name="__main__")


if __name__ == "__main__":
    main()
