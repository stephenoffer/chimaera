"""``chimaera submit -- python app.py ...``: run a program on the cluster.

The program runs on a peer, as the one call of a detached run, with this
directory shipped as the run's project; this command prints the run's id and
returns. The program's output is the run's log (``chimaera logs RUN``),
``chimaera cancel RUN`` stops it, and ``chimaera runs`` lists it. It runs at
most once: if its peer is lost, the call fails rather than start it again.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

import chimaera as ch


@ch.remote(effects="external", max_attempts=1)
def _program(argv: list[str]) -> int:
    """Run `argv` here, as a driver of this peer, from the shipped project."""
    from . import _project

    dirs = next(iter(_project._installed.values()), [])  # the run's project, unpacked here
    cwd = dirs[0] if dirs else os.getcwd()
    if os.path.basename(argv[0]) in ("python", "python3"):
        argv = [sys.executable, *argv[1:]]  # this peer's Python, which has chimaera
    env = {k: v for k, v in os.environ.items() if k not in ("CHIMAERA_EXECUTOR", _project.ENV)}
    env["PYTHONPATH"] = os.pathsep.join([*dirs, env.get("PYTHONPATH", "")]).strip(os.pathsep)
    env["PYTHONUNBUFFERED"] = "1"
    env["CHIMAERA_SHIP_PROJECT"] = "1"  # its calls may run where the project is not unpacked
    # The program is a driver of this peer: CHIMAERA_PEER_SOCKET (inherited) reaches it.
    code = subprocess.run(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL).returncode
    if code != 0:
        raise RuntimeError(f"{' '.join(argv)} exited with status {code}")
    return code


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args[:1] == ["--"]:
        args = args[1:]
    if not args:
        print("usage: chimaera submit [--cluster NAME] -- python app.py [args...]", file=sys.stderr)
        return 2
    # The script (app.py) is the program: it ships even where git ignores it
    # (a scratch folder). Its other files ship as usual.
    named = args[1:] if os.path.basename(args[0]).startswith("python") else args[:1]
    script = next((a for a in named if not a.startswith("-") and os.path.isfile(a)), None)
    if script is not None:
        os.environ["CHIMAERA_PROJECT_INCLUDE"] = os.pathsep.join(
            [script, os.environ.get("CHIMAERA_PROJECT_INCLUDE", "")]
        ).strip(os.pathsep)
    run = ch.init(detach=True)
    _program.remote(args)
    # Return only once the cluster holds the call: a submission it never took dies with this process.
    deadline = time.monotonic() + 60
    while sum(ch.runs.current_run().status().values()) < 1:
        if time.monotonic() > deadline:
            print("chimaera submit: the cluster did not take the program within 60 s", file=sys.stderr)
            return 1
        time.sleep(0.05)
    print(run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
