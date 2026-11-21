"""Record what real Ray prints for each corpus program (ex_*.py -> ex_*.expected).

    python python/tests/compat_corpus/record.py [--ray-python PATH] [ex_name ...]

The interpreter must have Ray installed; the default is the repo's
`.venv-bench`, which pins Ray 2.60. test_compat_ray.py replays each program
under `python -m chimaera.compat.ray` and requires identical stdout.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
# Ray forwards raylet and worker logs to the driver's stdout with a "(raylet)"
# or "(fn pid=N)" prefix; they are runtime chatter, not program output.
RAY_LOG_LINE = re.compile(r"^(\x1b\[\d+m)?\((raylet|[\w.]* ?pid=\d+[^)]*)\)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ray-python", default=str(REPO / ".venv-bench" / "bin" / "python"))
    ap.add_argument("names", nargs="*", help="program stems to record (default: all)")
    args = ap.parse_args()
    version = subprocess.run(
        [args.ray_python, "-c", "import ray; print(ray.__version__)"], capture_output=True, text=True, check=True
    ).stdout.strip()
    programs = [p for p in sorted(HERE.glob("ex_*.py")) if not args.names or p.stem in args.names]
    failed = 0
    for program in programs:
        proc = subprocess.run(
            [args.ray_python, str(program)],
            capture_output=True,
            text=True,
            timeout=300,
            cwd=tempfile.mkdtemp(prefix="rayrec"),
        )
        if proc.returncode != 0:
            failed += 1
            print(f"{program.stem}: exit {proc.returncode}\n{proc.stderr[-2000:]}", file=sys.stderr)
            continue
        lines = proc.stdout.splitlines(keepends=True)
        program.with_suffix(".expected").write_text("".join(ln for ln in lines if not RAY_LOG_LINE.match(ln)))
        print(f"{program.stem}: recorded with Ray {version}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
