#!/usr/bin/env bash
# What `pip install chimaera` gives a user: install a wheel ($WHEEL, default
# the newest in dist/ from `make wheel`) into a fresh venv away from the
# checkout, with no Rust build on PATH, and run the `chimaera` command, a
# task, an actor and a 64 MiB result. Each argument is a Python interpreter
# to do this with (default python3): the one abi3 wheel per OS and CPU must
# work on every CPython the package supports.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
wheel=${WHEEL:-$(ls -t "$repo"/dist/chimaera-*.whl | head -1)}
[ $# -gt 0 ] || set -- python3
work=
cleanup() {
  if [ -n "$work" ]; then
    "$work/venv/bin/chimaera" --dir "$work/home" stop >/dev/null 2>&1 || true
    rm -rf "$work"
  fi
}
trap cleanup EXIT
for python in "$@"; do
  work=$(mktemp -d)
  "$python" -m venv "$work/venv"
  "$work/venv/bin/pip" install -q --disable-pip-version-check "$wheel"
  (
    cd "$work"
    export PATH="$work/venv/bin:/usr/bin:/bin" CHIMAERA_HOME="$work/home"
    unset CHIMAERA_BIN
    chimaera --version
    # A script file, as users run them.
    cat > smoke.py <<'EOF'
import sys

import chimaera as ch

@ch.remote
def square(x):
    return x * x

@ch.remote
class Counter:
    def __init__(self):
        self.n = 0

    def add(self, k):
        self.n += k
        return self.n

@ch.remote
def filled(n):
    return b"\x07" * n

assert square.remote(7).result(timeout=120) == 49
counter = Counter.remote()
assert counter.add.remote(2).result(timeout=120) == 2
assert counter.add.remote(3).result(timeout=120) == 5
size = 64 << 20
data = filled.remote(size).result(timeout=300)
assert len(data) == size and data.count(7) == size
print(f"wheel smoke: ok on Python {sys.version.split()[0]}: a task, an actor and a {size >> 20} MiB result")
EOF
    python smoke.py
  )
  cleanup
  work=
done
