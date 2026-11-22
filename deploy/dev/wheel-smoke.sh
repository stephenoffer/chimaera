#!/usr/bin/env bash
# What `pip install chimaera` gives a user: install the newest wheel in dist/
# (from `make wheel`) into a fresh venv away from the checkout, with no Rust
# build on PATH, and run the `chimaera` command and one remote call.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
wheel=$(ls -t "$repo"/dist/chimaera-*.whl | head -1)
work=$(mktemp -d)
cleanup() {
  "$work/venv/bin/chimaera" --dir "$work/home" stop >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
"${PYTHON:-python3}" -m venv "$work/venv"
"$work/venv/bin/pip" install -q "$wheel"
cd "$work"
export PATH="$work/venv/bin:/usr/bin:/bin" CHIMAERA_HOME="$work/home"
unset CHIMAERA_BIN
chimaera --version
python -c '
import chimaera as ch

@ch.remote
def square(x):
    return x * x

assert square.remote(7).result(timeout=60) == 49
print("wheel smoke: ok")
'
