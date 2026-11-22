#!/usr/bin/env bash
# Build the peer and the Python extension inside the dev container and run the
# Python suite there. Needs a writable cgroup tree for the isolated-accounting
# tests (`docker run --privileged --cgroupns=private`).
#
# Works on a copy of /src: an editable install would put a Linux extension
# module into the host's source tree and break the host's own Python.
set -euo pipefail
mkdir -p /tmp/src
tar -C /src --exclude=./target --exclude='./.venv*' --exclude=./.git -cf - . | tar -C /tmp/src -xf -
rm -f /tmp/src/python/chimaera/_native*.so
python3 -m venv /tmp/venv >/dev/null
/tmp/venv/bin/pip install -q --upgrade pip maturin cloudpickle pytest pytest-timeout numpy "mypy>=1.10,<3"
cd /tmp/src
cargo build -q -p chimaera-peer
VIRTUAL_ENV=/tmp/venv /tmp/venv/bin/maturin develop -q 2>&1 | grep -v "^warning" || true
export CHIMAERA_BIN=${CARGO_TARGET_DIR:-/target}/debug/chimaera
"$CHIMAERA_BIN" --dir /tmp/doctor doctor
exec /tmp/venv/bin/python -m pytest python/tests -q "$@"
