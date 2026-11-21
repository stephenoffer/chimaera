#!/usr/bin/env bash
# The gotcha traps (bench/evals/gotchas) in a memory-limited Linux container:
#
#   deploy/dev/gotchas.sh [run.py args...]
#
# Builds the peer and the extension once (no memory limit), installs Ray in its
# own venv, then runs every trap, memory traps included, in a container capped
# at GOTCHA_MEMORY (default 3g) with Docker's default /dev/shm, as a naive user
# would have it. Results land in bench/evals/gotchas/results/.
set -euo pipefail
cd "$(dirname "$0")/../.."
MEM=${GOTCHA_MEMORY:-3g}
IMAGE=chimaera-dev:latest
docker build -q -t "$IMAGE" deploy/dev >/dev/null
VOLS=(-v "$PWD":/src:ro -v chimaera-target:/target -v chimaera-cargo:/usr/local/cargo/registry)

docker run --rm "${VOLS[@]}" "$IMAGE" bash -euo pipefail -c '
  rm -rf /target/gotcha-src && mkdir -p /target/gotcha-src
  tar -C /src --exclude=./target --exclude="./.venv*" --exclude=./.git -cf - . | tar -C /target/gotcha-src -xf -
  rm -f /target/gotcha-src/python/chimaera/_native*.so
  cd /target/gotcha-src
  cargo build -q --release -p chimaera-peer
  [ -x /target/venvs/chimaera/bin/python ] || python3 -m venv /target/venvs/chimaera
  /target/venvs/chimaera/bin/pip install -q --upgrade pip maturin cloudpickle numpy
  VIRTUAL_ENV=/target/venvs/chimaera /target/venvs/chimaera/bin/maturin develop -q --release 2>&1 | grep -v "^warning" || true
  [ -x /target/venvs/ray/bin/python ] || { python3 -m venv /target/venvs/ray && /target/venvs/ray/bin/pip install -q "ray[default]" numpy; }
'

limit_bytes=$(python3 -c "import sys; u={'g':1<<30,'m':1<<20}; v='$MEM'; print(int(float(v[:-1])*u[v[-1].lower()]))")
docker run --rm --memory "$MEM" --memory-swap "$MEM" "${VOLS[@]}" \
  -e GOTCHA_IN_CONTAINER=1 -e GOTCHA_MEMORY_LIMIT="$limit_bytes" \
  -e RAY_PYTHON=/target/venvs/ray/bin/python -e CHIMAERA_PYTHON=/target/venvs/chimaera/bin/python \
  -e CHIMAERA_BIN=/target/release/chimaera -e PYTHONPATH=/target/gotcha-src/python \
  "$IMAGE" bash -c 'cd /target/gotcha-src/bench/evals/gotchas && /target/venvs/chimaera/bin/python run.py --out /target/gotcha-results "$@"' -- "$@"
# Copy the results out, then remove this script's build and venvs (about
# 1.7 GB) from the shared volume: the dev VM's disk is small.
docker run --rm -v chimaera-target:/target -v "$PWD/bench/evals/gotchas":/out "$IMAGE" \
  bash -c 'mkdir -p /out/results && cp /target/gotcha-results/* /out/results/ &&
    rm -rf /target/gotcha-src /target/release /target/venvs /target/gotcha-results'
echo "results in bench/evals/gotchas/results/"
