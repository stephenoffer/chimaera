#!/usr/bin/env bash
# Entry point of the one-machine comparison job (deploy/anyscale/compare.job.yaml):
# builds Chimaera from source on this node, installs Dask and joblib next to
# the image's Ray, then runs bench/compare/run.py against the node's Ray.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
for d in /mnt/user_storage /mnt/cluster_storage "$ROOT/results"; do
  if mkdir -p "$d/chimaera-compare/$STAMP" 2>/dev/null; then OUT="$d/chimaera-compare/$STAMP"; break; fi
done
echo "results -> $OUT"
if ! command -v protoc >/dev/null; then
  if command -v sudo >/dev/null; then sudo apt-get update -qq && sudo apt-get install -y -qq protobuf-compiler >/dev/null
  else apt-get update -qq && apt-get install -y -qq protobuf-compiler >/dev/null; fi
fi
if ! command -v cargo >/dev/null; then
  # rustup-init downloaded to a file and run, rather than a script piped to sh.
  curl -sSfo /tmp/rustup-init "https://static.rust-lang.org/rustup/dist/$(uname -m)-unknown-linux-gnu/rustup-init"
  chmod +x /tmp/rustup-init
  /tmp/rustup-init -y -q --profile minimal --default-toolchain none
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi
export CARGO_INCREMENTAL=0
cargo --version
time cargo build -q --release -p chimaera-peer
PY=${RAY_PYTHON:-python3}
"$PY" -m pip install -q maturin numpy "dask[distributed]" joblib
"$PY" -m maturin build -q --release -o /tmp/chimaera-wheels
"$PY" -m pip install -q --force-reinstall /tmp/chimaera-wheels/chimaera-*.whl
export CHIMAERA_BIN=${CARGO_TARGET_DIR:-$ROOT/target}/release/chimaera
"$CHIMAERA_BIN" --version
"$PY" -c 'import ray; ray.init(address="auto"); print("cpus", ray.cluster_resources().get("CPU"))'
COMMIT=${COMPARE_COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}
# COMPARE_ONLY / COMPARE_RUNTIMES: a subset of workloads or runtimes (comma lists).
"$PY" bench/compare/run.py --ray-address auto --trials "${COMPARE_TRIALS:-3}" --commit "$COMMIT" --out "$OUT" \
  ${COMPARE_ONLY:+--only "$COMPARE_ONLY"} ${COMPARE_RUNTIMES:+--runtimes "$COMPARE_RUNTIMES"}
echo "===== report ====="
cat "$OUT/report.md"
echo "===== results.json ====="
cat "$OUT/results.json"
