#!/usr/bin/env bash
# Entry point of the multi-node Anyscale eval jobs. Builds Chimaera once on the
# head node, then bench/evals/multi_node.py ships the binary and wheel to every
# node (through Ray), starts one peer per node, and runs each workload against
# Chimaera and against the node's Ray cluster.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
for d in /mnt/user_storage /mnt/cluster_storage "$ROOT/results"; do
  if mkdir -p "$d/chimaera-evals/multi-$STAMP" 2>/dev/null; then OUT="$d/chimaera-evals/multi-$STAMP"; break; fi
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
"$PY" -m pip install -q maturin numpy
"$PY" -m maturin build -q --release -o /tmp/chimaera-wheels
"$PY" -m pip install -q --force-reinstall /tmp/chimaera-wheels/chimaera-*.whl
DIST=/tmp/chimaera-dist
rm -rf "$DIST" && mkdir -p "$DIST"
cp "${CARGO_TARGET_DIR:-$ROOT/target}/release/chimaera" "$DIST/chimaera"
cp /tmp/chimaera-wheels/chimaera-*.whl "$DIST/"
"$DIST/chimaera" cluster token > "$DIST/token"
"$PY" -c 'import ray; ray.init(address="auto"); print("nodes", len([n for n in ray.nodes() if n["Alive"]]), "cpus", ray.cluster_resources().get("CPU"))'
"$PY" bench/evals/multi_node.py --dist "$DIST" --out "$OUT"
