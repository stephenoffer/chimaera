#!/usr/bin/env bash
# Entry point of the Anyscale eval job (also runnable in the dev container).
# Builds Chimaera from source on this node, then runs bench/evals for Ray and
# Chimaera and writes results.json + report.md.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
for d in /mnt/user_storage /mnt/cluster_storage "$ROOT/results"; do
  if mkdir -p "$d/chimaera-evals/$STAMP" 2>/dev/null; then OUT="$d/chimaera-evals/$STAMP"; break; fi
done
echo "results -> $OUT"

# protoc (prost codegen) and a Rust toolchain, unless present.
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
cargo --version   # installs the pinned toolchain from rust-toolchain.toml
time cargo build -q --release -p chimaera-peer

# Ray runs in this interpreter (the image's); Chimaera needs Python >= 3.11.
RAY_PYTHON=${RAY_PYTHON:-python3}
if "$RAY_PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
  CHIMAERA_PYTHON=$RAY_PYTHON
else
  command -v uv >/dev/null || "$RAY_PYTHON" -m pip install -q --user uv
  export PATH="$HOME/.local/bin:$PATH"
  uv venv -q --python 3.12 /tmp/chimaera-venv
  uv pip install -q --python /tmp/chimaera-venv/bin/python numpy
  CHIMAERA_PYTHON=/tmp/chimaera-venv/bin/python
fi
"$CHIMAERA_PYTHON" -m pip install -q maturin
"$CHIMAERA_PYTHON" -m maturin build -q --release -o /tmp/chimaera-wheels
"$CHIMAERA_PYTHON" -m pip install -q --force-reinstall /tmp/chimaera-wheels/chimaera-*.whl

export CHIMAERA_BIN=${CARGO_TARGET_DIR:-$ROOT/target}/release/chimaera CHIMAERA_HOME=/tmp/chimaera-eval-home
export RAY_PYTHON CHIMAERA_PYTHON
"$RAY_PYTHON" -c 'import ray, platform; print("ray", ray.__version__, "python", platform.python_version())'
"$CHIMAERA_BIN" --dir "$CHIMAERA_HOME" doctor
"$RAY_PYTHON" bench/evals/run_evals.py --out "$OUT"
echo "===== report ====="
cat "$OUT/report.md"
"$CHIMAERA_BIN" --dir "$CHIMAERA_HOME" stop || true
