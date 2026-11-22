# Sourced by the Anyscale job scripts: builds Chimaera from source on this node
# (the same steps as run_eval.sh) and exports CHIMAERA_BIN and CHIMAERA_PYTHON,
# an interpreter (>= 3.11) with the chimaera wheel installed.

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
cargo build -q --release -p chimaera-peer

# The image's interpreter if it is new enough, else a uv venv.
BASE_PYTHON=${RAY_PYTHON:-python3}
if "$BASE_PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
  CHIMAERA_PYTHON=$BASE_PYTHON
else
  command -v uv >/dev/null || "$BASE_PYTHON" -m pip install -q --user uv
  export PATH="$HOME/.local/bin:$PATH"
  uv venv -q --python 3.12 /tmp/chimaera-venv
  uv pip install -q --python /tmp/chimaera-venv/bin/python numpy
  CHIMAERA_PYTHON=/tmp/chimaera-venv/bin/python
fi
"$CHIMAERA_PYTHON" -m pip install -q maturin
"$CHIMAERA_PYTHON" -m maturin build -q --release -o /tmp/chimaera-wheels
"$CHIMAERA_PYTHON" -m pip install -q --force-reinstall /tmp/chimaera-wheels/chimaera-*.whl
export CHIMAERA_BIN=${CARGO_TARGET_DIR:-$PWD/target}/release/chimaera CHIMAERA_PYTHON
