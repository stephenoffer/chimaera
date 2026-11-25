#!/usr/bin/env bash
# Build the Linux wheel for this machine's CPU and smoke-test it on every
# supported CPython, inside the manylinux_2_28 image (`make wheel-linux`, and
# .github/workflows/wheels.yml). Building where glibc is 2.28 keeps the
# bundled peer binary as portable as the extension module's manylinux_2_28
# tag says; auditwheel checks only the extension module. The checkout is
# /src: the build runs in a copy, and only the wheel is written back, to
# /src/dist.
set -euo pipefail
PROTOC_VERSION=29.3
export PATH="$HOME/.cargo/bin:$PATH" CARGO_TARGET_DIR=/tmp/target CARGO_INCREMENTAL=0
if ! command -v rustup >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none
fi
if ! command -v protoc >/dev/null; then
  arch=$(uname -m)
  curl -sSfLo /tmp/protoc.zip \
    "https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-${arch/aarch64/aarch_64}.zip"
  unzip -qo /tmp/protoc.zip -d /usr/local bin/protoc 'include/*'
fi
mkdir -p /tmp/src
tar -C /src --exclude=./target --exclude='./.venv*' --exclude=./.git --exclude=./dist -cf - . | tar -C /tmp/src -xf -
cd /tmp/src
rm -f python/chimaera/_native*.so
/opt/python/cp312-cp312/bin/python -m venv .venv
.venv/bin/pip install -q "maturin>=1.9,<2"
make wheel MATURIN_ARGS="--compatibility manylinux_2_28"
wheel=$(ls dist/chimaera-*manylinux_2_28*.whl)
WHEEL=$wheel deploy/dev/wheel-smoke.sh /opt/python/cp311-cp311/bin/python /opt/python/cp312-cp312/bin/python \
  /opt/python/cp313-cp313/bin/python /opt/python/cp314-cp314/bin/python
mkdir -p /src/dist
cp "$wheel" /src/dist/
