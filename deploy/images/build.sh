#!/usr/bin/env bash
# Build the runtime image (deploy/images/Dockerfile) from this repository:
#   deploy/images/build.sh [TAG]     (default chimaera:dev)
# The context is only the sources the build needs, never target/ or .venv/.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
tag=${1:-chimaera:dev}
cd "$repo"
COPYFILE_DISABLE=1 tar -cf - \
  --exclude '__pycache__' --exclude '*.so' --exclude 'python/tests' --exclude 'python/chimaera/_bin' \
  Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml pyproject.toml crates proto python deploy/images/Dockerfile \
  | docker build -t "$tag" -f deploy/images/Dockerfile -
