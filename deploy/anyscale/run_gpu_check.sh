#!/usr/bin/env bash
# Entry point of the GPU check job (gpu-check.job.yaml): builds Chimaera, starts
# a peer on this GPU node, and runs bench/evals/gpu_check.py, which checks
# device discovery, grants and an NCCL gang against the real GPUs and copies
# the node's driver files into OUT/fixture.
set -euo pipefail
cd "$(dirname "$0")/../.."
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
for d in /mnt/user_storage /mnt/cluster_storage "$PWD/results"; do
  if mkdir -p "$d/chimaera-evals/gpu-$STAMP" 2>/dev/null; then OUT="$d/chimaera-evals/gpu-$STAMP"; break; fi
done
echo "results -> $OUT"
# shellcheck source=deploy/anyscale/build.sh
source deploy/anyscale/build.sh
"$CHIMAERA_PYTHON" -m pip install -q torch   # PyPI's Linux wheel bundles the CUDA runtime
nvidia-smi
export CHIMAERA_HOME=/tmp/chimaera-gpu-home
"$CHIMAERA_BIN" --dir "$CHIMAERA_HOME" doctor
status=0
"$CHIMAERA_PYTHON" bench/evals/gpu_check.py "$OUT" | tee "$OUT/checks.jsonl" || status=$?
"$CHIMAERA_BIN" --dir "$CHIMAERA_HOME" stop || true
exit "$status"
