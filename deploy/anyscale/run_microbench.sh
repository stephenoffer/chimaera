#!/usr/bin/env bash
# Entry point of the microbenchmark job (deploy/anyscale/microbench.job.yaml):
# builds Chimaera from source on this node, then runs Ray's own microbenchmark
# suite and its Chimaera port (bench/microbench/run.py), both on the image's
# Python, on the machine type Ray publishes its microbenchmark numbers for.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
for d in /mnt/user_storage /mnt/cluster_storage "$ROOT/results"; do
  if mkdir -p "$d/chimaera-microbench/$STAMP" 2>/dev/null; then OUT="$d/chimaera-microbench/$STAMP"; break; fi
done
echo "results -> $OUT"
export CARGO_INCREMENTAL=0
# shellcheck source=deploy/anyscale/build.sh
source deploy/anyscale/build.sh   # the peer and the wheel; CHIMAERA_BIN, CHIMAERA_PYTHON
RAY_PY=${RAY_PYTHON:-python3}
"$CHIMAERA_BIN" --version
"$RAY_PY" -c 'import ray; ray.init(address="auto"); print("cpus", ray.cluster_resources().get("CPU"))'
# run.py runs Ray from .venv-bench and Chimaera from .venv.
mkdir -p .venv/bin .venv-bench/bin /tmp/microbench  # peer homes on local disk, not shared storage
ln -sf "$(command -v "$CHIMAERA_PYTHON")" .venv/bin/python
ln -sf "$(command -v "$RAY_PY")" .venv-bench/bin/python
"$CHIMAERA_PYTHON" bench/microbench/run.py --rounds "${MICROBENCH_ROUNDS:-2}" --sleep "${MICROBENCH_SLEEP:-3}" \
  --chimaera-bin "$CHIMAERA_BIN" --workdir /tmp/microbench
cp /tmp/microbench/*.log "$OUT/" 2>/dev/null || true
echo "===== report ====="
cat bench/microbench/RESULTS.md
echo "===== results.json ====="
cat "$(ls -t bench/microbench/results-*.json | head -1)"
