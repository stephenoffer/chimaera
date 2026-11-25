#!/usr/bin/env bash
# Entry point of the release-scale jobs (deploy/anyscale/release-*node.job.yaml):
# builds Chimaera once on the head node, then bench/evals/release_scale.py ships
# it to every node (through Ray), starts one peer per node, and runs Ray's
# distributed release benchmarks against Chimaera and then Ray.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
for d in /mnt/user_storage /mnt/cluster_storage "$ROOT/results"; do
  if mkdir -p "$d/chimaera-evals/release-$STAMP" 2>/dev/null; then OUT="$d/chimaera-evals/release-$STAMP"; break; fi
done
echo "results -> $OUT"
export CARGO_INCREMENTAL=0
# shellcheck source=deploy/anyscale/build.sh
source deploy/anyscale/build.sh   # the peer and the wheel; CHIMAERA_BIN, CHIMAERA_PYTHON
# release_scale.py drives Ray and Chimaera from one interpreter: the image's, which build.sh
# picks on these Python 3.11 images.
PY=$CHIMAERA_PYTHON
DIST=/tmp/chimaera-dist
rm -rf "$DIST" && mkdir -p "$DIST"
cp "$CHIMAERA_BIN" "$DIST/chimaera"
cp /tmp/chimaera-wheels/chimaera-*.whl "$DIST/"
"$DIST/chimaera" cluster token > "$DIST/token"
"$PY" -c 'import ray; ray.init(address="auto"); print("nodes", len([n for n in ray.nodes() if n["Alive"]]), "cpus", ray.cluster_resources().get("CPU"))'
"$PY" bench/evals/release_scale.py --dist "$DIST" --out "$OUT"
echo "===== release_results.json ====="
cat "$OUT/release_results.json"
