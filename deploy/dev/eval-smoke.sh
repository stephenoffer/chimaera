#!/usr/bin/env bash
# Smoke-test the Anyscale eval job in the dev container: a local Ray head stands
# in for the node's cluster, and EVAL_SCALE shrinks the workloads.
set -euo pipefail
python3 -m venv /tmp/rayvenv && /tmp/rayvenv/bin/pip install -q "ray[default]" numpy
/tmp/rayvenv/bin/ray start --head --disable-usage-stats --include-dashboard=true >/dev/null
export RAY_ADDRESS=auto RAY_PYTHON=/tmp/rayvenv/bin/python EVAL_SCALE=${EVAL_SCALE:-0.1}
bash deploy/anyscale/run_eval.sh
