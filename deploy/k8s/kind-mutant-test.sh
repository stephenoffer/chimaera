#!/usr/bin/env bash
# Proof that kind-test catches a deployment regression the render's unit tests
# miss: the runtime image again, with `chimaera k8s render` mounting the peers'
# volume at /srv/data instead of /data, where the peer keeps its state. Passes
# only when kind-test fails because the pods never became Ready. Uses
# CHIMAERA_IMAGE as the base image, else builds chimaera:dev.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
base=${CHIMAERA_IMAGE:-chimaera:dev}
mutant=chimaera:mutant-data-mount
[ -n "${CHIMAERA_IMAGE:-}" ] || "$here/../images/build.sh" "$base"
log=$(mktemp)
trap 'rm -f "$log"' EXIT

docker build -q -t "$mutant" - >/dev/null <<EOF
FROM $base
USER root
RUN mv /usr/local/bin/chimaera /usr/local/bin/chimaera-real \
 && printf '%s\n' '#!/bin/sh' \
    'if [ "\$1 \$2" = "k8s render" ]; then /usr/local/bin/chimaera-real "\$@" | sed "s|mountPath: /data\$|mountPath: /srv/data|"; exit; fi' \
    'exec /usr/local/bin/chimaera-real "\$@"' > /usr/local/bin/chimaera \
 && chmod 755 /usr/local/bin/chimaera
USER 1000:1000
EOF
# What the mutant changes in the manifests kind-test applies: that line only.
if diff <(docker run --rm "$base" chimaera k8s render --image x --replicas 3) \
  <(docker run --rm "$mutant" chimaera k8s render --image x --replicas 3); then
  echo "FAIL: the mutant renders the same manifests as $base" >&2
  exit 1
fi

if CHIMAERA_IMAGE=$mutant "$here/kind-test.sh" >"$log" 2>&1; then
  tail -n 40 "$log"
  echo "FAIL: kind-test passed with the peers' volume moved off /data" >&2
  exit 1
fi
if ! grep -q "timed out waiting for the condition" "$log"; then
  tail -n 60 "$log"
  echo "FAIL: kind-test failed, but not because the pods never became Ready" >&2
  exit 1
fi
# The pods as kind-test's failure report lists them, and the first peer's log.
grep -E "timed out waiting|^(pod/)?chimaera-[0-9] " "$log" || true
grep -A 6 -e "^--- chimaera-0" "$log" || true
docker image rm -f "$mutant" >/dev/null
echo "kind-mutant-test: PASS (kind-test refused the mutant)"
