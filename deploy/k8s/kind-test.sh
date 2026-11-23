#!/usr/bin/env bash
# Gate: a 3-peer StatefulSet from `chimaera k8s render` forms on kind, runs a
# map across its pods, survives deleting founder pod chimaera-0, and runs the
# map again. Uses its own kind cluster and kubeconfig (your kubectl context is
# untouched) and deletes the cluster at the end. Needs docker, kind, kubectl.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# CHIMAERA_IMAGE names an already built runtime image to use instead of building one.
image=${CHIMAERA_IMAGE:-chimaera:dev}
cluster=chimaera-kind-test
export KUBECONFIG
KUBECONFIG=$(mktemp)

cleanup() {
  status=$?
  if [ $status -ne 0 ]; then
    echo "--- kind-test failed; pods, events and peer logs:" >&2
    kubectl get pods,pvc -o wide >&2 || true
    kubectl get events --sort-by=.lastTimestamp 2>&1 | tail -n 25 >&2 || true
    kubectl describe nodes 2>&1 | grep -E -A3 "^Taints|^Conditions" >&2 || true
    for p in chimaera-0 chimaera-1 chimaera-2; do
      echo "--- $p" >&2
      kubectl logs "$p" --tail=40 >&2 || true
    done
  fi
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  rm -f "$KUBECONFIG"
  exit $status
}
trap cleanup EXIT

[ -n "${CHIMAERA_IMAGE:-}" ] || "$here/../images/build.sh" "$image"
kind create cluster --name "$cluster" --kubeconfig "$KUBECONFIG" --wait 180s
kind load docker-image "$image" --name "$cluster"

# The cluster CA and token (`cluster init`), made in the image and kept in
# Secrets: peers issue themselves certificates from the CA (mutual TLS).
ca=$(mktemp -d)
docker run --rm "$image" sh -c 'chimaera cluster init --out /tmp/ca >/dev/null && tar -C /tmp/ca -cf - .' | tar -C "$ca" -xf -
kubectl create secret generic chimaera-token --from-file=token="$ca/token"
kubectl create secret generic chimaera-tls --from-file="$ca/ca.pem" --from-file="$ca/ca.key"
rm -rf "$ca"
docker run --rm "$image" chimaera k8s render --image "$image" --replicas 3 | kubectl apply -f -

wait_ready() {
  # The pods are Ready once their client port opens, i.e. the cluster formed.
  for _ in $(seq 150); do
    [ "$(kubectl get pods -l app.kubernetes.io/instance=chimaera --no-headers 2>/dev/null | wc -l)" -eq 3 ] && break
    sleep 2
  done
  kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=chimaera --timeout=300s
}

driver() {
  kubectl exec -i chimaera-0 -- sh -c 'cat > /tmp/driver.py' <<'EOF'
import collections, socket, time
import chimaera as ch

@ch.remote
def where(i):
    time.sleep(0.2)
    return socket.gethostname()

hosts = collections.Counter(where.map(range(60)))
print("HOSTS", len(hosts), dict(hosts))
EOF
  # Against pod 0's own peer: its socket is in /data.
  out=$(kubectl exec chimaera-0 -- env CHIMAERA_HOME=/data python3 /tmp/driver.py)
  echo "$out"
  n=$(echo "$out" | sed -n 's/^HOSTS \([0-9]*\).*/\1/p')
  if [ -z "$n" ] || [ "$n" -le 1 ]; then
    echo "FAIL: the map ran on ${n:-no} pods; expected more than one" >&2
    return 1
  fi
}

wait_ready
echo "--- run 1"
driver

echo "--- deleting founder pod chimaera-0"
old=$(kubectl get pod chimaera-0 -o jsonpath='{.metadata.uid}')
kubectl delete pod chimaera-0 --wait=true
for _ in $(seq 150); do
  uid=$(kubectl get pod chimaera-0 -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  [ -n "$uid" ] && [ "$uid" != "$old" ] && break
  sleep 2
done
wait_ready
echo "--- run 2 (after chimaera-0 came back)"
driver
echo "kind-test: PASS"
