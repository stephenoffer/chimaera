#!/usr/bin/env bash
# Gate (plan P6): queued work grows a Kubernetes pool, and a quiet pool shrinks
# after its peers drain. A one-pod StatefulSet (1 CPU per pod) on kind, the
# in-cluster provisioner from `chimaera k8s render --provisioner 3` (its own
# ServiceAccount, allowed only this StatefulSet's scale and reading its own
# pods), and a 40-call map from inside pod 0: the pool must grow to 3, every
# result must come back with work on the new pods, the pool must not shrink
# while its highest pod holds an actor, and must shrink back to 1 once it does
# not. Uses its own kind
# cluster and kubeconfig, and deletes the cluster at the end. Needs docker,
# kind and kubectl.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
image=${CHIMAERA_IMAGE:-chimaera:dev}
cluster=chimaera-kind-autoscale
export KUBECONFIG
KUBECONFIG=$(mktemp)

cleanup() {
  status=$?
  if [ $status -ne 0 ]; then
    echo "--- autoscale-test failed; provisioner log, pods and peer logs:" >&2
    kubectl logs deploy/chimaera-provisioner >&2 || true
    kubectl get pods -o wide >&2 || true
    for p in chimaera-0 chimaera-1 chimaera-2; do
      echo "--- $p" >&2
      kubectl logs "$p" --tail=30 >&2 || true
    done
  fi
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  rm -f "$KUBECONFIG"
  exit $status
}
trap cleanup EXIT

replicas() { kubectl get statefulset chimaera -o jsonpath='{.spec.replicas}'; }

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
docker run --rm "$image" chimaera k8s render --image "$image" --replicas 1 --cpus 1 \
  --namespace default --provisioner 3 | kubectl apply -f -
# Faster than the defaults, for a test: later flags win.
kubectl patch deployment chimaera-provisioner --type=json -p='[
  {"op": "add", "path": "/spec/template/spec/containers/0/command/-", "value": "--up-after=5"},
  {"op": "add", "path": "/spec/template/spec/containers/0/command/-", "value": "--down-after=40"},
  {"op": "add", "path": "/spec/template/spec/containers/0/command/-", "value": "--interval=2"},
  {"op": "add", "path": "/spec/template/spec/containers/0/command/-", "value": "--start-grace=30"}]'
for _ in $(seq 150); do kubectl get pod chimaera-0 >/dev/null 2>&1 && break; sleep 2; done
kubectl wait --for=condition=Ready pod/chimaera-0 --timeout=300s
kubectl rollout status deployment/chimaera-provisioner --timeout=300s

echo "--- the provisioner may read its own pods, and nothing else"
as=system:serviceaccount:default:chimaera-provisioner
[ "$(kubectl auth can-i get pod/chimaera-2 --as=$as)" = yes ] || { echo "FAIL: cannot read its pod" >&2; exit 1; }
for no in "get pod/chimaera-3" "get pod/other" "list pods" "delete pod/chimaera-2" "patch pod/chimaera-2"; do
  if [ "$(kubectl auth can-i $no --as=$as || true)" != no ]; then echo "FAIL: it may $no" >&2; exit 1; fi
done

echo "--- a map that needs more than one pod"
kubectl exec -i chimaera-0 -- sh -c 'cat > /tmp/driver.py' <<'EOF'
import collections, socket, time
import chimaera as ch

@ch.remote(cpus=1)
def where(i):
    time.sleep(3)
    return socket.gethostname()

hosts = collections.Counter(where.map(range(40)))
print("HOSTS", sum(hosts.values()), len(hosts), dict(hosts))
EOF
out=$(kubectl exec chimaera-0 -- env CHIMAERA_HOME=/data python3 /tmp/driver.py)
echo "$out"
read -r total hosts < <(echo "$out" | sed -n 's/^HOSTS \([0-9]*\) \([0-9]*\).*/\1 \2/p')
kubectl logs deploy/chimaera-provisioner
if [ "${total:-0}" != 40 ] || [ "${hosts:-0}" -lt 2 ]; then
  echo "FAIL: expected 40 results from more than one pod, got ${total:-none} from ${hosts:-no} pods" >&2
  exit 1
fi
kubectl logs deploy/chimaera-provisioner | grep -q '"to": 3' || { echo "FAIL: the pool never grew to 3" >&2; exit 1; }

echo "--- an actor on the highest pod keeps it"
kubectl exec -i chimaera-0 -- sh -c 'cat > /tmp/keep.py' <<'EOF'
import socket, time
import chimaera as ch

@ch.remote
class Keep:
    def host(self):
        return socket.gethostname()

k = Keep.options(placement=ch.dist.Placement(require={"host": "chimaera-2"})).remote()
print("ACTOR", k.host().result(), flush=True)
time.sleep(3600)
EOF
kubectl exec chimaera-0 -- sh -c 'CHIMAERA_HOME=/data nohup python3 /tmp/keep.py > /tmp/keep.log 2>&1 & echo $! > /tmp/keep.pid'
for _ in $(seq 60); do kubectl exec chimaera-0 -- grep -q '^ACTOR chimaera-2' /tmp/keep.log 2>/dev/null && break; sleep 2; done
kubectl exec chimaera-0 -- cat /tmp/keep.log
for _ in $(seq 60); do
  kubectl logs deploy/chimaera-provisioner | grep '"holding": 3' | grep -q 'holds 1 actors' && break
  sleep 2
done
kubectl logs deploy/chimaera-provisioner
if [ "$(replicas)" != 3 ] || ! kubectl logs deploy/chimaera-provisioner | grep '"holding": 3' | grep -q 'holds 1 actors'; then
  echo "FAIL: expected the pool held at 3 while chimaera-2 hosts an actor, got $(replicas) replicas" >&2
  exit 1
fi
kubectl exec chimaera-0 -- sh -c 'kill $(cat /tmp/keep.pid)'

echo "--- waiting for the quiet pool to shrink to 1"
for _ in $(seq 120); do
  [ "$(replicas)" = 1 ] && break
  sleep 2
done
kubectl logs deploy/chimaera-provisioner
if [ "$(replicas)" != 1 ]; then
  echo "FAIL: the pool is still $(replicas) replicas after the work ended" >&2
  exit 1
fi
kubectl wait --for=delete pod/chimaera-2 --timeout=120s >/dev/null 2>&1 || true
echo "autoscale-test: PASS"
