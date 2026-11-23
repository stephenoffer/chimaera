#!/usr/bin/env bash
# Gate: `chimaera up lab` from a laptop container starts peers on three sshd
# hosts (copying its binary there), a driver on the laptop reaches them over
# TCP with ch.init(cluster="lab") and maps across hosts, and `chimaera down
# lab` stops them. Needs only docker; removes its containers and network.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# CHIMAERA_IMAGE names an already built runtime image to use instead of building one.
base=${CHIMAERA_IMAGE:-chimaera:dev}
net=chimaera-ssh-test
hosts=(host1 host2 host3)
# Everything the laptop runs, as its user, with the test's profile file.
lap() { docker exec -i -u chimaera -e CHIMAERA_CLUSTERS=/tmp/clusters.toml "$net-laptop" "$@"; }

cleanup() {
  status=$?
  if [ $status -ne 0 ]; then
    for h in "${hosts[@]}"; do
      echo "--- $h peer log" >&2
      docker exec -u chimaera "$net-$h" sh -c 'tail -n 30 ~/.chimaera/peers/lab/peer.log' >&2 || true
    done
  fi
  for c in "${hosts[@]}" laptop; do docker rm -f "$net-$c" >/dev/null 2>&1 || true; done
  docker network rm "$net" >/dev/null 2>&1 || true
  exit $status
}
trap cleanup EXIT

[ -n "${CHIMAERA_IMAGE:-}" ] || "$here/../images/build.sh" "$base"
docker build -t chimaera-ssh:dev --build-arg BASE="$base" -f "$here/Dockerfile.ssh" "$here"
docker network create "$net" >/dev/null
for h in "${hosts[@]}"; do
  docker run -d --name "$net-$h" --hostname "$h" --network "$net" --network-alias "$h" chimaera-ssh:dev >/dev/null
done
docker run -d --name "$net-laptop" --hostname laptop --network "$net" chimaera-ssh:dev sleep infinity >/dev/null

lap sh -c 'cat > /tmp/clusters.toml' <<'EOF'
[clusters.lab]
provider = "ssh"
hosts = ["host1", "host2", "host3"]
peers = ["host1:7710", "host2:7710", "host3:7710"]
EOF
lap sh -c 'for h in host1 host2 host3; do
  for i in $(seq 30); do ssh -o BatchMode=yes "$h" true 2>/dev/null && break; sleep 1; done
done'

echo "--- chimaera up lab"
lap chimaera up lab

lap sh -c 'cat > /tmp/driver.py' <<'EOF'
import collections, socket, time
import chimaera as ch
from chimaera._runtime import Runtime

ch.init(cluster="lab")

@ch.remote
def where(i):
    time.sleep(0.2)
    return socket.gethostname()

hosts = collections.Counter(where.map(range(60)))
print("HOSTS", len(hosts), dict(hosts))
print("REMOTE", Runtime.get().remote)
EOF
echo "--- driver on the laptop"
out=$(lap python3 /tmp/driver.py)
echo "$out"
n=$(echo "$out" | sed -n 's/^HOSTS \([0-9]*\).*/\1/p')
if [ -z "$n" ] || [ "$n" -le 1 ] || ! echo "$out" | grep -qx "REMOTE True"; then
  echo "FAIL: expected the map to run on more than one host over TCP" >&2
  exit 1
fi

# Peers and drivers use TLS: `up` made the cluster CA beside the token, and
# each peer issued itself a certificate from it.
lap sh -c 'test -f ~/.chimaera/clusters/lab/ca.pem' || { echo "FAIL: no cluster CA on the laptop" >&2; exit 1; }
docker exec -u chimaera "$net-host1" sh -c 'test -f ~/.chimaera/peers/lab/tls/peer.pem' ||
  { echo "FAIL: host1's peer has no certificate of its own" >&2; exit 1; }

echo "--- chimaera down lab"
lap chimaera down lab
for h in "${hosts[@]}"; do
  if docker exec -u chimaera "$net-$h" sh -c '~/.chimaera/bin/chimaera --dir ~/.chimaera/peers/lab status' >/dev/null 2>&1; then
    echo "FAIL: the peer on $h still answers after down" >&2
    exit 1
  fi
done
echo "ssh-test: PASS"
