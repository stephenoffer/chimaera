#!/usr/bin/env bash
# Gate: a Slurm cluster in containers (slurmctld + 3 slurmd, munge) runs
# `sbatch chimaera.sbatch python3 driver.py`. slurm-launch starts a peer on
# each node through srun, and the driver's map must run on all 3 nodes.
# Needs only docker; removes its containers and network at the end.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# CHIMAERA_IMAGE names an already built runtime image to use instead of building one.
base=${CHIMAERA_IMAGE:-chimaera:dev}
net=chimaera-slurm-test
names=(slurmctld node1 node2 node3)
on() { local n=$1; shift; docker exec -i "$net-$n" "$@"; }

cleanup() {
  status=$?
  if [ $status -ne 0 ]; then
    for n in "${names[@]}"; do
      echo "--- $n" >&2
      docker logs --tail=30 "$net-$n" >&2 || true
    done
  fi
  for n in "${names[@]}"; do docker rm -f "$net-$n" >/dev/null 2>&1 || true; done
  docker network rm "$net" >/dev/null 2>&1 || true
  exit $status
}
trap cleanup EXIT

[ -n "${CHIMAERA_IMAGE:-}" ] || "$here/../images/build.sh" "$base"
docker build -t chimaera-slurm:dev --build-arg BASE="$base" "$here"
docker network create "$net" >/dev/null
for n in "${names[@]}"; do
  role=slurmd
  # slurmd manages cgroups for its job steps: a writable, private cgroup tree.
  extra=(--privileged --cgroupns=private)
  [ "$n" = slurmctld ] && role=slurmctld && extra=()
  docker run -d --name "$net-$n" --hostname "$n" --network "$net" --network-alias "$n" "${extra[@]}" chimaera-slurm:dev "$role" >/dev/null
done

idle=0
for _ in $(seq 60); do
  idle=$(on slurmctld sinfo -h -N -t idle -o %N 2>/dev/null | sort -u | wc -l | tr -d ' ')
  [ "$idle" -eq 3 ] && break
  sleep 2
done
on slurmctld sinfo -N
if [ "$idle" -ne 3 ]; then
  echo "FAIL: $idle of 3 nodes are idle" >&2
  exit 1
fi

# /shared stands in for the cluster's shared filesystem: the same files at
# the same path on every node.
put() {
  local body
  body=$(cat)
  for n in "${names[@]}"; do printf '%s\n' "$body" | docker exec -i -u chimaera "$net-$n" sh -c "cat > /shared/$1"; done
}
put chimaera.sbatch < "$here/chimaera.sbatch"
put driver.py <<'EOF'
import collections, socket, time
import chimaera as ch

@ch.remote
def where(i):
    time.sleep(0.2)
    return socket.gethostname()

hosts = collections.Counter(where.map(range(60)))
print("HOSTS", len(hosts), dict(hosts))
EOF

job=$(docker exec -u chimaera -w /shared "$net-slurmctld" sbatch --parsable chimaera.sbatch python3 /shared/driver.py)
echo "submitted job $job"
state=PENDING
for _ in $(seq 150); do
  state=$(on slurmctld scontrol show job "$job" | sed -n 's/.*JobState=\([A-Z_]*\).*/\1/p')
  case "$state" in PENDING | RUNNING | CONFIGURING | COMPLETING) sleep 2 ;; *) break ;; esac
done
batch_host=$(on slurmctld scontrol show job "$job" | sed -n 's/.*BatchHost=\([a-z0-9]*\).*/\1/p')
echo "--- job $job output (batch host $batch_host)"
out=$(on "$batch_host" cat "/shared/chimaera-$job.out")
echo "$out"
n=$(echo "$out" | sed -n 's/^HOSTS \([0-9]*\).*/\1/p')
echo "job $job: $state; the map ran on ${n:-no} nodes"
if [ "$state" != COMPLETED ] || [ "$n" != 3 ]; then
  echo "FAIL: expected a COMPLETED job whose map ran on 3 nodes" >&2
  exit 1
fi
# The peers must drain and exit on their own, not be killed by srun or Slurm.
if echo "$out" | grep -q -E "forcing job termination|task [0-9]+: Killed|did not stop within"; then
  echo "FAIL: the peers were killed instead of draining" >&2
  exit 1
fi
echo "slurm-test: PASS"
