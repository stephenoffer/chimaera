#!/usr/bin/env bash
# Gate (plan P6): a cluster started by `slurm-launch` in a one-node job grows
# with spare Slurm jobs and shrinks back. Slurm in containers (slurmctld + 3
# slurmd); the job runs the provisioner (`--slurm`) beside a map that needs
# more than one node: spares must join and take work, then be cancelled once
# the cluster is quiet. Another cluster's spare (a job of the same user, named
# as spares were before they were named per cluster) holds one node: it must
# be neither counted nor cancelled, and the spare that cannot start beside it
# must be cancelled as one that never joined. Needs only docker; removes its
# containers at the end.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# CHIMAERA_IMAGE names an already built runtime image to use instead of building one.
base=${CHIMAERA_IMAGE:-chimaera:dev}
net=chimaera-slurm-autoscale
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
import collections, os, socket, subprocess, sys, time
import chimaera as ch

log = open("/shared/provisioner.log", "w")
prov = subprocess.Popen(
    [sys.executable, "-m", "chimaera.provisioner", "--slurm", "--max", "3", "--up-after", "3",
     "--down-after", "20", "--interval", "2", "--start-grace", "90"],
    stdout=log, stderr=subprocess.STDOUT,
)

@ch.remote(cpus=4)  # a whole node each: the one-node job alone runs them one at a time
def where(i):
    time.sleep(3)
    return socket.gethostname()

hosts = collections.Counter(where.map(range(16)))
print("HOSTS", sum(hosts.values()), len(hosts), dict(hosts), flush=True)
spares = ["?"]
deadline = time.time() + 240
while time.time() < deadline:
    mine = f"chimaera-spare-{os.environ['SLURM_JOB_ID']}"
    spares = subprocess.run(["squeue", "-h", "-n", mine, "-o", "%i"], capture_output=True, text=True).stdout.split()
    if not spares:
        break
    time.sleep(3)
print("SPARES_LEFT", len(spares), flush=True)
prov.terminate()
EOF

token=$(on slurmctld chimaera cluster token)
other=$(docker exec -u chimaera -w /shared "$net-slurmctld" \
  sbatch --parsable --nodes=1 --exclusive --job-name chimaera-spare --wrap 'sleep 900')
job=$(docker exec -u chimaera -w /shared "$net-slurmctld" \
  sbatch --parsable --nodes=1 --export=ALL,CHIMAERA_CLUSTER_TOKEN="$token" chimaera.sbatch python3 /shared/driver.py)
echo "submitted job $job"
state=PENDING
for _ in $(seq 200); do
  state=$(on slurmctld scontrol show job "$job" | sed -n 's/.*JobState=\([A-Z_]*\).*/\1/p')
  case "$state" in PENDING | RUNNING | CONFIGURING | COMPLETING) sleep 2 ;; *) break ;; esac
done
batch_host=$(on slurmctld scontrol show job "$job" | sed -n 's/.*BatchHost=\([a-z0-9]*\).*/\1/p')
out=$(on "$batch_host" cat "/shared/chimaera-$job.out")
echo "--- job $job output"
echo "$out"
echo "--- provisioner"
on "$batch_host" cat /shared/provisioner.log || true
read -r total hosts < <(echo "$out" | sed -n 's/^HOSTS \([0-9]*\) \([0-9]*\).*/\1 \2/p')
if [ "$state" != COMPLETED ] || [ "${total:-0}" != 16 ] || [ "${hosts:-0}" -lt 2 ]; then
  echo "FAIL: expected a COMPLETED job with 16 results from more than one node (state $state, ${total:-no} results, ${hosts:-no} nodes)" >&2
  exit 1
fi
if ! echo "$out" | grep -q "^SPARES_LEFT 0$"; then
  echo "FAIL: spare jobs were not cancelled after the work ended" >&2
  exit 1
fi
if [ -z "$(on slurmctld squeue -h -j "$other" -o %i 2>/dev/null)" ]; then
  echo "FAIL: another cluster's spare job $other was cancelled" >&2
  exit 1
fi
echo "slurm-autoscale-test: PASS"
