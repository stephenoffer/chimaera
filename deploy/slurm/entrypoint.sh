#!/bin/sh
# Start munge, then the Slurm daemon this container plays (logs to stderr).
set -e
runuser -u munge -- munged
case "$1" in
  slurmctld) exec slurmctld -D ;;
  slurmd)
    # cgroup v2 in a private namespace (the node runs privileged): move this
    # container's processes into a child group so controllers can be enabled
    # for children, then give slurmd the system.slice it expects.
    cg=/sys/fs/cgroup
    mkdir -p "$cg/init"
    xargs -rn1 < "$cg/cgroup.procs" > "$cg/init/cgroup.procs"
    sed -e 's/ / +/g' -e 's/^/+/' < "$cg/cgroup.controllers" > "$cg/cgroup.subtree_control"
    mkdir -p "$cg/system.slice"
    exec slurmd -D -N "$(hostname)" ;;
  *) exec "$@" ;;
esac
