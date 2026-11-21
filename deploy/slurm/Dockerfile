# Slurm nodes for the slurm-test gate: the chimaera runtime image
# (deploy/images/Dockerfile) plus Slurm and munge from Debian. Every node and
# the controller run this image, so they share the munge key, the chimaera
# binary and the Python the driver uses.
ARG BASE=chimaera:dev
FROM ${BASE}
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends slurm-wlm munge \
 && rm -rf /var/lib/apt/lists/*
RUN (test -f /etc/munge/munge.key || mungekey --create) \
 && chown munge:munge /etc/munge/munge.key && chmod 400 /etc/munge/munge.key \
 && mkdir -p /var/spool/slurmctld /var/spool/slurmd /run/munge /shared \
 && chown munge:munge /run/munge && chown 1000:1000 /shared
COPY slurm.conf /etc/slurm/slurm.conf
COPY cgroup.conf /etc/slurm/cgroup.conf
COPY entrypoint.sh /usr/local/bin/slurm-entrypoint
ENTRYPOINT ["slurm-entrypoint"]
