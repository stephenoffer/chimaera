#!/bin/bash
# A Chimaera program on a Slurm allocation: a peer on every node, and the
# driver on the first node against its local peer. The peers stop when the
# driver exits, and the job's exit code is the driver's.
#
#   sbatch chimaera.sbatch python3 app.py [ARGS...]
#   sbatch --nodes=8 --gpus-per-node=8 chimaera.sbatch python3 train.py
#
# `chimaera` must be on PATH on every node, and the driver's Python must have
# the chimaera package (the same Python and version everywhere).
#SBATCH --job-name=chimaera
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
# Whole nodes: each peer schedules every CPU of its node itself.
#SBATCH --exclusive
#SBATCH --output=chimaera-%j.out
# Two minutes before the time limit, slurm-launch gets SIGUSR1: it stops the
# driver (SIGTERM), then drains the peers.
#SBATCH --signal=B:USR1@120
exec chimaera slurm-launch -- "$@"
