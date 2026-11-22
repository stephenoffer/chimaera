"""GPU check for a CUDA node (deploy/anyscale/gpu-check.job.yaml): what the
device layer promises, on real hardware. Prints one JSON line per check and
exits non-zero if any fails.

- the peer finds every GPU nvidia-smi lists;
- a ``gpus=1`` call sees exactly one GPU, as torch counts them (plan rule 3),
  named by its UUID in CUDA_VISIBLE_DEVICES;
- concurrent ``gpus=1`` calls get different GPUs; a CPU call sees none;
- a 2-rank gang initializes NCCL through ``group.torch()`` and all-reduces on
  the GPUs;
- a CUDA tensor passed between calls is rebuilt where its reader is: on a GPU
  call's GPU, on a CPU call's CPU, and on the driver's GPU;
- the node's driver files are copied to OUT/fixture, a real topology tree to
  replace the hand-written ones in tests/fixtures/topology.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time

import chimaera as ch


@ch.remote(gpus=1)
def one_gpu(hold: float) -> dict:
    import torch

    time.sleep(hold)  # overlap with the other calls, so grants must differ
    return {
        "visible": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "torch_count": torch.cuda.device_count(),
        "device": ch.dist.device(),
        "uuid": str(torch.cuda.get_device_properties(0).uuid) if torch.cuda.is_available() else "",
    }


@ch.remote(cpus=1)
def cpu_call() -> dict:
    import torch

    return {"visible": os.environ.get("CUDA_VISIBLE_DEVICES"), "torch_count": torch.cuda.device_count()}


@ch.remote(gpus=1)
def make_on_gpu():
    import torch

    return torch.arange(1000, dtype=torch.float32, device="cuda:0")


@ch.remote(gpus=1)
def device_of_gpu(t) -> str:
    return str(t.device)


@ch.remote(cpus=1)
def device_of_cpu(t) -> str:
    return str(t.device)


@ch.remote(gpus=1)
def nccl_rank() -> tuple[int, float, str]:
    import torch

    g = ch.context().gang
    dist = g.torch()
    t = torch.tensor([float(g.rank + 1)], device="cuda:0")
    dist.all_reduce(t)
    return g.rank, t.item(), dist.get_backend()


def capture(out: str) -> int:
    """Copy the files CUDA discovery reads into OUT/fixture; the number of GPUs found there."""
    root = os.path.join(out, "fixture")
    gpus = "/proc/driver/nvidia/gpus"
    for bus in sorted(os.listdir(gpus)) if os.path.isdir(gpus) else []:
        dst = os.path.join(root, gpus.lstrip("/"), bus)
        os.makedirs(dst, exist_ok=True)
        shutil.copy(os.path.join(gpus, bus, "information"), dst)
        numa = f"/sys/bus/pci/devices/{bus.lower()}/numa_node"
        if os.path.exists(numa):
            os.makedirs(os.path.join(root, os.path.dirname(numa).lstrip("/")), exist_ok=True)
            shutil.copy(numa, os.path.join(root, numa.lstrip("/")))
    os.makedirs(os.path.join(root, "dev"), exist_ok=True)
    for node in os.listdir("/dev"):
        if node.startswith("nvidia") and node[6:].isdigit():
            open(os.path.join(root, "dev", node), "w").close()
    return len(os.listdir(os.path.join(root, gpus.lstrip("/")))) if os.path.isdir(os.path.join(root, gpus.lstrip("/"))) else 0


def main() -> int:
    out = sys.argv[1] if len(sys.argv) > 1 else "."
    failed = 0

    def check(name: str, ok: bool, **detail: object) -> None:
        nonlocal failed
        failed += not ok
        print(json.dumps({"check": name, "ok": ok, **detail}, default=str), flush=True)

    if shutil.which("nvidia-smi") is None:
        check("nvidia-smi lists GPUs", False, error="nvidia-smi is not installed: not an NVIDIA node")
        return 1
    smi = subprocess.run(["nvidia-smi", "--query-gpu=uuid", "--format=csv,noheader"], capture_output=True, text=True)
    uuids = sorted(smi.stdout.split()) if smi.returncode == 0 else []
    if not uuids:
        check("nvidia-smi lists GPUs", False, stderr=smi.stderr[-500:])
        return 1
    check("fixture captured", capture(out) == len(uuids), path=os.path.join(out, "fixture"))
    peer_gpus = sorted(g for n in ch.runs.cluster().nodes() if n.this_peer for g in n.gpus)
    check("the peer finds every GPU", peer_gpus == uuids, peer=peer_gpus, nvidia_smi=uuids)

    n = min(len(uuids), 4)
    grants = ch.get((one_gpu.remote(2.0) for _ in range(n)), timeout=600)
    check("a gpus=1 call sees one GPU", all(g["torch_count"] == 1 for g in grants), grants=grants)
    check("its CUDA_VISIBLE_DEVICES is a UUID", all(g["visible"] in uuids for g in grants))
    check("concurrent calls get different GPUs", len({g["visible"] for g in grants}) == n)
    check("ch.dist.device() is cuda:0", all(g["device"] == "cuda:0" for g in grants))
    cpu = cpu_call.remote().result(timeout=300)
    check("a CPU call sees no GPU", cpu["visible"] == "" and cpu["torch_count"] == 0, call=cpu)
    t = make_on_gpu.remote()
    check("a CUDA tensor reaches a GPU call on its GPU", device_of_gpu.remote(t).result(timeout=300) == "cuda:0")
    check("a CUDA tensor reaches a CPU call on the CPU", device_of_cpu.remote(t).result(timeout=300) == "cpu")
    check("a CUDA tensor result reaches the driver on its GPU", str(t.result(timeout=300).device).startswith("cuda"))

    if len(uuids) >= 2:
        ranks = nccl_rank.options(ranks=2).gang().result(timeout=600)
        check("a 2-rank gang all-reduces over NCCL", ranks == [(0, 3.0, "nccl"), (1, 3.0, "nccl")], ranks=ranks)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
