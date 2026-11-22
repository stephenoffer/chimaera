"""The accelerators a call was granted: ``ch.dist.device()`` and ``ch.dist.devices()``.

    @ch.remote(gpus=1)
    def train(batch):
        import torch
        model.to(ch.dist.device())          # "cuda:0", "xla:0", "hpu", "xpu:0" or "mps"

The peer starts the call's worker with only its granted devices visible
(``CUDA_VISIBLE_DEVICES`` and the other vendors' variables) and lists them in
``CHIMAERA_DEVICES``. Calls without ``gpus=``/``device=`` see none.
"""

from __future__ import annotations

import json
import os
from typing import Any


def devices() -> list[dict[str, Any]]:
    """This call's devices, numbered as the call sees them: ``kind``, ``id``,
    ``index``, ``torch`` (its torch device string), ``backend`` (its
    ``torch.distributed`` backend), ``model`` and ``memory_bytes``. Empty in
    the driver and in calls that asked for no accelerator."""
    raw = os.environ.get("CHIMAERA_DEVICES")
    return json.loads(raw) if raw else []


def device() -> str:
    """The torch device string of this call's first device, or ``"cpu"``.
    Needs no torch import."""
    granted = devices()
    return granted[0]["torch"] if granted else "cpu"
