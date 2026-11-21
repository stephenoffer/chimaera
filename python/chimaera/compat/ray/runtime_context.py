"""``ray.get_runtime_context()``: what the current task, actor or driver knows about itself."""

from __future__ import annotations

from typing import Any

import chimaera as ch

from . import _state
from ._unsupported import unsupported


class RuntimeContext:
    def get_job_id(self) -> str:
        """The Chimaera run id (Ray's job id)."""
        ctx = ch.context()
        return ctx.run_id if ctx is not None else ch.current_run().id

    def get_task_id(self) -> str | None:
        ctx = ch.context()
        return ctx.task_id if ctx is not None else None

    def get_actor_id(self) -> str | None:
        ctx = ch.context()
        return ctx.actor.hex() if ctx is not None and ctx.actor else None

    def get_node_id(self) -> str:
        """The peer id (hex) of this call's node, or the driver's peer."""
        from ._nodes import this_node_id

        return this_node_id()

    def get_accelerator_ids(self) -> dict[str, list[str]]:
        """The call's granted devices by Ray's resource names, as the call numbers them."""
        names = {"cuda": "GPU", "rocm": "GPU", "tpu": "TPU", "neuron": "neuron_cores", "hpu": "HPU", "xpu": "GPU", "mps": "GPU"}
        out: dict[str, list[str]] = {}
        for d in ch.devices():
            out.setdefault(names[d["kind"]], []).append(str(d["index"]))
        return out

    @property
    def namespace(self) -> str | None:
        return _state.namespace

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        lands = {"get_assigned_resources": "P5"}.get(name, "")
        raise unsupported(f"RuntimeContext.{name}", use="ch.context()", lands=lands)


def get_runtime_context() -> RuntimeContext:
    return RuntimeContext()
