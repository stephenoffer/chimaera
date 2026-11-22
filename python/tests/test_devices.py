"""Accelerator grants: gpus=/device= on this host, as ch.dist.device() and ch.dist.devices() see them."""

from __future__ import annotations

import platform
import sys

import pytest

import chimaera as ch

APPLE_SILICON = sys.platform == "darwin" and platform.machine() == "arm64"


@ch.remote
def seen():
    return ch.dist.device(), ch.dist.devices()


def test_calls_without_accelerators_see_none_and_the_driver_neither():
    assert ch.dist.device() == "cpu" and ch.dist.devices() == []
    assert seen.remote().result(timeout=60) == ("cpu", [])


def test_more_devices_than_the_host_has_is_infeasible_with_the_reason():
    with pytest.raises(ch.Infeasible, match="accelerator"):
        seen.options(gpus=64).remote().result(timeout=60)
    with pytest.raises(ch.Infeasible, match="tpu"):
        seen.options(device="tpu:v5e").remote().result(timeout=60)


@pytest.mark.skipif(not APPLE_SILICON, reason="needs an Apple GPU")
def test_a_call_granted_the_apple_gpu_sees_mps():
    where, granted = seen.options(gpus=1).remote().result(timeout=60)
    assert where == "mps"
    [d] = granted
    assert d["kind"] == "mps" and d["backend"] == "gloo" and d["memory_bytes"] > 0 and d["model"].startswith("M")
    # The same through device= alone (one unit), and an actor keeps its grant.
    assert seen.options(device="mps").remote().result(timeout=60)[0] == "mps"

    @ch.remote(gpus=1)
    class Holder:
        def where(self):
            return ch.dist.device()

    h = Holder.remote()
    assert [h.where.remote().result(timeout=60) for _ in range(3)] == ["mps"] * 3
    h.close()


@pytest.mark.skipif(not APPLE_SILICON, reason="needs an Apple GPU")
def test_a_single_peer_lists_its_accelerators_in_the_cluster_view():
    # The GPU check on an A10G node found a lone peer listing none (grants worked).
    [me] = [n for n in ch.runs.cluster().nodes() if n.this_peer]
    assert me.gpus == ["mps:0"]
    assert ch.runs.cluster().resources()["gpus"] == 1
