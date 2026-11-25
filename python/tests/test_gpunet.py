"""The GPU-to-GPU path for CUDA tensors read on another machine (_gpunet):
when it is on, and that every failure leaves the reader with None, so it
reads the host copy. The transfer itself needs two machines with GPUs; it is
measured in bench/objects/RESULTS.md."""

from __future__ import annotations

import json
import os
import socket
import struct
import threading
import time

import pytest

from chimaera import _gpunet


@pytest.mark.parametrize(
    "setting, on",
    [
        (None, False),  # off unless asked for, RDMA or not: it neither authenticates nor encrypts
        ("", False),
        ("0", False),
        ("1", True),  # NCCL
        ("nixl", True),  # NIXL
        ("yes", False),
    ],
)
def test_it_is_on_only_when_asked_for(monkeypatch, setting, on):
    monkeypatch.delenv("CHIMAERA_CUDA_NET", raising=False)
    if setting is not None:
        monkeypatch.setenv("CHIMAERA_CUDA_NET", setting)
    monkeypatch.setattr(os, "listdir", lambda path: ["mlx5_0"])  # an RDMA device everywhere: still off
    assert _gpunet.enabled() is on


class _Recording:
    """A socket that notes how many bytes each read asked for."""

    def __init__(self, sock):
        self.sock, self.asked = sock, []

    def recv(self, n):
        self.asked.append(n)
        return self.sock.recv(n)


def test_a_message_longer_than_the_bound_is_refused_before_anything_is_read_for_it():
    a, b = socket.socketpair()
    with a, b:
        a.sendall(struct.pack("<I", (4 << 30) - 1))  # announces 4 GiB, sends nothing more
        r = _Recording(b)
        with pytest.raises(ConnectionError, match="above"):
            _gpunet._recv_msg(r)
        assert r.asked == [4], "only the length header was read"
        # A holder's reply may be larger than a request (NIXL's agent metadata).
        reply = json.dumps({"meta": "ab" * (200 << 10)}).encode()
        writer = threading.Thread(target=a.sendall, args=(struct.pack("<I", len(reply)) + reply,))
        writer.start()  # more than the socket buffers: it is read while it is written
        assert len(_gpunet._recv_msg(b, _gpunet._REPLY_MAX)["meta"]) == 400 << 10
        writer.join()


class _Held:  # any object a weakref can hold stands in for a tensor here
    pass


def _listener(monkeypatch):
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "1")
    held = _Held()
    host, port = _gpunet.offer(held)["addr"].rsplit(":", 1)
    return held, (host, int(port))


def test_the_listener_closes_a_connection_that_announces_too_much(monkeypatch):
    _, addr = _listener(monkeypatch)
    with socket.create_connection(addr, timeout=5) as s:
        s.sendall(struct.pack("<I", (4 << 30) - 1))
        assert s.recv(1) == b"", "closed without reading the 4 GiB it announced"


def test_connections_past_the_cap_are_closed_at_once(monkeypatch):
    _, addr = _listener(monkeypatch)
    monkeypatch.setattr(_gpunet, "_handlers", threading.BoundedSemaphore(1))
    with socket.create_connection(addr, timeout=5) as first:  # takes the one slot and sends nothing
        time.sleep(0.2)
        with socket.create_connection(addr, timeout=5) as second:
            t = time.monotonic()
            assert second.recv(1) == b""
            assert time.monotonic() - t < 2, "closed at once, not after the first one's timeout"
        first.sendall(struct.pack("<I", 2) + b"{}")  # the first is still being served: it is answered
        assert first.recv(1) == b"", "a request without a token is closed after it is read"


def test_a_holder_that_is_gone_answers_none_at_once(monkeypatch):
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "1")
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # nothing listens there now
    t = time.monotonic()
    assert _gpunet.fetch({"addr": f"127.0.0.1:{port}", "token": "00" * 16}, "cuda:0") is None
    assert time.monotonic() - t < 5


@pytest.mark.parametrize("via", ["1", "nixl"])
def test_an_unknown_token_is_refused_and_the_reader_falls_back(monkeypatch, via):
    monkeypatch.setenv("CHIMAERA_CUDA_NET", via)

    class Held:  # any object a weakref can hold stands in for a tensor here
        pass

    held = Held()
    offer = _gpunet.offer(held)
    assert offer is not None and set(offer) == {"addr", "token"}
    forged = {"addr": offer["addr"], "token": "ff" * 16}
    assert _gpunet.fetch(forged, "cuda:0") is None


def test_a_holder_that_cannot_send_with_nixl_leaves_the_reader_none_at_once(monkeypatch):
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "nixl")

    class Held:  # not a CUDA tensor: registering it with NIXL fails, or nixl is not installed
        shape, dtype, device = (4,), "torch.float32", "cuda:0"

    held = Held()
    offer = _gpunet.offer(held)
    t = time.monotonic()
    assert _gpunet.fetch(offer, "cuda:0") is None
    assert time.monotonic() - t < 5
    assert not _gpunet._nixl_held, "nothing stays registered"


def test_a_read_that_fails_once_begun_is_not_tried_again_from_that_holder(monkeypatch):
    torch = pytest.importorskip("torch")
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "nixl")
    monkeypatch.setattr(_gpunet, "_failed", set())
    monkeypatch.setattr(_gpunet, "_warned", set())
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)  # no GPU needed to get this far
    asked = []

    def send(conn, t, token):  # a holder that offers it over NIXL
        asked.append(token)
        _gpunet._send_msg(conn, {"ok": True, "shape": [4], "dtype": "float32", "descs": "", "meta": "", "name": "h"})
        _gpunet._recv_msg(conn)

    def stuck(reply, device):
        raise RuntimeError("the NIXL read ended in PROC")

    monkeypatch.setattr(_gpunet, "_send_nixl", send)
    monkeypatch.setattr(_gpunet, "_read_nixl", stuck)
    held = torch.ones(4)
    offer = _gpunet.offer(held)
    with pytest.warns(RuntimeWarning, match="over nixl .* failed .*PROC.*host copies from now on"):
        assert _gpunet.fetch(offer, "cpu") is None
    assert len(asked) == 1
    t = time.monotonic()
    assert _gpunet.fetch(offer, "cpu") is None, "the host copy, without asking the holder again"
    assert len(asked) == 1 and time.monotonic() - t < 1
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "1")
    assert ("nccl", offer["addr"]) not in _gpunet._failed, "only the transport that failed is given up"


def test_a_nixl_read_waits_about_as_long_as_its_size_needs():
    assert _gpunet._nixl_wait(0) == _gpunet._NIXL_MIN_WAIT
    assert _gpunet._nixl_wait(1 << 30) < 10, "1 GiB: a few seconds, where the host copy takes about one"
    assert _gpunet._nixl_wait(1 << 40) == _gpunet.TIMEOUT.total_seconds(), "never past TIMEOUT"


def test_a_failed_nixl_read_still_unregisters_its_buffer(monkeypatch):
    torch = pytest.importorskip("torch")
    calls = []

    class Agent:  # a NIXL agent whose holder is already known, and cannot then be removed
        name = "reader"

        def register_memory(self, tensors):
            calls.append("register")
            return "registration"

        def add_remote_agent(self, meta):
            calls.append("add")
            raise RuntimeError("the holder's agent is there already")

        def remove_remote_agent(self, name):
            calls.append("remove")
            raise RuntimeError("no such agent")

        def deregister_memory(self, registration):
            calls.append("deregister")

    monkeypatch.setattr(_gpunet, "_agent", Agent)
    reply = {"shape": [4], "dtype": "float32", "meta": "00", "descs": "00", "name": "holder"}
    with pytest.raises(RuntimeError, match="there already"):
        _gpunet._read_nixl(reply, torch.device("cpu"))
    assert calls == ["register", "add", "remove", "deregister"]
