"""The GPU-to-GPU path for CUDA tensors read on another machine (_gpunet):
when it is on, and that every failure leaves the reader with None, so it
reads the host copy. The transfer itself needs two machines with GPUs; it is
measured in bench/objects/RESULTS.md."""

from __future__ import annotations

import collections
import json
import os
import socket
import struct
import threading
import time
import weakref

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


class _Clock:
    """_gpunet's time, moved by hand."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        time.sleep(s)


def test_a_read_that_fails_once_begun_is_not_tried_again_from_that_holder_for_a_while(monkeypatch):
    # Given up for 30 s, then asked again; failing again, for twice as long;
    # once a read works, a later failure starts again at 30 s. A holder whose
    # transport got working again is not shut out for the life of the process.
    torch = pytest.importorskip("torch")
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "nixl")
    monkeypatch.setattr(_gpunet, "_failed", {})
    monkeypatch.setattr(_gpunet, "_warned", set())
    clock = _Clock()
    monkeypatch.setattr(_gpunet, "time", clock)
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)  # no GPU needed to get this far
    asked, works = [], []

    def send(conn, t, token, put, raw):  # a holder that offers it over NIXL
        asked.append(token)
        _gpunet._send_msg(conn, {"ok": True, "shape": [4], "dtype": "float32", "descs": "", "meta": "", "name": "h"})
        _gpunet._recv_msg(conn)

    def read(reply, out):
        if not works:
            raise RuntimeError("the NIXL read ended in PROC")
        out.fill_(7)
        return out

    monkeypatch.setattr(_gpunet, "_send_nixl", send)
    monkeypatch.setattr(_gpunet, "_read_nixl", read)
    held = torch.ones(4)
    offer = _gpunet.offer(held)
    with pytest.warns(RuntimeWarning, match="over nixl .* failed .*PROC.*host copies for 30 s, then tries it again"):
        assert _gpunet.fetch(offer, "cpu") is None
    assert len(asked) == 1
    assert _gpunet.fetch(offer, "cpu") is None, "the host copy, without asking the holder again"
    assert len(asked) == 1
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "1")
    assert ("nccl", offer["addr"]) not in _gpunet._failed, "only the transport that failed is given up"
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "nixl")
    clock.now += 31
    assert _gpunet.fetch(offer, "cpu") is None and len(asked) == 2, "asked again once its time is up"
    assert _gpunet._failed[("nixl", offer["addr"])][1] == 60, "and given up for twice as long"
    clock.now += 61
    works.append(True)
    assert _gpunet.fetch(offer, "cpu").tolist() == [7.0] * 4 and len(asked) == 3
    assert ("nixl", offer["addr"]) not in _gpunet._failed, "working again: forgotten"


def test_a_failure_to_make_room_here_does_not_give_the_holder_up(monkeypatch):
    # The reader's own GPU is full: not the holder's or the transport's fault.
    torch = pytest.importorskip("torch")
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "nixl")
    monkeypatch.setattr(_gpunet, "_failed", {})
    monkeypatch.setattr(torch.cuda, "set_device", lambda device: None)

    def send(conn, t, token, *put_and_raw):
        _gpunet._send_msg(conn, {"ok": True, "shape": [4], "dtype": "float32", "descs": "", "meta": "", "name": "h"})
        _gpunet._recv_msg(conn)

    def full(*args, **kwargs):
        raise RuntimeError("CUDA out of memory")

    monkeypatch.setattr(_gpunet, "_send_nixl", send)
    monkeypatch.setattr(torch, "empty", full)
    held = torch.ones(4)
    assert _gpunet.fetch(_gpunet.offer(held), "cpu") is None
    assert not _gpunet._failed


def test_metadata_past_the_old_reply_bound_is_read_raw():
    # NIXL's agent metadata grows with what the holder has registered. Sent as
    # hex inside the JSON, 2 MiB of it passed the 4 MiB reply bound; sent raw
    # after the JSON, it is read up to _REPLY_RAW_MAX, and more is refused
    # before it is read.
    a, b = socket.socketpair()
    with a, b:
        for size in (1 << 20, 2 << 20, 8 << 20):
            meta = os.urandom(size)
            writer = threading.Thread(target=_gpunet._send_msg, args=(a, {"ok": True}, {"meta": meta, "descs": b"d"}))
            writer.start()
            got = _gpunet._recv_msg(b, _gpunet._REPLY_MAX, _gpunet._REPLY_RAW_MAX)
            writer.join()
            assert got == {"ok": True, "meta": meta, "descs": b"d"}
        a.sendall(struct.pack("<I", 2) + b"{}")
        assert _gpunet._recv_msg(b) == {}, "a message without raw fields reads as before"
        head = json.dumps({"raw": {"meta": _gpunet._REPLY_RAW_MAX + 1}}).encode()
        a.sendall(struct.pack("<I", len(head)) + head)
        r = _Recording(b)
        with pytest.raises(ConnectionError, match="above"):
            _gpunet._recv_msg(r, _gpunet._REPLY_MAX, _gpunet._REPLY_RAW_MAX)
        assert r.asked == [4, len(head)], "refused before the raw bytes were read"
        head = json.dumps({"raw": {"meta": 4}}).encode()
        a.sendall(struct.pack("<I", len(head)) + head)
        with pytest.raises(ConnectionError, match="above the 0"):
            _gpunet._recv_msg(b)  # the listener's requests carry none


def test_a_reply_too_large_to_read_gives_the_holder_up_for_a_while(monkeypatch):
    # The reader asked and the holder answered with more than it reads: the
    # next reads must not pay the holder's sync and registration again.
    torch = pytest.importorskip("torch")
    monkeypatch.setenv("CHIMAERA_CUDA_NET", "nixl")
    monkeypatch.setattr(_gpunet, "_failed", {})
    monkeypatch.setattr(_gpunet, "_warned", set())
    monkeypatch.setattr(_gpunet, "_REPLY_RAW_MAX", 1 << 10)
    asked = []

    def send(conn, t, token, put, raw):
        asked.append(raw)
        _gpunet._send_msg(conn, {"ok": True, "shape": [4], "dtype": "float32", "name": "h"}, {"meta": bytes(2 << 10)})

    monkeypatch.setattr(_gpunet, "_send_nixl", send)
    held = torch.ones(4)
    offer = _gpunet.offer(held)
    with pytest.warns(RuntimeWarning, match="raw fields of 2048 bytes, above the 1024"):
        assert _gpunet.fetch(offer, "cpu") is None
    assert _gpunet.fetch(offer, "cpu") is None
    assert asked == [True], "asked once, for raw fields"


def test_a_nixl_read_waits_about_as_long_as_its_size_needs():
    assert _gpunet._nixl_wait(0) == _gpunet._NIXL_MIN_WAIT
    assert _gpunet._nixl_wait(1 << 30) < 10, "1 GiB: a few seconds, where the host copy takes about one"
    assert _gpunet._nixl_wait(1 << 40) == _gpunet.TIMEOUT.total_seconds(), "never past TIMEOUT"


def test_a_failed_nixl_read_still_unregisters_its_buffer(monkeypatch):
    torch = pytest.importorskip("torch")
    monkeypatch.setattr(_gpunet, "_nixl_loaded", collections.OrderedDict())
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
        _gpunet._read_nixl(reply, torch.ones(4))
    assert calls == ["register", "add", "remove", "deregister"]
    assert not _gpunet._nixl_loaded, "loaded anew by the next read"


class _Fabric:
    """What NIXL agents reach: each registered tensor, by an id its
    descriptors carry. A read copies from it once its transfer is let go."""

    def __init__(self) -> None:
        self.regions: dict[int, object] = {}
        self.go: dict[str, threading.Event] = {}  # holder name -> lets its transfers finish


class _Agent:
    """A NIXL agent over a _Fabric, counting its calls."""

    def __init__(self, fabric: _Fabric, name: str) -> None:
        self.fabric, self.name, self.calls = fabric, name, collections.Counter()
        self.mine: dict[int, object] = {}  # registration id -> tensor
        self.loaded: set[str] = set()
        self.next = 0

    def register_memory(self, tensors):
        self.calls["register"] += 1
        self.next += 1
        rid = (id(self) << 20) + self.next
        self.mine[rid] = self.fabric.regions[rid] = weakref.ref(tensors[0])  # NIXL keeps no Python reference
        return rid

    def deregister_memory(self, rid):
        self.calls["deregister"] += 1
        del self.mine[rid], self.fabric.regions[rid]

    def get_xfer_descs(self, tensors):
        return next(rid for rid, t in self.mine.items() if t() is tensors[0])

    def get_serialized_descs(self, rid):
        return str(rid).encode()

    def deserialize_descs(self, data):
        return int(data)

    def get_agent_metadata(self):  # grows with what is registered
        return json.dumps([self.name, sorted(self.mine)]).encode()

    def add_remote_agent(self, meta):
        self.calls["add"] += 1
        name = json.loads(meta)[0]
        assert name not in self.loaded, "loaded twice"
        self.loaded.add(name)
        return name

    def remove_remote_agent(self, name):
        self.calls["remove"] += 1
        self.loaded.remove(name)

    def initialize_xfer(self, op, local, remote, name, note):
        assert name in self.loaded
        return [local, remote, name]

    def transfer(self, handle):
        return self.check_xfer_state(handle)

    def check_xfer_state(self, handle):
        local, remote, name = handle
        if not self.fabric.go.setdefault(name, threading.Event()).is_set():
            return "PROC"
        self.mine[local]().copy_(self.fabric.regions[remote]())
        return "DONE"

    def release_xfer_handle(self, handle):
        self.calls["release"] += 1


class _Put:
    """The event a put recorded: a holder waits for it, not for the device."""

    def __init__(self) -> None:
        self.waited = 0

    def synchronize(self) -> None:
        self.waited += 1


def _nixl_pair(monkeypatch):
    """A holder agent and a reader agent in this process, with _gpunet's
    holder and reader state fresh, and no device-wide waits allowed."""
    torch = pytest.importorskip("torch")
    fabric = _Fabric()
    holder, reader = _Agent(fabric, "holder"), _Agent(fabric, "reader")
    for name, value in [
        ("_nixl_held", {}),
        ("_nixl_idle", collections.OrderedDict()),
        ("_nixl_dead", collections.deque()),
        ("_nixl_loaded", collections.OrderedDict()),
        ("_nixl_holders", {}),
    ]:
        monkeypatch.setattr(_gpunet, name, value)

    def device_wide(device=None):
        raise AssertionError("waited for the whole device")

    monkeypatch.setattr(torch.cuda, "synchronize", device_wide)
    return torch, fabric, holder, reader


def _read(monkeypatch, holder, reader, t, raw=True):
    """One read of `t`: the holder serves it on one end of a socket pair, the
    reader asks and reads it as fetch does."""
    a, b = socket.socketpair()
    put = _Put()
    with a, b:
        monkeypatch.setattr(_gpunet, "_agent", lambda: holder)
        serving = threading.Thread(target=_gpunet._send_nixl, args=(a, t, id(t).to_bytes(8, "little"), put, raw))
        serving.start()
        reply = _gpunet._recv_msg(b, _gpunet._REPLY_MAX, _gpunet._REPLY_RAW_MAX)
        monkeypatch.setattr(_gpunet, "_agent", lambda: reader)
        out = _gpunet._read_nixl(reply, t.new_zeros(t.shape))
        _gpunet._send_msg(b, {"done": True})
        serving.join(10)
    assert put.waited == 1
    return out


def test_a_tensor_read_again_is_registered_once_and_its_holder_loaded_once(monkeypatch):
    torch, fabric, holder, reader = _nixl_pair(monkeypatch)
    fabric.go["holder"] = threading.Event()
    fabric.go["holder"].set()
    t = torch.arange(8.0)
    for _ in range(3):
        assert _read(monkeypatch, holder, reader, t).tolist() == t.tolist()
    assert holder.calls["register"] == 1 and holder.calls["deregister"] == 0, "kept registered between readers"
    assert reader.calls["add"] == 1 and reader.calls["remove"] == 0, "the holder's metadata did not change"
    assert reader.calls["register"] == reader.calls["deregister"] == 3, "each read's own buffer is let go"
    u = torch.arange(4.0)  # the holder registers more: its metadata changes, and is loaded anew
    assert _read(monkeypatch, holder, reader, u).tolist() == u.tolist()
    assert reader.calls["add"] == 2 and reader.calls["remove"] == 1
    old = _read(monkeypatch, holder, reader, t, raw=False)  # a reader from before raw fields gets hex
    assert old.tolist() == t.tolist()


def test_a_freed_tensor_is_deregistered_and_idle_registrations_are_bounded(monkeypatch):
    import gc

    torch, fabric, holder, reader = _nixl_pair(monkeypatch)
    fabric.go["holder"] = threading.Event()
    fabric.go["holder"].set()
    monkeypatch.setattr(_gpunet, "_NIXL_IDLE", 3)
    t = torch.arange(8.0)
    _read(monkeypatch, holder, reader, t)
    del t
    gc.collect()
    keep = [torch.full((4,), float(i)) for i in range(5)]
    for k in keep:
        assert _read(monkeypatch, holder, reader, k).tolist() == k.tolist()
    assert holder.calls["register"] == 6
    assert holder.calls["deregister"] == 3, "the freed one at the next send, then the two past the bound"
    assert list(_gpunet._nixl_idle) == [id(k).to_bytes(8, "little") for k in keep[2:]]


@pytest.mark.timeout(60)
def test_reads_from_different_holders_overlap_and_from_one_holder_take_turns(monkeypatch):
    torch, fabric, first, reader = _nixl_pair(monkeypatch)
    second = _Agent(fabric, "second")
    for name in ("holder", "second"):
        fabric.go[name] = threading.Event()
    slow, quick, again = torch.arange(4.0), torch.arange(6.0), torch.arange(5.0)
    replies = {}
    for agent, t in [(first, slow), (second, quick), (first, again)]:
        rid = agent.register_memory([t])
        replies[id(t)] = {"name": agent.name, "meta": agent.get_agent_metadata(), "descs": str(rid).encode()}
    monkeypatch.setattr(_gpunet, "_agent", lambda: reader)
    done = []

    def read(t):
        _gpunet._read_nixl(replies[id(t)], t.new_zeros(t.shape))
        done.append(id(t))

    readers = [threading.Thread(target=read, args=(t,)) for t in (slow, again)]
    readers[0].start()
    time.sleep(0.2)
    readers[1].start()  # waits behind the first: both from `holder`
    time.sleep(0.2)
    fabric.go["second"].set()
    read(quick)  # another holder: not held up by `holder`'s read in flight
    assert done == [id(quick)]
    fabric.go["holder"].set()
    for r in readers:
        r.join(30)
    assert done == [id(quick), id(slow), id(again)]
