"""CUDA tensors read on another machine, GPU to GPU over NCCL.

The process that ``ch.put`` a CUDA tensor keeps it alive (the put's future
holds it, as for same-machine CUDA IPC) and offers it here under a random
token. A reader on another machine, in a call granted a GPU, connects to the
holder, and the two send it GPU to GPU through a two-rank NCCL group made for
that pair and kept for their later transfers. NCCL pipelines the copy
through its own pinned buffers, so no whole host copy is fetched and copied
up to the reader's GPU.

Anything that fails here (the holder gone, no NCCL, a timeout) returns None,
and the reader reads the host copy the put stored as well, which is
replicated like any value: a dead holder costs speed, never the value.
It is off unless ``CHIMAERA_CUDA_NET`` turns it on; see `enabled`.

``CHIMAERA_CUDA_NET=nixl`` sends it with NIXL instead, as Ray Direct
Transport's ``tensor_transport="nixl"`` does: the holder registers the
tensor's memory and sends the reader where it is, and the reader reads it
one-sided (UCX, or LIBFABRIC over EFA), with no group to set up. Both sides
need the ``nixl`` package; a holder without it leaves the reader the host
copy.

The token in a value is the capability to read it, and the listener
answers nothing else; neither it nor NCCL's store authenticates or encrypts.
It listens on all interfaces: turn it on only on a trusted cluster network.
"""

from __future__ import annotations

import glob
import json
import os
import secrets
import socket
import struct
import threading
import time
import weakref
from datetime import timedelta
from typing import Any

TIMEOUT = timedelta(seconds=60)

_lock = threading.Lock()
_offered: dict[bytes, Any] = {}  # token -> weakref to the tensor
_server: tuple[str, int] | None = None
_held_groups: dict[str, tuple[Any, Any, threading.Lock]] = {}  # pair id -> (store, group, lock), holder side
_read_groups: dict[tuple[str, str], tuple[str, Any, Any]] = {}  # (holder, device) -> (pair, store, group)
_nixl: Any = None  # this process's NIXL agent, made on first use
_nixl_held: dict[bytes, list[Any]] = {}  # token -> [registration, readers], holder side
_nixl_lock = threading.Lock()  # every call into the agent, from holder and reader threads alike
_nixl_reads = threading.Lock()  # one read at a time adds and removes its holder's agent
_handlers = threading.BoundedSemaphore(64)  # requests served at once; the listener closes more
_REQUEST_MAX = 64 << 10  # what the listener reads: a token and a few fields
_REPLY_MAX = 4 << 20  # a holder's reply: NIXL's agent metadata grows with what it has registered


def enabled() -> bool:
    """On only when ``CHIMAERA_CUDA_NET`` is ``1`` (NCCL) or ``nixl`` (NIXL),
    set by a user on a trusted cluster network (see the module's note), RDMA
    or not. On plain TCP both were slower than the host copy: 1 GiB between
    two 10 Gb/s g5 nodes took 1.9 s over NCCL and 3.0 s over NIXL, against
    1.2 s fetching the host copy and copying it up to the GPU."""
    return os.environ.get("CHIMAERA_CUDA_NET", "") in ("1", "nixl")


def _via() -> str:
    return "nixl" if os.environ.get("CHIMAERA_CUDA_NET") == "nixl" else "nccl"


def _efa_present() -> bool:
    """Whether this machine has an AWS EFA device (a netdev, or in a
    container an RDMA device bound to the efa driver), as Ray checks."""
    if glob.glob("/sys/class/net/efa*"):
        return True
    devices = glob.glob("/sys/class/infiniband/*")
    return any(os.path.basename(os.path.realpath(os.path.join(d, "device", "driver"))) == "efa" for d in devices)


def _agent() -> Any:
    """This process's NIXL agent: LIBFABRIC over EFA, UCX elsewhere."""
    global _nixl
    with _lock:
        if _nixl is None:
            from nixl._api import nixl_agent, nixl_agent_config

            backend = "LIBFABRIC" if _efa_present() else "UCX"
            _nixl = nixl_agent(f"chimaera-{secrets.token_hex(8)}", nixl_agent_config(backends=[backend]))
        return _nixl


def _my_ip() -> str:
    """This machine's address on its default route (no packet is sent)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())
    finally:
        s.close()


def offer(t: Any) -> dict[str, str] | None:
    """Make `t`, a contiguous CUDA tensor its caller keeps alive, readable by
    other machines: {"addr", "token"}, or None when this cannot serve."""
    if not enabled():
        return None
    try:
        addr = _serve()
    except OSError:
        return None
    token = secrets.token_bytes(16)
    with _lock:
        if len(_offered) > 4096:
            for k in [k for k, r in _offered.items() if r() is None]:
                del _offered[k]
        _offered[token] = weakref.ref(t)
    return {"addr": f"{addr[0]}:{addr[1]}", "token": token.hex()}


def _serve() -> tuple[str, int]:
    global _server
    with _lock:
        if _server is None:
            ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ls.bind(("0.0.0.0", 0))
            ls.listen(64)
            _server = (_my_ip(), ls.getsockname()[1])
            threading.Thread(target=_accept, args=(ls,), name="chimaera-gpunet", daemon=True).start()
        return _server


def _accept(ls: socket.socket) -> None:
    while True:
        conn, _ = ls.accept()
        slots = _handlers
        if not slots.acquire(blocking=False):
            conn.close()  # its reader reads the host copy
            continue
        threading.Thread(target=_serve_one, args=(conn, slots), name="chimaera-gpunet-send", daemon=True).start()


def _serve_one(conn: socket.socket, slots: threading.BoundedSemaphore) -> None:
    try:
        _handle(conn)
    finally:
        slots.release()


def _send_msg(s: socket.socket, msg: dict[str, Any]) -> None:
    data = json.dumps(msg).encode()
    s.sendall(struct.pack("<I", len(data)) + data)


def _recv_msg(s: socket.socket, limit: int = _REQUEST_MAX) -> dict[str, Any]:
    """One message; a length above `limit` is refused before anything is
    read or allocated for it (the listener takes connections from anyone on
    the network)."""

    def exact(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            got = s.recv(n - len(buf))
            if not got:
                raise ConnectionError("the other side closed")
            buf += got
        return buf

    (n,) = struct.unpack("<I", exact(4))
    if n > limit:
        raise ConnectionError(f"a message of {n} bytes, above the {limit} this side reads")
    return json.loads(exact(n))


def _group(store: Any, pair: str, rank: int) -> Any:
    import torch.distributed as dist

    prefixed = dist.PrefixStore(pair, store)
    try:
        opts = dist.ProcessGroupNCCL.Options()
        opts._timeout = TIMEOUT
        return dist.ProcessGroupNCCL(prefixed, rank, 2, opts)
    except (AttributeError, TypeError):  # older torch: (store, rank, size, timeout)
        return dist.ProcessGroupNCCL(prefixed, rank, 2, TIMEOUT)


def _handle(conn: socket.socket) -> None:
    """One request: send the tensor named by its token to the reader."""
    pair = ""
    with conn:
        conn.settimeout(TIMEOUT.total_seconds())
        try:
            req = _recv_msg(conn)
            token = bytes.fromhex(req["token"])
            ref = _offered.get(token)
            t = ref() if ref is not None else None
            if t is None:
                _send_msg(conn, {"ok": False})
                return
            if req.get("via") == "nixl":
                _send_nixl(conn, t, token)
                return
            import torch
            import torch.distributed as dist

            pair = str(req["pair"])
            with _lock:
                held = _held_groups.get(pair)
            reply: dict[str, Any] = {"ok": True, "shape": list(t.shape), "dtype": str(t.dtype).split(".")[-1]}
            if held is None:
                store = dist.TCPStore("0.0.0.0", 0, 2, True, TIMEOUT, wait_for_workers=False)
                reply["store"] = f"{_my_ip()}:{store.port}"
            _send_msg(conn, reply)
            if held is None:
                torch.cuda.set_device(t.device)
                held = (store, _group(store, pair, 0), threading.Lock())
                with _lock:
                    _held_groups[pair] = held
            _, group, pair_lock = held
            with pair_lock:
                torch.cuda.set_device(t.device)
                group.send([t], 1, 0).wait()
            _recv_msg(conn)  # the reader has it
        except Exception:  # noqa: BLE001 -- the reader falls back to the host copy
            with _lock:
                _held_groups.pop(pair, None)  # a broken pair is made anew next time


def _send_nixl(conn: socket.socket, t: Any, token: bytes) -> None:
    """Let one reader read `t` with NIXL: register its memory (once for
    readers at the same time), send where it is, and keep it registered
    until the reader says it has it or goes."""
    import torch

    agent = _agent()
    torch.cuda.synchronize(t.device)
    with _nixl_lock:
        held = _nixl_held.get(token)
        if held is None:
            held = _nixl_held[token] = [agent.register_memory([t]), 0]
        held[1] += 1
    try:
        with _nixl_lock:
            descs, meta = agent.get_serialized_descs(agent.get_xfer_descs([t])), agent.get_agent_metadata()
        where = {"descs": descs.hex(), "meta": meta.hex(), "name": agent.name}
        _send_msg(conn, {"ok": True, "shape": list(t.shape), "dtype": str(t.dtype).split(".")[-1], **where})
        _recv_msg(conn)  # the reader has it
    finally:
        with _nixl_lock:
            held[1] -= 1
            if held[1] == 0:
                del _nixl_held[token]
                agent.deregister_memory(held[0])


def _read_nixl(reply: dict[str, Any], device: Any) -> Any:
    """Read the tensor `reply` describes from its holder's GPU onto `device`."""
    import torch

    agent = _agent()
    out = torch.empty(reply["shape"], dtype=getattr(torch, reply["dtype"]), device=device)
    with _nixl_lock:
        mine = agent.register_memory([out])
    handle = remote = None
    try:
        with _nixl_lock:
            remote = reply["name"]  # removed below even if adding it fails (it may be there from before)
            agent.add_remote_agent(bytes.fromhex(reply["meta"]))
            theirs = agent.deserialize_descs(bytes.fromhex(reply["descs"]))
            handle = agent.initialize_xfer("READ", agent.get_xfer_descs([out]), theirs, remote, b"chimaera")
            state = agent.transfer(handle)
        deadline = time.monotonic() + TIMEOUT.total_seconds()
        while state != "DONE":
            if state == "ERR" or time.monotonic() > deadline:
                raise RuntimeError(f"the NIXL read ended in {state}")
            time.sleep(0.0005)
            with _nixl_lock:
                state = agent.check_xfer_state(handle)
        return out
    finally:
        # Each step on its own: one that fails must not leave `out` registered.
        with _nixl_lock:
            if handle is not None:
                _quietly(agent.release_xfer_handle, handle)
            if remote is not None:
                _quietly(agent.remove_remote_agent, remote)
            _quietly(agent.deregister_memory, mine)


def _quietly(f: Any, *args: Any) -> None:
    try:
        f(*args)
    except Exception:  # noqa: BLE001, S110 -- cleanup after a read; the caller already has its outcome
        pass


def fetch(net: dict[str, str], target: str) -> Any:
    """The tensor `net` offers, sent from its holder's GPU onto `target` (a
    CUDA device of this process), or None (the caller reads the host copy)."""
    if not enabled():
        return None
    try:
        import torch
        import torch.distributed as dist

        host, port = net["addr"].rsplit(":", 1)
        key = (net["addr"], target)
        with _lock:
            known = _read_groups.get(key)
        pair = known[0] if known is not None else secrets.token_hex(8)
        via = _via()
        with socket.create_connection((host, int(port)), timeout=10) as s:
            s.settimeout(TIMEOUT.total_seconds())
            _send_msg(s, {"token": net["token"], "pair": pair, "via": via})
            reply = _recv_msg(s, _REPLY_MAX)
            if not reply.get("ok"):
                return None
            device = torch.device(target)
            torch.cuda.set_device(device)
            if via == "nixl":
                with _nixl_reads:
                    out = _read_nixl(reply, device)
                _send_msg(s, {"done": True})
                return out
            if "store" in reply or known is None:  # a new pair (or the holder lost ours)
                shost, sport = reply["store"].rsplit(":", 1)
                store = dist.TCPStore(shost, int(sport), 2, False, TIMEOUT)
                known = (pair, store, _group(store, pair, 1))
                with _lock:
                    _read_groups[key] = known
            out = torch.empty(reply["shape"], dtype=getattr(torch, reply["dtype"]), device=device)
            known[2].recv([out], 0, 0).wait()
            _send_msg(s, {"done": True})
            return out
    except Exception:  # noqa: BLE001 -- any failure: the host copy
        with _lock:
            _read_groups.pop((net.get("addr", ""), target), None)
        return None
