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
replicated like any value: a dead holder costs speed, never the value. A
transfer that fails once it has begun is not tried again from that holder
for a while, longer each time (see `_give_up`): a transport that cannot work
there would otherwise hold every read until it timed out.
It is off unless ``CHIMAERA_CUDA_NET`` turns it on; see `enabled`.

``CHIMAERA_CUDA_NET=nixl`` sends it with NIXL instead, as Ray Direct
Transport's ``tensor_transport="nixl"`` does: the holder registers the
tensor's memory and sends the reader where it is, and the reader reads it
one-sided (UCX, or LIBFABRIC over EFA), with no group to set up. Both sides
need the ``nixl`` package; a holder without it leaves the reader the host
copy. A holder keeps a tensor registered between readers, and a reader keeps
a holder's agent loaded while its metadata is unchanged, so a tensor read
again costs only the transfer. Reads from different holders overlap.

A holder sends a tensor once the writes queued on the stream that put it
are done (an event recorded at the put), not after the whole device is idle.
Like a same-machine reader, it reads the tensor as it is then: one written
to after its put is sent as written.

The token in a value is the capability to read it, and the listener
answers nothing else; neither it nor NCCL's store authenticates or encrypts.
It listens on all interfaces: turn it on only on a trusted cluster network.
"""

from __future__ import annotations

import collections
import glob
import json
import os
import secrets
import socket
import struct
import threading
import time
import warnings
import weakref
from datetime import timedelta
from typing import Any

TIMEOUT = timedelta(seconds=60)

_lock = threading.Lock()
_offered: dict[bytes, tuple[Any, Any]] = {}  # token -> (weakref to the tensor, the event its put recorded or None)
_server: tuple[str, int] | None = None
_held_groups: dict[str, tuple[Any, Any, threading.Lock]] = {}  # pair id -> (store, group, lock), holder side
_read_groups: dict[tuple[str, str], tuple[str, Any, Any]] = {}  # (holder, device) -> (pair, store, group)
_nixl: Any = None  # this process's NIXL agent, made on first use
# Every call into the agent, from holder and reader threads alike: an agent made
# without a listener does no locking of its own.
_nixl_lock = threading.Lock()
# Holder side, under _nixl_lock. token -> [registration, readers]: a tensor stays
# registered once its readers are done (in _nixl_idle, oldest first, at most
# _NIXL_IDLE), so the next reader neither registers it again nor finds the
# agent's metadata changed; it is deregistered once freed (_nixl_dead).
_nixl_held: dict[bytes, list[Any]] = {}
_nixl_idle: collections.OrderedDict[bytes, None] = collections.OrderedDict()
_nixl_dead: collections.deque[bytes] = collections.deque()  # freed tensors' tokens, deregistered at the next send
_NIXL_IDLE = 64
# Reader side. One read at a time per holder agent: a read may load that agent's
# newer metadata (the tensor must be registered in the metadata loaded), which a
# read in flight from it must not see removed. Reads from different holders overlap.
_nixl_holders: dict[str, threading.Lock] = {}  # holder agent name -> its reads' lock, under _lock
_nixl_loaded: collections.OrderedDict[str, bytes] = collections.OrderedDict()  # name -> metadata loaded, under _nixl_lock
_NIXL_REMOTES = 256  # holder agents kept loaded past their reads; more are unloaded, least recently read first
_handlers = threading.BoundedSemaphore(64)  # requests served at once; the listener closes more
_REQUEST_MAX = 64 << 10  # what the listener reads: a token and a few fields
_REPLY_MAX = 4 << 20  # a reply's JSON: a holder from before raw fields sends NIXL's metadata in it as hex
_REPLY_RAW_MAX = 64 << 20  # a reply's raw fields: NIXL's agent metadata grows with what it has registered
# (transport, holder address) -> (monotonic time to ask it again, seconds it was given up for)
_failed: dict[tuple[str, str], tuple[float, float]] = {}
_GIVE_UP_S = (30.0, 600.0)  # first, longest: doubled at each failure in a row
_warned: set[str] = set()  # transports whose first failure was reported
_NIXL_MIN_WAIT = 5.0  # seconds any NIXL read may take; see _nixl_wait


class _Refused(ConnectionError):
    """A message this side does not read: too large, or malformed."""


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
    put = _put_event(t)
    with _lock:
        if len(_offered) > 4096:
            for k in [k for k, (r, _) in _offered.items() if r() is None]:
                del _offered[k]
        _offered[token] = (weakref.ref(t), put)
    return {"addr": f"{addr[0]}:{addr[1]}", "token": token.hex()}


def _put_event(t: Any) -> Any:
    """An event recorded now on the stream this thread uses on `t`'s device:
    once it has passed, the writes queued before the put are done. None when
    `t` is not a CUDA tensor or no event can be recorded; the holder then
    waits for the whole device before a read."""
    try:
        import torch

        if not t.is_cuda:
            return None
        event = torch.cuda.Event()
        event.record(torch.cuda.current_stream(t.device))
        return event
    except Exception:  # noqa: BLE001 -- not a CUDA tensor, or no torch: the device-wide wait
        return None


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


def _send_msg(s: socket.socket, msg: dict[str, Any], raw: dict[str, bytes] | None = None) -> None:
    """One message: its length, its JSON, then the byte strings in `raw` as
    they are, named and sized in the JSON's "raw" (not as hex inside it)."""
    if raw:
        msg = {**msg, "raw": {k: len(v) for k, v in raw.items()}}
    data = json.dumps(msg).encode()
    s.sendall(struct.pack("<I", len(data)) + data)
    for v in (raw or {}).values():
        s.sendall(v)


def _recv_msg(s: socket.socket, limit: int = _REQUEST_MAX, raw_limit: int = 0) -> dict[str, Any]:
    """One message, with its raw fields (see _send_msg) as bytes. A JSON
    longer than `limit`, or raw fields longer than `raw_limit` in all, is
    refused before anything is read or allocated for it (the listener takes
    connections from anyone on the network)."""

    def exact(n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            got = s.recv(n - len(buf))
            if not got:
                raise ConnectionError("the other side closed")
            buf += got
        return bytes(buf)

    (n,) = struct.unpack("<I", exact(4))
    if n > limit:
        raise _Refused(f"a message of {n} bytes, above the {limit} this side reads")
    msg = json.loads(exact(n))
    sizes = msg.pop("raw", None) or {}
    if not isinstance(sizes, dict) or not all(type(v) is int and v >= 0 for v in sizes.values()):
        raise _Refused("a message whose raw fields are malformed")
    if sum(sizes.values()) > raw_limit:
        raise _Refused(f"raw fields of {sum(sizes.values())} bytes, above the {raw_limit} this side reads")
    for k, size in sizes.items():
        msg[k] = exact(size)
    return msg


def _bytes(v: str | bytes) -> bytes:
    """A reply's binary field: raw, or hex from a holder from before raw fields."""
    return v if isinstance(v, bytes) else bytes.fromhex(v)


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
            ref, put = _offered.get(token, (None, None))
            t = ref() if ref is not None else None
            if t is None:
                _send_msg(conn, {"ok": False})
                return
            if req.get("via") == "nixl":
                _send_nixl(conn, t, token, put, bool(req.get("raw_ok")))
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
                if put is not None:  # NCCL's stream waits for this thread's, which waits for the put's writes
                    torch.cuda.current_stream(t.device).wait_event(put)
                group.send([t], 1, 0).wait()
            _recv_msg(conn)  # the reader has it
        except Exception:  # noqa: BLE001 -- the reader falls back to the host copy
            with _lock:
                _held_groups.pop(pair, None)  # a broken pair is made anew next time


def _send_nixl(conn: socket.socket, t: Any, token: bytes, put: Any, raw: bool) -> None:
    """Let one reader read `t` with NIXL: register its memory (once, kept
    for later readers; see _nixl_held), send where it is, and keep it
    registered at least until the reader says it has it or goes. `raw`: the
    reader takes binary fields raw, not as hex."""
    import torch

    agent = _agent()
    if put is not None:
        put.synchronize()  # the writes queued before the put, not the whole device
    else:
        torch.cuda.synchronize(t.device)
    with _nixl_lock:
        while _nixl_dead:
            _deregister(agent, _nixl_dead.popleft())
        held = _nixl_held.get(token)
        if held is None:
            held = _nixl_held[token] = [agent.register_memory([t]), 0]
            weakref.finalize(t, _nixl_dead.append, token)  # no agent call here: the next send makes it
        _nixl_idle.pop(token, None)
        held[1] += 1
    try:
        with _nixl_lock:
            descs, meta = agent.get_serialized_descs(agent.get_xfer_descs([t])), agent.get_agent_metadata()
        head = {"ok": True, "shape": list(t.shape), "dtype": str(t.dtype).split(".")[-1], "name": agent.name}
        if raw:
            _send_msg(conn, head, {"descs": descs, "meta": meta})
        else:  # a reader from before raw fields
            _send_msg(conn, {**head, "descs": descs.hex(), "meta": meta.hex()})
        _recv_msg(conn)  # the reader has it
    finally:
        with _nixl_lock:
            held[1] -= 1
            if held[1] == 0 and _nixl_held.get(token) is held:
                _nixl_idle[token] = None
                while len(_nixl_idle) > _NIXL_IDLE:
                    _deregister(agent, _nixl_idle.popitem(last=False)[0])


def _deregister(agent: Any, token: bytes) -> None:
    """Drop the registration of `token`'s tensor unless a reader is reading
    it (under _nixl_lock)."""
    held = _nixl_held.get(token)
    if held is not None and held[1] == 0:
        del _nixl_held[token]
        _nixl_idle.pop(token, None)
        _quietly(agent.deregister_memory, held[0])


def _nixl_wait(nbytes: int) -> float:
    """Seconds a NIXL read of `nbytes` may take before it counts as failed:
    a few, plus the time at 500 MB/s. The host copy of 1 GiB takes about a
    second, so a read slower than this is not working; waiting for TIMEOUT
    instead held each read for a minute where reads never completed."""
    return min(TIMEOUT.total_seconds(), _NIXL_MIN_WAIT + nbytes / 500e6)


def _give_up(via: str, holder: str, e: BaseException) -> None:
    """A transfer from `holder` over `via` failed after it began, or its reply
    could not be read: read host copies from it for a while, twice as long as
    the last time in a row (_GIVE_UP_S), and say so the first time for each
    transport. A holder whose transport got working again is not shut out
    for the life of the process."""
    with _lock:
        _, last = _failed.get((via, holder), (0.0, 0.0))
        rest = min(_GIVE_UP_S[1], last * 2) if last else _GIVE_UP_S[0]
        _failed[(via, holder)] = (time.monotonic() + rest, rest)
        first = via not in _warned
        _warned.add(via)
    if first:
        warnings.warn(
            f"CHIMAERA_CUDA_NET: a GPU-to-GPU read over {via} from {holder} failed ({type(e).__name__}: {e}); "
            f"this process reads that holder's host copies for {rest:.0f} s, then tries it again",
            RuntimeWarning,
            stacklevel=3,
        )


def _resting(via: str, holder: str) -> bool:
    """Whether reads from `holder` over `via` are given up for now (_give_up)."""
    with _lock:
        given = _failed.get((via, holder))
        return given is not None and time.monotonic() < given[0]


def _read_nixl(reply: dict[str, Any], out: Any) -> Any:
    """Read the tensor `reply` describes from its holder's GPU into `out`,
    one read at a time from that holder (see _nixl_holders)."""
    agent, remote = _agent(), reply["name"]
    with _lock:
        holder = _nixl_holders.setdefault(remote, threading.Lock())
    with holder:
        _read_from(agent, remote, reply, out)
    _unload_idle(agent)
    return out


def _read_from(agent: Any, remote: str, reply: dict[str, Any], out: Any) -> None:
    meta, handle, done = _bytes(reply["meta"]), None, False
    with _nixl_lock:
        mine = agent.register_memory([out])
    try:
        with _nixl_lock:
            if _nixl_loaded.get(remote) != meta:  # first read from it, or it registered more since
                if _nixl_loaded.pop(remote, None) is not None:
                    agent.remove_remote_agent(remote)
                _nixl_loaded[remote] = meta  # unloaded below if adding it fails (it may be part there)
                agent.add_remote_agent(meta)
            _nixl_loaded.move_to_end(remote)
            theirs = agent.deserialize_descs(_bytes(reply["descs"]))
            handle = agent.initialize_xfer("READ", agent.get_xfer_descs([out]), theirs, remote, b"chimaera")
            state = agent.transfer(handle)
        deadline = time.monotonic() + _nixl_wait(out.numel() * out.element_size())
        while state != "DONE":
            if state == "ERR" or time.monotonic() > deadline:
                raise RuntimeError(f"the NIXL read ended in {state}")
            time.sleep(0.0005)
            with _nixl_lock:
                state = agent.check_xfer_state(handle)
        done = True
    finally:
        # Each step on its own: one that fails must not leave `out` registered.
        with _nixl_lock:
            if handle is not None:
                _quietly(agent.release_xfer_handle, handle)
            if not done and _nixl_loaded.pop(remote, None) is not None:
                _quietly(agent.remove_remote_agent, remote)  # loaded anew by the next read
            _quietly(agent.deregister_memory, mine)


def _unload_idle(agent: Any) -> None:
    """Unload the holder agents read longest ago, past _NIXL_REMOTES, that
    no read is using."""
    with _nixl_lock:
        names = list(_nixl_loaded)[: max(0, len(_nixl_loaded) - _NIXL_REMOTES)]
    for name in names:
        with _lock:
            holder = _nixl_holders.get(name)
        if holder is None or not holder.acquire(blocking=False):
            continue
        try:
            with _nixl_lock:
                if _nixl_loaded.pop(name, None) is not None:
                    _quietly(agent.remove_remote_agent, name)
        finally:
            holder.release()


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
    via, holder = _via(), net.get("addr", "")
    if _resting(via, holder):
        return None
    begun = False  # past room for it here: a failure from here on is the transport's
    try:
        import torch
        import torch.distributed as dist

        host, port = net["addr"].rsplit(":", 1)
        key = (net["addr"], target)
        with _lock:
            known = _read_groups.get(key)
        pair = known[0] if known is not None else secrets.token_hex(8)
        with socket.create_connection((host, int(port)), timeout=10) as s:
            s.settimeout(TIMEOUT.total_seconds())
            _send_msg(s, {"token": net["token"], "pair": pair, "via": via, "raw_ok": True})
            reply = _recv_msg(s, _REPLY_MAX, _REPLY_RAW_MAX)
            if not reply.get("ok"):
                return None
            device = torch.device(target)
            torch.cuda.set_device(device)
            out = torch.empty(reply["shape"], dtype=getattr(torch, reply["dtype"]), device=device)
            begun = True
            if via == "nixl":
                _read_nixl(reply, out)
            else:
                if "store" in reply or known is None:  # a new pair (or the holder lost ours)
                    shost, sport = reply["store"].rsplit(":", 1)
                    store = dist.TCPStore(shost, int(sport), 2, False, TIMEOUT)
                    known = (pair, store, _group(store, pair, 1))
                    with _lock:
                        _read_groups[key] = known
                known[2].recv([out], 0, 0).wait()
            _send_msg(s, {"done": True})
        with _lock:
            _failed.pop((via, holder), None)  # working again: a later failure is given up for the shortest time
        return out
    except Exception as e:  # noqa: BLE001 -- any failure: the host copy
        with _lock:
            _read_groups.pop((holder, target), None)
        # The transport's failure, or a reply this side cannot read; not a lack
        # of room here, nor a holder that is gone or too busy to answer.
        if begun or isinstance(e, _Refused):
            _give_up(via, holder, e)
        return None
