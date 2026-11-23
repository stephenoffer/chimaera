"""The direct actor path (docs/architecture.md, D2): on a local peer, an
eligible call to a one-at-a-time actor goes from the driver straight to the
actor's executor over a Unix socket, and its outcome comes straight back.

The peer still records every call. Once the outcome is in, the driver submits
the call with ``ran`` set: the shard stores it as done (nothing runs again),
and the peer files the executor's timings as its attempt. So ``explain()``,
``chimaera runs``, accounting and later calls that take its future all see it,
a millisecond or so after the caller did.

Which calls go direct (``eligible``): calls from a driver (not from inside a
call) on a local, attached run, to an actor that is not durable and batches no
method, with no deadline, no exception types to retry, no generator and no
arguments that wait on other calls. Everything else takes the peer as before.
The executor runs them as it runs the peer's: one at a time, or in the
actor's lanes when it has ``concurrency`` or groups.

Order: a handle's direct calls start in the order made (one connection per
actor, read in order). A handle switching paths first waits for its calls on
the other path, so calls never overtake each other. Concurrent actors promise
no order, as through the peer.

Failure: if the connection breaks (the executor died, the actor restarted or
was closed), every call still waiting on it is resubmitted through the peer
under its own key, and the actor stays on the peer path in this process. A call
whose record reached the peer is a duplicate there and does not run again; one
that ran without its outcome reaching anyone may run again, which is why only
``effects="none"`` and ``"idempotent"`` calls go direct.

The socket is ``<peer home>/direct/<actor id>.sock``, mode 0600, like the
peer's own client and executor sockets: any process of the peer's OS user may
connect, which is the boundary those already have.
"""

from __future__ import annotations

import os
import pickle
import socket
import struct
import threading
import time
from typing import Any

from ._errors import ActorDied
from ._runtime import CALL_ACTOR_METHOD

HEADER = struct.Struct(">I")
DIRECT_HANDLE = 1 << 63  # direct calls' handle ids: their own sequence, apart from the peer path's
MAX_SOCKET_PATH = 100  # sun_path is 104 bytes on macOS
MAX_CONNS = 256  # actors one process talks to directly; the rest take the peer path
EFFECTS_DIRECT = (0, 1)  # EFFECTS_CODE of "none" and "idempotent"
_counter = {"direct": 0}  # calls sent direct in this process (tests)
ENABLED = os.environ.get("CHIMAERA_DIRECT", "1") != "0"
READ_DELAY_S = 0.0  # tests: hold each direct call in the executor's reader this long


def socket_path(home: str, actor: bytes) -> str | None:
    path = os.path.join(home, "direct", actor.hex() + ".sock")
    return path if len(path.encode()) <= MAX_SOCKET_PATH else None


def _send(sock: socket.socket, lock: threading.Lock, obj: Any) -> None:
    data = pickle.dumps(obj, protocol=5)
    with lock:
        sock.sendall(HEADER.pack(len(data)) + data)


def _recv(sock: socket.socket) -> Any:
    head = _exactly(sock, HEADER.size)
    if head is None:
        return None
    body = _exactly(sock, HEADER.unpack(head)[0])
    return None if body is None else pickle.loads(body)


def _exactly(sock: socket.socket, n: int) -> bytes | None:
    buf = bytearray(n)
    view, got = memoryview(buf), 0
    while got < n:
        k = sock.recv_into(view[got:])
        if k == 0:
            return None
        got += k
    return bytes(buf)


# ---- executor side ----------------------------------------------------------


def listen(executor: Any, msg: dict[str, Any]) -> None:
    """After an actor's constructor ran: serve direct calls unless the actor
    is durable (its replies wait for committed state) or batches a method
    (its batcher collects calls the peer sends)."""
    cls = type(executor.instance)
    if msg.get("durable"):
        return
    if any((getattr(getattr(cls, n, None), "__chimaera_method__", None) or {}).get("batch") for n in dir(cls)):
        return
    path = socket_path(os.path.dirname(executor.conn.welcome["peer_socket"]), msg["actor"])
    if path is None:
        return
    os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
    try:
        os.unlink(path)  # left by an executor of this actor that died
    except FileNotFoundError:
        pass
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    old = os.umask(0o177)
    try:
        server.bind(path)
    finally:
        os.umask(old)
    server.listen(64)
    executor.direct_path = path
    threading.Thread(target=_accept, args=(executor, server, msg), name="direct-accept", daemon=True).start()


def _accept(executor: Any, server: socket.socket, ctor: dict[str, Any]) -> None:
    while True:
        try:
            sock, _ = server.accept()
        except OSError:
            return
        threading.Thread(target=_serve, args=(executor, sock, ctor), name="direct-reader", daemon=True).start()


class Reply:
    """Where a direct call's outcome goes: its caller's connection."""

    def __init__(self, sock: socket.socket, lock: threading.Lock, executor_id: int) -> None:
        self.sock, self.lock, self.executor_id = sock, lock, executor_id

    def done(self, key: bytes, outcome: tuple[Any, ...], entry: int, cpu0: int) -> None:
        cpu = time.process_time_ns() - cpu0
        try:
            _send(self.sock, self.lock, ("done", key, outcome, entry, time.time_ns(), cpu, self.executor_id))
        except OSError:
            pass  # the caller is gone: it resubmits what it was waiting for through the peer


def _serve(executor: Any, sock: socket.socket, ctor: dict[str, Any]) -> None:
    """Read one caller's calls in order and queue them for the main thread,
    as the peer's Execute messages are (the reader stays free for cancels)."""
    reply = Reply(sock, threading.Lock(), getattr(executor, "executor_id", 0))
    while True:
        try:
            m = _recv(sock)
        except (OSError, pickle.UnpicklingError, EOFError, ValueError):
            m = None
        if m is None:
            sock.close()
            return
        if m[0] == "call":
            _, key, method, args_ref, policy = m
            # Lanes follow the actor's own concurrency, which a handle found
            # by name does not know.
            policy = {**policy, "concurrency": ctor["policy"].get("concurrency") or 0}
            if READ_DELAY_S:
                time.sleep(READ_DELAY_S)
            executor.inbox.put(
                (
                    "execute",
                    {
                        "kind": CALL_ACTOR_METHOD,
                        "fn_digest": ctor["fn_digest"],
                        "method": method,
                        "actor": ctor["actor"],
                        "run": key[:16],
                        "deadline_unix_ms": 0,
                        "policy": policy,
                        "retry_exception_types": [],
                        "durable": False,
                        "items": [(key, 1, args_ref, [], 0)],
                        "direct": reply,
                    },
                )
            )
        elif m[0] == "cancel":
            _cancel(executor, m[1])


def _cancel(executor: Any, key: bytes) -> None:
    """As the peer's cancel: flag a running call (interrupted after a while if
    it runs on the main thread); a queued one is answered as cancelled when
    its turn comes (``starting``)."""
    from ._executor import CANCEL_INTERRUPT_S

    ctx = executor.running.get(key)
    if ctx is None:
        executor.__dict__.setdefault("direct_cancelled", set()).add(key)
        return
    ctx._cancelled.set()
    t = threading.Timer(CANCEL_INTERRUPT_S, executor.interrupt, (ctx,))
    t.daemon = True
    t.start()


CANCELLED = ("err", "cancelled", "chimaera.Cancelled", "the call was cancelled before it started", b"", "", "")


def starting(executor: Any, msg: dict[str, Any]) -> bool:
    """On the main thread, as a direct call starts: note the process's CPU
    time. True if it was cancelled while queued (answered here, not run)."""
    msg["cpu0"] = time.process_time_ns()
    key = msg["items"][0][0]
    cancelled = executor.__dict__.get("direct_cancelled")
    if cancelled and key in cancelled:
        cancelled.discard(key)
        msg["direct"].done(key, CANCELLED, time.time_ns(), msg["cpu0"])
        return True
    return False


# ---- driver side ------------------------------------------------------------


def eligible(rt: Any, actor: Any, spec: dict[str, Any]) -> bool:
    """Whether this call may go straight to the actor's executor."""
    eff = actor._eff
    return (
        ENABLED
        and rt.info.get("direct_actor_calls")  # a peer that records direct calls (an older one would run them again)
        and not rt.in_executor
        and not rt.remote
        and not rt.detached
        and spec["kind"] == CALL_ACTOR_METHOD
        and set(spec["deps"]) <= {actor._ctor}
        and spec["deadline_unix_ms"] == 0
        and spec["effects"] in EFFECTS_DIRECT
        and not spec["retry_exception_types"]
        and not spec.get("stream")
        and not spec.get("command_id")
        and eff.get("state") is None
    )


class Conn:
    """This process's connection to one actor's executor."""

    def __init__(self, rt: Any, sock: socket.socket) -> None:
        self.rt, self.sock = rt, sock
        self.lock = threading.Lock()  # pending and broken
        self.send_lock = threading.Lock()  # one frame at a time, whichever handle sends it
        self.pending: dict[bytes, tuple[Any, dict[str, Any]]] = {}
        self.broken = False
        threading.Thread(target=self._read, name="direct-results", daemon=True).start()

    def send(self, spec: dict[str, Any], fut: Any) -> bool:
        args = (
            "ok",
            spec.get("args_inline") if spec.get("args_inline") is not None or not spec.get("args_size") else None,
            spec["key"][:16],
            spec.get("args_object") or b"",
            spec.get("args_size") or 0,
            spec.get("args_oob") or 0,
        )
        with self.lock:
            if self.broken:
                return False
            self.pending[spec["key"]] = (fut, spec)
        try:
            _send(self.sock, self.send_lock, ("call", spec["key"], spec["method"], args, spec["policy"]))
        except OSError:
            with self.lock:
                self.pending.pop(spec["key"], None)
                self.broken = True
            return False
        return True

    def cancel(self, key: bytes) -> None:
        try:
            _send(self.sock, self.send_lock, ("cancel", key))
        except OSError:
            pass

    def _read(self) -> None:
        rt = self.rt
        while True:
            try:
                m = _recv(self.sock)
            except (OSError, pickle.UnpicklingError, EOFError, ValueError):
                m = None
            if m is None:
                break
            _, key, outcome, entry, exit_ns, cpu, executor = m
            with self.lock:
                fut, spec = self.pending.pop(key, (None, None))
            if fut is None:
                continue
            if outcome[0] == "forward":
                _forward(rt, fut, outcome[1])
                continue
            spec["ran"] = (outcome, entry, exit_ns, cpu, executor)
            rt.client.submit([spec])  # the record: coalesced with other submissions
            rt._reads.pop(key, None)
            fut._set(_driver_outcome(outcome))
            with rt.cond:
                rt.cond.notify_all()
        with self.lock:
            self.broken = True
            waiting, self.pending = list(self.pending.values()), {}
        # The executor is gone: the peer runs (or answers from its record)
        # whatever was still waiting, under the same keys.
        if waiting:
            rt.client.submit([spec for _, spec in waiting])


def _forward(rt: Any, fut: Any, target: bytes) -> None:
    """The method returned another call's future: resolve to its outcome (the
    peer runs this call again for its record, which forwards there too)."""
    from ._future import Future

    other: Future[Any] = Future(rt, target)
    other._escaped = True

    def copy(f: Any) -> None:
        if f._outcome is not None:
            fut._set(f._outcome)
        elif f._exc is not None:
            fut._set_exception(f._exc)
        with rt.cond:
            rt.cond.notify_all()

    other._on_done(copy)
    rt.submit_watch(target, other)


def _driver_outcome(o: tuple[Any, ...]) -> tuple[Any, ...]:
    """An executor's outcome tuple in the shape the driver's futures read."""
    if o[0] == "ok":
        _, run, object_id, size, inline, oob = o[:6]
        return ("ok", inline, run or None, object_id, size, oob)
    _, kind, type_name, message, exc_pickle, tb, detail = o
    return ("err", kind, type_name, message, exc_pickle, tb, None, 1, detail)


class _Handle:
    """One handle's direct calls: their own sequence, and the ones in flight."""

    __slots__ = ("seq", "prev", "pending")

    def __init__(self) -> None:
        self.seq, self.prev, self.pending = 0, None, []


_conns: dict[bytes, Conn | None] = {}
_conns_lock = threading.Lock()


def _conn(rt: Any, actor: bytes) -> Conn | None:
    with _conns_lock:
        if actor in _conns:
            c = _conns[actor]
            return c if c is not None and not c.broken else None
        path = socket_path(rt.home, actor)
        if path is None or len(_conns) >= MAX_CONNS or not os.path.exists(path):
            return None  # not listening yet (still constructing), or on another peer: asked again later
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect(path)
        except OSError:
            s.close()
            _conns[actor] = None  # left by an executor that died: this actor takes the peer path
            return None
        c = _conns[actor] = Conn(rt, s)
        return c


def take(rt: Any, actor: Any, spec: dict[str, Any], fut: Any) -> bool:
    """Send `spec` straight to its actor's executor if it may go (or fail it if
    the handle was closed); called with the handle's order lock held. False:
    it takes the peer path (after this handle's direct calls in flight finish)."""
    if actor.__dict__.get("_direct_closed"):
        fut._set_exception(ActorDied("the actor was closed (close() on this handle)"))
        return True
    h = actor.__dict__.get("_direct")  # not getattr: an Actor makes a method of unknown names
    # Calls on the peer path finish in order (one at a time): the last one
    # unfinished means some are. `_unfinished` holds them strongly, unlike
    # `_futures`, so a call whose future was dropped still counts.
    peer_busy = actor._prev is not None and actor._prev in rt._unfinished
    if not peer_busy and eligible(rt, actor, spec):
        c = _conn(rt, actor._actor)
        if c is not None:
            if h is None:
                h = actor.__dict__["_direct"] = _Handle()
            h.pending = [f for f in h.pending if not f.done()]
            spec["handle"], spec["seq"], spec["prev"] = actor._handle | DIRECT_HANDLE, h.seq, h.prev
            rt.submit([spec], [fut], send=False)
            # First, so it reaches the peer before the call's record can.
            rt.client.submit([{**spec, **_NOTICE}])
            if c.send(spec, fut):
                h.seq += 1
                h.prev = spec["key"]
                h.pending.append(fut)
                actor._calls[0] += 1  # CH121 counts calls through a handle, direct or not
                _counter["direct"] += 1
                return True
            rt.client.submit([spec])  # registered above: the peer runs it instead
            return True
    if h is not None:
        for f in h.pending:  # the peer path must not overtake them
            f._wait(None)
        h.pending = []
    return False


# What turns a call's spec into its in-flight notice: no arguments, never placed.
_NOTICE = {"direct": True, "args_inline": None, "args_object": None, "args_size": 0, "args_oob": 0, "blobs": [], "deps": []}


def forget(handle: Any) -> None:
    """``handle.close()``: later calls through this handle fail at once with
    ActorDied (the peer ends the actor a moment later, and a call submitted
    meanwhile could still run), and no call from this process goes to the
    actor directly again (calls in flight are resubmitted through the peer by
    the connection's reader as it closes)."""
    handle.__dict__["_direct_closed"] = True
    with _conns_lock:
        c, _conns[handle._actor] = _conns.get(handle._actor), None
    if c is not None:
        try:
            c.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def cancel(fut: Any) -> None:
    """Pass a cancel to the executor running a direct call."""
    for c in list(_conns.values()):
        if c is not None and fut.key in c.pending:
            c.cancel(fut.key)
