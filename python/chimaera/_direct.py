"""The direct actor path (docs/architecture.md, D2): on a local peer, an
eligible call to a one-at-a-time actor goes from the driver straight to the
actor's executor over a Unix socket, and its outcome comes straight back.

The peer still records every call. Once the outcome is in, the driver submits
the call with ``ran`` set: the shard stores it as done (nothing runs again),
and the peer files the executor's timings as its attempt. So ``explain()``,
``chimaera runs``, accounting and later calls that take its future all see it,
a millisecond or so after the caller did.

Which calls go direct (``eligible``): calls from a driver on a local, attached
run, or from inside a call on this machine (not to the caller's own actor), to
an actor that is not durable and batches no method, whose class is known here,
to a method with no mailbox limit (its peer keeps those), with no deadline, no
exception types to retry and no generator. A call whose inputs are futures of
this process waits here until they are in, then goes with their values.
Everything else takes the peer as before.
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

import collections
import functools
import inspect
import os
import pickle
import socket
import struct
import threading
import time
from typing import Any

from . import _context
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


def socket_dir(rt: Any) -> str:
    """Where this process's peer keeps its sockets, as its executors see it
    (a long home puts them under /tmp): the peer socket's own directory."""
    from ._runtime import peer_socket

    return os.path.dirname(os.environ.get("CHIMAERA_PEER_SOCKET") or peer_socket(rt.home))


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
            _, key, method, args_ref, policy, *rest = m
            deps = rest[0] if rest else []  # the outcomes of its inputs, in TaskSpec.deps order
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
                        "items": [(key, 1, args_ref, deps, 0)],
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
        and not _own_actor(actor)
        and actor._cls is not None
        and not _bounded(actor._cls, spec["method"])
        and not rt.remote
        and not rt.detached
        and spec["kind"] == CALL_ACTOR_METHOD
        and spec["deadline_unix_ms"] == 0
        and spec["effects"] in EFFECTS_DIRECT
        and not spec["retry_exception_types"]
        and not spec.get("stream")
        and not spec.get("command_id")
        and not spec["policy"].get("durability")  # its peer publishes it with its class (retained: copies first)
        and eff.get("state") is None
    )


@functools.lru_cache(maxsize=1024)
def _bounded(cls: type, method: str) -> bool:
    """The method has a mailbox limit, which its peer keeps: its calls go there."""
    declared = getattr(inspect.getattr_static(cls, method, None), "__chimaera_method__", None) or {}
    return declared.get("mailbox") is not None


@functools.lru_cache(maxsize=1024)
def _concurrent_class(cls: type) -> bool:
    from ._remote import _lane_limits

    return bool(_lane_limits(cls))


def _ordered(actor: Any) -> bool:
    """Whether calls through a handle to this actor run in the order made: one
    call at a time, no lanes. Concurrent actors promise no order."""
    eff = actor._eff
    if int(eff.get("concurrency") or 1) > 1 or eff.get("groups"):
        return False
    return actor._cls is None or not _concurrent_class(actor._cls)


def _own_actor(actor: Any) -> bool:
    """A call an actor makes to itself goes through the peer, which sees the
    actor busy and fails the call at once if its caller waits on it."""
    ctx = _context.current_task()
    return ctx is not None and bool(ctx.actor) and ctx.actor == actor._actor


class Conn:
    """This process's connection to one actor's executor."""

    def __init__(self, rt: Any, sock: socket.socket) -> None:
        self.rt, self.sock = rt, sock
        self.lock = threading.Lock()  # pending and broken
        self.send_lock = threading.Lock()  # one frame at a time, whichever handle sends it
        # key -> (future, spec, input futures held until the reply)
        self.pending: dict[bytes, tuple[Any, dict[str, Any], list[Any]]] = {}
        self.broken = False
        threading.Thread(target=self._read, name="direct-results", daemon=True).start()

    def send(self, spec: dict[str, Any], fut: Any, deps: list[Any] = (), inputs: list[Any] = ()) -> bool:
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
            self.pending[spec["key"]] = (fut, spec, list(inputs))
        try:
            _send(self.sock, self.send_lock, ("call", spec["key"], spec["method"], args, spec["policy"], list(deps)))
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
                fut, spec, _inputs = self.pending.pop(key, (None, None, None))
            if fut is None:
                continue
            if outcome[0] == "forward":
                _forward(rt, fut, spec, outcome[1], (entry, exit_ns, cpu, executor))
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
            rt.client.submit([spec for _, spec, _ in waiting])


def _forward(rt: Any, fut: Any, spec: dict[str, Any], target: bytes, timing: tuple[int, int, int, int]) -> None:
    """The method returned another call's future: resolve to its outcome, and
    record the call with that outcome once it is known."""
    from ._future import Future

    other: Future[Any] = Future(rt, target)
    other._escaped = True
    _forwarding[target] = other  # until it lands: the runtime's own table holds futures weakly

    def copy(f: Any) -> None:
        _forwarding.pop(f.key, None)
        if f._outcome is not None:
            spec["ran"] = (_exec_outcome(f._outcome), *timing)
            rt.client.submit([spec])
            fut._set(f._outcome)
        elif f._exc is not None:
            fut._set_exception(f._exc)
        with rt.cond:
            rt.cond.notify_all()

    other._on_done(copy)
    rt.submit_watch(target, other)


_forwarding: dict[bytes, Any] = {}  # futures of calls a direct call forwarded to, by key


def _exec_outcome(o: tuple[Any, ...]) -> tuple[Any, ...]:
    """A driver's outcome tuple in the shape an executor reports (for a record)."""
    if o[0] == "ok":
        _, inline, run, object_id, size, oob = o[:6]
        return ("ok", run or b"", object_id, size, inline, oob)
    _, kind, type_name, message, exc_pickle, tb, _upstream, _attempt, detail = o
    return ("err", kind, type_name, message, exc_pickle, tb, detail)


def _driver_outcome(o: tuple[Any, ...]) -> tuple[Any, ...]:
    """An executor's outcome tuple in the shape the driver's futures read."""
    if o[0] == "ok":
        _, run, object_id, size, inline, oob = o[:6]
        return ("ok", inline, run or None, object_id, size, oob)
    _, kind, type_name, message, exc_pickle, tb, detail = o
    return ("err", kind, type_name, message, exc_pickle, tb, None, 1, detail)


class _Handle:
    """One handle's direct calls: their own sequence, the ones in flight, and
    the ones waiting for inputs (sent in order as those resolve)."""

    __slots__ = ("seq", "prev", "pending", "queue", "lock", "armed", "last_peer")

    def __init__(self) -> None:
        self.seq, self.prev, self.pending = 0, None, []
        self.last_peer: Any = None  # this handle's latest call through the peer
        self.queue: collections.deque[tuple[Any, ...]] = collections.deque()
        self.lock = threading.RLock()  # an input landing as its callback is set re-enters _pump
        self.armed = False  # a callback waits on the queue head's inputs


_conns: dict[bytes, Conn | None] = {}
_conns_lock = threading.Lock()


def _conn(rt: Any, actor: bytes) -> Conn | None:
    with _conns_lock:
        if actor in _conns:
            c = _conns[actor]
            return c if c is not None and not c.broken else None
        path = socket_path(socket_dir(rt), actor)
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
    # unfinished means some are. Held here, so a dropped future still counts,
    # in a driver or inside a call alike.
    ordered = _ordered(actor)
    peer_busy = ordered and h is not None and h.last_peer is not None and not h.last_peer.done()
    inputs = _inputs(rt, actor, spec)
    if not peer_busy and inputs is not None and eligible(rt, actor, spec):
        c = _conn(rt, actor._actor)
        if c is not None:
            if h is None:
                h = actor.__dict__["_direct"] = _Handle()
            h.pending = [f for f in h.pending if not f.done()]
            spec["handle"], spec["seq"], spec["prev"] = actor._handle | DIRECT_HANDLE, h.seq, h.prev
            h.seq, h.prev = h.seq + 1, spec["key"]
            rt.submit([spec], [fut], send=False)
            h.pending.append(fut)
            actor._calls[0] += 1  # CH121 counts calls through a handle, direct or not
            _counter["direct"] += 1
            with h.lock:
                h.queue.append((rt, c, spec, fut, inputs))
                _queued[spec["key"]] = h
            _pump(h)
            return True
    if h is None:
        h = actor.__dict__["_direct"] = _Handle()
    if ordered:
        for f in h.pending:  # the peer path must not overtake them
            f._wait(None)
        h.pending = []
    h.last_peer = fut
    return False


def _inputs(rt: Any, actor: Any, spec: dict[str, Any]) -> list[Any] | None:
    """The futures of a call's inputs in TaskSpec.deps order (None for its
    actor's constructor), or None if one is not a future this process holds."""
    out = []
    for key in spec["deps"]:
        if key == actor._ctor:
            out.append(None)
            continue
        f = rt._futures.get(key)
        if f is None:
            return None
        out.append(f)
    return out


def _pump(h: _Handle) -> None:
    """Send the handle's queued calls whose inputs are in, in order; a callback
    on the first input still out sends the rest when it lands."""
    with h.lock:
        while h.queue:
            rt, c, spec, fut, inputs = h.queue[0]
            waiting = next((f for f in inputs if f is not None and not f.done()), None)
            if waiting is not None:
                if not h.armed:
                    h.armed = True
                    waiting._on_done(lambda _f: _rearm(h))
                return
            h.queue.popleft()
            _queued.pop(spec["key"], None)
            if not fut.done():  # else cancelled while it waited
                _dispatch(rt, c, spec, fut, inputs)


def _rearm(h: _Handle) -> None:
    with h.lock:
        h.armed = False
    _pump(h)


def _dispatch(rt: Any, c: Conn, spec: dict[str, Any], fut: Any, inputs: list[Any]) -> None:
    """Send a call whose inputs are in, with their outcomes. One whose input
    failed goes through the peer instead, which fails it as that input's dependent."""
    deps = []
    for f in inputs:
        if f is None:
            deps.append(_UNREAD)  # the constructor: the call reads no value from it
        elif f._outcome is not None and f._outcome[0] == "ok":
            deps.append(f._outcome)
        else:
            rt.client.submit([spec])
            return
    # First, so it reaches the peer before the call's record can.
    rt.client.submit([{**spec, **_NOTICE}])
    if not c.send(spec, fut, deps, [f for f in inputs if f is not None]):
        rt.client.submit([spec])  # registered already: the peer runs it instead


_UNREAD = ("ok", b"", None, b"", 0, 0)
_CANCELLED = ("err", "cancelled", "chimaera.Cancelled", "the call was cancelled before it started", b"", "", None, 0, "")
_queued: dict[bytes, _Handle] = {}  # calls waiting for their inputs, by key

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
    """Cancel a direct call: one still waiting here for its inputs never goes;
    one sent is passed to its executor."""
    h = _queued.get(fut.key)
    if h is not None:
        with h.lock:
            for i, entry in enumerate(h.queue):
                if entry[3] is fut:
                    del h.queue[i]
                    break
            _queued.pop(fut.key, None)
        fut._set(_CANCELLED)
        with fut._rt.cond:
            fut._rt.cond.notify_all()
        _pump(h)
        return
    for c in list(_conns.values()):
        if c is not None and fut.key in c.pending:
            c.cancel(fut.key)
