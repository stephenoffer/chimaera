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
exception types to retry and no generator, that may run again: ``effects``
"none" or "idempotent", and ``deterministic`` (a direct call runs before its
peer has a record of it, so a retry of its caller may run it again). Those
are the call's options: its view's over its method's over its actor's, as
for every option. A handle passed to a call keeps its actor's ``effects`` and
``deterministic`` (``_remote.REPLAY_OPTIONS``), and a call that a bundle's
or a gang rank's host makes for a user's function or method carries that
code's (``_remote.with_replay``). A handle found by name does not know them,
so an actor made with ``deterministic=False`` serves no direct calls
(``listen``), whatever a call to it says. A call whose inputs are futures of
this process waits here until they are in, then goes with their values.
Everything else takes the peer as before, and so does a call to a concurrent
actor while ``MARKS`` of this process's direct calls to it are in flight.
The executor runs them as it runs the peer's: one at a time, or in the
actor's lanes when it has ``concurrency`` or groups.

Order: a handle's direct calls start in the order made (one connection per
actor, read in order). A handle switching paths first waits for its calls on
the other path, so calls never overtake each other. Concurrent actors promise
no order, as through the peer.

Failure: a call that began never runs again. The caller greets the executor
as it connects, and sends its calls without waiting; the executor answers
before it reads any of them, with the connection's marks: a little shared
memory both map, where the executor marks each call begun before it runs it.
They outlive the executor's process. If the connection breaks, the caller
asks the executor, on a new connection, what became of each call still
waiting (``Conn._reclaim``): it sends each finished call's outcome (kept for
the asking) and names those still running, whose outcomes follow, and none of
the rest begins after that. Of those, the ones the marks say began lost their
outcome on the way back: they end ``OutcomeUnknown`` (an actor method may
change the actor's state, so none is run on a guess). The others go through
the peer under their own keys, but one cancelled first ends ``Cancelled``. If
that executor is gone (nobody, or another instance of the actor, listens
there now, and its process ended), calls the marks say began raise
``ActorDied``, as a call running when its actor's process dies does through
the peer, and the rest go through the peer, which runs them on the next
instance in order. An executor that is alive but does not answer leaves the
calls that began ``OutcomeUnknown``, and the rest too if this side broke the
connection (on an answer it could not read): the executor may not have seen
that yet, and may still begin them. Calls in flight when a handle in this
process closes the actor raise ``ActorDied``. A call made while all that is
worked out waits for it, then goes through the peer behind those calls, so it
never overtakes its handle's earlier ones. Either way the actor stays on the
peer path in this process.

Generations: an executor serves direct calls for one instance of its actor,
of the generation its constructor's Execute names, and greets each caller
with it. Once that instance may no longer be current, the executor's
generation is ``RETIRED`` and no direct call begins there: as each one's
turn comes, the executor refuses it (``Reply.begin``). That is once its peer
closes the process (the actor was closed, or its run ended), the peer is
gone, or the peer's lease lapsed (``_leased``). The peer renews that lease
only while it hears most of its cluster, so a peer paused, or cut off from
most of its cluster, long enough to be declared dead lets it lapse before its
actors are placed again elsewhere. A split in which the peer still hears most
of its cluster but not most of an actor's group is not covered: that group
may place the actor again while this instance still serves direct calls
(docs/guides/actors.md). A refused call never began, so its caller sends it through the
peer under its own key (or ends it ``Cancelled`` if it was cancelled), and
every later call that process makes to the actor goes the same way: the
peer runs them on the actor's current instance, in order, or fails them as
the actor is dead. Calls that began before then finish there. Each call's
record names the instance that ran it (``explain()`` shows it) and keeps its
outcome, which its caller already has. A peer that finds the actor dead, or
placed again on another peer, while it still runs an earlier instance stops
that process. It finds out from its copy of the actor's group (as the
group's log or a snapshot brings it up to date), or from a record of the
actor sent to it.

The socket is ``<peer home>/direct/<actor id>.<PROTOCOL>.sock``, mode 0600,
like the peer's own client and executor sockets: any process of the peer's OS
user may connect, which is the boundary those already have. A caller talks
direct only to an executor of its own version: another listens at another name.
"""

from __future__ import annotations

import collections
import functools
import inspect
import itertools
import mmap
import os
import pickle
import socket
import struct
import sys
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
_counter = {"direct": 0, "refused": 0}  # calls sent direct in this process, and those refused (tests)
ENABLED = os.environ.get("CHIMAERA_DIRECT", "1") != "0"
READ_DELAY_S = 0.0  # tests: hold each direct call in the executor's reader this long
TEST_FORGET_RECLAIMS = False  # tests: this executor answers every reclaim as one that forgot the connection
TEST_HELLO_DELAY_S = 0.0  # tests: this executor waits this long before it answers a caller's hello
PROTOCOL = 1  # the direct wire's version, in the socket's name and both sides' hellos
CONNECT_WAIT_S = 5.0  # how long a caller waits to connect (a backlog full); then the actor takes the peer path here
RECLAIM_WAIT_S = 30.0  # how long a caller whose connection broke waits for the executor to greet it again
KEEP_S = 2 * RECLAIM_WAIT_S  # how long an executor keeps a broken connection's outcomes for its caller to ask
MARKS = 16384  # a concurrent actor's calls one process may have in flight direct (each holds a mark)
FULL = -1  # Conn.reserve: no mark is free
RETIRED = -1  # an executor's generation once its instance is no longer current: no direct call begins there
UNTIL_BITS = 48  # the lease word: its epoch above these bits, its deadline (LEASE_CLOCK ms) in them
# The peer's clock for the lease (crates/chimaera-peer/src/direct/lease.rs, CLOCK): one every process on the
# machine reads, which counts while the machine is suspended.
if sys.platform.startswith("linux"):
    LEASE_CLOCK = time.CLOCK_BOOTTIME
else:
    LEASE_CLOCK = time.CLOCK_MONOTONIC
FOREVER = (1 << UNTIL_BITS) - 1  # a lease deadline that never comes: a peer not in a cluster


def socket_path(home: str, actor: bytes) -> str | None:
    path = os.path.join(home, "direct", f"{actor.hex()}.{PROTOCOL}.sock")
    return path if len(path.encode()) <= MAX_SOCKET_PATH else None


def lease_path(peer_socket: str) -> str:
    """The lease the peer listening at `peer_socket` holds out to its
    executors (crates/chimaera-peer/src/direct/lease.rs): beside the direct
    sockets, named after the peer's (peers may share that directory)."""
    return os.path.join(os.path.dirname(peer_socket), "direct", os.path.basename(peer_socket) + ".lease")


def socket_dir(rt: Any) -> str:
    """Where this process's peer keeps its sockets, as its executors see it
    (a long home puts them under /tmp): the peer socket's own directory."""
    from ._runtime import peer_socket

    return os.path.dirname(os.environ.get("CHIMAERA_PEER_SOCKET") or peer_socket(rt.home))


def _frame(obj: Any) -> bytes:
    data = pickle.dumps(obj, protocol=5)
    return HEADER.pack(len(data)) + data


def _send(sock: socket.socket, lock: threading.Lock, obj: Any) -> None:
    data = _frame(obj)
    with lock:
        sock.sendall(data)


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


def listen(executor: Any, msg: dict[str, Any], epoch: int | None) -> None:
    """After an actor's constructor ran: serve direct calls unless the actor
    is durable (its replies wait for committed state), was made with
    ``deterministic=False`` (a direct call may run again, ``eligible``, and
    a handle found by name does not know that option) or batches a method
    (its batcher collects calls the peer sends), its instance is retired
    already (its peer went while it was built: a newer instance may listen
    at its path), or its peer leases no direct calls in `epoch`, the lease's
    epoch as the constructor began (``lease_epoch``, ``_leased``): one that
    lapsed while the instance was built may have let the actor be placed
    again elsewhere."""
    cls = type(executor.instance)
    if msg.get("durable") or not msg["policy"].get("deterministic") or executor.generation == RETIRED:
        return
    if any((getattr(getattr(cls, n, None), "__chimaera_method__", None) or {}).get("batch") for n in dir(cls)):
        return
    peer_socket = executor.conn.welcome["peer_socket"]
    path = socket_path(os.path.dirname(peer_socket), msg["actor"])
    if path is None:
        return
    lease = None if epoch is None else _map_lease(lease_path(peer_socket))
    if lease is None:
        return
    executor.direct_lease = (lease, epoch)  # and the epoch it serves in
    if not _leased(executor):
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
    # Who answers there (a reclaim must reach this instance, not a later one),
    # whether it runs one call at a time (which says how it marks them), and
    # its instance's generation (Reply.begin).
    serial = executor.actor_concurrency <= 1 and not _concurrent_class(cls)
    who = (os.urandom(8), serial, msg.get("actor_generation", 0))
    threading.Thread(target=_accept, args=(executor, server, msg, who), name="direct-accept", daemon=True).start()


def lease_epoch(executor: Any) -> int | None:
    """The epoch of the executor's peer's lease now, read as an actor's
    constructor begins (``listen``); None if the peer keeps none."""
    try:
        fd = os.open(lease_path(executor.conn.welcome["peer_socket"]), os.O_RDONLY)
    except OSError:
        return None
    try:
        data = os.pread(fd, 8, 0)
    finally:
        os.close(fd)
    return struct.unpack("=Q", data)[0] >> UNTIL_BITS if len(data) == 8 else None


def _map_lease(path: str) -> memoryview | None:
    """The peer's lease word, mapped to read; None if the peer keeps none."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return None
    try:
        return memoryview(mmap.mmap(fd, 8, access=mmap.ACCESS_READ)).cast("Q")
    except (OSError, ValueError):
        return None
    finally:
        os.close(fd)


def _leased(executor: Any) -> bool:
    """Whether the executor's peer still leases it direct calls: in the epoch
    it began serving in, before the lease's deadline. The peer renews it only
    while it hears most of its cluster, so it runs out before anyone can
    declare the peer dead and place its actors again. One that ran out comes
    back in a new epoch: by then this instance may have been replaced, so it
    is retired here for good, and its callers go through the peer."""
    lease, epoch = executor.direct_lease
    word = lease[0]
    until = word & FOREVER
    if word >> UNTIL_BITS == epoch and (
        until == FOREVER or until > time.clock_gettime_ns(LEASE_CLOCK) // 1_000_000
    ):
        return True
    executor.generation = RETIRED
    return False


def _accept(executor: Any, server: socket.socket, ctor: dict[str, Any], who: tuple[bytes, bool, int]) -> None:
    while True:
        try:
            sock, _ = server.accept()
        except OSError:
            return
        threading.Thread(target=_serve, args=(executor, sock, ctor, who), name="direct-reader", daemon=True).start()


class Reply:
    """Where a direct call's outcome goes: its caller's connection. Once that
    breaks, its calls that had not begun never do, and the outcomes of those
    that had are kept a while for the caller to ask for them (``_reclaim``)."""

    def __init__(self, sock: socket.socket, executor: Any, who: tuple[bytes, bool, int]) -> None:
        self.sock, self.executor, self.executor_id = sock, executor, getattr(executor, "executor_id", 0)
        # Whether the actor runs one call at a time, and the generation of the
        # instance its caller was greeted as: the one its calls are for.
        self.serial, self.generation = who[1], who[2]
        self.lock = threading.Lock()  # writes to sock, and the fields below
        self.broken = False
        self.marks: memoryview | None = None  # shared with the caller once it greeted: which calls began
        self.order: dict[bytes, tuple[int, int | None]] = {}  # calls read and not begun: place in that order, mark
        self.started: set[bytes] = set()  # began here; outcome not out yet
        self.kept: dict[bytes, tuple[Any, ...]] = {}  # outcomes that finished after the break
        self.to: Reply | None = None  # the connection its caller asked from: where outcomes go now

    def write(self, frame: tuple[Any, ...]) -> None:
        """Send a frame; called with ``lock`` held."""
        data = pickle.dumps(frame, protocol=5)
        self.sock.sendall(HEADER.pack(len(data)) + data)

    def end(self) -> None:
        """The connection broke; called with ``lock`` held. Its reader stops."""
        self.broken = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def begin(self, key: bytes) -> bool:
        """As a direct call starts, on the thread that runs it: False if its
        caller's connection broke first, or if the instance it is for is no
        longer current (the executor's generation is not its own, or its
        peer's lease lapsed, ``_leased``: its caller is told,
        ``Conn._refused``), and then it never runs. Else it is
        marked begun where its caller reads it, even once this process is gone
        (``Conn._began``): a one-at-a-time actor begins a connection's calls
        in the order read and marks the latest, a concurrent one marks each
        in the mark its caller gave it."""
        with self.lock:
            got = self.order.pop(key, None)
            if self.broken:
                return False
            if self.generation != self.executor.generation or not _leased(self.executor):
                try:
                    self.write(("refused", key))
                except OSError:
                    self.end()  # its caller asks what became of its calls, and learns this one never began
                return False
            if self.marks is not None and got is not None:
                i, mark = got
                if self.serial:
                    self.marks[0] = i + 1
                elif mark is not None:
                    self.marks[1 + mark] = i + 1
            self.started.add(key)
            return True

    def done(self, key: bytes, outcome: tuple[Any, ...], entry: int, cpu0: int) -> None:
        cpu = time.process_time_ns() - cpu0
        frame = ("done", key, outcome, entry, time.time_ns(), cpu, self.executor_id)
        with self.lock:
            self.started.discard(key)
            self.order.pop(key, None)  # answered without beginning: cancelled while queued
            if self.to is not None:
                with self.to.lock:
                    try:
                        self.to.write(frame)
                    except OSError:
                        pass  # that connection broke too: its caller ends the call as one that may have run
                return
            if not self.broken:
                try:
                    self.write(frame)
                    return
                except OSError:
                    self.end()
            self.kept[key] = frame


_replies: dict[int, Reply] = {}  # this executor's connections by id: live ones, and broken ones kept a while
_replies_lock = threading.Lock()
_reply_ids = itertools.count(1)


def _serve(executor: Any, sock: socket.socket, ctor: dict[str, Any], who: tuple[bytes, bool, int]) -> None:
    """Read one caller's calls in order and queue them for the main thread,
    as the peer's Execute messages are (the reader stays free for cancels).
    Its caller greets first and its calls follow, read only once it is
    answered (``_greet``): none begins before it can learn which did."""
    reply = Reply(sock, executor, who)
    with _replies_lock:
        conn = next(_reply_ids)
        _replies[conn] = reply
    try:
        _read_calls(executor, reply, ctor, who, conn)
    except (OSError, ImportError):  # ImportError: no shared memory here to greet with (_shared)
        pass
    finally:
        with reply.lock:
            reply.broken = True  # before the caller can see the connection end
            sock.close()
            keep = bool(reply.started or reply.kept)
        if keep:  # outcomes its caller may ask for (``_reclaim``): kept a while
            t = threading.Timer(KEEP_S, _forget_reply, (conn,))
            t.daemon = True
            t.start()
        else:
            _forget_reply(conn)


def _forget_reply(conn: int) -> None:
    with _replies_lock:
        _replies.pop(conn, None)


def _greet(reply: Reply, who: tuple[bytes, bool, int], conn: int) -> memoryview:
    """Answer a caller's hello: this instance's token, the connection's id,
    whether the actor runs one call at a time, this process's id, the
    instance's generation (its calls' records name it), and the
    connection's marks, shared memory sent as a descriptor, where ``begin``
    marks each call begun."""
    if TEST_HELLO_DELAY_S:
        time.sleep(TEST_HELLO_DELAY_S)
    token, serial, generation = who
    size = _marks_size(serial)
    fd = _shared(size)
    try:  # named nowhere: freed once this mapping and the caller's are
        marks = memoryview(mmap.mmap(fd, size)).cast("Q")
        with reply.lock:
            reply.write(("hello", PROTOCOL, token, conn, serial, os.getpid(), generation))
            socket.send_fds(reply.sock, [b"m"], [fd])
    finally:
        os.close(fd)
    return marks


def _marks_size(serial: bool) -> int:
    """A connection's marks: the latest call begun, or one per call in flight."""
    return 8 * (1 if serial else 1 + MARKS)


def _shared(size: int) -> int:
    """A descriptor of `size` zeroed bytes of shared memory that no name
    reaches: a memfd on Linux, else a POSIX shared memory object unlinked at
    once (a file would do, but its mmap on macOS first syncs the disk, which
    takes milliseconds). Its pages are taken now where the system can, so a
    shortage of shared memory fails the greeting rather than a write to
    them (SIGBUS)."""
    if hasattr(os, "memfd_create"):
        fd = os.memfd_create("chimaera-direct-marks")
    else:
        import _posixshmem  # what multiprocessing.shared_memory uses

        name = "/chm" + os.urandom(8).hex()
        fd = _posixshmem.shm_open(name, os.O_CREAT | os.O_EXCL | os.O_RDWR, mode=0o600)
        _posixshmem.shm_unlink(name)
    try:
        os.ftruncate(fd, size)
        if hasattr(os, "posix_fallocate"):
            os.posix_fallocate(fd, 0, size)
    except BaseException:
        os.close(fd)
        raise
    return fd


def _read_calls(executor: Any, reply: Reply, ctor: dict[str, Any], who: tuple[bytes, bool, int], conn: int) -> None:
    sock, read = reply.sock, 0
    while True:
        try:
            m = _recv(sock)
        except Exception:  # noqa: BLE001 - a frame that will not decode ends the connection
            m = None
        if m is None:
            return
        if m[0] == "call":
            # deps: the outcomes of its inputs, in TaskSpec.deps order; mark: where a
            # concurrent actor marks it begun (Conn.reserve)
            _, key, method, args_ref, policy, deps, mark = m
            reply.order[key] = (read, mark)  # its caller numbers the calls it sends alike
            read += 1
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
        elif m[0] == "reclaim":
            # ("reclaim", token, connection id, keys, cancelled keys): the
            # cancels ride in the frame, so the caller writes nothing more
            # before it reads the answer, which may not fit the socket's buffer.
            _reclaim(reply, m[2] if m[1] == who[0] else None, m[3])
            for key in m[4]:
                _cancel(executor, key)
        elif m[0] == "hello":
            reply.marks = _greet(reply, who, conn)


def _reclaim(new: Reply, conn: int | None, keys: list[bytes]) -> None:
    """A caller whose connection `conn` here broke asks, on `new`, what became
    of its calls `keys`: it is sent the outcome of each that finished, then
    which are still running (their outcomes follow on `new`). The old
    connection is marked broken first, so none of the rest begins after this,
    and the caller's marks say which of them began."""
    with _replies_lock:
        old = None if conn is None else _replies.pop(conn, None)
    if old is not None and TEST_FORGET_RECLAIMS:
        with old.lock:
            old.end()  # as if it ended long ago: what it kept is gone
        old = None
    if old is None:  # forgotten, or a connection of another instance: the caller cannot learn more
        with new.lock:
            new.write(("forgot", conn))
        return
    with old.lock:
        old.end()  # a caller that broke on its side: its reader here may not have seen it yet
        old.to = new
        with new.lock:
            for key in keys:
                frame = old.kept.pop(key, None)
                if frame is not None:
                    new.write(frame)
            new.write(("reclaimed", [key for key in keys if key in old.started]))


def _cancel(executor: Any, key: bytes) -> None:
    """As the peer's cancel: flag a running call (interrupted after a while if
    it runs on the main thread); a queued one is answered as cancelled when
    its turn comes (``starting``)."""
    from ._executor import CANCEL_INTERRUPT_S

    ctx = executor.running.get(key)
    if ctx is None:
        executor.__dict__.setdefault("direct_cancelled", set()).add(key)
        return
    if ctx._cancelled.is_set():
        return  # asked again: a caller sends its cancels again after a reclaim
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
        # It runs before its peer has a record of it, so a retry of its caller
        # may run it again: only a call that may run again goes direct.
        and spec["effects"] in EFFECTS_DIRECT
        and spec["policy"]["deterministic"]
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


def _dial(path: str, timeout: float) -> socket.socket | str:
    """Connect to the executor listening at `path` and greet it (its answer
    is read later, ``_greeting``); or why not: "gone" (nobody listens
    there) or "silent" (it did not take the connection within `timeout`)."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(path)
        s.sendall(_frame(("hello", PROTOCOL)))
    except OSError as e:
        s.close()
        return "silent" if isinstance(e, TimeoutError) else "gone"
    return s


def _greeting(s: socket.socket) -> tuple[tuple[Any, ...], memoryview] | str:
    """The executor's answer to a hello (``_greet``) and the connection's
    marks; or why not: "gone" (it closed first, so it read no call),
    "silent" (no answer within the socket's timeout) or "garbled" (it would
    not decode)."""
    fds: list[int] = []
    got: Any = "garbled"
    try:
        hello = _recv(s)
        if hello is None:
            got = "gone"
        elif isinstance(hello, tuple) and hello[:2] == ("hello", PROTOCOL):
            data, fds, _, _ = socket.recv_fds(s, 1, 1)
            size = _marks_size(hello[4])
            if not data:
                got = "gone"
            elif len(fds) == 1 and os.fstat(fds[0]).st_size >= size:
                got = (hello, memoryview(mmap.mmap(fds[0], size)).cast("Q"))
    except TimeoutError:
        got = "silent"
    except OSError:
        got = "gone"
    except Exception:  # noqa: BLE001 - its answer would not decode
        pass
    for fd in fds:
        os.close(fd)
    return got


def _connect(path: str, timeout: float) -> tuple[socket.socket, tuple[Any, ...], memoryview] | str:
    """Greet the executor listening at `path` and wait for its answer: the
    socket, its hello and the connection's marks; or why not (``_dial``,
    ``_greeting``)."""
    s = _dial(path, timeout)
    if isinstance(s, str):
        return s
    got = _greeting(s)
    if isinstance(got, str):
        s.close()
        return got
    s.settimeout(None)
    return (s, *got)


def _exited(pid: int) -> bool:
    """Whether process `pid` has ended, waiting a while for it to: until then
    a call it read may still begin."""
    deadline = time.monotonic() + RECLAIM_WAIT_S
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except OSError:
            pass  # another user's process: it exists
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)


class Conn:
    """This process's connection to one actor's executor."""

    def __init__(self, rt: Any, path: str, sock: socket.socket) -> None:
        self.rt, self.path, self.sock = rt, path, sock
        # The executor's answer to this side's hello, once read (``_greet``): ("hello", PROTOCOL,
        # token, connection id, serial, pid, generation); and the marks that say which calls began.
        self.hello: tuple[Any, ...] = ()
        self.marks: memoryview | None = None
        self.serial: bool | None = None  # whether the actor runs one call at a time, once its executor said
        self.lock = threading.Lock()  # pending, parked, marks handed out, and the states below
        self.send_lock = threading.Lock()  # one frame at a time, whichever handle sends it
        # key -> (future, spec, input futures held until the reply, place in the order sent, its mark)
        self.pending: dict[bytes, tuple[Any, dict[str, Any], list[Any], int | None, int | None]] = {}
        # Calls made after it broke, before the calls sent on it were settled:
        # they go through the peer behind those (``_settle``), never ahead of
        # their handle's earlier calls. Shaped as `pending`, never sent.
        self.parked: dict[bytes, tuple[Any, dict[str, Any], list[Any], int | None, int | None]] = {}
        self.sent = 0  # calls sent: the next one's place in that order
        self.top = 0  # a concurrent actor's marks handed out so far
        self.freed: list[int] = []  # and those free again
        self.cancels: set[bytes] = set()  # calls cancelled in flight: asked again after a reclaim
        self.broken = False
        self.settled = False  # it broke and the calls sent on it are accounted for: later ones take the peer
        self.closed = False  # a handle in this process closed the actor (forget)
        self.refusing = False  # its executor refused a call: it begins none here again, so none is sent
        self.reclaiming = False  # reading the executor's answers on a new connection (_reclaim)
        # The executor ended the connection, or answered on a new one: either
        # way it marked it broken first, so none of its calls begins after.
        self.fenced = False
        threading.Thread(target=self._read, name="direct-results", daemon=True).start()

    def reserve(self) -> int | None:
        """A mark for a call about to go here, held until its outcome is in:
        None if the actor runs one call at a time (its executor marks the
        latest call begun), ``FULL`` while MARKS of this process's calls to it
        hold one (then it takes the peer path, under its handle's own order)."""
        if self.serial:  # once true it stays so: no lock on this path
            return None
        with self.lock:
            if self.serial:
                return None
            if self.freed:
                return self.freed.pop()
            if self.top == MARKS:
                return FULL
            self.top += 1
            return self.top - 1

    def release(self, mark: int | None) -> None:
        """A reserved mark whose call did not go here after all."""
        if mark is not None:
            with self.lock:
                self.freed.append(mark)

    def send(self, spec: dict[str, Any], fut: Any, deps: list[Any], inputs: list[Any], mark: int | None) -> bool:
        """Send a call with the mark reserved for it. False if it must go
        through the peer now: the connection broke and its calls were
        settled. One made after it broke, before then, is parked: it goes
        through the peer behind those calls."""
        key = spec["key"]
        args = (
            "ok",
            spec.get("args_inline") if spec.get("args_inline") is not None or not spec.get("args_size") else None,
            key[:16],
            spec.get("args_object") or b"",
            spec.get("args_size") or 0,
            spec.get("args_oob") or 0,
        )
        data = _frame(("call", key, spec["method"], args, spec["policy"], list(deps), mark))
        with self.send_lock:  # `pending` in the order the executor reads the calls
            with self.lock:
                if self.broken:
                    return self._park(key, fut, spec)
                i = self.sent
                self.pending[key] = (fut, spec, list(inputs), i, mark)
            try:
                self.sock.sendall(data)
            except OSError:  # not all of it went: the executor never reads it
                with self.lock:
                    self.pending.pop(key, None)
                    self.broken = True
                    return self._park(key, fut, spec)
            self.sent = i + 1
        return True

    def _park(self, key: bytes, fut: Any, spec: dict[str, Any]) -> bool:
        """A call for a broken connection; called with ``lock`` held."""
        if self.settled:
            return False  # its handle's earlier calls are through the peer already: it goes behind them
        self.parked[key] = (fut, spec, [], None, None)
        return True

    def cancel(self, key: bytes) -> None:
        with self.send_lock:
            self.cancels.add(key)
            if key in self.parked:
                return  # never sent: it ends cancelled as the connection is settled
            try:
                self.sock.sendall(_frame(("cancel", key)))
            except OSError:
                pass

    def _read(self) -> None:
        got = _greeting(self.sock)
        if isinstance(got, str):  # it ended before it answered, so read no call; or its answer would not decode
            how = "ended" if got == "gone" else "garbled"
        else:
            with self.lock:
                (self.hello, self.marks), self.serial = got, bool(got[0][4])
            how = self._drain()
        self.fenced = how == "ended"  # not one this side could not read
        with self.lock:
            self.broken = True
        gone, why = False, "" if self.hello else "its answer to this caller's greeting would not decode"
        if self.pending and not self.closed and self.hello:
            gone, why = self._reclaim()
        with self.send_lock:
            try:
                self.sock.close()
            except OSError:
                pass
        self._settle(gone, why)

    def _drain(self) -> str:
        """Handle the executor's frames until the connection ends; returns how:
        "ended", "garbled" (on a frame that would not decode: what came after
        it is lost) or "forgot" (the executor no longer knows the connection
        it was asked about)."""
        rt = self.rt
        while True:
            if self.reclaiming and not self.pending:
                return "ended"  # the executor accounted for every call it was asked about
            try:
                m = _recv(self.sock)
            except OSError:
                return "ended"  # a reset: reported after what was sent before it was read
            except Exception:  # noqa: BLE001
                self.fenced = self.fenced or self.reclaiming  # on a new connection, only answers come
                return "garbled"
            if m is None:
                return "ended"
            tag = m[0]
            if self.reclaiming:
                self.fenced = True
                if tag == "forgot":
                    return "forgot"
                if tag == "reclaimed":  # the rest began here and lost their outcome, or never will begin
                    self._settle(False, "its outcome was lost on the way back", keep=set(m[1]))
                    continue
            if tag == "refused":  # the instance it is for was no longer current: it never began there
                self._refused(m[1])
                continue
            with self.lock:
                fut, spec, _inputs, _i, mark = self.pending.pop(m[1], (None, None, None, None, None))
                if mark is not None:
                    self.freed.append(mark)
            if self.cancels:
                self.cancels.discard(m[1])
            if fut is None:
                continue
            _, key, outcome, entry, exit_ns, cpu, executor = m
            if outcome[0] == "forward":
                _forward(rt, fut, spec, outcome[1], (entry, exit_ns, cpu, executor))
                continue
            # The record names the instance that ran it (its attempt in explain() shows it).
            spec["ran"] = (outcome, entry, exit_ns, cpu, executor, self.hello[6])
            rt.client.submit([spec])  # the record: coalesced with other submissions
            rt._reads.pop(key, None)
            fut._set(_driver_outcome(outcome))
            with rt.cond:
                rt.cond.notify_all()

    def _reclaim(self) -> tuple[bool, str]:
        """Ask the executor this connection reached what became of the calls
        still waiting, on a new connection read from then on as this one was.
        Returns whether that executor is gone, and else why it could not
        account for the calls still waiting after."""
        with self.send_lock:
            try:
                self.sock.close()  # its reader there stops, if it had not
            except OSError:
                pass
        got = _connect(self.path, RECLAIM_WAIT_S)
        if not (isinstance(got, tuple) and got[1][2] == self.hello[2]):
            return self._verdict(got)
        with self.send_lock:
            self.sock, self.reclaiming = got[0], True
            with self.lock:
                keys = list(self.pending)
            # A cancel sent as the connection broke may have been lost: each
            # goes again, inside the one frame written before the answer is read.
            cancels = list(self.cancels.intersection(keys))
            try:
                self.sock.sendall(_frame(("reclaim", self.hello[2], self.hello[3], keys, cancels)))
            except OSError:
                pass  # it went meanwhile: the connection's end says so
        how = self._drain()
        if how == "forgot":  # the connection ended there with nothing kept, or what it kept went
            return False, "its outcome was lost on the way back, or is no longer kept there"
        if not self.pending:
            return False, ""
        gone, why = self._verdict(_connect(self.path, RECLAIM_WAIT_S))  # still there?
        broke = "its answer would not decode" if how == "garbled" else "the connection it answered on broke too"
        return gone, why or broke

    def _verdict(self, got: tuple[socket.socket, tuple[Any, ...], memoryview] | str) -> tuple[bool, str]:
        """From a new connection's greeting (``_connect``): whether this
        connection's executor is gone (nobody, or another instance, listens
        at its path, and its process ended), and else why it cannot be asked."""
        if isinstance(got, tuple):
            got[0].close()
            if got[1][2] == self.hello[2]:
                return False, ""
            got = "gone"  # another instance listens there now
        if got == "silent":
            return False, f"it did not answer within {RECLAIM_WAIT_S:g} s"
        if got == "garbled":
            return False, "its answer would not decode"
        if _exited(self.hello[5]):
            return True, ""
        return False, "it no longer answers, though its process is still running"

    def _settle(self, gone: bool, why: str, keep: set[bytes] | frozenset[bytes] = frozenset()) -> None:
        """End or resubmit the calls still waiting but `keep` (``_fate``), in
        the order sent, then the calls parked meanwhile. From then on a call
        for this connection goes through the peer behind them."""
        rt = self.rt
        records, ended = [], []
        with self.lock:
            left = [v for k, v in self.pending.items() if k not in keep] + list(self.parked.values())
            self.pending = {k: v for k, v in self.pending.items() if k in keep}
            self.parked, self.settled = {}, True
            for fut, spec, _, i, mark in left:
                outcome = self._fate(spec["key"], i, self._began(i, mark), gone, why)
                if outcome is not None:
                    spec["ran"] = (outcome, 0, 0, 0, 0, 0)  # no timings or instance: nothing here saw it run
                    ended.append((fut, spec["key"], outcome))
                records.append(spec)  # its record, or, with no outcome, the call: the peer runs it under its key
            if records:
                rt.client.submit(records)  # under the lock: a call parked too late (_park) goes behind these
        self._ended(ended)

    def _refused(self, key: bytes) -> None:
        """The executor did not begin call `key`: the instance it is for was
        no longer current as its turn came (``Reply.begin``). It never runs
        there, so it ends as a call that never began on an executor now gone
        does (``_fate``): through the peer, which runs it on the actor's
        current instance or fails it as the actor is dead, after the calls
        sent before it (a one-at-a-time actor's executor refuses its calls in
        the order it read them). Later calls take the peer path, behind it."""
        self.refusing = True
        with self.lock:
            got = self.pending.pop(key, None)
            if got is None:
                return
            fut, spec, _, i, mark = got
            if mark is not None:
                self.freed.append(mark)
        _counter["refused"] += 1
        outcome = self._fate(key, i, False, True, "")
        self.cancels.discard(key)
        if outcome is not None:
            spec["ran"] = (outcome, 0, 0, 0, 0, 0)
        self.rt.client.submit([spec])  # its record, or, with no outcome, the call: the peer runs it under its key
        if outcome is not None:
            self._ended([(fut, key, outcome)])

    def _ended(self, ended: list[tuple[Any, bytes, tuple[Any, ...]]]) -> None:
        """Give the calls ended here their outcomes (an executor's shape)."""
        rt = self.rt
        for fut, key, outcome in ended:
            rt._reads.pop(key, None)
            fut._set(_driver_outcome(outcome))
        if ended:
            with rt.cond:
                rt.cond.notify_all()

    def _began(self, i: int | None, mark: int | None) -> bool | None:
        """Whether call `i` (its place in the order sent; None: parked, never
        sent) began, by the marks its executor wrote before running it
        (``Reply.begin``); None if that cannot be known: its executor's
        greeting would not decode here."""
        if i is None:
            return False
        if self.marks is None:  # no greeting: if the executor ended first, it read no call
            return False if self.fenced else None
        # A call reserved no mark only once the actor was known to run one call at a time.
        return i < self.marks[0] if self.serial or mark is None else self.marks[1 + mark] == i + 1

    def _fate(self, key: bytes, i: int | None, began: bool | None, gone: bool, why: str) -> tuple[Any, ...] | None:
        """The outcome a call still waiting ends with as its connection is
        settled, or None to send it through the peer. One that began (or may
        have) never runs again: it raises ``ActorDied`` if its executor is
        gone, else ``OutcomeUnknown`` (`why` says what is known). One that had
        not begun goes through the peer, under its own key (or ends cancelled,
        if it was), unless its executor is alive and may begin it yet: this
        side broke the connection, and the executor did not answer."""
        if self.closed:
            return _failure("actor_died", "the actor was closed (close() on a handle) while this call was in flight")
        if began is not False:
            if gone:
                return _failure(
                    "actor_died",
                    "the actor process died while this call was running (it went straight to the actor's "
                    "executor, and its outcome never came back); it is not run again",
                )
            return _failure(
                "outcome_unknown",
                f"this call went straight to its actor's executor and {'began' if began else 'may have begun'} "
                f"there, and the connection broke before its outcome came back: {why}. An actor method may change "
                "the actor's state, so it is not run again",
            )
        if gone or self.fenced or i is None:  # it never began, and now never will there
            return CANCELLED if key in self.cancels else None
        return _failure(
            "outcome_unknown",
            "this call went straight to its actor's executor, which had not begun it when the connection broke "
            f"and could not be asked whether it will: {why}. It is not sent again, since it may still run there",
        )


def _failure(kind: str, message: str) -> tuple[Any, ...]:
    """An executor-shaped error outcome of a runtime error kind (errors.py)."""
    name = {"actor_died": "ActorDied", "outcome_unknown": "OutcomeUnknown"}[kind]
    return ("err", kind, f"chimaera.{name}", message, b"", "", "")


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
            # No instance named: the outcome is the other call's, and lands
            # any time later, long after the actor's may have changed.
            spec["ran"] = (_exec_outcome(f._outcome), *timing, 0)
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
            return c if c is not None and not c.broken and not c.refusing else None
        path = socket_path(socket_dir(rt), actor)
        if path is None or len(_conns) >= MAX_CONNS or not os.path.exists(path):
            return None  # not listening yet (still constructing), or on another peer: asked again later
    # Its answer is read by the connection's reader: calls go meanwhile.
    s = _dial(path, CONNECT_WAIT_S)  # not under the lock: a busy executor's backlog may hold it a moment
    with _conns_lock:
        if actor in _conns:  # another thread connected meanwhile
            if not isinstance(s, str):
                s.close()
            c = _conns[actor]
            return c if c is not None and not c.broken and not c.refusing else None
        if isinstance(s, str):
            # Left by an executor that died (or one too busy to take it): this actor takes the peer path.
            _conns[actor] = None
            return None
        s.settimeout(None)
        c = _conns[actor] = Conn(rt, path, s)
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
        mark = FULL if c is None else c.reserve()  # a concurrent actor's mark, before the call takes a place here
        if c is not None and mark != FULL:
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
                h.queue.append((rt, c, spec, fut, inputs, mark))
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
            rt, c, spec, fut, inputs, mark = h.queue[0]
            waiting = next((f for f in inputs if f is not None and not f.done()), None)
            if waiting is not None:
                if not h.armed:
                    h.armed = True
                    waiting._on_done(lambda _f: _rearm(h))
                return
            h.queue.popleft()
            _queued.pop(spec["key"], None)
            if fut.done():  # cancelled while it waited
                c.release(mark)
                _never_sent(rt, spec)
            else:
                _dispatch(rt, c, spec, fut, inputs, mark)


def _rearm(h: _Handle) -> None:
    with h.lock:
        h.armed = False
    _pump(h)


def _dispatch(rt: Any, c: Conn, spec: dict[str, Any], fut: Any, inputs: list[Any], mark: int | None) -> None:
    """Send a call whose inputs are in, with their outcomes. One whose input
    failed goes through the peer instead, which fails it as that input's dependent."""
    deps = []
    for f in inputs:
        if f is None:
            deps.append(_UNREAD)  # the constructor: the call reads no value from it
        elif f._outcome is not None and f._outcome[0] == "ok":
            deps.append(f._outcome)
        else:
            c.release(mark)
            rt.client.submit([spec])
            return
    # First, so it reaches the peer before the call's record can.
    rt.client.submit([{**spec, **_NOTICE}])
    if not c.send(spec, fut, deps, [f for f in inputs if f is not None], mark):
        rt.client.submit([spec])  # registered already: the peer runs it, behind the calls its connection settled


def _never_sent(rt: Any, spec: dict[str, Any]) -> None:
    """Record a direct call cancelled before it was sent: it holds a place in
    its handle's order, and a peer that runs the handle's later calls (after a
    break) waits for each place until its call has a record."""
    rt.client.submit([{**spec, "ran": (CANCELLED, 0, 0, 0, 0, 0)}])


_UNREAD = ("ok", b"", None, b"", 0, 0)
_CANCELLED = ("err", "cancelled", "chimaera.Cancelled", "the call was cancelled before it started", b"", "", None, 0, "")
_queued: dict[bytes, _Handle] = {}  # calls waiting for their inputs, by key

# What turns a call's spec into its in-flight notice: no arguments, never placed.
_NOTICE = {"direct": True, "args_inline": None, "args_object": None, "args_size": 0, "args_oob": 0, "blobs": [], "deps": []}


def forget(handle: Any) -> None:
    """``handle.close()``: later calls through this handle fail at once with
    ActorDied (the peer ends the actor a moment later, and a call submitted
    meanwhile could still run), and no call from this process goes to the
    actor directly again (calls in flight raise ActorDied, as through the
    peer: the connection's reader ends them as it closes)."""
    handle.__dict__["_direct_closed"] = True
    with _conns_lock:
        c, _conns[handle._actor] = _conns.get(handle._actor), None
    if c is not None:
        c.closed = True
        try:
            c.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def cancel(fut: Any) -> None:
    """Cancel a direct call: one still waiting here for its inputs never goes;
    one sent is passed to its executor, and one parked on a broken
    connection ends cancelled as that is settled."""
    h = _queued.get(fut.key)
    if h is not None:
        entry = None
        with h.lock:
            for i, e in enumerate(h.queue):
                if e[3] is fut:
                    entry = e
                    del h.queue[i]
                    _queued.pop(fut.key, None)
                    break
        if entry is not None:  # else _pump sent it meanwhile: its executor is told below
            rt, c, spec, _, _, mark = entry
            c.release(mark)  # its mark: it never goes
            _never_sent(rt, spec)
            fut._set(_CANCELLED)
            with rt.cond:
                rt.cond.notify_all()
            _pump(h)
            return
    for c in list(_conns.values()):
        if c is not None and (fut.key in c.pending or fut.key in c.parked):
            c.cancel(fut.key)
