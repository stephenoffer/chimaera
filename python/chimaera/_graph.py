"""Compiled actor graphs: ``ch.dist.graph``.

A graph is a function made of actor method calls. ``.remote()`` runs it as
written, one call after another through the peers. ``.compile()`` traces it
once and sets up each actor to run its part of every invocation in a loop of
its own: inputs come straight from the driver, values go straight from actor
to actor and back to the driver over sockets opened at compile time, and an
invocation commits nothing and schedules nothing. At most ``max_in_flight``
invocations run at once, each actor working on the next while the one after
it finishes the last.

Links between actors on one host are Unix sockets in their peer's socket
directory (mode 0600). Links between hosts are TCP, authenticated with the
graph's key but not encrypted, so they need ``compile(cross_host=True)``.

While compiled, each actor is busy with the graph: its other calls wait until
``close()``. Values passed between two calls on one actor are not copied. A
graph breaks for good when one of its actors or its driver goes away (an
actor's restart starts it over without the graph): compile it again.
"""

from __future__ import annotations

import asyncio
import inspect
import multiprocessing
import os
import pickle
import secrets
import socket
import struct
import threading
import time
from multiprocessing.connection import Client, Connection, Listener, answer_challenge, deliver_challenge, wait
from typing import Any, Callable

from . import _context, _serde
from ._errors import ActorDied, Closed, from_outcome

FRAME = struct.Struct(">BQiB")  # kind, invocation, node (or the sender, in HELLO), ok
HELLO, RUN, VALUE, STOP = range(4)
DRIVER = -1
SETUP_S = 60.0  # how long an actor waits for the graph's other ends, and keeps a setup that never starts
HANDSHAKE_S = 5.0  # how long a connection has to prove it holds the graph's key and say who it is
HANDSHAKES = 64  # connections proving it at once; one past them is closed (each holds a thread up to HANDSHAKE_S)
MAX_SOCKET_PATH = 100  # sun_path is 104 bytes on macOS
# The host links are judged by. Tests set it per peer to stand for separate machines.
HOST = os.environ.get("CHIMAERA_GRAPH_HOST") or socket.gethostname()

_NESTED = (
    "a compiled graph passes its values only as direct arguments of actor method calls (not inside lists, "
    "dicts or other objects, and not to functions): pass each one as an argument of its own"
)

# ---- tracing ------------------------------------------------------------------

_trace = threading.local()


def tracing() -> bool:
    return getattr(_trace, "nodes", None) is not None


class Input:
    """The graph function's ``index``th argument, while it is traced."""

    def __init__(self, index: int) -> None:
        self.index = index

    def __reduce__(self) -> Any:
        raise TypeError(_NESTED)

    def __bool__(self) -> bool:
        raise TypeError("a graph's values are not known while it is compiled, so it cannot branch on them")

    def result(self, *args: Any, **kwargs: Any) -> Any:
        raise TypeError("inside a graph, pass a call's result straight to the next call; read the graph's results")


class Node(Input):
    """The result of one actor method call in a traced graph."""

    def __init__(self, index: int, method: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        super().__init__(index)
        self.actor, self.method, self.args, self.kwargs = method._actor, method._name, args, kwargs


def bind(method: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Node:
    """``RemoteMethod.remote`` while a graph is traced: record the call."""
    if method._view:
        raise TypeError(f"{method._name}: per-call .options() do not apply inside a compiled graph")
    if (getattr(method._fn, "__chimaera_method__", None) or {}).get("batch"):
        raise TypeError(f"{method._name} batches its calls, which a compiled graph does not: call it with .remote()")
    node = Node(len(_trace.nodes), method, args, kwargs)
    _trace.nodes.append(node)
    return node


def graph(fn: Callable[..., Any]) -> Graph:
    """Make ``fn``, a function of actor method calls, a graph:
    ``g.compile()`` runs it with no per-call scheduling, ``g.remote(...)``
    runs it as written."""
    return Graph(fn)


class Graph:
    def __init__(self, fn: Callable[..., Any]) -> None:
        self._fn = fn

    def remote(self, *args: Any) -> Any:
        """Run the function as written: each call goes through the peers."""
        return self._fn(*args)

    def compile(self, *, max_in_flight: int = 8, cross_host: bool = False) -> CompiledGraph:
        """Trace the function and set up its actors to run it. ``max_in_flight``
        invocations may run at once (``remote()`` waits for a free one);
        ``cross_host`` allows links between hosts, which are not encrypted."""
        if max_in_flight < 1:
            raise ValueError("max_in_flight must be at least 1")
        kinds = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        n = sum(p.kind in kinds for p in inspect.signature(self._fn).parameters.values())
        _trace.nodes = []
        try:
            out = self._fn(*(Input(i) for i in range(n)))
            nodes: list[Node] = _trace.nodes
        finally:
            _trace.nodes = None
        outs = list(out) if isinstance(out, (tuple, list)) else [out]
        if not outs or not all(isinstance(o, Node) for o in outs):
            raise TypeError("a graph function returns an actor method call's result, or a tuple or list of them")
        shape = None if isinstance(out, Node) else type(out)
        name = getattr(self._fn, "__qualname__", "graph")
        return CompiledGraph(name, nodes, [o.index for o in outs], shape, n, max_in_flight, cross_host)


# ---- the driver's end -----------------------------------------------------------


class GraphFuture:
    """One invocation of a compiled graph: ``result()`` or ``await``."""

    def __init__(self, outs: list[int], shape: type | None) -> None:
        self._outs, self._shape = outs, shape
        self._parts: dict[int, tuple[int, memoryview]] = {}
        self._error: BaseException | None = None
        self._done = threading.Event()

    def _add(self, node: int, ok: int, payload: memoryview) -> bool:
        self._parts[node] = (ok, payload)
        if all(k in self._parts for k in self._outs):
            self._done.set()
            return True
        return False

    def _fail(self, error: BaseException) -> None:
        self._error = error
        self._done.set()

    def done(self) -> bool:
        return self._done.is_set()

    def result(self, timeout: float | None = None) -> Any:
        if not self._done.wait(timeout):
            raise TimeoutError(f"the graph's invocation did not finish within {timeout} s")
        if self._error is not None:
            raise self._error
        values = []
        for k in self._outs:
            ok, payload = self._parts[k]
            if not ok:
                raise from_outcome(pickle.loads(payload) + (1, ""))
            values.append(_serde.loads(payload))
        return values[0] if self._shape is None else self._shape(values)

    def __await__(self) -> Any:
        return asyncio.to_thread(self.result).__await__()


class CompiledGraph:
    """A graph its actors run: ``remote(*args)`` starts an invocation."""

    def __init__(
        self,
        name: str,
        nodes: list[Node],
        outs: list[int],
        shape: type | None,
        n_inputs: int,
        max_in_flight: int,
        cross_host: bool,
    ) -> None:
        from ._remote import RemoteMethod

        self._n, self._outs, self._shape = n_inputs, outs, shape
        actors: list[Any] = []
        index: dict[bytes, int] = {}
        owner = []
        for node in nodes:
            a = index.setdefault(node.actor._actor, len(actors))
            if a == len(actors):
                actors.append(node.actor)
            owner.append(a)
        self._actors = actors

        def spec(x: Any) -> tuple[str, Any]:
            if isinstance(x, Node):
                return ("n", x.index)
            return ("i", x.index) if isinstance(x, Input) else ("c", x)

        specs = [([spec(x) for x in n.args], {k: spec(v) for k, v in n.kwargs.items()}) for n in nodes]
        refs = [[*args, *kwargs.values()] for args, kwargs in specs]
        deps = [[v for t, v in r if t == "n"] for r in refs]
        fanout: list[set[int]] = [set() for _ in nodes]
        for k, ds in enumerate(deps):
            for j in ds:
                if owner[j] != owner[k]:
                    fanout[j].add(owner[k])

        gid, key = secrets.token_bytes(16), secrets.token_bytes(32)

        def call(a: Any, phase: str, arg: Any) -> Any:
            method: Any = RemoteMethod(a, _context.GRAPH_METHOD, None)
            return method.remote(phase, gid, arg)

        opened = [call(a, "open", {"key": key, "tcp": cross_host}) for a in actors]
        info: list[dict[str, Any]] = []
        try:
            info.extend(f.result() for f in opened)
            dial = _Dialer(info, cross_host)
            plans, self._inputs = [], {}
            for a in range(len(actors)):
                mine = [k for k in range(len(nodes)) if owner[k] == a]
                ins = sorted({v for k in mine for t, v in refs[k] if t == "i"})
                triggered = bool(ins) or any(not deps[k] for k in mine)  # else every call waits on another actor
                if triggered:
                    self._inputs[a] = ins
                plans.append(
                    {
                        "me": a,
                        "mine": [(k, nodes[k].method, *specs[k], sorted(fanout[k]), k in outs) for k in mine],
                        "inputs": ins if triggered else None,
                        "expect": sorted({owner[j] for k in mine for j in deps[k] if owner[j] != a}),
                        "send": {b: dial(info[a], b) for k in mine for b in fanout[k]},
                    }
                )
            to_actor = [dial({"host": HOST}, a) for a in range(len(actors))]
            self._runs = [call(a, "run", plan) for a, plan in zip(actors, plans)]
        except BaseException:
            for a, _ in zip(actors, info):
                call(a, "close", None)  # one already running finds nothing to close, and times out alone
            raise
        self._conns: list[Connection] = []
        for address in to_actor:
            conn = Client(address, authkey=key)
            conn.send_bytes(FRAME.pack(HELLO, 0, DRIVER, 1))
            self._conns.append(conn)
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(max_in_flight)
        self._pending: dict[int, GraphFuture] = {}
        self._next = 0
        self._broken: BaseException | None = None
        self._closing = False
        self._plan = [f"graph {name}: {len(nodes)} calls on {len(actors)} actors, up to {max_in_flight} at once"]
        for k, node in enumerate(nodes):
            names = [f"in{v}" if t == "i" else f"n{v}" if t == "n" else repr(v) for t, v in refs[k]]
            to = " -> driver" if k in outs else ""
            host = info[owner[k]]["host"]
            self._plan.append(f"  n{k} {node.actor!r}.{node.method}({', '.join(names)}) on {host}{to}")
        threading.Thread(target=self._read, name="graph-driver", daemon=True).start()

    def remote(self, *args: Any) -> GraphFuture:
        if len(args) != self._n:
            raise TypeError(f"the graph takes {self._n} arguments ({len(args)} given)")
        frames = {a: _serde.dumps(tuple(args[k] for k in ins))[0] for a, ins in self._inputs.items()}
        self._slots.acquire()
        with self._lock:
            if self._broken is not None or self._closing:
                self._slots.release()
                raise self._broken or Closed("the compiled graph is closed")
            r = self._next
            self._next += 1
            fut = self._pending[r] = GraphFuture(self._outs, self._shape)
            try:
                for a, payload in frames.items():
                    self._conns[a].send_bytes(FRAME.pack(RUN, r, DRIVER, 1) + payload)
            except OSError:
                pass  # that actor is gone: the reader breaks the graph
        return fut

    def _read(self) -> None:
        live = list(self._conns)
        while live:
            try:
                ready = wait(live)
            except (OSError, ValueError):  # closed by close(): the graph is over
                return
            for conn in ready:
                assert isinstance(conn, Connection)
                try:
                    data = conn.recv_bytes()
                except (EOFError, OSError):
                    live.remove(conn)
                    if not self._closing:
                        self._break(self._actors[self._conns.index(conn)])
                    continue
                _, r, node, ok = FRAME.unpack_from(data)
                with self._lock:
                    fut = self._pending.get(r)
                    if fut is None or not fut._add(node, ok, memoryview(data)[FRAME.size :]):
                        continue
                    del self._pending[r]
                self._slots.release()

    def _break(self, actor: Any) -> None:
        error = ActorDied(f"{actor!r}, part of a compiled graph, went away; the graph is broken: compile it again")
        with self._lock:
            self._broken = self._broken or error
            pending, self._pending = list(self._pending.values()), {}
        for fut in pending:
            fut._fail(error)
            self._slots.release()

    def close(self, timeout: float = 30.0) -> None:
        """Let the running invocations finish (up to ``timeout`` seconds), then
        free the actors for their other calls."""
        with self._lock:
            if self._closing:
                return
            self._closing = True
            pending = list(self._pending.values())
        deadline = time.monotonic() + timeout
        for fut in pending:
            fut._done.wait(max(0.0, deadline - time.monotonic()))
        for conn in self._conns:
            try:
                conn.send_bytes(FRAME.pack(STOP, 0, DRIVER, 1))
            except OSError:
                pass
        for run in self._runs:
            try:
                run.result(timeout=max(0.1, deadline - time.monotonic()))
            except Exception:
                pass  # an actor that died has nothing to free
        for conn in self._conns:
            conn.close()
        with self._lock:
            left, self._pending = list(self._pending.values()), {}
        for fut in left:
            fut._fail(Closed("the compiled graph was closed before this invocation finished"))

    def explain(self) -> str:
        """The compiled plan: each call, its inputs and its actor's host."""
        return "\n".join(self._plan)

    def __enter__(self) -> CompiledGraph:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class _Dialer:
    """The address one end of a link dials: the other end's Unix socket on one
    host (or its loopback port, if its socket path is too long), else its TCP
    port at its peer's mesh address."""

    def __init__(self, info: list[dict[str, Any]], cross_host: bool) -> None:
        self.info, self.cross_host = info, cross_host
        self.peers: dict[str, str] | None = None

    def __call__(self, frm: dict[str, Any], to: int) -> Any:
        end = self.info[to]
        if end["host"] == frm["host"]:
            return end["unix"] or ("127.0.0.1", end["port"])
        if not self.cross_host:
            raise ValueError(
                f"the graph links {frm['host']} with {end['host']}, where its actor {to} runs: links between hosts "
                "are authenticated with the graph's key but not encrypted. Pass compile(cross_host=True) to allow "
                "them, or keep the driver and the graph's actors on one host"
            )
        if self.peers is None:
            from ._cluster import ClusterView

            self.peers = {n.peer: n.address for n in ClusterView().nodes()}
        host = self.peers[end["peer"]].rpartition("://")[2].rsplit(":", 1)[0].strip("[]")
        return (host, end["port"])


# ---- an actor's end ---------------------------------------------------------------

_servers: dict[bytes, _Server] = {}


def _hang_up(conn: Connection) -> None:
    """End a connection whose reader may be blocked in it: a shutdown wakes
    the read (closing the descriptor alone may not, on Linux)."""
    try:
        with socket.fromfd(conn.fileno(), socket.AF_INET, socket.SOCK_STREAM) as s:
            s.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass  # closed already


class _Stopped(Exception):
    pass


class _Failed:
    """A call's error, passed on in place of its value."""

    def __init__(self, blob: bytes) -> None:
        self.blob = blob


def serve(executor: Any, phase: str, gid: bytes, arg: Any) -> Any:
    """The actor's ``GRAPH_METHOD``: open the graph's listeners and return
    where they are, then (``run``) run the actor's part until the driver
    stops it, or (``close``) drop a setup that failed."""
    if phase == "open":
        _servers[gid] = _Server(executor, gid, arg["key"], arg["tcp"])
        return _servers[gid].info
    server = _servers.pop(gid) if gid in _servers else None
    if server is None:
        raise RuntimeError("the compiled graph's setup expired before it started: compile it again")
    if phase == "close":
        server.close()
        return None
    return server.run(executor, arg)


class _Server:
    def __init__(self, executor: Any, gid: bytes, key: bytes, tcp: bool) -> None:
        from ._runtime import Runtime

        self.gid, self.key = gid, key
        self.conns: dict[int, Connection] = {}  # by sender, once it said HELLO
        self.frames: dict[tuple[int, int, int], tuple[int, memoryview]] = {}
        self.cond = threading.Condition()
        self.done = self.started = False
        self.listeners: list[Listener] = []
        self.handshakes = threading.BoundedSemaphore(HANDSHAKES)
        ctx = _context.current_task()
        name = f"g{gid.hex()[:12]}-{(ctx.actor if ctx else b'').hex()[:12]}.sock"
        path = os.path.join(os.path.dirname(executor.conn.welcome["peer_socket"]), "direct", name)
        self.info: dict[str, Any] = {"host": HOST, "peer": Runtime.get().info["peer"].hex(), "unix": None, "port": None}
        if len(path.encode()) <= MAX_SOCKET_PATH:
            os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
            old = os.umask(0o177)
            try:
                self.listeners.append(Listener(path, "AF_UNIX", backlog=64))
            finally:
                os.umask(old)
            self.info["unix"] = path
        if tcp or self.info["unix"] is None:
            listener = Listener(("0.0.0.0" if tcp else "127.0.0.1", 0), "AF_INET", backlog=64)
            self.listeners.append(listener)
            self.info["port"] = listener.address[1]
        for listener in self.listeners:
            threading.Thread(target=self.accept, args=(listener,), name="graph-accept", daemon=True).start()
        timer = threading.Timer(SETUP_S, self.expire)
        timer.daemon = True
        timer.start()

    def accept(self, listener: Listener) -> None:
        # The key's challenge runs beside the accepting, each connection on its
        # own: one that connects and never answers holds up no other.
        while not self.done:
            try:
                conn = listener.accept()
            except OSError:
                continue
            if not self.handshakes.acquire(blocking=False):
                conn.close()  # a flood of connections: the graph's own ends retry no handshake, so they come first
                continue
            threading.Thread(target=self.admit, args=(conn,), name="graph-admit", daemon=True).start()

    def admit(self, conn: Connection) -> None:
        """Keep `conn` once it proves it holds the graph's key and says who it
        is, within HANDSHAKE_S; else drop it."""
        late = threading.Timer(HANDSHAKE_S, _hang_up, args=(conn,))
        late.daemon = True
        late.start()
        try:
            deliver_challenge(conn, self.key)
            answer_challenge(conn, self.key)
            kind, _, sender, _ = FRAME.unpack(conn.recv_bytes())
        except (OSError, EOFError, struct.error, multiprocessing.AuthenticationError):
            conn.close()
            return
        finally:
            late.cancel()
            self.handshakes.release()
        if kind != HELLO:
            conn.close()
            return
        with self.cond:
            self.conns[sender] = conn
            self.cond.notify_all()

    def expire(self) -> None:
        if not self.started and _servers.get(self.gid) is self:
            del _servers[self.gid]
            self.close()

    def close(self) -> None:
        with self.cond:
            self.done = True
            self.cond.notify_all()
        for listener in self.listeners:
            listener.close()
        for conn in self.conns.values():
            conn.close()

    def run(self, executor: Any, plan: dict[str, Any]) -> None:
        self.started = True
        out: dict[int, Connection] = {}
        try:
            for b, address in plan["send"].items():
                out[b] = Client(address, authkey=self.key)
                out[b].send_bytes(FRAME.pack(HELLO, 0, plan["me"], 1))
            expect = {*plan["expect"], DRIVER}
            with self.cond:
                if not self.cond.wait_for(lambda: self.done or expect.issubset(self.conns), SETUP_S):
                    raise TimeoutError("the compiled graph's other ends did not connect in time")
                if self.done:
                    return
                incoming = [self.conns[s] for s in expect]
            threading.Thread(target=self.read, args=(incoming,), name="graph-read", daemon=True).start()
            self.loop(executor, plan, out, self.conns[DRIVER])
        except _Stopped:
            pass
        finally:
            self.close()
            for conn in out.values():
                conn.close()

    def read(self, conns: list[Connection]) -> None:
        while not self.done:
            try:
                ready = wait(conns)
            except (OSError, ValueError):  # closed under it: the graph is over
                return
            for conn in ready:
                assert isinstance(conn, Connection)
                try:
                    data = conn.recv_bytes()
                except (EOFError, OSError):  # the driver, or an actor before this one, went away
                    data = FRAME.pack(STOP, 0, 0, 0)
                kind, r, node, ok = FRAME.unpack_from(data)
                with self.cond:
                    if kind == STOP:
                        self.done = True
                    else:
                        self.frames[(kind, r, node)] = (ok, memoryview(data)[FRAME.size :])
                    self.cond.notify_all()
                if kind == STOP:
                    return

    def take(self, key: tuple[int, int, int]) -> tuple[int, memoryview]:
        with self.cond:
            self.cond.wait_for(lambda: key in self.frames or self.done)
            if key not in self.frames:
                raise _Stopped
            return self.frames.pop(key)

    def loop(self, executor: Any, plan: dict[str, Any], out: dict[int, Connection], driver: Connection) -> None:
        from ._executor import _awaitable

        inputs = plan["inputs"]
        r = 0
        while True:
            values: dict[int, Any] = {}
            given: dict[int, Any] = {}
            if inputs is not None:
                _, inputs_frame = self.take((RUN, r, DRIVER))
                given = dict(zip(inputs, _serde.loads(inputs_frame)))

            def get(ref: tuple[str, Any]) -> Any:
                tag, v = ref
                if tag == "c":
                    return v
                if tag == "i":
                    return given[v]
                if v not in values:  # another actor's: read once per invocation
                    ok, frame = self.take((VALUE, r, v))
                    values[v] = _serde.loads(frame) if ok else _Failed(bytes(frame))
                return values[v]

            for k, method, args, kwargs, fanout, to_driver in plan["mine"]:
                a = [get(x) for x in args]
                kw = {name: get(x) for name, x in kwargs.items()}
                failed = next((x for x in (*a, *kw.values()) if isinstance(x, _Failed)), None)
                payload = b""
                if failed is None:
                    try:
                        result = getattr(executor.instance, method)(*a, **kw)
                        if inspect.isawaitable(result):
                            done = asyncio.run_coroutine_threadsafe(_awaitable(result), executor._actor_loop())
                            result = done.result()
                        if fanout or to_driver:
                            payload = _serde.dumps(result)[0]
                        values[k] = result
                    except Exception as e:
                        failed = _Failed(pickle.dumps(executor.err(e)))
                if failed is not None:
                    values[k], payload = failed, failed.blob
                frame = FRAME.pack(VALUE, r, k, failed is None) + payload
                for b in fanout:
                    out[b].send_bytes(frame)
                if to_driver:
                    driver.send_bytes(frame)
            r += 1
