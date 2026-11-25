"""The client side of a run: connection, submission and outcome delivery.

The first submission connects to the local peer (starting it if needed) and
creates a run with this interpreter, working directory and import path, so
executors run the same code as the driver. A dispatcher thread turns session
events into resolved futures; waiting never holds the GIL in Rust.
"""

from __future__ import annotations

import atexit
import collections
import fnmatch
import inspect
import itertools
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
import threading
import time
import warnings
import weakref
from typing import Any, Callable

from . import _cluster, _context, _objects, _project, _serde, _values
from ._errors import ChimaeraError, DataLost, Infeasible, QuorumLost, SerializationError, divergence
# VERSION: the release, as the extension module and its peer binary report it.
from ._native import VERSION, Client, blake3_digest, child_id, derive_id, socket_path, vshard_of

# Functions whose code digest is remembered (newest kept).
CODE_CACHE_MAX = 1024
# CH120: the same code sent as this many distinct functions.
REWRAP_WARN_CALLS = 100
CALL_FUNCTION, CALL_ACTOR_CTOR, CALL_ACTOR_METHOD, CALL_MAP_ITEM = 0, 1, 2, 3
EFFECTS_CODE = {"none": 0, "idempotent": 1, "external": 2}
# Items a generator call may run ahead of its consumer (unless max_in_flight says),
# and their stored bytes (unless max_in_flight_bytes says).
STREAM_WINDOW = 64
STREAM_WINDOW_BYTES = 256 << 20
MAX_CAPTURE_BYTES = 256 << 20
MAX_PENDING_CALLS = 1_000_000
WARN_CAPTURE_BYTES = 1 << 20


def default_home() -> str:
    return os.environ.get("CHIMAERA_HOME") or os.path.join(os.path.expanduser("~"), ".chimaera", "local")


def _peer_binary(*, skip: str = "") -> str:
    """The `chimaera` peer binary: $CHIMAERA_BIN, the one a wheel installs
    with this package, a development checkout's build, then PATH. `skip` is a
    path to pass over (the `chimaera` command's launcher, which would run
    itself)."""
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(os.path.dirname(here))  # <repo>/python/chimaera
    candidates = [
        os.environ.get("CHIMAERA_BIN", ""),
        os.path.join(here, "_bin", "chimaera"),
        os.path.join(repo, "target", "release", "chimaera"),
        os.path.join(repo, "target", "debug", "chimaera"),
        shutil.which("chimaera") or "",
    ]
    for c in candidates:
        if c and os.path.isfile(c) and os.access(c, os.X_OK) and not (skip and _same_file(c, skip)):
            return c

    def shown(c: str) -> str:
        return c + (" (not executable: chmod +x it)" if os.path.isfile(c) and not os.access(c, os.X_OK) else "")

    raise ChimaeraError(
        "cannot find the `chimaera` peer binary (looked for "
        + ", ".join(shown(c) for c in candidates if c)
        + "). A wheel built with `make wheel` includes it; in a source checkout, build it with "
        "`cargo build --release -p chimaera-peer`, or set CHIMAERA_BIN to its path."
    )


def _same_file(a: str, b: str) -> bool:
    try:
        return os.path.samefile(a, b)
    except OSError:
        return False


def peer_socket(home: str) -> str:
    # The directory must exist first: the rule hashes its canonical path.
    os.makedirs(home, exist_ok=True)
    return socket_path(home, "peer.sock")


def _binary_description(path: str) -> str:
    """`path size mtime_ns`, as the peer writes it to `peer.binary`."""
    real = os.path.realpath(path)
    st = os.stat(real)
    return f"{real}\n{st.st_size}\n{st.st_mtime_ns}\n"


def _check_peer_binary(home: str) -> None:
    """Warn (CH111) when the running local peer is not the binary this
    program would start: rebuilt or upgraded since, it keeps serving until it
    idles out, and its behavior is the old one."""
    try:
        with open(os.path.join(home, "peer.binary")) as f:
            running = f.read()
        wanted = _binary_description(_peer_binary())
    except (OSError, ChimaeraError):
        return  # an older peer, or no binary here to compare with
    if running != wanted:
        warnings.warn(
            f"CH111: the local peer in {home} runs {running.split(chr(10))[0]}, which is not the chimaera binary "
            f"this program would start ({wanted.split(chr(10))[0]}, rebuilt or upgraded since?). It keeps serving "
            "until it is idle for ten minutes; run `chimaera stop` to switch now (the next call starts the new one).",
            stacklevel=4,
        )


def _check_cluster_version(name: str, version: str) -> None:
    """Warn (CH116) when a remote cluster runs another Chimaera version: the
    wire version matched on connect, but behavior may not."""
    if version and version != VERSION:
        warnings.warn(
            f"CH116: cluster {name!r} runs Chimaera {version}, this program {VERSION}. The protocol matches, but "
            "behavior may differ; install the cluster's version here (or upgrade the cluster) if calls misbehave.",
            stacklevel=4,
        )


# What a local peer, and so every executor it starts, takes from the driver
# that happens to start it: where programs and the user's files are, and
# Chimaera's own settings. Nothing else, or that driver's variables would reach
# the calls of every later run on the peer (see run_env for what a run ships).
PEER_ENV = ("PATH", "HOME", "USER", "LOGNAME", "SHELL", "TMPDIR", "TZ", "LANG")
# A local peer's log past this size is rotated when the next peer starts.
PEER_LOG_MAX = 16 << 20
# How a local peer exits when another peer holds its directory (runtime.rs
# DIR_HELD_EXIT): the program that started it waits for that one's socket.
DIR_HELD_EXIT = 75


def _peer_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if k in PEER_ENV or k.startswith(("LC_", "CHIMAERA_"))}


def _ensure_local_peer(home: str) -> None:
    sock = peer_socket(home)
    if os.path.exists(sock):
        _check_peer_binary(home)
        return
    log_path = os.path.join(home, "peer.log")
    try:
        if os.path.getsize(log_path) > PEER_LOG_MAX:
            os.replace(log_path, log_path + ".1")  # keep one previous log, not every one
    except OSError:
        pass
    with open(log_path, "ab") as log:
        start = log.tell()
        peer = subprocess.Popen(
            [_peer_binary(), "--dir", home, "peer", "start", "--idle-timeout", "600"],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            close_fds=True,
            env=_peer_env(),
        )
    deadline = time.monotonic() + 20
    while not os.path.exists(sock):
        # Exiting before its socket exists is a failed start, unless the
        # peer found another one, starting there now, holding the directory.
        code = peer.poll()
        if code is not None and code != DIR_HELD_EXIT:
            raise ChimaeraError(_start_failure(log_path, start, code))
        if time.monotonic() > deadline:
            raise ChimaeraError(f"the local peer did not start; see {log_path}")
        time.sleep(0.02)


def _start_failure(log_path: str, start: int, code: int) -> str:
    """Why a local peer exited while starting: the error it printed last
    (`chimaera: ...`), else the end of what it wrote to `log_path` after
    offset `start`."""
    try:
        with open(log_path, "rb") as f:
            f.seek(start)
            lines = [ln for ln in f.read().decode(errors="replace").splitlines() if ln.strip()]
    except OSError:
        lines = []
    errors = [ln.removeprefix("chimaera: ") for ln in lines if ln.startswith("chimaera: ")]
    why = errors[-1] if errors else "\n".join(lines[-20:]) or "it printed nothing"
    return f"the local peer exited while starting (status {code}): {why}\n(its log: {log_path})"


def _check_socket_allowed(path: str) -> None:
    """Raise, naming the call, the path and the remedy, when a sandbox, a
    seccomp profile or the socket's permissions refuse this process the Unix
    socket at `path`, so a live peer's socket is not taken for a dead one's."""
    import socket

    call = "socket"
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            call = "connect"
            s.connect(path)
    except PermissionError as e:
        raise ChimaeraError(
            f"{call}() on the Unix socket {path}: {e.strerror} (os error {e.errno}). Unix sockets are refused "
            "here, by a sandbox or seccomp profile that denies them or by the socket's permissions (another "
            "user's peer): allow Unix sockets for this program, or set CHIMAERA_HOME to a directory of your own "
            "on a local disk"
        ) from None
    except OSError:
        pass  # nothing listens there: a peer that died left it


# The call whose result this thread is loading (`Runtime.read_value`): blobs
# its frame links are fetched in its name, so a lost one is made again.
_reading = threading.local()

# Why a lost value that no call returned stays lost.
_NO_CALL = "the program stored it (ch.put, or an argument), and no call can make it again"


def _no_call(durability: str) -> str:
    """Why a lost value no call returned stays lost, naming the durability
    class its ch.put chose (`durability`; "" for the default)."""
    return f"{_NO_CALL} (it was stored with durability={durability!r})" if durability else _NO_CALL


def read_object(
    object_dir: str, run: bytes, obj: bytes, task: bytes | None = None, *, decode: bool = True, durability: str = ""
) -> Any:
    """Load an object from this peer's store, asking the peer to fetch it
    first when another peer produced it. `task` is the call whose result it
    is: when every copy is lost, the peer runs that call again if it can.
    With ``decode=False`` it is only made local (and None returned).
    `durability` is a ch.put value's class, named if it is lost."""
    path = _serde.object_path(object_dir, run, obj)
    if not decode and os.path.exists(path):
        return None
    try:
        return _serde.read_file(path) if decode else None
    except FileNotFoundError:
        pass
    try:
        Runtime.get().localize(run, obj, task)
    except FileNotFoundError as e:
        # Every peer that held it is gone (or freed it), and its call could
        # not run again (the peer's message says why).
        why = e if task is not None else _no_call(durability)
        raise DataLost(f"value {obj.hex()} of run {run.hex()} is no longer stored on any live peer: {why}") from e
    if not decode:
        return None
    try:
        return _serde.read_file(path)
    except FileNotFoundError as e:  # a part or buffer it links, stored apart from it
        raise DataLost(f"part of value {obj.hex()} of run {run.hex()} is no longer stored on any live peer") from e


# Client variables every run ships to its executors: locale and hashing only.
# Anything else (secrets, PATH, VIRTUAL_ENV, CUDA_VISIBLE_DEVICES) stays on the
# client unless named in CHIMAERA_ENV_PASSTHROUGH or passed to ch.init(env=...);
# executors otherwise see their own peer's environment. The run's variables
# are replicated with the run record, so they must not carry secrets.
# Calls a second a driver submits before its calls spread over the run's
# groups: below it one group commits them easily, and keeping a DAG in one
# group saves the notices between groups that its dependencies would need.
SPREAD_RATE = float(os.environ.get("CHIMAERA_SPREAD_RATE", "2000"))

DEFAULT_PASSTHROUGH = ("LANG", "LC_ALL", "LC_CTYPE", "TZ", "PYTHONHASHSEED")


def run_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """The environment variables a new run ships to its executors."""
    listed = os.environ.get("CHIMAERA_ENV_PASSTHROUGH", "")
    patterns = [*DEFAULT_PASSTHROUGH, *(p.strip() for p in listed.split(",") if p.strip())]
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("CHIMAERA_") and any(fnmatch.fnmatchcase(k, p) for p in patterns)
    }
    for k, v in (extra or {}).items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise TypeError(f"env must map str to str; got {k!r}: {type(v).__name__}")
        env[k] = v
    return env


def machine_digest() -> str:
    """A short digest of this machine's identity (CHIMAERA_MACHINE_ID, else
    its host name): a call that fails on a missing file can tell whether it
    ran on another machine (CH123)."""
    import socket

    return blake3_digest((os.environ.get("CHIMAERA_MACHINE_ID") or socket.gethostname()).encode()).hex()[:8]


def env_digests(shipped: dict[str, str]) -> str:
    """Short digests of the names of the program's environment variables that
    a run does not ship (names and values never travel): a call that fails
    reading one can then say how to pass it (CH122)."""
    names = (k for k in os.environ if k not in shipped and not k.startswith("CHIMAERA_"))
    return ",".join(sorted({blake3_digest(k.encode()).hex()[:8] for k in names}))


def code_fingerprint() -> str:
    """Identity of the driver's own source files (loaded modules outside the
    interpreter and installed packages): path, size and modification time.
    Executors are shared only by runs with the same fingerprint, so a module
    edited between runs is never served from a warm executor's stale import.
    Modules first imported inside tasks are not covered."""
    paths = sysconfig.get_paths()
    skip = tuple({paths[k] for k in ("stdlib", "platstdlib", "purelib", "platlib") if k in paths})
    lines = []
    for mod in list(sys.modules.values()):
        path = getattr(mod, "__file__", None)
        if not path or path.startswith(skip) or "site-packages" in path:
            continue
        try:
            st = os.stat(path)
        except OSError:
            continue
        lines.append(f"{path}\0{st.st_size}\0{st.st_mtime_ns}")
    return blake3_digest("\n".join(sorted(lines)).encode())[:16].hex()


def _connect_remote(target: _cluster.Target, client_name: str) -> Client:
    """Connect to the first reachable peer of a remote cluster."""
    errors = []
    for address in target.addresses:
        try:
            return Client(address, client_name, target.token, target.ca)
        except ConnectionError as e:
            errors.append(f"{address}: {e}")
    raise ChimaeraError(f"cannot reach cluster {target.name!r}: " + "; ".join(errors))


class _ForkedClient:
    """The client in a process forked from one using Chimaera: the threads
    that drive the connection were not copied, so a call through it would
    wait forever (as ray.get does in a forked DataLoader worker)."""

    def __getattr__(self, name: str) -> Any:
        raise ChimaeraError(
            "CH113: this process was forked from one using Chimaera, so it cannot submit, wait or fetch through "
            "its parent's connection. Start it with multiprocessing.get_context('spawn') or 'forkserver', or "
            "make the work a Chimaera call instead of a child process."
        )


def _after_fork_in_child() -> None:
    rt = Runtime._instance
    if rt is not None:
        rt.forked = True  # its values belong to the parent: dropping them here frees nothing
        rt.client = _ForkedClient()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


class Runtime:
    _instance: Runtime | None = None
    _lock = threading.Lock()
    forked = False

    @classmethod
    def get(cls) -> Runtime:
        rt = cls._instance
        if rt is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = Runtime()
                rt = cls._instance
        return rt

    @classmethod
    def current(cls) -> Runtime | None:
        return cls._instance

    def __init__(
        self,
        home: str | None = None,
        detached: bool | None = None,
        env: dict[str, str] | None = None,
        cluster: str | None = None,
        tenant: str | None = None,
    ) -> None:
        self.home = home or default_home()
        # How workers' output is printed here: text, "(fn pid=N) line", or one
        # JSON object per line for log collectors.
        self.log_format = os.environ.get("CHIMAERA_LOG_FORMAT", "text").strip().lower() or "text"
        if self.log_format not in ("text", "json"):
            raise ValueError(f"CHIMAERA_LOG_FORMAT={self.log_format!r}: expected 'text' or 'json'")
        self.in_executor = os.environ.get("CHIMAERA_EXECUTOR") == "1"
        client_name = f"python-{platform.python_version()} chimaera-{VERSION}"
        target = None if self.in_executor else _cluster.resolve(cluster)
        self.remote = target is not None
        if target is not None:
            self.client = _connect_remote(target, client_name)
        else:
            self.client = self._connect_local(client_name)
        self.info = self.client.info
        # Executors get their run's tenant in its environment; a driver
        # credential names its own.
        self.tenant: str = (
            tenant
            or os.environ.get("CHIMAERA_TENANT")
            or _cluster.tenant_of(target.token if target is not None else None)
            or "default"
        )
        self.client.set_tenant(self.tenant)
        self.inline_limit: int = self.info["inline_limit"]
        if target is not None:
            _check_cluster_version(target.name, self.info.get("version", ""))
        if self.remote:
            # The peer's store is on another machine: values are uploaded as
            # blobs and fetched into a local cache to be read.
            self.object_dir: str = os.path.join(self.home, "remote", self.info["peer"].hex(), "objects")
            self.blob_sink: Any = _cluster.RemoteBlobSink(self.client)
        else:
            self.object_dir = self.info["object_dir"]
            self.blob_sink = _values.LocalBlobSink(self.object_dir)
            _objects.enable(self.object_dir)
        # A blob another peer stored is fetched into this peer's store on read.
        _serde.localize_hook = self.localize
        self._futures: weakref.WeakValueDictionary[bytes, Any] = weakref.WeakValueDictionary()
        # Call key -> the ch.put futures and arena links it reads, held until the call has an
        # outcome (its own future may be dropped first): a value is never
        # released under a reader still running.
        self._reads: dict[bytes, list[Any]] = {}
        self._streams: weakref.WeakValueDictionary[bytes, Any] = weakref.WeakValueDictionary()
        self.cond = threading.Condition()
        # id(callable) -> (callable, digest), newest last; bounded, since a
        # program wrapping a fresh lambda per call would keep every one.
        self._code: collections.OrderedDict[int, tuple[Any, bytes]] = collections.OrderedDict()
        # CH120: code object -> how many distinct functions of it were sent.
        self._rewraps: collections.Counter[Any] = collections.Counter()
        self._registered: set[bytes] = set()
        self._seq = itertools.count()
        self._nonce = os.urandom(8)
        self._waiters: dict[int, list[Any]] = {}
        # Answers to actor creations that came in before their creator waited.
        self._early: dict[int, tuple[Any, ...]] = {}
        # Driver backpressure (wait_for_room): arguments of unfinished calls.
        self._pending_bytes = 0
        self._pending_calls = 0
        # Held until their outcome arrives, even if the caller dropped them,
        # so exit can wait for them; maps not yet read to the end; generator
        # calls (nobody reads those after exit).
        self._unfinished: dict[bytes, Any] = {}
        # Call key -> the function's name, for labelling what calls print.
        self._names: collections.OrderedDict[bytes, str] = collections.OrderedDict()
        self.unread_errors = 0  # failed calls whose errors nothing read (see _future._note_unread)
        self._open_maps: set[Any] = set()
        self._stream_keys: set[bytes] = set()
        self._room_warned: set[str | None] = set()
        self._releases: list[bytes] = []
        self.run: bytes | None = None
        # Vshards (in different groups) the run's calls are spread over; empty
        # on a single peer, for a keyed run, or after attaching to a run.
        self._spread: list[int] = []
        # Submission rate over 250 ms windows (see SPREAD_RATE).
        self._rate, self._rate_n, self._rate_t0 = 0.0, 0, time.monotonic()
        self.detached = False
        if not self.in_executor:
            want_detached = detached if detached is not None else os.environ.get("CHIMAERA_DETACH") == "1"
            ship = {
                **run_env(env),
                "CHIMAERA_CODE_FINGERPRINT": code_fingerprint(),
                # Executors log at the driver's level (stderr reaches the driver).
                "CHIMAERA_LOG_LEVEL": logging.getLevelName(logging.getLogger().getEffectiveLevel()),
                # A remote peer's workers run its own Python: they name a mismatch
                # when the program's functions will not load (CH115).
                "CHIMAERA_DRIVER_PYTHON": f"{sys.version_info.major}.{sys.version_info.minor}",
                "CHIMAERA_TENANT": self.tenant,
            }
            ship["CHIMAERA_DRIVER_ENV"] = env_digests(ship)
            ship["CHIMAERA_DRIVER_MACHINE"] = machine_digest()
            sys_path = [p or os.getcwd() for p in sys.path]
            # A remote peer has no copy of the project's own modules: ship them.
            # A program `chimaera submit` started also ships its project: its
            # calls run on other peers, where the project was never unpacked.
            shipping = self.remote or os.environ.get("CHIMAERA_SHIP_PROJECT") == "1"
            project = _project.pack(sys_path) if shipping else None
            if project is not None:
                ship[_project.ENV] = project[0].hex()
            try:
                self.run, self.detached, self._spread = self.client.create_run(
                    self.tenant,
                    "",
                    want_detached,
                    sys.executable,
                    os.getcwd(),
                    sys_path,
                    ship,
                    " ".join(sys.argv)[:200],
                )
            except ConnectionError as e:
                if str(e).startswith("quorum lost"):
                    raise QuorumLost(str(e)) from None
                raise
            _project.ship(self.blob_sink, self.run, project)
            self.client.attach(self.run)
            atexit.register(self._finish)
        self._dispatcher = threading.Thread(target=self._dispatch_loop, name="chimaera-events", daemon=True)
        self._dispatcher.start()

    @property
    def run_id(self) -> str:
        return self.run.hex() if self.run else ""

    # ---- submission -------------------------------------------------------

    def new_key(self, spread: bool = False, deps: list[bytes] | tuple[bytes, ...] = ()) -> bytes:
        """Deterministic task key: a call inside a task derives from its parent
        and sequence number, so a retried parent re-submits the same children.

        `spread` (plain function calls): a child stays in its parent's
        vshard, so its group commits the whole subtree and cancelling or
        budgeting the parent reaches it; a driver's call goes to a vshard of the
        run's spread: its inputs' when they share one, the run's own when
        they do not, and while the driver submits faster than SPREAD_RATE the
        next by sequence number, so a large run is committed by several group
        leaders. Actor and generator calls stay in the run's own vshard, with
        the actors whose events place them."""
        ctx = _context.current_task()
        if ctx is not None:
            run, seq = ctx.run, ctx.next_child().to_bytes(8, "little")
            if spread:
                return run + child_id(ctx.task, [run, b"child", seq])
            return run + child_id(run, [ctx.task, b"child", seq])
        if self.run is None:
            raise ChimaeraError("no run: submit from a driver or from inside a task")
        run, seq = self.run, next(self._seq)
        parts = [self._nonce, seq.to_bytes(8, "little")]
        if not (spread and self._spread):
            return run + child_id(run, parts)
        homes = {vshard_of(d[16:]) if d[:16] == run else -1 for d in deps}
        only = next(iter(homes)) if len(homes) == 1 else -1
        if only in self._spread:
            v = only
        elif deps or not self._submitting_fast():
            # Inputs from several vshards: the run's own collects their notices
            # (spreading such calls would multiply the notices between groups).
            return run + child_id(run, parts)
        else:
            v = self._spread[seq % len(self._spread)]
        return run + derive_id(int.from_bytes(run[:2], "big"), v, [run, *parts])

    def _submitting_fast(self) -> bool:
        """Whether this driver submits faster than SPREAD_RATE: the last full
        250 ms window's rate, or the current window's count already."""
        now = time.monotonic()
        if now - self._rate_t0 >= 0.25:
            self._rate, self._rate_n, self._rate_t0 = self._rate_n / (now - self._rate_t0), 0, now
        self._rate_n += 1
        return self._rate >= SPREAD_RATE or self._rate_n >= SPREAD_RATE / 4

    def code_digest(self, obj: Any) -> bytes:
        cached = self._code.get(id(obj))
        if cached is not None and cached[0] is obj:
            self._code.move_to_end(id(obj))
            return cached[1]
        preflight_captures(obj)
        blob = _serde.dump_code(obj)
        digest = blake3_digest(blob)
        if digest not in self._registered:
            self.client.put_code(digest, blob)
            self._registered.add(digest)
        self._code[id(obj)] = (obj, digest)
        if len(self._code) > CODE_CACHE_MAX:
            self._code.popitem(last=False)
        self._note_rewrap(obj)
        return digest

    def _note_rewrap(self, obj: Any) -> None:
        """CH120: the same code wrapped as a new function again and again
        (ch.remote(lambda ...) inside a loop). Each new function is pickled
        and hashed on its first call, about 0.3 ms, 6x the cost of a call
        through a function defined once."""
        code = getattr(obj, "__code__", None)
        if code is None:
            return
        self._rewraps[code] += 1
        if self._rewraps[code] == REWRAP_WARN_CALLS:
            warnings.warn(
                f"CH120: {getattr(obj, '__qualname__', 'a function')} ({code.co_filename}:{code.co_firstlineno}) "
                f"was sent as a new function {REWRAP_WARN_CALLS} times, each pickled and hashed again. Define the "
                "remote function once, outside the loop, and call it many times.",
                stacklevel=5,
            )
        if len(self._rewraps) > CODE_CACHE_MAX:
            self._rewraps = collections.Counter(dict(self._rewraps.most_common(CODE_CACHE_MAX // 2)))

    def _connect_local(self, client_name: str) -> Client:
        # Workers, and programs a call starts (``chimaera submit``), use the
        # peer that runs them rather than starting one of their own.
        socket = os.environ.get("CHIMAERA_PEER_SOCKET")
        given = socket is not None
        if socket is None:
            _ensure_local_peer(self.home)
            socket = peer_socket(self.home)
        try:
            return Client(socket, client_name)
        except ConnectionError:
            # A stale socket from a peer that died: remove it and start fresh.
            if given:
                raise
            _check_socket_allowed(socket)
            try:
                os.unlink(socket)
            except FileNotFoundError:
                pass
            _ensure_local_peer(self.home)
            return Client(socket, client_name)

    def localize(self, run: bytes, obj: bytes, task: bytes | None = None, traffic: str = "") -> None:
        """Make an object readable at its path under ``object_dir``: the local peer
        fetches it into its store (running `task` again if it is the result of
        that call and every copy is lost); a remote client downloads it into
        its cache. Without `task`, the call whose result this thread is
        loading (`read_value`) is named: a blob its frame links is part of it.
        `traffic` ("shuffle"; default: a call reads it now) is the class its
        holder paces the bytes as."""
        if task is None:
            task = getattr(_reading, "task", None)
        if self.remote:
            self.client.fetch_object(run, obj, _serde.object_path(self.object_dir, run, obj), task)
        else:
            self.client.localize(run, obj, task, traffic)

    def pack(self, key: bytes, value: Any, site: str | None = None, holds: list[Any] | None = None) -> dict[str, Any]:
        """Encode a call frame by size: inline when small, else a file for this
        call; large buffers and arguments become blobs shared by every call
        that passes the same bytes (see _values). Arena arrays linked for the
        call go into `holds`; without it they stay for the run."""
        run = key[:16]
        enc = _values.encode_frame(value, run, self.blob_sink, None if self.in_executor else self._release_values)
        for hold in enc.holds:
            if holds is None:
                hold.keep()
            else:
                holds.append(hold)
        _values.repeats.note(site, enc)
        if enc.collection is not None:
            _values.collection_lint.note(site, *enc.collection)
        packed = {
            "args_size": enc.size,
            "args_oob": enc.oob,
            "blobs": list(enc.blobs.items()),
            "read_bytes": enc.total_bytes,
        }
        if enc.size <= self.inline_limit:
            data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in enc.parts)
            return {**packed, "args_inline": data, "args_object": None}
        if self.remote:
            # Uploaded like any blob: named by its content, read by the peer from its store.
            data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in enc.parts)
            obj = blake3_digest(data)[:16]
            self.blob_sink.put(run, obj, data)
            return {**packed, "args_inline": None, "args_object": obj}
        obj = child_id(run, [key[16:], b"args"])
        _serde.write_file(_serde.object_path(self.object_dir, run, obj), enc.parts)
        return {**packed, "args_inline": None, "args_object": obj}

    def _release_values(self, run: bytes, object_ids: list[bytes]) -> None:
        """Free stored values nothing refers to any more, on every peer."""
        self.client.release_objects(run, object_ids)

    def release_later(self, key: bytes) -> None:
        """A future was dropped: unpin its result (sent in batches)."""
        self._releases.append(key)

    def submit_watch(self, key: bytes, future: Any) -> None:
        self._futures[key] = future
        self.client.watch([key])

    def submit(self, specs: list[dict[str, Any]], futures: list[Any], send: bool = True) -> None:
        for s, f in zip(specs, futures):
            self._futures[s["key"]] = f
            if not self.in_executor:
                name = s["policy"].get("fn_name", "")
                self._names[s["key"]] = ".".join(name.split(".")[-2:]) if s.get("method") else name.rsplit(".", 1)[-1]
                if len(self._names) > 65536:
                    self._names.popitem(last=False)
                with self.cond:
                    self._pending_bytes += f._arg_bytes
                    self._pending_calls += 1
                    self._unfinished[s["key"]] = f
                f._on_done(self._release_room)
        if send:  # else the direct path sends them (_direct.take)
            self.client.submit(specs)

    def _release_room(self, f: Any) -> None:
        with self.cond:
            self._pending_bytes -= f._arg_bytes
            self._pending_calls -= 1
            self._unfinished.pop(f.key, None)
            self._stream_keys.discard(f.key)
            self.cond.notify_all()

    def wait_for_room(self, nbytes: int, site: str | None) -> None:
        """Driver backpressure: a loop submitting faster than calls finish
        waits here while their unconsumed arguments would pass half the
        peer's memory, or a million calls are outstanding (Ray has no bound;
        its object store fills instead). Never inside a task: a waiting task
        would hold its CPU."""
        if self.in_executor or threading.current_thread() is self._dispatcher:
            return  # a done-callback submitting more must not stop outcomes
        limit = int(os.environ.get("CHIMAERA_MAX_PENDING_BYTES") or 0)
        limit = limit or max(256 << 20, int(self.info["memory_bytes"]) // 2)
        with self.cond:
            if self._pending_calls < MAX_PENDING_CALLS and (
                self._pending_bytes == 0 or self._pending_bytes + nbytes <= limit
            ):
                return
        if site not in self._room_warned:
            self._room_warned.add(site)
            warnings.warn(
                f"CH110: calls from {site} are submitted faster than they finish; {self._pending_calls} are "
                f"outstanding with {self._pending_bytes >> 20} MiB of arguments, so .remote() waits for room. "
                "Use fn.map(items) to stream them, or wait on results as they come (ch.as_completed).",
                stacklevel=4,
            )
        self.client.flush()
        with self.cond:
            while self._pending_calls >= MAX_PENDING_CALLS or (
                self._pending_bytes > 0 and self._pending_bytes + nbytes > limit
            ):
                self.cond.wait(1.0)

    def wait_actor(self, seq: int, timeout: float = 60.0) -> tuple[Any, ...]:
        box: list[Any] = []
        with self.cond:
            early = self._early.pop(seq, None)
            if early is not None:
                return early
            self._waiters[seq] = box
            deadline = time.monotonic() + timeout
            while not box:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise TimeoutError("timed out creating the actor")
                self.cond.wait(left)
        return box[0]

    # ---- outcomes ---------------------------------------------------------

    def _dispatch_loop(self) -> None:
        while True:
            if self._releases:
                batch, self._releases = self._releases, []
                self.client.flush()  # a direct call's record goes before its release
                self.client.release(batch)
            events = self.client.next_events(0.5, 4096)
            if events is None:
                break
            if not events:
                continue
            changed = False
            for ev in events:
                tag = ev[0]
                if tag == "outcome":
                    self._reads.pop(ev[1], None)
                    f = self._futures.get(ev[1])
                    if f is not None:
                        f._set(ev[2])
                        changed = True
                elif tag == "acks":
                    for key, status, reason, outcome in ev[2]:
                        if status == "Rejected" or outcome is not None:
                            self._reads.pop(key, None)
                        f = self._futures.get(key) if key else None
                        if f is None:
                            continue
                        if status == "Rejected":
                            if reason.startswith("infeasible"):
                                exc: ChimaeraError = Infeasible(reason)
                            elif reason.startswith("workflow divergence"):
                                exc = divergence(reason)
                            elif reason.startswith("quorum lost"):
                                exc = QuorumLost(reason)
                            else:
                                exc = ChimaeraError(reason)
                            f._set_exception(exc)
                            changed = True
                        elif outcome is not None:
                            f._set(outcome)
                            changed = True
                elif tag == "stream_item":
                    s = self._streams.get(ev[1])
                    if s is not None:
                        s._add(ev[3], ev[4])
                        changed = True
                elif tag == "log":
                    _, key, stream, text, pid = ev
                    out = sys.stderr if stream == "stderr" else sys.stdout
                    name = self._names.get(key)
                    if self.log_format == "json":
                        task = key.hex() if key else ""
                        for line in text.splitlines():
                            rec = {"time": time.time(), "task": task, "function": name or "", "pid": pid, "stream": stream}
                            out.write(json.dumps({**rec, "line": line}) + "\n")
                    else:
                        label = f"({name} pid={pid})" if name else f"(pid={pid})"
                        for line in text.splitlines():
                            out.write(f"{label} {line}\n")
                    out.flush()
                elif tag in ("actor", "error"):
                    # Sending a creation releases the GIL, so its answer can
                    # come in before its creator waits for it (wait_actor).
                    with self.cond:
                        box = self._waiters.pop(ev[1], None)
                        if box is None:
                            self._early[ev[1]] = ev
                        else:
                            box.append(ev)
                            changed = True
            # Drop this batch's last future and stream before blocking for the
            # next: held here, a dropped future would outlive the program's
            # last reference (and keep its result and its call's inputs).
            f = s = None
            if changed:
                with self.cond:
                    self.cond.notify_all()
        with self.cond:
            self.cond.notify_all()

    def read_value(self, outcome: tuple[Any, ...], task: bytes | None = None, *, durability: str = "") -> Any:
        """Load a stored value. `task` is the call that returned it: when its
        file, or a blob its frame links, is lost with every peer that held it,
        the peer runs that call again (see ``localize``). `durability`: a
        ch.put value's class, named if it is lost."""
        _, inline, run, obj, _size, _oob = outcome
        outer, _reading.task = getattr(_reading, "task", None), task
        try:
            if inline is not None:
                run_dir = _serde.run_dir(self.object_dir, run)
                # A large bytes value is a frame of a few bytes naming its blob
                # (an arena array or bytes, linked at the put): read it with one
                # pread, not through a mapping whose pages fault in as copied
                # (2,048 reads of 1 MiB: 870 us each mapped, 500 with pread).
                fast = _serde._plain_bytes(inline, -1, run_dir)
                if fast is not None:
                    return fast
                try:
                    return _serde.loads(inline, run_dir)
                except FileNotFoundError as e:  # a blob it links, and its call could not run again
                    where = f"part of value {obj.hex()} of run {run.hex()}"
                    why = e if task is not None else _no_call(durability)
                    raise DataLost(f"{where} is no longer stored on any live peer: {why}") from e
            return read_object(self.object_dir, run, obj, task, durability=durability)
        finally:
            _reading.task = outer

    def _finish(self, cancel: bool = False) -> None:
        try:
            if not self.detached:
                if not cancel:
                    self._wait_for_work()
                    self._report_unread()
                self.client.finish_run()
            else:
                self.client.flush()
            # Give the session a moment to deliver the finish message.
            time.sleep(0.05)
            self.client.close()
        except Exception as e:  # interpreter is exiting; report, do not raise
            sys.stderr.write(f"chimaera: could not finish run {self.run_id} cleanly: {e}\n")

    def shutdown(self, cancel: bool) -> None:
        """Finish now what exit would (see _finish), and stop being the
        process's runtime: the next call connects again."""
        atexit.unregister(self._finish)
        self._finish(cancel)

    def _report_unread(self) -> None:
        """At exit, the calls that failed while nothing read their errors: the
        ones still referenced now, and a count of those not shown before."""
        from ._future import UNREAD_SHOWN, _note_unread, _unread_failure

        _note_unread(self, [f for f in list(self._futures.values()) if _unread_failure(f)])
        if self.unread_errors > UNREAD_SHOWN:
            more = self.unread_errors - UNREAD_SHOWN
            sys.stderr.write(f"chimaera: {more} more failed call{'s' * (more != 1)} whose errors nothing read\n")

    def _wait_for_work(self) -> None:
        """At exit, finish the script's work, as concurrent.futures waits for
        its pool: calls it never waited on, and maps it did not read to the
        end. Generator calls are stopped (nobody reads their items now). A
        script that failed or was interrupted cancels instead, and Ctrl-C
        during the wait cancels what is left; finish_run then cancels it."""
        for key in list(self._stream_keys):
            if key in self._unfinished:
                self.client.cancel(key, False)
        if getattr(sys, "last_exc", None) is None and getattr(sys, "last_value", None) is None:
            from ._future import start_pending

            start_pending()  # maps made but never read run now, as they would have
        maps = list(self._open_maps)
        calls = sum(1 for k in self._unfinished if k not in self._stream_keys)
        if not calls and not maps:
            return
        what = " and ".join(
            p for p in (f"{calls} unfinished call{'s' * (calls != 1)}" if calls else "",
                        f"{len(maps)} unread map{'s' * (len(maps) != 1)}" if maps else "") if p
        )
        failed = getattr(sys, "last_exc", None) or getattr(sys, "last_value", None)
        if failed is not None:
            why = "was interrupted" if isinstance(failed, KeyboardInterrupt) else "failed"
            sys.stderr.write(f"chimaera: cancelling {what}: the script {why}\n")
            return
        sys.stderr.write(
            f"chimaera: waiting for {what} before exiting (Ctrl-C cancels them; "
            "ch.init(detach=True) lets work outlive the script)\n"
        )
        try:
            failures, first = 0, None
            for m in maps:
                for item in m.outcomes():
                    if item.error is not None:
                        failures += 1
                        first = first or item.error
            self.client.flush()
            with self.cond:
                while any(k not in self._stream_keys for k in self._unfinished):
                    self.cond.wait(1.0)
            if failures:
                sys.stderr.write(f"chimaera: {failures} map item{'s' * (failures != 1)} you did not read failed; the first: {first!r}\n")
        except KeyboardInterrupt:
            sys.stderr.write("chimaera: cancelling the rest\n")


def preflight_captures(obj: Any) -> None:
    """Fail before submission if a captured variable cannot be serialized, and
    warn when one is large (it is snapshotted and shipped once)."""
    fn = obj.__init__ if inspect.isclass(obj) else obj
    fn = getattr(fn, "__func__", fn)
    if not inspect.isfunction(fn):
        return
    try:
        cv = inspect.getclosurevars(fn)
    except (TypeError, ValueError):
        return
    captured = {**{f"global {k}": v for k, v in cv.globals.items()}, **{f"closure {k}": v for k, v in cv.nonlocals.items()}}
    where = f"{fn.__code__.co_filename}:{fn.__code__.co_firstlineno}"
    for name, value in captured.items():
        if inspect.ismodule(value) or callable(value) and getattr(value, "__module__", None) not in (None, "__main__"):
            continue
        try:
            size = len(_serde.dump_code(value))
        except Exception as e:
            raise SerializationError(
                f"{fn.__qualname__} ({where}) captures {name} of type {type(value).__name__}, which cannot be "
                f"serialized ({e}). Create it inside the function, or pass what it needs as an argument."
            ) from None
        if size > MAX_CAPTURE_BYTES:
            raise SerializationError(
                f"{fn.__qualname__} ({where}) captures {name} ({size >> 20} MiB); pass it with ch.put(...) instead"
            )
        if size > WARN_CAPTURE_BYTES:
            warnings.warn(
                f"CH108: {fn.__qualname__} captures {name} ({size >> 20} MiB); it is snapshotted when first "
                "submitted and shipped once per peer. Pass it with ch.put(...) if it changes.",
                stacklevel=4,
            )


def callable_name(obj: Callable[..., Any]) -> str:
    return getattr(obj, "__qualname__", getattr(obj, "__name__", type(obj).__name__))
