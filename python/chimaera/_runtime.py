"""The client side of a run: connection, submission and outcome delivery.

The first submission connects to the local peer (starting it if needed) and
creates a run with this interpreter, working directory and import path, so
executors run the same code as the driver. A dispatcher thread turns session
events into resolved futures; waiting never holds the GIL in Rust.
"""

from __future__ import annotations

import atexit
import fnmatch
import inspect
import itertools
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
import warnings
import weakref
from typing import Any, Callable

from . import _context, _serde
from ._errors import ChimaeraError, DataLost, Infeasible, SerializationError
from ._native import Client, blake3_digest, child_id, socket_path

VERSION = "0.1.0"
CALL_FUNCTION, CALL_ACTOR_CTOR, CALL_ACTOR_METHOD, CALL_MAP_ITEM = 0, 1, 2, 3
EFFECTS_CODE = {"none": 0, "idempotent": 1, "external": 2}
MAX_CAPTURE_BYTES = 256 << 20
WARN_CAPTURE_BYTES = 1 << 20


def default_home() -> str:
    return os.environ.get("CHIMAERA_HOME") or os.path.join(os.path.expanduser("~"), ".chimaera", "local")


def _peer_binary() -> str:
    candidates = [os.environ.get("CHIMAERA_BIN", "")]
    here = os.path.dirname(os.path.abspath(__file__))
    candidates.append(os.path.join(here, "_bin", "chimaera"))
    candidates.append(shutil.which("chimaera") or "")
    # Development checkout: <repo>/python/chimaera -> <repo>/target/{release,debug}/chimaera
    repo = os.path.dirname(os.path.dirname(here))
    candidates += [os.path.join(repo, "target", "release", "chimaera"), os.path.join(repo, "target", "debug", "chimaera")]
    for c in candidates:
        if c and os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    raise ChimaeraError("cannot find the `chimaera` peer binary; set CHIMAERA_BIN or reinstall the package")


def peer_socket(home: str) -> str:
    # The directory must exist first: the rule hashes its canonical path.
    os.makedirs(home, exist_ok=True)
    return socket_path(home, "peer.sock")


def _ensure_local_peer(home: str) -> None:
    sock = peer_socket(home)
    if os.path.exists(sock):
        return
    with open(os.path.join(home, "peer.log"), "ab") as log:
        subprocess.Popen(
            [_peer_binary(), "--dir", home, "peer", "start", "--idle-timeout", "600"],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            close_fds=True,
        )
    deadline = time.monotonic() + 20
    while not os.path.exists(sock):
        if time.monotonic() > deadline:
            raise ChimaeraError(f"the local peer did not start; see {os.path.join(home, 'peer.log')}")
        time.sleep(0.02)


def read_object(object_dir: str, run: bytes, obj: bytes) -> Any:
    """Load an object from this peer's store, asking the peer to fetch it
    first when another peer produced it."""
    path = _serde.object_path(object_dir, run, obj)
    try:
        return _serde.read_file(path)
    except FileNotFoundError:
        pass
    try:
        Runtime.get().client.localize(run, obj)
    except FileNotFoundError as e:
        # The peer that held it is gone (or freed it); nothing rebuilds it.
        raise DataLost(f"value {obj.hex()} of run {run.hex()} is no longer stored on any live peer") from e
    return _serde.read_file(path)


# Client variables every run ships to its executors: locale and hashing only.
# Anything else (secrets, PATH, VIRTUAL_ENV, CUDA_VISIBLE_DEVICES) stays on the
# client unless named in CHIMAERA_ENV_PASSTHROUGH or passed to ch.init(env=...);
# executors otherwise see their own peer's environment. The run's variables
# are replicated with the run record, so they must not carry secrets.
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


class Runtime:
    _instance: Runtime | None = None
    _lock = threading.Lock()

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
        self, home: str | None = None, detached: bool | None = None, env: dict[str, str] | None = None
    ) -> None:
        self.home = home or default_home()
        self.in_executor = os.environ.get("CHIMAERA_EXECUTOR") == "1"
        socket = os.environ.get("CHIMAERA_PEER_SOCKET") if self.in_executor else None
        if socket is None:
            _ensure_local_peer(self.home)
            socket = peer_socket(self.home)
        client_name = f"python-{platform.python_version()} chimaera-{VERSION}"
        try:
            self.client = Client(socket, client_name)
        except ConnectionError:
            # A stale socket from a peer that died: remove it and start fresh.
            if self.in_executor:
                raise
            try:
                os.unlink(socket)
            except FileNotFoundError:
                pass
            _ensure_local_peer(self.home)
            self.client = Client(socket, client_name)
        self.info = self.client.info
        self.object_dir: str = self.info["object_dir"]
        self.inline_limit: int = self.info["inline_limit"]
        self._futures: weakref.WeakValueDictionary[bytes, Any] = weakref.WeakValueDictionary()
        self.cond = threading.Condition()
        self._code: dict[int, tuple[Any, bytes]] = {}
        self._registered: set[bytes] = set()
        self._seq = itertools.count()
        self._nonce = os.urandom(8)
        self._waiters: dict[int, list[Any]] = {}
        self._releases: list[bytes] = []
        self.run: bytes | None = None
        self.detached = False
        if not self.in_executor:
            want_detached = detached if detached is not None else os.environ.get("CHIMAERA_DETACH") == "1"
            ship = run_env(env)
            sys_path = [p or os.getcwd() for p in sys.path]
            self.run, self.detached = self.client.create_run(
                "default",
                "",
                want_detached,
                sys.executable,
                os.getcwd(),
                sys_path,
                ship,
                " ".join(sys.argv)[:200],
            )
            self.client.attach(self.run)
            atexit.register(self._finish)
        self._dispatcher = threading.Thread(target=self._dispatch_loop, name="chimaera-events", daemon=True)
        self._dispatcher.start()

    @property
    def run_id(self) -> str:
        return self.run.hex() if self.run else ""

    # ---- submission -------------------------------------------------------

    def new_key(self) -> bytes:
        """Deterministic task key: a call inside a task derives from its parent
        and sequence number, so a retried parent re-submits the same children."""
        ctx = _context.current_task()
        if ctx is not None:
            run = ctx.run
            task = child_id(run, [ctx.task, b"child", ctx.next_child().to_bytes(8, "little")])
        else:
            if self.run is None:
                raise ChimaeraError("no run: submit from a driver or from inside a task")
            run = self.run
            task = child_id(run, [self._nonce, next(self._seq).to_bytes(8, "little")])
        return run + task

    def code_digest(self, obj: Any) -> bytes:
        cached = self._code.get(id(obj))
        if cached is not None and cached[0] is obj:
            return cached[1]
        preflight_captures(obj)
        blob = _serde.dump_code(obj)
        digest = blake3_digest(blob)
        if digest not in self._registered:
            self.client.put_code(digest, blob)
            self._registered.add(digest)
        self._code[id(obj)] = (obj, digest)
        return digest

    def pack(self, key: bytes, value: Any) -> dict[str, Any]:
        """Encode a call frame: inline when small, else an object file."""
        parts, size, oob = _serde.dumps_parts(value)
        if size <= self.inline_limit:
            data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)
            return {"args_inline": data, "args_object": None, "args_size": size, "args_oob": oob}
        run = key[:16]
        obj = child_id(run, [key[16:], b"args"])
        _serde.write_file(_serde.object_path(self.object_dir, run, obj), parts)
        return {"args_inline": None, "args_object": obj, "args_size": size, "args_oob": oob}

    def release_later(self, key: bytes) -> None:
        """A future was dropped: unpin its result (sent in batches)."""
        self._releases.append(key)

    def submit_watch(self, key: bytes, future: Any) -> None:
        self._futures[key] = future
        self.client.watch([key])

    def submit(self, specs: list[dict[str, Any]], futures: list[Any]) -> None:
        for s, f in zip(specs, futures):
            self._futures[s["key"]] = f
        self.client.submit(specs)

    def wait_actor(self, seq: int, timeout: float = 60.0) -> tuple[Any, ...]:
        box: list[Any] = []
        with self.cond:
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
                    f = self._futures.get(ev[1])
                    if f is not None:
                        f._set(ev[2])
                        changed = True
                elif tag == "acks":
                    for key, status, reason, outcome in ev[2]:
                        f = self._futures.get(key) if key else None
                        if f is None:
                            continue
                        if status == "Rejected":
                            exc = Infeasible(reason) if reason.startswith("infeasible") else ChimaeraError(reason)
                            f._set_exception(exc)
                            changed = True
                        elif outcome is not None:
                            f._set(outcome)
                            changed = True
                elif tag == "log":
                    _, _key, stream, text, pid = ev
                    out = sys.stderr if stream == "stderr" else sys.stdout
                    for line in text.splitlines():
                        out.write(f"(pid={pid}) {line}\n")
                    out.flush()
                elif tag in ("actor", "error"):
                    box = self._waiters.pop(ev[1], None)
                    if box is not None:
                        box.append(ev)
                        changed = True
            if changed:
                with self.cond:
                    self.cond.notify_all()
        with self.cond:
            self.cond.notify_all()

    def read_value(self, outcome: tuple[Any, ...]) -> Any:
        _, inline, run, obj, _size, _oob = outcome
        if inline is not None:
            return _serde.loads(inline)
        return read_object(self.object_dir, run, obj)

    def _finish(self) -> None:
        try:
            if not self.detached:
                self.client.finish_run()
            else:
                self.client.flush()
            # Give the session a moment to deliver the finish message.
            time.sleep(0.05)
            self.client.close()
        except Exception as e:  # interpreter is exiting; report, do not raise
            sys.stderr.write(f"chimaera: could not finish run {self.run_id} cleanly: {e}\n")


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
