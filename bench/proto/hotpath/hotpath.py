"""Driver, executor and benchmark for the D2 call-path prototype (src/main.rs).

    python hotpath.py bench --bin PATH [--pause S] [--json OUT]

starts two backups, an agent and the executors, then runs Ray's actor
microbenchmarks (ray/_private/ray_perf.py) through the agent with Ray's
timeit method, plus round-trip latency. Small results come back inline,
method calls are pickled, and each call is its own socket write, as in
Ray's benchmark. The waiting thread reads the socket itself: there is no
dispatcher thread.
"""

import argparse
import json
import multiprocessing
import os
import pickle
import select
import socket
import struct
import subprocess
import sys
import tempfile
import time

HDR = struct.Struct("<IBQII")  # length of the rest, kind, call, actor, driver
CALL, RESULT, HELLO_DRIVER, HELLO_EXEC, CALL_EXT, STATS = 1, 2, 3, 4, 5, 6
N_CPU = multiprocessing.cpu_count() // 2  # Ray's n:n actor count
M_CLIENTS, N_CALLS = 4, 5000  # Ray's n:n: 4 clients, 5000 calls each


def frame(kind, call, actor, driver, payload=b""):
    return HDR.pack(HDR.size - 4 + len(payload), kind, call, actor, driver) + payload


class Reader:
    def __init__(self, sock):
        self.sock, self.buf = sock, bytearray()

    def read(self):
        data = self.sock.recv(1 << 20)
        if not data:
            raise EOFError("connection closed")
        self.buf += data
        out, i, n = [], 0, len(self.buf)
        while n - i >= 4:
            (length,) = struct.unpack_from("<I", self.buf, i)
            if n - i < 4 + length:
                break
            _, kind, call, actor, driver = HDR.unpack_from(self.buf, i)
            out.append((kind, call, actor, driver, bytes(self.buf[i + HDR.size : i + 4 + length])))
            i += 4 + length
        del self.buf[:i]
        return out


class Client:
    """A driver. With `direct`, it talks to one executor with no agent.

    Single-threaded: the socket is non-blocking, and a send that would block
    reads replies until it can go on, so a burst of calls never deadlocks
    against replies filling the socket buffer.
    """

    def __init__(self, path, direct=False):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.connect(path)
        self.sock.setblocking(False)
        self.reader, self.seq, self.done, self.id = Reader(self.sock), 0, {}, 0
        if not direct:
            self._send(frame(HELLO_DRIVER, 0, 0, 0))
            ((_, _, _, self.id, _),) = self._frames()

    def _frames(self, writing=False):
        """Wait until readable (or writable, when `writing`); return the frames read."""
        r, _, _ = select.select([self.sock], [self.sock] if writing else [], [])
        return self.reader.read() if r else []

    def _send(self, data):
        view = memoryview(data)
        while view:
            try:
                view = view[self.sock.send(view) :]
            except BlockingIOError:
                for _, call, _, _, payload in self._frames(writing=True):
                    self.done[call] = payload

    def call(self, actor, method, *args, external=False):
        self.seq += 1
        payload = pickle.dumps((method, args))
        self._send(frame(CALL_EXT if external else CALL, self.seq, actor, self.id, payload))
        return self.seq

    def get(self, ids):
        many = isinstance(ids, list)
        for i in ids if many else [ids]:
            while i not in self.done:
                for _, call, _, _, payload in self._frames():
                    self.done[call] = payload
        if not many:
            return pickle.loads(self.done.pop(ids))
        return [pickle.loads(self.done.pop(i)) for i in ids]

    def stats(self):
        self._send(frame(STATS, 0, 0, self.id))
        ((_, _, _, _, payload),) = self._frames()
        return json.loads(payload)


class Actor:  # ray_perf.Actor
    def small_value(self):
        return b"ok"

    def small_value_arg(self, x):
        return b"ok"


def executor(path, actor, listen):
    if listen:
        server = socket.socket(socket.AF_UNIX)
        server.bind(path)
        server.listen(1)
        sock, _ = server.accept()
    else:
        sock = socket.socket(socket.AF_UNIX)
        sock.connect(path)
        sock.sendall(frame(HELLO_EXEC, 0, actor, 0))
    reader, inst = Reader(sock), Actor()
    while True:
        out = bytearray()
        try:
            batch = reader.read()
        except EOFError:
            return
        for _, call, a, driver, payload in batch:
            method, args = pickle.loads(payload)
            out += frame(RESULT, call, a, driver, pickle.dumps(getattr(inst, method)(*args)))
        sock.sendall(out)


def nn_client(path, conn):
    """One of Ray's n:n `work` tasks: N_CALLS calls round-robin over N_CPU actors."""
    c = Client(path)
    while conn.recv():
        c.get([c.call(1 + i % N_CPU, "small_value") for i in range(N_CALLS)])
        conn.send(True)


def timeit(name, fn, multiplier=1, pause=3.0):
    """ray._private.ray_microbenchmark_helpers.timeit, with the pause as a parameter."""
    time.sleep(pause)
    start, count = time.perf_counter(), 0
    while time.perf_counter() - start < 1:
        fn()
        count += 1
    step, stats = count // 10 + 1, []
    for _ in range(4):
        start, count = time.perf_counter(), 0
        while time.perf_counter() - start < 2:
            for _ in range(step):
                fn()
            count += step
        stats.append(multiplier * count / (time.perf_counter() - start))
    mean = sum(stats) / len(stats)
    print(f"{name} per second {mean:,.0f}", flush=True)
    return name, mean


def latency(fn, n=20000):
    for _ in range(1000):
        fn()
    samples = []
    for _ in range(n):
        t = time.perf_counter_ns()
        fn()
        samples.append(time.perf_counter_ns() - t)
    samples.sort()
    return {"p50_us": samples[n // 2] / 1000, "p99_us": samples[int(n * 0.99)] / 1000}


def bench(args):
    tmp = tempfile.mkdtemp(prefix="hp-")  # the system temp dir keeps socket paths under macOS's 104 bytes
    sock = lambda name: os.path.join(tmp, name)  # noqa: E731
    procs = []

    def start(cmd, ready):
        procs.append(subprocess.Popen(cmd))
        while not os.path.exists(ready):
            if procs[-1].poll() is not None:
                raise RuntimeError(f"{cmd} exited with {procs[-1].returncode}")
            time.sleep(0.02)

    me = os.path.abspath(__file__)
    try:
        for b in ("b1", "b2"):
            start([args.bin, "backup", sock(b)], sock(b))
        start([args.bin, "agent", sock("agent"), sock("b1"), sock("b2")], sock("agent"))
        for actor in range(N_CPU + 1):  # actor 0 for 1:1, 1..N_CPU for n:n
            procs.append(subprocess.Popen([sys.executable, me, "executor", sock("agent"), str(actor)]))
        start([sys.executable, me, "executor", sock("direct"), "0", "--listen"], sock("direct"))
        time.sleep(1)
        if any(p.poll() is not None for p in procs):
            raise RuntimeError("a process exited during startup")
        c = Client(sock("agent"))
        rows = dict(
            [
                timeit("1:1 actor calls sync", lambda: c.get(c.call(0, "small_value")), pause=args.pause),
                timeit(
                    "1:1 actor calls async",
                    lambda: c.get([c.call(0, "small_value") for _ in range(1000)]),
                    1000,
                    pause=args.pause,
                ),
                timeit(
                    "1:1 actor calls sync, effects=external",
                    lambda: c.get(c.call(0, "small_value", external=True)),
                    pause=args.pause,
                ),
            ]
        )
        pipes = []
        for _ in range(M_CLIENTS):
            ours, theirs = multiprocessing.Pipe()
            multiprocessing.Process(target=nn_client, args=(sock("agent"), theirs), daemon=True).start()
            pipes.append(ours)

        def nn():
            for p in pipes:
                p.send(True)
            for p in pipes:
                p.recv()

        rows.update([timeit("n:n actor calls async", nn, M_CLIENTS * N_CALLS, pause=args.pause)])
        for p in pipes:
            p.send(False)
        d = Client(sock("direct"), direct=True)
        lat = {
            "agent": latency(lambda: c.get(c.call(0, "small_value"))),
            "agent, effects=external": latency(lambda: c.get(c.call(0, "small_value", external=True))),
            "direct, no agent": latency(lambda: d.get(d.call(0, "small_value"))),
        }
        out = {
            "rates": rows,
            "latency": lat,
            "journal": c.stats(),
            "load_avg_1m": os.getloadavg()[0],
            "python": sys.version.split()[0],
            "n_cpu_actors": N_CPU,
        }
        print(json.dumps(out, indent=1))
        if args.json:
            with open(args.json, "w") as f:
                json.dump(out, f, indent=1)
    finally:
        for p in procs:
            p.kill()


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bench")
    b.add_argument("--bin", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "target/release/hotpath"))
    b.add_argument("--pause", type=float, default=3.0, help="seconds before each benchmark (Ray uses 10)")
    b.add_argument("--json")
    e = sub.add_parser("executor")
    e.add_argument("path")
    e.add_argument("actor", type=int)
    e.add_argument("--listen", action="store_true")
    a = p.parse_args()
    if a.cmd == "bench":
        bench(a)
    else:
        executor(a.path, a.actor, a.listen)


if __name__ == "__main__":
    main()
