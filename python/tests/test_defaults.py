"""Defaults that keep a naive driver safe and informed: submission waits for
room instead of filling memory, a wait on a call that cannot start says why,
and log records from tasks reach the driver at its level."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import pytest
from conftest import BIN, run_script

from chimaera._executor import python_mismatch

PYTHON = f"{sys.version_info.major}.{sys.version_info.minor}"


def test_calling_an_actor_method_directly_says_to_use_remote():
    # The usual first mistake: c.inc(1) instead of c.inc.remote(1). Python's
    # own error named only a type ("'RemoteMethod' object is not callable").
    r = run_script(
        """
        import chimaera as ch

        @ch.remote
        class Counter:
            def inc(self, k):
                return k

        c = Counter.remote()
        try:
            c.inc(1)
        except TypeError as e:
            print("ERROR", e)
        print("TEMPORARY", Counter.remote().inc.remote(5).result(timeout=60))  # a handle never stored works
        """
    )
    assert r.returncode == 0, r.stderr
    assert "Counter.inc" in r.stdout and ".inc.remote(...)" in r.stdout, r.stdout
    assert "TEMPORARY 5" in r.stdout, r.stdout


def test_an_async_actors_awaiting_calls_hold_no_threads():
    # An I/O actor (async methods, high concurrency) awaits many calls at once.
    # Each awaiting call held a thread: 500 calls, 505 threads. Its coroutines
    # now run on the actor's loop and wait without one.
    r = run_script(
        """
        import asyncio, threading, time
        import chimaera as ch

        @ch.remote(concurrency=300)
        class Waiter:
            async def wait(self, s):
                await asyncio.sleep(s)
                return ch.context() is not None

            def threads(self):
                return threading.active_count()

        w = Waiter.remote()
        calls = [w.wait.remote(2) for _ in range(200)]
        time.sleep(1)
        print("THREADS", w.threads.remote().result(timeout=60))
        print("DONE", sum(ch.get(calls, timeout=120)))
        """,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    out = dict(line.split() for line in r.stdout.splitlines() if line.startswith(("THREADS", "DONE")))
    assert out["DONE"] == "200", r.stdout  # every call finished, each with its task context
    assert int(out["THREADS"]) < 50, r.stdout


def test_a_function_wrapped_anew_for_every_call_warns_once():
    # ch.remote(lambda ...) inside a loop sends a new function each time, each
    # pickled and hashed again (6x slower than a function defined once).
    r = run_script(
        """
        import warnings
        import chimaera as ch

        warnings.simplefilter("always")
        assert ch.get([ch.remote(lambda x: x).remote(i) for i in range(150)], timeout=120) == list(range(150))
        once = ch.remote(lambda x: 2 * x)
        assert ch.get([once.remote(i) for i in range(150)], timeout=120) == [2 * i for i in range(150)]
        """,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert r.stderr.count("CH120") == 1 and "Define the remote function once" in r.stderr, r.stderr[-2000:]


def test_a_cuda_error_in_a_call_given_no_gpu_says_how_to_ask_for_one():
    # The peer hides GPUs (CUDA_VISIBLE_DEVICES empty) from calls that did not
    # ask for one, on machines that have them; torch then raises an error that
    # never says why. Simulated here: no GPU on the test machine.
    r = run_script(
        """
        import os
        import chimaera as ch

        @ch.remote
        def cuda(hidden):
            if hidden:
                os.environ["CUDA_VISIBLE_DEVICES"] = ""
            else:
                os.environ.pop("CUDA_VISIBLE_DEVICES", None)
            raise RuntimeError("No CUDA GPUs are available")

        for hidden in (True, False):
            try:
                cuda.remote(hidden).result(timeout=60)
            except Exception as e:
                notes = " ".join(getattr(e, "__notes__", []))
                print("HINT" if "CH119" in notes and "gpus=1" in notes else "PLAIN", hidden)
        """
    )
    assert r.returncode == 0, r.stderr
    assert "HINT True" in r.stdout and "PLAIN False" in r.stdout, r.stdout + r.stderr[-1500:]


def test_a_function_that_will_not_load_on_another_python_says_so(monkeypatch):
    # Workers learn the program's Python version from its run...
    r = run_script(
        """
        import os
        import chimaera as ch

        @ch.remote
        def driver_python():
            return os.environ.get("CHIMAERA_DRIVER_PYTHON")

        print(driver_python.remote().result(timeout=60))
        """
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split()[-1] == PYTHON, r.stdout
    # ...and a function that fails to load names a version difference (a
    # remote cluster's workers run the cluster's Python), and nothing else.
    monkeypatch.setenv("CHIMAERA_DRIVER_PYTHON", "3.99")
    err = python_mismatch(TypeError("code() argument 13 must be bytes, not int"))
    assert err is not None and "CH115" in str(err) and "Python 3.99" in str(err) and f"Python {PYTHON}" in str(err)
    monkeypatch.setenv("CHIMAERA_DRIVER_PYTHON", PYTHON)
    assert python_mismatch(TypeError("x")) is None


def test_a_driver_submitting_faster_than_calls_finish_waits_for_room():
    r = run_script(
        """
        import time, warnings
        import numpy as np
        import chimaera as ch

        @ch.remote
        def slow(a):
            time.sleep(0.3)
            return float(a[0])

        warnings.simplefilter("always")
        started = time.monotonic()
        futures = [slow.remote(np.full(2 << 20, float(i))) for i in range(12)]  # 16 MiB each, all distinct
        submitted = time.monotonic() - started
        assert [f.result(timeout=120) for f in futures] == [float(i) for i in range(12)]
        print(round(submitted, 2))
        """,
        env={"CHIMAERA_MAX_PENDING_BYTES": str(48 << 20)},
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert "CH110" in r.stderr, r.stderr
    # At most three 16 MiB calls are outstanding, so submitting twelve takes
    # several of their 0.3 s turns.
    assert float(r.stdout.split()[-1]) >= 0.6, r.stdout


def test_calls_reading_one_stored_value_do_not_wait_for_room():
    # A ch.put value is stored once, before its calls: passing it to many
    # calls adds no pending memory, so it must not count once per call.
    r = run_script(
        """
        import time, warnings
        import numpy as np
        import chimaera as ch

        @ch.remote
        def slow(a):
            time.sleep(0.3)
            return float(a[0])

        warnings.simplefilter("always")
        v = ch.put(np.ones(2 << 20))  # 16 MiB
        started = time.monotonic()
        futures = [slow.remote(v) for _ in range(12)]
        submitted = time.monotonic() - started
        assert [f.result(timeout=120) for f in futures] == [1.0] * 12
        print(round(submitted, 2))
        """,
        env={"CHIMAERA_MAX_PENDING_BYTES": str(48 << 20)},
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert "CH110" not in r.stderr, r.stderr
    assert float(r.stdout.split()[-1]) < 0.3, r.stdout


def test_a_wait_on_a_call_that_cannot_start_says_why():
    r = run_script(
        """
        import os, time
        import chimaera as ch


        @ch.remote
        def hog(seconds):
            time.sleep(seconds)
            return seconds

        cores = ch._runtime.Runtime.get().info["usable_cores"]
        first = hog.options(cpus=cores).remote(3)
        second = hog.options(cpus=cores).remote(0)
        print(second.result(timeout=60))
        """,
        env={"CHIMAERA_STALL_NOTICE_S": "1"},
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert "chimaera: waited" in r.stderr and "needs" in r.stderr, r.stderr


def test_log_records_from_tasks_reach_the_driver_at_its_level():
    r = run_script(
        """
        import logging
        import chimaera as ch

        logging.basicConfig(level=logging.INFO)

        @ch.remote
        def work():
            logging.getLogger("task").info("hello from a task")
            logging.getLogger("task").debug("too detailed")
            return 1

        print(work.remote().result(timeout=60))
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert "hello from a task" in r.stderr + r.stdout
    assert "too detailed" not in r.stderr + r.stdout


def test_a_program_warns_when_the_local_peer_runs_another_binary(tmp_path):
    import os
    import shutil
    import subprocess
    import tempfile
    import time

    from conftest import BIN

    home = tempfile.mkdtemp(prefix="chb", dir="/tmp")
    exe = tmp_path / "chimaera"
    shutil.copy2(BIN, exe)
    script = "import chimaera as ch\n@ch.remote\ndef f():\n    return 1\nprint(f.remote().result(timeout=60))\n"
    try:
        first = run_script(script, home=home, env={"CHIMAERA_BIN": str(exe)})
        assert first.returncode == 0, first.stderr
        assert "CH111" not in first.stderr, "the peer this program started is its own binary"
        later = time.time_ns() + 10**9
        os.utime(exe, ns=(later, later))  # rebuilt while the peer keeps running
        second = run_script(script, home=home, env={"CHIMAERA_BIN": str(exe)})
        assert second.returncode == 0, second.stderr
        assert "CH111" in second.stderr and "chimaera stop" in second.stderr, second.stderr
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)


def test_work_waiting_on_cpu_that_actors_hold_says_so():
    import os
    import subprocess
    import tempfile
    import time

    from chimaera._native import socket_path
    from conftest import BIN

    home = tempfile.mkdtemp(prefix="cha", dir="/tmp")
    subprocess.Popen(
        [BIN, "--dir", home, "peer", "start", "--cpus", "2", "--idle-timeout", "60"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    deadline = time.monotonic() + 30
    while not os.path.exists(socket_path(home, "peer.sock")) and time.monotonic() < deadline:
        time.sleep(0.05)
    try:
        r = run_script(
            """
            import time
            import chimaera as ch

            @ch.remote(cpus=2)
            class Holder:
                def ping(self):
                    return 1

            @ch.remote(cpus=1)
            def work():
                return 1

            h = Holder.remote()
            h.ping.remote().result(timeout=60)  # the actor now holds both CPUs
            f = work.remote()
            time.sleep(1)
            print(f.explain()["pending_reason"])
            h.close()
            print(f.result(timeout=60))
            """,
            home=home,
            timeout=120,
        )
        assert r.returncode == 0, r.stderr
        assert "held by 1 actor(s)" in r.stdout and "cpus=" in r.stdout, r.stdout
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)


def _pids_cgroup() -> str | None:
    """A fresh cgroup with the pids controller, or None: not Linux, no
    writable cgroup v2, or pids not delegated outside a container (moving a
    host's processes around is not a test's business)."""
    root = "/sys/fs/cgroup"
    control = os.path.join(root, "cgroup.subtree_control")
    if sys.platform != "linux" or not os.access(control, os.W_OK):
        return None
    try:
        with open(control) as f:
            delegated = "pids" in f.read().split()
        if not delegated:
            if not os.path.exists("/.dockerenv"):
                return None
            # A cgroup holding processes cannot give its children controllers
            # (cgroup v2): move this container's processes into a leaf first,
            # as the peer does for isolated attempts.
            leaf = os.path.join(root, "init")
            os.makedirs(leaf, exist_ok=True)
            with open(os.path.join(root, "cgroup.procs")) as f:
                pids = f.read().split()
            for pid in pids:
                try:
                    with open(os.path.join(leaf, "cgroup.procs"), "w") as f:
                        f.write(pid)
                except OSError:
                    pass  # exited meanwhile
            with open(control, "w") as f:
                f.write("+pids")
        path = os.path.join(root, f"chimaera-pids-{os.getpid()}")
        os.makedirs(path, exist_ok=True)
    except OSError:
        return None
    return path if os.path.exists(os.path.join(path, "pids.max")) else None


def test_under_a_task_limit_calls_wait_for_workers_instead_of_failing(tmp_path):
    # A pod's pids.max counts every thread. Starting more workers than fit
    # failed with EAGAIN ("executor process exited") and lost the calls; now
    # the peer starts only what fits, calls wait, and status names the limit.
    cg = _pids_cgroup()
    if cg is None:
        pytest.skip("needs a writable cgroup v2 with the pids controller (the Linux dev container)")
    home = str(tmp_path / "home")

    def enter() -> None:
        with open(os.path.join(cg, "cgroup.procs"), "w") as f:
            f.write(str(os.getpid()))

    peer = subprocess.Popen(
        [BIN, "--dir", home, "peer", "start", "--cpus", "4"],
        preexec_fn=enter,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 60
        while not os.path.exists(os.path.join(home, "peer.sock")) and time.monotonic() < deadline:
            time.sleep(0.2)
        time.sleep(1)
        with open(os.path.join(cg, "pids.current")) as f:
            idle = int(f.read())
        with open(os.path.join(cg, "pids.max"), "w") as f:
            f.write(str(idle + 10))  # room for two one-thread workers (3 tasks each), not the four it wants
        r = run_script(
            """
            import time
            import chimaera as ch

            @ch.remote
            def work(i):
                time.sleep(0.2)
                return i

            print("DONE", sum(ch.get([work.remote(i) for i in range(16)], timeout=120)))
            """,
            env={"CHIMAERA_HOME": home},
            timeout=180,
        )
        assert r.returncode == 0, r.stderr[-2000:]
        assert r.stdout.split()[-1] == str(sum(range(16))), r.stdout
        status = subprocess.run([BIN, "--dir", home, "status", "--json"], capture_output=True, text=True, timeout=30)
        assert any("task limit" in a for a in json.loads(status.stdout)["automatic"]), status.stdout
    finally:
        peer.terminate()
        try:
            peer.wait(timeout=30)
        except subprocess.TimeoutExpired:
            peer.kill()
            peer.wait(timeout=30)
        try:
            os.rmdir(cg)
        except OSError:
            pass  # a worker still exiting: the cgroup goes with the container


def test_a_failed_call_whose_error_nothing_read_is_reported():
    # Ray Client drops an error nobody ray.get()s: the failure vanishes. Here a
    # dropped failed future says so on stderr (as asyncio does for a task's
    # exception never retrieved), and so do ones still held at exit. Errors that
    # were read, handed to another call, or cancelled stay quiet.
    r = run_script(
        """
        import gc, time
        import chimaera as ch

        @ch.remote
        def dropped():
            raise ValueError("first failure")

        @ch.remote
        def read():
            raise ValueError("read failure")

        @ch.remote
        def upstream():
            raise ValueError("upstream failure")

        @ch.remote
        def ident(x):
            return x

        @ch.remote
        def nap():
            time.sleep(30)

        @ch.remote
        def held():
            raise KeyError("held at exit")

        f = dropped.remote()
        ch.wait([f], timeout=60)
        del f
        gc.collect()

        g = read.remote()
        try:
            g.result(timeout=60)
        except ValueError:
            pass
        del g

        up = upstream.remote()
        down = ident.remote(up)
        try:
            down.result(timeout=60)
        except ValueError:
            pass
        del up, down

        c = nap.remote()
        time.sleep(0.5)
        c.cancel()
        ch.wait([c], timeout=60)
        del c
        gc.collect()

        h = held.remote()
        ch.wait([h], timeout=60)
        print("end")
        """,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    lines = [line for line in r.stderr.splitlines() if "nothing read its error" in line]
    assert len(lines) == 2, r.stderr
    assert "dropped" in lines[0] and "ValueError: first failure" in lines[0] and "script.py:" in lines[0], lines
    assert "held" in lines[1] and "KeyError" in lines[1], lines


def test_shared_objects_in_a_calls_arguments_stay_shared():
    # Ray pickles each argument on its own: the same list passed twice arrives
    # as two lists, and two bound methods of one object get two objects. A
    # call's arguments are pickled together here, so identity survives.
    r = run_script(
        """
        import chimaera as ch

        class Counter:
            def __init__(self):
                self.n = 0

            def bump(self):
                self.n += 1

            def read(self):
                return self.n

        @ch.remote
        def same(a, b):
            return a is b

        @ch.remote
        def both(bump, read):
            bump()
            return bump.__self__ is read.__self__, read()

        x = [1, 2, 3]
        c = Counter()
        print(same.remote(x, x).result(timeout=60), same.remote([1], [1]).result(timeout=60))
        print(*both.remote(c.bump, c.read).result(timeout=60))
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["True", "False", "True", "1"], r.stdout


def test_settings_left_empty_mean_their_defaults():
    # `CHIMAERA_MEMORY_POOL=` in a compose file or pod spec is common; the peer
    # used to refuse to start ("expected a size like 8GiB"), and an empty
    # CHIMAERA_PYTHON named no interpreter at all.
    import tempfile

    home = tempfile.mkdtemp(prefix="chm", dir="/tmp")
    try:
        r = run_script(
            """
            import chimaera as ch

            @ch.remote
            def one():
                return 1

            print(one.remote().result(timeout=60))
            """,
            home=home,
            env={"CHIMAERA_MEMORY_POOL": "", "CHIMAERA_PYTHON": " "},
            timeout=120,
        )
        assert r.returncode == 0, r.stderr
        assert r.stdout.split() == ["1"], r.stdout
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)


def test_many_calls_waiting_on_calls_they_started_are_reported_once():
    # With hosting off (CHIMAERA_HOST_WHILE_WAITING=0), each call waiting inside
    # a call keeps its own worker process; Ray warns with a process count only.
    # Here the driver is told once, with the fix. (With hosting on, the default,
    # their processes run other calls meanwhile and stay few.)
    import tempfile

    home = tempfile.mkdtemp(prefix="chm", dir="/tmp")
    try:
        r = run_script(
            """
            import os, time
            import chimaera as ch

            @ch.remote
            def tree(depth):
                if depth == 0:
                    time.sleep(0.02)
                    return 1
                return sum(ch.get([tree.remote(depth - 1) for _ in range(3)]))

            # Enough waiting calls (a 3-way tree of depth d has (3**d - 1) / 2) to
            # pass the notice's bar of max(32, 4 per CPU) processes.
            depth = 1
            while (3**depth - 1) // 2 < max(32, 4 * (os.cpu_count() or 1)) + 8:
                depth += 1
            print(tree.remote(depth).result(timeout=240) == 3**depth)
            """,
            home=home,
            env={"CHIMAERA_HOST_WHILE_WAITING": "0"},
            timeout=300,
        )
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["True"], r.stdout
    notes = [line for line in r.stderr.splitlines() if "held by calls waiting inside a call" in line]
    assert len(notes) == 1, r.stderr
    assert "pass them to the next call" in notes[0], notes



def test_an_argument_nested_too_deeply_to_pickle_says_so():
    # A linked list 100,000 levels deep overflows the pickler's stack: the error
    # names the depth and how to send it, not advice about unpicklable objects.
    r = run_script(
        """
        import chimaera as ch

        @ch.remote
        def length(node):
            return 0

        head = None
        for _ in range(100_000):
            head = [head]
        try:
            length.remote(head)
        except ch.SerializationError as e:
            print(e)
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert "argument 'node' nests too deeply to pickle" in r.stdout, r.stdout
    assert "send it flat" in r.stdout, r.stdout


def test_a_stream_or_map_read_to_the_end_says_so_when_read_again():
    # Ray's ObjectRefGenerator yields nothing the second time, silently: a
    # sum over it is 0 and a max of it fails as "empty".
    r = run_script(
        """
        import chimaera as ch

        @ch.remote
        def gen(n):
            for i in range(n):
                yield i

        @ch.remote
        def sq(x):
            return x * x

        s = gen.remote(3)
        print(list(s))
        try:
            list(s)
        except RuntimeError as e:
            print("stream:", e)
        m = sq.map(range(4))
        print(sum(m))
        try:
            max(m)
        except RuntimeError as e:
            print("map:", e)
        part = gen.remote(4)
        for x in part:
            break
        print(list(part))  # a pass that stopped early goes on where it stopped
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert lines[0] == "[0, 1, 2]", lines
    assert lines[1].startswith("stream: this stream was already read to the end"), lines
    assert lines[2] == "14", lines
    assert lines[3].startswith("map: this map was already read to the end"), lines
    assert lines[4] == "[1, 2, 3]", lines


def test_closing_an_actor_runs_its_cleanup():
    # Ray's kill skips __del__ and atexit: connections and locks stay open.
    import tempfile

    marks = tempfile.mkdtemp(prefix="chl", dir="/tmp")
    r = run_script(
        f"""
        import os, time
        import chimaera as ch

        @ch.remote
        class Holder:
            def __init__(self, path):
                import atexit
                self.path = path
                atexit.register(lambda: open(path + ".atexit", "w").close())

            def __del__(self):
                open(self.path + ".del", "w").close()

            def pid(self):
                return os.getpid()

        h = Holder.remote({os.path.join(marks, "m")!r})
        pid = h.pid.remote().result(timeout=60)
        start = time.time()
        h.close()
        while time.time() - start < 10:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        print(round(time.time() - start, 2))
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert os.path.exists(os.path.join(marks, "m.del")), "__del__ did not run"
    assert os.path.exists(os.path.join(marks, "m.atexit")), "atexit handlers did not run"
    assert float(r.stdout.split()[-1]) < 3, f"the worker took {r.stdout.split()[-1]} s to exit after close"


def test_an_actor_waiting_on_its_own_queued_call_fails_fast():
    # A one-at-a-time actor whose method waits for another call to itself can
    # never get it; Ray hangs. Here the wait raises at once, saying why.
    r = run_script(
        """
        import time
        import chimaera as ch

        @ch.remote
        class Loop:
            def __init__(self):
                self.me = None

            def set(self, h):
                self.me = h

            def inner(self):
                return 1

            def outer(self):
                return self.me.inner.remote().result()

        h = Loop.remote()
        h.set.remote(h).result(timeout=60)
        start = time.time()
        try:
            h.outer.remote().result(timeout=60)
        except ch.Infeasible as e:
            print(round(time.time() - start, 1), str(e).splitlines()[0])
        print(h.inner.remote().result(timeout=60))  # the actor keeps serving
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    first, last = r.stdout.splitlines()[0], r.stdout.splitlines()[-1]
    assert float(first.split()[0]) < 10, first
    assert "deadlock: Loop.outer waits for Loop.inner" in first, first
    assert last == "1", r.stdout


def test_actors_made_for_a_call_or_two_each_warn_once():
    # A new actor per item or request is a new worker process each time (the
    # guides' "actor churn"); actors made once and reused, and named actors
    # looked up again and again, stay quiet.
    r = run_script(
        """
        import chimaera as ch

        @ch.remote
        class Scorer:
            def score(self, x):
                return x + 1

        for i in range(22):
            s = Scorer.remote()
            s.score.remote(i).result(timeout=60)
            s.close()

        team = [Scorer.remote() for _ in range(22)]
        for s in team:
            for i in range(3):
                s.score.remote(i).result(timeout=60)
        for s in team:
            s.close()

        for i in range(22):
            Scorer.options(name="one").remote().score.remote(i).result(timeout=60)
        print("ok")
        """,
        timeout=300,
    )
    assert r.returncode == 0, r.stderr
    notes = [line for line in r.stderr.splitlines() if "CH121" in line]
    assert len(notes) == 1, r.stderr
    assert "script.py:10: UserWarning: CH121: Scorer actors made here" in notes[0], notes


def test_an_async_method_that_blocks_its_actors_loop_is_named_once():
    # A blocking call inside an ``async def`` holds the actor's one event loop:
    # every other call waits, and Ray says nothing.
    r = run_script(
        """
        import asyncio, time
        import chimaera as ch

        @ch.remote
        class Service:
            async def slow(self):
                time.sleep(1.6)
                return 1

            async def fine(self):
                await asyncio.sleep(1.6)
                return 2

        s = Service.remote()
        print(ch.get([s.slow.remote(), s.slow.remote(), s.fine.remote()], timeout=60))
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    notes = [line for line in r.stderr.splitlines() if "held its actor's event loop" in line]
    assert len(notes) == 1, r.stderr
    assert "Service.slow has held" in notes[0] and "script.py:8," in notes[0], notes  # the time.sleep line


def test_a_call_failing_on_an_environment_variable_the_program_has_says_how_to_pass_it():
    # Calls see only the variables a run passes (secrets stay home); a bare
    # KeyError('MY_SETTING') used to be all a call reading one got.
    r = run_script(
        """
        import os
        import chimaera as ch

        @ch.remote
        def read(name):
            return os.environ[name]

        for name in ("CH_TEST_SETTING", "CH_TEST_NOWHERE"):
            try:
                read.remote(name).result(timeout=60)
            except KeyError as e:
                notes = [n for n in getattr(e, "__notes__", []) if n.startswith("CH122")]
                print(name, notes[0] if notes else "no note")
        ch.shutdown()
        ch.init(env={"CH_TEST_SETTING": os.environ["CH_TEST_SETTING"]})
        print(read.remote("CH_TEST_SETTING").result(timeout=60))
        """,
        env={"CH_TEST_SETTING": "blue"},
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert lines[0].startswith("CH_TEST_SETTING CH122: CH_TEST_SETTING is set where the program runs"), lines
    assert "ch.init(env={'CH_TEST_SETTING'" in lines[0], lines
    assert lines[1] == "CH_TEST_NOWHERE no note", lines
    assert lines[2] == "blue", lines


def test_named_actors_from_threads_exit_in_a_call_and_futures_across_sessions():
    # Three Ray traps: get-or-create of a named actor from several threads is
    # not thread-safe; exit() inside a function fails to pickle or kills the
    # worker; an ObjectRef from before a re-init hangs. Here each just works.
    r = run_script(
        """
        import sys, threading
        import chimaera as ch

        @ch.remote
        class Counter:
            def __init__(self):
                self.n = 0

            def bump(self):
                self.n += 1
                return self.n

        seen = []
        def make():
            seen.append(Counter.options(name="shared").remote().bump.remote().result(timeout=60))
        threads = [threading.Thread(target=make) for _ in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        print(sorted(seen))

        @ch.remote
        def leave(how):
            if how == "exit":
                exit(3)
            sys.exit(4)

        @ch.remote
        def sq(x):
            return x * x

        for how in ("exit", "sys"):
            try:
                leave.remote(how).result(timeout=60)
            except Exception as e:
                print(how, type(e).__name__, "SystemExit" in str(e))
        print(sq.remote(7).result(timeout=60))

        before = sq.remote(3)
        before.result(timeout=60)
        ch.shutdown()
        ch.init()
        print(before.result(timeout=10), sq.remote(4).result(timeout=60))
        """,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert lines[0] == "[1, 2, 3, 4, 5, 6, 7, 8]", lines  # one actor, eight calls
    assert lines[1] == "exit RemoteError True" and lines[2] == "sys RemoteError True", lines
    assert lines[3] == "49", lines  # the worker survived
    assert lines[4] == "9 16", lines


def test_a_call_on_another_machine_missing_a_file_says_files_are_not_copied():
    # "Works on my machine": a path that exists where the program runs is
    # missing on the peer that ran the call. A peer with another machine id
    # stands in for another machine.
    import tempfile

    home = tempfile.mkdtemp(prefix="chm", dir="/tmp")
    data = os.path.join(home, "data.csv")
    with open(data, "w") as f:
        f.write("1,2\n")
    peer = subprocess.Popen([BIN, "--dir", home, "peer", "start"], env=dict(os.environ, CHIMAERA_MACHINE_ID="elsewhere"),
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        r = run_script(
            f"""
            import os
            import chimaera as ch

            @ch.remote
            def load(path):
                return open(path).read()

            for path in ({data!r}, "/nowhere/at/all.csv"):
                try:
                    load.remote(path + ".gone").result(timeout=60)
                except FileNotFoundError as e:
                    notes = [n for n in getattr(e, "__notes__", []) if n.startswith("CH123")]
                    print(notes[0][:60] if notes else "no note")
            """,
            home=home,
            timeout=120,
        )
        assert r.returncode == 0, r.stderr
        lines = r.stdout.splitlines()
        assert len(lines) == 2 and all(line.startswith("CH123: this call ran on ") for line in lines), lines
        same = run_script(
            """
            import chimaera as ch

            @ch.remote
            def load(path):
                return open(path).read()

            try:
                load.remote("/nowhere/at/all.csv").result(timeout=60)
            except FileNotFoundError as e:
                print(any(n.startswith("CH123") for n in getattr(e, "__notes__", [])))
            """,
            home=home,
            env={"CHIMAERA_MACHINE_ID": "elsewhere"},  # the program on the peer's machine: no note
            timeout=120,
        )
        assert same.returncode == 0, same.stderr
        assert same.stdout.split() == ["False"], same.stdout
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)
        peer.wait(timeout=30)


def test_nested_pydantic_models_arrive_whole_or_fail_saying_how_to_fix_it():
    # Ray loses field values of nested pydantic v1 models passed as arguments,
    # silently. Pydantic 2 models round-trip whole; a v1 model defined in the
    # main script cannot travel (its compiled validators do not pickle), and
    # the error says to define it in a module instead.
    pydantic = pytest.importorskip("pydantic")
    r = run_script(
        """
        import pydantic
        import chimaera as ch
        from pydantic import BaseModel

        class Inner(BaseModel):
            x: int
            tags: list = []

        class Outer(BaseModel):
            name: str
            inner: Inner
            items: list = []

        @ch.remote
        def echo(o):
            return o

        o = Outer(name="a", inner=Inner(x=7, tags=["t"]), items=[Inner(x=1)])
        try:
            print(echo.remote(o).result(timeout=60) == o)
        except ch.SerializationError as e:
            print("define the model in a module" in str(e))
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["True"], (pydantic.VERSION, r.stdout)


def test_driver_threads_submit_get_and_wait_at_once():
    # Ray documents ray.get/ray.wait from several driver threads as a special
    # case; here a threaded driver needs nothing special.
    r = run_script(
        """
        import threading
        import chimaera as ch

        @ch.remote
        def sq(x):
            return x * x

        bad = []

        def worker(k):
            try:
                fs = [sq.remote(k * 100 + i) for i in range(30)]
                done, _ = ch.wait(fs[:10], num_returns=10, timeout=60)
                if len(done) != 10 or ch.get(fs, timeout=60) != [(k * 100 + i) ** 2 for i in range(30)]:
                    bad.append(k)
            except BaseException as e:
                bad.append(repr(e))

        threads = [threading.Thread(target=worker, args=(k,)) for k in range(12)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        print(bad)
        """,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["[]"], r.stdout


def test_init_with_other_settings_after_the_first_call_says_so():
    # Ray's ray.init is first-call-wins: a later runtime_env is silently ignored.
    r = run_script(
        """
        import chimaera as ch

        @ch.remote
        def one():
            return 1

        one.remote().result(timeout=60)
        try:
            ch.init(env={"MODE": "fast"})
        except RuntimeError as e:
            print("refused:", "ch.shutdown()" in str(e))
        ch.shutdown()
        ch.init(env={"MODE": "fast"})
        print(one.remote().result(timeout=60))
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["refused:", "True", "1"], r.stdout


def test_importing_and_using_chimaera_leaves_the_programs_logging_alone():
    # `import ray` replaces root handlers, patches Logger.makeRecord, and its
    # loggers do not propagate: a program's own logging setup breaks.
    r = run_script(
        """
        import logging, sys
        logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
        before = (list(logging.getLogger().handlers), logging.Logger.makeRecord, logging.getLogRecordFactory())
        import chimaera as ch

        @ch.remote
        def one():
            return 1

        one.remote().result(timeout=60)
        after = (list(logging.getLogger().handlers), logging.Logger.makeRecord, logging.getLogRecordFactory())
        print(before[0] == after[0], before[1] is after[1], before[2] is after[2])
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["True", "True", "True"], r.stdout


def test_a_future_pickled_by_one_program_is_read_by_another():
    # A Ray ObjectRef rebuilt from bytes in another process fails or hangs:
    # its owner is unknown there. A future names its call, which any program
    # of the tenant can read while the run that made it lasts.
    import tempfile

    from conftest import HOME, REPO

    out = os.path.join(tempfile.mkdtemp(prefix="chf", dir="/tmp"), "future.pkl")
    maker = subprocess.Popen(
        [sys.executable, "-c", (
            "import pickle, sys, time\n"
            "import chimaera as ch\n"
            "@ch.remote\n"
            "def sq(x):\n"
            "    return x * x\n"
            "f = sq.remote(9)\n"
            "f.result(timeout=60)\n"
            f"open({out!r}, 'wb').write(pickle.dumps(f))\n"
            "time.sleep(30)\n"
        )],
        env=dict(os.environ, CHIMAERA_HOME=HOME, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python")),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 60
        while not os.path.exists(out) and time.monotonic() < deadline:
            time.sleep(0.2)
        r = run_script(
            f"""
            import pickle
            f = pickle.loads(open({out!r}, "rb").read())
            print(f.result(timeout=30))
            """,
            timeout=120,
        )
    finally:
        maker.kill()
        maker.wait(timeout=30)
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["81"], r.stdout


def test_calls_run_in_the_programs_directory_so_relative_paths_work():
    # Ray's workers run in /tmp/ray/session_*: open("data.csv") fails there.
    r = run_script(
        """
        import os
        import chimaera as ch

        with open("data.csv", "w") as f:
            f.write("a,b")

        @ch.remote
        def where(driver_cwd):
            return os.getcwd() == driver_cwd, open("data.csv").read()

        print(*where.remote(os.getcwd()).result(timeout=60))
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["True", "a,b"], r.stdout
