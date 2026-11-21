"""A run's output is kept on the peer: Run.logs() and `chimaera logs RUN`."""

from __future__ import annotations

import subprocess
import uuid

import chimaera as ch
from conftest import BIN, HOME, run_script


def test_output_outlives_the_driver_and_reads_back_from_python_and_the_cli():
    proc = run_script(
        """
        import sys, chimaera as ch

        @ch.remote
        def chatty(i):
            print(f"line {i}")
            print(f"warning {i}", file=sys.stderr)
            return i

        run = ch.init()
        assert sum(chatty.map(range(3))) == 3
        print("RUN", run)
        """
    )
    assert proc.returncode == 0, proc.stderr
    run_id = proc.stdout.split("RUN ")[1].strip()
    lines = list(ch.attach(run_id).logs())  # the driver has exited
    assert sorted(line.split("| ", 1)[1] for line in lines if " stdout| " in line) == ["line 0", "line 1", "line 2"]
    assert sorted(line.split("| ", 1)[1] for line in lines if " stderr| " in line) == ["warning 0", "warning 1", "warning 2"]
    assert all(len(line.split(" ", 1)[0]) == 12 for line in lines)  # each line names its call
    cli = subprocess.run([BIN, "--dir", HOME, "logs", run_id], capture_output=True, text=True, timeout=60)
    assert cli.returncode == 0, cli.stderr
    assert cli.stdout.splitlines() == lines


@ch.remote
def slow_printer(token):
    import time

    for i in range(5):
        print(f"{token} tick {i}", flush=True)
        time.sleep(0.3)
    print(f"{token} done", flush=True)


def test_follow_yields_lines_while_the_call_runs():
    token = uuid.uuid4().hex  # the session's run also carries other tests' output
    f = slow_printer.remote(token)
    seen = []
    for line in ch.current_run().logs(follow=True):
        if token in line:
            seen.append(line.split(f"{token} ", 1)[1])
            if len(seen) == 1:
                assert not f.done(), "the first line arrives while the call still runs"
            if seen[-1] == "done":
                break
    assert seen == ["tick 0", "tick 1", "tick 2", "tick 3", "tick 4", "done"]
    f.result(timeout=30)
