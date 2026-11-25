"""Durable workflows: a workflow whose worker dies runs again and is answered
from the run's record; finished steps never run twice; a replay that asks for
something else diverges."""

from __future__ import annotations

import asyncio
import gc
import inspect
import json
import os
import random
import signal
import subprocess
import sys
import tempfile
import time

import pytest
from conftest import REPO, run_script

import chimaera as ch


@ch.remote
def step(i, path):
    with open(path, "a") as f:
        f.write(f"{i}\n")
    return i * 10


@ch.remote(workflow="durable")
async def pipeline(path, pids):
    total = 0
    for i in range(4):
        total += await step.remote(i, path)
        if i == 1:
            with open(pids, "a") as f:
                f.write(f"{os.getpid()}\n")
            time.sleep(1.0)  # the test kills the worker here, once
    return total


@ch.remote(workflow="durable")
async def draws(pids):
    r = await ch.durable.random()
    t = await ch.durable.now()
    with open(pids, "a") as f:
        f.write(f"{os.getpid()} {r} {t}\n")
    time.sleep(1.0)
    return r, t


@ch.remote(workflow="durable")
async def nondeterministic(path, pids):
    first = await step.remote(random.random(), path)  # differs on every run
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)
    return first


@ch.remote(workflow="durable")
async def branches(path, pids):
    if ch.context().attempt == 1:
        first = await step.remote(1, path)  # the history's branch
    else:
        first = await step.remote(2, path)  # the replay's branch
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)
    return first


@ch.remote(workflow="durable")
async def recorded_branches(pids):
    if ch.context().attempt == 1:
        value = await ch.durable.record(lambda: "history's branch")
    else:
        value = await ch.durable.record(lambda: "replay's branch")  # the same bytecode, another constant
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)
    return value


def same(x):
    return x


@ch.remote(workflow="durable")
async def gathered(pids):
    n = ch.context().attempt
    a, b = await asyncio.gather(ch.durable.record(same, n), ch.durable.record(same, 0))  # steps 1 and 2
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)
    return a


@ch.remote(workflow="durable")
async def mapped(path, pids):
    first = list(step.map([ch.context().attempt], [path]))  # differs on replay
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)
    return first


def line_of(fn, text):
    """file:line of the line of `fn`'s source that contains `text`."""
    src, start = inspect.getsourcelines(fn._fn)
    line = start + next(i for i, ln in enumerate(src) if text in ln)
    return f"{os.path.basename(inspect.getsourcefile(fn._fn))}:{line}"


@ch.remote(workflow="durable")
async def dropped_step(path, pids):
    first = await step.remote(1, path)  # its future is dropped at once
    gc.collect()
    time.sleep(1.5)  # long enough for a release of that step to reach the peer
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)  # the test kills the worker here, once
    return first


def kill_first_run(pids):
    deadline = time.monotonic() + 60
    while True:
        try:
            with open(pids) as f:
                line = f.readline()
            if line.endswith("\n"):
                break
        except FileNotFoundError:
            pass
        assert time.monotonic() < deadline, "the workflow never reached its first checkpoint"
        time.sleep(0.02)
    os.kill(int(line.split()[0]), signal.SIGKILL)


def lines(path):
    with open(path) as f:
        return f.read().splitlines()


def test_a_replayed_workflow_does_not_repeat_finished_steps():
    d = tempfile.mkdtemp()
    steps, pids = os.path.join(d, "steps"), os.path.join(d, "pids")
    f = pipeline.remote(steps, pids)
    kill_first_run(pids)
    assert f.result(timeout=120) == 60
    assert sorted(lines(steps)) == ["0", "1", "2", "3"], "every step ran exactly once"
    assert len(lines(pids)) == 2, "the workflow itself ran twice"


def test_recorded_values_are_the_same_on_replay():
    pids = os.path.join(tempfile.mkdtemp(), "pids")
    f = draws.remote(pids)
    kill_first_run(pids)
    r, t = f.result(timeout=120)
    first, second = (ln.split()[1:] for ln in lines(pids))
    assert first == second == [repr(r), repr(t)]


def test_a_replay_that_makes_a_different_call_diverges():
    d = tempfile.mkdtemp()
    f = nondeterministic.remote(os.path.join(d, "steps"), os.path.join(d, "pids"))
    kill_first_run(os.path.join(d, "pids"))
    with pytest.raises(ch.errors.WorkflowDivergence) as e:
        f.result(timeout=120)
    err, site = e.value, line_of(nondeterministic, "step.remote(random.random()")
    assert err.step == 1, "the workflow's first call"
    for call in (err.recorded, err.received):
        assert call["function"] == f"{__name__}.step"
        assert call["site"].endswith(site)
        assert call["args"].startswith("(0.")
        assert len(call["fingerprint"]) == 64
    assert err.recorded["args"] != err.received["args"]
    assert err.recorded["fingerprint"] != err.received["fingerprint"]
    for shown in (err.recorded["args"], err.received["args"], err.recorded["fingerprint"][:16], "ch.durable.now()"):
        assert shown in str(err)


def test_a_changed_branch_names_the_replays_call_site_and_the_recorded_one():
    d = tempfile.mkdtemp()
    f = branches.remote(os.path.join(d, "steps"), os.path.join(d, "pids"))
    kill_first_run(os.path.join(d, "pids"))
    with pytest.raises(ch.errors.WorkflowDivergence) as e:
        f.result(timeout=120)
    err = e.value
    assert err.step == 1
    assert err.recorded["site"].endswith(line_of(branches, "the history's branch"))
    assert err.received["site"].endswith(line_of(branches, "the replay's branch"))
    assert err.recorded["args"].startswith("(1, ") and err.received["args"].startswith("(2, ")
    assert err.recorded["site"] in str(err) and err.received["site"] in str(err)
    assert lines(os.path.join(d, "steps")) == ["1"], "the replay's call never ran"


def test_a_changed_lambda_is_another_step():
    pids = os.path.join(tempfile.mkdtemp(), "pids")
    f = recorded_branches.remote(pids)
    kill_first_run(pids)
    with pytest.raises(ch.errors.WorkflowDivergence) as e:
        f.result(timeout=120)
    assert e.value.recorded["site"].endswith(line_of(recorded_branches, "history's branch"))
    assert e.value.received["site"].endswith(line_of(recorded_branches, "replay's branch"))


def test_steps_run_together_name_the_workflows_line():
    # asyncio.gather runs each helper's coroutine from asyncio's loop: the
    # site is read where the helper was called.
    pids = os.path.join(tempfile.mkdtemp(), "pids")
    f = gathered.remote(pids)
    kill_first_run(pids)
    with pytest.raises(ch.errors.WorkflowDivergence) as e:
        f.result(timeout=120)
    assert e.value.step == 1
    for call in (e.value.recorded, e.value.received):
        assert call["site"].endswith(line_of(gathered, "asyncio.gather("))
    assert (e.value.recorded["args"], e.value.received["args"]) == ("(1)", "(2)")


def test_a_maps_items_name_the_workflows_line():
    d = tempfile.mkdtemp()
    f = mapped.remote(os.path.join(d, "steps"), os.path.join(d, "pids"))
    kill_first_run(os.path.join(d, "pids"))
    with pytest.raises(ch.errors.WorkflowDivergence) as e:
        f.result(timeout=120)
    for call in (e.value.recorded, e.value.received):
        assert call["site"].endswith(line_of(mapped, "step.map("))


def test_a_divergence_of_another_shape_still_makes_the_error():
    # It is made on the thread that settles every future, which must not die.
    from chimaera._errors import divergence

    err = divergence('workflow divergence: [{"step": 3}, null]')
    assert isinstance(err, ch.errors.WorkflowDivergence) and err.step == 3
    for reason in ('workflow divergence: ["a", "b"]', "workflow divergence: [1, 2, 3]", "workflow divergence"):
        err = divergence(reason)
        assert isinstance(err, ch.errors.WorkflowDivergence) and str(err) == reason
    assert str(divergence("workflow divergence: " + "[" * 100_000)).startswith("workflow divergence: [[[")


WORKFLOW = """
import os
import time

import chimaera as ch


@ch.remote
def step(i, path):
    with open(path, "a") as f:
        f.write(f"{{i}}\\n")
    return i


{decorator}
async def {name}({params}steps, pids):
    a = await step.remote({first}, steps)
    with open(pids, "a") as f:
        f.write(f"{{os.getpid()}}\\n")
    time.sleep(1.0)  # the deployment happens here, and the worker dies
    b = await step.remote(2, steps)
    return {result}

{bound}
"""

# How the module makes `flow`: (decorator, function name, binding, bound
# parameters). Named, the module's own name for the function is the function
# itself, which a plain pickle sends by reference (as whatever is deployed
# when it loads). Bound, `flow` is a .partial() of that, still durable.
FORMS = {
    "decorated": ('@ch.remote(workflow="durable", {options})', "flow", "", ""),
    "named": ("", "_flow", 'flow = ch.remote(workflow="durable", {options})(_flow)', ""),
    "bound": ("", "_flow", 'flow = ch.remote(workflow="durable", {options})(_flow).partial("bound")', "tag, "),
}

# Workers that imported version 1 before the deployment, as pooled ones have:
# a replay is likely to land on one.
WARM = """
@ch.remote
def warm():
    import wf

    time.sleep(0.5)


ch.get([warm.remote() for _ in range(16)])
"""

DEPLOY = """
import json, os, signal, sys, time

sys.dont_write_bytecode = True
sys.path.insert(0, {mods!r})
import chimaera as ch
import wf
{warm}
steps, pids = os.path.join({mods!r}, "steps"), os.path.join({mods!r}, "pids")
f = wf.flow.remote(steps, pids)
while not (os.path.exists(pids) and open(pids).read().endswith("\\n")):
    time.sleep(0.02)
deployed = os.path.join({mods!r}, "wf.py")
os.replace(os.path.join({mods!r}, "wf_v2.py.txt"), deployed)  # deploy version 2
os.utime(deployed, (time.time() - 3600, time.time() - 3600))  # an older modification time, as rsync -a keeps
os.kill(int(open(pids).readline()), signal.SIGKILL)
print(json.dumps([f.result(timeout=120), open(steps).read().split()]))
"""


def deploy_midway(form, v2_options, v2_first, v2_result):
    """Start version 1 of a workflow module, deploy version 2 over it while
    the workflow is in flight, kill its worker: what the replay returns."""
    mods = tempfile.mkdtemp()

    def module(options, first, result):
        decorator, name, bound, params = (s.format(options=options) for s in FORMS[form])
        return WORKFLOW.format(decorator=decorator, name=name, bound=bound, params=params, first=first, result=result)

    with open(os.path.join(mods, "wf.py"), "w") as f:
        f.write(module("version=1", 1, 'f"v1:{a + b}"'))
    with open(os.path.join(mods, "wf_v2.py.txt"), "w") as f:
        f.write(module(v2_options, v2_first, v2_result))
    r = run_script(DEPLOY.format(mods=mods, warm=WARM))
    assert r.returncode == 0, r.stdout + r.stderr
    return json.loads(r.stdout.splitlines()[-1])


@pytest.mark.parametrize("form", FORMS)
def test_a_history_resumes_on_the_code_it_started_with_after_a_deployment(form):
    # Version 2 makes another first call: run against version 1's history it
    # would diverge, so the history keeps version 1's code.
    assert deploy_midway(form, "version=2", 10, 'f"v2:{a + b}"') == ["v1:3", ["1", "2"]]


@pytest.mark.parametrize("form", FORMS)
def test_a_history_moves_to_a_new_version_that_declares_it_compatible(form):
    assert deploy_midway(form, "version=2, compatible=1", 1, 'f"v2:{a + b}"') == ["v2:3", ["1", "2"]]


HOPS = """
import functools
import os
import time

import chimaera as ch


@functools.lru_cache
def first():
    return 1  # an lru_cache function pickles only by name


@ch.remote
def step(i, path):
    with open(path, "a") as f:
        f.write(f"{{i}}\\n")
    return i


def checkpoint(pids):
    with open(pids, "a") as f:
        f.write(f"{{os.getpid()}}\\n")
    time.sleep(1.0)  # a deployment may happen here, and the worker die


@ch.remote(workflow="durable", {options})
async def flow(steps, pids):
    a = await step.remote(first(), steps)
    checkpoint(pids)
    b = await step.remote({second}, steps)
    checkpoint(pids)
    return f"{name}:{{a + b}}"
"""

DEPLOY_TWICE = """
import json, os, signal, sys, time

sys.dont_write_bytecode = True
sys.path.insert(0, {mods!r})
import chimaera as ch
import wf
{warm}

def deploy_and_kill(version, line):
    while len(open(pids).read().split()) < line:
        time.sleep(0.02)
    os.replace(os.path.join({mods!r}, version), os.path.join({mods!r}, "wf.py"))
    os.kill(int(open(pids).read().split()[line - 1]), signal.SIGKILL)


steps, pids = os.path.join({mods!r}, "steps"), os.path.join({mods!r}, "pids")
open(pids, "w").close()
f = wf.flow.remote(steps, pids)
deploy_and_kill("v2.txt", 1)  # version 1's first checkpoint
deploy_and_kill("v3.txt", 3)  # version 2's second, after a step version 1 does not make
print(json.dumps([f.result(timeout=120), open(steps).read().split()]))
"""


def test_a_history_that_moved_continues_on_the_version_it_moved_to():
    # Version 2 takes over version 1's history and extends it with a call of
    # its own; version 3 continues version 2's histories only. Run with
    # version 1's code, the history would diverge at that call.
    mods = tempfile.mkdtemp()
    for file, options, second, name in [
        ("wf.py", "version=1", 2, "v1"),
        ("v2.txt", "version=2, compatible=1", 20, "v2"),
        ("v3.txt", "version=3, compatible=(2,)", 20, "v3"),
    ]:
        with open(os.path.join(mods, file), "w") as f:
            f.write(HOPS.format(options=options, second=second, name=name))
    r = run_script(DEPLOY_TWICE.format(mods=mods, warm=WARM))
    assert r.returncode == 0, r.stdout + r.stderr
    assert json.loads(r.stdout.splitlines()[-1]) == ["v3:21", ["1", "20"]]


# A workflow module: what of it travels with a call, and what is looked up.
MODULE = """
import functools

import chimaera as ch

TAG = {tag!r}


def helper():
    return TAG + " helper"


class Box:
    tag = TAG + " class"


@functools.lru_cache
def cached():
    return TAG + " cached"


@ch.remote
def step():
    return TAG + " step"


@ch.remote(workflow="durable", {options})
async def flow():
    return [TAG, step._fn(), helper(), Box.tag, cached()]


async def _bound(suffix):
    return [TAG + suffix, step._fn(), helper(), Box.tag, cached()]


bound = ch.remote(workflow="durable", {options})(_bound).partial("")
"""


def test_a_history_carries_its_code_and_the_values_it_names(tmp_path, monkeypatch):
    # As the guide says: the workflow's code, the module-level values it
    # names and its @ch.remote functions travel with the call; other
    # functions and classes are looked up by name where it runs.
    from chimaera import _serde

    monkeypatch.setattr(sys, "dont_write_bytecode", True)
    monkeypatch.syspath_prepend(str(tmp_path))
    path = tmp_path / "wf_carried.py"
    path.write_text(MODULE.format(tag="v1", options="version=1"))
    import wf_carried

    sent = [_serde.dump_code(wf_carried.flow._fn), _serde.dump_code(wf_carried.bound._fn)]  # the module names _bound
    path.write_text(MODULE.format(tag="v2", options="version=1"))
    del sys.modules["wf_carried"]
    for blob in sent:
        assert asyncio.run(_serde.load_code(blob)()) == ["v1", "v1 step", "v2 helper", "v2 class", "v2 cached"]
    del sys.modules["wf_carried"]


def test_a_process_that_loaded_an_older_deployment_finds_the_new_one(tmp_path, monkeypatch):
    # A pooled worker that imported version 1 before the deployment keeps it
    # in sys.modules; a replay there must still see what is on disk, though
    # the deployment kept an older modification time (rsync -a, cp -p).
    from chimaera import _serde, _workflow

    monkeypatch.setattr(sys, "dont_write_bytecode", True)
    monkeypatch.syspath_prepend(str(tmp_path))
    path = tmp_path / "wf_loaded.py"

    def deploy(tag, options):
        path.write_text(MODULE.format(tag=tag, options=options))
        os.utime(path, (1e9, 1e9))  # 2001: older than any process

    deploy("v1", "version=1")
    import wf_loaded

    v1 = wf_loaded.flow._fn
    deploy("v1 again", "version=1")
    assert _workflow._deployed(v1)[0] is v1, "the same version: the definition this process loaded"
    deploy("v2", "version=2, compatible=1")
    deployed, module = _workflow._deployed(v1)
    assert deployed.__chimaera_workflow_version__[0] == 2
    assert asyncio.run(deployed()) == ["v2", "v2 step", "v2 helper", "v2 class", "v2 cached"]
    assert sys.modules["wf_loaded"].flow._fn is v1, "what the process loaded stays as it was"
    # The record of the move carries what a call would, from a module this
    # process imported: the functions and classes it looks up go by name.
    moved = _serde.dump_code(deployed, names=module)
    deploy("v3", "version=3")
    del sys.modules["wf_loaded"]
    assert asyncio.run(_serde.load_code(moved)()) == ["v2", "v2 step", "v3 helper", "v3 class", "v3 cached"]
    del sys.modules["wf_loaded"]


SHIP = """
import sys
import chimaera as ch
from chimaera import _serde


class Payments(ch.durable.Activity[str]):  # a script's class: it travels by value
    idempotent = True

    def run(self, key: str, cents: int) -> str:
        return f"receipt-{key}"


async def checkout(order_id):
    return Payments()


open(sys.argv[1], "wb").write(_serde.dump_code(checkout))  # the workflow, as its call ships it
"""

# A worker replaying the workflow: it never compiled the script.
FINGERPRINT = """
import pickle, sys
import cloudpickle
from chimaera import _serde, _workflow

code, *first = sys.argv[1:]
for path in first:  # what this process loaded before: the step's arguments
    pickle.loads(open(path, "rb").read())
cls = _serde.load_code(open(code, "rb").read()).__globals__["Payments"]
frame = (((cls().run, ("order-1", 500), {}),), {})
spec, packed = {"fn_digest": b""}, {"blobs": [], "args_inline": b""}
print(_workflow._fingerprint((_workflow._call, "", frame), spec, packed).hex())
print(cloudpickle.dumps(frame[0][0], protocol=5).hex())
"""


def test_a_scripts_class_has_one_fingerprint_whatever_its_process_loaded_before():
    # A step's arguments are pickled again on every replay, in whatever
    # process runs it. By value, a class pickles to other bytes in a process
    # that loaded it from another pickle first (which equal strings are one
    # object differs), so a fingerprint of those bytes would diverge.
    d = tempfile.mkdtemp()
    code, sent = os.path.join(d, "code.pkl"), os.path.join(d, "sent.pkl")

    def run(script, *argv):
        path = os.path.join(d, "script.py")
        with open(path, "w") as f:
            f.write(script)
        env = dict(os.environ, PYTHONPATH=os.path.join(REPO, "python"))
        out = subprocess.run([sys.executable, path, *argv], capture_output=True, text=True, env=env, timeout=60)
        assert out.returncode == 0, out.stderr
        return out.stdout.split()

    run(SHIP, code)
    first, args = run(FINGERPRINT, code)
    with open(sent, "wb") as f:
        f.write(bytes.fromhex(args))
    again, _ = run(FINGERPRINT, code, sent)  # a replay's process that ran the step before
    assert again == first


def test_versions_are_for_durable_workflows():
    async def flow() -> None:
        pass

    with pytest.raises(TypeError, match="durable workflows"):
        ch.remote(version=2)(flow)
    with pytest.raises(ch.OptionsError, match="compatible"):
        ch.remote(workflow="durable", compatible=0)(flow)


def test_a_replay_reads_a_step_whose_future_was_dropped():
    # The replay answers the step from the run's record: dropping the step's
    # future must not have released that record.
    d = tempfile.mkdtemp()
    f = dropped_step.remote(os.path.join(d, "steps"), os.path.join(d, "pids"))
    kill_first_run(os.path.join(d, "pids"))
    assert f.result(timeout=120) == 10
    assert lines(os.path.join(d, "steps")) == ["1"], "the step ran once"


def test_workflow_helpers_need_a_workflow():
    with pytest.raises(RuntimeError, match="workflow='durable'"):
        asyncio.run(ch.durable.now())
