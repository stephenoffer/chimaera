"""Durable actors: state committed with each call, repeats answered from the
stored reply, restarts resume from the committed state."""

from __future__ import annotations

import hashlib
import os
import random
import signal
import subprocess
import tempfile
import uuid

import pytest
from conftest import BIN, run_script

import chimaera as ch


@ch.remote(state=ch.durable.State())
class Account:
    def __init__(self):
        self.balance = 0

    def deposit(self, amount):
        self.balance += amount
        return self.balance

    def overdraw(self):
        self.balance = -1_000_000
        raise ValueError("refused")

    def get(self):
        return self.balance

    def pid(self):
        return os.getpid()


def test_a_repeated_command_returns_its_first_reply_and_changes_nothing():
    a = Account.remote()
    assert a.deposit.options(command_id="op-1").remote(5).result(timeout=60) == 5
    assert a.deposit.options(command_id="op-2").remote(2).result(timeout=60) == 7
    # The client retries op-1 (say its first reply was lost): same answer, no second deposit.
    assert a.deposit.options(command_id="op-1").remote(5).result(timeout=60) == 5
    assert a.get.remote().result(timeout=60) == 7


def test_a_command_id_reused_with_other_arguments_is_a_conflict():
    a = Account.remote()
    assert a.deposit.options(command_id="op-1").remote(5).result(timeout=60) == 5
    with pytest.raises(ch.errors.CommandConflict):
        a.deposit.options(command_id="op-1").remote(9).result(timeout=60)
    assert a.get.remote().result(timeout=60) == 5


def test_a_call_that_raises_leaves_no_trace_in_the_state():
    a = Account.remote()
    a.deposit.remote(7).result(timeout=60)
    with pytest.raises(ValueError, match="refused"):
        a.overdraw.remote().result(timeout=60)
    assert a.get.remote().result(timeout=60) == 7


def test_a_restarted_instance_resumes_from_the_committed_state():
    a = Account.remote()
    a.deposit.remote(3).result(timeout=60)
    a.deposit.remote(4).result(timeout=60)
    first = a.pid.remote().result(timeout=60)
    os.kill(first, signal.SIGKILL)
    # The next call runs on a new process that did not run __init__ again.
    assert a.get.remote().result(timeout=60) == 7
    assert a.pid.remote().result(timeout=60) != first


def test_durable_actors_run_one_call_at_a_time():
    with pytest.raises(ch.OptionsError, match="one call at a time"):
        Account.options(concurrency=4).remote()


def test_command_id_names_one_call_only():
    with pytest.raises(TypeError, match="names one call"):
        Account.options(command_id="x")


@ch.remote(state=ch.durable.State())
class Ledger:
    def __init__(self):
        self.entries = []

    def add(self, x):
        self.entries.append(x)
        return len(self.entries)

    def pid(self):
        return os.getpid()


def ledger_v2(state, version):
    assert version == 1
    return {"items": state["entries"], "total": sum(state["entries"])}


@ch.remote(state=ch.durable.State(version=2, migrate=ledger_v2))
class LedgerV2:
    def add(self, x):
        self.items.append(x)
        self.total += x
        return len(self.items)

    def summary(self):
        return self.items, self.total


def broken(state, version):
    raise ValueError(f"cannot read version {version}")


@ch.remote(state=ch.durable.State(version=3, migrate=broken))
class LedgerBroken:
    def summary(self):
        return "unreachable"


@ch.remote(state=ch.durable.State(version=4, migrate=lambda state, version: {**state, "seen": version}))
class LedgerV4:
    def summary(self):
        return self.items, self.total, self.seen


def test_v1_state_restores_with_v2_code_and_keeps_its_command_receipts():
    name = f"ledger-{uuid.uuid4().hex[:8]}"
    v1 = Ledger.options(name=name).remote()
    assert v1.add.options(command_id="a").remote(3).result(timeout=60) == 1
    assert v1.add.options(command_id="b").remote(4).result(timeout=60) == 2
    os.kill(v1.pid.remote().result(timeout=60), signal.SIGKILL)
    # The same name with a class of a newer state version binds the actor to it.
    v2 = LedgerV2.options(name=name).remote()
    assert v2.id == v1.id
    assert v2.summary.remote().result(timeout=60) == ([3, 4], 7)
    # A command committed under v1 answers from its receipt: v2 does not apply it again.
    assert v2.add.options(command_id="a").remote(3).result(timeout=60) == 1
    assert v2.add.options(command_id="c").remote(5).result(timeout=60) == 3
    assert v2.summary.remote().result(timeout=60) == ([3, 4, 5], 12)
    # Older code never binds it back.
    assert Ledger.options(name=name).remote().add.remote(1).result(timeout=60) == 4
    assert v2.summary.remote().result(timeout=60) == ([3, 4, 5, 1], 13)
    # A migration that raises fails the calls and leaves the committed state;
    # a fixed one restores from it.
    broke = LedgerBroken.options(name=name).remote()
    with pytest.raises(ValueError, match="cannot read version 2"):
        broke.summary.remote().result(timeout=60)
    v4 = LedgerV4.options(name=name).remote()
    assert v4.summary.remote().result(timeout=60) == ([3, 4, 5, 1], 13, 2)


@ch.remote(state=ch.durable.State())
class Blob:
    def __init__(self):
        self.data = b""
        self.grown = 0

    def grow(self, size, seed):
        self.data += random.Random(seed).randbytes(size)
        self.grown += 1
        return len(self.data)

    def digest(self):
        return hashlib.sha256(self.data).hexdigest(), self.grown

    def pid(self):
        return os.getpid()


def expected(*grown: tuple[int, int]) -> tuple[str, int]:
    return hashlib.sha256(b"".join(random.Random(seed).randbytes(n) for n, seed in grown)).hexdigest(), len(grown)


def test_state_past_the_inline_limit_commits_in_chunks_and_restores():
    b = Blob.remote()
    assert b.grow.remote(3 << 20, 1).result(timeout=120) == 3 << 20
    assert b.grow.remote(1024, 2).result(timeout=120) == (3 << 20) + 1024  # only its last chunk changes
    os.kill(b.pid.remote().result(timeout=60), signal.SIGKILL)
    assert b.digest.remote().result(timeout=120) == expected((3 << 20, 1), (1024, 2))


CRASH_MID_COMMIT = """
import hashlib, os, random, signal, time
import chimaera as ch

@ch.remote(state=ch.durable.State())
class Blob:
    def __init__(self):
        self.data = b""
        self.grown = 0

    def grow(self, size, seed):
        self.data += random.Random(seed).randbytes(size)
        self.grown += 1
        return len(self.data)

    def digest(self):
        return hashlib.sha256(self.data).hexdigest(), self.grown

    def pid(self):
        return os.getpid()

def expected(*grown):
    return hashlib.sha256(b"".join(random.Random(seed).randbytes(n) for n, seed in grown)).hexdigest(), len(grown)

b = Blob.remote()
b.grow.remote(3 << 20, 1).result(timeout=120)
pid = b.pid.remote().result(timeout=60)
grown = b.grow.remote(3 << 20, 2)
time.sleep(1.5)  # its commit is putting chunks, 0.3 s apart
os.kill(pid, signal.SIGKILL)
print("SIZE", grown.result(timeout=240))
print("STATE", b.digest.remote().result(timeout=120) == expected((3 << 20, 1), (3 << 20, 2)))
"""


def test_a_crash_while_its_chunks_are_written_leaves_one_complete_revision():
    home = tempfile.mkdtemp(prefix="chd", dir="/tmp")
    env = {"CHIMAERA_HOME": home, "CHIMAERA_STATE_CHUNK_DELAY_MS": "300"}
    try:
        out = run_script(CRASH_MID_COMMIT, env=env, home=home, timeout=290)
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)
    assert out.returncode == 0, out.stderr[-3000:]
    # The call ran again on the restarted instance, from the first revision: applied once.
    assert f"SIZE {6 << 20}" in out.stdout and "STATE True" in out.stdout, out.stdout
