"""Durable actors: state committed with each call, repeats answered from the
stored reply, restarts resume from the committed state."""

from __future__ import annotations

import os
import signal

import pytest

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
