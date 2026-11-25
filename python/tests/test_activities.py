"""Activities: a durable workflow's writes to other systems. Each runs under a
key the run keeps; an attempt lost after the write runs again only when the
activity says that is safe, is settled by asking the other system, or raises
ch.OutcomeUnknown with its key."""

from __future__ import annotations

import json
import os
import signal
import tempfile
import time
import uuid

import pytest

import chimaera as ch


class Gateway(ch.durable.Activity[dict]):
    """A payment gateway stand-in: each charge is a line in a ledger file, and
    every call of run() a line in a calls file. The first run with a crash
    file not yet made dies right after charging, before its receipt is back."""

    def __init__(self, root: str) -> None:
        self.ledger, self.calls, self.crash = (os.path.join(root, n) for n in ("ledger", "calls", "crashed"))

    def charges(self, key: str) -> list[dict]:
        return [c for c in entries(self.ledger) if c["key"] == key]

    def run(self, key: str, cents: int) -> dict:
        with open(self.calls, "a") as f:
            f.write(key + "\n")
        receipt = {"key": key, "cents": cents, "charge": uuid.uuid4().hex}
        with open(self.ledger, "a") as f:
            f.write(json.dumps(receipt) + "\n")
        if not os.path.exists(self.crash):
            open(self.crash, "x").close()
            os._exit(1)  # the money moved; the receipt never comes back
        return receipt

    def compensate(self, key: str, receipt: dict) -> None:
        with open(self.ledger, "a") as f:
            f.write(json.dumps({"key": key, "refund": receipt["charge"]}) + "\n")


class IdempotentGateway(Gateway):
    """Charges once per key: a repeat returns the first charge's receipt."""

    idempotent = True

    def run(self, key: str, cents: int) -> dict:
        done = self.charges(key)
        if done:
            with open(self.calls, "a") as f:
                f.write(key + "\n")
            return done[0]
        return super().run(key, cents)


class LookupGateway(Gateway):
    """Not idempotent, but answers what it did under a key."""

    def status(self, key: str) -> dict | None:
        done = self.charges(key)
        return done[0] if done else None


def entries(path: str) -> list[dict]:
    try:
        with open(path) as f:
            return [json.loads(line) for line in f]
    except FileNotFoundError:
        return []


def lines(path: str) -> list[str]:
    try:
        with open(path) as f:
            return f.read().splitlines()
    except FileNotFoundError:
        return []


@ch.remote(workflow="durable")
async def checkout(gateway: Gateway, key: str | None, pids: str) -> dict:
    receipt = await ch.durable.record(gateway, 500, key=key)
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)  # the test kills the workflow's worker here, once
    return receipt


@ch.remote(workflow="durable")
async def charge_then_refund(gateway: Gateway, key: str) -> list[dict]:
    receipt = await ch.durable.record(gateway, 700, key=key)
    await ch.durable.compensate(gateway, key, receipt)
    return entries(gateway.ledger)


@ch.remote(workflow="durable")
async def charge_with_its_own_key(gateway: Gateway) -> dict:
    return await ch.durable.record(gateway, 300)


def kill_first(pids: str) -> None:
    deadline = time.monotonic() + 60
    while not lines(pids):
        assert time.monotonic() < deadline, "the workflow never got past its activity"
        time.sleep(0.02)
    os.kill(int(lines(pids)[0]), signal.SIGKILL)


def test_a_retried_idempotent_charge_takes_effect_once_with_a_receipt_replays_read_back():
    gw = IdempotentGateway(tempfile.mkdtemp())
    pids = os.path.join(tempfile.mkdtemp(), "pids")
    f = checkout.remote(gw, "order-1", pids)
    kill_first(pids)  # after the charge's receipt is recorded: the replay reads it back
    receipt = f.result(timeout=120)
    assert gw.charges("order-1") == [receipt], "one charge, and the receipt is the gateway's own"
    assert receipt["cents"] == 500
    assert lines(gw.calls) == ["order-1", "order-1"], "the lost attempt ran again; the replay did not"
    assert len(lines(pids)) == 2, "the workflow itself ran twice"


def test_an_activity_that_cannot_repeat_surfaces_the_uncertain_outcome_with_its_key():
    gw = Gateway(tempfile.mkdtemp())
    f = checkout.remote(gw, "order-2", os.path.join(tempfile.mkdtemp(), "pids"))
    with pytest.raises(ch.OutcomeUnknown) as e:
        f.result(timeout=120)
    assert e.value.key == "order-2"
    assert "order-2" in "".join(e.value.__notes__)
    assert len(gw.charges("order-2")) == 1, "the write happened once and was not repeated"
    assert lines(gw.calls) == ["order-2"]


def test_a_status_lookup_settles_a_lost_attempt_under_the_runs_own_key():
    gw = LookupGateway(tempfile.mkdtemp())
    receipt = charge_with_its_own_key.remote(gw).result(timeout=120)
    [charge] = entries(gw.ledger)
    assert receipt == charge, "the lookup found the charge the lost attempt made"
    assert lines(gw.calls) == [receipt["key"]], "and it was not charged again"
    run_id, task_id, _ = receipt["key"].split("-")
    assert run_id == ch.runs.current_run().id
    assert len(task_id) == 32


def test_a_compensation_runs_once_after_its_activity():
    gw = IdempotentGateway(tempfile.mkdtemp())
    open(gw.crash, "x").close()  # no crash: charge, then refund
    charge, refund = charge_then_refund.remote(gw, "order-3").result(timeout=120)
    assert refund == {"key": "order-3", "refund": charge["charge"]}


def test_activity_helpers_need_a_workflow():
    import asyncio

    with pytest.raises(RuntimeError, match="workflow='durable'"):
        asyncio.run(ch.durable.compensate(Gateway(tempfile.mkdtemp()), "k", {}))
