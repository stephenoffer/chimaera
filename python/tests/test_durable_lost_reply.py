"""A durable actor whose commit landed but whose answer was lost.

When the leader of an actor's group dies between a commit and its answer,
the host learns neither that the commit landed nor the record the leader
would have pushed after it. The call is retried under the same command id
and answered from the stored reply, and the instance must then carry on
from the committed state: before, it restored its own older copy, and every
later commit was refused as "state version moved" until the call gave up.
Two debug-build test settings stand in for that leader's death: one drops
the answer to the peer's second durable commit (the first deposit's; the
constructor's is the first), the other the record pushes that follow it.
"""

from __future__ import annotations

import pytest
from test_cluster import Cluster, lines


@pytest.mark.parametrize("pushes", ["kept", "lost"])
def test_after_a_lost_commit_answer_the_actor_carries_on_from_the_committed_state(pushes):
    env = {"CHIMAERA_TEST_LOSE_COMMIT_REPLY": "2", "CHIMAERA_TEST_NO_RECORD_PUSH": "1" if pushes == "lost" else "0"}
    c = Cluster(1, env={0: env})
    try:
        out = c.run(
            0,
            """
import chimaera as ch

@ch.remote(state=ch.durable.State())
class Account:
    def __init__(self):
        self.balance = 0

    def deposit(self, amount):
        self.balance += amount
        return self.balance

acct = Account.remote()
first = acct.deposit.remote(1)
print("FIRST", first.result(timeout=60), flush=True)
print("WHY", " | ".join(str(f) for f in first.explain()["failures"]), flush=True)
print("SECOND", acct.deposit.remote(1).result(timeout=60), flush=True)
""",
        )
        assert lines(out, "FIRST") == ["1"], out.stdout + out.stderr[-2000:]
        why = lines(out, "WHY")
        assert why and "the commit's answer was dropped" in why[0], (
            f"the test setting dropped no answer (it works only in a debug build of the peer): {why}"
        )
        assert lines(out, "SECOND") == ["2"], out.stdout + out.stderr[-2000:]
    finally:
        c.close()
