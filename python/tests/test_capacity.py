"""A forecast provisioner's plans (chimaera/capacity in the store) as calls
see them: ch.runs.cluster().capacity(), and explain() while a call waits."""

from __future__ import annotations

import os

from conftest import run_script


def test_a_waiting_call_shows_each_pools_plan_and_a_finished_one_does_not():
    pool = f"k8s/capacity-test-{os.getpid()}"
    r = run_script(
        f"""
        import time
        import chimaera as ch

        store = ch.coord.Store("chimaera/capacity")
        plan = {{"pool": {pool!r}, "replicas": 2, "want": 3, "action": "grow", "reason": "grow from 2 to 3 peers"}}
        store.put("plan:{pool}", plan)
        try:
            assert [p for p in ch.runs.cluster().capacity() if p["pool"] == {pool!r}] == [plan]

            @ch.remote
            def nap(s):
                time.sleep(s)
                return s

            waiting = nap.remote(3)
            shown = [p for p in waiting.explain().get("capacity", ()) if p["pool"] == {pool!r}]
            print("WAITING", shown == [plan])
            waiting.result(timeout=60)
            print("DONE", "capacity" in waiting.explain())
        finally:
            store.delete("plan:{pool}")
        """
    )
    assert r.returncode == 0, r.stderr
    assert "WAITING True" in r.stdout and "DONE False" in r.stdout, r.stdout
