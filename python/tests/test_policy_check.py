"""The peer checks a call's policy itself: options are checked in Python, but
any client may talk to a peer, so a NaN or an inverted bound that gets past
the package is refused before it is stored."""

from __future__ import annotations

from conftest import run_script


def test_a_policy_the_package_would_refuse_is_refused_by_the_peer():
    r = run_script(
        """
        import chimaera as ch
        from chimaera import _options

        @ch.remote
        def add(a, b):
            return a + b

        assert add.remote(1, 2).result(timeout=60) == 3
        real = _options.policy_dict
        changes = ({"cpus": float("nan")}, {"cpus_min": 4.0, "cpus_max": 1.0}, {"gpus": -1.0})
        for i, change in enumerate(changes):
            # A client that skipped the checks: the same call, another policy
            # (a new priority each time, so the resolved options are not reused).
            _options.policy_dict = lambda eff, change=change: {**real(eff), **change}
            try:
                add.options(priority=i + 1).remote(1, 2).result(timeout=60)
                print("ran", change)
            except ch.ChimaeraError as e:
                print(type(e).__name__, e)
        """
    )
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    assert len(lines) == 3, r.stdout
    assert "cpus: expected a finite number >= 0, got NaN" in lines[0], lines[0]
    assert "cpus: ch.auto min 4 is more than max 1" in lines[1], lines[1]
    assert "gpus: expected a finite number >= 0, got -1" in lines[2], lines[2]
