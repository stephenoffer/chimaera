"""Calls that failed for good: listed by their run, retried, or discarded."""

import uuid

import pytest

import chimaera as ch

TRIES = ch.coord.Store("test-failed-calls")


def fails_first(tag, x):
    """Fails on its first run per tag, then works."""
    if TRIES.counter(tag).add() == 1:
        raise ValueError(f"bad row {x}")
    return x * 2


def big(n):
    return b"x" * n


def size_after_failing_once(tag, data):
    if TRIES.counter(tag).add() == 1:
        raise RuntimeError("flaky")
    return len(data)


def run():
    return ch.runs.current_run()


def mine(tag):
    return [c for c in run().failed() if tag in str(c.error)]


def test_a_call_that_failed_is_listed_and_a_retry_runs_it_again():
    tag = uuid.uuid4().hex
    f = ch.remote(fails_first).remote(tag, 21)
    with pytest.raises(ValueError):
        f.result(timeout=60)
    (failed,) = [c for c in run().failed() if c.id == f.key[16:].hex()]
    assert failed.function.endswith("fails_first") and isinstance(failed.error, ValueError)
    (again,) = run().retry_failed([failed])
    assert again.result(timeout=60) == 42
    assert failed.id not in [c.id for c in run().failed()], "a retried call leaves the list"


def test_calls_whose_input_failed_and_cancelled_calls_are_not_listed():
    tag = uuid.uuid4().hex
    root = ch.remote(fails_first).remote(tag, 1)
    after = ch.remote(lambda x: x).remote(root)
    with pytest.raises(ValueError):
        after.result(timeout=60)
    slow = ch.remote(lambda: __import__("time").sleep(30)).remote()
    slow.cancel()
    ids = {c.id for c in run().failed()}
    assert root.key[16:].hex() in ids
    assert after.key[16:].hex() not in ids and slow.key[16:].hex() not in ids
    run().discard_failed([root.key[16:].hex()])


def test_a_retry_reads_a_large_input_whose_future_was_dropped():
    # The shard keeps the failed call's inputs pinned (shard tests count the
    # pins); here, the retry reads the input after the driver let it go.
    tag = uuid.uuid4().hex
    data = ch.remote(big).remote(4 << 20)
    f = ch.remote(size_after_failing_once).remote(tag, data)
    with pytest.raises(RuntimeError):
        f.result(timeout=60)
    failed_id = f.key[16:].hex()
    del data, f  # f's future keeps its inputs' futures too
    (failed,) = [c for c in run().failed() if c.id == failed_id]
    (again,) = run().retry_failed([failed])
    assert again.result(timeout=60) == 4 << 20


def test_discarding_forgets_failed_calls_and_unknown_ids_are_refused():
    tags = [uuid.uuid4().hex for _ in range(3)]
    futures = [ch.remote(fails_first).remote(t, 1) for t in tags]
    for f in futures:
        with pytest.raises(ValueError):
            f.result(timeout=60)
    ids = [f.key[16:].hex() for f in futures]
    assert run().discard_failed(ids) == 3
    assert not set(ids) & {c.id for c in run().failed()}
    with pytest.raises(ValueError, match="not among this run's failed calls"):
        run().retry_failed(ids[:1])


def test_the_cli_lists_failed_calls_and_retries_them_with_yes():
    import subprocess

    from conftest import BIN, HOME

    tag = uuid.uuid4().hex
    f = ch.remote(fails_first).remote(tag, 5)
    with pytest.raises(ValueError):
        f.result(timeout=60)
    call = f.key[16:].hex()
    cli = [BIN, "--dir", HOME, "retry", run().id, "--call", call]
    listed = subprocess.run(cli, capture_output=True, text=True, timeout=60)
    assert listed.returncode == 0 and call in listed.stdout and "add --yes" in listed.stdout, listed.stdout
    assert call in [c.id for c in run().failed()], "listing alone retries nothing"
    done = subprocess.run([*cli, "--yes"], capture_output=True, text=True, timeout=60)
    assert done.returncode == 0 and f"retrying {call}" in done.stdout, done.stdout + done.stderr
    deadline = __import__("time").monotonic() + 30
    while call in [c.id for c in run().failed()] and __import__("time").monotonic() < deadline:
        __import__("time").sleep(0.05)
    assert call not in [c.id for c in run().failed()]
    # The CLI does not wait for the new call: it runs a moment later.
    while TRIES.counter(tag).value() < 2 and __import__("time").monotonic() < deadline:
        __import__("time").sleep(0.05)
    assert TRIES.counter(tag).value() == 2, "it ran again"
