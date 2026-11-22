# When things go wrong: errors, retries and recovery

Calls fail, workers die, peers go away. You can tell each failure apart from
your script, let Chimaera retry what is safe to retry, and make side effects,
long runs and multi-step workflows survive the rest.

## Catch your own exceptions

An exception raised inside a call comes back from `result()` with its own
type, and the remote traceback is attached as a note:

```python run
import chimaera as ch

@ch.remote
def parse(text: str) -> int:
    return int(text)

try:
    parse.remote("abc").result()
except ValueError as e:
    assert "abc" in str(e)
    assert e.__notes__[0].startswith("remote traceback")
```

Python prints the notes under the traceback, so an uncaught error shows both
your local stack and the line in the call that raised. A call that raises
ran once: Chimaera never retries your exception unless you list it in
`retry_exceptions`.

A call that depends on a failed call never runs. It raises the root cause's
type, with a note naming the call that raised it:

```python run
@ch.remote
def add(a: int, b: int) -> int:
    return a + b

err = add.remote(parse.remote("x"), 1).exception()     # waits, returns the error
assert isinstance(err, ValueError) and "this call never ran" in err.__notes__[0]
```

The original type comes back only if it can be unpickled in your script.
When it can't, you get `ch.errors.RemoteError` with `type_name`, `message` and
`remote_traceback`. The usual cause is an exception whose `__init__` takes
different arguments from the ones it passes to `super().__init__()`. Python
rebuilds an exception as `cls(*e.args)`, so pass every argument through:

```python run
class PaymentDeclined(Exception):
    def __init__(self, order: int, code: str):
        super().__init__(order, code)    # e.args == (order, code): it unpickles
        self.order, self.code = order, code

@ch.remote
def pay(order: int) -> None:
    raise PaymentDeclined(order, "insufficient_funds")

err = pay.remote(7).exception()
assert isinstance(err, PaymentDeclined) and err.code == "insufficient_funds"
```

`SystemExit`, `KeyboardInterrupt` and `GeneratorExit` raised in a call also
come back as `RemoteError`, so they can't end your script. A failed call whose
error nothing reads is reported on stderr (`chimaera: ... failed and nothing
read its error`), not dropped. In a map, `results.outcomes()` yields
`(index, value, error)` per item instead of raising at the first failure
([tasks.md](tasks.md)).

## Know which error you got

Every error Chimaera raises derives from `ch.ChimaeraError`, apart from the
built-in `TimeoutError` of a wait. The common ones are on `ch`; all of them
are in `ch.errors`.

| Error | Raised when | What it carries |
|---|---|---|
| your own type | your code raised it | the remote traceback as a note |
| `ch.errors.RemoteError` | your exception can't be rebuilt here | `type_name`, `message`, `remote_traceback` |
| `ch.InfrastructureFailure` | workers or peers were lost on every attempt (`max_attempts`) | message: how many attempts, and the last cause |
| `ch.OutOfMemory` | killed for memory on its first try and both retries, or the retry can't fit the peer | message: peak and limit in bytes |
| `ch.ActorDied` | the actor's process died, or it was closed ([actors.md](actors.md)) | the reason |
| `ch.errors.DataLost` | a value is on no live peer and its call can't run again | why it can't be remade |
| `ch.errors.StreamInterrupted` | a generator call ended before all its items arrived | message: items received of the total |
| `ch.errors.QuorumLost` | a group of the cluster lost the majority of its peers ([clusters.md](clusters.md)) | the group |
| `ch.errors.GangFailed` | every generation of a gang lost a rank ([gangs.md](gangs.md)) | the reason |
| `ch.errors.WorkerCrashed` | defined, but the current peer reports repeated worker deaths as `InfrastructureFailure` | |
| `ch.OutcomeUnknown` | an `effects="external"` call was lost after it started | the attempt and the cause |
| `ch.Cancelled` | you, its caller, its run or your script's exit canceled it | who canceled it |
| `ch.DeadlineExceeded` | its deadline passed | |
| `ch.errors.BudgetExceeded` | its call tree used up its `budget` ([resources.md](resources.md)) | the budget and what was used |
| `ch.Infeasible` | no peer can ever run the request | the constraint |
| `ch.OptionsError` | options are invalid or conflict (also a `ValueError` and `TypeError`) | each field and where it was set |
| `ch.SerializationError` | an argument or captured variable can't be pickled | which argument or variable |
| `ch.errors.PreflightError` | base of `SerializationError`: the call failed a check before it was sent | |
| `ch.NotFound` | no actor or queue under that name (also a `LookupError`) | the name |
| `ch.errors.Closed` | a queue or topic was closed | |
| `ch.errors.VersionConflict` | a store write's `if_version` didn't match | `version`, the current one (0: absent) |
| `ch.errors.LeaseLost` | a lock's lease expired or passed to another holder | |
| `ch.errors.CommandConflict` | a durable actor's `command_id` was reused with other arguments | |
| `ch.errors.WorkflowDivergence` | a durable workflow made a different call on replay | the step |
| `ch.errors.Overloaded` | defined with `retry_after`; nothing raises it yet | `retry_after` seconds |
| `TimeoutError` | your wait ran out; the call is still running | |

Two base classes catch families. `except ch.InfrastructureFailure` covers
`OutOfMemory`, `ActorDied`, `DataLost`, `StreamInterrupted`, `QuorumLost`,
`GangFailed` and `WorkerCrashed`. `except ch.Cancelled` covers
`DeadlineExceeded` and `BudgetExceeded`, and `future.cancelled()` is true for
all three.

## Retry what is safe to retry

Lost workers and peers are retried for you. Your own exceptions are retried
only when you list them in `retry_exceptions` (a class or a tuple; subclasses
match):

```python run
class Throttled(Exception):
    pass

@ch.remote(retry_exceptions=Throttled, max_attempts=4)
def fetch(page: int) -> str:
    attempt = ch.context().attempt          # 1 on the first try
    if attempt < 3:
        raise Throttled(f"429 on attempt {attempt}")
    return f"page {page}"

assert fetch.remote(1).result() == "page 1"
```

`max_attempts` (default 3) counts every attempt, the first included, and
worker losses and listed exceptions share it. Retries start again at once,
with no backoff. One rule surprises people: when an attempt fails exactly
like the one before it (same type, message and traceback lines), Chimaera
stops and raises it, because a failure that repeats word for word won't go
away by itself:

```python run
@ch.remote(retry_exceptions=ConnectionError, max_attempts=5)
def query() -> None:
    raise ConnectionRefusedError("db is down")

f = query.remote()
assert isinstance(f.exception(), ConnectionRefusedError)
assert len(f.explain()["attempts"]) == 2         # not 5
```

Running out of memory has its own allowance, apart from `max_attempts`
([Cap memory](#cap-memory)). A durable workflow defaults to 100 attempts.

## Declare side effects

Retrying is only safe if running the call twice is harmless. `effects` tells
Chimaera how careful to be:

| `effects=` | Use it for | A lost attempt | `retry_exceptions` | A lost result |
|---|---|---|---|---|
| `"none"` (default) | pure functions | runs again, up to `max_attempts` | honored | is remade by running the call again |
| `"idempotent"` | writes that are safe to repeat | runs again, up to `max_attempts` | honored | is remade by running the call again |
| `"external"` | charges, emails, anything that must not repeat | runs again only if lost before it started; after that, `ch.OutcomeUnknown` | ignored: your exception is raised | is not remade (`DataLost`) |

An `"idempotent"` call also stays in its own cell: only `"none"` calls may be
sent to another cell ([../federation.md](../federation.md)).

For an `"external"` call, the peer records the start in the run's replicated
record before your code runs. If the worker or peer is lost before that, the
call is tried again, up to 3 attempts in all. If it's lost after, nobody
knows whether the effect happened, so the call fails with `ch.OutcomeUnknown`
and Chimaera never runs it again. Asking for more than one attempt is an
error: `max_attempts=3` on an external call raises `ch.OptionsError` at
`.remote()`.

The way to handle `OutcomeUnknown` is an idempotency key: an id you choose
(the order id here) that the external system uses to recognize a repeat.
Many payment and messaging APIs accept one. Then trying again is safe. Here a
directory stands in for a payment API that charges once per key, and the
worker dies right after the money moves:

```python run
import os
import tempfile

GATEWAY = tempfile.mkdtemp()

def gateway_charge(key: str, cents: int) -> str:
    """Charge once per key; a repeat returns the first receipt."""
    try:
        with open(os.path.join(GATEWAY, key), "x") as f:
            f.write(str(cents))
    except FileExistsError:
        pass                                 # already charged under this key
    return f"receipt-{key}"

@ch.remote(effects="external")
def charge(order_id: str, cents: int, crash: bool = False) -> str:
    receipt = gateway_charge(key=order_id, cents=cents)
    if crash:
        os._exit(1)                          # the worker dies after the charge
    return receipt

try:
    receipt = charge.remote("order-17", 500, crash=True).result()
except ch.OutcomeUnknown:
    receipt = charge.remote("order-17", 500).result()   # same key: no second charge

assert receipt == "receipt-order-17"
assert os.listdir(GATEWAY) == ["order-17"]               # charged once
```

Only the decorator can choose a weaker contract. `.options(effects=...)` and
`with ch.defaults(effects=...)` can make a call stricter, and one that would
loosen the decorator's setting raises `ch.OptionsError`. Durable actors handle
repeats with a `command_id` instead ([actors.md](actors.md)).

## Cap memory

```python run
import time

@ch.remote(memory_limit="100 MiB")
def hog(mib: int) -> int:
    data = b"x" * (mib * 2**20)
    time.sleep(1)
    return len(data)

f = hog.remote(300)
err = f.exception()
assert isinstance(err, ch.OutOfMemory)
assert "peak" in str(err) and "limit 104857600 bytes" in str(err)
assert [a["end_reason"] for a in f.explain()["attempts"]] == ["oom"] * 3
```

The peer checks each call's memory every 100 ms and stops the worker of one
that goes over `memory_limit`. It then runs the call again up to twice more.
Each retry reserves 1.5 times the peak the killed attempt reached, so it
waits until that much memory is free, but the limit stays where you set it.
A call that really needs more than its limit therefore fails after three
tries with `ch.OutOfMemory`, whose message gives the peak and the limit in
bytes. If the retry's reservation is bigger than the peer's whole memory
pool, it fails at once. Without a `memory_limit`, the peer stops a call only
when the host is nearly out of memory: it picks the call furthest over its
reservation, and the same retry rules apply. See
[resources.md](resources.md) for reservations and learned memory.

## Fail fast on impossible requests

A request that no peer can ever satisfy fails at once with `ch.Infeasible`,
instead of waiting forever. The message names the constraint:

```python run
err = parse.options(cpus=10_000).remote("1").exception(timeout=10)
assert isinstance(err, ch.Infeasible) and "cpus=10000" in str(err)
```

A request that is only too big for now (all CPUs busy, memory in use) waits.
`future.explain()["pending_reason"]` says what it's waiting for
([observability.md](observability.md)).

## Put a deadline on a call

```python run
import datetime

@ch.remote
def nap(seconds: float) -> float:
    time.sleep(seconds)
    return seconds

f = nap.options(deadline="0.5s").remote(30)
try:
    f.result()
except ch.DeadlineExceeded:
    pass
assert f.cancelled()                         # stopped at 0.5 s, not after 30

soon = datetime.datetime.now() + datetime.timedelta(seconds=0.5)
assert isinstance(nap.options(deadline=soon).remote(30).exception(), ch.DeadlineExceeded)
```

A deadline is relative (`"500ms"`, `"30s"`, a number of seconds, a
`timedelta`) or a `datetime`, and it counts from `.remote()`, so time spent
waiting for a worker counts too. When it passes, the peer cancels the call,
queued or running, and it stops the way any canceled call does (next
section). Calls made inside it inherit it, and when a call has its own
deadline as well, the earlier one wins. Inside a call,
`ch.context().deadline_unix_ms` gives the deadline in force (0 for none).

`DeadlineExceeded` is a `ch.Cancelled`. It's different from a wait timeout:
`f.result(timeout=5)` and `ch.get(futures, timeout=5)` raise `TimeoutError`
and leave the call running. Cancel it if you no longer want it.

## Cancel work

`future.cancel()` returns at once: `True` if the call hadn't finished (its
result now raises `ch.Cancelled`), `False` if it had (it keeps its outcome).
It also cancels every call the canceled one made, and their calls in turn.
Inside a call, `ch.context().cancelled()` turns true so the code can stop
early:

```python run
@ch.remote
def trial(i: int) -> int:
    while not ch.context().cancelled():      # stop early when asked
        time.sleep(0.05)
    return i

@ch.remote
def search(n: int) -> int:
    return sum(ch.get([trial.remote(i) for i in range(n)]))

run = ch.runs.current_run()
f = search.remote(3)
while sum(state != "Done" for _, state in run.tasks()) < 4:
    time.sleep(0.05)                         # the search and its 3 trials
assert f.cancel()
assert isinstance(f.exception(), ch.Cancelled)
task_id = f.id[32:]                          # f.id is the run id, then the task id
trials = [e for _, _, e in run.outcomes() if task_id in str(e)]
assert len(trials) == 3 and all(isinstance(e, ch.Cancelled) for e in trials)
```

Each trial's error says "the call that submitted it (...) was cancelled",
naming its parent. A running call is stopped in steps:

1. Right away, `ch.context().cancelled()` turns true inside the call. What
   the call returns after that is dropped: its outcome is already
   `ch.Cancelled`.
2. After 2 s, if the call runs on its worker's main thread, Chimaera
   interrupts it: a `time.sleep` or a pure-Python loop raises `ch.Cancelled`,
   and the worker stays for the next call.
3. After 5 s, a call still running (a C extension that never returns to
   Python, a thread) has its worker process killed.

`cancel(force=True)` skips to step 3 and kills the worker process at once.
An actor method only gets step 1, since the actor's state lives in that
process; `force=True` kills the actor too ([tasks.md](tasks.md),
[actors.md](actors.md)). `run.cancel()` cancels every unfinished call of a
run, and `chimaera cancel RUN` does it from a shell.

## When a worker dies

A worker that dies while running a function call (a segfault, `os._exit`, a
kill) costs that attempt, and the call runs again on a fresh worker:

```python run
@ch.remote
def fragile(x: int) -> int:
    if ch.context().attempt == 1:
        os._exit(1)                          # the worker process dies
    return x * 2

f = fragile.remote(21)
assert f.result() == 42
first, second = f.explain()["attempts"]
assert first["end_reason"] == "killed" and second["end_reason"] == "ok"
assert first["detail"] == "executor process exited (exit status: 1)"

@ch.remote
def doomed() -> None:
    os._exit(1)

err = doomed.remote().exception()
assert isinstance(err, ch.InfrastructureFailure)
assert str(err).startswith("3 attempts lost to infrastructure failures")
```

`future.explain()["attempts"]` lists every attempt with its `end_reason`
(`"ok"`, `"killed"`, `"oom"`...) and `detail`, and `chimaera explain RUN
--task TASK` prints the same from a shell. An actor whose process dies is
different: the call it was running raises `ch.ActorDied` and is not replayed,
and its `restart` policy decides what happens next ([actors.md](actors.md)).

## When a peer dies

On a cluster, a peer can crash or lose its machine. Runs, calls and their
outcomes are kept in groups replicated with Raft, so the record survives
the peer. What was running there is lost:

- Its calls' attempts count as infrastructure failures. They run again on
  another peer within `max_attempts`, except `"external"` calls that had
  started, which fail with `ch.OutcomeUnknown`.
- Actors it hosted are lost, and their `restart` policy applies.
- Values live on the peer whose call returned them. On Linux, each one is
  copied to the next peer about a second after it's written
  (`CHIMAERA_OBJECT_REPLICAS`, default 1; 0 turns copies off), and a reader
  finds the copy ([../objects.md](../objects.md)).

When every copy of a value is gone, reading it (in your script or as another
call's input) runs the call that returned it again, with the same inputs.
Its own lost inputs are remade the same way. That works only for a function
call or map item whose effects are not `"external"`, while its run's record
still exists. Nothing else is remade: values from `ch.put`, arguments you
passed, actor method results and external calls' results are lost with their
last copy, and reading one raises `ch.errors.DataLost` with the reason.

A single machine keeps its record on disk. If the local peer is killed in
the middle of a detached run and started again, it resumes the run, and
`ch.runs.attach(run_id)` sees every result exactly once.

## Keep a run going without your script

A script's run ends with the script. At a normal exit Chimaera waits for
calls you never waited on. If the script raised or you pressed Ctrl-C, it
cancels them, and if the connection just drops (the process was killed), the
peer cancels the run ([tasks.md](tasks.md)). A detached run keeps going after
its script exits, and any process can reattach to it by id:

```python run
import subprocess
import sys
import textwrap

script = textwrap.dedent("""
    import chimaera as ch

    @ch.remote
    def square(x: int) -> int:
        return x * x

    run = ch.init(detach=True)        # before the first call
    for i in range(5):
        square.remote(i)
    print(run)                        # a Run prints as its id
""")
run_id = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True).stdout.strip()

run = ch.runs.attach(run_id)
assert run.wait(timeout=60) == {"Done": 5}
assert sorted(value for _, value, error in run.outcomes()) == [0, 1, 4, 9, 16]
run.finish()
```

`run.wait(timeout=None)` blocks until every call of the run is done and
returns the counts by state. `run.outcomes()` yields `(task_id, value,
error)` for every finished call, with the error as the exception it would
raise. A detached run stays open until `run.finish()`, `run.cancel()` or
`chimaera cancel RUN`. Its records are kept for 24 hours after it ends. From
a shell, `chimaera run --detach` does the same for a script that doesn't call
`ch.init(detach=True)` itself:

```bash
$ chimaera run --detach -- python nightly.py
submitted run 0000035051d3f711106dbf78a87d936e
$ chimaera runs
RUN                               STATE       AGE  COMMAND
0000035051d3f711106dbf78a87d936e  active       0s  nightly.py  (detached)
$ chimaera cancel 0000035051d3f711106dbf78a87d936e
run 0000035051d3f711106dbf78a87d936e cancelled
```

(`nightly.py` prints its own run id with `ch.runs.current_run()`.) See
[../cli.md](../cli.md) for `chimaera logs` and `chimaera inspect`.

## Make a workflow durable

A long chain of steps shouldn't start over when the worker driving it dies.
Mark it `workflow="durable"` (it must be an `async def`):

```python run
STEPS = os.path.join(tempfile.mkdtemp(), "steps")

@ch.remote
def step(name: str) -> str:
    with open(STEPS, "a") as f:
        f.write(name + "\n")
    return name.upper()

@ch.remote(workflow="durable")
async def pipeline() -> tuple[str, str, float]:
    raw = await step.remote("extract")
    started = await ch.durable.now()
    if ch.context().attempt == 1:
        os._exit(1)                          # the worker dies once, mid-workflow
    loaded = await step.remote("load")
    return raw, loaded, started

raw, loaded, started = pipeline.remote().result()
assert (raw, loaded) == ("EXTRACT", "LOAD")
assert open(STEPS).read().split() == ["extract", "load"]   # extract ran once
```

When the workflow's worker or peer is lost, the workflow runs again from the
top, and every call it makes is answered from the run's record if that call
already ran: finished steps return their recorded result, and steps still
running are joined, not started twice. So the workflow must make the same
calls, in the same order, with the same arguments, every time. Anything that
varies goes through `ch.durable`, which runs it once and records the answer:

| Helper | Returns on every replay |
|---|---|
| `await ch.durable.now()` | the `time.time()` of the first run of this step |
| `await ch.durable.random()` | the same number in [0, 1) |
| `await ch.durable.record(fn, *args, **kwargs)` | `fn`'s first result; pass a module-level function |
| `await ch.durable.sleep(seconds)` | once the first run's sleep is over (a replay waits only for what is left) |

A replay that asks for something different at a step raises
`ch.errors.WorkflowDivergence` from that step:

```python run
@ch.remote(workflow="durable")
async def careless() -> str:
    await step.remote(f"attempt {ch.context().attempt}")   # differs on replay
    if ch.context().attempt == 1:
        os._exit(1)
    return "done"

assert isinstance(careless.remote().exception(), ch.errors.WorkflowDivergence)
```

A workflow is retried for lost workers up to 100 times by default; your own
exceptions still fail it unless listed in `retry_exceptions`. The helpers
raise `RuntimeError` outside a durable workflow. For an actor whose state
survives crashes, see [actors.md](actors.md).

Next: [observability.md](observability.md) shows how to find out what
happened to a call after the fact.
