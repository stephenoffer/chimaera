# When things go wrong: errors, retries and recovery

Worker crashed, or a whole peer went down? Chimaera runs the lost calls again.
It won't retry your own exceptions unless you list them, and it never reruns an
`effects="external"` call that may already have started. Detached runs outlive
the script that started them, and durable workflows don't repeat finished steps
when their worker dies.

## Catch your own exceptions

Your function raised? `result()` gives you that exception back, same type, with
the remote traceback attached as a note:

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

<details>
<summary>Failed inputs and custom exceptions</summary>

A call that depends on a failed one never runs. It raises the root cause's type
instead, with a note naming the failed call:

```python run
@ch.remote
def add(a: int, b: int) -> int:
    return a + b

err = add.remote(parse.remote("x"), 1).exception()     # waits, returns the error
assert isinstance(err, ValueError) and "this call never ran" in err.__notes__[0]
```

Leave it uncaught and it prints your local stack, with the remote line that
raised it underneath.

If your script can't unpickle the original type, you get
`ch.errors.RemoteError` (`type_name`, `message`, `remote_traceback`) instead.
The usual culprit is an `__init__` that takes different arguments from the ones
it passes to `super().__init__()`. Python rebuilds an exception as
`cls(*e.args)`, so when you write your own exception class, pass every
`__init__` argument to `super().__init__()`. Then it survives the trip back:

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

- `SystemExit`, `KeyboardInterrupt` and `GeneratorExit` raised remotely come
  back as `RemoteError`, so they can't end your script.
- An error nothing reads isn't dropped: it's reported on stderr
  (`chimaera: ... failed and nothing read its error`).
- In a map, `results.outcomes()` yields `(index, value, error)` per item
  instead of raising at the first failure ([tasks.md](tasks.md)).

</details>

## Know which error you got

Every error Chimaera raises itself derives from `ch.ChimaeraError`, except one:
a wait's built-in `TimeoutError`.

| Error | Raised when | What it carries |
|---|---|---|
| your own type | your code raised it | the remote traceback as a note |
| `ch.errors.RemoteError` | your exception can't be rebuilt here | `type_name`, `message`, `remote_traceback` |
| `ch.InfrastructureFailure` | workers or peers were lost on every attempt (`max_attempts`) | message: how many attempts, and the last cause |
| `ch.OutOfMemory` | killed for memory on its first try and both retries, or the retry can't fit the peer | message: peak and limit in bytes |
| `ch.ActorDied` | the actor's process died, or it was closed ([actors.md](actors.md)) | the reason |
| `ch.errors.DataLost` | a value is on no live peer and its call can't run again | why it can't be remade |
| `ch.OutcomeUnknown` | an `effects="external"` call was lost after it started | the attempt and the cause |
| `ch.Cancelled` | you, its caller, its run or your script's exit canceled it | who canceled it |
| `ch.DeadlineExceeded` | its deadline passed | |
| `ch.Infeasible` | no peer can ever run the request | the constraint |
| `TimeoutError` | your wait ran out; the call is still running | |

<details>
<summary>Base classes and other errors</summary>

The common ones are on `ch`, all of them in `ch.errors`. The rest are in the
table below.

Two base classes catch whole families. `except ch.InfrastructureFailure`
covers `OutOfMemory`, `ActorDied`, `DataLost`, `StreamInterrupted`,
`QuorumLost`, `GangFailed` and `WorkerCrashed`. `except ch.Cancelled` covers
`DeadlineExceeded` and `BudgetExceeded` (`future.cancelled()` is true for all
three).

| Error | Raised when | What it carries |
|---|---|---|
| `ch.errors.StreamInterrupted` | a generator call ended before all its items arrived | message: items received of the total |
| `ch.errors.QuorumLost` | a group of the cluster lost the majority of its peers ([clusters.md](clusters.md)) | the group |
| `ch.errors.GangFailed` | every generation of a gang lost a rank ([gangs.md](gangs.md)) | the reason |
| `ch.errors.WorkerCrashed` | defined, but the current peer reports repeated worker deaths as `InfrastructureFailure` | |
| `ch.errors.BudgetExceeded` | its call tree used up its `budget` ([resources.md](resources.md)) | the budget and what was used |
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

</details>

## Retry what is safe to retry

Calls lost with a worker or peer are retried for you. So are a dropped
connection and a timeout (`ConnectionError`, `TimeoutError`, and requests',
aiohttp's and urllib3's equivalents) in calls that may run again:
`effects="none"` or `"idempotent"`, not actor calls or streams. A map retries
each such item on its own. A wait in the call that runs out
(`.result(timeout=)`) is not retried, since a retry would start the calls it
waited on again. Your other exceptions? Only if you list them in
`retry_exceptions` (a class or a tuple; subclasses match), which replaces that
default; `retry_exceptions=()` turns it off. A retry waits 0.1 s, doubling to
at most 2 s, and retries stop early when an attempt fails exactly like the one
before:

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

<details>
<summary>How retries count</summary>

`max_attempts` (default 3) counts every attempt, including the first, and
worker losses share it with listed exceptions.

When an attempt fails exactly like the one before (type, message and traceback
lines), Chimaera stops retrying and raises it. Why keep going? A failure that
repeats word for word won't go away by itself:

```python run
@ch.remote(retry_exceptions=ConnectionError, max_attempts=5)
def query() -> None:
    raise ConnectionRefusedError("db is down")

f = query.remote()
assert isinstance(f.exception(), ConnectionRefusedError)
assert len(f.explain()["attempts"]) == 2         # not 5
```

Memory kills have their own allowance, apart from `max_attempts`
([Cap memory](#cap-memory)).

</details>

## Declare side effects

Got a function that must not run twice? Mark it `effects="external"`. If it's
lost after it has started, you get `ch.OutcomeUnknown` instead of a second run,
and an idempotency key makes retrying it yourself safe:

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

<details>
<summary>Effects table and idempotency keys</summary>

Retrying is safe only if running twice is harmless. `effects` tells
Chimaera how careful to be:

| `effects=` | Use it for | A lost attempt | `retry_exceptions` | A lost result |
|---|---|---|---|---|
| `"none"` (default) | pure functions | runs again, up to `max_attempts` | honored | is remade by running the call again |
| `"idempotent"` | writes that are safe to repeat | runs again, up to `max_attempts` | honored | is remade by running the call again |
| `"external"` | charges, emails, anything that must not repeat | runs again only if lost before it started; after that, `ch.OutcomeUnknown` | ignored: your exception is raised | is not remade (`DataLost`) |

Once an `"external"` call has started, losing it means nobody knows whether the
effect happened. So handle `OutcomeUnknown` with an idempotency key: an id you
choose (the order id in the example) that the external system uses to spot a
repeat, which makes trying again safe. Plenty of payment and messaging APIs
accept one. In the example, a directory stands in for a payment API that
charges once per key, and the worker dies right after the money moves.

Before your code runs, an `"external"` call's start goes into the run's
replicated record. If it's lost before that point, it runs again, up to 3
attempts in all. That allowance is separate from `max_attempts`, which an
external call doesn't use, and setting it yourself is an error:
`max_attempts=3` on an external call raises `ch.OptionsError` at `.remote()`.

- Only the decorator can weaken `effects`. `.options(effects=...)` and
  `with ch.defaults(effects=...)` can make it stricter; loosening the
  decorator's setting raises `ch.OptionsError`.
- In a fleet of clusters (cells), an `"idempotent"` call stays in its own cell; only `"none"` calls may go to
  another ([../federation.md](../federation.md)).
- Durable actors handle repeats with a `command_id` instead
  ([actors.md](actors.md)).

</details>

## Cap memory

Set `memory_limit`, and anything that goes over it gets stopped:

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

<details>
<summary>Memory checks and retry reservations</summary>

The peer checks memory every 100 ms, stops the worker of anything over its
limit, and runs it up to twice more. The limit stays where you set it, so a
function that really needs more fails after three attempts, like the one above.

Each retry reserves 1.5 times the killed attempt's peak and waits until that
much is free. If that's more than the peer's whole memory pool, the retry fails
at once.

No `memory_limit`? Then the peer steps in only when the host is nearly out of
memory, and it stops whatever is furthest over its reservation. Retries follow
the rules above. [resources.md](resources.md) covers reservations and learned
memory.

</details>

## Fail fast on impossible requests

A request no peer can ever satisfy fails right away with `ch.Infeasible`, so
you're not left waiting forever:

```python run
err = parse.options(cpus=10_000).remote("1").exception(timeout=10)
assert isinstance(err, ch.Infeasible) and "cpus=10000" in str(err)
```

<details>
<summary>Requests that wait instead</summary>

A request that's only too big for now (all CPUs busy, memory in use) just
waits, and `future.explain()["pending_reason"]` tells you what for
([observability.md](observability.md)).

</details>

## Put a deadline on a call

Once a call's deadline passes, it's canceled, queued or running:

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

<details>
<summary>Deadline formats and nested calls</summary>

A deadline is relative (`"500ms"`, `"30s"`, a number of seconds, a `timedelta`)
or a `datetime`, counted from `.remote()`, so waiting for a worker counts too.
Don't mix it up with a wait timeout: `f.result(timeout=5)` and
`ch.get(futures, timeout=5)` raise `TimeoutError` and leave the call running,
so cancel it if you don't want it anymore.

Calls past their deadline stop the way canceled ones do (next section). Nested
calls inherit the deadline, and if one sets its own, the earlier one wins.
Inside your function, `ch.context().deadline_unix_ms` is the deadline in force
(0 for none).

</details>

## Cancel work

Changed your mind? `future.cancel()` cancels a call and every call it started:

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

<details>
<summary>How a running call stops</summary>

`future.cancel()` returns at once: `True` if the call hadn't finished (its
result now raises `ch.Cancelled`), `False` if it had (it keeps its outcome). To
stop early, your code can watch `ch.context().cancelled()`.

`run.cancel()` cancels every unfinished call in the run, and
`chimaera cancel RUN` does the same from a shell.

Each trial's error names its parent:
`the call that submitted it (...) was cancelled`. Here's how a running call
stops:

1. Right away, `ch.context().cancelled()` turns true inside it, and anything
   it returns after that is dropped: its outcome is already `ch.Cancelled`.
2. After 2 s, it's interrupted if it runs on its worker's main thread: a
   `time.sleep` or a pure-Python loop raises `ch.Cancelled`, and that worker
   sticks around for the next call.
3. After 5 s, if it's still running (a C extension that never returns to
   Python, a thread), its worker process is killed.

`cancel(force=True)` skips straight to step 3. An actor method only gets step
1, because the actor's state lives in that process, and `force=True` kills the
actor too ([tasks.md](tasks.md), [actors.md](actors.md)).

</details>

## When a worker dies

If a worker dies mid-function (a segfault, `os._exit`, a kill), you lose that
attempt and the function is retried on a fresh worker:

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

<details>
<summary>Actor deaths and end reasons</summary>

Actors work differently. When an actor's process dies, the method call it was
running raises `ch.ActorDied` and isn't replayed. Its
[`restart` policy](actors.md) decides what happens next.

`future.explain()["attempts"]` lists each attempt's `end_reason` (`"ok"`,
`"killed"`, `"oom"`...) and `detail`. From a shell,
`chimaera explain RUN --task TASK` prints them.

</details>

## When a peer dies

Peers crash, and machines go away. On a cluster, Chimaera picks up after them:
calls run again elsewhere, actors follow their `restart` policy, and values are
read from a copy or remade.

<details>
<summary>What survives a lost peer</summary>

Runs, calls and their outcomes live in groups replicated with Raft, so losing a
peer doesn't take the record with it. What was running on it, though, is lost:

- Its calls' attempts count as infrastructure failures and run again on
  another peer within `max_attempts`, except `"external"` calls that had
  started, which fail with `ch.OutcomeUnknown`.
- Actors it hosted are lost; their `restart` policy applies.
- Values live on the peer whose call returned them. On Linux, each one is
  copied to the next peer about a second after it's written
  (`CHIMAERA_OBJECT_REPLICAS`, default 1; 0 turns copies off), and a reader
  finds the copy ([../objects.md](../objects.md)).

And when every copy of a value is gone? Reading it (in your script or as
another call's input) reruns the function that returned it on its original
inputs, and remakes any lost input the same way. That only works for a function
call or map item whose effects aren't `"external"`, and only while its run's
record still exists. Nothing else is remade: values from `ch.put`, arguments
you passed, actor method results and external calls' results die with their
last copy, and reading one raises `ch.errors.DataLost` with the reason.

A single machine keeps its record on disk. Kill a local peer during a detached
run and it resumes the run when started again; `ch.runs.attach(run_id)` sees
every result exactly once.

</details>

## Keep a run going without your script

Want a run to outlive its script? Detach it, and any process can reattach to it
by id:

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

<details>
<summary>When a detached run ends</summary>

A detached run stays open until `run.finish()`, `run.cancel()` or
`chimaera cancel RUN`, and its records stick around for 24 hours after it ends.
`chimaera run --detach` detaches a script that doesn't call
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

`run.wait(timeout=None)` blocks until every call is done and returns counts by
state. `run.outcomes()` yields `(task_id, value, error)` per finished call,
where the error is the exception it would raise.

A run that isn't detached ends with its script: a normal exit waits for
calls you never waited on, an exception or Ctrl-C cancels them, and if the
connection drops (the process was killed), the run is canceled once the peer
has waited five seconds for the script to reconnect ([tasks.md](tasks.md)).
The other way round, a script whose connection breaks while it runs picks
its session up again on a new one and loses no result. If its peer is gone,
the script gives up after about three seconds of tries.

Inside a script started this way, `ch.runs.current_run()` returns its run. See [../cli.md](../cli.md) for
`chimaera logs` and `chimaera inspect`.

</details>

## Make a workflow durable

Got a long chain of steps you'd rather not redo? Mark it `workflow="durable"`
(it must be an `async def`), and its finished steps won't run again when the
worker driving it dies:

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

<details>
<summary>Durable helpers and replay rules</summary>

When its worker or peer is lost, the workflow restarts from the top, and each
call it already made is answered from the run's record. That means it has to
repeat itself exactly: same calls, same order, same arguments, every time.
Anything that varies goes through `ch.durable`, which runs it once and records
the answer:

| Helper | Returns on every replay |
|---|---|
| `await ch.durable.now()` | the `time.time()` of the first run of this step |
| `await ch.durable.random()` | the same number in [0, 1) |
| `await ch.durable.record(fn, *args, **kwargs)` | `fn`'s first result; pass a module-level function that reads or computes (a lost attempt runs it again) |
| `await ch.durable.record(activity, *args, key=...)` | the receipt of a write to another system, done once (below) |
| `await ch.durable.sleep(seconds)` | once the first run's sleep is over (a replay waits only for what is left) |

Finished steps return their recorded result, and running ones are joined rather
than started twice. A replay that asks for something different at a step raises
`ch.errors.WorkflowDivergence` right there:

```python run
@ch.remote(workflow="durable")
async def careless() -> str:
    await step.remote(f"attempt {ch.context().attempt}")   # differs on replay
    if ch.context().attempt == 1:
        os._exit(1)
    return "done"

err = careless.remote().exception()
assert isinstance(err, ch.errors.WorkflowDivergence)
assert err.step == 1                                     # the workflow's first call
assert err.recorded["args"] != err.received["args"]      # what the history holds, and what the replay asked
```

The error names the step, and for the recorded call and the replay's: the
function, the workflow's `file:line` that made it, a short repr of its
arguments and its fingerprint (as `err.recorded` and `err.received`).

A write to another system (a charge, an email) goes through an activity:
subclass `ch.durable.Activity`, implement `run(key, *args)`, and say what is
safe to repeat. The key names the operation to that system: pass your own
(an order id), or leave it out and the run gives the step one that stays the
same on every replay.

```python run
LEDGER = tempfile.mkdtemp()

class Payments(ch.durable.Activity[str]):
    idempotent = True                        # a repeat under the same key changes nothing

    def run(self, key: str, cents: int) -> str:
        try:
            with open(os.path.join(LEDGER, key), "x") as f:
                f.write(str(cents))
        except FileExistsError:
            pass                             # charged already: same receipt
        return f"receipt-{key}"

@ch.remote(workflow="durable")
async def checkout(order_id: str) -> str:
    receipt = await ch.durable.record(Payments(), 500, key=order_id)
    if ch.context().attempt == 1:
        os._exit(1)                          # the worker dies after the charge
    return receipt

assert checkout.remote("order-18").result() == "receipt-order-18"
assert os.listdir(LEDGER) == ["order-18"]   # charged once
```

An attempt lost after `run` started may or may not have done its write.
What happens next is what the activity declares:

| The activity | A lost attempt |
|---|---|
| `idempotent = True` | runs again under the same key |
| defines `status(key)`, returning the receipt or None | is settled by asking; None runs it again |
| neither | raises `ch.OutcomeUnknown`, whose `key` is the activity's |

`compensate(key, receipt)` on the activity undoes an operation (a refund);
`await ch.durable.compensate(activity, key, receipt)` runs it once as a step.
A lost attempt of it runs again if the activity is idempotent, and otherwise
raises `ch.OutcomeUnknown` with the key: `status(key)` describes the
operation, not its undoing, so it is not asked.

A replay runs the workflow function's code from when the call was submitted.
That code travels with the call, and so do the module-level values it names,
as they were then: constants, dicts and the `@ch.remote` functions it calls.
Other functions and classes it names, from its own module or another, are
looked up by name where the replay runs, as in any call; a worker that
imported their module before a deployment keeps what it imported. Give a
definition a `version`, and a new one can declare which older histories it
continues: a replay after the deployment then runs the new code against the
recorded steps, which it must make in the same order. To find the version
deployed, a replay runs the module's source again on whichever worker it lands
(so code the module runs on import runs again). The run records that the
history moved, so later replays stay on that version's code even after another
deployment replaces it, until a newer version lists that one in its
`compatible`.

```python
@ch.remote(workflow="durable", version=2, compatible=1)   # takes over version 1's histories
async def checkout(order_id: str) -> str: ...
```

A definition without `version` is version 1. Functions are matched to the
recorded steps by name (`module.qualname`) and arguments, so a step whose code
changed is still the same step; a lambda or nested function is matched by its
code too. Upgrading Chimaera itself can change how steps are matched, so
finish or cancel durable workflows in flight first: a history recorded by
another release may diverge at its first step.

A durable workflow is retried for lost workers up to 100 times by default, but
your exceptions still fail it unless you list them in `retry_exceptions`.
Outside a durable workflow, the helpers raise `RuntimeError`. Want an actor
whose state survives crashes? See [durable actors](actors.md).

</details>

Next: [observability.md](observability.md) shows how to find out what
happened to a call after the fact.
