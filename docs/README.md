# Chimaera docs

New here? Start with [Getting started](getting-started.md). It takes about ten
minutes, and every example in it runs on a laptop.

## Guides

Each guide is about one job, with examples you can paste and run.

| Guide | Read it when you want to |
|---|---|
| [Tasks and futures](guides/tasks.md) | run functions in parallel, chain them, map over data, stream results |
| [Actors](guides/actors.md) | keep state in a worker, share it by name, run pools of replicas, survive crashes |
| [Failures](guides/failures.md) | know what gets retried, handle side effects safely, cancel, reattach to a run |
| [Resources and GPUs](guides/resources.md) | ask for CPUs, memory and accelerators, or see what the runtime picked for you |
| [Data](guides/data.md) | pass large values cheaply, build pipelines and shuffles, share queues and state |
| [Gangs](guides/gangs.md) | run distributed training: ranks, collectives, elastic size, PyTorch |
| [Run on a cluster](guides/clusters.md) | go from a laptop to many machines, securely |
| [Observability](guides/observability.md) | find out what your program is doing and why something waits |
| [Integrations](guides/integrations.md) | use Chimaera from `concurrent.futures`, joblib or asyncio |
| [Coming from Ray](guides/from-ray.md) | move a Ray program over, or run it unchanged |

## Reference

- [API](api.md): every public name and what it promises
- [CLI](cli.md): every `chimaera` command and flag
- [Configuration](configuration.md): every `CHIMAERA_` environment variable
- [Deploying](deploy.md): Kubernetes, Slurm, SSH, clouds and accelerators
- [Ray compatibility](compat-ray.md): how each Ray Core API maps

## How it works

- [Design](design.md): why the API looks the way it does, and how calls and data flow
- [Architecture](architecture.md): how Chimaera grows to many machines, and why
- [Objects](objects.md): how values move, spill and survive a lost machine
- [Federation](federation.md): cells that lend each other capacity
- [Benchmarks](benchmarks.md): Chimaera against Ray on clusters of up to 64 machines, and on Ray's own tests
- [Qualification](qualification.md): the measurements, how they compare with Ray, and what isn't tested yet
- [Parity](parity.md) and [the other runtimes](landscape.md): Ray Core and the rest, item by item
- [Lessons from Ray](ray-lessons.md), [Ray gotchas](ray-gotchas.md) and [requirements](requirements.md): what Chimaera does about the problems Ray users hit
