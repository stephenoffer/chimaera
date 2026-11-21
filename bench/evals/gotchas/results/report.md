# Gotchas: naive code on Ray and Chimaera

{'host': 'Linux aarch64', 'cores': 4, 'container': True, 'memory_limit': '3221225472', 'reps': 1}

| Trap | Runtime | Wall s (median) | Peak extra MiB | OOM kills | OK | Errors |
|---|---|---|---|---|---|---|
| mem_heavy | ray | 10.823 | 3024 | 43 | 3/12 | [{"WorkerCrashedError": 9}] |
| mem_heavy | chimaera | 12.887 | 1102 | 0 | 12/12 | [] |
| leak | ray | 1.124 | 3013 | 3 | 230/230 | [] |
| leak | chimaera | 0.332 | 1594 | 0 | 230/230 | [] |
| size_scaling | ray | None | 3035 | 0 | None/None | ["thon3.11/site-packages/ray/util/tracing/tracing_helper.py\", line 329, in _invocation_remote_span\n    return method(self, args, kwargs, *_args, **_kwargs)\n  |
| size_scaling | chimaera | 8.779 | 2018 | 0 | 120/120 | [] |
| runaway | ray | None | 2881 | 0 | None/None | ["timed out after 180 s"] |
| runaway | chimaera | 4.59 | 2605 | 0 | 0/1 | [{"Infeasible": 1}] |
| mem_heavy_again | ray | 13.087 | 2999 | 44 | 3/12 | [{"WorkerCrashedError": 9}] |
| mem_heavy_again | chimaera | 12.741 | 1098 | 0 | 12/12 | [] |
