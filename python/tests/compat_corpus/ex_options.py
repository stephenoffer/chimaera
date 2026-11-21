"""Resource options, per-call overrides, and the runtime context."""
import ray

ray.init()


@ray.remote(num_cpus=2, memory=100 * 1024 * 1024)
def heavy(x):
    return x + 1


@ray.remote(num_cpus=0)
def light():
    return "ok"


@ray.remote(retry_exceptions=[KeyError], max_retries=2)
def once():
    return "fine"


print(ray.get(heavy.options(num_cpus=1).remote(1)))
print(ray.get([light.remote() for _ in range(3)]))
print(ray.get(once.remote()))
print(bool(ray.get_runtime_context().get_job_id()))
