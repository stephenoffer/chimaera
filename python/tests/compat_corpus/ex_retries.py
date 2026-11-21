"""A worker that dies is retried; with max_retries=0 the crash is reported."""
import os
import tempfile

import ray

ray.init()
marker = os.path.join(tempfile.mkdtemp(), "crashed")


@ray.remote(max_retries=2)
def flaky(path):
    if not os.path.exists(path):
        open(path, "w").close()
        os._exit(1)
    return "recovered"


@ray.remote(max_retries=0)
def always_dies():
    os._exit(1)


print(ray.get(flaky.remote(marker)))
try:
    ray.get(always_dies.remote())
except ray.exceptions.WorkerCrashedError:
    print("worker crashed")
