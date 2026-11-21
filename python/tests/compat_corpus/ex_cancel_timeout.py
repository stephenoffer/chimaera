"""Timeouts leave the task running; cancel ends it."""
import time

import ray

ray.init()


@ray.remote
def slow():
    time.sleep(30)
    return 1


r = slow.remote()
try:
    ray.get(r, timeout=0.5)
except ray.exceptions.GetTimeoutError as e:
    print("timeout", isinstance(e, TimeoutError))
ray.cancel(r, force=True)
try:
    ray.get(r, timeout=30)
except ray.exceptions.TaskCancelledError:
    print("cancelled")
