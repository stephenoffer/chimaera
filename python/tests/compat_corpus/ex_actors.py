"""Actors: state, named get-or-create, kill."""
import time

import ray

ray.init()


@ray.remote
class Counter:
    def __init__(self, start=0):
        self.n = start

    def incr(self, k=1):
        self.n += k
        return self.n


c = Counter.remote(10)
print(ray.get([c.incr.remote() for _ in range(5)]))
a = Counter.options(name="shared", get_if_exists=True).remote()
b = Counter.options(name="shared", get_if_exists=True).remote()
ray.get(a.incr.remote(5))
print(ray.get(b.incr.remote(1)))
ray.kill(c)
for _ in range(100):  # ray.kill is asynchronous in Ray: a call may still race it
    try:
        ray.get(c.incr.remote())
        time.sleep(0.1)
    except ray.exceptions.RayActorError as e:
        print("actor died:", type(e).__name__)
        break
