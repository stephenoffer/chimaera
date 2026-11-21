"""Ray Core features that map onto Chimaera's Tier 1 API: num_returns,
method options, concurrency groups, get_actor and ray.util.queue."""
import threading

import ray
from ray.util.queue import Queue

ray.init()


@ray.remote(num_returns=2)
def bounds(xs):
    return min(xs), max(xs)


@ray.remote
def width(lo, hi):
    return hi - lo


lo, hi = bounds.remote([4, 1, 9])
print(ray.get([lo, hi]), ray.get(width.remote(lo, hi)))


@ray.remote(concurrency_groups={"io": 2})
class Service:
    def __init__(self):
        self.gate = threading.Event()

    @ray.method(concurrency_group="io")
    def release(self):
        self.gate.set()
        return "released"

    def wait(self):
        return self.gate.wait(timeout=20)

    @ray.method(num_returns=2)
    def pair(self):
        return "a", "b"


svc = Service.options(name="svc", get_if_exists=True).remote()
blocked = svc.wait.remote()           # occupies the default group
print(ray.get(svc.release.remote()))  # runs in "io" while wait() is blocked
print(ray.get(blocked))
a, b = svc.pair.remote()
print(ray.get(a), ray.get(b))
c, d = svc.pair.options(num_returns=2).remote()
print(ray.get([c, d]))
print(ray.get(ray.get_actor("svc").pair.remote()[1]))

q = Queue(maxsize=2)
q.put(1)
q.put(2)
print(q.full(), q.qsize())
try:
    q.put(3, block=False)
except Exception as e:
    print("full:", type(e).__name__)
print(q.get(), q.get_nowait_batch(1), q.empty())
