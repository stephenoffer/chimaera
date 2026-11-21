"""max_concurrency: two calls meet at a barrier inside one actor."""
import threading

import ray

ray.init()


@ray.remote(max_concurrency=2)
class Rendezvous:
    def __init__(self):
        self.barrier = threading.Barrier(2, timeout=20)

    def meet(self, i):
        self.barrier.wait()
        return i


r = Rendezvous.remote()
print(ray.get([r.meet.remote(1), r.meet.remote(2)]))
