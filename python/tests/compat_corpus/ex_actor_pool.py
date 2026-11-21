"""ray.util.ActorPool: ordered, unordered, and manual submission."""
import ray
from ray.util import ActorPool

ray.init()


@ray.remote
class Doubler:
    def double(self, x):
        return 2 * x


pool = ActorPool([Doubler.remote() for _ in range(3)])
print(list(pool.map(lambda a, v: a.double.remote(v), range(8))))
print(sorted(pool.map_unordered(lambda a, v: a.double.remote(v), range(8))))
pool.submit(lambda a, v: a.double.remote(v), 21)
print(pool.get_next(), pool.has_next())
