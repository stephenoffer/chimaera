"""ray.put: a large array stored once, passed by reference at top level and nested."""
import numpy as np
import ray

ray.init()
arr = np.arange(1_000_000, dtype=np.int64)
ref = ray.put(arr)


@ray.remote
def total(a):
    return int(a.sum())


@ray.remote
def total_nested(refs):
    return sum(int(ray.get(r).sum()) for r in refs)


print(ray.get(total.remote(ref)))
print(ray.get(total_nested.remote([ref, ref])))
print(isinstance(ref, ray.ObjectRef), int(ray.get(ref)[-1]))
