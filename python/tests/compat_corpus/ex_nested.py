"""Tasks that start tasks and wait for them."""
import ray

ray.init()


@ray.remote
def leaf(i):
    return i * i


@ray.remote
def tree(n):
    return sum(ray.get([leaf.remote(i) for i in range(n)]))


print(ray.get([tree.remote(n) for n in (3, 5, 10)]))
