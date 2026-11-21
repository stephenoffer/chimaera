"""Tasks, and futures passed as arguments."""
import ray

ray.init()


@ray.remote
def square(x):
    return x * x


@ray.remote
def add(a, b):
    return a + b


print(ray.get([square.remote(i) for i in range(10)]))
print(ray.get(add.remote(square.remote(3), square.remote(4))))
ray.shutdown()
