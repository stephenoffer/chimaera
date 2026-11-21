"""Task errors: catchable as RayTaskError and as the original type, through dependencies."""
import ray

ray.init()


@ray.remote
def fail(x):
    raise ValueError(f"bad input {x}")


@ray.remote
def use(x):
    return x + 1


try:
    ray.get(fail.remote(1))
except ray.exceptions.RayTaskError as e:
    print("RayTaskError", isinstance(e, ValueError), type(e).__name__)
try:
    ray.get(fail.remote(2))
except ValueError as e:
    print("ValueError", "bad input 2" in str(e))
try:
    ray.get(use.remote(fail.remote(3)))
except ray.exceptions.RayTaskError as e:
    print("upstream", isinstance(e, ValueError), "bad input 3" in str(e))
