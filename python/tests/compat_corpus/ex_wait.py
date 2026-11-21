"""ray.wait: process results as they finish."""
import time

import ray

ray.init()


@ray.remote
def sleepy(i):
    time.sleep(i * 0.3)
    return i


refs = [sleepy.remote(i) for i in range(5)]
ready, not_ready = ray.wait(refs, num_returns=2)
print(len(ready), len(not_ready), ray.get(ready))
done = []
while refs:
    ready, refs = ray.wait(refs)
    done.extend(ray.get(ready))
print(sorted(done))
ready, not_ready = ray.wait([sleepy.remote(10)], timeout=0.1)
print(ready, len(not_ready))
