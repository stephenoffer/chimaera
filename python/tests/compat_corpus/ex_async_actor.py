"""An async actor: one call waits on an event another call sets."""
import asyncio

import ray

ray.init()


@ray.remote
class Signal:
    def __init__(self):
        self.ev = asyncio.Event()

    async def send(self):
        self.ev.set()

    async def wait(self):
        await self.ev.wait()
        return "released"


s = Signal.remote()
w = s.wait.remote()
ready, _ = ray.wait([w], timeout=0.5)
print(len(ready))
ray.get(s.send.remote())
print(ray.get(w))
