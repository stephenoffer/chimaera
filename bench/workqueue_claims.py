"""WorkQueue claims under many consumers (a local measurement, not a cluster claim).

Puts the items, closes the queue, then consumer threads claim and finish
every one. Reports puts and claims per second, and how many keys each claim
looked at in the shard: the claim index keeps that at two or fewer, however
long the queue and however many consumers.

    python bench/workqueue_claims.py --consumers 100 --items 100000
"""

from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
import uuid

import chimaera as ch


def main() -> None:
    p = argparse.ArgumentParser(description="WorkQueue claims under many consumers")
    p.add_argument("--consumers", type=int, default=100)
    p.add_argument("--items", type=int, default=100_000)
    args = p.parse_args()
    q = ch.coord.WorkQueue(f"bench-{uuid.uuid4().hex[:8]}")
    t = time.perf_counter()
    for i in range(args.items):
        q.put(i)
    put_s = time.perf_counter() - t
    q.close()
    examined: list[int] = []

    def consume() -> None:
        while True:
            c = q.claim()
            try:
                with c:
                    examined.append(c._examined)
            except ch.errors.Closed:
                return

    threads = [threading.Thread(target=consume) for _ in range(args.consumers)]
    t = time.perf_counter()
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    claim_s = time.perf_counter() - t
    assert len(examined) == args.items and q.qsize() == 0
    examined.sort()
    print(
        json.dumps(
            {
                "consumers": args.consumers,
                "items": args.items,
                "puts_per_s": round(args.items / put_s),
                "claims_per_s": round(args.items / claim_s),
                "keys_examined_per_claim": {
                    "mean": round(statistics.mean(examined), 3),
                    "p99": examined[int(0.99 * (len(examined) - 1))],
                    "max": examined[-1],
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
