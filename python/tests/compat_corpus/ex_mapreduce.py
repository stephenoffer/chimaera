"""Word count: put the chunks, map, then reduce."""
import collections

import ray

ray.init()
text = ["the quick brown fox", "jumps over the lazy dog", "the dog barks"] * 4
chunks = [ray.put(text[i::3]) for i in range(3)]


@ray.remote
def map_count(lines):
    return collections.Counter(w for line in lines for w in line.split())


@ray.remote
def reduce_counts(*counters):
    total = collections.Counter()
    for c in counters:
        total.update(c)
    return total


total = ray.get(reduce_counts.remote(*[map_count.remote(c) for c in chunks]))
print(total["the"], total["dog"], len(total))
