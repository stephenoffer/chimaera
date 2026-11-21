import sys
sys.path.insert(0, "python/tests")
from test_cluster import Cluster, WHERE, lines
c = Cluster(3)
try:
    out = c.run(0, WHERE + """
import numpy as np
total = ch.remote(lambda a: float(a[:10].sum()))
ok = 0
for rep in range(25):
    ref = ch.put(np.ones(64 * 2**20 // 8))
    try:
        ch.gather(*[total.remote(ref) for _ in range(24)], timeout=120)
        ok += 1
    except Exception as e:
        print("FAIL rep", rep, type(e).__name__, str(e)[:200])
        break
print("OK", ok)
""", timeout=600)
    print(lines(out, "OK"), [l for l in out.stdout.splitlines() if l.startswith("FAIL")], out.stderr[-400:] if out.returncode else "")
finally:
    c.close()
