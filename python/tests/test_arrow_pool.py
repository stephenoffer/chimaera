"""Arrow's large buffers in the shared-memory arena (_arrow_pool): a put of
Arrow data Arrow allocated itself links its files instead of copying them."""

from __future__ import annotations

import sys

import pytest
from conftest import run_script

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="the arena is Linux's (tmpfs, MAP_FIXED remaps)")


def test_a_column_arrow_allocated_is_stored_by_link_not_copied():
    pytest.importorskip("pyarrow")
    r = run_script(
        """
        import pyarrow as pa
        import pyarrow.compute as pc
        import chimaera as ch
        from chimaera import _arrow_pool, _native
        from chimaera._runtime import Runtime

        Runtime.get()  # the arena is installed with the runtime
        assert _arrow_pool.install(), "the pool's library builds here"
        print("POOL", pa.default_memory_pool().backend_name)
        col = pc.add(pa.array(range(1 << 18), pa.int64()), 1)  # 2 MiB, allocated by Arrow
        small = pc.add(pa.array(range(1000), pa.int64()), 1)  # under the arena's minimum
        table = pa.table({"x": col})
        claims = _native.arena_stats()[3]  # (live, bytes, cached, claims, fallbacks)
        f = ch.put(table)
        print("LINKED", _native.arena_stats()[3] > claims)
        print("SAME", ch.get(f).equals(table), small.sum().as_py() == sum(range(1, 1001)))
        grown = pa.concat_arrays([col, col])  # built in one buffer of 4 MiB
        print("GROWN", len(grown) == 2 * len(col), grown[-1].as_py() == 1 << 18)
        """,
        timeout=600,
    )
    assert r.returncode == 0, r.stderr
    out = dict(line.split(" ", 1) for line in r.stdout.splitlines() if " " in line)
    assert out["POOL"].startswith("chimaera-arena+"), out
    assert out["LINKED"] == "True", "the put linked the column's file"
    assert out["SAME"] == "True True" and out["GROWN"] == "True True", out
