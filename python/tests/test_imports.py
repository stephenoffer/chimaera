"""The base package needs only cloudpickle: importing it loads no ML or data
library, and a script runs where none is installed."""

from conftest import run_script

# Libraries an optional integration may use; the core must never need one.
HEAVY = ("torch", "numpy", "pyarrow", "pandas", "jax", "joblib", "sklearn", "tensorflow")
PUBLIC = "chimaera, chimaera.coord, chimaera.dist, chimaera.data, chimaera.runs, chimaera.durable, chimaera.errors"


def test_importing_chimaera_loads_no_ml_or_data_library():
    out = run_script(
        f"""
import sys
import {PUBLIC}, chimaera.typing, chimaera.ext
print("LOADED", sorted({{m.split(".")[0] for m in sys.modules}} & set({HEAVY!r})))
"""
    )
    assert out.returncode == 0, out.stderr[-3000:]
    assert "LOADED []" in out.stdout, out.stdout


def test_a_script_runs_where_no_ml_or_data_library_is_installed():
    out = run_script(
        f"""
import importlib.abc, sys

class Absent(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in {HEAVY!r}:
            raise ModuleNotFoundError(f"No module named {{name!r}}", name=name)
        return None

sys.meta_path.insert(0, Absent())
import {PUBLIC}
import chimaera as ch

@ch.remote
def square(x):
    return x * x

print("RESULTS", ch.get([square.remote(i) for i in range(4)]))
"""
    )
    assert out.returncode == 0, out.stderr[-3000:]
    assert "RESULTS [0, 1, 4, 9]" in out.stdout, out.stdout
