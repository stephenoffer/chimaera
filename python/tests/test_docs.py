"""Examples in the docs that claim to run today do run.

A block fenced as ```python run is executed; all such blocks in one document
run in order, in one fresh interpreter, against the session's peer. Their
asserts are the checks. Plain ```python blocks show planned API and are only
compiled, so a typo in them is still caught.
"""

from __future__ import annotations

import pathlib
import re

import pytest

from conftest import REPO, run_script

DOCS = pathlib.Path(REPO, "docs")
# The API docs, the getting-started page and the user guides; design docs aren't policed here.
OWN = [
    DOCS / name for name in ("api.md", "deploy.md", "compat-ray.md", "getting-started.md") if (DOCS / name).exists()
] + sorted((DOCS / "guides").glob("*.md"))
FENCE = re.compile(r"^```python( run)?\n(.*?)^```", re.M | re.S)


def blocks(doc: pathlib.Path) -> list[tuple[bool, str]]:
    return [(bool(m.group(1)), m.group(2)) for m in FENCE.finditer(doc.read_text())]


DOCS_WITH_RUNNABLE = [d for d in OWN if any(run for run, _ in blocks(d))]


@pytest.mark.parametrize("doc", DOCS_WITH_RUNNABLE, ids=lambda d: d.name)
def test_runnable_examples_run(doc):
    code = "\n\n".join(src for run, src in blocks(doc) if run)
    proc = run_script(code, timeout=300)
    assert proc.returncode == 0, f"{doc.name}:\n{proc.stderr[-3000:]}"


@pytest.mark.parametrize("doc", OWN, ids=lambda d: d.name)
def test_every_python_example_compiles(doc):
    for i, (_, src) in enumerate(blocks(doc)):
        try:
            compile(src, f"{doc.name} block {i}", "exec", flags=0x2000)  # PyCF_ALLOW_TOP_LEVEL_AWAIT
        except SyntaxError as e:
            pytest.fail(f"{doc.name} block {i}: {e}")
