"""The README's status table names real things: every ``ch.`` name in a row
that says it works resolves, and a "Not yet" row does not claim one."""

from __future__ import annotations

import os
import re

import chimaera as ch
from conftest import REPO


def _rows() -> list[tuple[str, str]]:
    with open(os.path.join(REPO, "README.md")) as f:
        text = f.read()
    table = text.split("| Area | State |", 1)[1].split("\n\n", 1)[0]
    rows = []
    for line in table.splitlines()[2:]:
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 2:
            rows.append((cells[0], cells[1]))
    return rows


def _resolves(dotted: str) -> bool:
    obj: object = ch
    for part in dotted.split(".")[1:]:
        if not hasattr(obj, part):
            return False
        obj = getattr(obj, part)
    return True


def _names(cell: str) -> list[str]:
    return [m.group(1) for m in re.finditer(r"`(ch(?:\.\w+)+)", cell)]


def test_every_working_row_names_only_what_exists():
    rows = _rows()
    assert len(rows) >= 10, rows
    for area, state in rows:
        if state.startswith("Not yet"):
            continue
        missing = [n for n in _names(area) if not _resolves(n)]
        assert not missing, f"README says {area!r} works, but {missing} do not exist"


def test_not_yet_rows_claim_nothing_that_exists():
    for area, state in _rows():
        if state.startswith("Not yet"):
            present = [n for n in _names(area) if _resolves(n)]
            assert not present, f"{area!r} is 'Not yet' but {present} exist: update the README"
