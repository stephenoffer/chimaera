"""Draw the speedup charts in docs/assets from a small JSON spec:

    python bench/compare/chart.py SPEC.json OUT_PREFIX    # writes OUT_PREFIX-light.svg and -dark.svg

The spec: {"aria": "...", "series": ["Ray", ...], "rows": [{"label": "...",
"values": {"Ray": 2.5, ...}}]}, and optionally "versus": a prefix for every
series name ("Ray on ", when the series are cluster sizes), shown as the
legend's lead-in. A value is how many times faster Chimaera is than that
runtime: above 1 the bar goes right, below 1 it goes left (the
other runtime is faster). A runtime missing from a row has no bar there.
The scale is logarithmic and every bar carries its value.
"""

from __future__ import annotations

import json
import math
import sys
from html import escape

# Categorical slots in a fixed order (validated for color-vision deficiency on
# both surfaces), one per runtime Chimaera is compared with.
SLOTS = {"light": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"], "dark": ["#3987e5", "#d95926", "#199e70", "#c98500"]}
INK = {"light": ("#1f2328", "#59636e", "#d1d9e0", "#818b98"), "dark": ("#e6edf3", "#9198a1", "#30363d", "#656c76")}
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
W, LABEL_X, PLOT_L, PLOT_R = 820, 286, 300, 770
BAR, GAP, ROW_GAP = 12, 2, 16


def times(v: float) -> str:
    x = v if v >= 1 else 1 / v
    if x >= 10:
        s = f"{x:,.0f}"
    elif x < 1.1:  # near parity: two decimals, so 1.04x does not read as a tie
        s = f"{x:.2f}"
    else:
        s = f"{x:.1f}".rstrip("0").rstrip(".")
    return s + "x"


def draw(spec: dict, theme: str) -> str:
    primary, secondary, grid, axis = INK[theme]
    series = spec["series"]
    versus = spec.get("versus", "")
    color = dict(zip(series, SLOTS[theme]))
    vals = [v for r in spec["rows"] for v in r["values"].values()]
    lo = min(1.0, min(vals)) / 1.6
    hi = max(1.0, max(vals)) * 3.2  # room for the value label
    a, b = math.log10(lo), math.log10(hi)

    def x(v: float) -> float:
        return PLOT_L + (math.log10(v) - a) / (b - a) * (PLOT_R - PLOT_L)

    legend = len(series) > 1
    top = 84 if legend else 48
    heights = [max(1, len(r["values"])) * (BAR + GAP) - GAP for r in spec["rows"]]
    height = top + sum(heights) + ROW_GAP * len(heights) + 8
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {height}" width="{W}" height="{height}" '
           f'font-family="{FONT}" role="img" aria-label="{escape(spec["aria"])}">']
    one = x(1.0)
    out.append(f'<text x="{one - 10:.1f}" y="16" text-anchor="end" font-size="13" fill="{secondary}">'
               f'◀ {escape(spec.get("left", "other runtime faster"))}</text>')
    out.append(f'<text x="{one + 10:.1f}" y="16" font-size="13" fill="{secondary}">'
               f'{escape(spec.get("right", "Chimaera faster"))} ▶</text>')
    if legend:
        lx = PLOT_L
        lead = f"Chimaera against {versus}".rstrip()
        out.append(f'<text x="{LABEL_X}" y="44" text-anchor="end" font-size="12" fill="{secondary}">'
                   f'{escape(lead)}</text>')
        for s in series:
            out.append(f'<rect x="{lx}" y="34" width="12" height="12" rx="3" fill="{color[s]}"/>')
            out.append(f'<text x="{lx + 17}" y="44" font-size="12" fill="{primary}">{escape(s)}</text>')
            lx += 32 + 6.6 * len(s)
    y_axis = top - 6
    steps = [3, 10, 30, 100, 300, 1000, 3000, 10000]
    ticks = [1.0] + steps + [1 / k for k in steps]
    for t in ticks:
        if lo <= t <= hi / 2.2:
            tx = x(t)
            strong = abs(t - 1) < 1e-9
            out.append(f'<line x1="{tx:.1f}" x2="{tx:.1f}" y1="{y_axis}" y2="{height - 8}" '
                       f'stroke="{axis if strong else grid}" stroke-width="{1.5 if strong else 1}"/>')
            label = "1x" if strong else times(t)
            out.append(f'<text x="{tx:.1f}" y="{y_axis - 4}" text-anchor="middle" font-size="11" '
                       f'fill="{secondary}">{label}</text>')
    y = top + 4
    for r, h in zip(spec["rows"], heights):
        out.append(f'<text x="{LABEL_X}" y="{y + h / 2 + 5:.1f}" text-anchor="end" font-size="14" '
                   f'fill="{primary}">{escape(r["label"])}</text>')
        by = y
        for s in series:
            if s not in r["values"]:
                continue
            v = r["values"][s]
            end = x(v)
            rad = min(4, abs(end - one) / 2)
            if v >= 1:
                d = (f"M{one:.1f},{by} H{end - rad:.1f} Q{end:.1f},{by} {end:.1f},{by + rad} V{by + BAR - rad} "
                     f"Q{end:.1f},{by + BAR} {end - rad:.1f},{by + BAR} H{one:.1f} Z")
                tx, anchor = end + 6, "start"
            else:
                d = (f"M{one:.1f},{by} H{end + rad:.1f} Q{end:.1f},{by} {end:.1f},{by + rad} V{by + BAR - rad} "
                     f"Q{end:.1f},{by + BAR} {end + rad:.1f},{by + BAR} H{one:.1f} Z")
                tx, anchor = end - 6, "end"
            who = f"Chimaera {times(v)} faster" if v >= 1 else f"{versus}{s} {times(v)} faster"
            out.append(f'<path d="{d}" fill="{color[s]}"><title>{escape(r["label"])}, against {escape(versus + s)}: '
                       f'{who}</title></path>')
            out.append(f'<text x="{tx:.1f}" y="{by + BAR - 2}" text-anchor="{anchor}" font-size="11" '
                       f'font-weight="600" fill="{primary}">{times(v)}</text>')
            by += BAR + GAP
        y += h + ROW_GAP
    out.append("</svg>")
    return "\n".join(out) + "\n"


NAMES = {"ray": "Ray", "dask": "Dask", "multiprocessing": "multiprocessing", "joblib": "joblib"}


def from_compare(path: str, rows: dict[str, str]) -> dict:
    """A spec from bench/compare results: `rows` maps a workload to its label.
    A runtime that failed a row, or cannot run it, has no bar there."""
    doc = json.load(open(path))
    others = [r for r in doc["runtimes"] if r != "chimaera"]
    spec = {"series": [NAMES[r] for r in others], "rows": [],
            "aria": f"Chimaera against {', '.join(NAMES[r] for r in others)} on one {doc['machine']['cpus']}-CPU "
                    "machine: times faster per workload"}
    for w, label in rows.items():
        cells = doc["summary"].get(w, {})
        better = next(r["better"] for r in doc["records"] if r["workload"] == w and "better" in r)
        me = cells.get("chimaera", {}).get("median")
        values = {}
        for r in others:
            them = cells.get(r, {}).get("median")
            if me and them:
                values[NAMES[r]] = me / them if better == "higher" else them / me
        spec["rows"].append({"label": label, "values": values})
    return spec


def main() -> None:
    spec = json.load(open(sys.argv[1]))
    if "rows_from" in spec:  # {"rows_from": "results.json", "rows": {"tasks": "label", ...}}
        spec = from_compare(spec["rows_from"], spec["rows"])
    for theme in ("light", "dark"):
        with open(f"{sys.argv[2]}-{theme}.svg", "w") as f:
            f.write(draw(spec, theme))


if __name__ == "__main__":
    main()
