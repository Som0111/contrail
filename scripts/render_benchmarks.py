"""Turn saved benchmark JSON into the Markdown tables BENCHMARKS.md quotes.

    python -m scripts.render_benchmarks benchmarks/results/*.json [--out tables.md]

The tables are generated, never typed: every number comes from a result file, and each
section names the commit, machine and date that produced it. Paste or include the output;
if a number in BENCHMARKS.md or the README is not in a table from here, it is not
backed by a saved run.
"""

import argparse
from pathlib import Path

from scripts.benchlib import load_results


def _env_line(r: dict) -> str:
    e = r["environment"]
    commit = (e.get("git_commit") or "unknown")[:10] + (" (dirty tree)" if e.get("git_dirty") else "")
    mem = e.get("memory_bytes")
    mem_s = f"{mem / 2**30:.0f} GiB" if mem else "unknown RAM"
    return (f"commit `{commit}`, {e['recorded_at']}, python {e['python']}, {e['platform']}, "
            f"{e['cpu_count']} CPU, {mem_s}")


def spread(m: dict, digits: int = 2) -> str:
    """`median [min - max]`, or a dash when no run produced a value."""
    if not m or m.get("n", 0) == 0:
        return "-"
    f = f"{{:,.{digits}f}}"
    if m["n"] == 1:
        return f.format(m["median"]) + " (1 run)"
    return f"{f.format(m['median'])} [{f.format(m['min'])} - {f.format(m['max'])}]"


def render_windowing(r: dict) -> str:
    n = len(r["runs"])
    seeds = ", ".join(str(x["seed"]) for x in r["runs"])
    out = [f"### Claim 1: windowing ({n} seeds: {seeds})", "", _env_line(r), "",
           "Median [min - max] across seeds; watermark at L = max skew.", "",
           "| level | events misplaced, naive | events misplaced, watermark "
           "| windows wrong, naive | windows wrong, watermark | silent errors, naive -> watermark |",
           "|---|---|---|---|---|---|"]
    for level, m in r["summary"].items():
        out.append(
            f"| {level} | {spread(m['naive_misplaced_pct'])}% | {spread(m['watermark_misplaced_pct'])}% "
            f"| {spread(m['naive_windows_wrong_pct'])}% | {spread(m['watermark_windows_wrong_pct'])}% "
            f"| {spread(m['naive_silent'], 0)} -> {spread(m['watermark_silent'], 0)} |")
    if not r["summary"]:
        out.append("| (single run: no spread to report) | | | | | |")
    return "\n".join(out)


CONTROL_ROWS = (
    ("peak lag", "peak_lag", 0), ("final lag", "final_lag", 0),
    ("time to stabilise (s)", "time_to_stabilize_s", 1), ("shed", "shed_dropped", 0),
    ("undrained", "undrained", 0), ("scale actions", "scale_actions", 0),
    ("false-positive actions", "false_positive_actions", 0),
    ("p95 latency (s)", "p95_s", 2), ("p99 latency (s)", "p99_s", 2),
    ("CPU (s)", "cpu_s", 1), ("peak RSS (MB)", "peak_rss_mb", 0),
)


def _arm_table(summary: dict) -> list[str]:
    names = list(summary)
    out = ["| metric | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
    for label, key, digits in CONTROL_ROWS:
        out.append(f"| {label} | " + " | ".join(spread(summary[n][key], digits) for n in names) + " |")
    return out


def render_control(r: dict) -> str:
    c = r["config"]
    head = (f"### Claim 2: control ({len(r['runs'])} runs)\n\n{_env_line(r)}\n\n"
            f"{c['aircraft']} aircraft, {c['base_rate']:g} -> {c['burst_rate']:g} Hz for "
            f"{c['burst_s']:g}s, cap {c['max_rate']:g} ev/s/worker, "
            f"{c['min_workers']}-{c['max_workers']} workers. Median [min - max].\n")
    return head + "\n" + "\n".join(_arm_table(r["summary"]))


def render_sweep(r: dict) -> str:
    out = [f"### Claim 2: controller sweep ({len(r['cells'])} cells)", "", _env_line(r), ""]
    for cell in r["cells"]:
        c = cell["cell"]
        out += [f"**burst {c['burst_rate']:g} Hz for {c['burst_s']:g}s, base {c['base_rate']:g} Hz, "
                f"cap {c['max_rate']:g}, max workers {c['max_workers']}** "
                f"(overload ratio {cell['overload_ratio']:.2f}, {len(cell['runs'])} runs)", "",
                *_arm_table(cell["summary"]), ""]
    return "\n".join(out).rstrip()


RENDERERS = {"windowing": render_windowing, "control": render_control, "control-sweep": render_sweep}


def render(results: list[dict]) -> str:
    return "\n\n".join(RENDERERS[r["benchmark"]](r) for r in results) + "\n"


def main() -> None:
    p = argparse.ArgumentParser(description="Render saved benchmark JSON as Markdown tables.")
    p.add_argument("files", nargs="+", type=Path)
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()
    text = render(load_results(args.files))
    if args.out:
        args.out.write_text(text, encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
