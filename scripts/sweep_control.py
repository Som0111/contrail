"""Claim 2 beyond a single overload point: sweep the controller over a workload grid.

Every cell is the same static-vs-adaptive comparison `benchmark_control` runs, repeated
`--runs` times, so the table shows how the result moves with burst size, burst length,
baseline rate, per-worker cap and worker ceiling -- and where it does not hold. Cells
that overload the pool (the overload ratio below 1 means the burst fits under the cap
and nothing needs shedding) are the ones that exercise shedding; the default grid
includes `--max-workers 2` for that reason.

    overload_ratio = burst events/s / (per-worker cap x max workers)

A long burst against a pool that cannot keep up is the sustained-overload case, and the
recovery phase after it gives the post-overload time to stabilise.

This takes a long time: each cell runs 2+ arms of several minutes, `--runs` times.

Usage:  python -m scripts.sweep_control --burst-rates 12,24 --max-workers 2,4 --runs 3
"""

import argparse
import asyncio
import itertools
import time
from pathlib import Path

from scripts.benchlib import write_result
from scripts.benchmark_control import (
    build_parser,
    config_record,
    run_comparison,
    summarize_arms,
)
from src.common.logging import configure


def floats(s: str) -> list[float]:
    return [float(x) for x in s.split(",")]


def cells(grid: dict[str, list]) -> list[dict]:
    keys = list(grid)
    return [dict(zip(keys, combo, strict=True)) for combo in itertools.product(*grid.values())]


def overload_ratio(args) -> float:
    return args.aircraft * args.burst_rate / (args.max_rate * args.max_workers)


async def main() -> None:
    p = argparse.ArgumentParser(description="Sweep the lag controller over a workload grid.")
    p.add_argument("--burst-rates", type=floats, default=[12.0, 24.0])
    p.add_argument("--burst-durations", type=floats, default=[70.0, 140.0])
    p.add_argument("--base-rates", type=floats, default=[2.0])
    p.add_argument("--worker-caps", type=floats, default=[250.0])
    p.add_argument("--max-workers", type=lambda s: [int(x) for x in s.split(",")], default=[2, 4])
    p.add_argument("--static-sizes", default="1,2", help="static baseline pool sizes, per cell")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--out-dir", default=None)
    sweep = p.parse_args()
    configure("WARNING")

    grid = {
        "burst_rate": sweep.burst_rates, "burst_s": sweep.burst_durations,
        "base_rate": sweep.base_rates, "max_rate": sweep.worker_caps,
        "max_workers": sweep.max_workers,
    }
    todo = cells(grid)
    print(f"sweep: {len(todo)} cells x {sweep.runs} runs, static sizes {sweep.static_sizes}")

    started = time.perf_counter()
    results = []
    for n, cell in enumerate(todo, 1):
        args = build_parser().parse_args([])
        for k, v in cell.items():
            setattr(args, k, v)
        args.runs, args.static_sizes = sweep.runs, sweep.static_sizes
        print(f"[{n}/{len(todo)}] {cell}  overload ratio {overload_ratio(args):.2f}", flush=True)
        runs = [await run_comparison(args, quiet=True) for _ in range(sweep.runs)]
        by_arm: dict[str, list[dict]] = {}
        for analyses in runs:
            for a in analyses:
                by_arm.setdefault(a["name"], []).append(a)
        results.append({
            "cell": cell, "overload_ratio": overload_ratio(args),
            "config": config_record(args), "runs": runs,
            "summary": {name: summarize_arms(items) for name, items in by_arm.items()},
        })

    elapsed = time.perf_counter() - started
    path = write_result("control-sweep", {"grid": grid, "cells": results, "wall_s": elapsed},
                        Path(sweep.out_dir) if sweep.out_dir else None)
    print(f"done in {elapsed / 60:.1f} min; saved to {path}")


if __name__ == "__main__":
    asyncio.run(main())
