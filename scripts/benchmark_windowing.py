"""Core claim #1, measured: processing-time windows vs event-time watermarks.

Both processors are run over the *identical* event list at each disorder level,
and both are scored against ground truth computed from that same list. Nothing
here is estimated — every number is counted from the aggregates the two
implementations actually produced.

The stream comes from `SyntheticSource.simulate()` rather than from Kafka. That
is deliberate (DESIGN_DECISIONS.md 1.3): both processors are pure functions over
a list of events in arrival order, which is exactly what `collect()` returns from
a real topic, so the transport cannot change the result — while feeding them the
seeded generator directly removes broker scheduling noise and makes every row
reproducible from the config printed beside it.

The sweep is split into two groups on purpose, because the generator has two
disorder mechanisms with very different magnitudes and mixing them makes the
result unreadable:

  BOUNDED   out-of-order arrival within `max_skew_s`. The watermark engine's
            guarantee applies, and the table shows it reaching *exactly* zero
            error at L = max skew.
  UNBOUNDED adds late arrivals of 2-4 minutes, far beyond any sane bound. No
            lateness bound can absorb these, so the interesting question is not
            whether error appears but whether it is *reported* -- naive misfiles
            them silently, the watermark engine routes every one to a counted
            side output.

Usage:  python -m scripts.benchmark_windowing [--aircraft N] [--ticks S]
"""

import argparse
import logging
import platform
import time
from dataclasses import dataclass

from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.windowing import naive, watermark
from src.windowing.aggregates import compare, ground_truth, window_start

WINDOW_S = 60


@dataclass
class Level:
    group: str
    name: str
    chaos: ChaosConfig

    def describe(self) -> str:
        c = self.chaos
        late = (
            f", late {c.late_prob:.3f} @ {c.late_delay_s:g}-{2 * c.late_delay_s:g}s"
            if c.late_prob
            else ""
        )
        return (
            f"ooo {c.out_of_order_prob:.2f} @ skew {c.max_skew_s:g}s, "
            f"dup {c.duplicate_prob:.2f}{late}, drop {c.drop_prob:.2f}"
        )


LEVELS = [
    Level("BOUNDED", "none", ChaosConfig(duplicate_prob=0.05, drop_prob=0.01)),
    Level("BOUNDED", "low",
          ChaosConfig(out_of_order_prob=0.05, max_skew_s=5.0,
                      duplicate_prob=0.02, drop_prob=0.01)),
    Level("BOUNDED", "medium",
          ChaosConfig(out_of_order_prob=0.20, max_skew_s=20.0,
                      duplicate_prob=0.05, drop_prob=0.01)),
    Level("BOUNDED", "high",
          ChaosConfig(out_of_order_prob=0.40, max_skew_s=45.0,
                      duplicate_prob=0.10, drop_prob=0.02)),
    Level("UNBOUNDED", "medium+late",
          ChaosConfig(out_of_order_prob=0.20, max_skew_s=20.0, duplicate_prob=0.05,
                      late_prob=0.02, late_delay_s=90.0, drop_prob=0.01)),
    Level("UNBOUNDED", "high+late",
          ChaosConfig(out_of_order_prob=0.40, max_skew_s=45.0, duplicate_prob=0.10,
                      late_prob=0.05, late_delay_s=120.0, drop_prob=0.02)),
]


def misattributed_by_naive(events, window_s=WINDOW_S) -> int:
    """Events the baseline files under a window that is not their own."""
    seen, wrong = set(), 0
    for e in events:
        if e.dedup_key in seen:
            continue
        seen.add(e.dedup_key)
        if window_start(e.ingest_time, window_s) != window_start(e.event_time, window_s):
            wrong += 1
    return wrong


def lateness_bounds(chaos: ChaosConfig) -> list[float]:
    """Sweep around the property under test: exactness should arrive at L == max skew."""
    skew = chaos.max_skew_s
    bounds = {0.0, skew / 2, skew, 2 * skew}
    if chaos.late_prob:
        bounds.add(2 * chaos.late_delay_s + skew)  # wide enough to absorb even the stragglers
    return sorted(bounds)


def run_level(level: Level, aircraft: int, ticks: int, seed: int) -> list[dict]:
    events = list(
        SyntheticSource(
            n_aircraft=aircraft, rate_hz=1.0, chaos=level.chaos,
            seed=seed, duration_s=ticks,
        ).simulate()
    )
    truth = ground_truth(events, WINDOW_S)
    unique = sum(a.count for a in truth.values())

    rows = [{
        "processor": "naive", "bound": None,
        "cmp": compare(truth, naive.aggregate(events, WINDOW_S)),
        "misplaced": misattributed_by_naive(events, WINDOW_S),
        # The baseline has no side channel: every event it misfiles is misfiled
        # silently, and shows up only as a wrong aggregate.
        "reported": 0,
    }]
    for bound in lateness_bounds(level.chaos):
        result = watermark.aggregate(events, WINDOW_S, bound)
        rows.append({
            "processor": "watermark", "bound": bound,
            "cmp": compare(truth, result.windows),
            # A watermark engine never files an event under the wrong window; it
            # either places it correctly or withholds it to the side output.
            "misplaced": len(result.late),
            "reported": len(result.late),
        })

    for r in rows:
        r.update(unique=unique, generated=aircraft * ticks,
                 arrived=len(events), truth_windows=len(truth))
    return rows


def print_level(level: Level, rows: list[dict]) -> None:
    head = rows[0]
    print(f"\n  {level.name:<12} {level.describe()}")
    print(
        f"    stream: {head['generated']:,} generated, {head['arrived']:,} arrived, "
        f"{head['unique']:,} unique, {head['truth_windows']:,} true windows"
    )
    print(
        f"    {'processor':<10} {'L':>7} {'windows wrong':>15} {'win err':>8}"
        f" {'events misplaced':>17} {'evt err':>8} {'worst window':>13} {'silent':>9}"
    )
    for r in rows:
        c = r["cmp"]
        silent = r["misplaced"] - r["reported"]
        bound = "-" if r["bound"] is None else f"{r['bound']:g}s"
        note = ""
        if r["bound"] is not None and r["bound"] == level.chaos.max_skew_s and r["bound"]:
            note = "  <- L = max skew"
        print(
            f"    {r['processor']:<10} {bound:>7} "
            f"{c.windows_wrong:>6,} / {c.windows_total:<6,} "
            f"{100 * c.window_error_rate:>7.2f}% "
            f"{r['misplaced']:>17,} "
            f"{100 * r['misplaced'] / r['unique']:>7.2f}% "
            f"{c.worst_count_error:>13,} "
            f"{silent:>9,}{note}"
        )


def main() -> None:
    p = argparse.ArgumentParser(description="Benchmark naive vs watermark windowing.")
    p.add_argument("--aircraft", type=int, default=50)
    p.add_argument("--ticks", type=int, default=1800, help="seconds of event time at 1 Hz")
    p.add_argument("--seed", type=int, default=20260827)
    args = p.parse_args()

    # The engine logs every late event at WARNING; that is right in the pipeline
    # and pure noise in a benchmark that deliberately creates millions of them.
    logging.getLogger("contrail.windowing.watermark").setLevel(logging.ERROR)

    print("CONTRAIL — core claim #1: event-time windowing vs processing-time baseline")
    print("=" * 104)
    print(
        f"window {WINDOW_S}s | {args.aircraft} aircraft @ 1 Hz | {args.ticks}s of event time"
        f" | seed {args.seed}\npython {platform.python_version()} on {platform.platform()}"
    )
    print(
        "\n'silent' = events placed wrongly with no record of it. The whole point:"
        "\nthe baseline cannot tell you it was wrong, the watermark engine always can."
    )

    started = time.perf_counter()
    results, group = {}, None
    for level in LEVELS:
        if level.group != group:
            group = level.group
            print(f"\n{group} DISORDER" + ("" if group == "BOUNDED" else " (late arrivals no bound can absorb)"))
            print("-" * 104)
        rows = run_level(level, args.aircraft, args.ticks, args.seed)
        results[level.name] = rows
        print_level(level, rows)

    print(f"\n{'=' * 104}\nHEADLINE  (watermark at L = max skew, the operationally sane setting)")
    for level in LEVELS:
        rows = results[level.name]
        nv = rows[0]
        at_skew = [r for r in rows[1:] if r["bound"] == level.chaos.max_skew_s]
        wm = at_skew[0] if at_skew else rows[1]
        print(
            f"  {level.name:<12} events misplaced "
            f"{100 * nv['misplaced'] / nv['unique']:6.2f}% -> "
            f"{100 * wm['misplaced'] / wm['unique']:5.2f}%"
            f"   |  windows wrong {100 * nv['cmp'].window_error_rate:6.2f}% -> "
            f"{100 * wm['cmp'].window_error_rate:6.2f}%"
            f"   |  silent errors {nv['misplaced']:,} -> {wm['misplaced'] - wm['reported']:,}"
        )
    print(f"\n  completed in {time.perf_counter() - started:.1f}s\n")


if __name__ == "__main__":
    main()
