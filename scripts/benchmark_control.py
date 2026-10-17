"""Core claim #2, measured: adaptive lag control vs a static worker pool.

The same burst is run twice against the same infrastructure -- identical
generator seed, identical rate schedule, identical per-worker throughput cap,
identical lag sampling -- with the only difference being whether the controller's
decisions are applied or merely logged. Anything the table shows is therefore
adaptation, not instrumentation.

Each arm gets a fresh topic and consumer group rather than deleting and
recreating a shared one. That is not tidiness: a recreated topic leaves stale
group offsets and stale broker metadata behind, which is exactly how the 1.4
integration run silently reported zero lag three times in a row.

Latency is `processed_at - event_time` read back from TimescaleDB: the instant
the aircraft was in that state, to the instant the row was durably committed.
Chaos is off for this benchmark -- injected arrival skew would add a constant to
both arms and dilute the queueing delay that is actually under test.

Each comparison is repeated (--runs) and reported as median with min/max, and the
result is saved as JSON together with the commit and machine it ran on.

Usage:  python -m scripts.benchmark_control [--burst-rate 12] [--max-rate 250] [--runs 3]
"""

import argparse
import asyncio
import platform
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from scripts.benchlib import (
    actions_before,
    resource_snapshot,
    summarize,
    time_to_stabilize,
    write_result,
)
from src.common.config import get_settings
from src.common.logging import configure
from src.control.controller import ControllerConfig
from src.control.supervisor import SupervisionResult, supervise
from src.ingestor.base import publish
from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.storage import timescale

LATENCY_SQL = """
SELECT
    count(*)                                                   AS rows,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY latency)      AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY latency)      AS p95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY latency)      AS p99,
    max(latency)                                               AS max
FROM (
    SELECT extract(epoch FROM (processed_at - event_time)) AS latency
    FROM flight_events
    WHERE icao24 = ANY($1::text[])
) s
"""


@dataclass
class Arm:
    name: str
    published: int
    stored: int
    supervision: SupervisionResult
    p50: float
    p95: float
    p99: float
    max_latency: float
    static: bool
    resources: dict = field(default_factory=dict)
    timing: dict = field(default_factory=dict)


async def load(bootstrap: str, topic: str, phases: list[tuple[float, float]],
               aircraft: int, seed: int) -> int:
    """Run the rate schedule. Chaos off: this measures queueing, not skew."""
    total = 0
    for duration, rate in phases:
        source = SyntheticSource(
            n_aircraft=aircraft, rate_hz=rate, chaos=ChaosConfig(),
            seed=seed, duration_s=duration,
        )
        total += await publish(source, bootstrap, topic, get_settings().kafka_partitions)
    return total


async def run_arm(name: str, static_workers: int | None, args) -> Arm:
    s = get_settings()
    tag = uuid.uuid4().hex[:8]
    topic, group = f"bench.control.{tag}", f"bench-control-{tag}"

    # Scope everything to the aircraft this arm generates. The demo pipeline runs
    # continuously as a compose service and writes to the same table, so counting
    # or truncating the whole table would mix its traffic into the measurement.
    fleet = [
        a.icao24 for a in SyntheticSource(
            n_aircraft=args.aircraft, rate_hz=1.0, seed=args.seed, duration_s=1
        ).fleet
    ]
    pool = await timescale.connect(s.postgres_dsn, min_size=1, max_size=2)
    await timescale.ensure_schema(pool)
    await timescale.delete_events(pool, fleet)

    phases = [
        (args.baseline_s, args.base_rate),
        (args.burst_s, args.burst_rate),
        (args.recovery_s, args.base_rate),
    ]
    # Enough headroom past the load for a healthy arm to drain; the unhealthy one
    # will still be behind, which is the finding.
    duration = args.baseline_s + args.burst_s + args.recovery_s + args.drain_s

    t_start = time.perf_counter()
    cpu_before = resource_snapshot()["cpu_s"]
    supervisor = asyncio.create_task(
        supervise(
            s.kafka_bootstrap, topic, group, s.postgres_dsn,
            ControllerConfig(
                min_workers=args.min_workers, max_workers=args.max_workers,
                cooldown_s=args.cooldown_s,
            ),
            interval_s=args.interval, duration_s=duration,
            max_rate=args.max_rate, static_workers=static_workers,
        )
    )
    await asyncio.sleep(2.0)  # let the pool join the group before load starts
    t_load = time.perf_counter()
    published = await load(s.kafka_bootstrap, topic, phases, args.aircraft, args.seed)
    t_loaded = time.perf_counter()
    supervision = await supervisor
    t_done = time.perf_counter()
    snap = resource_snapshot()
    # CPU is this process only (supervisor and sink workers share it); the broker and
    # database run in other containers and are not included.
    resources = {"cpu_s": snap["cpu_s"] - cpu_before, "peak_rss_mb": snap["peak_rss_mb"]}

    stored = await timescale.count_events(pool, fleet)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(LATENCY_SQL, fleet)
    await pool.close()

    return Arm(
        name=name, published=published, stored=stored, supervision=supervision,
        p50=float(row["p50"] or 0), p95=float(row["p95"] or 0),
        p99=float(row["p99"] or 0), max_latency=float(row["max"] or 0),
        static=static_workers is not None,
        resources=resources,
        # startup = supervisor launch to first load; load = the rate schedule;
        # drain = load end to supervisor exit; teardown/measurement is not in here.
        timing={"startup_s": t_load - t_start, "load_s": t_loaded - t_load,
                "drain_s": t_done - t_loaded},
    )


def print_arm(arm: Arm) -> None:
    sup = arm.supervision
    print(f"\n  {arm.name}")
    print(f"    published {arm.published:,}   stored {arm.stored:,}"
          f"   ({100 * arm.stored / arm.published:.1f}% of the burst absorbed)")
    print(f"    peak lag  {sup.peak_lag:,}   final lag {sup.final_lag:,}"
          f"   shed {sup.shed_dropped:,}")
    print(f"    latency   p50 {arm.p50:7.2f}s   p95 {arm.p95:7.2f}s"
          f"   p99 {arm.p99:7.2f}s   max {arm.max_latency:7.2f}s")
    # Lag and shedding over time. A single peak number cannot show whether lag
    # stabilised or was still climbing when the run ended, which is the whole
    # question once shedding is involved.
    samples = sup.samples
    if samples and not arm.static:
        step = max(1, len(samples) // 12)
        print("    trajectory (lag / workers / shedding):")
        for i in range(0, len(samples), step):
            d = sup.decisions[i]
            bar = "#" * min(40, int(samples[i].total / 250))
            print(f"      t={i * 2:>4}s lag={samples[i].total:>7,} w={d.workers} "
                  f"{'SHED' if d.shedding else '    '} {bar}")

    if arm.static:
        # The controller still runs in the static arm so both arms share one
        # sampling path -- but nothing it decides is applied. Saying "actions"
        # here would claim the baseline adapted, which is the opposite of the
        # thing being measured.
        print(f"    controller: observed only, {len(sup.actions)} decisions"
              f" NOT applied (pool pinned)")
    elif sup.actions:
        print("    actions applied:")
        for d in sup.actions:
            print(f"      {d.action:<11} lag={d.lag:>7,} slope={d.slope:>8.1f}/s"
                  f" t={d.t_stat:>6.2f} -> workers={d.workers} shed={d.shedding}")
    else:
        print("    actions applied: none")


def analyze(arm: Arm, args) -> dict:
    """Everything a run is judged on, as plain numbers (JSON-safe, summarisable)."""
    sup = arm.supervision
    interval = args.interval
    lags = [s.total for s in sup.samples]
    burst_start = int(args.baseline_s / interval)
    burst_end = int((args.baseline_s + args.burst_s) / interval)
    return {
        "name": arm.name, "static": arm.static,
        "published": arm.published, "stored": arm.stored,
        "shed_dropped": sup.shed_dropped,
        # Published but neither stored nor deliberately shed: still queued when the run
        # ended, so a non-zero value means the arm had not drained.
        "undrained": arm.published - arm.stored - sup.shed_dropped,
        "absorbed_pct": 100 * arm.stored / arm.published if arm.published else 0.0,
        "peak_lag": sup.peak_lag, "final_lag": sup.final_lag,
        "time_to_stabilize_s": time_to_stabilize(
            lags, interval, burst_end, args.recovered_lag),
        "scale_actions": len(sup.actions),
        # The first phase is steady baseline load, so any action there is a false trigger.
        "false_positive_actions": actions_before(sup.decisions, burst_start),
        "p50_s": arm.p50, "p95_s": arm.p95, "p99_s": arm.p99, "max_s": arm.max_latency,
        "cpu_s": arm.resources.get("cpu_s"), "peak_rss_mb": arm.resources.get("peak_rss_mb"),
        "timing_s": arm.timing,
        "lag_samples": lags,
        "workers": [d.workers for d in sup.decisions],
    }


# Metrics worth a median-and-spread across repeated runs.
SUMMARY_KEYS = ("peak_lag", "final_lag", "time_to_stabilize_s", "shed_dropped", "undrained",
                "absorbed_pct", "scale_actions", "false_positive_actions",
                "p50_s", "p95_s", "p99_s", "max_s", "cpu_s", "peak_rss_mb")


def summarize_arms(analyses: list[dict]) -> dict:
    out = {}
    for key in SUMMARY_KEYS:
        vals = [a[key] for a in analyses if a[key] is not None]
        out[key] = summarize(vals)
        # A run that never stabilised has no time to report; say how many, don't drop silently.
        out[key]["missing"] = len(analyses) - len(vals)
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Benchmark static vs adaptive lag control.")
    p.add_argument("--aircraft", type=int, default=60)
    p.add_argument("--base-rate", type=float, default=2.0)
    p.add_argument("--burst-rate", type=float, default=12.0, help="6x the baseline")
    p.add_argument("--baseline-s", type=float, default=30.0)
    p.add_argument("--burst-s", type=float, default=70.0)
    p.add_argument("--recovery-s", type=float, default=40.0)
    p.add_argument("--drain-s", type=float, default=60.0)
    p.add_argument("--max-rate", type=float, default=250.0, help="per-worker events/s cap")
    p.add_argument("--min-workers", type=int, default=1)
    p.add_argument("--max-workers", type=int, default=4)
    p.add_argument("--cooldown-s", type=float, default=6.0)
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--seed", type=int, default=20260827)
    p.add_argument("--runs", type=int, default=3,
                   help="repeat the whole static-vs-adaptive comparison; reported as median and spread")
    p.add_argument("--static-sizes", default=None,
                   help="comma-separated pool sizes for the static baseline (default: --min-workers)")
    p.add_argument("--recovered-lag", type=int, default=200,
                   help="lag at or below this, sustained, counts as stabilised")
    p.add_argument("--out-dir", default=None,
                   help="where to write the JSON result (default benchmarks/results; '-' skips)")
    return p


def static_sizes(args) -> list[int]:
    return [int(x) for x in args.static_sizes.split(",")] if args.static_sizes else [args.min_workers]


async def run_comparison(args, quiet: bool = False) -> list[dict]:
    """One adaptive arm and one static arm per baseline size, same load. Returns analyses."""
    analyses = []
    for size in static_sizes(args):
        arm = await run_arm(f"STATIC   ({size} worker{'s' if size != 1 else ''}, no controller)", size, args)
        if not quiet:
            print_arm(arm)
        analyses.append(analyze(arm, args))
    arm = await run_arm(f"ADAPTIVE ({args.min_workers}-{args.max_workers} workers)", None, args)
    if not quiet:
        print_arm(arm)
    analyses.append(analyze(arm, args))
    return analyses


def config_record(args) -> dict:
    """Everything that defines the workload; also the key a sweep cell is identified by."""
    return {k: v for k, v in vars(args).items() if k not in ("out_dir",)}


async def main() -> None:
    args = build_parser().parse_args()
    configure("WARNING")  # the arms log per-sample; keep the table readable

    print("CONTRAIL — core claim #2: adaptive lag control vs a static pool")
    print("=" * 92)
    base = args.aircraft * args.base_rate
    burst = args.aircraft * args.burst_rate
    sizes = static_sizes(args)
    print(
        f"{args.aircraft} aircraft | baseline {base:,.0f} ev/s for {args.baseline_s:g}s"
        f" -> BURST {burst:,.0f} ev/s ({args.burst_rate / args.base_rate:g}x) for {args.burst_s:g}s"
        f" -> recovery {base:,.0f} ev/s for {args.recovery_s:g}s"
        f"\nper-worker cap {args.max_rate:g} ev/s | static arm(s) pinned at {sizes}"
        f" | adaptive arm {args.min_workers}-{args.max_workers} workers"
        f"\nchaos off | seed {args.seed} | {args.runs} run(s) | python {platform.python_version()}"
    )

    started = time.perf_counter()
    runs = []
    for i in range(args.runs):
        if args.runs > 1:
            print(f"\n{'#' * 40} run {i + 1} of {args.runs}")
        runs.append(await run_comparison(args))

    by_arm: dict[str, list[dict]] = {}
    for analyses in runs:
        for a in analyses:
            by_arm.setdefault(a["name"], []).append(a)
    summary = {name: summarize_arms(items) for name, items in by_arm.items()}

    print(f"\n{'=' * 92}\nHEADLINE  (median [min - max] over {args.runs} run(s))")
    for name, sm in summary.items():
        print(f"\n  {name}")
        for key in SUMMARY_KEYS:
            m = sm[key]
            if m["n"] == 0:
                print(f"    {key:<24} never (0 of {args.runs} runs)")
                continue
            note = f"  ({m['missing']} run(s) without a value)" if m["missing"] else ""
            print(f"    {key:<24}{m['median']:>12,.2f}  [{m['min']:,.2f} - {m['max']:,.2f}]{note}")
    elapsed = time.perf_counter() - started
    print(f"\n  completed in {elapsed:.0f}s\n")

    if args.out_dir != "-":
        path = write_result("control", {
            "config": config_record(args), "runs": runs, "summary": summary, "wall_s": elapsed,
        }, Path(args.out_dir) if args.out_dir else None)
        print(f"  result saved to {path}\n")


if __name__ == "__main__":
    asyncio.run(main())
