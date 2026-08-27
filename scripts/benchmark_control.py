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

Usage:  python -m scripts.benchmark_control [--burst-rate 12] [--max-rate 250]
"""

import argparse
import asyncio
import platform
import time
import uuid
from dataclasses import dataclass

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
    published = await load(s.kafka_bootstrap, topic, phases, args.aircraft, args.seed)
    supervision = await supervisor

    stored = await timescale.count_events(pool, fleet)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(LATENCY_SQL, fleet)
    await pool.close()

    return Arm(
        name=name, published=published, stored=stored, supervision=supervision,
        p50=float(row["p50"] or 0), p95=float(row["p95"] or 0),
        p99=float(row["p99"] or 0), max_latency=float(row["max"] or 0),
        static=static_workers is not None,
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


async def main() -> None:
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
    args = p.parse_args()

    configure("WARNING")  # the arms log per-sample; keep the table readable

    print("CONTRAIL — core claim #2: adaptive lag control vs a static pool")
    print("=" * 92)
    base = args.aircraft * args.base_rate
    burst = args.aircraft * args.burst_rate
    print(
        f"{args.aircraft} aircraft | baseline {base:,.0f} ev/s for {args.baseline_s:g}s"
        f" -> BURST {burst:,.0f} ev/s ({args.burst_rate / args.base_rate:g}x) for {args.burst_s:g}s"
        f" -> recovery {base:,.0f} ev/s for {args.recovery_s:g}s"
        f"\nper-worker cap {args.max_rate:g} ev/s | static arm pinned at {args.min_workers}"
        f" worker | adaptive arm {args.min_workers}-{args.max_workers} workers"
        f"\nchaos off | seed {args.seed} | python {platform.python_version()}"
    )

    started = time.perf_counter()
    static = await run_arm(f"STATIC   ({args.min_workers} worker, no controller)",
                           args.min_workers, args)
    print_arm(static)
    adaptive = await run_arm(f"ADAPTIVE ({args.min_workers}-{args.max_workers} workers)",
                             None, args)
    print_arm(adaptive)

    print(f"\n{'=' * 92}\nHEADLINE")
    def better(a: float, b: float) -> str:
        return f"{a / b:.1f}x lower" if b and a > b else "-"
    print(f"  {'':<16}{'static':>12}{'adaptive':>12}{'':>6}")
    for label, sv, av in (
        ("peak lag", static.supervision.peak_lag, adaptive.supervision.peak_lag),
        ("final lag", static.supervision.final_lag, adaptive.supervision.final_lag),
        ("p50 latency", static.p50, adaptive.p50),
        ("p95 latency", static.p95, adaptive.p95),
        ("p99 latency", static.p99, adaptive.p99),
        ("max latency", static.max_latency, adaptive.max_latency),
    ):
        unit = "" if "lag" in label else "s"
        print(f"  {label:<16}{sv:>12,.2f}{unit}{av:>11,.2f}{unit}   {better(sv, av)}")
    print(f"\n  completed in {time.perf_counter() - started:.0f}s\n")


if __name__ == "__main__":
    asyncio.run(main())
