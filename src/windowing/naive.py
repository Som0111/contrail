"""THE NAIVE BASELINE — deliberately the wrong way. Not the product.

This processor buckets each event by *when the pipeline received it*, not by
when the aircraft was actually in that state. That is the default a stream
processor gives you if you never think about event time, and it is the thing
Phase 1.2's watermark engine is measured against.

Its failure mode is misattribution: an event delayed across a window boundary
is counted in the window it *arrived* in, inflating that window's count and
skewing its averages while starving the window it belonged to. Under an ordered
stream it is exactly correct, which is what makes it seductive — the bug only
appears once the network misbehaves.

One deliberate handicap in the baseline's favour: processing time here is the
`ingest_time` recorded on the record, not wall-clock-at-consume. See
DESIGN_DECISIONS.md — a wall-clock version is strictly *more* wrong, so this
biases the comparison toward the naive approach rather than against it.
"""

import argparse
import asyncio
import logging

from src.common.config import get_settings
from src.common.logging import configure
from src.common.models import FlightState
from src.windowing.aggregates import (
    DEFAULT_WINDOW_S,
    Aggregator,
    WindowAggregate,
    WindowKey,
    window_start,
)

log = logging.getLogger("contrail.windowing.naive")


def aggregate(
    events: list[FlightState], window_s: int = DEFAULT_WINDOW_S
) -> dict[WindowKey, WindowAggregate]:
    """Bucket by arrival time. Wrong on purpose."""
    agg = Aggregator(window_s)
    for event in events:
        agg.add(window_start(event.ingest_time, window_s), event)
    return agg.finalize()


def print_windows(
    aggregates: dict[WindowKey, WindowAggregate], title: str, limit: int = 20
) -> None:
    print(f"\n{title}   ({len(aggregates)} windows)")
    print(f"  {'window start':<22} {'cell':<10} {'count':>7} {'avg alt m':>11} {'avg vel m/s':>12}")
    for key in sorted(aggregates)[:limit]:
        a = aggregates[key]
        print(
            f"  {a.window_start.isoformat():<22} {a.partition_key:<10} "
            f"{a.count:>7,} {a.avg_altitude_m:>11,.1f} {a.avg_velocity_ms:>12,.2f}"
        )
    if len(aggregates) > limit:
        print(f"  ... {len(aggregates) - limit} more")


async def _main() -> None:
    from src.ingestor.base import collect

    s = get_settings()
    p = argparse.ArgumentParser(description="Run the naive processing-time baseline.")
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--group", default="contrail-naive")
    p.add_argument("--window-s", type=int, default=DEFAULT_WINDOW_S)
    p.add_argument("--duration", type=float, default=120.0)
    p.add_argument("--idle-timeout", type=float, default=10.0)
    args = p.parse_args()
    configure(s.log_level)

    events = await collect(
        args.bootstrap, args.topic, args.group, args.duration, args.idle_timeout
    )
    naive = aggregate(events, args.window_s)

    from src.windowing.aggregates import ground_truth

    truth = ground_truth(events, args.window_s)
    print_windows(naive, "NAIVE (processing-time windows)")
    print_windows(truth, "GROUND TRUTH (event-time windows)")

    wrong = sum(
        1 for k, a in truth.items() if k not in naive or naive[k].count != a.count
    )
    print(
        f"\n  events consumed {len(events):,}"
        f"\n  windows where naive disagrees with truth: {wrong} of {len(truth)}"
        f"  ({100.0 * wrong / len(truth):.1f}%)\n"
        if truth
        else "\n  no events consumed\n"
    )


if __name__ == "__main__":
    asyncio.run(_main())
