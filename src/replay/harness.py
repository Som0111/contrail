"""Deterministic replay — core claim #3.

The claim: replaying a recorded stream through the windowing pipeline produces
byte-identical aggregate output, every time, including after the replay is
killed and restarted mid-flight.

Determinism here is a property of three things holding together, and all three
are load-bearing:

  1. *A total order on the input.* `collect()` sorts arrivals by
     `(ingest_time, icao24, event_time)`. Sorting on the arrival instant alone
     is not enough -- a generator tick emits a whole fleet with the same
     `ingest_time`, and a stable sort would leave those ties in whatever order
     the broker happened to interleave the partitions that run.
  2. *Order-independent-enough folding.* Aggregates are counts and means; the
     means are rounded at finalisation so that float addition, which is not
     associative, cannot leak the summation order into the output.
  3. *No wall clock anywhere in the aggregate.* Windows are keyed on
     `event_time`, the watermark advances on `event_time`, and `trace_id` is
     derived from event content. Nothing in the hashed output is stamped by the
     machine running the replay.

The mid-stream-kill test rests on a fourth point, which is the interesting one:
because the replay is a pure function of the recorded topic, a crash costs
progress but never correctness. There is no partial state to reconcile, no
checkpoint to repair -- the restarted replay re-reads from offset 0 and lands on
the same digest. That is a much stronger operational property than "the crash
was handled".

Usage:
  python -m src.replay.harness --record --events 20000   # lay down a recording
  python -m src.replay.harness --topic <t> --runs 3      # replay and hash
"""

import argparse
import asyncio
import json
import logging
import uuid
from dataclasses import asdict, dataclass
from hashlib import sha256

from src.common.config import get_settings
from src.common.logging import configure
from src.ingestor.base import collect, publish_events
from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.windowing import watermark
from src.windowing.aggregates import DEFAULT_WINDOW_S, WindowAggregate, WindowKey

log = logging.getLogger("contrail.replay")

DEFAULT_LATENESS_S = 30.0


def canonical(windows: dict[WindowKey, WindowAggregate]) -> str:
    """A stable text rendering of an aggregate set.

    Sorted, fixed-precision, one window per line. Dict iteration order and float
    repr are both things that could otherwise vary without the aggregates
    actually differing, so neither is allowed anywhere near the digest.
    """
    return "\n".join(
        f"{a.window_start.isoformat()}|{a.partition_key}|{a.count}|"
        f"{a.avg_altitude_m:.4f}|{a.avg_velocity_ms:.4f}"
        for _, a in sorted(windows.items())
    )


def digest(windows: dict[WindowKey, WindowAggregate]) -> str:
    return sha256(canonical(windows).encode()).hexdigest()


@dataclass
class ReplayResult:
    events: int
    windows: int
    late: int
    duplicates: int
    digest: str


async def record(
    bootstrap: str, topic: str, aircraft: int, ticks: int, seed: int,
    chaos: ChaosConfig | None = None,
) -> int:
    """Lay down a recording to replay against. Returns messages published."""
    source = SyntheticSource(
        n_aircraft=aircraft, rate_hz=1.0, seed=seed, duration_s=ticks,
        chaos=chaos or ChaosConfig(
            out_of_order_prob=0.25, max_skew_s=15.0, duplicate_prob=0.05,
            late_prob=0.02, late_delay_s=120.0, drop_prob=0.01,
        ),
    )
    published = await publish_events(
        list(source.simulate()), bootstrap, topic, get_settings().kafka_partitions
    )
    log.info("recorded %d messages to %s", published, topic)
    return published


async def replay(
    bootstrap: str, topic: str,
    window_s: int = DEFAULT_WINDOW_S,
    allowed_lateness_s: float = DEFAULT_LATENESS_S,
    duration_s: float = 120.0,
    idle_timeout_s: float = 4.0,
) -> ReplayResult:
    """Read the recording from offset 0 and window it. Pure function of the topic.

    A fresh consumer group every time, deliberately: a replay must not be able to
    resume from someone else's committed offsets, or it would hash a suffix of
    the stream rather than the stream.
    """
    group = f"replay-{uuid.uuid4().hex[:12]}"
    events = await collect(bootstrap, topic, group, duration_s, idle_timeout_s)
    result = watermark.aggregate(events, window_s, allowed_lateness_s)
    return ReplayResult(
        events=len(events),
        windows=len(result.windows),
        late=len(result.late),
        duplicates=result.duplicates,
        digest=digest(result.windows),
    )


async def _main() -> None:
    s = get_settings()
    p = argparse.ArgumentParser(description="Deterministic replay harness.")
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=None)
    p.add_argument("--record", action="store_true", help="publish a fresh recording first")
    p.add_argument("--aircraft", type=int, default=40)
    p.add_argument("--ticks", type=int, default=400, help="seconds of event time at 1 Hz")
    p.add_argument("--seed", type=int, default=20260827)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--window-s", type=int, default=s.window_s)
    p.add_argument("--allowed-lateness-s", type=float, default=s.allowed_lateness_s)
    p.add_argument("--json", action="store_true", help="emit only the final result as JSON")
    args = p.parse_args()
    configure("WARNING" if args.json else s.log_level)

    topic = args.topic or f"replay.{uuid.uuid4().hex[:8]}"
    if args.record:
        await record(args.bootstrap, topic, args.aircraft, args.ticks, args.seed)

    results = []
    for i in range(args.runs):
        r = await replay(args.bootstrap, topic, args.window_s, args.allowed_lateness_s)
        results.append(r)
        if not args.json:
            print(f"  run {i + 1}: {r.events:,} events -> {r.windows:,} windows, "
                  f"{r.late:,} late, {r.duplicates:,} dupes  digest {r.digest}")

    identical = len({r.digest for r in results}) == 1
    if args.json:
        print(json.dumps({"topic": topic, "identical": identical,
                          **asdict(results[-1])}))
    else:
        print(f"\n  topic {topic}")
        print(f"  {args.runs} runs, digests identical: {identical}")


if __name__ == "__main__":
    asyncio.run(_main())
