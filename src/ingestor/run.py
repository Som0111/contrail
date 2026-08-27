"""Single ingestion entry point. `SOURCE` picks the feed; nothing else changes.

Both sources satisfy the `EventSource` protocol defined in `ingestor/base.py`,
so this module is the only place in the codebase that knows more than one exists.
Downstream -- publisher, sink, windowing, control loop, API -- is identical
either way.
"""

import argparse
import asyncio
import logging

from src.common.config import get_settings
from src.common.logging import configure
from src.ingestor.base import EventSource, publish
from src.ingestor.opensky import OpenSkySource, bbox_from
from src.ingestor.synthetic import ChaosConfig, SyntheticSource

log = logging.getLogger("contrail.ingestor.run")


def build_source(args) -> EventSource:
    s = get_settings()
    if args.source == "opensky":
        return OpenSkySource(
            poll_interval_s=args.poll_interval,
            bbox=bbox_from(args.bbox),
            duration_s=args.duration,
        )
    if args.source == "synthetic":
        return SyntheticSource(
            n_aircraft=args.aircraft, rate_hz=args.rate, seed=args.seed,
            duration_s=args.duration, chaos=ChaosConfig.from_settings(s),
        )
    raise SystemExit(f"unknown SOURCE {args.source!r}: expected 'synthetic' or 'opensky'")


def _parse_args(argv=None) -> argparse.Namespace:
    s = get_settings()
    p = argparse.ArgumentParser(description="Publish flight telemetry from the configured source.")
    p.add_argument("--source", default=s.source, choices=["synthetic", "opensky"])
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--partitions", type=int, default=s.kafka_partitions)
    p.add_argument("--duration", type=float, default=None)
    # synthetic
    p.add_argument("--aircraft", type=int, default=s.gen_aircraft)
    p.add_argument("--rate", type=float, default=s.gen_rate_hz)
    p.add_argument("--seed", type=int, default=s.gen_seed)
    # opensky
    p.add_argument("--poll-interval", type=float, default=s.opensky_poll_interval_s)
    p.add_argument("--bbox", default=s.opensky_bbox)
    return p.parse_args(argv)


async def _main(argv=None) -> None:
    args = _parse_args(argv)
    configure(get_settings().log_level)
    source = build_source(args)
    log.info("ingestion starting", extra={"source": source.name, "topic": args.topic})
    await publish(source, args.bootstrap, args.topic, args.partitions)


if __name__ == "__main__":
    asyncio.run(_main())
