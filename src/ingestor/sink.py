"""Kafka -> TimescaleDB sink.

At-least-once by construction: offsets are committed only after the batch is
durably written. That means a crash between write and commit replays the batch,
which is safe purely because the write is idempotent (see storage/timescale.py).
No exactly-once machinery, no dedup cache in the consumer.
"""

import argparse
import asyncio
import logging
import signal
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer

from src.common.config import get_settings
from src.ingestor.base import ensure_topic
from src.common.logging import configure
from src.common.models import FlightState
from src.storage import timescale

log = logging.getLogger("contrail.ingestor.sink")


@dataclass
class SinkStats:
    consumed: int = 0
    inserted: int = 0
    suppressed: int = 0
    batches: int = 0


async def run(
    bootstrap: str,
    topic: str,
    group: str,
    dsn: str,
    batch_size: int = 500,
    poll_timeout_ms: int = 1000,
    idle_timeout_s: float | None = None,
    partitions: int | None = None,
    stop: asyncio.Event | None = None,
) -> SinkStats:
    # Subscribing to a missing topic auto-creates it with ONE partition, which
    # would silently cap parallelism. Claim the right shape regardless of whether
    # the consumer or the producer starts first.
    await ensure_topic(bootstrap, topic, partitions or get_settings().kafka_partitions)
    pool = await timescale.connect(dsn, min_size=1, max_size=4)
    await timescale.ensure_schema(pool)

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        enable_auto_commit=False,  # the whole point: we commit after the write
        auto_offset_reset="earliest",
        max_poll_records=batch_size,
    )
    await consumer.start()
    stats = SinkStats()
    idle_for = 0.0

    try:
        while not (stop and stop.is_set()):
            batches = await consumer.getmany(timeout_ms=poll_timeout_ms)
            records = [r for rs in batches.values() for r in rs]

            if not records:
                idle_for += poll_timeout_ms / 1000.0
                if idle_timeout_s is not None and idle_for >= idle_timeout_s:
                    log.info("idle, stopping", extra={"idle_s": idle_for})
                    break
                continue
            idle_for = 0.0

            events = [FlightState.model_validate_json(r.value) for r in records]
            inserted = await timescale.insert_events(pool, events)
            await consumer.commit()

            stats.consumed += len(events)
            stats.inserted += inserted
            stats.suppressed += len(events) - inserted
            stats.batches += 1
            log.info(
                "batch written",
                extra={
                    "consumed": len(events),
                    "inserted": inserted,
                    "suppressed": len(events) - inserted,
                    "first_trace_id": timescale.trace_id(events[0]),
                    "offsets": {
                        f"{tp.topic}:{tp.partition}": (rs[0].offset, rs[-1].offset)
                        for tp, rs in batches.items()
                        if rs
                    },
                },
            )
    finally:
        await consumer.stop()
        await pool.close()
        log.info(
            "sink stopped",
            extra={
                "consumed": stats.consumed,
                "inserted": stats.inserted,
                "suppressed": stats.suppressed,
                "batches": stats.batches,
            },
        )
    return stats


def _parse_args(argv=None) -> argparse.Namespace:
    s = get_settings()
    p = argparse.ArgumentParser(description="Consume flight events into TimescaleDB.")
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--group", default=s.kafka_consumer_group)
    p.add_argument("--dsn", default=s.postgres_dsn)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--partitions", type=int, default=s.kafka_partitions)
    p.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="exit after this many seconds with no records; omit to run forever",
    )
    return p.parse_args(argv)


async def _main(argv=None) -> None:
    args = _parse_args(argv)
    configure(get_settings().log_level)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await run(
        bootstrap=args.bootstrap,
        topic=args.topic,
        group=args.group,
        dsn=args.dsn,
        batch_size=args.batch_size,
        idle_timeout_s=args.idle_timeout,
        partitions=args.partitions,
        stop=stop,
    )


if __name__ == "__main__":
    asyncio.run(_main())
