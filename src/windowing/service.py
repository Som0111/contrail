"""Live windowing service: Kafka -> watermark engine -> Redis.

Publishes each finalized window twice, for two different readers:

  * a pub/sub channel, which the WebSocket endpoint fans out to clients;
  * a hash of the latest aggregate per geographic cell, which the REST endpoint
    reads.

The API never touches Kafka. That is deliberate -- an HTTP process joining a
consumer group would take a partition assignment, so scaling the API would
silently steal work from the pipeline and rebalance it on every deploy. Redis is
the seam: the pipeline writes, the API reads, and neither can disturb the other.

Each Kafka partition is a separate watermark source (see `WatermarkProcessor`),
so a partition running ahead cannot finalize windows on behalf of one that has
not delivered yet.
"""

import argparse
import asyncio
import json
import logging

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

from src.common.config import get_settings
from src.common.logging import configure
from src.common.models import FlightState
from src.ingestor.base import ensure_topic
from src.windowing.aggregates import WindowAggregate
from src.windowing.watermark import WatermarkProcessor

log = logging.getLogger("contrail.windowing.service")

CHANNEL = "contrail.windows"
CURRENT_KEY = "contrail:windows:current"
LATE_KEY = "contrail:windows:late_total"


def encode(a: WindowAggregate) -> str:
    return json.dumps({
        "window_start": a.window_start.isoformat(),
        "partition_key": a.partition_key,
        "count": a.count,
        "avg_altitude_m": a.avg_altitude_m,
        "avg_velocity_ms": a.avg_velocity_ms,
    })


async def run(
    bootstrap: str, topic: str, group: str, redis_url: str,
    window_s: int, allowed_lateness_s: float,
    duration_s: float | None = None, poll_timeout_ms: int = 500,
    stop: asyncio.Event | None = None,
) -> int:
    """Consume, window, publish. Returns the number of windows finalized."""
    await ensure_topic(bootstrap, topic, get_settings().kafka_partitions)
    redis = aioredis.from_url(redis_url)
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=bootstrap, group_id=group,
        enable_auto_commit=True, auto_offset_reset="latest",
    )
    await consumer.start()

    pending: list[WindowAggregate] = []
    processor = WatermarkProcessor(window_s, allowed_lateness_s, on_finalize=pending.append)
    finalized = 0
    loop = asyncio.get_running_loop()
    deadline = None if duration_s is None else loop.time() + duration_s

    try:
        while not (stop and stop.is_set()) and (deadline is None or loop.time() < deadline):
            batches = await consumer.getmany(timeout_ms=poll_timeout_ms)
            for tp, records in batches.items():
                source = f"{tp.topic}:{tp.partition}"
                for r in records:
                    processor.process(FlightState.model_validate_json(r.value), source)

            if pending:
                pipe = redis.pipeline()
                for aggregate in pending:
                    payload = encode(aggregate)
                    pipe.publish(CHANNEL, payload)
                    pipe.hset(CURRENT_KEY, aggregate.partition_key, payload)
                await pipe.execute()
                finalized += len(pending)
                log.info(
                    "windows finalized",
                    extra={"count": len(pending), "total": finalized,
                           "watermark": processor.watermark},
                )
                pending.clear()
    finally:
        result = processor.close()
        if pending:  # whatever close() flushed
            pipe = redis.pipeline()
            for aggregate in pending:
                pipe.publish(CHANNEL, encode(aggregate))
                pipe.hset(CURRENT_KEY, aggregate.partition_key, encode(aggregate))
            await pipe.execute()
            finalized += len(pending)
        await redis.set(LATE_KEY, len(result.late))
        await consumer.stop()
        await redis.aclose()
        log.info("service stopped", extra={"finalized": finalized, "late": len(result.late)})
    return finalized


async def _main() -> None:
    s = get_settings()
    p = argparse.ArgumentParser(description="Live event-time windowing service.")
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--group", default="contrail-windowing")
    p.add_argument("--redis-url", default=s.redis_url)
    p.add_argument("--window-s", type=int, default=s.window_s)
    p.add_argument("--allowed-lateness-s", type=float, default=s.allowed_lateness_s)
    p.add_argument("--duration", type=float, default=None)
    args = p.parse_args()
    configure(s.log_level)
    await run(args.bootstrap, args.topic, args.group, args.redis_url,
              args.window_s, args.allowed_lateness_s, args.duration)


if __name__ == "__main__":
    asyncio.run(_main())
