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
import signal
from datetime import UTC, datetime

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

from src.common import metrics
from src.common.config import get_settings
from src.common.logging import configure
from src.common.models import FlightState
from src.ingestor.base import ensure_topic
from src.windowing.aggregates import WindowAggregate, window_start
from src.windowing.offsets import OffsetTracker
from src.windowing.watermark import WatermarkProcessor, min_seen_retention_windows

log = logging.getLogger("contrail.windowing.service")

_PREFIX = get_settings().redis_key_prefix
CHANNEL = f"{_PREFIX}.windows"
CURRENT_KEY = f"{_PREFIX}:windows:current"
LATE_KEY = f"{_PREFIX}:windows:late_total"
# Dedup keeps keys for this many windows past the closed frontier, never fewer than
# the lateness-derived minimum. See min_seen_retention_windows().
SEEN_RETENTION_WINDOWS = 10


# Publish only if this aggregate is not older than what Redis already holds for the
# cell. After a crash the restart replays from the last safe offset, which can hold
# only the tail of a window that was already published in full; without this guard
# that fraction would overwrite the correct aggregate. Same-window replays of equal
# or larger count still go through, so re-delivery stays idempotent.
PUBLISH_IF_NEWER = """
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local c = cjson.decode(cur)
  if c.window_start > ARGV[3] or (c.window_start == ARGV[3] and c.count > tonumber(ARGV[4])) then
    return 0
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('PUBLISH', KEYS[2], ARGV[2])
return 1
"""


async def publish(redis, aggregates: list[WindowAggregate]) -> None:
    pipe = redis.pipeline()
    for a in aggregates:
        pipe.eval(
            PUBLISH_IF_NEWER, 2, CURRENT_KEY, CHANNEL,
            a.partition_key, encode(a), a.window_start.isoformat(), a.count,
        )
    await pipe.execute()


def encode(a: WindowAggregate) -> str:
    return json.dumps({
        "window_start": a.window_start.isoformat(),
        "partition_key": a.partition_key,
        "count": a.count,
        "avg_altitude_m": a.avg_altitude_m,
        "avg_velocity_ms": a.avg_velocity_ms,
    })


async def _lag(consumer) -> int:
    total = 0
    for tp in consumer.assignment():
        high = consumer.highwater(tp)
        if high is not None:
            total += max(0, high - await consumer.position(tp))
    return total


async def report_late(redis, late_count: int, reported: int) -> int:
    """Push the late-event delta since the last call; returns the new reported total.

    LATE_KEY is a running total across restarts (INCRBY, never SET): the processor's
    own count resets with the process, so overwriting would erase history. It is
    approximate under crash recovery -- records replayed from the last safe offset
    can be counted late a second time.
    """
    delta = late_count - reported
    if delta > 0:
        await redis.incrby(LATE_KEY, delta)
        metrics.LATE_EVENTS.inc(delta)
        return late_count
    return reported


async def run(
    bootstrap: str, topic: str, group: str, redis_url: str,
    window_s: int, allowed_lateness_s: float,
    duration_s: float | None = None, poll_timeout_ms: int = 500,
    stop: asyncio.Event | None = None, idle_timeout_s: float | None = None,
    ready: asyncio.Event | None = None,
) -> int:
    """Consume, window, publish. Returns the number of windows finalized."""
    await ensure_topic(bootstrap, topic, get_settings().kafka_partitions)
    redis = aioredis.from_url(redis_url)
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=bootstrap, group_id=group,
        enable_auto_commit=False, auto_offset_reset="latest",
    )
    await consumer.start()

    # Partitions with a backlog at start (a restart resuming from committed offsets) must
    # each deliver before the watermark moves; see WatermarkProcessor(await_sources).
    assigned = list(consumer.assignment())
    ends = await consumer.end_offsets(assigned) if assigned else {}
    backlog = {f"{tp.topic}:{tp.partition}" for tp in assigned if ends[tp] > await consumer.position(tp)}
    if backlog:
        log.info("resuming with a backlog", extra={"partitions": sorted(backlog)})

    pending: list[WindowAggregate] = []
    # This process never exits, so every accumulator has to be bounded. Without
    # these three the dedup set alone reached ~7.4M entries and over a gigabyte a
    # day at the live rate; windows and late events grew alongside it.
    processor = WatermarkProcessor(
        window_s, allowed_lateness_s, on_finalize=pending.append,
        seen_retention_windows=max(
            SEEN_RETENTION_WINDOWS, min_seen_retention_windows(window_s, allowed_lateness_s)),
        late_retention=1000, retain_windows=False,
        idle_timeout_s=idle_timeout_s, await_sources=backlog,
    )
    tracker = OffsetTracker(window_s)
    committed: dict = {}
    finalized = 0
    reported_late = 0
    reported_idle = reported_resumed = 0
    reported_dups = reported_evicted = 0

    async def commit_safe() -> None:
        # Only after the publish above returned: a crash between the two replays
        # records into the same windows, which the guarded publish tolerates.
        nonlocal committed
        safe = tracker.safe()
        if safe != committed:
            await consumer.commit(safe)
            committed = safe
    loop = asyncio.get_running_loop()
    deadline = None if duration_s is None else loop.time() + duration_s

    try:
        while not (stop and stop.is_set()) and (deadline is None or loop.time() < deadline):
            batches = await consumer.getmany(timeout_ms=poll_timeout_ms)
            if ready is not None:
                ready.set()  # assigned and positioned: records published now will be seen
            metrics.WINDOWING_RECORDS.inc(sum(len(r) for r in batches.values()))
            for tp, records in batches.items():
                source = f"{tp.topic}:{tp.partition}"
                for r in records:
                    event = FlightState.model_validate_json(r.value)
                    tracker.observe(tp, r.offset, window_start(event.event_time, window_s))
                    processor.process(event, source)
            processor.tick()  # a silent partition sends nothing to trigger this
            tracker.close_before(processor.finalized_before)
            metrics.WINDOWING_LAG.set(await _lag(consumer))

            if pending:
                await publish(redis, pending)
                finalized += len(pending)
                metrics.WINDOWS_FINALIZED.inc(len(pending))
                log.info(
                    "windows finalized",
                    extra={"count": len(pending), "total": finalized,
                           "watermark": processor.watermark},
                )
                pending.clear()
            await commit_safe()

            mark = processor.watermark
            if mark is not None:
                metrics.WATERMARK_SKEW_WALLCLOCK.set(
                    (datetime.now(UTC) - mark).total_seconds()
                )
                metrics.WATERMARK_SKEW_EVENT.set(
                    (processor.max_event_time - mark).total_seconds()
                )
            metrics.DEDUP_KEYS.set(processor.seen_size)
            metrics.DEDUP_SUPPRESSED.inc(processor.duplicate_count - reported_dups)
            metrics.DEDUP_EVICTED.inc(processor.seen_evicted - reported_evicted)
            reported_dups, reported_evicted = processor.duplicate_count, processor.seen_evicted
            metrics.IDLE_PARTITIONS.set(len(processor.idle_sources))
            if processor.idle_transitions > reported_idle:
                metrics.IDLE_TRANSITIONS.labels("idle").inc(processor.idle_transitions - reported_idle)
                reported_idle = processor.idle_transitions
            if processor.resume_transitions > reported_resumed:
                metrics.IDLE_TRANSITIONS.labels("resumed").inc(
                    processor.resume_transitions - reported_resumed)
                reported_resumed = processor.resume_transitions
            reported_late = await report_late(redis, processor.late_count, reported_late)
    finally:
        # Windows finalized but not yet published go out first, and offsets are
        # committed against that state only. close() then flushes the still-open
        # windows as a best-effort preview; their offsets stay uncommitted, so the
        # next start rebuilds them in full and the guarded publish supersedes this.
        if pending:
            await publish(redis, pending)
            finalized += len(pending)
            pending.clear()
        await commit_safe()
        result = processor.close()
        if pending:
            await publish(redis, pending)
            finalized += len(pending)
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
    p.add_argument("--idle-timeout-s", type=float, default=s.idle_timeout_s,
                   help="seconds of silence before a partition stops holding back the "
                        "watermark; must exceed --allowed-lateness-s; off by default")
    p.add_argument("--metrics-port", type=int, default=s.metrics_port)
    args = p.parse_args()
    configure(s.log_level)
    if args.metrics_port:
        metrics.serve(args.metrics_port)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows event loop; containers are Linux
            pass
    await run(args.bootstrap, args.topic, args.group, args.redis_url,
              args.window_s, args.allowed_lateness_s, args.duration, stop=stop,
              idle_timeout_s=args.idle_timeout_s)


if __name__ == "__main__":
    asyncio.run(_main())
