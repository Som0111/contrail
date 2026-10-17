"""Kafka -> TimescaleDB sink.

At-least-once by construction: offsets are committed only after the batch is
durably written. That means a crash between write and commit replays the batch,
which is safe purely because the write is idempotent (see storage/timescale.py).
No exactly-once machinery, no dedup cache in the consumer.
"""

import argparse
import asyncio
import logging
import random
import signal
from datetime import UTC, datetime
from collections.abc import Callable
from dataclasses import dataclass

import asyncpg
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from pydantic import ValidationError

from src.common.config import get_settings
from src.ingestor.base import ensure_topic
from src.common.logging import configure
from src.common import metrics
from src.common.models import FlightState
from src.storage import timescale

log = logging.getLogger("contrail.ingestor.sink")


@dataclass
class SinkStats:
    consumed: int = 0
    inserted: int = 0
    suppressed: int = 0
    shed: int = 0
    batches: int = 0
    quarantined: int = 0


# Worth retrying: the database may well be back in a moment.
TRANSIENT = (
    OSError, TimeoutError, asyncpg.PostgresConnectionError,
    asyncpg.InsufficientResourcesError, asyncpg.CannotConnectNowError,
)
MAX_ATTEMPTS = 5
BACKOFF_BASE_S = 0.2


class RetriesExhausted(Exception):
    """Every attempt failed transiently. Not a verdict on the data, so never quarantined."""


async def _write(pool, events: list[FlightState]) -> int:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return await timescale.insert_events(pool, events)
        except TRANSIENT as exc:
            if attempt == MAX_ATTEMPTS:
                metrics.SINK_RETRIES_EXHAUSTED.inc()
                raise RetriesExhausted(str(exc)) from exc
            metrics.SINK_RETRIES.inc()
            delay = BACKOFF_BASE_S * 2 ** (attempt - 1) * random.uniform(0.5, 1.5)
            log.warning("db write failed, retrying",
                        extra={"attempt": attempt, "delay_s": round(delay, 2), "error": repr(exc)})
            await asyncio.sleep(delay)


async def report_lag(consumer) -> None:
    """Per-partition lag as this consumer sees it: last fetched high watermark - position."""
    for tp in consumer.assignment():
        high = consumer.highwater(tp)
        if high is not None:
            lag = max(0, high - await consumer.position(tp))
            metrics.SINK_PARTITION_LAG.labels(str(tp.partition)).set(lag)


async def write_isolating(pool, pairs: list[tuple]) -> tuple[int, list[tuple]]:
    """Insert (record, event) pairs; return (rows inserted, [(record, error)] rejected).

    A permanent database error is bisected down to the record causing it, so one
    poison record costs O(log n) extra writes instead of the whole batch.
    """
    if not pairs:
        return 0, []
    try:
        return await _write(pool, [e for _, e in pairs]), []
    except (asyncpg.PostgresError, asyncpg.DataError) as exc:
        if len(pairs) == 1:
            return 0, [(pairs[0][0], repr(exc))]
    mid = len(pairs) // 2
    n1, bad1 = await write_isolating(pool, pairs[:mid])
    n2, bad2 = await write_isolating(pool, pairs[mid:])
    return n1 + n2, bad1 + bad2


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
    event_filter: Callable[[FlightState], bool] | None = None,
    max_rate: float | None = None,
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

            rejected: list[tuple] = []  # (record, error) for the dead-letter table
            parsed: list[tuple] = []
            for r in records:
                try:
                    parsed.append((r, FlightState.model_validate_json(r.value)))
                except ValidationError as exc:
                    rejected.append((r, f"invalid event: {exc.error_count()} validation errors"))
            consumed = [e for _, e in parsed]
            events = consumed
            shed = 0
            if event_filter is not None:
                parsed = [(r, e) for r, e in parsed if event_filter(e)]
                events = [e for _, e in parsed]
                shed = len(consumed) - len(events)

            if max_rate:
                # ponytail: per-worker throughput ceiling so worker count is the
                # real capacity knob; without it one batched writer out-runs any
                # generator this machine can produce and scaling proves nothing.
                #
                # Charged on `events`, not `consumed`: the ceiling stands in for
                # per-worker *write* capacity, and a shed record is never written.
                # Billing for discarded records made shedding structurally unable
                # to buy back any throughput, so it could relieve lag only by
                # coincidence -- which is exactly how it behaved before this was
                # fixed. Deserialising and filtering a record is not free, but it
                # is far cheaper than the insert this models.
                await asyncio.sleep(len(events) / max_rate)
            inserted, bad = await write_isolating(pool, parsed)
            rejected += bad
            bad_ids = {id(r) for r, _ in bad}
            events = [e for r, e in parsed if id(r) not in bad_ids]
            if rejected:
                await timescale.quarantine(pool, [
                    (r.topic, r.partition, r.offset, r.value, err) for r, err in rejected
                ])
                metrics.EVENTS.labels("quarantined").inc(len(rejected))
                stats.quarantined += len(rejected)
                log.error("records quarantined", extra={
                    "count": len(rejected),
                    "offsets": [f"{r.partition}:{r.offset}" for r, _ in rejected[:10]],
                })
            # Shed and quarantined records are committed too: both were consumed and
            # deliberately set aside, so replaying them would undo that decision.
            try:
                await consumer.commit()
            except KafkaError as exc:
                # The write is durable; an uncommitted batch just replays as a no-op.
                metrics.SINK_COMMIT_FAILURES.inc()
                log.warning("offset commit failed", extra={"error": repr(exc)})

            await report_lag(consumer)
            now = datetime.now(UTC)
            for e in events:
                metrics.E2E_LATENCY.observe((now - e.event_time).total_seconds())
            metrics.EVENTS.labels("inserted").inc(inserted)
            metrics.EVENTS.labels("suppressed").inc(len(events) - inserted)
            if shed:
                metrics.EVENTS.labels("shed").inc(shed)
                metrics.SHED_EVENTS.inc(shed)

            stats.consumed += len(consumed)
            stats.inserted += inserted
            stats.suppressed += len(events) - inserted
            stats.shed += shed
            stats.batches += 1
            log.info(
                "batch written",
                extra={
                    "consumed": len(consumed),
                    "inserted": inserted,
                    "suppressed": len(events) - inserted,
                    "shed": shed,
                    "first_trace_id": timescale.trace_id(consumed[0]) if consumed else None,
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
                "shed": stats.shed,
                "batches": stats.batches,
                "quarantined": stats.quarantined,
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
    p.add_argument("--metrics-port", type=int, default=s.metrics_port)
    p.add_argument("--max-rate", type=float, default=None, help="per-worker events/s cap")
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
    if args.metrics_port:
        metrics.serve(args.metrics_port)

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
        max_rate=args.max_rate,
    )


if __name__ == "__main__":
    asyncio.run(_main())
