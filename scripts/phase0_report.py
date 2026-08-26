"""Phase 0 end-to-end report.

Cross-checks two independent sources: how many messages Redpanda actually holds
on the raw topic, and what ended up in TimescaleDB. The gap between them is the
work the idempotency key did, and it should reconcile exactly:

    messages on topic == rows stored + duplicates suppressed

Everything printed here is read back from live infrastructure. Nothing is
computed from what the generator *intended* to emit.
"""

import argparse
import asyncio

from aiokafka import AIOKafkaConsumer, TopicPartition

from src.common.config import get_settings
from src.storage import timescale

OUT_OF_ORDER_SQL = """
WITH by_arrival AS (
    SELECT
        event_time,
        lag(event_time) OVER (ORDER BY ingest_time, icao24) AS prev_event_time
    FROM flight_events
)
SELECT count(*) FROM by_arrival WHERE event_time < prev_event_time
"""

LATENESS_SQL = """
SELECT
    count(*)                                                    AS rows,
    avg(lateness)                                               AS mean,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY lateness)      AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY lateness)      AS p95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY lateness)      AS p99,
    max(lateness)                                               AS max,
    count(*) FILTER (WHERE lateness >= $1)                      AS beyond_bound
FROM (
    SELECT extract(epoch FROM (ingest_time - event_time)) AS lateness
    FROM flight_events
) s
"""

SPAN_SQL = """
SELECT
    min(event_time), max(event_time),
    count(DISTINCT icao24), count(DISTINCT partition_key)
FROM flight_events
"""


async def topic_message_count(bootstrap: str, topic: str) -> tuple[int, dict[int, int]]:
    """Messages currently retained on the topic, from the broker's own offsets."""
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            return 0, {}
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        begin = await consumer.beginning_offsets(tps)
        end = await consumer.end_offsets(tps)
        per_partition = {tp.partition: end[tp] - begin[tp] for tp in tps}
        return sum(per_partition.values()), per_partition
    finally:
        await consumer.stop()


async def main() -> None:
    s = get_settings()
    p = argparse.ArgumentParser(description="Phase 0 pipeline report.")
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--dsn", default=s.postgres_dsn)
    p.add_argument(
        "--lateness-bound",
        type=float,
        default=60.0,
        help="seconds; events later than this would miss a 60s window",
    )
    args = p.parse_args()

    on_topic, per_partition = await topic_message_count(args.bootstrap, args.topic)

    pool = await timescale.connect(args.dsn, min_size=1, max_size=2)
    try:
        stored = await timescale.count_events(pool)
        async with pool.acquire() as conn:
            inversions = await conn.fetchval(OUT_OF_ORDER_SQL)
            late = await conn.fetchrow(LATENESS_SQL, args.lateness_bound)
            first, last, aircraft, cells = await conn.fetchrow(SPAN_SQL)
    finally:
        await pool.close()

    suppressed = on_topic - stored
    span = (last - first).total_seconds() if first and last else 0.0

    print(f"\nCONTRAIL — Phase 0 report   (topic: {args.topic})")
    print("=" * 62)
    print("\nVolume")
    print(f"  messages received on topic     {on_topic:>10,}")
    print(f"  unique events stored           {stored:>10,}")
    print(f"  duplicates suppressed          {suppressed:>10,}"
          f"   ({pct(suppressed, on_topic)} of received)")
    print(f"  reconciles                     {'  yes' if stored + suppressed == on_topic else '   NO':>10}"
          f"   (stored + suppressed == received)")

    print("\nPartitioning")
    print(f"  kafka partitions               {len(per_partition):>10}")
    print(f"  geographic cells seen          {cells:>10,}")
    print(f"  distinct aircraft              {aircraft:>10,}")
    spread = "  ".join(f"p{k}:{v:,}" for k, v in sorted(per_partition.items()))
    print(f"  messages per partition         {spread}")

    print("\nDisorder (measured on stored rows, in arrival order)")
    print(f"  event-time inversions          {inversions:>10,}"
          f"   ({pct(inversions, stored)} of stored rows)")
    print(f"  arrival lag mean / p50         {fmt(late['mean'])} / {fmt(late['p50'])}")
    print(f"  arrival lag p95 / p99 / max    {fmt(late['p95'])} / {fmt(late['p99'])}"
          f" / {fmt(late['max'])}")
    print(f"  later than {args.lateness_bound:g}s bound          {late['beyond_bound']:>10,}"
          f"   ({pct(late['beyond_bound'], stored)} of stored rows)")

    print("\nCoverage")
    print(f"  event_time span                {span:>10,.0f} s")
    print(f"  first event                    {first}")
    print(f"  last event                     {last}")
    print()


def pct(n, total) -> str:
    return f"{(100.0 * n / total):.2f}%" if total else "n/a"


def fmt(seconds) -> str:
    return f"{float(seconds):.2f}s" if seconds is not None else "n/a"


if __name__ == "__main__":
    asyncio.run(main())
