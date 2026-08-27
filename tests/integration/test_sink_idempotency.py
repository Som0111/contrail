"""Proof that re-processing the same events is a no-op.

This is the first evidence for core claim #3 (determinism). Both tests run
against live Redpanda + TimescaleDB, because the guarantee being tested is a
property of the database constraint and the offset-commit ordering -- mocking
either one would test nothing.
"""

import asyncio
import os
import sys
import uuid
from collections import Counter
from datetime import UTC, datetime

import pytest
from aiokafka import AIOKafkaProducer

from src.common.config import get_settings
from src.ingestor.base import ensure_topic
from src.ingestor.sink import run
from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.storage import timescale

SETTINGS = get_settings()
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _fleet(n_aircraft=30, seed=4242) -> list[str]:
    """The icao24 values this module owns, so its assertions can be scoped."""
    src = SyntheticSource(n_aircraft=n_aircraft, rate_hz=1.0, seed=seed,
                          duration_s=1, start_time=T0)
    return [a.icao24 for a in src.fleet]


def make_events(n_aircraft=25, ticks=40, seed=4242):
    """A fixed batch with real duplicates in it, so suppression has work to do."""
    source = SyntheticSource(
        n_aircraft=n_aircraft,
        rate_hz=1.0,
        chaos=ChaosConfig(out_of_order_prob=0.2, duplicate_prob=0.1, drop_prob=0.05),
        seed=seed,
        duration_s=ticks,
        start_time=T0,
    )
    return list(source.simulate())


OUR_AIRCRAFT = _fleet()


async def produce(topic, events, partitions=3):
    await ensure_topic(SETTINGS.kafka_bootstrap, topic, partitions)
    producer = AIOKafkaProducer(bootstrap_servers=SETTINGS.kafka_bootstrap, acks="all")
    await producer.start()
    try:
        for e in events:
            await producer.send(
                topic, value=e.model_dump_json().encode(), key=e.partition_key.encode()
            )
    finally:
        await producer.stop()


@pytest.fixture
async def pool():
    """Scoped to this module's aircraft, never TRUNCATE.

    The demo pipeline runs continuously as a compose service and writes to the
    same table, so a test that truncates and then counts every row is counting
    the demo's traffic as its own -- which is exactly how these two tests started
    failing once `pipeline` became a compose service.
    """
    p = await timescale.connect(SETTINGS.postgres_dsn, min_size=1, max_size=4)
    await timescale.ensure_schema(p)
    await timescale.delete_events(p, OUR_AIRCRAFT)
    yield p
    await timescale.delete_events(p, OUR_AIRCRAFT)
    await p.close()


async def count_ours(pool) -> int:
    return await timescale.count_events(pool, OUR_AIRCRAFT)


def drain(topic, group):
    return run(
        bootstrap=SETTINGS.kafka_bootstrap,
        topic=topic,
        group=group,
        dsn=SETTINGS.postgres_dsn,
        batch_size=200,
        poll_timeout_ms=500,
        idle_timeout_s=2.0,
    )


async def test_replaying_the_same_batch_twice_changes_nothing(pool):
    events = make_events()
    unique = len({e.dedup_key for e in events})
    assert unique < len(events), "test batch must actually contain duplicates"

    topic = f"test.replay.{uuid.uuid4().hex[:8]}"
    await produce(topic, events)

    first = await drain(topic, f"g1-{uuid.uuid4().hex[:8]}")
    after_first = await count_ours(pool)

    # Second pass: a brand-new consumer group re-reads the same topic from offset 0.
    second = await drain(topic, f"g2-{uuid.uuid4().hex[:8]}")
    after_second = await count_ours(pool)

    assert first.consumed == len(events)
    assert after_first == unique, "row count must equal unique events, not messages"
    assert second.consumed == len(events), "second pass must really re-read everything"
    assert second.inserted == 0, "nothing in the second pass is new"
    assert after_second == after_first, "replay must not add a single row"
    assert first.inserted + first.suppressed == first.consumed


async def test_consumer_killed_mid_run_leaves_no_duplicates(pool):
    """SIGKILL the sink process, restart it, assert the table is exactly right.

    Killing a real process (not cancelling a task) is the point: it can die
    between the write and the offset commit, which is precisely the window
    at-least-once delivery exposes.
    """
    events = make_events()
    unique = len({e.dedup_key for e in events})
    topic = f"test.kill.{uuid.uuid4().hex[:8]}"
    group = f"g-kill-{uuid.uuid4().hex[:8]}"
    await produce(topic, events)

    env = {**os.environ, "PYTHONPATH": "/app"}
    cmd = [
        sys.executable, "-m", "src.ingestor.sink",
        "--topic", topic, "--group", group,
        "--batch-size", "50", "--idle-timeout", "2",
    ]
    proc = await asyncio.create_subprocess_exec(*cmd, cwd="/app", env=env)

    # Wait until it has committed real work, then kill it hard mid-stream.
    written = 0
    for _ in range(100):
        await asyncio.sleep(0.2)
        written = await count_ours(pool)
        if 0 < written < unique:
            break
    assert 0 < written < unique, f"kill must land mid-stream, saw {written}/{unique} rows"

    proc.kill()
    await proc.wait()
    assert proc.returncode != 0, "process must have been killed, not exited cleanly"
    killed_at = await count_ours(pool)

    # Restart with the SAME group: it resumes from the last committed offset and
    # re-delivers anything that was written but not committed.
    restarted = await drain(topic, group)
    final = await count_ours(pool)

    print(
        f"\n  killed after {killed_at}/{unique} rows; restart re-read "
        f"{restarted.consumed} records, {restarted.suppressed} suppressed as "
        f"already-written, {restarted.inserted} new"
    )
    assert final == unique, f"expected {unique} unique rows, got {final}"
    assert restarted.consumed >= unique - killed_at, "restart must re-read the remaining tail"

    async with pool.acquire() as conn:
        dupes = await conn.fetchval(
            "SELECT count(*) FROM ("
            "  SELECT icao24, event_time FROM flight_events"
            "  WHERE icao24 = ANY($1::text[])"
            "  GROUP BY icao24, event_time HAVING count(*) > 1"
            ") d", OUR_AIRCRAFT
        )
    assert dupes == 0, "unique constraint must have held across the restart"


async def test_trace_id_is_stable_across_runs():
    events = make_events(n_aircraft=5, ticks=5)
    ids = [timescale.trace_id(e) for e in events]
    assert ids == [timescale.trace_id(e) for e in events]
    # Duplicated copies of one event share an id -- that is what makes them
    # visibly the same event in the logs.
    by_key = Counter(e.dedup_key for e in events)
    dup_key = next(k for k, c in by_key.items() if c > 1)
    dup_ids = {timescale.trace_id(e) for e in events if e.dedup_key == dup_key}
    assert len(dup_ids) == 1


async def test_collect_returns_events_in_recorded_arrival_order():
    """Regression: `getmany()` hands back per-partition batches.

    Concatenating them interleaves partitions in arbitrary chunks, which silently
    wrecks any single global watermark downstream -- it was reporting 33x too many
    late events before `collect()` restored arrival order.
    """
    from src.ingestor.base import collect

    events = make_events(n_aircraft=30, ticks=60)
    topic = f"test.order.{uuid.uuid4().hex[:8]}"
    await produce(topic, events, partitions=6)

    got = await collect(
        SETTINGS.kafka_bootstrap, topic, f"g-order-{uuid.uuid4().hex[:8]}",
        duration_s=30.0, idle_timeout_s=2.0,
    )
    assert len(got) == len(events)
    arrivals = [e.ingest_time for e in got]
    assert arrivals == sorted(arrivals), "collect() must restore recorded arrival order"
    assert {e.dedup_key for e in got} == {e.dedup_key for e in events}
