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
from tests.helpers import unique, wait_for

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
    async def mid_stream():
        return 0 < await count_ours(pool) < unique

    async def state():
        return {"rows": await count_ours(pool), "unique_events": unique, "topic": topic}

    await wait_for(mid_stream, timeout=20, interval=0.05, what="the sink to be mid-stream",
                   state=state)

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


# --- schema initialisation (needs the compose TimescaleDB) -----------------------

async def test_ensure_schema_is_idempotent_and_safe_to_race():
    pool = await timescale.connect(SETTINGS.postgres_dsn, min_size=1, max_size=8)
    try:
        await asyncio.gather(*[timescale.ensure_schema(pool) for _ in range(8)])
        await timescale.ensure_schema(pool)
        async with pool.acquire() as conn:
            tables = {r["tablename"] for r in await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE tablename IN "
                "('flight_events', 'flight_events_dead')")}
            hypertables = await conn.fetchval(
                "SELECT count(*) FROM timescaledb_information.hypertables "
                "WHERE hypertable_name = 'flight_events'")
        assert tables == {"flight_events", "flight_events_dead"} and hypertables == 1
    finally:
        await pool.close()


async def test_ensure_schema_upgrades_a_table_missing_processed_at_and_dead_letter_table():
    """A scratch database standing in for an install from before those existed."""
    import asyncpg

    admin = await asyncpg.connect(SETTINGS.postgres_dsn)
    name = f"upgrade_{uuid.uuid4().hex[:8]}"
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        dsn = SETTINGS.postgres_dsn.rsplit("/", 1)[0] + f"/{name}"
        old = await asyncpg.connect(dsn)
        await old.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        await old.execute("""
            CREATE TABLE flight_events (
              event_time TIMESTAMPTZ NOT NULL, icao24 TEXT NOT NULL, callsign TEXT NOT NULL,
              lat DOUBLE PRECISION NOT NULL, lon DOUBLE PRECISION NOT NULL,
              altitude_m DOUBLE PRECISION NOT NULL, velocity_ms DOUBLE PRECISION NOT NULL,
              heading DOUBLE PRECISION NOT NULL, ingest_time TIMESTAMPTZ NOT NULL,
              partition_key TEXT NOT NULL, trace_id TEXT NOT NULL,
              UNIQUE (icao24, event_time))""")
        # An install from before processed_at existed already had a hypertable.
        await old.execute("SELECT create_hypertable('flight_events', 'event_time')")
        await old.execute("INSERT INTO flight_events VALUES (now(),'abc123','X',1,1,1,1,1,now(),'0:0','t')")
        await old.close()

        pool = await timescale.connect(dsn, min_size=1, max_size=2)
        try:
            await timescale.ensure_schema(pool)
            async with pool.acquire() as conn:
                assert await conn.fetchval("SELECT count(processed_at) FROM flight_events") == 1
                assert await conn.fetchval("SELECT count(*) FROM flight_events_dead") == 0
        finally:
            await pool.close()
    finally:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.close()


# --- poison records ----------------------------------------------------------------

async def test_poison_records_are_quarantined_and_the_rest_of_the_batch_lands(pool):
    """A record that cannot become an event must not wedge the consumer or lose its neighbours."""
    events = make_events(n_aircraft=10, ticks=20)
    unique_events = len({e.dedup_key for e in events})
    topic = unique("test.poison")

    bad_json = b"this is not json"
    bad_contract = events[0].model_copy(update={"altitude_m": -9999.0}).model_dump_json().encode()

    await ensure_topic(SETTINGS.kafka_bootstrap, topic, 1)  # one partition: offsets are predictable
    producer = AIOKafkaProducer(bootstrap_servers=SETTINGS.kafka_bootstrap, acks="all")
    await producer.start()
    try:
        for i, e in enumerate(events):
            if i == 3:
                await producer.send(topic, value=bad_json)
            if i == 7:
                await producer.send(topic, value=bad_contract)
            await producer.send(topic, value=e.model_dump_json().encode())
    finally:
        await producer.stop()

    group = unique("g-poison")
    first = await drain(topic, group)
    assert first.quarantined == 2
    assert await count_ours(pool) == unique_events, "every valid record must still be stored"

    async with pool.acquire() as conn:
        dead = await conn.fetch(
            "SELECT kafka_off, payload FROM flight_events_dead WHERE topic = $1 ORDER BY kafka_off", topic)
    assert [bytes(r["payload"]) for r in dead] == [bad_json, bad_contract]

    # Same group again: the poison was committed past, so nothing is re-read or re-quarantined.
    again = await drain(topic, group)
    assert again.consumed == 0 and again.quarantined == 0

    # A brand-new group re-reads it all; the dead-letter key makes that idempotent too.
    third = await drain(topic, unique("g-poison"))
    assert third.quarantined == 2
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM flight_events_dead WHERE topic = $1", topic) == 2
        await conn.execute("DELETE FROM flight_events_dead WHERE topic = $1", topic)
