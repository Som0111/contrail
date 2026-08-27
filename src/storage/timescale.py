"""TimescaleDB schema and writes for raw flight events.

The idempotency guarantee lives here, in the database, not in the consumer:
`UNIQUE (icao24, event_time)` plus `ON CONFLICT DO NOTHING` means re-delivering
an event is a no-op no matter how it got re-delivered. That is what lets the
consumer be plain at-least-once (commit offsets after the write) and still be
safe to kill at any point.
"""

import logging
from hashlib import blake2s

import asyncpg

from src.common.models import FlightState

log = logging.getLogger("contrail.storage")

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS flight_events (
    event_time    TIMESTAMPTZ      NOT NULL,
    icao24        TEXT             NOT NULL,
    callsign      TEXT             NOT NULL,
    lat           DOUBLE PRECISION NOT NULL,
    lon           DOUBLE PRECISION NOT NULL,
    altitude_m    DOUBLE PRECISION NOT NULL,
    velocity_ms   DOUBLE PRECISION NOT NULL,
    heading       DOUBLE PRECISION NOT NULL,
    ingest_time   TIMESTAMPTZ      NOT NULL,
    partition_key TEXT             NOT NULL,
    trace_id      TEXT             NOT NULL,
    -- Stamped by the database at write time, so end-to-end latency
    -- (event_time -> durably stored) is a property of the row, not of whatever
    -- clock the benchmark script happens to read.
    processed_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    -- Idempotency key. A hypertable's unique index must include the
    -- partitioning column, which event_time already is.
    UNIQUE (icao24, event_time)
);

ALTER TABLE flight_events
    ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NOT NULL DEFAULT now();

SELECT create_hypertable('flight_events', 'event_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS flight_events_partition_idx
    ON flight_events (partition_key, event_time DESC);
"""

INSERT = """
INSERT INTO flight_events (
    event_time, icao24, callsign, lat, lon, altitude_m,
    velocity_ms, heading, ingest_time, partition_key, trace_id
)
SELECT * FROM unnest(
    $1::timestamptz[], $2::text[], $3::text[], $4::float8[], $5::float8[],
    $6::float8[], $7::float8[], $8::float8[], $9::timestamptz[], $10::text[],
    $11::text[]
)
ON CONFLICT (icao24, event_time) DO NOTHING
"""


def trace_id(event: FlightState) -> str:
    """Deterministic per-event correlation id.

    Derived from the event's identity rather than minted at random so that a
    replay produces the same ids as the original run (Phase 1.6 needs that), and
    so two copies of a duplicated event are visibly the same event in the logs.
    """
    key = f"{event.icao24}|{event.event_time.isoformat()}"
    return blake2s(key.encode(), digest_size=8).hexdigest()


async def connect(dsn: str, **kwargs) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, **kwargs)


async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
    log.info("schema ready", extra={"table": "flight_events"})


async def insert_events(pool: asyncpg.Pool, events: list[FlightState]) -> int:
    """Write a batch, returning how many rows were actually new.

    Anything not returned was suppressed by the idempotency key -- either a
    duplicate inside this batch or a redelivery from an earlier one.
    """
    if not events:
        return 0
    cols = list(
        zip(
            *[
                (
                    e.event_time,
                    e.icao24,
                    e.callsign,
                    e.lat,
                    e.lon,
                    e.altitude_m,
                    e.velocity_ms,
                    e.heading,
                    e.ingest_time,
                    e.partition_key,
                    trace_id(e),
                )
                for e in events
            ]
        )
    )
    async with pool.acquire() as conn:
        status = await conn.execute(INSERT, *[list(c) for c in cols])
    # asyncpg returns the command tag, e.g. "INSERT 0 42".
    return int(status.rsplit(" ", 1)[1])


async def count_events(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT count(*) FROM flight_events")


async def truncate(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE flight_events")
