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

from src.common.config import get_settings
from src.common.models import FlightState

log = logging.getLogger("contrail.storage")

# One statement per entry, run in order inside a single transaction by ensure_schema().
# Every step is idempotent, so it is safe on a fresh database, on one created by an
# older version (no processed_at, no dead-letter table) and when re-run.
SCHEMA_STEPS = [
    """
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
    )
    """,
    # For tables created before processed_at existed; a no-op on a fresh one.
    "ALTER TABLE flight_events ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "SELECT create_hypertable('flight_events', 'event_time', if_not_exists => TRUE)",
    # NOTE: no query in src/ filters on partition_key, so nothing currently reads this
    # index; see scripts/explain_queries.py before relying on it or dropping it.
    """
    CREATE INDEX IF NOT EXISTS flight_events_partition_idx
        ON flight_events (partition_key, event_time DESC)
    """,
    # Records the sink could not parse or the database permanently rejected. Keyed by
    # Kafka position so a replayed batch quarantines the same record only once.
    """
    CREATE TABLE IF NOT EXISTS flight_events_dead (
        topic       TEXT        NOT NULL,
        kafka_part  INT         NOT NULL,
        kafka_off   BIGINT      NOT NULL,
        payload     BYTEA,
        error       TEXT        NOT NULL,
        failed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (topic, kafka_part, kafka_off)
    )
    """,
]

# Arbitrary constant naming "contrail schema init" for pg_advisory_xact_lock.
SCHEMA_LOCK_ID = 7_251_094_113

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


async def ensure_schema(pool: asyncpg.Pool, retention_days: int | None = None) -> None:
    """Create or upgrade the schema. Safe to call from several processes at once.

    Runs in one transaction behind a transaction-scoped advisory lock: concurrent
    starters (sink workers, benchmark scripts) queue up instead of racing through
    `create_hypertable`, and the lock is released by commit or rollback, so a crashed
    initialiser can never leave it held.

    `retention_days` (default: the RETENTION_DAYS setting, itself off by default) adds a
    TimescaleDB policy that drops chunks older than that. Left unset, nothing is ever
    dropped and nothing already configured is touched. To change or remove by hand:
        SELECT remove_retention_policy('flight_events');
        SELECT add_retention_policy('flight_events', INTERVAL '30 days');
    """
    if retention_days is None:
        retention_days = get_settings().retention_days
    async with pool.acquire() as conn:
        # TimescaleDB wants this as the first command of a session, so it goes before
        # the transaction and the lock. Two starters can still race on a database that
        # has never had the extension; the loser's error is the extension already
        # existing, which is the state we wanted.
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        except (asyncpg.UniqueViolationError, asyncpg.DuplicateObjectError):
            pass
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)", SCHEMA_LOCK_ID)
        for step in SCHEMA_STEPS:
            await conn.execute(step)
        if retention_days is not None:
            await conn.execute("SELECT remove_retention_policy('flight_events', if_exists => TRUE)")
            await conn.execute(
                "SELECT add_retention_policy('flight_events', make_interval(days => $1))",
                retention_days,
            )
    log.info("schema ready", extra={"table": "flight_events", "retention_days": retention_days})


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


async def quarantine(
    pool: asyncpg.Pool, rows: list[tuple[str, int, int, bytes | None, str]]
) -> None:
    """Park (topic, partition, offset, payload, error) rows in the dead-letter table."""
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO flight_events_dead (topic, kafka_part, kafka_off, payload, error)"
            " VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
            rows,
        )


async def count_events(pool: asyncpg.Pool, icao24s: list[str] | None = None) -> int:
    """Count rows, optionally scoped to a set of aircraft.

    The scoped form exists because the demo pipeline runs continuously as a
    compose service, so any test or benchmark that counts the whole table is
    counting someone else's traffic too. Scoping to the aircraft you generated
    makes a test independent of whatever else is writing.
    """
    async with pool.acquire() as conn:
        if icao24s is None:
            return await conn.fetchval("SELECT count(*) FROM flight_events")
        return await conn.fetchval(
            "SELECT count(*) FROM flight_events WHERE icao24 = ANY($1::text[])",
            list(icao24s),
        )


async def delete_events(pool: asyncpg.Pool, icao24s: list[str]) -> int:
    """Remove only this caller's rows. Preferred over truncate() for isolation."""
    async with pool.acquire() as conn:
        status = await conn.execute(
            "DELETE FROM flight_events WHERE icao24 = ANY($1::text[])", list(icao24s)
        )
    return int(status.rsplit(" ", 1)[1])


async def truncate(pool: asyncpg.Pool) -> None:
    """Wipe the whole table. Only safe when nothing else is writing."""
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE flight_events")
