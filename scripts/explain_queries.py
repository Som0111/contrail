"""EXPLAIN (ANALYZE, BUFFERS) for the queries the API runs against flight_events.

The checklist asked for evidence that `flight_events_partition_idx` serves a real
query. Reading src/ shows no query filters on partition_key at all, and only
/api/status touches the table (`count(*)`, `max(event_time)`). This prints the real
plans so that claim can be checked, or refuted, against your data:

    python -m scripts.explain_queries > explain.txt

Run it against a populated database; plans on an empty table prove nothing.
"""

import asyncio

from src.common.config import get_settings
from src.storage import timescale

QUERIES = {
    "/api/status: row count": "SELECT count(*) FROM flight_events",
    "/api/status: latest event": "SELECT max(event_time) FROM flight_events",
    # Not run by the service today; the query shape the partition index is built for.
    "per-cell recent events (partition_key, event_time DESC)": (
        "SELECT * FROM flight_events WHERE partition_key = '10:2' "
        "ORDER BY event_time DESC LIMIT 100"
    ),
}


async def main() -> None:
    pool = await timescale.connect(get_settings().postgres_dsn, min_size=1, max_size=1)
    try:
        async with pool.acquire() as conn:
            print(await conn.fetchval("SELECT version()"))
            print("rows:", await conn.fetchval("SELECT count(*) FROM flight_events"), "\n")
            for name, sql in QUERIES.items():
                print(f"=== {name}\n{sql}")
                for row in await conn.fetch("EXPLAIN (ANALYZE, BUFFERS) " + sql):
                    print(row[0])
                print()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
