# DESIGN DECISIONS — Contrail

Non-obvious choices, the alternative considered, and why this one won.

## 0.1 — `/healthz` probes the real client path, not a TCP dial
A liveness check that only opens a socket proves the port is bound, not that the dependency is
usable by the code that matters. Alternative: cheap TCP connect per dependency, which is faster and
never fails for auth or extension reasons. Rejected because the failure mode we actually care about
in later phases is "Postgres is up but TimescaleDB isn't loaded" or "broker is up but metadata
fetches hang" — both return green on a TCP dial. So each probe uses the same client library the
pipeline will use (aiokafka `list_topics`, asyncpg querying `pg_extension` for `timescaledb`,
redis `PING`), runs concurrently, and is bounded by a 3s timeout so a hung dependency degrades the
endpoint instead of hanging it.

## 0.1 — `/healthz` returns 503 when a dependency is down
Alternative: always 200 with a per-dependency body, letting the caller decide. Rejected because
Docker Compose healthchecks and any future orchestrator read the status code, not the body. The
body still carries per-dependency detail for humans; the status code carries the aggregate verdict.
