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

## 0.2 — Partition by a lat/lon grid cell, not H3
Kafka needs a key that keeps related events co-located and spreads load. Alternative considered:
H3 at low resolution, which is the "proper" geospatial answer and gives equal-area cells. Rejected
for now because it adds a compiled dependency and an index-encoding step for a property this project
never uses — we do not do neighbour lookups, k-rings, or hierarchical rollups anywhere in the
roadmap. A 5-degree lat/lon cell (`floor(lat/5):floor(lon/5)`) gives what we actually need: a stable
key an aircraft stays inside for many minutes, and enough distinct values to spread across
partitions (a 40-aircraft run filled all 6 partitions). The cost is unequal cell areas near the
poles, which skews partition load slightly; if that ever matters, `grid_cell()` is one function to
swap for an H3 call.

## 0.2 — Chaos is modelled as arrival delay, never as a corrupted `event_time`
The generator holds each event in a release heap keyed by `event_time + delay`, so disorder,
lateness and duplication all come from *when the pipeline sees* an event, while `event_time` stays
truthful. Alternative considered: perturbing `event_time` directly, which is a one-liner. Rejected
because it destroys the ground truth — Phase 1.3 has to compare naive and watermark aggregates
against a known-correct answer, and that answer only exists if `event_time` is never lied about.
As a bonus, `ingest_time >= event_time` holds by construction, which is a real-world invariant a
perturbation model would violate.

## 0.2 — Pure virtual-clock core, thin real-time shell
`SyntheticSource.simulate()` is a seeded, synchronous generator over a virtual clock;
`stream()` is a small async wrapper that paces those same events to wall clock before publishing.
Alternative: one async generator that sleeps between ticks. Rejected because it would make every
chaos-rate test take as long as the window it measures — the duplicate/drop-rate tests need 20,000
events and run in under a second against the pure core. It also hands Phase 1.6 a deterministic
replay source for free: same seed and same `start_time` produce byte-identical events.

## 0.2 — One producer interface from the start
`ingestor/base.py` defines an `EventSource` protocol (`name` + `stream()`) and a `publish()`
function that knows nothing about which source it is draining. Phase 2.4's OpenSky adapter is a
second implementation rather than a fork of the publish path. Alternative: write it straight for the
synthetic source and generalise later. Rejected because the roadmap explicitly depends on the
interface existing now, and it is about six lines.
