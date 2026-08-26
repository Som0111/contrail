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

## 0.3 — Idempotency key is `(icao24, event_time)`, enforced by the database
An aircraft has exactly one state at one instant, so the natural key is already unique — no
synthetic id needed. Alternative considered: a producer-minted message UUID. Rejected because a
UUID only dedups *redeliveries of the same message*; it would happily insert two rows for the
generator's duplicate-emission chaos, since those are two messages describing one event. The
natural key catches both cases with the same constraint. TimescaleDB requires a unique index to
include the partitioning column, and `event_time` is that column, so the constraint costs nothing
extra. The cost is that a genuine correction to an already-stored event would be dropped rather
than applied; if that ever matters the fix is `ON CONFLICT ... DO UPDATE`, not a different key.

## 0.3 — At-least-once delivery, made safe by the write, not by the consumer
The sink commits offsets only after the batch is durably written, and carries no dedup cache,
no transactional producer, no exactly-once machinery. Alternative: Kafka transactions for
end-to-end exactly-once. Rejected as a large amount of coordination to buy a guarantee the unique
constraint already gives us for one line of SQL, and one that would not survive the pipeline being
restarted against a fresh consumer group anyway. The crash window this deliberately leaves open —
written but not committed — is exercised directly by the kill test, which sees 86-124 records
re-delivered and absorbed per run.

## 0.3 — First proof point for core claim #3 (determinism)
Two integration tests, both against live Redpanda and TimescaleDB rather than mocks, since the
property under test belongs to the constraint and the commit ordering: replaying an identical batch
through a fresh consumer group inserts zero new rows and leaves the row count untouched; and
`SIGKILL`-ing the sink process mid-stream then restarting it lands on exactly the unique-event count
with zero duplicate keys. Phase 1.6 extends this from "same row count" to "byte-identical aggregate
output".

## 0.3 — Correlation id derived from the event, not minted at random
`trace_id` is `blake2s(icao24|event_time)`. Alternative: a random UUID per consumed message.
Rejected on two counts — a random id differs between a run and its replay, which would make the
Phase 1.6 output hash unstable, and it would give the two copies of a duplicated event different
ids, hiding exactly the relationship an operator is trying to see in the logs.

## 0.4 — The report reconciles two independent sources, not one
`scripts/phase0_report.py` reads the message count from Redpanda's own partition offsets and the
row count from TimescaleDB, then asserts `stored + suppressed == received`. Alternative: have the
generator report how many events it emitted and compare against the database. Rejected because the
generator's intent is not evidence — if the producer silently failed to send, an intent-based report
would still balance. Offsets and rows are two things the pipeline actually did, so the identity only
holds if the whole path worked. The observable consequence is that `drop_prob` is invisible to the
report by construction: a dropped event never reaches the topic, so it cannot be counted, only
inferred by comparing against the generator's configured rate.

## 0.4 — The sink claims the topic's partition count too
Subscribing a consumer to a missing topic auto-creates it with a single partition. Starting the sink
before the generator therefore silently capped the pipeline at one partition — caught during the
Phase 0 integration run, when the sink was assigned only `partition=0`. Alternative: turn off broker
auto-creation, or document a required start order. Rejected both: a documented ordering constraint
is a trap that fires under Compose restarts, and disabling auto-create turns the mistake into a
crash somewhere less obvious. Instead the sink calls the same idempotent `ensure_topic()` the
producer does, so whichever starts first creates the topic with the configured partition count.

## 1.1 — Deduplication is shared by both processors, not left to each
`Aggregator.add()` drops repeat `(icao24, event_time)` keys, so the naive baseline and the Phase 1.2
watermark engine both see each event exactly once. Alternative: let the naive processor double-count
duplicates, since a truly naive implementation would. Rejected because it would inflate naive's
measured error with a second, unrelated fault, and core claim #1 is specifically about event-time
attribution. Keeping dedup identical on both sides means every difference the 1.3 benchmark reports
is attribution error and nothing else — a smaller, but honest, number.

## 1.1 — Ground truth is defined over arrived events, not generated ones
`ground_truth()` attributes by `event_time` over the events that actually reached the processor.
Alternative: compare against everything the generator emitted, including dropped events. Rejected
because no processor can attribute a record it never received — counting the drop rate against both
of them would add a constant error to each and measure the network, not the windowing. Drops stay
visible as a separate configured rate, verified independently in Phase 0.

## 1.1 — The naive baseline uses recorded `ingest_time`, not wall-clock-at-consume
A textbook processing-time window keys on `now()` at the moment the consumer handles the record.
This one keys on the `ingest_time` stamped when the record entered the pipeline. Alternative: call
`now()` in the consume loop, which is more literally "processing time". Rejected for two reasons.
It would make the baseline non-reproducible — the same recorded stream would bucket differently
depending on how fast the machine drained the topic, and 1.3 has to re-run both processors over an
identical stream. And it is strictly *more* wrong: consume-time adds queueing delay on top of
arrival delay, so it would misattribute more events, not fewer. Using `ingest_time` therefore
handicaps the comparison in the baseline's favour, which is the safe direction for a claim we intend
to defend.

## 1.2 — Watermark = max observed event_time minus a fixed allowed-lateness bound
The classic bounded-disorder heuristic, and the reason the engine is immune to the failure that
breaks the naive baseline: the watermark advances on *event time only*, so a delayed event cannot
close its own window. Alternatives considered: a processing-time-driven watermark (wall clock minus
a bound), which reintroduces exactly the arrival-time dependence we are trying to eliminate — a slow
consumer would start finalizing windows early; and a percentile-based adaptive watermark that learns
the observed skew distribution, which is genuinely better under variable disorder but is unfalsifiable
in a benchmark, since the bound would move while being measured. A fixed configurable bound gives a
guarantee that can be stated and tested as an equality:

    allowed_lateness >= max arrival skew  =>  output is identical to ground truth

`test_lateness_bound_at_or_above_max_skew_gives_exact_output` asserts that equality directly, and
`test_error_appears_only_once_disorder_exceeds_the_bound` asserts the degradation is monotone as the
bound tightens. That relationship is what makes the 1.3 sanity check meaningful rather than a vibe.

## 1.2 — Late events go to a counted side output, never to /dev/null
An event whose window has already been finalized is appended to a side-output list and logged at
WARNING with its window, the watermark that closed it, and how late it was. Alternative: drop it
(what most naive implementations do), or reopen the window and re-emit a correction. Dropping is
unacceptable because it makes data loss invisible — the aggregate is simply quietly wrong. Window
re-opening was rejected as out of scope: it means retracting an already-published aggregate, which
needs downstream consumers that understand retractions, and nothing in this project has one yet.
The accounting identity is tested: `windowed + late == processed`, so every arrived event is either
in a window or in the side output, and never in neither.

## 1.2 — `collect()` restores recorded arrival order; a live consumer would need per-partition watermarks
Found by running the engine against the real topic rather than trusting the unit tests: the L=30s
run reported 6,816 late events where ~200 were expected. Cause: `getmany()` returns per-partition
record batches, and concatenating them interleaves six partitions in arbitrary chunks, so a chunk
from one partition carries event_times from late in the run. Those drag a single global watermark
forward and prematurely finalize windows the other partitions have not delivered yet — every
subsequent record from those partitions is then, correctly by the engine's own logic, reported late.
The engine was right; the input was not the stream it claimed to be. Fix: `collect()` sorts on
`ingest_time`, which is the arrival instant the recording captured, so offline replay sees the stream
as it actually arrived. An integration test now asserts that ordering, since the failure was silent.
**Scope limit, stated plainly: the sort is valid for offline replay only, and must not be carried
into live ingestion.** It works here because `collect()` reads a bounded, already-recorded topic
into memory and can therefore see every record before deciding the order. Neither condition holds
for a live consumer: it cannot sort a stream it has not finished reading, and it has nowhere to
buffer an unbounded one. Reusing this approach live would mean holding records back until "enough"
have arrived — which is an allowed-lateness bound implemented badly, in the wrong layer, with
unbounded memory.

The real fix for live multi-partition ingestion is a per-partition watermark, with the global
watermark taken as the *minimum* across partitions — what Flink and Beam do. Each partition's
watermark advances only on the event_times that partition itself delivers, so a partition running
ahead can no longer finalize windows on another's behalf, which is exactly the failure above. It is
deferred to the live windowing service in Phase 2 rather than pre-built now, and it brings its own
problem: a partition that goes idle stops advancing its watermark and stalls the global minimum
indefinitely, so it needs an idleness timeout that drops a quiet partition out of the minimum.

Concretely, what today's implementation does and does not support: **the 1.3 benchmark numbers
describe event-time attribution over a correctly-ordered replay. They do not demonstrate correct
watermarking under live multi-partition consumption** — that claim is unavailable until
per-partition watermarks exist, and the README must not imply otherwise.
