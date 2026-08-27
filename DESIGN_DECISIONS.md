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

## 1.3 — The benchmark replays the seeded generator, not a Kafka topic
Both processors are pure functions over a list of events in arrival order — exactly what `collect()`
returns from a real topic — so the transport cannot change their output. Alternative: publish each
configuration to Redpanda and consume it back, which is more end-to-end. Rejected because it adds
broker scheduling and consumer-fetch variance to a measurement of *windowing correctness*, and every
number would then depend on how the machine happened to schedule that run. The seeded generator makes
each row reproducible from the config printed beside it, which is the property that matters for a
benchmark someone else will re-run. The end-to-end Kafka path is exercised by the 1.2 live run and
by Phase 1.6's replay harness, which is where transport actually is the thing under test.

## 1.3 — Two disorder groups, because one number would have been a lie
The generator has two disorder mechanisms of very different magnitude: bounded out-of-order arrival
within `max_skew_s`, and late arrivals of 90-240s. The first draft swept them mixed together, and the
watermark engine showed 85.69% window error at L = max skew — which reads as "the engine does not
work". It was working exactly as designed: the residual error was entirely late-chaos events beyond
any sane bound, correctly routed to the side output. Reporting that mixed number would have
understated the engine on bounded disorder and overstated what a lateness bound can do about
unbounded lateness. Splitting the sweep shows both truthfully: exactly 0.00% error under bounded
disorder at L = max skew, and honest, *reported* degradation under unbounded lateness.

## 1.3 — Three metrics, because each one alone misleads
Window error rate is a sensitivity measure: at low disorder the baseline misplaces 0.22% of events
but corrupts 22.05% of windows, since one stray event out of ~57 is enough to make a window's
aggregate wrong. Event misattribution rate is the magnitude, but it flattens the worst case. So the
benchmark reports window error rate, event misattribution rate, and the worst single window's count
deviation — plus a fourth column, `silent`, which is the one that matters operationally. Every event
the baseline misplaces is misplaced with no record; every event the watermark engine cannot place is
in a counted side output. Under high disorder with late arrivals that is 16,487 silent errors against
0. A single "accuracy" headline would have hidden the difference that actually matters in production.

## 1.4 — Lag derivative, not lag threshold
A threshold controller asks "is lag above N?", which is wrong in both directions. A large but
*shrinking* lag needs no action -- the system is already recovering and extra workers are waste --
yet recovery and collapse look identical to a threshold. A small but rapidly growing lag needs action
immediately, but a threshold cannot act until the burst has already been unabsorbed long enough to
cross the line. Worse, it flaps: with lag hovering near N, consecutive samples straddling the line
produce opposite decisions, and each decision is a consumer-group rebalance that halts consumption
briefly, raising lag, triggering the next one. The failure feeds itself. So the controller fits a
least-squares line over a sliding window and acts on the gradient. Alternatives considered: a PID
loop (rejected -- three coefficients to tune with no principled way to choose them here, and the
integral term is actively harmful when the actuator is quantised to whole workers), and an EWMA of
lag (rejected -- smooths the level, still answers the threshold question).

## 1.4 — A slope is not enough: the trend must be statistically significant
The first version used slope alone against a 5 events/s threshold, and the anti-flap test caught it
immediately: fed lag jittering +/-12% around a flat 1000, it scaled up three times and then shed.
A random walk over a short window produces slopes far above any fixed rate threshold, and least
squares does not save you -- on four points, one 10x spike still yields 91% of the naive endpoint
slope. So the fitted gradient is divided by its own standard error and must clear a significance
bound as well as the rate threshold. Under noise the residuals are large, the standard error swamps
the gradient, and nothing fires.

The threshold value itself was the second mistake. `significance = 3.0` was chosen as "3 sigma",
which is a *normal* approximation and wrong for a 6-sample window: with 4 degrees of freedom the
t-distribution has heavy tails, and measurement showed t >= 3.0 firing on 4.62% of pure-noise
windows. The default is now 4.6, the 1% point for df=4, measured at 1.25%. **This is coupled to
`window_samples`** -- df=2 needs 8.6 for the same rate, df=6 only 4.0 -- and both the config comment
and a test assert the coupling, so the next person to widen the window does not silently make the
controller trigger-happy.

## 1.4 — Shed whole geographic cells, not a random sample of events
At max workers with lag still climbing, something must give. Dropping a random fraction of events
biases *every* cell's aggregate low, and silently: each cell looks plausible and each is wrong.
Dropping a deterministic hash-selected fraction of cells leaves every surviving cell exactly correct
and makes the loss enumerable -- the supervisor logs precisely which cells went dark and for how
long (the burst run shed 1,816 events across 12 named cells). Partial correctness you can describe
beats uniform corruption you cannot. The hash is stable, so the same cells stay shed for the whole
episode rather than a different arbitrary slice each batch.

## 1.4 — Scale down when lag is low and flat, not only while it is falling
Caught by `test_never_exceeds_max_or_drops_below_min`: the first scale-down rule required an actively
negative slope, so once the system went idle and the slope flattened to zero, the pool stayed at max
forever. The rule is now "lag below the low-water mark and not growing", which covers both the
draining case and the settled-idle case.

## 1.4 — A monitor that cannot see must not report zero
The burst run produced no control actions three times running, because `LagMonitor` reported zero lag
while the broker showed 17,860. Cause: the monitor's consumer never subscribes to anything, so it
never refreshes its cached cluster metadata; started before the topic existed, it reported no
partitions forever, and "no partitions" fell through to `total=0` -- indistinguishable from "keeping
up perfectly". `consumer.topics()` does not fix it either: it returns fresh metadata without
installing it in the cache that `partitions_for_topic` reads. Partitions are now discovered through
the admin client's `describe_topics`, a real request every sample. The deeper lesson is recorded in
the code as a comment: a health signal whose unknown state renders as its healthy value will hide
exactly the incident it exists to detect. The supervisor now also logs every sample, not only the
ones that act, because the three silent runs were undiagnosable without the holds and their reasons.

## 1.6 — Determinism is a property of the input order, not just the arithmetic
`collect()` sorts arrivals by `(ingest_time, icao24, event_time)`, a total order, rather than by
`ingest_time` alone. The weaker key looks sufficient and is not: one generator tick emits a whole
fleet sharing an `ingest_time`, and Python's sort is stable, so those ties keep whatever order the
broker happened to interleave the six partitions in on that run. The aggregate could then differ
between replays of identical bytes. Alternative considered: make the fold order-independent instead
(sum in a canonical order at finalisation, or use exact decimal arithmetic). Rejected as solving a
smaller problem -- input order also determines *when a window finalises relative to a late arrival*,
so two orderings can legitimately disagree about which events are late, and no amount of arithmetic
care fixes that. Ordering the input is the fix; the rounding at finalisation is belt-and-braces
against float non-associativity. `test_replay_is_insensitive_to_partition_interleaving` publishes
one event list to a 1-partition and a 6-partition topic and asserts equal digests.

## 1.6 — A recording is written unpaced; only a live source is paced
`publish_events()` writes an already-materialised event list as fast as the broker accepts it, next
to `publish()` which drains a live source at wall-clock pace. The first version of the replay
harness used `publish()`, so laying down 300 seconds of event time took 300 real seconds and the
test suite ran for a quarter of an hour. A recording carries its own `event_time` and `ingest_time`
in each message, so the rate at which it is written is invisible to everything downstream -- pacing
it buys nothing and costs the entire runtime. The paced path stays for the live pipeline, where the
timing *is* the point.

## 1.6 — Every replay gets a fresh consumer group
`replay()` mints a new group id per call. Reusing a group would let a replay resume from committed
offsets and hash a *suffix* of the recording while reporting it as the whole thing -- and it would
do so silently, producing a plausible digest over the wrong data. A fresh group makes "read from
offset 0" structural rather than something the caller has to remember.

## 2.1 — Per-partition watermarks, finally implemented for the live path
DESIGN_DECISIONS 1.2 deferred this to "the live windowing service in Phase 2", and 2.1 is that
service, so it is implemented rather than deferred again. `WatermarkProcessor` now keeps a
high-water mark per source (Kafka partition) and takes the global watermark as the **minimum** across
them. A single global max is wrong the moment input is partitioned: one partition running ahead
finalizes windows on behalf of partitions that have not delivered yet, and every one of their events
is then correctly-but-uselessly reported late — the exact 33x over-reporting measured in 1.2. Offline
replay sidesteps it by sorting the whole stream first; a live consumer cannot, because it has not
seen the whole stream. The default source is `""`, so every existing call site and all the replay
determinism guarantees are unchanged.

**Not done: idle-partition handling.** A partition that stops delivering never advances its mark, so
the global minimum stalls and windows stop finalizing. Every partition carries traffic under the
synthetic load, so this does not bite here, but it is the standard next problem and needs an
idleness timeout that drops a quiet partition out of the minimum. Deliberately left out because the
timeout would have to consult the wall clock, and that would put non-determinism into the same
processor the Phase 1.6 replay claim depends on — it needs to be an opt-in path, not a default.

## 2.1 — Redis is the seam between the pipeline and the API
The windowing service publishes each finalized window to a Redis channel (for WebSocket fan-out) and
into a hash keyed by geographic cell (for REST). The API never touches Kafka. Alternative: have the
API consume Kafka directly, which is fewer moving parts. Rejected because an HTTP process that joins
a consumer group takes a partition assignment — so scaling the API to two instances would steal
partitions from the pipeline, and every deploy would trigger a rebalance that stops consumption
mid-flight. Availability of the read path and correctness of the write path should not be able to
damage each other, and a pub/sub seam makes that structural rather than a rule someone has to
remember.

## 2.1 — Token bucket, not a fixed window
A fixed window lets a client spend a full quota in the last instant of one window and again in the
first instant of the next: double the intended rate, precisely at the boundary. A token bucket
cannot be gamed that way because credit accrues continuously. `test_no_boundary_double_spend`
asserts it directly. Two details the tests forced out: `updated` is not seeded from
`time.monotonic()` (that made the first `take()` with an injected clock compute a huge negative
elapsed and empty a full bucket), and the "enough tokens" comparison carries a 1e-9 epsilon because
elapsed time accumulates in floating point — refilling across two 0.05s hops lands on
0.9999999999999432, and refusing that is an unfairness that depends on how the caller sliced time.

## 2.1 — `/healthz` is exempt from rate limiting, so it caches instead
An orchestrator's liveness probe must never be told to back off; a 429 during a traffic burst would
take a healthy instance out of rotation exactly when it is needed. But exempting it means it can be
hammered, and each probe opens a real connection to all three dependencies — 60 concurrent requests
took `/healthz` from 200 to 503, i.e. the probe exhausted the very things it was checking. A 1s TTL
cache collapses a burst into one round of probes while staying fresh enough for a poll every few
seconds. Caching a health check is normally a smell; here the alternative is a self-inflicted
outage, and the TTL is far shorter than any sensible probe interval.

## 2.1 — WebSocket auth is checked before `accept()`
The token arrives as a query parameter because browsers cannot set headers on a WebSocket handshake.
It is verified before the socket is accepted, so an unauthenticated client is refused at the
handshake rather than being allowed to hold an open connection it can never use. The tradeoff is
that the token can appear in access logs and proxy logs in a way an `Authorization` header would not;
with a 1-hour TTL and a single operator account that is acceptable here, and the fix if it ever
matters is a short-lived ticket issued over REST and exchanged at the handshake.

## 2.2 — Two watermark skews, because either alone lies
`watermark_skew_wallclock` is `now - watermark`: how stale the engine's notion of completeness is.
`watermark_skew_event` is `max event_time seen - watermark`: whether the watermark is doing what it
was configured to do, and it sits at almost exactly the allowed lateness. The pair is necessary
because each is blind in the opposite direction. Wall-clock skew grows both when the pipeline falls
behind *and* when data simply stops arriving, so an idle pipeline looks broken. Event skew stays
flat at the configured bound whatever happens upstream, so a completely stalled pipeline looks
healthy. Only together do they distinguish "behind", "idle" and "misconfigured". The live dashboard
shows event skew pinned at 30.0s against a configured 30s allowed lateness, which is also a
continuous check that the config in the file is the config in the process.

## 2.2 — Each process is its own scrape target
`api`, `windowing` and `pipeline` each serve `/metrics` and Prometheus scrapes them separately with
a `component` label. Alternative: one exporter, or a pushgateway. Rejected because the question this
dashboard exists to answer is "which stage is behind", and aggregating the processes behind a single
endpoint erases exactly that. The cost is a gotcha, described next.

## 2.2 — Every dashboard query is scoped to the component that owns the metric
All three processes import the shared metrics module, so all three *register* the full metric set and
expose unset gauges as `0`. An unscoped `contrail_workers` returns three series -- `pipeline`=1 and
two phantom zeros -- and `sum()` over it would be silently wrong. Caught by querying Prometheus
during the check and seeing 0 where the process itself reported 1. Every panel now selects on
`{component="..."}`. The alternative, having each process register only its own metrics, is
structurally cleaner but means splitting the metrics module by domain and losing the single place
where names are defined; scoping the queries keeps names in one file and puts ownership in the
dashboard, where it is visible.

## 2.2 — Grafana provisioning must pin the datasource UID
The dashboard was blank on first render: 72 console errors, all `Datasource prometheus was not
found`. Without an explicit `uid:` in the datasource provisioning file, Grafana generates a random
one (`PBFA97CFB590B2093` here), while every panel in `contrail.json` references `uid: "prometheus"`.
Nothing else catches this -- the dashboard API returned 200 with all 12 panels, and every panel query
returned data when run against Prometheus directly. Only looking at the rendered page revealed it.
That is why the roadmap's "take a screenshot" step is a real check and not documentation busywork.

## 2.2 — The demo pipeline runs as compose services, so tests scope to their own data
`windowing`, `pipeline` and `generator` are now long-running compose services, so a single
`docker compose up` produces a live system with a populated dashboard instead of an empty one. The
consequence: something is always writing to `flight_events`. Two integration tests that
`TRUNCATE`d the table and counted every row immediately began failing -- deterministically, this
time; the same tests had flaked once before under transient contention, which in hindsight was the
early warning. Both now derive the icao24 set of the fleet they generate and scope their deletes,
counts and duplicate checks to it, which makes them independent of anything else writing. The
control benchmark got the same treatment, since a demo pipeline writing rows would otherwise pollute
its latency percentiles.

## 2.4 — OpenSky is a second `EventSource`, not a fork of the publisher
The protocol defined back in 0.2 (`name` + `stream()`) paid for itself here: `OpenSkySource` is a new
implementation and nothing downstream changed. `publish()`, the sink, the windowing service, the
control loop and the API are byte-identical under either source, and `ingestor/run.py` is the only
module in the codebase that knows two sources exist. A test asserts both satisfy the protocol, so the
claim is checked rather than asserted in prose.

## 2.4 — Substituted values and invented values are counted separately
A null `baro_altitude` can be filled from `geo_altitude`, which is a real reading from a different
sensor. A null `velocity` can only be filled with 0.0, which is a guess. The first draft counted both
as `repaired_fields`, and a test caught the conflation. They are now `substituted_altitude` and
`repaired_fields`. The distinction matters operationally: an operator seeing "5% repaired" should
know whether that means "we used the other altimeter" or "we made a number up", and a single counter
lets the second hide inside the first. This is the same principle as the late-event side output --
degraded data stays visible as degraded.

## 2.4 — Live feed messiness is absorbed, never allowed to reach the pipeline as an exception
Rate limits (429, with or without `Retry-After`), 5xx, connection timeouts and non-JSON bodies all
return "retry this poll" rather than raising. The poll loop backs off exponentially with jitter and
resets to the normal interval on the first success, so a single blip does not leave the poller
permanently slowed. Jitter is not decoration: without it, a fleet of pollers recovering from one
outage re-synchronises and hammers the API in lockstep. A `Retry-After` header is honoured over our
own backoff, because the server knows better than we do. Each of these paths has a test with an
injected failure -- a live feed's failure modes cannot be summoned on demand.

## 2.4 — Duplicates from the live feed are left to the sink, not filtered in the adapter
OpenSky returns the same aircraft on consecutive polls with an unchanged `time_position` whenever it
has no newer observation. Those are genuine duplicates of one event. The adapter does not filter
them: the sink's `(icao24, event_time)` idempotency key already handles exactly this, and filtering
in two places means two things to keep correct. The live run bore it out -- 26,407 states emitted
against 22,702 rows stored, roughly 3,700 real duplicates absorbed by a constraint written in
Phase 0.3 for synthetic data.

## 2.4 — The geographic partition key behaves differently on live data, and that is worth knowing
Under the default bounding box (central Europe, 10 degrees square), the 5-degree grid yields only
**four** cells, and traffic concentrates heavily in one of them. The synthetic generator spreads
aircraft worldwide and fills all six Kafka partitions evenly; the live feed does not. That is not a
bug in either -- it is what happens when a uniform grid meets a non-uniform world -- but it means
partition balance measured against synthetic traffic does not transfer to a regional live feed.
Widening the bbox or shrinking `GRID_DEG` both help; the honest framing is that the partitioning
scheme is tuned for the synthetic benchmark and would need revisiting for a regional deployment.

## 2.8 — Long-running services need bounded accumulators; making them permanent was a regression
Phase 2.2 promoted `windowing`, `pipeline` and `generator` from ad-hoc commands to always-on compose
services. That silently invalidated an assumption three data structures were written under. The
dedup set in `Aggregator` even carried a comment saying it was "fine for the bounded replay runs the
benchmarks use" — which stopped being true the moment the service stopped exiting. Measured at the
live rate of ~86 events/s: about 7.4 million entries and over a gigabyte a day in that set alone,
with the finalized-window dict, the late-event list and the controller history growing beside it.
The windowing service would have run out of memory in roughly a week.

Fixed by making all three bounded **only where boundedness is correct**:
`seen_retention_windows`, `late_retention` and `retain_windows` all default to unbounded, because
replay determinism depends on a dedup set that never forgets and the benchmarks depend on keeping
every window. Only the live service sets them. Eviction is bucketed by the event's own window so it
costs O(buckets) rather than a scan of millions of keys.

The tradeoff is stated rather than hidden: a duplicate arriving after its bucket is evicted is no
longer recognised as a duplicate and is reported as a late event instead. Its window closed long
ago either way, so no aggregate changes — only which counter it lands in. `late_count` is tracked
separately from the capped list so the total stays exact while only a recent sample is retained.

## 2.8 — The committed JWT placeholder is never honoured as a signing key
`jwt_secret` defaults to `dev-only-change-me`, which is in git and in `.env.example`, and a code
comment saying it "MUST be overridden" enforces nothing — anyone could mint a valid token for any
deployment that never set it. The API now signs with a random per-process secret whenever the
placeholder is still in use, and warns once. `docker compose up` still works out of the box; the only
cost is that tokens do not survive a restart, which is the right behaviour for a secret nobody
configured. A test asserts a token signed with the placeholder is rejected.

## 2.8 — Known limitation: a windowing crash loses in-flight aggregates
The sink is crash-safe by construction (commit after write, idempotent key) and replay is
reproducible, but the **windowing service is neither**. It holds partially-accumulated windows in
memory and auto-commits offsets on a timer, so a crash loses every window that had not yet finalized,
and the events that would have completed them are already marked consumed. The aggregate for that
period is then silently incomplete — the exact failure class this project otherwise sets out to
eliminate.

This was missed until an audit because the Phase 2.5 chaos test kills the *pipeline*, not the
windowing service, and no test covers a windowing restart. The obvious partial fix is worse than
nothing: committing manually after each publish means a restart reprocesses from the last commit and
re-finalizes an already-published window from a fraction of its events, overwriting a correct
aggregate in Redis with a wrong one. A real fix needs the window state checkpointed alongside the
offsets — a Flink-style barrier, or writing partial aggregates to Redis and rehydrating on start.
That is a genuine feature, not a patch, so it is recorded here and in the README rather than
half-done.

## 2.8 — Every long-running service runs python as a child, not as PID 1
The Phase 2.5 shell wrapper was applied only to `pipeline`, so `windowing` and `generator` still ran
python as PID 1 — which the kernel makes immune to signals from inside its own namespace, meaning
neither could be crash-tested at all and neither would have honoured its restart policy. All three
now use the same wrapper. Uniformity here is not tidiness: an untestable service is one whose
recovery behaviour you are guessing at.
