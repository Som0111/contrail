# CONTRAIL — Build Roadmap for Claude Code

## READ THIS FIRST, EVERY SESSION

You are building **Contrail**: an event-time stream processor for flight
telemetry. This is a portfolio/placement project. It must be REAL — no
stubbed metrics, no hardcoded "example" numbers, no TODO-and-move-on.
If something doesn't work, debug it yourself until it does, within the
current phase's scope. Do not silently skip a requirement — if you truly
cannot complete something after real effort, stop and say so explicitly
in the session log, don't fake it.

**Session discipline:** Each session below is scoped to ~20 minutes of
work. Do ONLY the sub-phase you are on. Do not read ahead and
pre-build later sub-phases "while you're at it" — it wastes tokens and
causes drift from the checkpoint structure. When you finish a
sub-phase, run its Final Check, write the Session Log entry, and STOP.
Wait for the human to say "continue" before starting the next sub-phase.

**State file:** Maintain `PROGRESS.md` in the repo root. At the end of
every sub-phase, append a dated entry: what was built, what was
verified, any deviation from this roadmap and why, any known issue
carried forward. Read `PROGRESS.md` at the start of every session
before doing anything else — it is your memory across sessions.

**Never touch `HUMAN_MODIFICATIONS.md`** — that section at the bottom
of this file is for the human, after the project is complete. Do not
implement those items.

---

## THE THREE THINGS THAT MATTER MOST

Everything else in this project is scaffolding around these three. If
time runs short, protect these before anything else:

1. **Event-time windowing with watermarks** — correctly handling
   out-of-order and late events, with a measured comparison against a
   naive processing-time baseline.
2. **Lag-driven adaptive control loop** — a controller that reads
   consumer lag and reacts (scale/shed), reacting on the *trend* not
   just a static threshold, with measured behavior under burst load.
3. **Deterministic replay** — proof that replaying the same input
   produces byte-identical aggregate output, including after a
   mid-stream consumer kill.

The WebSocket API, JWT auth, and dashboards are real requirements but
are NOT the differentiators. Do not gold-plate them at the expense of
the three above.

---

## GLOBAL TECH STACK (do not substitute without logging why in PROGRESS.md)

- Python 3.12, asyncio
- FastAPI (REST + WebSocket)
- Redpanda (Kafka-API compatible, single container, no Zookeeper)
- TimescaleDB (Postgres + time-series extension)
- Redis (cache + pub/sub)
- Prometheus + Grafana
- Docker Compose (single `docker compose up` must always work)
- pytest for all correctness checks (especially determinism)
- Locust for load testing
- GitHub Actions for CI (lint + unit tests on push)

## REPO STRUCTURE (create in Phase 0.1, do not deviate)

```
contrail/
  docker-compose.yml
  README.md                  (written last, Phase 2.4)
  DESIGN_DECISIONS.md         (append-as-you-go, see instructions below)
  BENCHMARKS.md               (filled in Phase 1.4, 2.2, 2.3)
  PROGRESS.md                 (your session memory, see above)
  HUMAN_MODIFICATIONS.md      (human-only, do not edit content)
  .github/workflows/ci.yml
  src/
    ingestor/                 (trace generator + later, real OpenSky)
    windowing/                (watermark engine — core #1)
    control/                  (lag controller — core #2)
    replay/                   (determinism harness — core #3)
    storage/                  (TimescaleDB + Redis clients)
    api/                      (FastAPI REST + WS)
    common/                   (shared config, models, metrics)
  tests/
    unit/
    integration/
  load/                       (Locust files)
  scripts/                    (helper CLI scripts, e.g. run benchmark)
```

## DESIGN_DECISIONS.md — ongoing requirement

Every sub-phase that makes a non-obvious design choice (e.g. "why
lag-derivative not lag-threshold", "why this watermark lateness bound")
must append a short entry: the decision, the alternative considered,
and why this one won. This file is what lets the human defend the
project in interviews. Keep entries to 3-5 sentences each. Do this as
you go, not retroactively.

---

# PHASE 0 — Foundation & Synthetic Data

Goal of this phase: a running skeleton where fake-but-realistic flight
events flow through Kafka into storage, with the deliberate messiness
(out-of-order, duplicates, late events) that Phase 1 needs to prove
anything interesting. No windowing logic yet. No live OpenSky yet.

### Sub-phase 0.1 — Skeleton & Compose stack (~20 min)

**Do:**
- Create repo structure above.
- `docker-compose.yml` with: Redpanda, TimescaleDB, Redis. All with
  healthchecks. All on a shared network.
- Basic `common/config.py` reading env vars (12-factor style), with a
  `.env.example` committed and `.env` gitignored.
- Empty FastAPI app with a `/healthz` endpoint that checks Redpanda,
  TimescaleDB, Redis connectivity and returns per-dependency status.
- `PROGRESS.md` and `DESIGN_DECISIONS.md` created with a one-line header each.

**Final check (must pass before stopping):**
- `docker compose up` brings up all three infra containers healthy.
- `/healthz` returns 200 with all three dependencies reporting "ok".
- `git log` shows a clean initial commit.

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 0.2 — Data model & synthetic trace generator (~20 min)

**Do:**
- Define the flight state event schema in `common/models.py`
  (pydantic): icao24, callsign, lat, lon, altitude_m, velocity_ms,
  heading, event_time (when the aircraft was actually at this state),
  ingest_time (when we received it) — these two timestamps being
  DIFFERENT and controllably skewed is the entire point.
- Build `ingestor/synthetic.py`: a generator that simulates N aircraft
  moving along simple flight paths, emitting state events at a
  configurable rate, with CONFIGURABLE:
  - out-of-order probability + max skew (event_time vs. arrival order)
  - duplicate probability
  - late-event probability (arrives after its window would have closed)
  - drop probability (simulates packet loss)
- These parameters must be settable via config/CLI, not hardcoded —
  Phase 1 benchmarking depends on being able to dial chaos up and down.
- Wire the generator to publish to a Redpanda topic (`flight.events.raw`),
  partitioned by a geographic bucket (H3 index at low resolution, or a
  simple lat/lon grid cell if H3 adds too much friction — your call,
  log the choice in DESIGN_DECISIONS.md).

**Final check:**
- Running the generator for 60 seconds produces messages on the topic,
  verifiable with `rpk topic consume` or equivalent.
- Unit test proves: with out-of-order probability set to 0, output
  is monotonic; set to >0, output demonstrably contains inversions.
- Unit test proves duplicate and drop rates land within tolerance of
  configured values over a large sample.

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 0.3 — Idempotent consumer + TimescaleDB sink (~20 min)

**Do:**
- `storage/timescale.py`: schema for raw events (hypertable), with a
  unique constraint that makes re-processing the same event a no-op
  (idempotency key: icao24 + event_time, or a message-level UUID if
  you added one in 0.2).
- A basic consumer in `ingestor/sink.py` that reads from
  `flight.events.raw`, writes to TimescaleDB, commits offsets only
  after successful write (at-least-once, made safe by the idempotent
  write).
- Structured logging with a correlation/trace id per message.

**Final check:**
- Kill the consumer mid-run and restart it — no duplicate rows in
  TimescaleDB (verify by count + unique constraint, not just "it ran").
- Replaying the exact same batch of messages twice results in the
  identical row count as replaying once.
- This is your FIRST proof point for core #3 (determinism/idempotency)
  — note that in DESIGN_DECISIONS.md.

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 0.4 — Phase 0 integration check (~10-15 min)

**Do:**
- No new features. Run the full pipeline end to end: generator → Redpanda
  → consumer → TimescaleDB, for 5 minutes, with moderate chaos settings
  (some out-of-order, some duplicates, some late events).
- Write a small script `scripts/phase0_report.py` that queries
  TimescaleDB and prints: total events, unique events, out-of-order
  count observed (ingest order vs event_time order), duplicate count
  suppressed by idempotency.

**Final check (Phase 0 exit gate — do not proceed to Phase 1 until ALL pass):**
- [ ] `docker compose up` works from a clean checkout with no manual steps
- [ ] Generator chaos parameters are config-driven and verified by tests
- [ ] Idempotent sink proven under consumer restart
- [ ] `phase0_report.py` output makes sense (numbers are internally
      consistent — e.g. unique + suppressed duplicates == total received)
- [ ] `DESIGN_DECISIONS.md` has entries for: partitioning scheme choice,
      idempotency key choice

**Write PROGRESS.md entry summarizing Phase 0 completion. STOP.
Report the exit-gate checklist results to the human before continuing
to Phase 1.**

---

# PHASE 1 — The Hard Core (protect this phase above all else)

Goal: the three differentiating technical claims, each with a real,
measured, reproducible number. This is the phase interviewers will
drill into. Do not rush it to get to Phase 2.

### Sub-phase 1.1 — Naive baseline processor (~15-20 min)

**Do:**
- Build the "wrong way" first, deliberately: `windowing/naive.py` —
  fixed processing-time windows (e.g. tumbling 60s windows keyed by
  when the event was CONSUMED, not event_time). Aggregates: count,
  avg altitude, avg velocity per window per geo-partition.
- This is your baseline for comparison, not the final feature. Keep
  it simple and clearly labeled as the naive approach in code comments.

**Final check:**
- Run against the Phase 0 generator with moderate chaos on. Produces
  window aggregates.
- Write a test that PROVES the naive approach misattributes events:
  feed it a controlled sequence where you know some events are late
  enough to land in the wrong window, and assert the naive aggregate
  is measurably wrong vs. ground truth (which you can compute directly
  from the generator's known event_times, since it's synthetic).

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 1.2 — Watermark-based event-time windowing (~20 min)

**Do:**
- `windowing/watermark.py`: implement watermark generation (e.g.
  max observed event_time minus a configurable allowed-lateness bound),
  event-time keyed windows, and a side-output path for events that
  arrive after their window has been finalized (too late even for
  the lateness allowance) — log/store these separately, don't drop
  silently.
- Same aggregates as naive, for direct comparison.
- Make allowed-lateness configurable — you'll sweep this in the next
  sub-phase.

**Final check:**
- Same controlled test sequence from 1.1: watermark approach produces
  correct attribution where naive did not. Assert this directly,
  don't eyeball it.
- Late-event side output is non-empty when you deliberately send
  events later than the allowed-lateness bound, and empty when you don't.

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 1.3 — Baseline vs watermark measured comparison (~15-20 min)

**Do:**
- `scripts/benchmark_windowing.py`: runs BOTH naive and watermark
  processors against the same recorded/replayed event stream (use the
  generator's chaos settings, several configurations: low/medium/high
  disorder), and computes an attribution-error metric (e.g. % of
  windows where the aggregate differs from ground truth by more than
  a small tolerance).
- This produces the real number for the resume bullet. It must come
  from an actual run, not be invented.

**Final check:**
- Script runs end to end and prints a clear before/after table.
- Numbers are sane (naive error > 0 under disorder, watermark error
  near 0, watermark error only rises when disorder exceeds the
  configured lateness bound — if that last part isn't true, something
  is wrong with the implementation, fix it before moving on).
- Append the actual results to `BENCHMARKS.md` with the exact config
  used to produce them (chaos settings, lateness bound, event count,
  machine spec) so it's reproducible.

**Write PROGRESS.md entry. STOP. Report the numbers to the human
before continuing — this is core claim #1, worth a manual sanity look.**

---

### Sub-phase 1.4 — Lag-driven control loop (~20 min)

**Do:**
- `control/lag_monitor.py`: reads consumer group lag from Redpanda
  periodically.
- `control/controller.py`: a control loop that reacts to lag TREND
  (e.g. rate of change over a sliding window), not just a static
  threshold — this is a deliberate, defensible design choice, write
  it up in DESIGN_DECISIONS.md including why threshold-only causes
  flapping.
- Two reactions: (a) scale — spin up additional consumer worker
  coroutines/processes up to a configured max, (b) shed — under
  sustained overload beyond what scaling can absorb, deliberately
  degrade (e.g. widen window granularity or sample a fraction of
  partitions) rather than let lag grow unbounded. Log every
  scale/shed decision with the lag values that triggered it.

**Final check:**
- Unit tests for the controller logic in isolation (feed it synthetic
  lag time series, assert correct scale-up, scale-down, shed, and
  no-flap-on-noise behavior) — don't only test this against live infra,
  a deterministic unit test is more convincing and faster to debug.
- Integration run: throttle the generator up sharply (simulate a
  burst, e.g. 6x rate) and observe via logs/metrics that the
  controller reacts.

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 1.5 — Control loop measured comparison (~15-20 min)

**Do:**
- `scripts/benchmark_control.py`: run the SAME burst scenario twice —
  once with static worker count (no controller), once with the
  adaptive controller — and measure end-to-end p50/p95/p99 latency
  (event_time to processed) and max observed lag during the burst.

**Final check:**
- Real numbers, both runs, same burst profile, same machine.
- Append to `BENCHMARKS.md` with exact config for reproducibility.
- Sanity check: adaptive run should show bounded p99 and recovering
  lag; static run should show lag growing unboundedly or a much worse
  p99. If the adaptive version isn't clearly better, debug the
  controller before declaring this done — do not report a weak or
  ambiguous number as if it were the finding.

**Write PROGRESS.md entry. STOP. Report the numbers to the human —
this is core claim #2.**

---

### Sub-phase 1.6 — Deterministic replay harness (~20 min)

**Do:**
- `replay/harness.py`: capability to replay a recorded raw event
  stream (captured earlier, e.g. from a Phase 0 or Phase 1 run) from
  offset 0 through the watermark windowing pipeline, and hash/diff
  the resulting aggregate output.
- Test: run the same recorded stream through the pipeline 3 times,
  assert identical output hash each time.
- Test: run once normally, run again but kill and restart the
  consumer process partway through, assert identical final output
  hash to the uninterrupted run.

**Final check:**
- Both tests pass reliably (run them 3+ times to rule out flakiness,
  not just once).
- Append a short section to `BENCHMARKS.md`: event count replayed,
  number of runs, confirmation of identical hashes, and the
  mid-stream-kill scenario result.

**Write PROGRESS.md entry. STOP. Report to human — this is core claim #3.
All three hard cores are now done.**

---

### Sub-phase 1.7 — Phase 1 exit gate (~10 min, review only, minimal new code)

**Final check (do not proceed to Phase 2 until ALL pass):**
- [ ] BENCHMARKS.md contains real, reproducible numbers for all three
      core claims, each with exact configuration used
- [ ] DESIGN_DECISIONS.md explains: naive vs watermark tradeoff,
      lag-derivative vs threshold choice, idempotency/determinism approach
- [ ] All Phase 1 unit + integration tests pass in one full run
- [ ] No hardcoded/fake numbers anywhere in code or benchmark scripts —
      grep for suspicious literals if unsure

**Write PROGRESS.md entry. STOP. Report full exit-gate checklist to
human before continuing to Phase 2.**

---

# PHASE 2 — API, Observability, Real Data, Polish

Goal: make the hard core usable and observable, and add the live
OpenSky connection as an isolated, swappable add-on. Lower stakes
than Phase 1 — if time is short, cut scope here first (see priority
order at the end of this phase).

### Sub-phase 2.1 — FastAPI REST + WebSocket + auth (~20 min)

**Do:**
- REST endpoints: current window aggregates by geo-partition, list of
  active alerts (if time permits — see priority note), health/status.
- WebSocket endpoint: fan-out live window aggregate updates to
  connected clients as they're produced (subscribe via Redis pub/sub
  from the windowing engine, don't couple API directly to Kafka).
- JWT auth (simple: login issues a token, protected endpoints require
  it). Rate limiting on REST endpoints (basic token-bucket per client
  is fine).

**Final check:**
- REST endpoints return real data from TimescaleDB/Redis, verified
  with curl/httpie.
- WebSocket client (a simple test script is fine) receives live
  updates as new windows finalize.
- Auth actually rejects unauthenticated requests to protected routes
  (test this, don't assume).

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 2.2 — Prometheus + Grafana (~15-20 min)

**Do:**
- Instrument: consumer lag, watermark skew (difference between
  watermark and wall-clock, and between watermark and max event_time
  seen), end-to-end latency histogram, shed-rate, API request
  latency/rate.
- Add Prometheus + Grafana to docker-compose, provision one dashboard
  covering the above metrics (as code/JSON, not manual clicking, so
  it's reproducible).

**Final check:**
- Grafana dashboard loads and shows live-updating panels while the
  pipeline runs.
- Take a screenshot for the README (note this for Phase 2.4, don't
  do README yet).

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 2.3 — Load testing with Locust (~15-20 min)

**Do:**
- `load/locustfile.py`: simulate concurrent WebSocket clients + REST
  polling clients against the running API.
- Run a load test, capture real throughput/latency numbers.

**Final check:**
- Load test runs and completes without the API crashing.
- Append real results to BENCHMARKS.md (concurrent clients, sustained
  rate, p95/p99 API latency) — again, actual numbers from an actual run.

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 2.4 — Live OpenSky adapter (isolated add-on) (~20 min)

**Do:**
- `ingestor/opensky.py`: implements the SAME producer interface as
  `ingestor/synthetic.py` (they should share an interface/protocol
  defined back in 0.2 — if that wasn't set up cleanly, fix it now,
  don't hack around it). Pulls real ADS-B state vectors from the
  OpenSky REST API on a poll interval respecting their rate limits.
- Config flag to switch the pipeline's data source between synthetic
  and live OpenSky without code changes.
- Handle the real-world messiness explicitly: missing fields, API
  timeouts/errors (retry with backoff, don't crash the pipeline),
  rate-limit responses.

**Final check:**
- Pipeline runs successfully with `SOURCE=opensky` for at least 5
  minutes, real aircraft data visible in TimescaleDB and Grafana.
- Pipeline still runs successfully with `SOURCE=synthetic` — the
  switch doesn't break anything.
- If OpenSky is unreachable/rate-limited during this session, log
  that clearly in PROGRESS.md as a known limitation — do not fake
  live data to make the check "pass."

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 2.5 — Chaos test: consumer kill under live traffic (~10-15 min)

**Do:**
- With the full pipeline running (synthetic source is fine, doesn't
  need to be live OpenSky), kill a consumer process mid-stream and
  confirm the system recovers: lag drains back down, no data loss
  (idempotency holds), controller reacts sanely to the lag spike from
  the outage.
- Document this scenario and result in BENCHMARKS.md — it's a strong,
  concrete interview story ("what happens when a node dies").

**Final check:**
- System recovers without manual intervention within a reasonable
  time window (define and record what "recovered" means: lag back to
  near-zero, no duplicate/missing aggregates).

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 2.6 — CI pipeline (~10-15 min)

**Do:**
- `.github/workflows/ci.yml`: on push, run lint (ruff/black) + unit
  tests (not the full integration/load tests — those need live infra
  and don't belong in CI for a project this size). Keep it fast.

**Final check:**
- Push triggers the workflow, it passes on a clean run.

**Write PROGRESS.md entry. STOP. Wait for "continue".**

---

### Sub-phase 2.7 — README, architecture diagram, final assembly (~20 min)

**Do:**
- `README.md`: what it is, architecture diagram (mermaid or an
  embedded image), the three core claims stated plainly with their
  real numbers pulled from BENCHMARKS.md, quickstart
  (`docker compose up`), and a link to DESIGN_DECISIONS.md and
  BENCHMARKS.md for depth.
- Do NOT oversell. State clearly: synthetic generator is primary for
  benchmarking (controllable chaos), live OpenSky is a real,
  optional data source. This honesty is a strength, not a weakness —
  keep it in the README explicitly.
- Clean up any dead code, unused config, stray debug prints.

**Final check (Phase 2 exit gate — project considered code-complete):**
- [ ] `docker compose up` works from a truly clean checkout (delete
      local volumes/images and try again if unsure)
- [ ] README accurately describes what exists — no aspirational claims
- [ ] BENCHMARKS.md and DESIGN_DECISIONS.md are complete and consistent
      with the code
- [ ] All three hard-core claims are traceable from README → BENCHMARKS.md
      → the actual code that produced them

**Write final PROGRESS.md entry. STOP. Report full project status to
human.**

---

## IF TIME RUNS SHORT — PRIORITY ORDER TO CUT

If a sub-phase in Phase 2 is at risk, cut in this order (never cut
Phase 0 or Phase 1):
1. Locust load testing (2.3) — nice to have, not core
2. Live OpenSky adapter (2.4) — synthetic alone is defensible if
   framed honestly
3. Grafana dashboard polish (2.2) — raw Prometheus metrics without a
   pretty dashboard still proves observability
4. WebSocket fan-out (2.1) — REST-only is acceptable as a fallback
Never cut: watermark windowing, lag controller, replay determinism,
their three benchmark scripts, or DESIGN_DECISIONS.md entries.

---

## SESSION LOG FORMAT (for PROGRESS.md entries)

```
## [Phase X.Y] — YYYY-MM-DD
Built: <short summary>
Verified: <what final checks passed>
Deviations: <any change from this roadmap, and why>
Known issues: <anything unresolved, carried forward>
```

---

# HUMAN MODIFICATIONS (Claude Code: do not implement anything in this
# section — this is a placeholder for the human to fill in and do
# themselves after Phase 2 is complete)

- [ ] (to be filled in by Malay/Soumya after project completion)
