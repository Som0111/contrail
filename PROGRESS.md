# PROGRESS — Contrail session log

Read this file at the start of every session before doing anything else.
Entry format is defined at the bottom of `ROADMAP.md`.

## [Phase 0.1] — 2026-08-27
Built: Repo skeleton per roadmap layout (`src/{ingestor,windowing,control,replay,storage,api,common}`,
`tests/{unit,integration}`, `load/`, `scripts/`, `.github/workflows/`). `docker-compose.yml` with
Redpanda, TimescaleDB and Redis, each healthchecked, plus an `api` service built from `Dockerfile`
that waits on all three being healthy. `src/common/config.py` (pydantic-settings, 12-factor) with
`.env.example` committed and `.env` gitignored. FastAPI app with `/healthz` that probes all three
dependencies concurrently and reports per-dependency status.
Verified: `docker compose up -d --build` from a clean state brought redpanda, timescaledb and redis
to `healthy`; `GET /healthz` returned HTTP 200 with
`{"redpanda":"ok","timescaledb":"ok","redis":"ok"}`; clean initial commit in `git log`.
Deviations: Added an `api` service to compose (not spelled out in the roadmap) so that a single
`docker compose up` satisfies the `/healthz` check without host-side Python. Added `Dockerfile`,
`requirements.txt`, `requirements-dev.txt` for the same reason. `.github/workflows/` holds only a
`.gitkeep` — `ci.yml` is Phase 2.6.
Known issues: None.

## [Phase 0.2] — 2026-08-27
Built: `common/models.py` — `FlightState` (pydantic) with separate `event_time`/`ingest_time`,
a `partition_key` property over a 5-degree lat/lon `grid_cell()`, and a `dedup_key` natural key
for 0.3. `ingestor/base.py` — `EventSource` protocol, `ensure_topic()` (explicit partition count,
not the broker default of 1) and a source-agnostic `publish()`. `ingestor/synthetic.py` —
N aircraft on constant-turn-rate paths; pure seeded `simulate()` on a virtual clock plus a
wall-clock-paced `stream()`. All five chaos knobs (out-of-order prob + max skew, duplicate prob,
late prob + late delay, drop prob) are pydantic-settings fields with argparse overrides; nothing
hardcoded. Added a `dev` Dockerfile stage and a compose `tests` service (profile `dev`) so tests
run on Python 3.12 in-container — the host only has 3.14.
Verified: 12 unit tests pass. Ordering: chaos off gives arrival order == event order and
`ingest_time == event_time`; out-of-order prob 0.3 produces inversions while `ingest_time >=
event_time` still holds everywhere; duplicates/drops alone do not reorder. Rates over 20,000-event
samples: duplicate rate and drop rate both within 10% relative of configured 0.05 and 0.20; late-
event rate within 10% of 0.10 with all late arrivals >= the configured bound; skew bounded by
`max_skew_s`. Same seed + same `start_time` reproduces events exactly. Live: 60s run
(40 aircraft, 2 Hz, ooo 0.15 / dup 0.05 / late 0.02 / drop 0.01) published 4,996 events —
matches the 4,800 generated minus ~1% dropped plus ~5% duplicated — spread across all 6 partitions
(375/621/876/1129/997/998), verified with `rpk topic describe` and `rpk topic consume`.
`ruff check src tests` clean.
Deviations: Added `dev` build stage + `tests` compose service and `pytest.ini` (not in the
roadmap) because no Python 3.12 exists on the host. Chose the lat/lon grid over H3 — reasoning in
DESIGN_DECISIONS.md.
Known issues: Grid cells are unequal-area near the poles, so partition load is mildly skewed.
Contained to `grid_cell()` if it ever matters.

## [Phase 0.3] — 2026-08-27
Built: `storage/timescale.py` — `flight_events` hypertable on `event_time` with
`UNIQUE (icao24, event_time)`, batched `INSERT ... SELECT unnest(...) ON CONFLICT DO NOTHING`
returning the count of genuinely new rows (so suppression is measurable, not assumed), and a
deterministic `trace_id()`. `ingestor/sink.py` — `AIOKafkaConsumer` with `enable_auto_commit=False`
that writes each batch then commits, exiting on an idle timeout or SIGTERM. `common/logging.py` —
JSON-line formatter; every batch logs consumed/inserted/suppressed, a trace id and the per-partition
offset range.
Verified: 15 tests pass (12 unit, 3 integration), `ruff check` clean. Replay: producing a fixed
1,050-message batch containing real duplicates, draining it, then re-reading the whole topic from
offset 0 with a fresh consumer group — second pass consumed all 1,050 records, inserted 0, row count
unchanged at 950. Kill: `SIGKILL` on the sink *process* (not a cancelled task) mid-stream, then
restart on the same group — ran 3x, killed at 184/409/552 of 950 rows, restart re-read 890/640/440
records of which 124/99/86 were suppressed as already-written, and every run finished at exactly 950
rows with zero duplicate `(icao24, event_time)` keys. Integration suite run 3x back-to-back, no
flakiness.
Deviations: None.
Known issues: The suppressed counter mixes two causes — redelivery after the crash and the
generator's own duplicate-emission chaos — so it is not a clean measure of redelivery alone. The
proof does not depend on separating them (`final == unique` and zero duplicate keys do the work),
but `scripts/phase0_report.py` in 0.4 should report the two separately.

## [Phase 0.4] — 2026-08-27
Built: `scripts/phase0_report.py` only — no new pipeline features. It reads message counts from
Redpanda's partition offsets and row counts/disorder statistics from TimescaleDB, and reconciles the
two. Created `BENCHMARKS.md` with a header (missed in 0.1; Phase 1 fills it in).
Verified: 5-minute end-to-end run, generator -> Redpanda -> sink -> TimescaleDB, at 60 aircraft x
2 Hz with chaos ooo 0.15 / max-skew 8s / dup 0.05 / late 0.02 at 90s / drop 0.01. Generator
published 37,491 messages; sink consumed 37,491, inserted 35,643, suppressed 1,848. Report output
reconciles exactly (35,643 + 1,848 == 37,491) and every chaos knob is recoverable from it:
duplicates 4.93% (configured 5%), drops 357 of 36,000 generated = 0.99% (configured 1%), events
later than the 60s bound 1.89% (configured 2%), arrival-lag p95 6.23s (under the configured 8s
max skew), max 179.86s (under 2x the configured 90s late delay). Event-time inversions 8.45% of
stored rows. Traffic spread over all 6 partitions and 70 geographic cells, 60 distinct aircraft,
event_time span exactly 300s. Full clean-boot check: `docker compose down -v --remove-orphans`
then `docker compose up -d` — all four services healthy in 13.5s with no manual steps, `/healthz`
200 with all three dependencies ok. 15 tests pass, `ruff check src tests scripts` clean.
Deviations: Found and fixed a real bug during the run rather than working around it — the sink
auto-created the topic with one partition when it started before the generator, silently capping
parallelism. The sink now calls the same `ensure_topic()` the producer does. Also mounted
`./scripts` into the `tests` container so lint covers it.
Known issues: None carried forward. The 0.3 note about the `suppressed` counter mixing redelivery
with generator duplicates is resolved for reporting purposes — the report derives duplicates from
topic-vs-table reconciliation, which is unambiguous.

### Phase 0 exit gate
- [x] `docker compose up` works from a clean checkout with no manual steps (verified after
      `down -v`, 13.5s to all-healthy)
- [x] Generator chaos parameters are config-driven and verified by tests (12 unit tests; rates
      measured within 10% relative over 20,000-event samples)
- [x] Idempotent sink proven under consumer restart (0.3: SIGKILL mid-stream x3, exact unique row
      count, zero duplicate keys, 86-124 records re-delivered and absorbed per run)
- [x] `phase0_report.py` output is internally consistent (stored + suppressed == received, and all
      four chaos rates recoverable from the output)
- [x] `DESIGN_DECISIONS.md` has entries for partitioning scheme (0.2) and idempotency key (0.3)

## [Phase 1.1] — 2026-08-27
Built: `windowing/aggregates.py` — shared `WindowAggregate` shape, `window_start()` flooring,
an `Aggregator` that deduplicates on `(icao24, event_time)` as it folds, and `ground_truth()`
(attribution by `event_time` over arrived events). `windowing/naive.py` — the deliberate baseline,
tumbling windows keyed on arrival time, aggregating count / avg altitude / avg velocity per
(window, geo cell), with a CLI that prints both its own output and ground truth side by side.
Added `collect()` to `ingestor/base.py` to read a topic into memory in arrival order.
Verified: 22 tests pass (7 new), `ruff check src tests scripts` clean. Controlled hand-built
sequence: three events all belonging to window A, one delayed from 00:50 to arrival at 01:30 —
ground truth puts all 3 in window A (avg altitude 4000 m); naive puts 2 in window A (avg altitude
1500 m, off by exactly 2500 m) and invents a window B holding the stolen event. Asserted directly,
not eyeballed. Also asserted: naive is *exactly* equal to ground truth when nothing is delayed
(the baseline is fragile, not broken — this is the sanity check on the harness itself), duplicates
are counted once on both sides, disorder conserves the total event count (naive misfiles, it does
not lose), and late arrivals push naive past the end of the real event span. Live run: generator
180s at 40 aircraft x 2 Hz, chaos ooo 0.20 / max-skew 20s / dup 0.05 / late 0.03 at 90s / drop 0.01
-> 14,960 events consumed, naive produced 259 windows against ground truth's 167 (92 invented by
late arrivals), and 164 of 167 real windows (98.2%) disagreed on count.
Deviations: None.
Known issues: None. The 98.2% figure is a single ad-hoc run and is NOT the headline number — 1.3
produces the real measurement by sweeping disorder levels over a replayed stream.
