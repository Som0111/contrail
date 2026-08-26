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
