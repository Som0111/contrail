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
