# Operating Contrail

Everything here is about running the stack, as opposed to what it proves (see the README and
[BENCHMARKS.md](../BENCHMARKS.md)). Statements are marked **implemented**, **verified** (an
automated test exercised it against the real thing) or **not provided**. Where a behaviour is
implemented but only unit-tested, it says so.

## Local demo versus production

`docker compose up` is a **local demo**. It is deliberately zero-configuration, which means it is
unsafe to expose:

| | local demo (default) | production |
|---|---|---|
| `APP_ENV` | `development` | `production` |
| API login | `operator` / `contrail` | your own `API_USER` / `API_PASSWORD`; startup **fails** on the default |
| JWT signing key | placeholder, so a random per-process key is used and tokens die on restart | `JWT_SECRET`, at least 32 characters; startup **fails** on the placeholder or a short key |
| ports for broker, databases, Prometheus, Grafana | loopback only (`BIND_HOST=127.0.0.1`) | leave on loopback and put a proxy or private network in front |
| API port | `0.0.0.0:8000` | set `API_BIND_HOST` as needed, terminate TLS in front of it |
| Grafana | anonymous viewer, `admin`/`admin` | `GRAFANA_ANONYMOUS=false`, `GRAFANA_ADMIN_PASSWORD` set |
| client address for rate limits | the TCP peer | `TRUSTED_PROXY_HOPS=<number of proxies>` if behind any |
| window-state recovery | on, same as production | same |
| `IDLE_TIMEOUT_S` | unset | set it (greater than `ALLOWED_LATENESS_S`) if a partition can go quiet |
| `RETENTION_DAYS` | unset, keeps everything | set it, or `flight_events` grows without bound |

Generate secrets with `python -c "import secrets; print(secrets.token_urlsafe(48))"`. Never commit a
`.env`; `.env.example` is local-only and none of its values are production-safe.

**Not provided, so do not assume it:** TLS (terminate it in a proxy), per-user accounts (there is one
operator account), rate limits shared between API replicas (they are per process), a secrets
manager, or high availability for Redpanda, TimescaleDB or Redis (each runs as a single node in the
compose file). `POSTGRES_PASSWORD` is not checked by production mode.

Other facts worth knowing before deploying:

- **Images** run as an unprivileged `contrail` user (uid 10001); the test image runs as root.
  Compose images are pinned to version tags, not digests; Dependabot proposes bumps.
- **Dependencies** are installed from hash-locked files (`requirements.txt`, generated from
  `requirements.in`). A weekly job runs `pip-audit`; as of the last run Starlette 0.41.3 has known
  advisories whose fix needs a FastAPI upgrade that has not been taken.
- **Tokens** live `JWT_TTL_S` seconds (default 3600). A WebSocket is closed when its token expires.
- **Persistent data:** only the `timescale-data` volume (events and the dead-letter table).
  `docker compose down` keeps it, **`docker compose down -v` deletes it**. Redis, Redpanda,
  Prometheus and Grafana state is ephemeral in the compose file.
- **Stopping:** `windowing`, `pipeline` and `timescaledb` have grace periods (30 s, 30 s, 60 s) and
  forward SIGTERM so they flush before exit. Checked by hand: `docker compose stop windowing` logs
  "service stopped" and exits in about two seconds; there is no automated test of the wrapper.

## Configuration reference

Every value is an environment variable (also read from `.env`). Invalid values stop the process at
startup with a message naming the variable. Secrets are marked; keep them out of images and git.

| variable | default | valid | secret | meaning |
|---|---|---|---|---|
| `KAFKA_BOOTSTRAP` | `redpanda:29092` | host:port | no | Redpanda/Kafka bootstrap address |
| `KAFKA_RAW_TOPIC` | `flight.events.raw` | topic name | no | topic carrying raw flight events |
| `KAFKA_PARTITIONS` | `6` | > 0 | no | partitions the topic is created with; also the parallelism ceiling |
| `KAFKA_CONSUMER_GROUP` | `contrail-sink` | group id | no | consumer group of the sink workers |
| `SOURCE` | `synthetic` | `synthetic` or `opensky` | no | which feed the ingestor publishes |
| `OPENSKY_POLL_INTERVAL_S` | `15.0` | seconds | no | OpenSky poll period |
| `OPENSKY_BBOX` | `45,5,55,15` | `lamin,lomin,lamax,lomax`, min < max, or empty | no | OpenSky bounding box; empty = whole world |
| `GEN_AIRCRAFT` | `50` | > 0 | no | synthetic fleet size |
| `GEN_RATE_HZ` | `1.0` | > 0 | no | synthetic events per aircraft per second |
| `GEN_SEED` | `1337` | integer | no | synthetic generator seed |
| `CHAOS_OUT_OF_ORDER_PROB` | `0.0` | 0 - 1 | no | synthetic: probability an event arrives out of order |
| `CHAOS_MAX_SKEW_S` | `5.0` | &ge; 0 | no | synthetic: maximum arrival skew of an out-of-order event |
| `CHAOS_DUPLICATE_PROB` | `0.0` | 0 - 1 | no | synthetic: probability of a duplicate delivery |
| `CHAOS_LATE_PROB` | `0.0` | 0 - 1 | no | synthetic: probability of a very late arrival |
| `CHAOS_LATE_DELAY_S` | `90.0` | &ge; 0 | no | synthetic: delay of a very late arrival |
| `CHAOS_DROP_PROB` | `0.0` | 0 - 1 | no | synthetic: probability an event is dropped |
| `WINDOW_S` | `60` | > 0 | no | tumbling window length |
| `ALLOWED_LATENESS_S` | `30.0` | &ge; 0 | no | watermark lateness bound; exact output needs it &ge; the real arrival skew |
| `IDLE_TIMEOUT_S` | `unset` | unset, or > `ALLOWED_LATENESS_S` | no | wall-clock seconds of silence before a partition stops holding back the watermark; unset keeps replay clock-free |
| `RETENTION_DAYS` | `unset` | unset, or > 0 | no | drop `flight_events` chunks older than this; unset keeps everything |
| `POSTGRES_HOST` | `timescaledb` | host | no | TimescaleDB host |
| `POSTGRES_PORT` | `5432` | port | no | TimescaleDB port |
| `POSTGRES_DB` | `contrail` | name | no | database name |
| `POSTGRES_USER` | `contrail` | name | no | database user |
| `POSTGRES_PASSWORD` | `contrail` | string | **yes** | database password; not checked by production mode, change it yourself |
| `REDIS_URL` | `redis://redis:6379/0` | URL | if it holds a password | Redis connection |
| `REDIS_KEY_PREFIX` | `contrail` | non-empty string | no | namespace for the window channel and Redis keys; the default keeps the historic names, change it to run an independent pipeline on the same Redis |
| `APP_ENV` | `development` | `development` or `production` | no | `production` refuses to start on default credentials |
| `JWT_SECRET` | `dev-only-change-me` | &ge; 32 chars in production | **yes** | HS256 signing key; with the placeholder, a random per-process key is used and tokens die on restart |
| `JWT_TTL_S` | `3600` | > 0 | no | access token lifetime in seconds |
| `API_USER` | `operator` | string | no | the single operator account |
| `API_PASSWORD` | `contrail` | string, not the default in production | **yes** | operator password |
| `LOGIN_RATE_LIMIT_RPS` | `0.5` | > 0 | no | refill rate of the per-client login bucket |
| `LOGIN_RATE_LIMIT_BURST` | `10.0` | > 0 | no | burst size of the per-client login bucket |
| `RATE_LIMIT_RPS` | `10.0` | > 0 | no | refill rate of the per-client API bucket |
| `RATE_LIMIT_BURST` | `20.0` | > 0 | no | burst size of the per-client API bucket |
| `RATE_LIMIT_IDLE_TTL_S` | `600.0` | > 0 | no | idle buckets are dropped after this long (keep `ttl x rps` &ge; burst) |
| `RATE_LIMIT_MAX_BUCKETS` | `10000` | > 0 | no | hard cap on buckets held in memory |
| `TRUSTED_PROXY_HOPS` | `0` | &ge; 0 | no | reverse proxies in front of the API that append `X-Forwarded-For`; 0 ignores the header |
| `LOG_LEVEL` | `INFO` | Python logging level | no | log verbosity |
| `METRICS_PORT` | `9100` | port, 0 disables | no | Prometheus `/metrics` port for non-HTTP processes |

Compose-only variables: `BIND_HOST` (default `127.0.0.1`), `API_BIND_HOST` (default `0.0.0.0`),
`GRAFANA_ANONYMOUS` (default `true`), `GRAFANA_ADMIN_PASSWORD` (default `admin`, **secret**).

## Delivery and failure semantics

| path | semantics | status |
|---|---|---|
| source to Redpanda | at-least-once; a duplicate is possible | implemented |
| Redpanda to TimescaleDB (sink) | at-least-once, made safe by `UNIQUE (icao24, event_time)`; offsets commit only after the write | **verified** (SIGKILL test, duplicate-free row count) |
| Redpanda to Redis (windowing) | at-least-once. Offsets commit only up to the oldest record still in an open window; on restart the watermark waits for every partition that has a backlog; republishing a window is idempotent because a stale or smaller aggregate never overwrites a newer or larger one | **verified** in one scenario: an integration test kills and restarts the service (SIGKILL, SIGTERM) and checks every window against ground truth |
| Redis to WebSocket clients | best-effort; pub/sub keeps nothing for absent clients. Reconcile with the snapshot or `GET /api/windows` | implemented, unit-tested |
| poison records in the sink | unparseable or permanently rejected records go to `flight_events_dead`; the batch continues | **verified** for an unparseable and a contract-violating record; the permanent-database-error path is unit-tested only |
| late events | counted side output; the finalized aggregate is not corrected | implemented |
| duplicate suppression (windowing) | within a rolling event-time horizon of `max(10, ceil(lateness / window) + 1)` windows; past it a repeat is reported as late, never double counted | implemented, unit-tested |

Nothing here is exactly-once. A crash after publishing to Redis but before committing offsets
replays records; the late-event total in `/api/status` can count some of them a second time.

## Recovery runbook

For each service: what happens when it restarts, what is kept, what is lost. "Verified" means an
automated or recorded run showed it; otherwise it is the intended behaviour from the code.

| service | on restart | kept | lost |
|---|---|---|---|
| **generator** | resumes publishing; synthetic stream restarts from its seed | everything already in Redpanda | the in-progress synthetic timeline (a new one begins) |
| **Redpanda** | consumers reconnect and resume from committed offsets | topic data survives a container restart; the compose file has **no volume**, so nothing survives recreating the container | all topics and committed offsets on recreation, including any replay recording |
| **pipeline** (sink workers + controller) | the restart policy restarts it; workers resume from committed offsets; replayed rows are suppressed by the database key | committed offsets, every stored row. **Verified** by the recorded chaos run (`scripts/chaos_kill.sh`, in BENCHMARKS.md) for SIGKILL | the controller's lag history and current pool size (it starts at the minimum) |
| **windowing** | resumes from the last safe offset, waits for every backlog partition to deliver, and rebuilds still-open windows from replay; a re-finalized window cannot overwrite a larger one in Redis | published windows in Redis; offsets up to the oldest open window | in-memory open windows and the dedup set, rebuilt by replay. Records consumed before the first commit of a fresh group are read from `latest`, so a brand-new group skips history. Kill-and-restart is covered by an integration test (one scenario) |
| **timescaledb** | crash recovery from WAL; the sink retries transient errors with backoff, then stops uncommitted and is restarted | the `timescale-data` volume | writes in flight (replayed) |
| **redis** | no volume: data survives a container restart only as far as Redis's default snapshots, and is gone when the container is recreated | whatever the last snapshot held | on recreation: the latest-per-cell hash, the running late-event total, pub/sub state; the hash refills as windows finalize, the late total restarts at zero |
| **api** | stateless; reconnects to dependencies at startup and on each failed health probe | nothing needed | issued tokens if `JWT_SECRET` is unset (random per-process key); open WebSockets (clients reconnect) |
| **prometheus / grafana** | no volume: history survives a restart, not recreation | provisioned dashboards and config | metric history on recreation (6 h retention anyway) |

Health: `GET /healthz` reports Redpanda, TimescaleDB and Redis (cached for one second) and returns 503
if any is down; `/api/status` returns 503 when the database is unreachable. Alert-worthy metrics:
`contrail_sink_retries_exhausted_total`, `contrail_idle_partitions`,
`contrail_events_total{outcome="quarantined"}`, `contrail_sink_partition_lag`, and
`contrail_watermark_skew_wallclock_seconds` growing without `contrail_watermark_skew_event_seconds`
moving (a stalled watermark). The windowing service also exports `contrail_windowing_records_total`
and `contrail_windowing_consumer_lag`.

## Implemented, known limitation, future work

**Implemented:** event-time windowing with per-partition watermarks; opt-in idle-partition
exclusion; bounded dedup, late and window state; safe-offset window recovery (see the caveat above);
idempotent sink with retries and a dead-letter table; sealed deterministic replay; adaptive control
and shedding; JWT auth, login throttling, bounded rate limiter; strict event and config validation.

**Known limitations:**
- Window-state recovery is integration-tested in a single kill scenario; other kill points and
  repeated crashes are not covered. The sink's permanent-database-error path is unit-tested only.
- Redis's latest-per-cell hash only moves forward in event time, so older-event-time data replayed
  through the live service is not published for a cell until the hash is cleared.
- With `IDLE_TIMEOUT_S` unset (the default), one silent partition stalls window finalization, and a
  partition that has never delivered an event is not tracked even when it is set.
- Redis is a single node without persistence; the API's rate limits are per process.
- Starlette advisories remain until FastAPI is upgraded (needs an integration run to validate).
- Windows are never corrected after finalization; late events only go to the side output.

**Future work:** a wider kill-and-restart matrix for the windowing service (repeated and
multi-point crashes); rate limits and the late total in a shared store if the API is replicated;
checkpointing of window state alongside offsets; benchmark re-runs with `--runs 3` and the
controller sweep, so claims 2 and 2b carry a spread.
