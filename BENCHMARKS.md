# BENCHMARKS — Contrail

Measured numbers only. Every section records the exact configuration that produced it, so any
result here can be re-run. Nothing in this file is estimated, extrapolated, or illustrative.

Machine for all runs unless stated otherwise: Intel Core i7-5500U @ 2.40 GHz (2 cores / 4 threads),
15.9 GB RAM, Windows 10 Pro 10.0.19045, Docker Desktop 29.7.2 (WSL2, kernel 6.18.33.2), containers on Python 3.12.14 / glibc 2.41.

---

# Claim #1 — Event-time windowing vs a processing-time baseline

**Reproduce:** `docker compose run --rm --no-deps tests python -m scripts.benchmark_windowing`

## Configuration

| | |
|---|---|
| window | 60s tumbling, keyed by (window, 5-degree geo cell) |
| stream | 50 aircraft @ 1 Hz, 1800s of event time = 90,000 events generated per level |
| seed / epoch | 20260827, `start_time` pinned to 2026-01-01T00:00Z |
| reproducibility | byte-identical between runs, verified by diffing two consecutive runs. The seed alone is *not* enough: `start_time` defaults to `now()`, which slides every timestamp and moves events across window boundaries, shifting the figures by a few tenths of a percent. The epoch is pinned for exactly that reason. |
| ground truth | every *arrived* event attributed by its own `event_time`, deduplicated |
| tolerance | window counts compared exactly; averages to 0.1% relative |
| dedup | identical on both processors, so measured error is attribution error and nothing else |
| runtime | ~85s for the whole sweep |

Disorder is swept in two groups because the generator has two mechanisms of very different magnitude:
**BOUNDED** is out-of-order arrival within `max_skew_s`, which the watermark guarantee covers;
**UNBOUNDED** adds 90-240s late arrivals that no sane lateness bound can absorb.

## Headline

At the operationally sane setting (allowed lateness L = max arrival skew):

| disorder | events misplaced, naive | events misplaced, watermark | windows wrong, naive | windows wrong, watermark | silent errors |
|---|---|---|---|---|---|
| none | 0.00% | 0.00% | 0.00% | 0.00% | 0 -> 0 |
| low (skew 5s) | 0.18% | **0.00%** | 19.00% | **0.00%** | 157 -> 0 |
| medium (skew 20s) | 3.06% | **0.00%** | 80.95% | **0.00%** | 2,725 -> 0 |
| high (skew 45s) | 13.66% | **0.00%** | 95.94% | **0.00%** | 12,049 -> 0 |
| medium + late arrivals | 4.89% | 1.86% | 88.99% | 66.36% | 4,358 -> 0 |
| high + late arrivals | 18.08% | 4.43% | 98.38% | 89.74% | 15,945 -> 0 |

**Under bounded disorder the watermark engine is exactly correct — not approximately, not
within tolerance. Zero misplaced events out of 88,199, against 12,049 for the baseline.**

Three things the table is careful about:

1. *Window error rate is a sensitivity measure, not a magnitude.* At `low`, naive misplaces only
   0.18% of events but corrupts 19.00% of windows -- one stray event out of ~57 is enough to make a
   window's aggregate wrong. Both columns are shown because either alone misleads.
2. *Worst-case matters more than the mean.* Under `high` disorder the worst single window's aircraft
   count was off by 18 (see the per-level tables below).
3. *`silent` is the column that matters operationally.* Every event the baseline misplaces is
   misplaced with no record of it -- the aggregate is simply, quietly wrong. Every event the
   watermark engine cannot place goes to a counted side output, logged with its window and the
   watermark that closed it. Under `high+late`, that is 15,945 silent errors against 0.

## Verifying the guarantee

The property the design rests on is `allowed_lateness >= max arrival skew => output is identical to
ground truth`. The sweep confirms it and shows the degradation below the bound is monotone:

| disorder | L=0 | L=skew/2 | **L=skew** | L=2*skew |
|---|---|---|---|---|
| low (skew 5s) | 9.99% | 1.00% | **0.00%** | 0.00% |
| medium (skew 20s) | 80.68% | 34.04% | **0.00%** | 0.00% |
| high (skew 45s) | 95.47% | 79.27% | **0.00%** | 0.00% |

(window error rate). Under UNBOUNDED disorder no bound below the late-arrival delay reaches zero,
which is the honest and expected result -- `medium+late` only reaches 0.00% at L=200s, `high+late`
at L=285s. Those bounds are far too wide to be useful in production, which is precisely why the
side output exists rather than an ever-growing lateness allowance.

## Full run

```
CONTRAIL — core claim #1: event-time windowing vs processing-time baseline
========================================================================================================
window 60s | 50 aircraft @ 1 Hz | 1800s of event time | seed 20260827
python 3.12.14 on Linux-6.18.33.2-microsoft-standard-WSL2-x86_64-with-glibc2.41
'silent' = events placed wrongly with no record of it. The whole point:
the baseline cannot tell you it was wrong, the watermark engine always can.
BOUNDED DISORDER
--------------------------------------------------------------------------------------------------------
  none         ooo 0.00 @ skew 5s, dup 0.05, drop 0.01
    stream: 90,000 generated, 93,589 arrived, 89,128 unique, 1,501 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -      0 / 1,501     0.00%                 0    0.00%             0         0
    watermark       0s      0 / 1,501     0.00%                 0    0.00%             0         0
    watermark     2.5s      0 / 1,501     0.00%                 0    0.00%             0         0
    watermark       5s      0 / 1,501     0.00%                 0    0.00%             0         0  <- L = max skew
    watermark      10s      0 / 1,501     0.00%                 0    0.00%             0         0
  low          ooo 0.05 @ skew 5s, dup 0.02, drop 0.01
    stream: 90,000 generated, 90,885 arrived, 89,114 unique, 1,501 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -    286 / 1,505    19.00%               157    0.18%             2       157
    watermark       0s    150 / 1,501     9.99%               153    0.17%             2         0
    watermark     2.5s     15 / 1,501     1.00%                15    0.02%             1         0
    watermark       5s      0 / 1,501     0.00%                 0    0.00%             0         0  <- L = max skew
    watermark      10s      0 / 1,501     0.00%                 0    0.00%             0         0
  medium       ooo 0.20 @ skew 20s, dup 0.05, drop 0.01
    stream: 90,000 generated, 93,466 arrived, 89,133 unique, 1,501 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -  1,249 / 1,543    80.95%             2,725    3.06%             8     2,725
    watermark       0s  1,211 / 1,501    80.68%             2,645    2.97%             8         0
    watermark      10s    511 / 1,501    34.04%               598    0.67%             3         0
    watermark      20s      0 / 1,501     0.00%                 0    0.00%             0         0  <- L = max skew
    watermark      40s      0 / 1,501     0.00%                 0    0.00%             0         0
  high         ooo 0.40 @ skew 45s, dup 0.10, drop 0.02
    stream: 90,000 generated, 96,938 arrived, 88,199 unique, 1,500 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -  1,511 / 1,575    95.94%            12,049   13.66%            18    12,049
    watermark       0s  1,432 / 1,500    95.47%            11,670   13.23%            27         0
    watermark    22.5s  1,189 / 1,500    79.27%             2,598    2.95%             7         0
    watermark      45s      0 / 1,500     0.00%                 0    0.00%             0         0  <- L = max skew
    watermark      90s      0 / 1,500     0.00%                 0    0.00%             0         0
UNBOUNDED DISORDER (late arrivals no bound can absorb)
--------------------------------------------------------------------------------------------------------
  medium+late  ooo 0.20 @ skew 20s, dup 0.05, late 0.020 @ 90-180s, drop 0.01
    stream: 90,000 generated, 93,465 arrived, 89,128 unique, 1,501 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -  1,463 / 1,644    88.99%             4,358    4.89%            11     4,358
    watermark       0s  1,365 / 1,501    90.94%             4,213    4.73%            10         0
    watermark      10s  1,125 / 1,501    74.95%             2,244    2.52%             7         0
    watermark      20s    996 / 1,501    66.36%             1,659    1.86%             6         0  <- L = max skew
    watermark      40s    992 / 1,501    66.09%             1,643    1.84%             6         0
    watermark     200s      0 / 1,501     0.00%                 0    0.00%             0         0
  high+late    ooo 0.40 @ skew 45s, dup 0.10, late 0.050 @ 120-240s, drop 0.02
    stream: 90,000 generated, 97,033 arrived, 88,197 unique, 1,501 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -  1,759 / 1,788    98.38%            15,945   18.08%            21    15,945
    watermark       0s  1,445 / 1,501    96.27%            15,437   17.50%            29         0
    watermark    22.5s  1,432 / 1,501    95.40%             6,631    7.52%            15         0
    watermark      45s  1,347 / 1,501    89.74%             3,903    4.43%            10         0  <- L = max skew
    watermark      90s  1,280 / 1,501    85.28%             3,510    3.98%            10         0
    watermark     285s      0 / 1,501     0.00%                 0    0.00%             0         0
========================================================================================================
HEADLINE  (watermark at L = max skew, the operationally sane setting)
  none         events misplaced   0.00% ->  0.00%   |  windows wrong   0.00% ->   0.00%   |  silent errors 0 -> 0
  low          events misplaced   0.18% ->  0.00%   |  windows wrong  19.00% ->   0.00%   |  silent errors 157 -> 0
  medium       events misplaced   3.06% ->  0.00%   |  windows wrong  80.95% ->   0.00%   |  silent errors 2,725 -> 0
  high         events misplaced  13.66% ->  0.00%   |  windows wrong  95.94% ->   0.00%   |  silent errors 12,049 -> 0
  medium+late  events misplaced   4.89% ->  1.86%   |  windows wrong  88.99% ->  66.36%   |  silent errors 4,358 -> 0
  high+late    events misplaced  18.08% ->  4.43%   |  windows wrong  98.38% ->  89.74%   |  silent errors 15,945 -> 0
  completed in 93.5s
```

---

# Claim #2 — Adaptive lag control vs a static worker pool

**Reproduce:** `docker compose run --rm --no-deps api python -m scripts.benchmark_control`

## Configuration

| | |
|---|---|
| load | 60 aircraft; 120 ev/s for 30s -> **720 ev/s (6x) for 70s** -> 120 ev/s for 40s -> 60s drain |
| per-worker cap | 250 ev/s (`--max-rate`), so worker count is the binding constraint |
| static arm | pinned at 1 worker; the controller still runs and logs, but nothing it decides is applied |
| adaptive arm | 1-4 workers, 2s sample interval, 6s cooldown |
| latency | `processed_at - event_time` read back from TimescaleDB: aircraft state -> row durably committed |
| chaos | off — injected arrival skew adds a constant to both arms and dilutes the queueing delay under test |
| isolation | each arm gets a fresh topic and consumer group, never a recreated one |
| seed / runtime | 20260827 / 403s total |

Both arms run identical load, identical caps and identical lag sampling. The only difference is
whether the controller's decisions are applied, so the gap below is adaptation, not instrumentation.

## Results

| | static (1 worker) | adaptive (1-4) | |
|---|---|---|---|
| events absorbed | 44,640 / 58,800 (**75.9%**) | 58,800 / 58,800 (**100%**) | |
| peak lag | 33,480 | **10,780** | 3.1x lower |
| final lag | **14,660 — never recovered** | **0** | fully drained |
| p50 latency | 51.56s | **9.74s** | 5.3x lower |
| p95 latency | 112.80s | **17.71s** | 6.4x lower |
| p99 latency | 115.50s | **21.96s** | 5.3x lower |
| max latency | 116.76s | **27.79s** | 4.2x lower |

The headline is not really the percentile ratio. It is that **the static arm never recovers**: 60
seconds after the burst ended it was still 14,660 events behind and had dropped 24% of the load on
the floor, while the adaptive arm was fully drained at zero. The latency figures understate the
difference, because the static arm's worst events are the ones it never processed at all and so are
absent from its own percentiles.

## Controller actions during the burst

```
scale_up    lag=  4,200 slope= +428.7/s t=18.90 -> workers=2
scale_up    lag=  7,520 slope= +533.2/s t=30.54 -> workers=3
scale_up    lag=  9,400 slope= +394.7/s t=10.44 -> workers=4
scale_down  lag=      6 slope= -281.1/s t=10.53 -> workers=3
scale_down  lag=      0 slope=  -69.2/s t= 2.96 -> workers=2
scale_down  lag=     60 slope=  +1.7/s  t= 0.60 -> workers=1
```

Three scale-ups during the ramp, then the pool is held at 4 for the whole drain and released only
once the backlog is essentially gone.

## What this run does NOT show

**Load shedding never triggered here, so this benchmark measures the scaling half of claim #2 only.**
Four workers at 250 ev/s each is 1,000 ev/s against a 720 ev/s burst, so scaling absorbed the load
completely and the shed path was correctly never reached. Shedding is exercised in the Phase 1.4
integration run (at max workers with lag still growing 248/s for 3 consecutive samples: 1,816 events
dropped across 12 named geographic cells, released once lag drained at -207/s), and can be forced
here with `--max-workers 2`. That variant has not been run, so no shed figures are claimed in this
table.

Also note the static arm is pinned at **one** worker. That is the honest baseline for "no
adaptation", but it is not the strongest possible fixed configuration: a static pool of 4 would have
absorbed this particular burst too, at the cost of running 4 workers permanently. The claim is about
reacting to load without over-provisioning for peak, not about beating any fixed pool at any size.

---

# Claim #3 — Deterministic replay

**Reproduce:** `docker compose run --rm tests pytest -q -s tests/integration/test_replay_determinism.py`
or, standalone: `python -m src.replay.harness --record --runs 3`

## Configuration

| | |
|---|---|
| recording | 30 aircraft x 300s of event time, chaos ooo 0.25 @ 15s skew, dup 0.05, late 0.02 @ 120-240s, drop 0.01 |
| messages replayed | **9,321** per replay, over 6 Kafka partitions |
| pipeline | `collect()` -> watermark event-time windowing, 60s windows, 30s allowed lateness |
| output hashed | SHA-256 over a canonical rendering: windows sorted, counts exact, means at fixed 4dp |
| runs | 3 replays per invocation, invocation repeated 3x (9 replays total) |

## Results

| check | result |
|---|---|
| 3 replays of one recording | **identical digest, 3/3** |
| windows produced | 185, identical across replays |
| events collected | 9,321, identical across replays |
| replay killed mid-stream, restarted | **digest matches the uninterrupted run** |
| same events over 1 partition vs 6 | **identical digest** |
| invocation repeated 3x | 4/4 tests pass each time, no flakes |

Example (one invocation): `01ee39c42205d038a3f8735502ebf6784a58c7ec6d7fa7fa7689444bf74ddef7`,
produced by three independent replays and again by a replay that was `SIGKILL`ed three seconds in
and restarted from scratch.

## What "identical" does and does not mean here

**The digest is stable across replays of one recording, not across recordings.** Each invocation of
the test lays down a fresh recording whose `start_time` is `now()`, so its absolute timestamps --
and therefore its window boundaries and its digest -- differ from the last invocation's. That is
correct: the claim is that replaying *the same bytes* yields the same aggregate, not that the
generator emits the same bytes forever. The invariants that *are* stable across invocations are the
structural ones: 9,321 messages and 185 windows every time.

## Why it holds

Three properties, all load-bearing, all of which were broken at some point during development:

1. **A total order on the input.** `collect()` sorts by `(ingest_time, icao24, event_time)`. Sorting
   on arrival instant alone is not enough: one generator tick emits a whole fleet sharing an
   `ingest_time`, and Python's stable sort would leave those ties in whatever order the broker
   interleaved the six partitions that run. The 1-partition-vs-6 test exists to pin this down.
2. **Folding that cannot leak summation order.** Means are rounded at finalisation, so float
   addition -- which is not associative -- cannot put the accumulation order into the output.
3. **No wall clock in the hashed output.** Windows key on `event_time`, the watermark advances on
   `event_time`, `trace_id` is derived from event content. Nothing the machine stamps at replay time
   reaches the digest.

The mid-stream-kill result rests on a fourth point, which is the operationally interesting one:
a replay is a pure function of the recorded topic, so a crash costs progress and nothing else. There
is no partial state to reconcile and no checkpoint to repair -- the restarted replay reads from
offset 0 and lands on the same digest. Combined with the Phase 0.3 result (the *sink* survives
`SIGKILL` with zero duplicate rows, via the database idempotency key), the pipeline is
crash-safe at both ends: writes are idempotent, and reads are reproducible.

---

# API load test (Locust)

**Reproduce:**
```
RATE_LIMIT_RPS=100000 RATE_LIMIT_BURST=100000 docker compose up -d api
docker compose run --rm --no-deps tests \
  locust -f /app/load/locustfile.py --headless -u 100 -r 20 -t 120s --host http://api:8000
docker compose up -d api        # restore the default 10 rps limit
```

## Configuration

| | |
|---|---|
| load profile | 4:1 mix of REST pollers to WebSocket subscribers; REST tasks weighted 6:2:1 across `/api/windows`, `/api/status`, `/healthz` |
| think time | 0.5-2.0s between REST tasks |
| rate limiting | raised to effectively unlimited for the run — the limiter is **per client IP** and a load generator is one IP, so at the default 10 rps this would measure the limiter, not the API. 429s are counted, not hidden, so a misconfigured run is visible in the results. |
| environment | everything co-resident on one 2-core i7-5500U: API, generator, adaptive pipeline, windowing service, Redpanda, TimescaleDB, Redis, Prometheus, Grafana, **and Locust itself** |

## Results

| | 20 users, 60s | 100 users, 120s |
|---|---|---|
| requests | 860 | 4,296 |
| throughput | 14.3 req/s | **36.9 req/s** |
| failures | 0 | 4 (0.09%) — WebSocket connect timeouts during ramp |
| `/api/windows` p50 | **25 ms** | 1,000 ms |
| `/api/windows` p95 | **200 ms** | 3,400 ms |
| `/api/windows` p99 | 320 ms | 7,000 ms |
| `/api/status` p95 | 400 ms | 5,800 ms |
| aggregate p95 | **240 ms** | 3,700 ms |
| WebSocket windows delivered | — | 574 (4.93/s), p95 2 ms |

**The 100-user column is a saturation datapoint, not the API's capability.** With ten containers and
the load generator sharing two cores, that run is CPU-bound on the host — `docker stats` showed the
API alone at ~62% CPU with TimescaleDB, Redpanda and the pipeline competing for what was left. The
20-user column, where the box is not starved, is the honest measure of the API's own latency:
**p50 25 ms and p95 200 ms** to read aggregates out of Redis. Neither figure is a claim about
capacity on real hardware; both were measured here and are reported as measured.

WebSocket delivery is essentially free once connected — p95 of 2 ms to hand a finalized window to a
subscriber, because the API is only relaying a Redis pub/sub message it already holds.

## The finding: connection-per-request

The first run of this test produced **p95 4,900 ms on `/api/windows`, 9,300 ms on `/api/status`, and
35.65% of `/healthz` requests returning 503** — with an aggregate failure rate of 2.64%. The API had
not crashed; it was opening a fresh Redis connection on every request and a fresh Postgres
connection on every `/api/status`, so 100 concurrent users meant 100 concurrent connection
handshakes. `/healthz` was worst hit, because each probe opened connections to all three
dependencies and its 3s probe timeout then expired under the contention it had created.

Fixed by moving to a process-wide Redis client, an asyncpg pool and a single Kafka admin client,
created once in a FastAPI lifespan handler. Same test, same machine, immediately after:

| | before pooling | after pooling |
|---|---|---|
| aggregate failures | 2.64% | **0.09%** |
| `/healthz` 503s | **82 (35.65%)** | **0** |
| throughput | 27.6 req/s | **36.9 req/s** |
| `/api/windows` p50 / p95 | 2,300 / 4,900 ms | **1,000 / 3,400 ms** |
| `/api/status` p95 | 9,300 ms | 5,800 ms |
| aggregate p95 | 5,400 ms | 3,700 ms |

This is the whole reason the load test is worth running: nothing in the unit or integration suite
exercises concurrency, so a connection-per-request API passes every correctness test and only
reveals itself under load.

## Known limitation

Four of roughly twenty WebSocket connections time out during the 100-user ramp (5s client timeout,
20 users/second arriving). It is a ramp-rate artefact on a saturated host rather than a steady-state
failure — no connection dropped once established, and the 20-user run had zero connect failures.
Not chased further, and recorded here rather than smoothed away by raising the client timeout.
