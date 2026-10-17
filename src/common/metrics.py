"""Prometheus instrumentation, defined in one place so names stay consistent.

Every pipeline process serves its own `/metrics` and Prometheus scrapes each as a
separate target. The alternative — funnelling everything through one exporter, or
a pushgateway — would blur which process a number came from, and "which worker is
behind" is exactly the question this pipeline needs to answer.

Two watermark skews are exported, and the difference between them is the point:

  * `watermark_skew_wallclock` = now - watermark. How far behind real time the
    engine's notion of completeness is. Grows when the pipeline falls behind OR
    when data stops arriving, so it answers "is my view of the world stale".
  * `watermark_skew_event` = max event_time seen - watermark. Should sit at
    almost exactly the configured allowed lateness. It answers "is the watermark
    doing what I configured", and it stays flat when the pipeline is merely idle.

Reading only the first would make an idle pipeline look broken; reading only the
second would make a stalled one look healthy.

Metric semantics: `*_total` are Counters (only ever increase), everything else that
moves both ways is a Gauge. Counters live in process memory and restart at zero when
the process does; Prometheus handles that for rate()/increase(), so dashboards should
use those rather than the raw value. The one number that must survive restarts, the
late-event total shown by /api/status, is kept in Redis instead.
"""

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger("contrail.metrics")

# Label cardinality rule: a label may only take values from a small, fixed set known
# at code time (an outcome, a partition number, a route template). Never an aircraft
# id, trace id, raw path or exception text -- each distinct value is a new time series.
# Every label below says what bounds it.

# --- ingestion / storage -----------------------------------------------------

# Accounting identity, per run once the pipeline has drained:
#   fetched == published + parse_failed + skipped
#   published == inserted + suppressed + shed + quarantined   (contrail_events_total)
INGEST_EVENTS = Counter(
    "contrail_ingest_events_total",
    "Records handled by the ingestor, by outcome.",
    # bounded: fetched | parse_failed (malformed) | skipped (expected, e.g. no position) | published
    ["outcome"],
)
SINK_PARTITION_LAG = Gauge(
    "contrail_sink_partition_lag",
    "Records the sink has not yet consumed, per partition: last fetched high watermark "
    "minus its position. Seen from the consumer, unlike the controller's admin-API lag.",
    ["partition"],  # bounded by KAFKA_PARTITIONS
)

EVENTS = Counter(
    "contrail_events_total",
    "Events handled by the sink, by outcome.",
    ["outcome"],  # bounded: inserted | suppressed | shed | quarantined
)

SINK_RETRIES = Counter(
    "contrail_sink_retries_total", "Transient DB write failures retried by the sink."
)
SINK_RETRIES_EXHAUSTED = Counter(
    "contrail_sink_retries_exhausted_total",
    "Batches abandoned after every retry failed; the sink stops uncommitted. Alert on this.",
)
SINK_COMMIT_FAILURES = Counter(
    "contrail_sink_commit_failures_total",
    "Offset commits that failed after a durable write; the batch replays idempotently.",
)

E2E_LATENCY = Histogram(
    "contrail_e2e_latency_seconds",
    "Event time to durable write.",
    buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600),
)

# --- windowing ---------------------------------------------------------------

WINDOWS_FINALIZED = Counter(
    "contrail_windows_finalized_total", "Window aggregates finalized and published."
)

LATE_EVENTS = Counter(
    "contrail_late_events_total",
    "Events that arrived after their window was finalized (side output).",
)

WATERMARK_SKEW_WALLCLOCK = Gauge(
    "contrail_watermark_skew_wallclock_seconds",
    "Wall clock now minus the current watermark.",
)

WINDOWING_RECORDS = Counter(
    "contrail_windowing_records_total", "Records consumed by the windowing service."
)
WINDOWING_LAG = Gauge(
    "contrail_windowing_consumer_lag",
    "Records the windowing service has not yet consumed, summed over its partitions "
    "(last fetched high watermark minus position).",
)

DEDUP_KEYS = Gauge("contrail_dedup_keys", "Dedup keys currently retained in memory.")
DEDUP_EVICTED = Counter("contrail_dedup_evicted_total", "Dedup keys evicted by the retention horizon.")
DEDUP_SUPPRESSED = Counter(
    "contrail_dedup_suppressed_total", "Events suppressed as duplicates by the windowing service."
)

IDLE_PARTITIONS = Gauge(
    "contrail_idle_partitions", "Partitions currently excluded from the watermark as idle."
)
IDLE_TRANSITIONS = Counter(
    "contrail_idle_transitions_total",
    "Partitions entering or leaving idle.",
    ["direction"],  # bounded: idle | resumed
)

WATERMARK_SKEW_EVENT = Gauge(
    "contrail_watermark_skew_event_seconds",
    "Max event_time seen minus the current watermark; should track allowed lateness.",
)

# --- control loop ------------------------------------------------------------

CONSUMER_LAG = Gauge("contrail_consumer_lag", "Consumer group lag, total across partitions.")
CONSUMER_LAG_PARTITION = Gauge(
    "contrail_consumer_lag_partition", "Consumer group lag per partition.",
    ["partition"],  # bounded by KAFKA_PARTITIONS
)
LAG_SLOPE = Gauge(
    "contrail_lag_slope_per_second", "Fitted lag trend the controller is acting on."
)
WORKERS = Gauge("contrail_workers", "Consumer workers currently running.")
SHEDDING = Gauge("contrail_shedding", "1 while load shedding is engaged, else 0.")
SHED_EVENTS = Counter("contrail_shed_events_total", "Events dropped by load shedding.")
CONTROL_ACTIONS = Counter(
    "contrail_control_actions_total", "Controller actions taken.",
    ["action"],  # bounded: the fixed set of controller decisions
)

# --- api ---------------------------------------------------------------------

API_REQUESTS = Counter(
    "contrail_api_requests_total", "API requests.",
    ["method", "path", "status"],  # bounded: HTTP verbs, route templates or "unmatched", status codes
)
API_LATENCY = Histogram(
    "contrail_api_request_seconds",
    "API request duration.",
    ["method", "path"],  # bounded like contrail_api_requests_total
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
WS_CLIENTS = Gauge("contrail_ws_clients", "Connected WebSocket clients.")


def serve(port: int) -> None:
    """Expose /metrics for a non-HTTP process (sink, windowing, supervisor)."""
    start_http_server(port)
    log.info("metrics endpoint listening", extra={"port": port})
