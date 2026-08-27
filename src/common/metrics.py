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
"""

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger("contrail.metrics")

# --- ingestion / storage -----------------------------------------------------

EVENTS = Counter(
    "contrail_events_total",
    "Events handled by the sink, by outcome.",
    ["outcome"],  # inserted | suppressed | shed
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

WATERMARK_SKEW_EVENT = Gauge(
    "contrail_watermark_skew_event_seconds",
    "Max event_time seen minus the current watermark; should track allowed lateness.",
)

# --- control loop ------------------------------------------------------------

CONSUMER_LAG = Gauge("contrail_consumer_lag", "Consumer group lag, total across partitions.")
CONSUMER_LAG_PARTITION = Gauge(
    "contrail_consumer_lag_partition", "Consumer group lag per partition.", ["partition"]
)
LAG_SLOPE = Gauge(
    "contrail_lag_slope_per_second", "Fitted lag trend the controller is acting on."
)
WORKERS = Gauge("contrail_workers", "Consumer workers currently running.")
SHEDDING = Gauge("contrail_shedding", "1 while load shedding is engaged, else 0.")
SHED_EVENTS = Counter("contrail_shed_events_total", "Events dropped by load shedding.")
CONTROL_ACTIONS = Counter(
    "contrail_control_actions_total", "Controller actions taken.", ["action"]
)

# --- api ---------------------------------------------------------------------

API_REQUESTS = Counter(
    "contrail_api_requests_total", "API requests.", ["method", "path", "status"]
)
API_LATENCY = Histogram(
    "contrail_api_request_seconds",
    "API request duration.",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
WS_CLIENTS = Gauge("contrail_ws_clients", "Connected WebSocket clients.")


def serve(port: int) -> None:
    """Expose /metrics for a non-HTTP process (sink, windowing, supervisor)."""
    start_http_server(port)
    log.info("metrics endpoint listening", extra={"port": port})
