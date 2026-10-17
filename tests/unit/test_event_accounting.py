"""Ingest-side counters add up, lag is reported per partition, labels stay bounded."""

import httpx
from fastapi.testclient import TestClient
from prometheus_client import generate_latest

from src.api import main
from src.api.auth import RateLimiter
from src.common import metrics
from src.ingestor import sink
from src.ingestor.opensky import OpenSkySource
from tests.unit.test_opensky import REAL, row


def ingest(outcome):
    return metrics.INGEST_EVENTS.labels(outcome)._value.get()


async def test_fetched_equals_emitted_plus_parse_failed_plus_skipped():
    rows = [
        REAL,                              # good
        row(**{"0": "abc123"}),            # good
        row(**{"6": None}),                # no position: expected, skipped
        row(**{"7": -9000.0}),             # malformed: contract violation
        REAL[:4],                          # malformed: truncated
    ]
    before = {k: ingest(k) for k in ("fetched", "parse_failed", "skipped")}

    def handler(request):
        return httpx.Response(200, json={"states": rows})

    src = OpenSkySource(poll_interval_s=0.01, duration_s=0.05,
                        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    async for _ in src.stream():  # runs whole polls to the deadline, so counts are exact
        pass

    d = {k: ingest(k) - before[k] for k in before}
    polls = src.stats.polls
    assert polls >= 1 and d["fetched"] == polls * len(rows)
    assert d["parse_failed"] == 2 * polls and d["skipped"] == polls
    assert d["fetched"] == src.stats.emitted + d["parse_failed"] + d["skipped"]


async def test_sink_lag_is_high_watermark_minus_position_per_partition():
    class TP:
        def __init__(self, p):
            self.partition = p

    tps = [TP(0), TP(1), TP(2)]

    class Consumer:
        def assignment(self):
            return set(tps)

        def highwater(self, tp):
            return {0: 100, 1: 50, 2: None}[tp.partition]

        async def position(self, tp):
            return {0: 70, 1: 55}.get(tp.partition, 0)

    await sink.report_lag(Consumer())
    def lag(p):
        return metrics.SINK_PARTITION_LAG.labels(str(p))._value.get()

    assert lag(0) == 30
    assert lag(1) == 0, "position past a stale high watermark must not go negative"


def test_unmatched_paths_and_odd_methods_do_not_mint_metric_series(monkeypatch):
    monkeypatch.setattr(main, "limiter", RateLimiter(1000, 1000))
    c = TestClient(main.app)
    for i in range(20):
        c.get(f"/no-such-route-{i}")
    c.request("BREW", "/no-such-route-x")
    text = generate_latest().decode()
    assert "no-such-route" not in text
    assert 'path="unmatched"' in text and 'method="OTHER"' in text
