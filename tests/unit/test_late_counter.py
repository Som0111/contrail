"""Late-event accounting: a live running total, not a value written at shutdown."""

import re
from pathlib import Path

from fastapi.testclient import TestClient

from src.api import main
from src.api.auth import RateLimiter, issue_token
from src.common import metrics
from src.windowing.service import LATE_KEY, report_late


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def incrby(self, key, n):
        self.store[key] = self.store.get(key, 0) + n

    async def get(self, key):
        return self.store.get(key)

    async def hlen(self, key):
        return 2


async def test_late_total_tracks_the_running_count_while_the_service_runs():
    r, reported = FakeRedis(), 0
    before = metrics.LATE_EVENTS._value.get()
    for seen in (0, 3, 3, 10):  # the processor's cumulative count after each loop
        reported = await report_late(r, seen, reported)
    assert r.store[LATE_KEY] == 10 and reported == 10
    assert metrics.LATE_EVENTS._value.get() - before == 10


async def test_total_survives_a_restart_instead_of_being_overwritten():
    r = FakeRedis()
    await report_late(r, 7, 0)  # first process
    await report_late(r, 2, 0)  # restarted process: its own counter starts at zero
    assert r.store[LATE_KEY] == 9


async def test_no_redis_write_when_nothing_is_late():
    r = FakeRedis()
    await report_late(r, 0, 0)
    assert r.store == {}


def test_counters_are_named_total_and_gauges_are_not():
    """A Counter only ever increases; a name ending _total promises that, so it is reserved."""
    src = Path(metrics.__file__).read_text(encoding="utf-8")
    declared = re.findall(r'(Counter|Gauge|Histogram)\(\s*"([a-z0-9_]+)"', src)
    assert len(declared) >= 20
    for kind, name in declared:
        assert name.endswith("_total") == (kind == "Counter"), f"{kind} {name}"


def test_status_reports_the_live_total_and_keeps_the_old_field(monkeypatch):
    class Conn:
        async def fetchval(self, q):
            return 5 if "count" in q else None

    class Acquire:
        async def __aenter__(self):
            return Conn()

        async def __aexit__(self, *a):
            return False

    class Pool:
        def acquire(self):
            return Acquire()

    monkeypatch.setattr(main, "limiter", RateLimiter(1000, 1000))
    r = FakeRedis()
    r.store[LATE_KEY] = 42
    main.conns["redis"], main.conns["pg"] = r, Pool()
    token, _ = issue_token("operator")
    body = TestClient(main.app).get("/api/status", headers={"Authorization": f"Bearer {token}"}).json()
    assert body["late_events_total"] == 42
    assert body["late_events_last_run"] == 42  # deprecated alias
