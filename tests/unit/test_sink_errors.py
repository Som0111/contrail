"""Sink write-path error handling: retry transient failures, isolate permanent ones."""

import asyncpg
import pytest

from src.ingestor import sink
from src.storage import timescale


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    monkeypatch.setattr(sink, "BACKOFF_BASE_S", 0.0)


def fake_insert(monkeypatch, fail):
    calls = []

    async def insert(pool, events):
        calls.append(list(events))
        fail(events, len(calls))
        return len(events)

    monkeypatch.setattr(timescale, "insert_events", insert)
    return calls


async def test_transient_error_is_retried_then_succeeds(monkeypatch):
    def fail(events, n):
        if n < 3:
            raise ConnectionResetError("db went away")

    calls = fake_insert(monkeypatch, fail)
    n, bad = await sink.write_isolating(None, [("r0", "e0"), ("r1", "e1")])
    assert (n, bad) == (2, []) and len(calls) == 3


async def test_exhausted_retries_raise_and_never_quarantine(monkeypatch):
    fake_insert(monkeypatch, lambda e, n: (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(sink.RetriesExhausted):
        await sink.write_isolating(None, [("r0", "e0")])


async def test_poison_record_is_isolated_and_rest_is_written(monkeypatch):
    def fail(events, n):
        if "poison" in events:
            raise asyncpg.CheckViolationError("bad row")

    calls = fake_insert(monkeypatch, fail)
    pairs = [(f"r{i}", "poison" if i == 5 else f"e{i}") for i in range(8)]
    n, bad = await sink.write_isolating(None, pairs)
    assert n == 7
    assert [r for r, _ in bad] == ["r5"]
    assert len(calls) < 2 * len(pairs)  # bisected, not one write per record
