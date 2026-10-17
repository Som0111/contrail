"""WebSocket handler against a fake Redis: snapshot, malformed input, failures, expiry, slow client."""

import asyncio
import json
import time

import jwt
import pytest
import redis.asyncio as aioredis
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from src.api import main
from src.api.auth import ALGORITHM, RateLimiter, issue_token, signing_secret
from src.common import metrics

WINDOW = {"window_start": "2026-01-01T00:00:00+00:00", "partition_key": "1:1", "count": 3,
          "avg_altitude_m": 1.0, "avg_velocity_ms": 2.0}


class FakePubSub:
    def __init__(self, messages, subscribe_error=None):
        self.messages, self.subscribe_error = list(messages), subscribe_error
        self.closed = False

    async def subscribe(self, channel):
        if self.subscribe_error:
            raise self.subscribe_error

    async def get_message(self, ignore_subscribe_messages=True, timeout=1.0):
        await asyncio.sleep(0.05)
        if self.messages:
            m = self.messages.pop(0)
            if isinstance(m, Exception):
                raise m
            return {"data": m}
        return None

    async def unsubscribe(self, channel):
        pass

    async def aclose(self):
        self.closed = True


class FakeRedis:
    def __init__(self, pubsub, snapshot=None):
        self._pubsub, self._snapshot = pubsub, snapshot or {}

    def pubsub(self):
        return self._pubsub

    async def hgetall(self, key):
        return self._snapshot


def gauge():
    return metrics.WS_CLIENTS._value.get()


@pytest.fixture
def connect(monkeypatch):
    monkeypatch.setattr(main, "limiter", RateLimiter(1000, 1000))
    token, _ = issue_token("operator")

    def go(pubsub, snapshot=None, tok=None):
        main.conns["redis"] = FakeRedis(pubsub, snapshot)
        return TestClient(main.app).websocket_connect(f"/ws/windows?token={tok or token}")

    return go


def test_snapshot_comes_first_then_live_windows_and_malformed_messages_are_skipped(connect):
    live = json.dumps({**WINDOW, "count": 9})
    before = gauge()
    ps = FakePubSub(["not json", live])
    with connect(ps, {"1:1": json.dumps(WINDOW)}) as ws:
        snap = ws.receive_json()
        assert snap["type"] == "snapshot" and snap["windows"] == [WINDOW]
        msg = ws.receive_json()
        assert msg["type"] == "window" and msg["count"] == 9  # malformed one skipped, not fatal
    assert gauge() == before and ps.closed


def test_redis_down_at_subscribe_closes_cleanly_and_does_not_skew_the_gauge(connect):
    before = gauge()
    ps = FakePubSub([], subscribe_error=aioredis.ConnectionError("down"))
    with pytest.raises(WebSocketDisconnect) as exc, connect(ps) as ws:
        ws.receive_json()
    assert exc.value.code == 1011
    assert gauge() == before, "dec() must not run for an inc() that never happened"
    assert ps.closed


def test_redis_failure_mid_stream_closes_with_1011(connect):
    before = gauge()
    ps = FakePubSub([aioredis.ConnectionError("lost")])
    with pytest.raises(WebSocketDisconnect) as exc, connect(ps) as ws:
        ws.receive_json()  # snapshot
        ws.receive_json()
    assert exc.value.code == 1011 and gauge() == before


def test_connection_is_closed_when_the_token_expires(connect):
    now = int(time.time())
    tok = jwt.encode({"sub": "operator", "iat": now, "exp": now + 1}, signing_secret(),
                     algorithm=ALGORITHM)
    started = time.time()
    with pytest.raises(WebSocketDisconnect) as exc, connect(FakePubSub([]), tok=tok) as ws:
        for _ in range(100):
            ws.receive_json()  # snapshot, then heartbeats until expiry
    assert exc.value.code == 1008 and time.time() - started < 5


async def test_a_slow_client_times_out_instead_of_stalling_the_handler(monkeypatch):
    class Stuck:
        async def send_json(self, payload):
            await asyncio.sleep(60)

    monkeypatch.setattr(main, "WS_SEND_TIMEOUT_S", 0.1)
    with pytest.raises(TimeoutError):
        await main._ws_send(Stuck(), {"type": "heartbeat"})
