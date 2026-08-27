"""API against the live stack: auth is enforced, REST serves real data, WS fans out.

Runs against the `api` service in compose, not an in-process test client, so the
WebSocket path exercises a real handshake and real Redis pub/sub rather than a
mocked one.
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime

import httpx
import pytest
import websockets
from websockets.exceptions import WebSocketException

from src.common.config import get_settings
from src.ingestor.base import ensure_topic, publish_events
from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.windowing import service

SETTINGS = get_settings()
BASE = "http://api:8000"
WS_BASE = "ws://api:8000"
CREDS = {"username": SETTINGS.api_user, "password": SETTINGS.api_password}


async def token() -> str:
    async with httpx.AsyncClient(base_url=BASE, timeout=20) as c:
        r = await c.post("/auth/login", json=CREDS)
        r.raise_for_status()
        return r.json()["access_token"]


@pytest.mark.parametrize("path", ["/api/windows", "/api/status"])
async def test_protected_routes_reject_unauthenticated_requests(path):
    async with httpx.AsyncClient(base_url=BASE, timeout=20) as c:
        assert (await c.get(path)).status_code == 401
        bad = await c.get(path, headers={"Authorization": "Bearer not.a.jwt"})
        assert bad.status_code == 401


async def test_login_rejects_wrong_credentials_and_accepts_right_ones():
    async with httpx.AsyncClient(base_url=BASE, timeout=20) as c:
        bad = await c.post("/auth/login", json={"username": "operator", "password": "wrong"})
        assert bad.status_code == 401
        good = await c.post("/auth/login", json=CREDS)
        assert good.status_code == 200
        assert good.json()["token_type"] == "bearer"


async def test_authenticated_status_returns_real_pipeline_state():
    async with httpx.AsyncClient(base_url=BASE, timeout=20) as c:
        r = await c.get("/api/status", headers={"Authorization": f"Bearer {await token()}"})
        assert r.status_code == 200
        body = r.json()
        # Real values read back from TimescaleDB and Redis, not a fixed payload.
        assert body["events_stored"] >= 0
        assert body["window_s"] == SETTINGS.window_s
        assert body["allowed_lateness_s"] == SETTINGS.allowed_lateness_s


async def test_healthz_is_exempt_from_rate_limiting():
    """An orchestrator's liveness probe must never be told to back off."""
    async with httpx.AsyncClient(base_url=BASE, timeout=30) as c:
        codes = {r.status_code for r in
                 await asyncio.gather(*[c.get("/healthz") for _ in range(60)])}
    assert codes == {200}


async def test_websocket_refuses_a_bad_token_at_the_handshake():
    with pytest.raises(WebSocketException):
        async with websockets.connect(f"{WS_BASE}/ws/windows?token=garbage") as ws:
            await asyncio.wait_for(ws.recv(), timeout=5)


async def test_websocket_and_rest_deliver_a_finalized_window():
    """End to end: publish -> windowing service -> Redis -> API -> client."""
    topic = f"api.test.{uuid.uuid4().hex[:8]}"
    group = f"api-test-{uuid.uuid4().hex[:8]}"
    await ensure_topic(SETTINGS.kafka_bootstrap, topic, SETTINGS.kafka_partitions)

    stop = asyncio.Event()
    svc = asyncio.create_task(service.run(
        SETTINGS.kafka_bootstrap, topic, group, SETTINGS.redis_url,
        window_s=60, allowed_lateness_s=5.0, poll_timeout_ms=250, stop=stop,
    ))
    await asyncio.sleep(3.0)  # the service reads from `latest`, so let it join first

    tok = await token()
    async with websockets.connect(f"{WS_BASE}/ws/windows?token={tok}") as ws:
        # 300s of event time at a fixed epoch: several windows, and enough
        # event-time span past each one for the watermark to finalize it.
        source = SyntheticSource(
            n_aircraft=15, rate_hz=1.0, seed=4242, duration_s=300,
            start_time=datetime(2026, 6, 1, tzinfo=UTC),
            chaos=ChaosConfig(out_of_order_prob=0.1, max_skew_s=3.0),
        )
        await publish_events(list(source.simulate()), SETTINGS.kafka_bootstrap,
                             topic, SETTINGS.kafka_partitions)

        window_msg = None
        deadline = asyncio.get_running_loop().time() + 40
        while asyncio.get_running_loop().time() < deadline:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
            if msg.get("type") == "window":
                window_msg = msg
                break
        assert window_msg is not None, "no window arrived over the websocket"

    for key in ("window_start", "partition_key", "count", "avg_altitude_m"):
        assert key in window_msg, f"{key} missing from {window_msg}"
    assert window_msg["count"] > 0

    stop.set()
    await svc

    # The same aggregate must be readable over REST.
    async with httpx.AsyncClient(base_url=BASE, timeout=20) as c:
        r = await c.get("/api/windows", headers={"Authorization": f"Bearer {tok}"})
        assert r.status_code == 200
        body = r.json()
        assert body["count"] > 0
        cells = {w["partition_key"] for w in body["windows"]}
        assert window_msg["partition_key"] in cells

        one = await c.get(f"/api/windows?cell={window_msg['partition_key']}",
                          headers={"Authorization": f"Bearer {tok}"})
        assert one.json()["count"] == 1
