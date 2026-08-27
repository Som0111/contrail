"""Contrail API: health, window aggregates over REST, live fan-out over WebSocket.

Reads only from Redis and TimescaleDB. It never joins the Kafka consumer group --
an HTTP process that did would take a partition assignment, so scaling the API
would steal work from the pipeline and trigger a rebalance on every deploy.
"""

import asyncio
import contextlib
import json
import logging

import asyncpg
import redis.asyncio as aioredis
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import Depends, FastAPI, Query, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.api.auth import RateLimiter, client_id, decode_token, issue_token, require_auth
from src.common.config import get_settings
from src.common.logging import configure
from src.windowing.service import CHANNEL, CURRENT_KEY

settings = get_settings()
configure(settings.log_level)
log = logging.getLogger("contrail.api")

app = FastAPI(title="Contrail", version="0.2.0")
limiter = RateLimiter(settings.rate_limit_rps, settings.rate_limit_burst)

PROBE_TIMEOUT = 3.0
# Each probe opens a real connection to each dependency, so an unthrottled probe
# storm can exhaust the very things it is checking -- 60 concurrent requests took
# healthz from 200 to 503. It is also exempt from rate limiting, so it needs its
# own protection. A short TTL keeps it honest (an orchestrator polls every few
# seconds) while collapsing a burst into one round of probes.
HEALTH_TTL_S = 1.0
_health_cache: tuple[float, dict, bool] | None = None
_health_lock = asyncio.Lock()


# --------------------------------------------------------------------------- infra

def redis_client() -> aioredis.Redis:
    return aioredis.from_url(settings.redis_url, decode_responses=True)


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    # /healthz is exempt: an orchestrator probing liveness must never be told to
    # back off, or a burst of real traffic would take the instance out of rotation.
    if request.url.path != "/healthz" and not limiter.allow(client_id(request)):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "rate limit exceeded"},
            headers={"Retry-After": "1"},
        )
    return await call_next(request)


# --------------------------------------------------------------------------- health

async def _check_redpanda() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap)
    await admin.start()
    try:
        await admin.list_topics()
    finally:
        await admin.close()


async def _check_timescale() -> None:
    conn = await asyncpg.connect(settings.postgres_dsn)
    try:
        version = await conn.fetchval(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        )
        if version is None:
            raise RuntimeError("timescaledb extension not installed")
    finally:
        await conn.close()


async def _check_redis() -> None:
    client = redis_client()
    try:
        await client.ping()
    finally:
        await client.aclose()


CHECKS = {
    "redpanda": _check_redpanda,
    "timescaledb": _check_timescale,
    "redis": _check_redis,
}


async def _run(name, probe) -> tuple[str, dict]:
    try:
        await asyncio.wait_for(probe(), timeout=PROBE_TIMEOUT)
        return name, {"status": "ok"}
    except Exception as exc:  # noqa: BLE001 - health probes report, never raise
        log.warning("healthz probe failed: %s: %s", name, exc)
        return name, {"status": "error", "detail": f"{type(exc).__name__}: {exc}"}


@app.get("/healthz")
async def healthz() -> JSONResponse:
    global _health_cache
    loop = asyncio.get_running_loop()
    async with _health_lock:
        if _health_cache is None or loop.time() - _health_cache[0] > HEALTH_TTL_S:
            results = dict(await asyncio.gather(*(_run(n, p) for n, p in CHECKS.items())))
            healthy = all(r["status"] == "ok" for r in results.values())
            _health_cache = (loop.time(), results, healthy)
    _, results, healthy = _health_cache
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "degraded", "dependencies": results},
    )


# --------------------------------------------------------------------------- auth

class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/auth/login")
async def login(body: LoginRequest) -> JSONResponse:
    if body.username != settings.api_user or body.password != settings.api_password:
        return JSONResponse(status_code=401, content={"detail": "invalid credentials"})
    token, ttl = issue_token(body.username)
    return JSONResponse({"access_token": token, "token_type": "bearer", "expires_in": ttl})


# --------------------------------------------------------------------------- data

@app.get("/api/windows")
async def windows(
    cell: str | None = Query(None, description="filter to one geographic cell"),
    _: str = Depends(require_auth),
) -> JSONResponse:
    """Latest finalized window aggregate per geographic cell."""
    client = redis_client()
    try:
        raw = await client.hgetall(CURRENT_KEY)
    finally:
        await client.aclose()
    items = [json.loads(v) for k, v in sorted(raw.items()) if cell is None or k == cell]
    return JSONResponse({"count": len(items), "windows": items})


@app.get("/api/status")
async def api_status(_: str = Depends(require_auth)) -> JSONResponse:
    """Pipeline state: rows stored, cells with a finalized window, late total."""
    client = redis_client()
    conn = await asyncpg.connect(settings.postgres_dsn)
    try:
        stored = await conn.fetchval("SELECT count(*) FROM flight_events")
        latest = await conn.fetchval("SELECT max(event_time) FROM flight_events")
        cells = await client.hlen(CURRENT_KEY)
        late = await client.get("contrail:windows:late_total")
    finally:
        await conn.close()
        await client.aclose()
    return JSONResponse({
        "events_stored": stored,
        "latest_event_time": latest.isoformat() if latest else None,
        "cells_with_windows": cells,
        "late_events_last_run": int(late) if late else 0,
        "window_s": settings.window_s,
        "allowed_lateness_s": settings.allowed_lateness_s,
    })


# --------------------------------------------------------------------------- websocket

@app.websocket("/ws/windows")
async def ws_windows(websocket: WebSocket, token: str = Query(...)) -> None:
    """Fan out finalized windows as the pipeline produces them.

    The token arrives as a query parameter because browsers cannot set headers on
    a WebSocket handshake. It is verified before `accept()`, so an unauthenticated
    client is refused at the handshake rather than being allowed to hold a socket.
    """
    try:
        decode_token(token)
    except Exception:  # noqa: BLE001 - any decode failure is a refusal
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    client = redis_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(CHANNEL)
    log.info("ws client subscribed", extra={"channel": CHANNEL})
    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            if message is None:
                # Keeps a dead peer from looking alive forever, and stops proxies
                # idling the connection out during quiet windows.
                await websocket.send_json({"type": "heartbeat"})
                continue
            await websocket.send_json({"type": "window", **json.loads(message["data"])})
    except (WebSocketDisconnect, RuntimeError, ConnectionError):
        log.info("ws client disconnected")
    finally:
        with contextlib.suppress(Exception):
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()
        await client.aclose()
