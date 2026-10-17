"""Contrail API: health, window aggregates over REST, live fan-out over WebSocket.

Reads only from Redis and TimescaleDB. It never joins the Kafka consumer group --
an HTTP process that did would take a partition assignment, so scaling the API
would steal work from the pipeline and trigger a rebalance on every deploy.
"""

import asyncio
import contextlib
import json
import logging
import secrets
import time

import asyncpg
import redis.asyncio as aioredis
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import Depends, FastAPI, Query, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.api.auth import RateLimiter, client_id, decode_token, issue_token, require_auth
from src.common import metrics
from src.common.config import get_settings
from src.common.logging import configure
from src.windowing.service import CHANNEL, CURRENT_KEY, LATE_KEY

settings = get_settings()
configure(settings.log_level)
log = logging.getLogger("contrail.api")

@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    """One Redis client, one Postgres pool, one Kafka admin client for the process.

    The first load test opened a fresh connection to every dependency on every
    request: p95 was 4.9s on /api/windows and a third of /healthz probes returned
    503, because 100 concurrent users meant 100 concurrent TCP handshakes and TLS-
    free-but-still-expensive Postgres startups. None of that was the API doing
    work -- it was the API establishing connections it should have already had.
    """
    conns["redis"] = aioredis.from_url(
        settings.redis_url, decode_responses=True, max_connections=64
    )
    conns["pg"] = await _with_retry("postgres", _make_pool)
    conns["kafka"] = await _with_retry("kafka", _start_admin)
    try:
        yield
    finally:
        for key, close in (("kafka", "close"), ("pg", "close"), ("redis", "aclose")):
            if conns.get(key) is not None:
                with contextlib.suppress(Exception):
                    await getattr(conns[key], close)()


async def _start_admin() -> AIOKafkaAdminClient:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap)
    try:
        await admin.start()
    except BaseException:  # incl. cancellation by the probe timeout
        # A client whose start() failed or was cancelled still owns sockets and a
        # background task; nobody else holds a reference, so close it here or it leaks
        # on every failed probe while the broker is down.
        with contextlib.suppress(Exception):
            await admin.close()
        raise
    return admin


async def _make_pool() -> asyncpg.Pool:
    pool = asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=16)
    try:
        await pool  # create_pool() returns the un-initialised pool; awaiting connects it
    except BaseException:
        pool.terminate()
        raise
    return pool


async def _with_retry(what: str, make, attempts: int = 30, delay_s: float = 2.0):
    """Build a connection, retrying, and return None rather than refusing to start.

    On a cold `docker compose up` TimescaleDB answers `pg_isready` while still
    finishing first-time initialisation, so the first connection is refused and an
    unguarded lifespan handler kills the process — the API was dead on a clean
    checkout for exactly this reason. Retrying covers the slow start; returning
    None covers the rest. An API that stays up and reports a broken dependency
    through /healthz is strictly more useful than one that will not boot, and a
    health endpoint that cannot run is the least useful thing in an outage.
    """
    for attempt in range(1, attempts + 1):
        try:
            return await make()
        except Exception as exc:  # noqa: BLE001
            if attempt == attempts:
                log.error("giving up connecting to %s; starting degraded", what,
                          extra={"error": f"{type(exc).__name__}: {exc}"})
                return None
            log.warning("waiting for %s (attempt %d/%d)", what, attempt, attempts)
            await asyncio.sleep(delay_s)
    return None


conns: dict = {}
app = FastAPI(title="Contrail", version="0.2.0", lifespan=lifespan)
limiter = RateLimiter(
    settings.rate_limit_rps, settings.rate_limit_burst,
    settings.rate_limit_idle_ttl_s, settings.rate_limit_max_buckets,
)
login_limiter = RateLimiter(
    settings.login_rate_limit_rps, settings.login_rate_limit_burst,
    settings.rate_limit_idle_ttl_s, settings.rate_limit_max_buckets,
)

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
    """The shared client. Callers must NOT close it -- it outlives the request."""
    return conns["redis"]


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}


@app.middleware("http")
async def observe(request: Request, call_next):
    # Label on the route template, never the raw path: labelling on `request.url.path`
    # would mint a new time series per distinct query target and blow up cardinality.
    #
    # The same goes for requests that match no route (404s): their path is whatever the
    # caller typed, so they share one label value, as does a nonstandard HTTP method.
    route = request.scope.get("route")
    path = getattr(route, "path", None) or "unmatched"
    method = request.method if request.method in _HTTP_METHODS else "OTHER"
    started = time.perf_counter()
    response = await call_next(request)
    metrics.API_LATENCY.labels(method, path).observe(time.perf_counter() - started)
    metrics.API_REQUESTS.labels(method, path, str(response.status_code)).inc()
    return response


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    # /healthz is exempt: an orchestrator probing liveness must never be told to
    # back off, or a burst of real traffic would take the instance out of rotation.
    if request.url.path not in ("/healthz", "/metrics") and not limiter.allow(
        client_id(request)
    ):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "rate limit exceeded"},
            headers={"Retry-After": "1"},
        )
    return await call_next(request)


# --------------------------------------------------------------------------- health

# One lock per dependency, so concurrent callers share a single reconnect.
_locks: dict[str, asyncio.Lock] = {}


def _lock(key: str) -> asyncio.Lock:
    return _locks.setdefault(key, asyncio.Lock())


async def _connect(key: str, make):
    """The live client for `key`, opened if there is none. The only place that opens one."""
    async with _lock(key):
        if conns.get(key) is None:
            conns[key] = await make()
        return conns[key]


async def _discard(key: str, client, close: str = "close") -> None:
    """Close a client that just failed, so the next probe reconnects instead of reusing it."""
    async with _lock(key):
        if conns.get(key) is client:  # someone else may already have replaced it
            conns[key] = None
            with contextlib.suppress(Exception):
                await getattr(client, close)()


async def _probe(key: str, make, use):
    client = await _connect(key, make)
    try:
        return await use(client)
    except (Exception, asyncio.CancelledError):  # a timed-out probe counts as a failure
        await _discard(key, client)
        raise


async def _check_redpanda() -> None:
    await _probe("kafka", _start_admin, lambda admin: admin.list_topics())


async def _check_timescale() -> None:
    async def use(pool):
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
            )

    version = await _probe("pg", _make_pool, use)
    if version is None:  # reachable but wrong: reconnecting would not help, so not discarded
        raise RuntimeError("timescaledb extension not installed")


async def _check_redis() -> None:
    await conns["redis"].ping()


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
async def login(body: LoginRequest, request: Request) -> JSONResponse:
    if not login_limiter.allow(client_id(request)):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "too many login attempts"},
            headers={"Retry-After": "2"},
        )
    # Both compared, in constant time, so the response does not reveal which was wrong.
    user_ok = secrets.compare_digest(body.username.encode(), settings.api_user.encode())
    pass_ok = secrets.compare_digest(body.password.encode(), settings.api_password.encode())
    if not (user_ok and pass_ok):
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
    raw = await conns["redis"].hgetall(CURRENT_KEY)
    items = [json.loads(v) for k, v in sorted(raw.items()) if cell is None or k == cell]
    return JSONResponse({"count": len(items), "windows": items})


@app.get("/api/status")
async def api_status(_: str = Depends(require_auth)) -> JSONResponse:
    """Pipeline state: rows stored, cells with a finalized window, late total."""
    if conns.get("pg") is None:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})
    try:
        async with conns["pg"].acquire() as conn:
            stored = await conn.fetchval("SELECT count(*) FROM flight_events")
            latest = await conn.fetchval("SELECT max(event_time) FROM flight_events")
    except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError):
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})
    cells = await conns["redis"].hlen(CURRENT_KEY)
    late = int(await conns["redis"].get(LATE_KEY) or 0)
    return JSONResponse({
        "events_stored": stored,
        "latest_event_time": latest.isoformat() if latest else None,
        "cells_with_windows": cells,
        # Running total of events sent to the late side output, kept in Redis across
        # restarts and updated as the windowing service runs. Approximate if the
        # service recovered from a crash (replayed records can be counted again).
        "late_events_total": late,
        # Deprecated alias of late_events_total. The old name claimed to be the last
        # run's count and was only written at shutdown; kept so existing readers work.
        "late_events_last_run": late,
        "window_s": settings.window_s,
        "allowed_lateness_s": settings.allowed_lateness_s,
    })


# --------------------------------------------------------------------------- websocket

WS_SEND_TIMEOUT_S = 5.0


async def _ws_send(websocket: WebSocket, payload: dict) -> None:
    """Send, but never let one slow client hold its handler (and a Redis connection) forever."""
    await asyncio.wait_for(websocket.send_json(payload), timeout=WS_SEND_TIMEOUT_S)


@app.websocket("/ws/windows")
async def ws_windows(websocket: WebSocket, token: str | None = Query(None)) -> None:
    """Fan out finalized windows as the pipeline produces them.

    DELIVERY IS BEST-EFFORT. Redis pub/sub keeps nothing for a client that is not
    connected, so anything published while a client is away (or too slow, or past
    its token's expiry) is gone. Messages:

      {"type": "snapshot", "windows": [...]}   once, right after subscribing: the
                                               latest window per cell, same as
                                               GET /api/windows
      {"type": "window", ...}                  each finalized window thereafter
      {"type": "heartbeat"}                    when idle

    A client should treat the snapshot (or GET /api/windows after any reconnect) as
    the truth and key live messages on (window_start, partition_key), since a window
    can arrive in both the snapshot and the live stream. The socket is closed with
    1008 when the token expires, and with 1011 if Redis fails or the client cannot
    keep up; reconnect with a fresh token and resynchronise.

    The token arrives as a query parameter because browsers cannot set headers on
    a WebSocket handshake. It is verified before `accept()`, so an unauthenticated
    client is refused at the handshake rather than being allowed to hold a socket.
    """
    try:
        expires_at = decode_token(token or "")["exp"]
    except Exception:  # noqa: BLE001 - missing, malformed, forged and expired all refuse alike
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    # pubsub() takes its own connection from the shared pool; the client itself
    # is process-wide and must not be closed when this socket ends.
    pubsub = conns["redis"].pubsub()
    counted = False
    try:
        # Subscribe first, then read the snapshot: the other order leaves a gap in
        # which a window is published and neither seen live nor in the snapshot.
        await pubsub.subscribe(CHANNEL)
        metrics.WS_CLIENTS.inc()
        counted = True
        raw = await conns["redis"].hgetall(CURRENT_KEY)
        await _ws_send(websocket, {"type": "snapshot",
                                   "windows": [json.loads(v) for _, v in sorted(raw.items())]})
        log.info("ws client subscribed", extra={"channel": CHANNEL})
        while True:
            if time.time() >= expires_at:
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            if message is None:
                # Keeps a dead peer from looking alive forever, and stops proxies
                # idling the connection out during quiet windows.
                await _ws_send(websocket, {"type": "heartbeat"})
                continue
            try:
                body = json.loads(message["data"])
            except (TypeError, ValueError):
                log.warning("ws dropped a malformed pub/sub message")
                continue
            await _ws_send(websocket, {"type": "window", **body})
    except (WebSocketDisconnect, RuntimeError, ConnectionError):
        log.info("ws client disconnected")
    except TimeoutError:
        log.warning("ws client too slow, closing")
        with contextlib.suppress(Exception):
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
    except aioredis.RedisError as exc:
        log.warning("ws closing, redis unavailable", extra={"error": type(exc).__name__})
        with contextlib.suppress(Exception):
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
    finally:
        if counted:  # only if the matching inc() ran
            metrics.WS_CLIENTS.dec()
        with contextlib.suppress(Exception):
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()
