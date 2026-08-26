"""Contrail API. Phase 0.1: dependency health only."""

import asyncio
import logging

import asyncpg
import redis.asyncio as aioredis
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.common.config import get_settings

settings = get_settings()
logging.basicConfig(level=settings.log_level)
log = logging.getLogger("contrail.api")

app = FastAPI(title="Contrail", version="0.1.0")

PROBE_TIMEOUT = 3.0


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
        # Also proves the timescaledb extension is actually loaded, not just Postgres.
        version = await conn.fetchval(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        )
        if version is None:
            raise RuntimeError("timescaledb extension not installed")
    finally:
        await conn.close()


async def _check_redis() -> None:
    client = aioredis.from_url(settings.redis_url)
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
    results = dict(
        await asyncio.gather(*(_run(n, p) for n, p in CHECKS.items()))
    )
    healthy = all(r["status"] == "ok" for r in results.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "degraded", "dependencies": results},
    )
