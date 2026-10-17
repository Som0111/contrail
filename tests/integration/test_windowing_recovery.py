"""Windowing-service crash recovery, against real Redpanda and Redis.

The property under test: kill the service mid-stream (SIGKILL or SIGTERM), restart it on the
same consumer group, and every window that should exist comes out with exactly its true
count -- nothing lost to offsets committed past unpublished windows, nothing doubled by the
replay that follows. Mocked unit tests cannot show this; the commit boundary is a property
of the broker, the Redis script and the process dying together.

Every aggregate the service publishes is observed on the pub/sub channel, so the check
covers all windows, not just the latest per cell that the Redis hash keeps.
"""

import asyncio
import json
import os
import re
import sys
from datetime import UTC, datetime

import pytest
import redis.asyncio as aioredis
from aiokafka.admin import AIOKafkaAdminClient

from src.common.config import get_settings
from src.ingestor.base import ensure_topic, publish_events
from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.windowing.aggregates import ground_truth
from tests.helpers import unique, wait_for

SETTINGS = get_settings()
WINDOW_S, LATENESS_S = 60, 10.0


def stream():
    """Disordered within the lateness bound, so ground truth is the exact answer."""
    source = SyntheticSource(
        n_aircraft=20, rate_hz=1.0, seed=7, duration_s=420,
        start_time=datetime(2026, 6, 1, tzinfo=UTC),
        chaos=ChaosConfig(out_of_order_prob=0.2, max_skew_s=5.0),
    )
    return list(source.simulate())


METRICS_PORT = 9101



async def scrape() -> dict[str, float]:
    import httpx

    try:
        async with httpx.AsyncClient(timeout=3) as c:
            text = (await c.get(f"http://localhost:{METRICS_PORT}/metrics")).text
    except httpx.HTTPError:
        return {}
    return {m.group(1): float(m.group(2))
            for m in re.finditer(r"^(contrail_windowing_[a-z_]+) ([0-9.e+-]+)$", text, re.M)}


async def start_service(topic, group, prefix, metrics_port=0):
    env = {**os.environ, "PYTHONPATH": "/app", "LOG_LEVEL": os.environ.get("TEST_SERVICE_LOG", "ERROR"),
           "REDIS_KEY_PREFIX": prefix}  # a replay logs every late event
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "src.windowing.service",
        "--topic", topic, "--group", group, "--redis-url", SETTINGS.redis_url,
        "--window-s", str(WINDOW_S), "--allowed-lateness-s", str(LATENESS_S),
        "--metrics-port", str(metrics_port), cwd="/app", env=env,
    )


async def group_state(group):
    admin = AIOKafkaAdminClient(bootstrap_servers=SETTINGS.kafka_bootstrap)
    await admin.start()
    try:
        described = await admin.describe_consumer_groups([group])
        # raw DescribeGroups response: groups = [(error, group_id, state, type, protocol, members)]
        members = len(described[0].groups[0][-1]) if described and described[0].groups else 0
        committed = await admin.list_consumer_group_offsets(group)
        return {"members": members, "committed": {str(tp): m.offset for tp, m in committed.items()}}
    finally:
        await admin.close()


async def end_offsets(topic):
    from aiokafka import AIOKafkaConsumer, TopicPartition

    consumer = AIOKafkaConsumer(bootstrap_servers=SETTINGS.kafka_bootstrap, group_id=None)
    await consumer.start()
    try:
        await consumer.topics()
        tps = [TopicPartition(topic, p) for p in sorted(consumer.partitions_for_topic(topic) or [])]
        return {tp.partition: o for tp, o in (await consumer.end_offsets(tps)).items()}
    finally:
        await consumer.stop()


@pytest.fixture
def prefix():
    """A Redis namespace per test. The latest-per-cell hash only moves forward in event time,
    so sharing one -- with the demo stack's windowing service, or with the previous test, which
    replays the same event times -- makes the older windows of the later run get refused."""
    return unique("t")


@pytest.fixture
async def seen(prefix):
    """Collect every aggregate published during the test: (window_start, cell) -> counts."""
    client = aioredis.from_url(SETTINGS.redis_url, decode_responses=True)
    pubsub = client.pubsub()
    await pubsub.subscribe(f"{prefix}.windows")
    counts: dict[tuple[str, str], list[int]] = {}

    async def listen():
        async for message in pubsub.listen():
            if message["type"] == "message":
                m = json.loads(message["data"])
                counts.setdefault((m["window_start"], m["partition_key"]), []).append(m["count"])

    task = asyncio.create_task(listen())
    yield counts
    task.cancel()
    await pubsub.aclose()
    await client.aclose()


def assert_exact(counts, events):
    truth = ground_truth(events, WINDOW_S)
    expected = {(w.isoformat(), cell): a.count for (w, cell), a in truth.items()}
    missing = sorted(set(expected) - set(counts))
    assert not missing, f"{len(missing)} windows never published, e.g. {missing[:3]}"
    for key, want in expected.items():
        assert max(counts[key]) == want, f"{key}: published {counts[key]}, truth {want}"
    # A replay must never produce a count above the truth (double counting).
    assert all(max(v) <= expected[k] for k, v in counts.items() if k in expected)


@pytest.mark.parametrize("how", ["sigkill", "sigterm"])
async def test_windows_are_exact_after_the_service_is_killed_and_restarted(seen, prefix, how):
    events = stream()
    cut = int(len(events) * 0.6)
    topic, group = unique("win.recovery"), unique("win-recovery")
    await ensure_topic(SETTINGS.kafka_bootstrap, topic, SETTINGS.kafka_partitions)

    first = await start_service(topic, group, prefix)
    try:
        # The service reads from `latest` on a new group, so it must have joined first.
        async def joined():
            return (await group_state(group))["members"] > 0

        async def diagnostics():
            return {"windows_seen": len(seen), "group": await group_state(group)}

        await wait_for(joined, timeout=60, what="the service to join its group", state=diagnostics)
        await publish_events(events[:cut], SETTINGS.kafka_bootstrap, topic, SETTINGS.kafka_partitions)

        async def mid_stream():
            st = await group_state(group)
            return bool(seen) and bool(st["committed"]) and st["committed"]

        await wait_for(mid_stream, timeout=60, what="windows published and offsets committed",
                       state=diagnostics)
        assert first.returncode is None, "the service exited before it could be killed"
        first.kill() if how == "sigkill" else first.terminate()
        await asyncio.wait_for(first.wait(), timeout=60)
    finally:
        if first.returncode is None:
            first.kill()

    # B goes in before the restart, so it is a backlog sitting next to whatever the dead
    # service left uncommitted.
    await publish_events(events[cut:], SETTINGS.kafka_bootstrap, topic, SETTINGS.kafka_partitions)
    second = await start_service(topic, group, prefix, metrics_port=METRICS_PORT)
    truth = ground_truth(events, WINDOW_S)
    try:
        # Partitions go quiet as aircraft cross cell boundaries, so the stream alone does not
        # finalize every window (the documented idle-partition limit, off by default). What
        # must hold is that once the service has consumed everything, a graceful stop flushes
        # every window with its exact count. "Consumed everything" is read off its own metrics.
        async def drained():
            m = await scrape()
            return (m.get("contrail_windowing_records_total", 0) >= len(events) - cut
                    and m.get("contrail_windowing_consumer_lag") == 0)

        async def drain_state():
            return {"metrics": await scrape(), "seen": len(seen), "service_exit": second.returncode,
                    "group": await group_state(group), "topic_end": await end_offsets(topic)}

        await wait_for(drained, timeout=120, what="the restarted service to consume the whole topic",
                       state=drain_state)
        second.terminate()
        await asyncio.wait_for(second.wait(), timeout=60)
    finally:
        if second.returncode is None:
            second.kill()

    await wait_for(lambda: len(seen) >= len(set(truth)), timeout=30, what="the final flush to be seen",
                   state=lambda: {"expected": len(truth), "seen": len(seen)})
    assert_exact(seen, events)
