"""The producer interface every data source implements.

`synthetic.SyntheticSource` and (Phase 2.4) `opensky.OpenSkySource` both satisfy
this, so `publish()` and the whole downstream pipeline never learn which one is
running.
"""

import asyncio
import logging
from typing import AsyncIterator, Protocol, runtime_checkable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from src.common.config import get_settings
from src.common.models import FlightState

log = logging.getLogger("contrail.ingestor")


@runtime_checkable
class EventSource(Protocol):
    name: str

    def stream(self) -> AsyncIterator[FlightState]:
        """Yield flight states in *arrival* order, paced to wall clock."""
        ...


async def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    """Create the topic explicitly so partition count is ours, not the broker default of 1."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
        )
        log.info("created topic %s with %d partitions", topic, partitions)
    except TopicAlreadyExistsError:
        log.info("topic %s already exists", topic)
    finally:
        await admin.close()


async def publish(
    source: EventSource, bootstrap: str, topic: str, partitions: int
) -> int:
    """Drain a source into Kafka, keyed by geographic cell. Returns count published."""
    await ensure_topic(bootstrap, topic, partitions)
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    published = 0
    try:
        async for event in source.stream():
            await producer.send(
                topic,
                value=event.model_dump_json().encode(),
                key=event.partition_key.encode(),
            )
            published += 1
            if published % 1000 == 0:
                log.info("published %d events", published)
    finally:
        await producer.stop()
        log.info("published %d events from source=%s", published, source.name)
    return published


async def collect(
    bootstrap: str,
    topic: str,
    group: str,
    duration_s: float,
    idle_timeout_s: float = 10.0,
    poll_timeout_ms: int = 1000,
) -> list[FlightState]:
    """Read a topic into memory in recorded arrival order, for offline windowing runs.

    Deliberately not the sink's consume loop: this one commits nothing and keeps
    everything in memory, because the windowing processors are compared over a
    fixed, replayable batch rather than run as a live service.

    The sort at the end is load-bearing, not tidiness. `getmany()` hands back
    per-partition batches, so concatenating them interleaves six partitions in
    arbitrary chunks -- a chunk from one partition carries event_times from late
    in the run, which would drag a single global watermark forward and prematurely
    finalize windows the other partitions have not delivered yet. `ingest_time` is
    the arrival instant the recording captured, so sorting on it restores the
    stream the way it actually arrived. See DESIGN_DECISIONS.md 1.2 -- a *live*
    multi-partition consumer needs per-partition watermarks instead, since it
    cannot sort a stream it has not finished reading.
    """
    await ensure_topic(bootstrap, topic, get_settings().kafka_partitions)
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    events: list[FlightState] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration_s
    idle_for = 0.0
    try:
        while loop.time() < deadline:
            batches = await consumer.getmany(timeout_ms=poll_timeout_ms)
            records = [r for rs in batches.values() for r in rs]
            if not records:
                idle_for += poll_timeout_ms / 1000.0
                if idle_for >= idle_timeout_s:
                    log.info("idle, stopping collect", extra={"collected": len(events)})
                    break
                continue
            idle_for = 0.0
            events.extend(FlightState.model_validate_json(r.value) for r in records)
    finally:
        await consumer.stop()
    events.sort(key=lambda e: e.ingest_time)
    log.info("collected %d events from %s", len(events), topic)
    return events
