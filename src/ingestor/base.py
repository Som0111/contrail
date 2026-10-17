"""The producer interface every data source implements.

`synthetic.SyntheticSource` and (Phase 2.4) `opensky.OpenSkySource` both satisfy
this, so `publish()` and the whole downstream pipeline never learn which one is
running.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import AsyncIterator, Protocol, runtime_checkable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from src.common import metrics
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


async def publish_events(
    events: list[FlightState], bootstrap: str, topic: str, partitions: int
) -> int:
    """Write an already-materialised event list as fast as the broker accepts it.

    Not `publish()`: that drains a live source and is paced to wall clock, so
    laying down 300s of event time would take 300 real seconds. A replay
    recording carries its own timestamps, so the rate it is written at is
    irrelevant to everything downstream -- only the contents matter.
    """
    await ensure_topic(bootstrap, topic, partitions)
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    try:
        for event in events:
            await producer.send(
                topic,
                value=event.model_dump_json().encode(),
                key=event.partition_key.encode(),
            )
    finally:
        await producer.stop()
    log.info("published %d recorded events to %s", len(events), topic)
    return len(events)


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
            metrics.INGEST_EVENTS.labels("published").inc()
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
    stream the way it actually arrived.

    OFFLINE REPLAY ONLY. This is correct here solely because the topic is bounded
    and already recorded, so every record can be seen before the order is decided.
    Do not reuse this for live ingestion: a live consumer cannot sort a stream it
    has not finished reading, and has nowhere to buffer an unbounded one. The live
    path needs per-partition watermarks with the global watermark taken as the
    minimum across partitions. See DESIGN_DECISIONS.md 1.2.
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
    # Total order, not just by arrival instant. Many events share an ingest_time
    # (one generator tick emits a whole fleet), and Python's sort is stable, so
    # sorting on the timestamp alone leaves those ties in Kafka delivery order --
    # which differs run to run. Determinism (Phase 1.6) needs the tie broken by
    # something intrinsic to the event, so the replayed stream is byte-identical
    # every time regardless of how the broker interleaved the partitions.
    _arrival_order(events)
    log.info("collected %d events from %s", len(events), topic)
    return events


def _arrival_order(events: list[FlightState]) -> None:
    events.sort(key=lambda e: (e.ingest_time, e.icao24, e.event_time))


class IncompleteStream(RuntimeError):
    """The recording could not be read to its end, so any digest of it would cover a suffix."""


async def collect_sealed(
    bootstrap: str,
    topic: str,
    duration_s: float,
    poll_timeout_ms: int = 1000,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
) -> tuple[list[FlightState], dict[int, int]]:
    """Read exactly the records the topic held when this call started.

    Unlike `collect()`, completion is decided by offsets, not by silence: the end
    offset of every partition is snapshotted up front and reading stops only when
    each partition's position has reached it. A slow broker therefore delays the
    result instead of truncating it, and if `duration_s` runs out first this
    raises `IncompleteStream` rather than returning a prefix. Records appended
    after the snapshot are ignored, so a topic that is still being recorded to is
    sealed at the moment of the call; the snapshot is returned so callers can
    report exactly what was sealed.

    No consumer group: nothing can resume from, or commit to, anyone's offsets.
    """
    await ensure_topic(bootstrap, topic, get_settings().kafka_partitions)
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap, group_id=None, enable_auto_commit=False,
    )
    await consumer.start()
    events: list[FlightState] = []
    try:
        await consumer.topics()  # forces a metadata fetch; partitions_for_topic only reads the cache
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            raise IncompleteStream(f"no partition metadata for {topic}")
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        consumer.assign(tps)
        await consumer.seek_to_beginning(*tps)
        ends = await consumer.end_offsets(tps)
        begins = await consumer.beginning_offsets(tps)
        if all(ends[tp] <= begins[tp] for tp in tps):
            raise IncompleteStream(f"{topic} is empty")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + duration_s
        while True:
            positions = {tp: await consumer.position(tp) for tp in tps}
            if all(positions[tp] >= ends[tp] for tp in tps):
                break
            if loop.time() >= deadline:
                behind = {tp.partition: (positions[tp], ends[tp])
                          for tp in tps if positions[tp] < ends[tp]}
                raise IncompleteStream(
                    f"deadline hit before the end of {topic}; (position, end) by partition: {behind}"
                )
            batches = await consumer.getmany(timeout_ms=poll_timeout_ms)
            for tp, records in batches.items():
                events.extend(
                    FlightState.model_validate_json(r.value)
                    for r in records if r.offset < ends[tp]
                )
            if on_progress and events:
                await on_progress(len(events))
    finally:
        await consumer.stop()
    _arrival_order(events)
    log.info("sealed %d events from %s", len(events), topic)
    return events, {tp.partition: ends[tp] for tp in tps}
