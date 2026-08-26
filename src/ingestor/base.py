"""The producer interface every data source implements.

`synthetic.SyntheticSource` and (Phase 2.4) `opensky.OpenSkySource` both satisfy
this, so `publish()` and the whole downstream pipeline never learn which one is
running.
"""

import logging
from typing import AsyncIterator, Protocol, runtime_checkable

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

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
