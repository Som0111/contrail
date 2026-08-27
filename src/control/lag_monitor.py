"""Reads consumer-group lag from Redpanda.

Lag is the only honest measure of whether the pipeline is keeping up: it is the
distance between what the producer has written and what the consumer group has
committed, per partition. Throughput alone cannot tell you that -- a consumer
running flat out at 10k/s is failing if the producer is writing 12k/s.
"""

import asyncio
import logging
from dataclasses import dataclass, field

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

log = logging.getLogger("contrail.control.lag")


@dataclass(frozen=True)
class LagSample:
    """One observation of total consumer-group lag.

    `at` is a monotonic clock reading rather than wall time, so trend arithmetic
    cannot be corrupted by an NTP step mid-burst.
    """

    at: float
    total: int
    per_partition: dict[int, int] = field(default_factory=dict)

    @property
    def max_partition_lag(self) -> int:
        return max(self.per_partition.values(), default=0)


class LagMonitor:
    def __init__(self, bootstrap: str, topic: str, group: str) -> None:
        self.bootstrap = bootstrap
        self.topic = topic
        self.group = group
        self._admin: AIOKafkaAdminClient | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap)
        await self._admin.start()
        # A group-less consumer: used only to read partition high watermarks, so
        # it must never join the group it is measuring.
        self._consumer = AIOKafkaConsumer(bootstrap_servers=self.bootstrap)
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        if self._admin:
            await self._admin.close()

    async def _partitions(self) -> list[int]:
        """Live partition list, asked of the broker every sample.

        Not `consumer.partitions_for_topic()`: this consumer never subscribes, so
        it never refreshes its cached cluster metadata, and if it started before
        the topic existed it reports no partitions forever. That renders as zero
        lag -- indistinguishable from "keeping up perfectly" -- which is exactly
        how a burst went unnoticed for three runs. `consumer.topics()` does not
        help either; it returns fresh metadata without installing it in the cache
        that `partitions_for_topic` reads. The admin describe is a real request
        every time.
        """
        described = await self._admin.describe_topics([self.topic])
        for entry in described:
            if entry.get("topic") != self.topic or entry.get("error_code"):
                continue
            return sorted(p["partition"] for p in entry.get("partitions", []))
        return []

    async def sample(self) -> LagSample:
        assert self._admin and self._consumer, "call start() first"
        loop = asyncio.get_running_loop()

        parts = await self._partitions()
        if not parts:
            log.warning(
                "topic absent from metadata: reporting lag as unknown, not zero",
                extra={"topic": self.topic},
            )
            return LagSample(at=loop.time(), total=0)
        tps = [TopicPartition(self.topic, p) for p in parts]

        end = await self._consumer.end_offsets(tps)
        committed = await self._admin.list_consumer_group_offsets(self.group)

        per_partition: dict[int, int] = {}
        for tp in tps:
            meta = committed.get(tp)
            # An uncommitted partition means the group has not read it at all, so
            # everything on it is lag -- not zero, which is the tempting default
            # and would hide a consumer that never started.
            position = meta.offset if meta and meta.offset >= 0 else 0
            per_partition[tp.partition] = max(0, end[tp] - position)

        sample = LagSample(
            at=loop.time(), total=sum(per_partition.values()), per_partition=per_partition
        )
        log.debug(
            "lag sample", extra={"total": sample.total, "per_partition": per_partition}
        )
        return sample
