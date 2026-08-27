"""Ties the controller to something that actually changes: consumer workers.

Scaling is real, not simulated. Each worker is an independent `sink.run()` loop
in the same consumer group, so adding one makes Redpanda rebalance partitions
across the larger group and genuinely doubles read parallelism. Removing one
sets its stop event and lets it finish its current batch, so no offsets are lost.

Shedding drops a deterministic fraction of *geographic cells* rather than a
random fraction of events. That distinction matters: sampling random events
would bias every cell's aggregate low and silently, whereas dropping whole cells
leaves every surviving cell exactly correct and the loss enumerable -- you can
say precisely which cells went dark and for how long.
"""

import argparse
import asyncio
import logging
from dataclasses import dataclass, field
from hashlib import blake2s

from src.common.config import get_settings
from src.common.logging import configure
from src.common.models import FlightState
from src.control.controller import ControllerConfig, Decision, LagController
from src.control.lag_monitor import LagMonitor, LagSample
from src.ingestor import sink

log = logging.getLogger("contrail.control.supervisor")


@dataclass
class ShedState:
    """Shared, mutable: workers consult it per event."""

    fraction: float = 0.0
    dropped: int = 0
    cells_shed: set[str] = field(default_factory=set)

    def keep(self, event: FlightState) -> bool:
        if self.fraction <= 0.0:
            return True
        cell = event.partition_key
        # Stable hash of the cell -> the same cells are shed for the whole
        # episode, instead of a different arbitrary slice every batch.
        bucket = int.from_bytes(blake2s(cell.encode(), digest_size=2).digest(), "big")
        if bucket / 65535.0 < self.fraction:
            self.dropped += 1
            self.cells_shed.add(cell)
            return False
        return True


@dataclass
class SupervisionResult:
    decisions: list[Decision] = field(default_factory=list)
    samples: list[LagSample] = field(default_factory=list)
    shed_dropped: int = 0

    @property
    def peak_lag(self) -> int:
        return max((s.total for s in self.samples), default=0)

    @property
    def final_lag(self) -> int:
        return self.samples[-1].total if self.samples else 0

    @property
    def actions(self) -> list[Decision]:
        return [d for d in self.decisions if d.changed]


class WorkerPool:
    def __init__(self, bootstrap: str, topic: str, group: str, dsn: str,
                 shed: ShedState, batch_size: int = 500,
                 max_rate: float | None = None) -> None:
        self.bootstrap, self.topic, self.group, self.dsn = bootstrap, topic, group, dsn
        self.shed = shed
        self.batch_size = batch_size
        self.max_rate = max_rate
        self._workers: list[tuple[asyncio.Task, asyncio.Event]] = []

    @property
    def size(self) -> int:
        return len(self._workers)

    def _spawn(self) -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(
            sink.run(
                bootstrap=self.bootstrap, topic=self.topic, group=self.group,
                dsn=self.dsn, batch_size=self.batch_size, poll_timeout_ms=500,
                stop=stop, event_filter=self.shed.keep, max_rate=self.max_rate,
            )
        )
        self._workers.append((task, stop))
        log.info("worker started", extra={"workers": self.size})

    async def scale_to(self, n: int) -> None:
        while self.size < n:
            self._spawn()
        while self.size > n:
            task, stop = self._workers.pop()
            stop.set()
            try:
                await asyncio.wait_for(task, timeout=15.0)
            except asyncio.TimeoutError:
                task.cancel()
            log.info("worker stopped", extra={"workers": self.size})

    async def close(self) -> None:
        await self.scale_to(0)


async def supervise(
    bootstrap: str, topic: str, group: str, dsn: str,
    config: ControllerConfig, interval_s: float = 2.0,
    duration_s: float | None = None, shed_fraction: float = 0.25,
    max_rate: float | None = None, static_workers: int | None = None,
) -> SupervisionResult:
    """Run the monitor -> controller -> pool loop.

    With `static_workers` set the controller still observes and still logs, but
    its decisions are not applied. That is deliberate: the 1.5 baseline must be
    measured through exactly the same sampling path as the adaptive arm, so any
    difference between the two is adaptation and not instrumentation.
    """
    shed = ShedState()
    controller = LagController(config)
    monitor = LagMonitor(bootstrap, topic, group)
    pool = WorkerPool(bootstrap, topic, group, dsn, shed, max_rate=max_rate)

    await monitor.start()
    await pool.scale_to(static_workers if static_workers is not None else config.min_workers)
    result = SupervisionResult()
    loop = asyncio.get_running_loop()
    deadline = None if duration_s is None else loop.time() + duration_s

    try:
        while deadline is None or loop.time() < deadline:
            await asyncio.sleep(interval_s)
            sample = await monitor.sample()
            decision = controller.observe(sample)
            result.samples.append(sample)
            result.decisions.append(decision)
            # Every sample, not only the ones that act: a controller that holds
            # is making a decision too, and without this the reason it held is
            # invisible. Also the lag time series the 1.5 benchmark measures.
            log.info(
                "lag sample",
                extra={
                    "t": round(sample.at, 2),
                    "lag": sample.total,
                    "slope_per_s": round(decision.slope, 1),
                    "t_stat": round(decision.t_stat, 2),
                    "workers": pool.size,
                    "shedding": decision.shedding,
                    "action": decision.action if static_workers is None else "static",
                    "reason": decision.reason,
                },
            )

            if static_workers is not None:
                continue  # observe and log, but never act

            if decision.action in ("scale_up", "scale_down"):
                await pool.scale_to(decision.workers)
            elif decision.action == "shed":
                shed.fraction = shed_fraction
                log.warning(
                    "SHEDDING ENGAGED", extra={"fraction": shed_fraction, "lag": sample.total}
                )
            elif decision.action == "unshed":
                log.warning(
                    "shedding released",
                    extra={"dropped": shed.dropped, "cells": len(shed.cells_shed)},
                )
                shed.fraction = 0.0
    finally:
        result.shed_dropped = shed.dropped
        await pool.close()
        await monitor.stop()
        log.info(
            "supervisor stopped",
            extra={
                "decisions": len(controller.state.history),
                "shed_dropped": shed.dropped,
                "shed_cells": sorted(shed.cells_shed)[:20],
            },
        )
    return result


async def _main() -> None:
    s = get_settings()
    p = argparse.ArgumentParser(description="Run the adaptive lag controller.")
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--group", default=s.kafka_consumer_group)
    p.add_argument("--dsn", default=s.postgres_dsn)
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--duration", type=float, default=None)
    p.add_argument("--min-workers", type=int, default=1)
    p.add_argument("--max-workers", type=int, default=4)
    p.add_argument("--shed-fraction", type=float, default=0.25)
    p.add_argument("--cooldown-s", type=float, default=10.0)
    p.add_argument("--growth-threshold", type=float, default=5.0)
    p.add_argument("--max-rate", type=float, default=None, help="per-worker events/s cap")
    p.add_argument("--static-workers", type=int, default=None,
                   help="disable the controller and hold this worker count (baseline run)")
    args = p.parse_args()
    configure(s.log_level)

    if args.static_workers:
        # The 1.5 baseline: same pipeline, no adaptation.
        shed = ShedState()
        pool = WorkerPool(args.bootstrap, args.topic, args.group, args.dsn, shed,
                          max_rate=args.max_rate)
        await pool.scale_to(args.static_workers)
        log.info("static pool running", extra={"workers": args.static_workers})
        try:
            await asyncio.sleep(args.duration or 3600)
        finally:
            await pool.close()
        return

    await supervise(
        args.bootstrap, args.topic, args.group, args.dsn,
        ControllerConfig(
            min_workers=args.min_workers, max_workers=args.max_workers,
            cooldown_s=args.cooldown_s, growth_threshold=args.growth_threshold,
        ),
        interval_s=args.interval, duration_s=args.duration,
        shed_fraction=args.shed_fraction, max_rate=args.max_rate,
    )


if __name__ == "__main__":
    asyncio.run(_main())
