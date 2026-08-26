"""Synthetic flight-telemetry source with dial-able messiness.

Split deliberately in two:

* `SyntheticSource.simulate()` is pure and runs on a virtual clock - seeded,
  deterministic, instant. Tests and Phase 1 benchmarks use this.
* `SyntheticSource.stream()` is a thin shell that paces the same events to wall
  clock so they can be published live.

The chaos model works by delaying *arrival* while leaving `event_time` truthful:
an event pushed onto the release heap with a delay is emitted later than events
generated after it, which is exactly what out-of-order arrival looks like.
"""

import argparse
import asyncio
import heapq
import itertools
import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import cos, radians, sin
from typing import AsyncIterator, Iterator

from src.common.config import get_settings
from src.common.models import FlightState

log = logging.getLogger("contrail.ingestor.synthetic")

M_PER_DEG_LAT = 111_320.0


@dataclass
class ChaosConfig:
    """Every knob Phase 1 needs to dial disorder up and down. Nothing hardcoded."""

    out_of_order_prob: float = 0.0
    max_skew_s: float = 5.0
    duplicate_prob: float = 0.0
    late_prob: float = 0.0
    late_delay_s: float = 90.0
    drop_prob: float = 0.0

    @classmethod
    def from_settings(cls, s=None) -> "ChaosConfig":
        s = s or get_settings()
        return cls(
            out_of_order_prob=s.chaos_out_of_order_prob,
            max_skew_s=s.chaos_max_skew_s,
            duplicate_prob=s.chaos_duplicate_prob,
            late_prob=s.chaos_late_prob,
            late_delay_s=s.chaos_late_delay_s,
            drop_prob=s.chaos_drop_prob,
        )


@dataclass
class Aircraft:
    icao24: str
    callsign: str
    lat: float
    lon: float
    heading: float
    velocity_ms: float
    altitude_m: float
    turn_rate: float  # deg/s, constant per aircraft -> wide circular tracks
    climb_rate: float  # m/s

    def advance(self, dt: float) -> None:
        self.heading = (self.heading + self.turn_rate * dt) % 360.0
        dist = self.velocity_ms * dt
        self.lat += dist * cos(radians(self.heading)) / M_PER_DEG_LAT
        # Guard the pole singularity: cos(lat) -> 0 makes the longitude step explode.
        self.lat = max(-85.0, min(85.0, self.lat))
        self.lon += (
            dist
            * sin(radians(self.heading))
            / (M_PER_DEG_LAT * max(0.1, cos(radians(self.lat))))
        )
        self.lon = (self.lon + 180.0) % 360.0 - 180.0
        self.altitude_m = max(0.0, min(13_000.0, self.altitude_m + self.climb_rate * dt))

    def state(self, event_time: datetime) -> FlightState:
        return FlightState(
            icao24=self.icao24,
            callsign=self.callsign,
            lat=self.lat,
            lon=self.lon,
            altitude_m=round(self.altitude_m, 1),
            velocity_ms=round(self.velocity_ms, 2),
            heading=round(self.heading, 2) % 360.0,  # round() can push 359.999 to 360.0
            event_time=event_time,
            ingest_time=event_time,  # replaced with the real arrival time at release
        )


class SyntheticSource:
    name = "synthetic"

    def __init__(
        self,
        n_aircraft: int = 50,
        rate_hz: float = 1.0,
        chaos: ChaosConfig | None = None,
        seed: int = 1337,
        duration_s: float | None = None,
        start_time: datetime | None = None,
    ) -> None:
        self.n_aircraft = n_aircraft
        self.rate_hz = rate_hz
        self.chaos = chaos or ChaosConfig()
        self.seed = seed
        self.duration_s = duration_s
        self.start_time = start_time or datetime.now(UTC)
        self._rnd = random.Random(seed)
        self.fleet = [self._spawn(i) for i in range(n_aircraft)]

    def _spawn(self, i: int) -> Aircraft:
        r = self._rnd
        return Aircraft(
            icao24=f"{r.getrandbits(24):06x}",
            callsign=f"CTR{i:04d}",
            lat=r.uniform(-60.0, 70.0),
            lon=r.uniform(-180.0, 180.0),
            heading=r.uniform(0.0, 359.9),
            velocity_ms=r.uniform(150.0, 260.0),
            altitude_m=r.uniform(2_000.0, 12_000.0),
            turn_rate=r.uniform(-0.15, 0.15),
            climb_rate=r.uniform(-3.0, 3.0),
        )

    def _arrival_delay(self) -> float:
        """Seconds between an event happening and the pipeline seeing it."""
        c = self.chaos
        r = self._rnd.random()
        if r < c.late_prob:
            # Far beyond any window we will configure -> genuinely late, not just skewed.
            return self._rnd.uniform(c.late_delay_s, 2.0 * c.late_delay_s)
        if r < c.late_prob + c.out_of_order_prob:
            return self._rnd.uniform(0.0, c.max_skew_s)
        return 0.0

    def simulate(self, duration_s: float | None = None) -> Iterator[FlightState]:
        """Virtual-clock event stream, yielded in arrival order."""
        duration_s = duration_s if duration_s is not None else self.duration_s
        dt = 1.0 / self.rate_hz
        ticks = (
            itertools.count()
            if duration_s is None
            else range(int(duration_s * self.rate_hz))
        )

        heap: list[tuple[float, int, FlightState]] = []
        seq = itertools.count()
        t = 0.0

        for _tick in ticks:
            event_time = self.start_time + timedelta(seconds=t)
            for aircraft in self.fleet:
                aircraft.advance(dt)
                if self._rnd.random() < self.chaos.drop_prob:
                    continue  # simulated packet loss: this event never arrives
                event = aircraft.state(event_time)
                copies = 2 if self._rnd.random() < self.chaos.duplicate_prob else 1
                for _copy in range(copies):
                    heapq.heappush(heap, (t + self._arrival_delay(), next(seq), event))
            yield from self._release(heap, until=t)
            t += dt

        yield from self._release(heap, until=float("inf"))

    def _release(self, heap, until: float) -> Iterator[FlightState]:
        while heap and heap[0][0] <= until:
            release_t, _seq, event = heapq.heappop(heap)
            yield event.model_copy(
                update={"ingest_time": self.start_time + timedelta(seconds=release_t)}
            )

    async def stream(self) -> AsyncIterator[FlightState]:
        """The same events as `simulate`, paced to wall clock for live publishing."""
        self.start_time = datetime.now(UTC)
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        for event in self.simulate():
            offset = (event.ingest_time - self.start_time).total_seconds()
            behind = t0 + offset - loop.time()
            if behind > 0:
                await asyncio.sleep(behind)
            yield event


def _parse_args(argv=None) -> argparse.Namespace:
    s = get_settings()
    p = argparse.ArgumentParser(
        description="Publish synthetic flight telemetry to Kafka."
    )
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--partitions", type=int, default=s.kafka_partitions)
    p.add_argument("--aircraft", type=int, default=s.gen_aircraft)
    p.add_argument("--rate", type=float, default=s.gen_rate_hz)
    p.add_argument("--seed", type=int, default=s.gen_seed)
    p.add_argument("--duration", type=float, default=None)
    p.add_argument("--out-of-order-prob", type=float, default=s.chaos_out_of_order_prob)
    p.add_argument("--max-skew-s", type=float, default=s.chaos_max_skew_s)
    p.add_argument("--duplicate-prob", type=float, default=s.chaos_duplicate_prob)
    p.add_argument("--late-prob", type=float, default=s.chaos_late_prob)
    p.add_argument("--late-delay-s", type=float, default=s.chaos_late_delay_s)
    p.add_argument("--drop-prob", type=float, default=s.chaos_drop_prob)
    return p.parse_args(argv)


async def _main(argv=None) -> None:
    from src.ingestor.base import publish

    args = _parse_args(argv)
    logging.basicConfig(level=get_settings().log_level)
    source = SyntheticSource(
        n_aircraft=args.aircraft,
        rate_hz=args.rate,
        seed=args.seed,
        duration_s=args.duration,
        chaos=ChaosConfig(
            out_of_order_prob=args.out_of_order_prob,
            max_skew_s=args.max_skew_s,
            duplicate_prob=args.duplicate_prob,
            late_prob=args.late_prob,
            late_delay_s=args.late_delay_s,
            drop_prob=args.drop_prob,
        ),
    )
    await publish(source, args.bootstrap, args.topic, args.partitions)


if __name__ == "__main__":
    asyncio.run(_main())
