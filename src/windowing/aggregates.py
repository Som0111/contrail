"""Shared window-aggregate machinery.

Both processors — the naive processing-time baseline (1.1) and the watermark
event-time engine (1.2) — produce exactly this shape, so 1.3 can diff them
against each other and against ground truth without any translation layer.

Deduplication lives here rather than in either processor, so that the only
difference between them is *which timestamp decides the window*. If naive were
allowed to double-count duplicates, its measured error would mix two unrelated
faults and the comparison would prove nothing about event-time attribution.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from src.common.models import FlightState

DEFAULT_WINDOW_S = 60

WindowKey = tuple[datetime, str]


def window_start(ts: datetime, window_s: int = DEFAULT_WINDOW_S) -> datetime:
    """Floor a timestamp to its tumbling-window boundary."""
    return datetime.fromtimestamp(int(ts.timestamp() // window_s) * window_s, UTC)


@dataclass(frozen=True)
class WindowAggregate:
    window_start: datetime
    partition_key: str
    count: int
    avg_altitude_m: float
    avg_velocity_ms: float

    @property
    def key(self) -> WindowKey:
        return (self.window_start, self.partition_key)


@dataclass
class _Sums:
    count: int = 0
    altitude_m: float = 0.0
    velocity_ms: float = 0.0


class Aggregator:
    """Folds events into (window, geo cell) buckets, dropping repeats on the way in."""

    def __init__(self) -> None:
        self._sums: dict[WindowKey, _Sums] = {}
        self._seen: set[tuple[str, datetime]] = set()

    def add(self, window: datetime, event: FlightState) -> bool:
        """Returns False if this event was already counted (in any window)."""
        if event.dedup_key in self._seen:
            return False
        self._seen.add(event.dedup_key)
        s = self._sums.setdefault((window, event.partition_key), _Sums())
        s.count += 1
        s.altitude_m += event.altitude_m
        s.velocity_ms += event.velocity_ms
        return True

    def finalize(self) -> dict[WindowKey, WindowAggregate]:
        return {
            key: WindowAggregate(
                window_start=key[0],
                partition_key=key[1],
                count=s.count,
                # Rounded so two runs that sum in a different order still compare
                # equal; float addition is not associative.
                avg_altitude_m=round(s.altitude_m / s.count, 4),
                avg_velocity_ms=round(s.velocity_ms / s.count, 4),
            )
            for key, s in self._sums.items()
        }


def ground_truth(
    events: list[FlightState], window_s: int = DEFAULT_WINDOW_S
) -> dict[WindowKey, WindowAggregate]:
    """The correct answer: every arrived event attributed by its own `event_time`.

    Defined over the events that actually *arrived*, not over everything the
    generator produced. A dropped event is not a windowing error — no processor
    could attribute a record it never received, so counting drops against them
    would make the comparison meaningless.
    """
    agg = Aggregator()
    for event in events:
        agg.add(window_start(event.event_time, window_s), event)
    return agg.finalize()
