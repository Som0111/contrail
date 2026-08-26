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

    def seen_before(self, event: FlightState) -> bool:
        """Check *and* mark. True means this event has already been counted.

        Split out from `add()` so the watermark engine can settle
        duplicate-or-not before it decides late-or-not -- a duplicate of a late
        event should be dropped as a duplicate, not reported twice in the side
        output.
        """
        # ponytail: unbounded seen-set, one entry per unique event. Fine for the
        # bounded replay runs the benchmarks use; bound it by the watermark if
        # this ever runs as a long-lived service.
        if event.dedup_key in self._seen:
            return True
        self._seen.add(event.dedup_key)
        return False

    def accumulate(self, window: datetime, event: FlightState) -> None:
        """Fold an event in without a duplicate check."""
        s = self._sums.setdefault((window, event.partition_key), _Sums())
        s.count += 1
        s.altitude_m += event.altitude_m
        s.velocity_ms += event.velocity_ms

    def add(self, window: datetime, event: FlightState) -> bool:
        """Returns False if this event was already counted (in any window)."""
        if self.seen_before(event):
            return False
        self.accumulate(window, event)
        return True

    def pop_window(self, window: datetime) -> dict[WindowKey, WindowAggregate]:
        """Finalize and evict every geo-cell bucket belonging to one window."""
        keys = [k for k in self._sums if k[0] == window]
        return {k: _build(k, self._sums.pop(k)) for k in keys}

    def pending_windows(self) -> set[datetime]:
        return {k[0] for k in self._sums}

    def finalize(self) -> dict[WindowKey, WindowAggregate]:
        return {key: _build(key, s) for key, s in self._sums.items()}


def _build(key: WindowKey, s: _Sums) -> WindowAggregate:
    return WindowAggregate(
        window_start=key[0],
        partition_key=key[1],
        count=s.count,
        # Rounded so two runs that sum in a different order still compare equal;
        # float addition is not associative.
        avg_altitude_m=round(s.altitude_m / s.count, 4),
        avg_velocity_ms=round(s.velocity_ms / s.count, 4),
    )


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


@dataclass
class Comparison:
    """How far an aggregate output sits from ground truth.

    Counted over the *union* of window keys, so a phantom window an engine
    invented is an error just as much as a real window it lost. Normalising by
    ground truth alone would let a processor hide invented windows for free.
    """

    windows_total: int = 0
    windows_wrong: int = 0
    missing: int = 0        # real window the processor never produced
    phantom: int = 0        # window the processor invented
    count_mismatch: int = 0
    value_mismatch: int = 0  # averages beyond tolerance
    abs_count_error: int = 0  # per-window count deviations, summed
    worst_count_error: int = 0  # largest single-window count deviation

    @property
    def window_error_rate(self) -> float:
        return self.windows_wrong / self.windows_total if self.windows_total else 0.0


def compare(
    truth: dict[WindowKey, WindowAggregate],
    got: dict[WindowKey, WindowAggregate],
    rel_tol: float = 1e-3,
) -> Comparison:
    c = Comparison(windows_total=len(set(truth) | set(got)))
    for key in set(truth) | set(got):
        t, g = truth.get(key), got.get(key)
        if g is None:
            c.missing += 1
            c.windows_wrong += 1
            c.abs_count_error += t.count
            c.worst_count_error = max(c.worst_count_error, t.count)
            continue
        if t is None:
            c.phantom += 1
            c.windows_wrong += 1
            c.abs_count_error += g.count
            c.worst_count_error = max(c.worst_count_error, g.count)
            continue
        wrong = False
        if t.count != g.count:
            c.count_mismatch += 1
            c.abs_count_error += abs(t.count - g.count)
            c.worst_count_error = max(c.worst_count_error, abs(t.count - g.count))
            wrong = True
        if not (
            _close(t.avg_altitude_m, g.avg_altitude_m, rel_tol)
            and _close(t.avg_velocity_ms, g.avg_velocity_ms, rel_tol)
        ):
            c.value_mismatch += 1
            wrong = True
        c.windows_wrong += wrong
    return c


def _close(a: float, b: float, rel_tol: float) -> bool:
    return abs(a - b) <= rel_tol * max(abs(a), abs(b), 1.0)
