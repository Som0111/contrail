"""Event-time windowing with watermarks — core claim #1.

A watermark is the engine's assertion about completeness: "I do not expect to
see any event older than this any more." Here it is the classic bounded-disorder
estimator,

    watermark = max(event_time seen so far) - allowed_lateness

and a window is finalized once the watermark passes its end. Crucially the
watermark is driven by *event time*, never by arrival time or wall clock, which
is exactly why a delayed event still lands in the window it belongs to: being
late does not advance anything, so it cannot close its own window.

That gives a provable guarantee, and the tests assert it directly:

    allowed_lateness >= max arrival skew  =>  output is identical to ground truth

Proof sketch — since `ingest_time >= event_time` always holds, every event that
has arrived when event `e` arrives has `event_time <= e.event_time + skew(e)`.
So the watermark when `e` arrives is at most `e.event_time + max_skew - L`,
which is below `e`'s window end whenever `L >= max_skew`. Its window is
therefore still open. Beyond that bound the guarantee degrades gracefully rather
than silently: events that miss their finalized window go to the side output,
counted and logged, never dropped in silence.
"""

import argparse
import asyncio
import logging
import math
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from src.common.config import get_settings
from src.common.logging import configure
from src.common.models import FlightState
from src.windowing.aggregates import (
    DEFAULT_WINDOW_S,
    Aggregator,
    WindowAggregate,
    WindowKey,
    window_start,
)

log = logging.getLogger("contrail.windowing.watermark")

DEFAULT_ALLOWED_LATENESS_S = 30.0


@dataclass
class LateEvent:
    """An event that missed its window even after the lateness allowance."""

    event: FlightState
    window: datetime
    watermark: datetime

    @property
    def lateness_s(self) -> float:
        return (self.watermark - self.window).total_seconds()


@dataclass
class WatermarkResult:
    windows: dict[WindowKey, WindowAggregate] = field(default_factory=dict)
    late: list[LateEvent] = field(default_factory=list)
    duplicates: int = 0
    processed: int = 0
    final_watermark: datetime | None = None


def min_seen_retention_windows(window_s: int, allowed_lateness_s: float) -> int:
    """Fewest dedup windows that still recognise a redelivery as a duplicate.

    A key is evicted once its window is `retention` windows behind the closed
    frontier, and the frontier trails the newest event by the lateness bound. Any
    retention of at least one keeps a key for as long as its window is open, so an
    aggregate is never double counted; this formula additionally keeps it for one
    lateness bound past that, so a redelivery inside the bound is still reported as
    a duplicate rather than as a late event.
    """
    return math.ceil(allowed_lateness_s / window_s) + 1


class WatermarkProcessor:
    """Streaming event-time windowing. Feed it events in arrival order."""

    def __init__(
        self,
        window_s: int = DEFAULT_WINDOW_S,
        allowed_lateness_s: float = DEFAULT_ALLOWED_LATENESS_S,
        on_finalize: Callable[[WindowAggregate], None] | None = None,
        seen_retention_windows: int | None = None,
        late_retention: int | None = None,
        retain_windows: bool = True,
        idle_timeout_s: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        await_sources: Iterable[str] = (),
    ) -> None:
        if idle_timeout_s is not None and idle_timeout_s <= allowed_lateness_s:
            # A source that is merely slow by less than the lateness bound would be
            # dropped from the minimum, and its still-on-time events then arrive late.
            raise ValueError(
                f"idle_timeout_s ({idle_timeout_s}) must exceed allowed_lateness_s "
                f"({allowed_lateness_s})"
            )
        if seen_retention_windows is not None:
            need = min_seen_retention_windows(window_s, allowed_lateness_s)
            if seen_retention_windows < need:
                log.warning(
                    "dedup retention shorter than the lateness bound; redeliveries inside it "
                    "will be reported as late events, not duplicates",
                    extra={"seen_retention_windows": seen_retention_windows, "recommended": need},
                )
        self.on_finalize = on_finalize
        # Idle-source handling is the one place this class may read a wall clock, so
        # it is opt-in. With idle_timeout_s=None the clock is never touched and replay
        # stays a pure function of the input.
        self.idle_timeout_s = idle_timeout_s
        self._clock = clock
        self._last_seen: dict[str, float] = {}
        self._idle: set[str] = set()
        # Sources known to have a backlog to deliver (a restarted service replaying from its
        # last safe offset). Until each has delivered once the watermark holds, because a
        # source we have not heard from yet would otherwise be ignored by the minimum and the
        # partitions that arrive first would close windows the backlog still belongs to.
        # Empty by default, which leaves a fresh live start and every replay unchanged.
        self._awaiting = set(await_sources)
        if idle_timeout_s is not None:
            for s in self._awaiting:  # a backlog source that never shows up times out like any other
                self._last_seen[s] = clock()
        self.idle_transitions = 0  # source went idle
        self.resume_transitions = 0  # idle source delivered again
        # All three default to unbounded, which is what replay and the benchmarks
        # need. The live service, which never exits, sets all three -- see
        # DESIGN_DECISIONS.md 2.8.
        self.seen_retention_windows = seen_retention_windows
        self.late_retention = late_retention
        self.retain_windows = retain_windows
        self.window_s = window_s
        self.allowed_lateness = timedelta(seconds=allowed_lateness_s)
        self._agg = Aggregator(window_s, seen_retention_windows)
        # One high-water mark per source (Kafka partition), with the global
        # watermark taken as the MINIMUM across them -- what Flink and Beam do.
        # A single global max is wrong the moment input is partitioned: one
        # partition running ahead would finalize windows on behalf of partitions
        # that have not delivered their events yet, and every one of those events
        # would then be correctly-but-uselessly reported late. Offline replay
        # sidesteps this by sorting the whole stream first (see collect()); a live
        # consumer cannot, because it has not seen the whole stream.
        self._max_by_source: dict[str, datetime] = {}
        self._finalized_before: datetime | None = None  # windows ending at/before this are closed
        self._late_total = 0
        self._result = WatermarkResult()

    @property
    def late_count(self) -> int:
        """Total events sent to the side output. Exact even when the list is capped."""
        return self._late_total

    @property
    def finalized_before(self) -> datetime | None:
        """Windows ending at or before this are closed; later events for them are late."""
        return self._finalized_before

    @property
    def max_event_time(self) -> datetime | None:
        return max(self._max_by_source.values(), default=None)

    @property
    def duplicate_count(self) -> int:
        """Events suppressed as duplicates so far."""
        return self._result.duplicates

    @property
    def seen_size(self) -> int:
        """Dedup keys currently retained."""
        return self._agg.seen_size

    @property
    def seen_evicted(self) -> int:
        """Dedup keys evicted so far."""
        return self._agg.evicted_keys

    @property
    def idle_sources(self) -> frozenset[str]:
        return frozenset(self._idle)

    @property
    def watermark(self) -> datetime | None:
        if not self._max_by_source:
            return None
        if any(s not in self._max_by_source and s not in self._idle for s in self._awaiting):
            return None
        # Idle sources drop out of the minimum. If every source is idle there is
        # nothing to advance on, so fall back to all of them and hold position.
        active = [t for s, t in self._max_by_source.items() if s not in self._idle]
        return min(active or self._max_by_source.values()) - self.allowed_lateness

    def tick(self) -> None:
        """Mark sources silent for `idle_timeout_s` idle and close what that releases.

        Has to be called periodically, since a silent source produces no event to
        trigger it. A no-op unless `idle_timeout_s` is set.
        """
        if self.idle_timeout_s is None:
            return
        now = self._clock()
        newly = [
            s for s, seen in self._last_seen.items()
            if s not in self._idle and now - seen >= self.idle_timeout_s
        ]
        for s in newly:
            self._idle.add(s)
            self.idle_transitions += 1
            log.warning("source idle, excluded from watermark",
                        extra={"source": s, "idle_after_s": self.idle_timeout_s,
                               "idle_sources": len(self._idle)})
        if newly:
            self._finalize()

    def process(self, event: FlightState, source: str = "") -> None:
        if self.idle_timeout_s is not None:
            self._last_seen[source] = self._clock()
            if source in self._idle:
                # Rejoin with the mark it had when it went quiet; its events for windows
                # closed in the meantime take the ordinary late path below.
                self._idle.discard(source)
                self.resume_transitions += 1
                log.warning("idle source resumed, rejoined watermark",
                            extra={"source": source, "idle_sources": len(self._idle)})
            self.tick()
        # Duplicate first: a repeat of a late event is a duplicate, not a second
        # late report. Keeps this identical to the naive baseline's dedup.
        if self._agg.seen_before(event):
            self._result.duplicates += 1
            return
        self._result.processed += 1

        window = window_start(event.event_time, self.window_s)
        window_end = window + timedelta(seconds=self.window_s)

        if self._finalized_before is not None and window_end <= self._finalized_before:
            late = LateEvent(event, window, self._finalized_before)
            self._late_total += 1
            self._result.late.append(late)
            if self.late_retention is not None and len(self._result.late) > self.late_retention:
                # Keep a recent sample for inspection; the count above stays exact.
                del self._result.late[: -self.late_retention]
            log.warning(
                "late event past allowed lateness",
                extra={
                    "icao24": event.icao24,
                    "event_time": event.event_time,
                    "ingest_time": event.ingest_time,
                    "window": window,
                    "watermark": self._finalized_before,
                    "late_by_s": round(late.lateness_s, 3),
                },
            )
        else:
            self._agg.accumulate(window, event)

        self._advance(event.event_time, source)

    def _advance(self, event_time: datetime, source: str = "") -> None:
        """Push this source's high-water mark forward and close what the global one passed."""
        current = self._max_by_source.get(source)
        if current is None or event_time > current:
            self._max_by_source[source] = event_time
        self._finalize()

    def _finalize(self) -> None:
        mark = self.watermark
        if mark is None:
            return
        self._finalized_before = (
            mark if self._finalized_before is None else max(self._finalized_before, mark)
        )
        if self.seen_retention_windows is not None:
            self._agg.forget_before(
                self._finalized_before
                - timedelta(seconds=self.window_s * self.seen_retention_windows)
            )
        for window in sorted(self._agg.pending_windows()):
            if window + timedelta(seconds=self.window_s) <= self._finalized_before:
                finalized = self._agg.pop_window(window)
                if self.retain_windows:
                    self._result.windows.update(finalized)
                if self.on_finalize:
                    for aggregate in finalized.values():
                        self.on_finalize(aggregate)

    def close(self) -> WatermarkResult:
        """End of stream: everything still open is complete by definition."""
        remaining = self._agg.finalize()
        if self.retain_windows:
            self._result.windows.update(remaining)
        if self.on_finalize:
            for aggregate in remaining.values():
                self.on_finalize(aggregate)
        self._result.final_watermark = self.watermark
        return self._result


def aggregate(
    events: list[FlightState],
    window_s: int = DEFAULT_WINDOW_S,
    allowed_lateness_s: float = DEFAULT_ALLOWED_LATENESS_S,
) -> WatermarkResult:
    processor = WatermarkProcessor(window_s, allowed_lateness_s)
    for event in events:
        processor.process(event)
    return processor.close()


async def _main() -> None:
    from src.ingestor.base import collect
    from src.windowing.aggregates import ground_truth
    from src.windowing.naive import print_windows

    s = get_settings()
    p = argparse.ArgumentParser(description="Run watermark event-time windowing.")
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--group", default="contrail-watermark")
    p.add_argument("--window-s", type=int, default=s.window_s)
    p.add_argument("--allowed-lateness-s", type=float, default=s.allowed_lateness_s)
    p.add_argument("--duration", type=float, default=120.0)
    p.add_argument("--idle-timeout", type=float, default=10.0)
    args = p.parse_args()
    configure(s.log_level)

    events = await collect(
        args.bootstrap, args.topic, args.group, args.duration, args.idle_timeout
    )
    result = aggregate(events, args.window_s, args.allowed_lateness_s)
    truth = ground_truth(events, args.window_s)

    print_windows(result.windows, "WATERMARK (event-time windows)")
    wrong = sum(
        1
        for k, a in truth.items()
        if k not in result.windows or result.windows[k].count != a.count
    )
    print(
        f"\n  events consumed {len(events):,}  (duplicates suppressed {result.duplicates:,})"
        f"\n  allowed lateness {args.allowed_lateness_s:g}s, window {args.window_s}s"
        f"\n  final watermark  {result.final_watermark}"
        f"\n  late side-output {len(result.late):,} events"
        f"\n  windows where watermark disagrees with truth: {wrong} of {len(truth)}\n"
    )


if __name__ == "__main__":
    asyncio.run(_main())
