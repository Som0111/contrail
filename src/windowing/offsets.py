"""Which Kafka offsets are safe to commit while window state lives only in memory.

An offset is safe once everything it contributed to has been published. A record
that sits in a still-open window is not: commit past it, crash, and the restart
never sees it again. So per partition the safe position is the lowest offset
belonging to any open window, or just past the last record read when none is open.
"""

from collections.abc import Hashable
from datetime import datetime, timedelta


class OffsetTracker:
    def __init__(self, window_s: int) -> None:
        self._window = timedelta(seconds=window_s)
        self._open: dict[datetime, dict[Hashable, int]] = {}  # window -> partition -> first offset
        self._next: dict[Hashable, int] = {}  # partition -> offset after the last record read

    def observe(self, partition: Hashable, offset: int, window: datetime) -> None:
        self._open.setdefault(window, {}).setdefault(partition, offset)
        self._next[partition] = offset + 1

    def close_before(self, boundary: datetime | None) -> None:
        """Forget windows that ended at or before `boundary` (finalized, or too late to count)."""
        if boundary is None:
            return
        for w in [w for w in self._open if w + self._window <= boundary]:
            del self._open[w]

    def safe(self) -> dict[Hashable, int]:
        out = dict(self._next)
        for per_partition in self._open.values():
            for p, offset in per_partition.items():
                out[p] = min(out[p], offset)
        return out
