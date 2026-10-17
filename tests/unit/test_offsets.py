"""The commit boundary for the windowing service: never past a record in an open window."""

from datetime import UTC, datetime, timedelta

from src.windowing.offsets import OffsetTracker

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def w(n):
    return T0 + timedelta(seconds=10 * n)


def test_nothing_open_commits_past_last_read():
    t = OffsetTracker(10)
    t.observe("p0", 5, w(0))
    t.observe("p0", 6, w(0))
    t.close_before(w(1))
    assert t.safe() == {"p0": 7}


def test_open_window_holds_commit_at_its_first_offset():
    t = OffsetTracker(10)
    t.observe("p0", 5, w(0))
    t.observe("p0", 6, w(1))
    t.observe("p1", 2, w(1))
    t.close_before(w(1))  # w(0) finalized; w(1) still open
    assert t.safe() == {"p0": 6, "p1": 2}


def test_out_of_order_record_keeps_older_window_pinned():
    t = OffsetTracker(10)
    t.observe("p0", 1, w(1))
    t.observe("p0", 2, w(0))  # earlier window arrives later
    t.close_before(w(1))
    assert t.safe() == {"p0": 1}
    t.close_before(w(2))
    assert t.safe() == {"p0": 3}
