"""Opt-in idle-partition handling: a silent source stops holding back the watermark."""

import pytest

from src.windowing.watermark import WatermarkProcessor
from tests.unit.test_naive_windowing import T0, event

LATENESS, IDLE = 10.0, 30.0


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def make(idle=IDLE):
    clock, out = Clock(), []
    p = WatermarkProcessor(60, LATENESS, on_finalize=out.append, idle_timeout_s=idle, clock=clock)
    return p, clock, out


def feed(p, offset_s, source, icao="aaa001"):
    p.process(event(offset_s, offset_s, 1000.0, icao=icao), source)


def test_silent_partition_stalls_watermark_until_timeout():
    p, clock, out = make()
    feed(p, 5, "p1")
    clock.now = 15  # p0 is last seen later than p1, so only p1 times out first
    feed(p, 200, "p0")  # p0 races ahead; p1 sits at t=5
    assert out == [] and p.watermark < T0.replace(second=59)

    clock.now = IDLE - 1
    p.tick()
    assert out == [] and not p.idle_sources

    clock.now = IDLE
    p.tick()
    assert p.idle_sources == {"p1"} and p.idle_transitions == 1
    assert len(out) >= 1  # windows released without any new event arriving


def test_resumed_partition_with_closed_window_goes_to_side_output():
    p, clock, out = make()
    feed(p, 5, "p1")
    clock.now = 15
    feed(p, 200, "p0")
    clock.now = IDLE
    p.tick()
    finalized = {a.window_start for a in out}

    feed(p, 20, "p1", icao="aaa002")  # p1 comes back with an event for a closed window
    assert p.resume_transitions == 1 and not p.idle_sources
    assert p.late_count == 1
    assert {a.window_start for a in out} == finalized  # closed window not touched again


def test_resumed_partition_with_open_window_event_is_counted():
    p, clock, _ = make()
    feed(p, 5, "p1")
    clock.now = 15  # p0 is last seen later than p1, so only p1 times out first
    feed(p, 200, "p0")
    clock.now = IDLE
    p.tick()
    feed(p, 210, "p1", icao="aaa002")  # in a still-open window
    assert p.late_count == 0 and p.resume_transitions == 1
    result = p.close()
    assert sum(a.count for a in result.windows.values()) == 3


def test_all_partitions_idle_holds_position():
    p, clock, out = make()
    feed(p, 5, "p0")
    feed(p, 7, "p1")
    clock.now = 1000
    p.tick()
    assert p.idle_sources == {"p0", "p1"}
    assert p.watermark is not None and out == []


def test_rapid_idle_resume_cycles_are_counted():
    p, clock, _ = make()
    feed(p, 5, "p0")
    feed(p, 6, "p1")
    for i in range(3):
        clock.now += IDLE
        feed(p, 7 + i, "p1")  # keeps p1 alive
        p.tick()
        assert p.idle_sources == {"p0"} or i > 0
        feed(p, 8 + i, "p0")  # p0 resumes
    assert p.idle_transitions == p.resume_transitions == 3


def test_timeout_must_exceed_lateness():
    with pytest.raises(ValueError):
        WatermarkProcessor(60, 30.0, idle_timeout_s=30.0)


def test_default_never_reads_the_clock():
    def boom():
        raise AssertionError("clock read with idle_timeout_s=None")

    p = WatermarkProcessor(60, LATENESS, clock=boom)
    feed(p, 5, "p0")
    feed(p, 200, "p1")
    p.tick()
    assert p.idle_sources == frozenset()


# --- sources awaited at startup (restart replaying a backlog) ----------------------

def awaiting(idle=None, clock=None, **kw):
    out = []
    p = WatermarkProcessor(60, LATENESS, on_finalize=out.append, idle_timeout_s=idle,
                           clock=clock or Clock(), await_sources={"p0", "p1"}, **kw)
    return p, out


def test_watermark_holds_until_every_backlog_source_has_delivered():
    p, out = awaiting()
    feed(p, 500, "p0")  # p0 races far ahead while p1's older backlog is still unread
    assert p.watermark is None and out == []
    feed(p, 10, "p1")  # p1 delivers an old event; it must still land in its window
    assert p.late_count == 0
    feed(p, 600, "p1")
    assert out, "windows close once both have delivered"
    assert p.late_count == 0


def test_without_awaited_sources_the_early_partition_does_close_windows():
    """The behaviour awaiting exists to prevent (and the unchanged default)."""
    out = []
    p = WatermarkProcessor(60, LATENESS, on_finalize=out.append)
    feed(p, 500, "p0")
    feed(p, 10, "p1")
    assert p.late_count == 1


def test_an_awaited_source_that_never_shows_up_times_out_when_idle_handling_is_on():
    clock = Clock()
    p, out = awaiting(idle=IDLE, clock=clock)
    feed(p, 500, "p0")
    feed(p, 600, "p1")
    assert p.watermark is not None  # both delivered, so the watermark is live
    p2, _ = awaiting(idle=IDLE, clock=clock)
    clock.now += 15  # p0 is heard from later than the seeded start, so only p1 times out first
    feed(p2, 500, "p0")
    assert p2.watermark is None
    clock.now += IDLE - 15
    p2.tick()
    assert p2.idle_sources == {"p1"} and p2.watermark is not None
