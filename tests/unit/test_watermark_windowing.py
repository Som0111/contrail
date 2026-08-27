"""Proof that watermark event-time windowing attributes correctly where naive did not.

Reuses the exact controlled sequence from `test_naive_windowing.py`, so the two
processors are judged on identical input.
"""

from datetime import timedelta

import pytest

from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.windowing import naive, watermark
from src.windowing.aggregates import ground_truth
from tests.unit.test_naive_windowing import CELL, T0, WIN_A, WIN_B, event

MAX_SKEW = 20.0


def test_watermark_attributes_the_delayed_event_correctly():
    """The same three events that broke the naive baseline in 1.1."""
    events = [
        event(offset_s=10, arrival_s=10, altitude_m=1000.0, icao="aaa001"),
        event(offset_s=20, arrival_s=20, altitude_m=2000.0, icao="aaa002"),
        # Happened at 00:50 (window A), arrived at 01:30 (window B).
        event(offset_s=50, arrival_s=90, altitude_m=9000.0, icao="aaa003"),
    ]
    truth = ground_truth(events)
    got = watermark.aggregate(events, allowed_lateness_s=0.0)
    wrong = naive.aggregate(events)

    assert got.windows == truth, "watermark must reproduce ground truth exactly"
    assert got.windows[(WIN_A, CELL)].count == 3
    assert got.windows[(WIN_A, CELL)].avg_altitude_m == pytest.approx(4000.0)
    assert (WIN_B, CELL) not in got.windows, "no phantom window"
    assert got.late == [], "nothing here is late -- arrival delay alone is not lateness"

    # And the baseline still gets it wrong on the identical input.
    assert wrong != truth
    assert wrong[(WIN_A, CELL)].count == 2


def test_arrival_delay_alone_never_makes_an_event_late():
    """The watermark is driven by event time, so a straggler cannot close its own window."""
    events = [
        event(offset_s=i, arrival_s=i + 500, altitude_m=1000.0, icao=f"ccc{i:03d}")
        for i in range(0, 50, 10)
    ]
    got = watermark.aggregate(events, allowed_lateness_s=0.0)
    assert got.late == []
    assert got.windows == ground_truth(events)


def test_side_output_catches_an_event_past_the_finalized_window():
    """Advance the watermark well past window A, then deliver a straggler for it."""
    events = [
        event(offset_s=10, arrival_s=10, altitude_m=1000.0, icao="ddd001"),
        event(offset_s=20, arrival_s=20, altitude_m=2000.0, icao="ddd002"),
        # These two are far in the future: they drag the watermark past window A's end.
        event(offset_s=200, arrival_s=200, altitude_m=3000.0, icao="ddd003"),
        event(offset_s=210, arrival_s=210, altitude_m=4000.0, icao="ddd004"),
        # Straggler belonging to window A, arriving after A was finalized.
        event(offset_s=55, arrival_s=260, altitude_m=9000.0, icao="ddd005"),
    ]
    got = watermark.aggregate(events, allowed_lateness_s=30.0)

    assert len(got.late) == 1, "the straggler must be reported, not silently dropped"
    assert got.late[0].event.icao24 == "ddd005"
    assert got.late[0].window == WIN_A
    assert got.late[0].lateness_s > 0

    # Window A closed with the two events it had; the straggler did not corrupt it,
    # and was not misfiled into a later window either.
    assert got.windows[(WIN_A, CELL)].count == 2
    assert got.windows[(WIN_A, CELL)].avg_altitude_m == pytest.approx(1500.0)
    assert sum(a.count for a in got.windows.values()) == 4


def test_side_output_is_empty_when_lateness_allowance_covers_the_delay():
    """Same stream, a lateness bound generous enough to still hold window A open."""
    events = [
        event(offset_s=10, arrival_s=10, altitude_m=1000.0, icao="ddd001"),
        event(offset_s=20, arrival_s=20, altitude_m=2000.0, icao="ddd002"),
        event(offset_s=200, arrival_s=200, altitude_m=3000.0, icao="ddd003"),
        event(offset_s=55, arrival_s=260, altitude_m=9000.0, icao="ddd005"),
    ]
    got = watermark.aggregate(events, allowed_lateness_s=200.0)
    assert got.late == []
    assert got.windows == ground_truth(events)
    assert got.windows[(WIN_A, CELL)].count == 3


def test_a_duplicate_of_a_late_event_is_a_duplicate_not_a_second_late_report():
    straggler = event(offset_s=55, arrival_s=260, altitude_m=9000.0, icao="eee005")
    events = [
        event(offset_s=10, arrival_s=10, altitude_m=1000.0, icao="eee001"),
        event(offset_s=200, arrival_s=200, altitude_m=3000.0, icao="eee003"),
        straggler,
        straggler,
    ]
    got = watermark.aggregate(events, allowed_lateness_s=30.0)
    assert len(got.late) == 1
    assert got.duplicates == 1


def _generated(chaos: ChaosConfig, ticks=600, aircraft=20, seed=11):
    source = SyntheticSource(
        n_aircraft=aircraft, rate_hz=1.0, chaos=chaos, seed=seed,
        duration_s=ticks, start_time=T0,
    )
    return list(source.simulate())


def test_lateness_bound_at_or_above_max_skew_gives_exact_output():
    """The guarantee from the module docstring, asserted on generated disorder."""
    events = _generated(
        ChaosConfig(out_of_order_prob=0.4, max_skew_s=MAX_SKEW, duplicate_prob=0.05)
    )
    truth = ground_truth(events)
    assert naive.aggregate(events) != truth, "control: the baseline is wrong on this stream"

    got = watermark.aggregate(events, allowed_lateness_s=MAX_SKEW)
    assert got.windows == truth
    assert got.late == []


def test_error_appears_only_once_disorder_exceeds_the_bound():
    """Sweep the bound downward: exact at/above max skew, degrading below it."""
    events = _generated(
        ChaosConfig(out_of_order_prob=0.4, max_skew_s=MAX_SKEW, duplicate_prob=0.05)
    )
    truth = ground_truth(events)

    late_counts = {}
    for bound in (MAX_SKEW, 10.0, 5.0, 0.0):
        got = watermark.aggregate(events, allowed_lateness_s=bound)
        late_counts[bound] = len(got.late)

    assert late_counts[MAX_SKEW] == 0
    # Monotone: a tighter bound can only make more events late, never fewer.
    assert late_counts[10.0] <= late_counts[5.0] <= late_counts[0.0]
    assert late_counts[0.0] > 0

    # Even at the tightest bound the watermark still beats naive on window count.
    tight = watermark.aggregate(events, allowed_lateness_s=0.0)
    wm_wrong = sum(
        1 for k, a in truth.items()
        if k not in tight.windows or tight.windows[k].count != a.count
    )
    nv = naive.aggregate(events)
    nv_wrong = sum(1 for k, a in truth.items() if k not in nv or nv[k].count != a.count)
    assert wm_wrong < nv_wrong


def test_late_events_are_accounted_for_not_lost():
    """Every arrived event ends up either in a window or in the side output."""
    events = _generated(
        ChaosConfig(out_of_order_prob=0.3, max_skew_s=MAX_SKEW,
                    late_prob=0.05, late_delay_s=180.0, duplicate_prob=0.05)
    )
    got = watermark.aggregate(events, allowed_lateness_s=30.0)
    assert len(got.late) > 0, "late chaos must actually produce late events"
    windowed = sum(a.count for a in got.windows.values())
    assert windowed + len(got.late) == got.processed
    assert got.processed + got.duplicates == len(events)


def test_watermark_never_runs_ahead_of_max_event_time():
    events = _generated(ChaosConfig(out_of_order_prob=0.3, max_skew_s=MAX_SKEW), ticks=120)
    proc = watermark.WatermarkProcessor(allowed_lateness_s=15.0)
    for e in events:
        proc.process(e)
        assert proc.watermark <= proc.max_event_time - timedelta(seconds=15.0)
    assert proc.close().final_watermark is not None
