"""Proof that processing-time windows misattribute delayed events.

The controlled sequence is hand-built rather than generated, so the expected
answer is arithmetic, not something recomputed by the code under test.
"""

from datetime import UTC, datetime, timedelta

import pytest

from src.common.models import FlightState
from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.windowing import naive
from src.windowing.aggregates import DEFAULT_WINDOW_S, ground_truth, window_start

T0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)  # a window boundary
WIN_A = T0
WIN_B = T0 + timedelta(seconds=60)


def event(offset_s: float, arrival_s: float, altitude_m: float, icao="aaa001"):
    """One aircraft, fixed position (so all events land in one geo cell)."""
    return FlightState(
        icao24=icao,
        callsign="TEST001",
        lat=51.4,
        lon=-0.1,
        altitude_m=altitude_m,
        velocity_ms=200.0,
        heading=90.0,
        event_time=T0 + timedelta(seconds=offset_s),
        ingest_time=T0 + timedelta(seconds=arrival_s),
    )


CELL = event(0, 0, 0).partition_key


def test_naive_misattributes_an_event_delayed_across_a_boundary():
    """Three events all belong to window A. One arrives 30s into window B."""
    events = [
        event(offset_s=10, arrival_s=10, altitude_m=1000.0, icao="aaa001"),
        event(offset_s=20, arrival_s=20, altitude_m=2000.0, icao="aaa002"),
        # Happened at 00:50 (window A), arrived at 01:30 (window B).
        event(offset_s=50, arrival_s=90, altitude_m=9000.0, icao="aaa003"),
    ]

    truth = ground_truth(events)
    got = naive.aggregate(events)

    # Ground truth: all three in window A, nothing in B.
    assert set(truth) == {(WIN_A, CELL)}
    assert truth[(WIN_A, CELL)].count == 3
    assert truth[(WIN_A, CELL)].avg_altitude_m == pytest.approx(4000.0)

    # Naive: only two in A, and a phantom window B built from the delayed event.
    assert set(got) == {(WIN_A, CELL), (WIN_B, CELL)}
    assert got[(WIN_A, CELL)].count == 2, "the delayed event was stolen from window A"
    assert got[(WIN_B, CELL)].count == 1, "and misattributed to window B"

    # The error is not cosmetic: window A's average altitude is off by 2500 m.
    assert got[(WIN_A, CELL)].avg_altitude_m == pytest.approx(1500.0)
    assert truth[(WIN_A, CELL)].avg_altitude_m - got[(WIN_A, CELL)].avg_altitude_m == (
        pytest.approx(2500.0)
    )


def test_naive_is_exactly_right_when_nothing_is_delayed():
    """The baseline is not broken, it is fragile. Ordered input hides the bug."""
    events = [event(offset_s=i * 5, arrival_s=i * 5, altitude_m=1000.0 * i) for i in range(1, 10)]
    for i, e in enumerate(events):
        events[i] = e.model_copy(update={"icao24": f"bbb{i:03d}"})
    assert naive.aggregate(events) == ground_truth(events)


def test_naive_counts_a_duplicate_only_once():
    """Dedup is shared, so measured error is attribution error and nothing else."""
    e = event(offset_s=10, arrival_s=10, altitude_m=1000.0)
    assert naive.aggregate([e, e, e])[(WIN_A, CELL)].count == 1


def _generated(chaos: ChaosConfig, ticks=600, aircraft=20, seed=11):
    source = SyntheticSource(
        n_aircraft=aircraft,
        rate_hz=1.0,
        chaos=chaos,
        seed=seed,
        duration_s=ticks,
        start_time=T0,
    )
    return list(source.simulate())


def test_generator_ordered_stream_gives_zero_naive_error():
    """Sanity check on the harness itself: with no disorder there is no error."""
    events = _generated(ChaosConfig(duplicate_prob=0.1, drop_prob=0.05))
    assert naive.aggregate(events) == ground_truth(events)


def test_generator_disordered_stream_makes_naive_measurably_wrong():
    events = _generated(
        ChaosConfig(out_of_order_prob=0.3, max_skew_s=20.0, late_prob=0.05, duplicate_prob=0.05)
    )
    truth = ground_truth(events)
    got = naive.aggregate(events)

    disagreeing = [
        k for k, a in truth.items() if k not in got or got[k].count != a.count
    ]
    assert len(disagreeing) > 0
    # Every window should be touched: with 20s of skew on a 60s window, roughly a
    # third of each window's events can cross a boundary.
    assert len(disagreeing) / len(truth) > 0.5

    # And the total is conserved -- naive loses nothing, it files it in the wrong place.
    assert sum(a.count for a in got.values()) == sum(a.count for a in truth.values())


def test_naive_invents_windows_beyond_the_real_event_span():
    """Late arrivals push the naive processor past the end of the actual data."""
    events = _generated(ChaosConfig(late_prob=0.2, late_delay_s=120.0), ticks=300)
    truth = ground_truth(events)
    got = naive.aggregate(events)
    assert max(k[0] for k in got) > max(k[0] for k in truth)


def test_window_start_floors_to_the_boundary():
    assert window_start(T0 + timedelta(seconds=59.999)) == T0
    assert window_start(T0 + timedelta(seconds=60)) == T0 + timedelta(seconds=60)
    assert window_start(T0 + timedelta(seconds=125), window_s=DEFAULT_WINDOW_S) == (
        T0 + timedelta(seconds=120)
    )
