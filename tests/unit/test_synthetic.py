"""Proofs that the chaos knobs do what they claim.

Phase 1 benchmarks are only meaningful if the disorder we dial in is the
disorder we actually get, so these assert on measured rates, not on the code
path having run.
"""

from collections import Counter
from datetime import UTC, datetime

import pytest

from src.common.models import grid_cell
from src.ingestor.synthetic import ChaosConfig, SyntheticSource

# Large enough that a rate measured over it is tight around its expectation.
BIG_AIRCRAFT = 200
BIG_TICKS = 100  # -> 20_000 generated events


# Pinned so runs are comparable across constructions.
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def run(chaos: ChaosConfig, aircraft=20, ticks=50, seed=7):
    source = SyntheticSource(
        n_aircraft=aircraft,
        rate_hz=1.0,
        chaos=chaos,
        seed=seed,
        duration_s=ticks,
        start_time=T0,
    )
    return list(source.simulate())


def test_ordered_when_chaos_off():
    events = run(ChaosConfig())
    times = [e.event_time for e in events]
    assert times == sorted(times), "with all chaos off, arrival order must match event order"
    assert all(e.ingest_time == e.event_time for e in events)


def test_out_of_order_produces_inversions():
    events = run(ChaosConfig(out_of_order_prob=0.3, max_skew_s=10.0))
    inversions = sum(
        1
        for a, b in zip(events, events[1:])
        if b.event_time < a.event_time
    )
    assert inversions > 0, "out-of-order probability > 0 must produce arrival inversions"
    # And the events themselves are still truthful: arrival never precedes occurrence.
    assert all(e.ingest_time >= e.event_time for e in events)


def test_no_inversions_without_out_of_order_even_at_high_rate():
    events = run(ChaosConfig(duplicate_prob=0.5, drop_prob=0.2))
    times = [e.event_time for e in events]
    assert times == sorted(times), "duplicates and drops alone must not reorder arrival"


@pytest.mark.parametrize("prob", [0.05, 0.2])
def test_duplicate_rate_matches_config(prob):
    events = run(ChaosConfig(duplicate_prob=prob), BIG_AIRCRAFT, BIG_TICKS)
    counts = Counter(e.dedup_key for e in events)
    unique = len(counts)
    duplicated = sum(1 for c in counts.values() if c > 1)
    measured = duplicated / unique
    assert measured == pytest.approx(prob, rel=0.10), f"measured {measured:.4f} vs configured {prob}"
    assert len(events) == unique + duplicated


@pytest.mark.parametrize("prob", [0.05, 0.2])
def test_drop_rate_matches_config(prob):
    generated = BIG_AIRCRAFT * BIG_TICKS
    events = run(ChaosConfig(drop_prob=prob), BIG_AIRCRAFT, BIG_TICKS)
    measured = 1.0 - len(events) / generated
    assert measured == pytest.approx(prob, rel=0.10), f"measured {measured:.4f} vs configured {prob}"


def test_late_events_land_beyond_the_lateness_bound():
    late_delay = 90.0
    events = run(ChaosConfig(late_prob=0.1, late_delay_s=late_delay), BIG_AIRCRAFT, BIG_TICKS)
    lateness = [(e.ingest_time - e.event_time).total_seconds() for e in events]
    late = [x for x in lateness if x >= late_delay]
    measured = len(late) / len(events)
    assert measured == pytest.approx(0.1, rel=0.10), f"measured late rate {measured:.4f}"
    assert max(lateness) <= 2.0 * late_delay


def test_skew_is_bounded_by_max_skew():
    max_skew = 4.0
    events = run(ChaosConfig(out_of_order_prob=0.5, max_skew_s=max_skew), BIG_AIRCRAFT, BIG_TICKS)
    lateness = [(e.ingest_time - e.event_time).total_seconds() for e in events]
    assert max(lateness) <= max_skew
    assert max(lateness) > 0.0


def test_same_seed_is_reproducible():
    chaos = ChaosConfig(out_of_order_prob=0.2, duplicate_prob=0.1, drop_prob=0.1, late_prob=0.05)
    a = run(chaos, seed=99)
    b = run(chaos, seed=99)
    assert [e.model_dump() for e in a] == [e.model_dump() for e in b]


def test_events_spread_across_geographic_partitions():
    events = run(ChaosConfig(), BIG_AIRCRAFT, 5)
    assert len({e.partition_key for e in events}) > 10


def test_grid_cell_is_stable_within_a_cell():
    assert grid_cell(51.4, -0.1, size_deg=5.0) == grid_cell(53.9, -4.9, size_deg=5.0)
    assert grid_cell(51.4, -0.1) != grid_cell(56.0, -0.1)
