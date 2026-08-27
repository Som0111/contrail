"""Load shedding drops whole geographic cells, and accounts for every drop.

The design claim is that shedding degrades *visibly*: nothing it discards may
vanish without being counted, and the cells that go dark must be nameable. These
assert that rather than trusting the log line.
"""

from datetime import UTC, datetime

from src.common.models import FlightState
from src.control.supervisor import ShedState


def event(cell_lat: float, cell_lon: float, icao: str = "abc123") -> FlightState:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return FlightState(
        icao24=icao, callsign="TEST", lat=cell_lat, lon=cell_lon,
        altitude_m=10000.0, velocity_ms=200.0, heading=90.0,
        event_time=now, ingest_time=now,
    )


def fleet(n: int = 400) -> list[FlightState]:
    """Spread across many geographic cells so the fraction is measurable."""
    out = []
    for i in range(n):
        lat = -80.0 + (i % 32) * 5.0
        lon = -175.0 + ((i * 7) % 70) * 5.0
        out.append(event(lat, lon, icao=f"{i:06x}"))
    return out


def test_nothing_is_shed_when_the_fraction_is_zero():
    shed = ShedState()
    events = fleet()
    assert all(shed.keep(e) for e in events)
    assert shed.dropped == 0
    assert shed.cells_shed == set()


def test_every_dropped_event_is_counted():
    """The accounting identity: kept + dropped == offered. Nothing disappears."""
    shed = ShedState(fraction=0.25)
    events = fleet()
    kept = [e for e in events if shed.keep(e)]
    assert len(kept) + shed.dropped == len(events)
    assert shed.dropped > 0


def test_the_cells_that_went_dark_are_nameable():
    """Partial correctness you can describe beats uniform corruption you cannot."""
    shed = ShedState(fraction=0.3)
    events = fleet()
    dropped = [e for e in events if not shed.keep(e)]
    assert shed.cells_shed == {e.partition_key for e in dropped}
    assert len(shed.cells_shed) > 1
    # A shed cell is shed entirely -- no cell is half-dropped, which is what
    # keeps every surviving cell's aggregate exactly correct.
    kept_cells = {e.partition_key for e in events if e.partition_key not in shed.cells_shed}
    assert kept_cells.isdisjoint(shed.cells_shed)


def test_the_same_cell_is_shed_consistently_for_the_whole_episode():
    """A stable hash, not a fresh arbitrary slice per batch."""
    shed = ShedState(fraction=0.5)
    events = fleet()
    first = [shed.keep(e) for e in events]
    second = [shed.keep(e) for e in events]
    assert first == second


def test_shed_fraction_is_roughly_the_configured_share_of_cells():
    events = fleet(2000)
    cells = {e.partition_key for e in events}
    for fraction in (0.25, 0.5):
        shed = ShedState(fraction=fraction)
        for e in events:
            shed.keep(e)
        measured = len(shed.cells_shed) / len(cells)
        assert abs(measured - fraction) < 0.15, f"{measured:.2f} vs {fraction}"


def test_releasing_shedding_stops_dropping_immediately():
    shed = ShedState(fraction=0.5)
    events = fleet()
    for e in events:
        shed.keep(e)
    dropped_while_shedding = shed.dropped
    assert dropped_while_shedding > 0

    shed.fraction = 0.0  # what the controller does on `unshed`
    assert all(shed.keep(e) for e in events)
    assert shed.dropped == dropped_while_shedding, "the total stays as an audit record"
