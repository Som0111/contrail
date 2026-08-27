"""OpenSky adapter: field mapping and failure handling, without touching the network.

A live feed's failure modes cannot be summoned on demand, so they are injected
here through a stub transport. The point is that a 429, a timeout and a 500 are
*tested* behaviours rather than hopes.
"""

import httpx
import pytest

from src.common.models import FlightState
from src.ingestor.base import EventSource
from src.ingestor.opensky import (
    BoundingBox,
    OpenSkySource,
    OpenSkyStats,
    bbox_from,
    parse_state,
)
from src.ingestor.synthetic import SyntheticSource

# A real row, captured from the live API.
REAL = ["3d10d0", "DEEPS   ", "Germany", 1787830924, 1787830924,
        9.1108, 49.9248, 601.98, False, 51.95, 187.97, 4.55, None, 624.84]


def row(**over):
    r = list(REAL)
    for idx, value in over.items():
        r[int(idx)] = value
    return r


def test_parses_a_real_state_vector():
    stats = OpenSkyStats()
    s = parse_state(list(REAL), stats)
    assert isinstance(s, FlightState)
    assert s.icao24 == "3d10d0"
    assert s.callsign == "DEEPS", "trailing padding must be stripped"
    assert s.lat == pytest.approx(49.9248)
    assert s.altitude_m == pytest.approx(601.98)
    assert s.heading == pytest.approx(187.97)
    # The whole point of this source: event_time is the observation instant,
    # ingest_time is now, and they are genuinely different.
    assert s.event_time.timestamp() == 1787830924
    assert s.ingest_time > s.event_time
    assert stats.repaired_fields == 0


@pytest.mark.parametrize("missing", [5, 6, 3])  # lon, lat, time_position
def test_rows_without_a_position_or_time_are_skipped(missing):
    stats = OpenSkyStats()
    assert parse_state(row(**{str(missing): None}), stats) is None
    assert stats.skipped_missing_position == 1


def test_substituting_geometric_altitude_is_not_the_same_as_inventing_one():
    """Swapping in another real sensor reading is not a guess, and is counted apart."""
    stats = OpenSkyStats()
    s = parse_state(row(**{"7": None}), stats)
    assert s.altitude_m == pytest.approx(624.84), "geo_altitude is the fallback"
    assert stats.substituted_altitude == 1
    assert stats.repaired_fields == 0, "a real reading from another sensor is not a repair"

    # With neither reading available, 0.0 is genuinely invented -- that is a repair.
    stats2 = OpenSkyStats()
    s2 = parse_state(row(**{"7": None, "13": None}), stats2)
    assert s2.altitude_m == 0.0
    assert stats2.substituted_altitude == 0
    assert stats2.repaired_fields == 1


def test_an_aircraft_on_the_ground_with_no_altitude_is_not_counted_as_repaired():
    """Sitting on a runway with no altitude reading is normal, not damage."""
    stats = OpenSkyStats()
    s = parse_state(row(**{"7": None, "13": None, "8": True}), stats)
    assert s.altitude_m == 0.0
    assert stats.repaired_fields == 0


def test_null_velocity_and_heading_are_repaired_and_counted():
    stats = OpenSkyStats()
    s = parse_state(row(**{"9": None, "10": None}), stats)
    assert s.velocity_ms == 0.0 and s.heading == 0.0
    assert stats.repaired_fields == 1, "one row repaired, not one per field"


def test_blank_callsign_falls_back_to_icao24():
    stats = OpenSkyStats()
    assert parse_state(row(**{"1": "        "}), stats).callsign == "3d10d0"
    assert parse_state(row(**{"1": None}), stats).callsign == "3d10d0"


def test_out_of_range_values_are_normalised_not_rejected():
    """The schema bounds heading to [0,360); a live feed will send 360.0."""
    stats = OpenSkyStats()
    assert parse_state(row(**{"10": 360.0}), stats).heading == 0.0
    assert parse_state(row(**{"5": 185.0}), stats).lon == pytest.approx(-175.0)
    assert parse_state(row(**{"9": -3.0}), stats).velocity_ms == 0.0


def stub(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def drain(source, limit=50):
    out = []
    async for state in source.stream():
        out.append(state)
        if len(out) >= limit:
            break
    return out


async def test_polls_and_emits_states():
    def handler(request):
        assert request.url.params["lamin"] == "45"
        return httpx.Response(200, json={"time": 1, "states": [REAL, row(**{"0": "abc123"})]})

    src = OpenSkySource(poll_interval_s=0.01, bbox=BoundingBox(45, 5, 55, 15),
                        duration_s=1.0, client=stub(handler))
    got = await drain(src, limit=2)
    assert [s.icao24 for s in got] == ["3d10d0", "abc123"]
    assert src.stats.emitted == 2


async def test_rate_limiting_is_retried_not_fatal():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"time": 1, "states": [REAL]})

    src = OpenSkySource(poll_interval_s=0.01, max_backoff_s=0.05,
                        duration_s=5.0, client=stub(handler))
    got = await drain(src, limit=1)
    assert len(got) == 1, "the source must recover from a 429, not die on it"
    assert src.stats.rate_limited == 1
    assert src.stats.retries == 1


@pytest.mark.parametrize("failure", [
    lambda r: httpx.Response(500),
    lambda r: httpx.Response(200, content=b"not json"),
])
async def test_server_errors_and_garbage_are_retried(failure):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return failure(request) if calls["n"] == 1 else httpx.Response(
            200, json={"time": 1, "states": [REAL]})

    src = OpenSkySource(poll_interval_s=0.01, max_backoff_s=0.05,
                        duration_s=5.0, client=stub(handler))
    assert len(await drain(src, limit=1)) == 1
    assert src.stats.errors == 1


async def test_a_timeout_does_not_escape_into_the_pipeline():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectTimeout("timed out")
        return httpx.Response(200, json={"time": 1, "states": [REAL]})

    src = OpenSkySource(poll_interval_s=0.01, max_backoff_s=0.05,
                        duration_s=5.0, client=stub(handler))
    assert len(await drain(src, limit=1)) == 1
    assert src.stats.errors == 1


async def test_backoff_resets_after_recovery():
    """A single blip must not leave the poller permanently slowed down."""
    seq = [httpx.Response(500), httpx.Response(500),
           httpx.Response(200, json={"time": 1, "states": [REAL]}),
           httpx.Response(200, json={"time": 2, "states": [REAL]})]

    def handler(request):
        return seq.pop(0) if seq else httpx.Response(200, json={"time": 3, "states": [REAL]})

    src = OpenSkySource(poll_interval_s=0.01, max_backoff_s=0.05,
                        duration_s=5.0, client=stub(handler))
    await drain(src, limit=2)
    assert src.stats.retries == 2
    assert src.stats.polls >= 2


async def test_an_empty_response_is_not_an_error():
    """Night over an empty bounding box returns no states. That is fine."""
    src = OpenSkySource(poll_interval_s=0.01, duration_s=0.2,
                        client=stub(lambda r: httpx.Response(200, json={"time": 1})))
    assert await drain(src) == []
    assert src.stats.errors == 0
    assert src.stats.polls >= 1


def test_both_sources_satisfy_the_same_protocol():
    """The 0.2 promise: OpenSky is a second implementation, not a fork."""
    assert isinstance(OpenSkySource(), EventSource)
    assert isinstance(SyntheticSource(), EventSource)
    assert OpenSkySource().name == "opensky"
    assert SyntheticSource().name == "synthetic"


def test_bbox_parsing():
    b = bbox_from("45,5,55,15")
    assert b.params() == {"lamin": 45.0, "lomin": 5.0, "lamax": 55.0, "lomax": 15.0}
    assert bbox_from("") is None and bbox_from(None) is None
