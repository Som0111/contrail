"""Event and configuration contracts: bad values are rejected, edge values still work."""

import math
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.common.config import Settings, parse_bbox
from src.ingestor.opensky import OpenSkyStats, bbox_from, parse_state
from tests.unit.test_opensky import REAL, row

T = datetime(2026, 1, 1, tzinfo=UTC)
GOOD = {"icao24": "3d10d0", "callsign": "DEEPS", "lat": 50.0, "lon": 9.0, "altitude_m": 100.0,
        "velocity_ms": 100.0, "heading": 90.0, "event_time": T, "ingest_time": T}


def make(**over):
    from src.common.models import FlightState
    return FlightState(**{**GOOD, **over})


@pytest.mark.parametrize("field,value", [
    ("altitude_m", -500.1), ("altitude_m", math.nan), ("altitude_m", math.inf),
    ("velocity_ms", -0.1), ("velocity_ms", math.nan), ("velocity_ms", -math.inf),
    ("lat", math.nan), ("lon", math.inf), ("heading", math.nan), ("heading", 360.0),
    ("icao24", "3D10D0"), ("icao24", "3d10d"), ("icao24", "3d10d0f"), ("icao24", "zzzzzz"),
    ("callsign", ""), ("callsign", "TOOLONG99"), ("callsign", "bad\nid"),
])
def test_invalid_event_values_are_rejected(field, value):
    with pytest.raises(ValidationError):
        make(**{field: value})


@pytest.mark.parametrize("field,value", [
    ("altitude_m", -500.0), ("altitude_m", 0.0), ("velocity_ms", 0.0),  # on the ground
    ("heading", 0.0), ("heading", 359.999), ("callsign", "A"), ("callsign", "ABCDEFGH"),
])
def test_valid_edge_values_are_accepted(field, value):
    make(**{field: value})


def test_malformed_rows_are_counted_apart_from_missing_position():
    stats = OpenSkyStats()
    bad = [
        row(**{"7": -9000.0}),  # barometric garbage -> contract violation
        row(**{"0": "NOTHEX"}),
        row(**{"6": "north"}),  # wrong type
        REAL[:5],  # truncated
        {"not": "a row"},
        row(**{"3": 10**30}),  # timestamp out of range
    ]
    assert [parse_state(r, stats) for r in bad] == [None] * len(bad)
    assert stats.skipped_malformed == len(bad)
    assert stats.skipped_missing_position == 0

    parse_state(row(**{"6": None}), stats)
    assert stats.skipped_missing_position == 1 and stats.skipped_malformed == len(bad)


@pytest.mark.parametrize("env,value", [
    ("WINDOW_S", "0"), ("WINDOW_S", "-5"), ("ALLOWED_LATENESS_S", "-1"),
    ("KAFKA_PARTITIONS", "0"), ("RATE_LIMIT_RPS", "0"), ("RATE_LIMIT_BURST", "-1"),
    ("GEN_RATE_HZ", "0"), ("CHAOS_DROP_PROB", "1.5"),
    ("OPENSKY_BBOX", "45,5,55"), ("OPENSKY_BBOX", "a,b,c,d"), ("OPENSKY_BBOX", "55,5,45,15"),
    ("OPENSKY_BBOX", "45,5,95,15"), ("OPENSKY_BBOX", "45,15,55,5"),
])
def test_invalid_settings_are_rejected_naming_the_variable(monkeypatch, env, value):
    monkeypatch.setenv(env, value)
    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None)
    assert env.lower() in str(exc.value).lower()


def test_idle_timeout_must_exceed_lateness(monkeypatch):
    monkeypatch.setenv("ALLOWED_LATENESS_S", "30")
    monkeypatch.setenv("IDLE_TIMEOUT_S", "30")
    with pytest.raises(ValidationError, match="IDLE_TIMEOUT_S"):
        Settings(_env_file=None)
    monkeypatch.setenv("IDLE_TIMEOUT_S", "31")
    assert Settings(_env_file=None).idle_timeout_s == 31


def test_defaults_and_empty_bbox_are_valid(monkeypatch):
    monkeypatch.setenv("OPENSKY_BBOX", "")
    s = Settings(_env_file=None)
    assert s.window_s == 60 and bbox_from(s.opensky_bbox) is None
    assert parse_bbox("45,5,55,15") == (45, 5, 55, 15)
