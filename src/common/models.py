"""Shared event schema.

The whole project hinges on `event_time` and `ingest_time` being different:
`event_time` is when the aircraft was actually in this state, `ingest_time` is
when the pipeline received the record. Every windowing claim in Phase 1 is a
claim about which of those two you key on.
"""

from datetime import datetime
from math import floor

from pydantic import BaseModel, Field

# Degrees per geographic partition cell. 5 degrees is coarse enough that a
# single aircraft stays in one cell for a long stretch (so per-cell aggregates
# are stable) and fine enough that traffic spreads over many Kafka partitions.
GRID_DEG = 5.0


class FlightState(BaseModel):
    icao24: str = Field(pattern=r"^[0-9a-f]{6}$")  # 24-bit ICAO address, lowercase hex
    callsign: str = Field(min_length=1, max_length=8, pattern=r"^[\x20-\x7e]+$")  # ICAO max is 8
    lat: float = Field(ge=-90.0, le=90.0, allow_inf_nan=False)
    lon: float = Field(ge=-180.0, le=180.0, allow_inf_nan=False)
    # -500 m is below the Dead Sea shore; barometric readings under it are sensor garbage.
    altitude_m: float = Field(ge=-500.0, allow_inf_nan=False)
    velocity_ms: float = Field(ge=0.0, allow_inf_nan=False)
    heading: float = Field(ge=0.0, lt=360.0, allow_inf_nan=False)
    event_time: datetime
    ingest_time: datetime

    @property
    def partition_key(self) -> str:
        """Geographic bucket, used as the Kafka message key."""
        return grid_cell(self.lat, self.lon)

    @property
    def dedup_key(self) -> tuple[str, datetime]:
        """Natural idempotency key: one aircraft has one state per instant."""
        return (self.icao24, self.event_time)


def grid_cell(lat: float, lon: float, size_deg: float = GRID_DEG) -> str:
    return f"{int(floor(lat / size_deg))}:{int(floor(lon / size_deg))}"
