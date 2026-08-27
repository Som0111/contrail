"""Live ADS-B state vectors from the OpenSky Network.

Satisfies the same `EventSource` protocol as the synthetic generator, so
`publish()` and everything downstream cannot tell which one is running. The
switch is `SOURCE=opensky` (see `ingestor/run.py`); no code changes.

Why OpenSky suits this pipeline unusually well: each state vector carries
`time_position`, the instant the aircraft was actually observed in that state,
separately from the moment we fetch it. That is a genuine event_time / ingest_time
split from a real network, with real skew, rather than one the generator injected.
The synthetic source remains primary for benchmarking precisely because its skew
is *controllable*; this one is the reality check.

Real-world messiness handled explicitly, because a live feed will hand you all of
it within minutes:

  * null `latitude`/`longitude`/`time_position` -- unusable, skipped and counted
  * null `velocity`, `true_track`, `baro_altitude` -- repaired with a fallback
    (geo_altitude, then 0.0) and counted separately from skips, so "we guessed"
    never silently reads as "we measured"
  * blank callsigns, which are common -- fall back to the icao24
  * HTTP 429 with or without `Retry-After`, timeouts and 5xx -- retried with
    exponential backoff and jitter; the poll loop never raises into the pipeline
  * the same aircraft returned across consecutive polls with an unchanged
    `time_position` -- a genuine duplicate, left for the sink's idempotency key
    to absorb rather than filtered here
"""

import argparse
import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import AsyncIterator

import httpx

from src.common.config import get_settings
from src.common.logging import configure
from src.common.models import FlightState

log = logging.getLogger("contrail.ingestor.opensky")

STATES_URL = "https://opensky-network.org/api/states/all"

# Field offsets in an OpenSky state vector (a bare array, not an object).
ICAO24, CALLSIGN, _COUNTRY, TIME_POSITION = 0, 1, 2, 3
_LAST_CONTACT, LON, LAT, BARO_ALT = 4, 5, 6, 7
ON_GROUND, VELOCITY, TRUE_TRACK, _VERT_RATE = 8, 9, 10, 11
_SENSORS, GEO_ALT = 12, 13


@dataclass
class OpenSkyStats:
    polls: int = 0
    received: int = 0
    emitted: int = 0
    skipped_missing_position: int = 0
    # Two different things, deliberately not one counter. A substitution swaps in
    # another real measurement (geometric altitude for barometric); a repair
    # invents a value because none was reported. Merging them would let "we
    # guessed" hide inside "we measured", which is the exact failure this
    # accounting exists to prevent.
    substituted_altitude: int = 0
    repaired_fields: int = 0
    errors: int = 0
    retries: int = 0
    rate_limited: int = 0


@dataclass
class BoundingBox:
    lamin: float
    lomin: float
    lamax: float
    lomax: float

    def params(self) -> dict[str, float]:
        return {"lamin": self.lamin, "lomin": self.lomin,
                "lamax": self.lamax, "lomax": self.lomax}


def parse_state(row: list, stats: OpenSkyStats) -> FlightState | None:
    """Turn one OpenSky state vector into a FlightState, or None if unusable."""
    lat, lon, when = row[LAT], row[LON], row[TIME_POSITION]
    if lat is None or lon is None or when is None:
        stats.skipped_missing_position += 1
        return None

    repaired = False
    altitude = row[BARO_ALT]
    if altitude is None:
        geo = row[GEO_ALT] if len(row) > GEO_ALT else None
        if geo is not None:
            altitude = geo
            stats.substituted_altitude += 1  # real reading, different sensor
    if altitude is None:
        # An aircraft on the ground legitimately reports no altitude.
        altitude = 0.0
        repaired = not row[ON_GROUND]
    velocity = row[VELOCITY]
    if velocity is None:
        velocity, repaired = 0.0, True
    heading = row[TRUE_TRACK]
    if heading is None:
        heading, repaired = 0.0, True
    if repaired:
        stats.repaired_fields += 1

    callsign = (row[CALLSIGN] or "").strip() or row[ICAO24]
    event_time = datetime.fromtimestamp(when, UTC)
    return FlightState(
        icao24=row[ICAO24],
        callsign=callsign,
        lat=max(-90.0, min(90.0, float(lat))),
        lon=(float(lon) + 180.0) % 360.0 - 180.0,
        altitude_m=float(altitude),
        velocity_ms=max(0.0, float(velocity)),
        heading=float(heading) % 360.0,
        event_time=event_time,
        ingest_time=datetime.now(UTC),
    )


class OpenSkySource:
    name = "opensky"

    def __init__(
        self,
        poll_interval_s: float = 15.0,
        bbox: BoundingBox | None = None,
        url: str = STATES_URL,
        timeout_s: float = 20.0,
        max_backoff_s: float = 300.0,
        duration_s: float | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.poll_interval_s = poll_interval_s
        self.bbox = bbox
        self.url = url
        self.timeout_s = timeout_s
        self.max_backoff_s = max_backoff_s
        self.duration_s = duration_s
        self._client = client
        self.stats = OpenSkyStats()

    async def _fetch(self, client: httpx.AsyncClient) -> list[list] | None:
        """One poll. Returns rows, or None if this attempt should be retried."""
        try:
            response = await client.get(
                self.url, params=self.bbox.params() if self.bbox else None,
                timeout=self.timeout_s,
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            self.stats.errors += 1
            log.warning("opensky request failed", extra={"error": f"{type(exc).__name__}: {exc}"})
            return None

        if response.status_code == 429:
            self.stats.rate_limited += 1
            retry_after = response.headers.get("Retry-After")
            log.warning("opensky rate limited", extra={"retry_after": retry_after})
            if retry_after and retry_after.isdigit():
                # Honour the server's own number rather than guessing over it.
                await asyncio.sleep(min(float(retry_after), self.max_backoff_s))
            return None
        if response.status_code >= 500:
            self.stats.errors += 1
            log.warning("opensky server error", extra={"status": response.status_code})
            return None
        if response.status_code != 200:
            self.stats.errors += 1
            log.error("opensky unexpected status", extra={"status": response.status_code})
            return None

        try:
            payload = response.json()
        except ValueError as exc:
            self.stats.errors += 1
            log.warning("opensky returned non-JSON", extra={"error": str(exc)})
            return None
        return payload.get("states") or []

    async def stream(self) -> AsyncIterator[FlightState]:
        client = self._client or httpx.AsyncClient(headers={"User-Agent": "contrail/0.2"})
        owns_client = self._client is None
        loop = asyncio.get_running_loop()
        deadline = None if self.duration_s is None else loop.time() + self.duration_s
        backoff = self.poll_interval_s

        try:
            while deadline is None or loop.time() < deadline:
                started = loop.time()
                rows = await self._fetch(client)

                if rows is None:
                    # Exponential backoff with jitter. Jitter matters: without it a
                    # fleet of pollers recovering from one outage re-synchronises
                    # and hammers the API in lockstep.
                    backoff = min(backoff * 2, self.max_backoff_s)
                    self.stats.retries += 1
                    delay = backoff * (0.5 + random.random() * 0.5)
                    log.info("backing off", extra={"seconds": round(delay, 1)})
                    await asyncio.sleep(delay)
                    continue

                backoff = self.poll_interval_s  # recovered
                self.stats.polls += 1
                self.stats.received += len(rows)
                for row in rows:
                    state = parse_state(row, self.stats)
                    if state is not None:
                        self.stats.emitted += 1
                        yield state

                log.info(
                    "opensky poll",
                    extra={"received": len(rows), "emitted": self.stats.emitted,
                           "skipped": self.stats.skipped_missing_position,
                           "substituted_alt": self.stats.substituted_altitude,
                           "repaired": self.stats.repaired_fields},
                )
                elapsed = loop.time() - started
                await asyncio.sleep(max(0.0, self.poll_interval_s - elapsed))
        finally:
            if owns_client:
                await client.aclose()
            log.info("opensky source stopped", extra=vars(self.stats))


def _parse_args(argv=None) -> argparse.Namespace:
    s = get_settings()
    p = argparse.ArgumentParser(description="Publish live OpenSky state vectors.")
    p.add_argument("--bootstrap", default=s.kafka_bootstrap)
    p.add_argument("--topic", default=s.kafka_raw_topic)
    p.add_argument("--partitions", type=int, default=s.kafka_partitions)
    p.add_argument("--poll-interval", type=float, default=s.opensky_poll_interval_s)
    p.add_argument("--duration", type=float, default=None)
    p.add_argument("--bbox", default=s.opensky_bbox,
                   help="lamin,lomin,lamax,lomax; empty for the whole world")
    return p.parse_args(argv)


def bbox_from(spec: str | None) -> BoundingBox | None:
    if not spec:
        return None
    lamin, lomin, lamax, lomax = (float(x) for x in spec.split(","))
    return BoundingBox(lamin, lomin, lamax, lomax)


async def _main(argv=None) -> None:
    from src.ingestor.base import publish

    args = _parse_args(argv)
    configure(get_settings().log_level)
    source = OpenSkySource(
        poll_interval_s=args.poll_interval,
        bbox=bbox_from(args.bbox),
        duration_s=args.duration,
    )
    await publish(source, args.bootstrap, args.topic, args.partitions)


if __name__ == "__main__":
    asyncio.run(_main())
