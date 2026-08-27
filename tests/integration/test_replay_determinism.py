"""Core claim #3: replaying a recording produces byte-identical aggregates.

Against live Redpanda, because the property under test is precisely that the
broker's delivery order -- which varies run to run across six partitions -- does
not reach the output.
"""

import asyncio
import json
import os
import sys
import uuid
from datetime import UTC, datetime

from src.common.config import get_settings
from src.ingestor.base import ensure_topic, publish_events
from src.ingestor.synthetic import ChaosConfig, SyntheticSource
from src.replay import harness
from src.windowing.aggregates import WindowAggregate

SETTINGS = get_settings()
AIRCRAFT, TICKS = 30, 300  # ~9,000 events, enough for many windows and real disorder


_RECORDING: tuple[str, int] | None = None


async def recording() -> tuple[str, int]:
    """One recording, laid down once and replayed by every test in this module."""
    global _RECORDING
    if _RECORDING is None:
        topic = f"replay.test.{uuid.uuid4().hex[:8]}"
        published = await harness.record(
            SETTINGS.kafka_bootstrap, topic, AIRCRAFT, TICKS, seed=99
        )
        assert published > 0
        _RECORDING = (topic, published)
    return _RECORDING


async def test_three_replays_produce_identical_digests():
    topic, published = await recording()
    runs = [await harness.replay(SETTINGS.kafka_bootstrap, topic) for _ in range(3)]

    # The digest must be of something real, not of an empty aggregate set.
    assert runs[0].events == published
    assert runs[0].windows > 10
    assert runs[0].late > 0, "the recording should contain genuinely late events"

    digests = {r.digest for r in runs}
    assert len(digests) == 1, f"replays diverged: {[r.digest[:16] for r in runs]}"
    assert {r.windows for r in runs} == {runs[0].windows}
    assert {r.events for r in runs} == {runs[0].events}
    print(f"\n  3 replays of {published:,} messages -> {runs[0].windows:,} windows, "
          f"digest {runs[0].digest}")


async def test_replay_survives_being_killed_mid_stream():
    """Kill the replay process partway, restart it, expect the same digest.

    Because a replay is a pure function of the recorded topic, a crash costs
    progress and nothing else -- there is no partial state to repair.
    """
    topic, _ = await recording()
    clean = await harness.replay(SETTINGS.kafka_bootstrap, topic)

    env = {**os.environ, "PYTHONPATH": "/app"}
    cmd = [sys.executable, "-m", "src.replay.harness",
           "--topic", topic, "--runs", "1", "--json"]

    # Start it, let it get properly into the stream, then kill it hard.
    victim = await asyncio.create_subprocess_exec(*cmd, cwd="/app", env=env)
    await asyncio.sleep(3.0)
    assert victim.returncode is None, "replay finished before it could be killed"
    victim.kill()
    await victim.wait()
    assert victim.returncode != 0

    # Restart from scratch and compare.
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd="/app", env=env, stdout=asyncio.subprocess.PIPE
    )
    out, _ = await proc.communicate()
    assert proc.returncode == 0, out.decode()[-2000:]
    restarted = json.loads(out.decode().strip().splitlines()[-1])

    assert restarted["digest"] == clean.digest
    assert restarted["events"] == clean.events
    assert restarted["windows"] == clean.windows
    print(f"\n  killed mid-replay, restarted -> digest matches ({clean.digest[:32]}...)")


async def test_digest_actually_changes_when_the_aggregate_changes():
    """Guard against a digest that would match no matter what."""
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    base = {(t0, "1:1"): WindowAggregate(t0, "1:1", 10, 100.0, 200.0)}
    changed = {k: WindowAggregate(a.window_start, a.partition_key, a.count + 1,
                                  a.avg_altitude_m, a.avg_velocity_ms)
               for k, a in base.items()}
    nudged = {k: WindowAggregate(a.window_start, a.partition_key, a.count,
                                 a.avg_altitude_m + 0.001, a.avg_velocity_ms)
              for k, a in base.items()}
    assert harness.digest(base) != harness.digest(changed)
    assert harness.digest(base) != harness.digest(nudged), "0.001m must move the digest"
    assert harness.digest(base) == harness.digest(dict(base))


async def test_replay_is_insensitive_to_partition_interleaving():
    """A recording spread over 1 partition and 6 must hash the same.

    This is the failure the total-order sort in `collect()` exists to prevent:
    six partitions arrive in a different interleaving every run.
    """
    # Generate ONCE and publish the same list to both topics. Constructing two
    # sources would give them different `start_time` defaults (now()), so the two
    # topics would hold genuinely different events and the digests would rightly
    # differ -- which is a bug in the test, not in the pipeline.
    source = SyntheticSource(
        n_aircraft=20, rate_hz=1.0, seed=555, duration_s=200,
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        chaos=ChaosConfig(out_of_order_prob=0.25, max_skew_s=15.0, duplicate_prob=0.05),
    )
    events = list(source.simulate())

    digests = []
    for partitions in (1, 6):
        topic = f"replay.parts{partitions}.{uuid.uuid4().hex[:8]}"
        await ensure_topic(SETTINGS.kafka_bootstrap, topic, partitions)
        await publish_events(events, SETTINGS.kafka_bootstrap, topic, partitions)
        result = await harness.replay(SETTINGS.kafka_bootstrap, topic)
        assert result.events == len(events)
        digests.append(result.digest)

    assert digests[0] == digests[1], "partition count leaked into the aggregate"
