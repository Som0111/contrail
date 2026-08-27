"""Controller behaviour against synthetic lag time series.

Deliberately infrastructure-free. A controller tested only against live Kafka is
a controller whose failure modes you discover during a burst -- here the exact
lag curve is chosen by the test, so "does it flap on noise" is a question with a
definite answer rather than an impression.
"""

import random

import pytest

from src.control.controller import ControllerConfig, LagController, fit_trend, slope
from src.control.lag_monitor import LagSample

INTERVAL = 2.0  # seconds between samples, as the supervisor uses


def feed(controller: LagController, lags, start=0.0, interval=INTERVAL):
    """Push a lag series through the controller, returning every decision."""
    out = []
    for i, lag in enumerate(lags):
        out.append(controller.observe(LagSample(at=start + i * interval, total=int(lag))))
    return out


def actions(decisions):
    return [d.action for d in decisions if d.changed]


def cfg(**kw):
    base = dict(window_samples=4, min_workers=1, max_workers=4, cooldown_s=4.0,
                growth_threshold=5.0, drain_threshold=-5.0, lag_floor=200,
                scale_down_lag=100, shed_after=3, unshed_after=3)
    base.update(kw)
    return ControllerConfig(**base)


# --- the trend primitive ------------------------------------------------------

def test_slope_measures_growth_per_second():
    assert slope([(0.0, 0.0), (1.0, 10.0), (2.0, 20.0)]) == pytest.approx(10.0)
    assert slope([(0.0, 100.0), (1.0, 90.0), (2.0, 80.0)]) == pytest.approx(-10.0)
    assert slope([(0.0, 50.0), (1.0, 50.0)]) == pytest.approx(0.0)
    assert slope([(0.0, 5.0)]) == 0.0, "a single point has no trend"


def test_least_squares_damps_an_outlier_but_does_not_defeat_it():
    """Least squares alone is not enough -- which is why significance exists."""
    clean = [(0.0, 0.0), (1.0, 10.0), (2.0, 20.0), (3.0, 30.0)]
    spiked = clean[:-1] + [(3.0, 300.0)]
    naive_endpoint = (spiked[-1][1] - spiked[0][1]) / 3.0
    assert naive_endpoint == pytest.approx(100.0)
    assert slope(spiked) < naive_endpoint, "least squares must damp the outlier"
    # ... but only to 91% of it. The real defence is that the fit is poor:
    assert slope(spiked) > 0.9 * naive_endpoint
    assert fit_trend(spiked).t_stat < 3.0, "a spike must not read as a significant trend"


def test_a_clean_ramp_is_highly_significant():
    ramp = [(float(i), 100.0 * i) for i in range(6)]
    trend = fit_trend(ramp)
    assert trend.slope == pytest.approx(100.0)
    assert trend.t_stat > 3.0


def noise_significant_rate(n, threshold, trials=4000, seed=3):
    rng = random.Random(seed)
    fired = sum(
        fit_trend([(float(i), 1000 + rng.uniform(-120, 120)) for i in range(n)]).t_stat
        >= threshold
        for _ in range(trials)
    )
    return fired / trials


def test_default_significance_keeps_noise_below_two_percent():
    """The default threshold must actually be the ~1% point, not a guess."""
    assert noise_significant_rate(6, ControllerConfig().significance) < 0.02


def test_significance_threshold_is_tied_to_window_size():
    """Why the config comment warns against changing one without the other.

    At t >= 3 a 6-sample window mistakes noise for a trend 4-5% of the time --
    which is precisely how the first version of this controller ended up scaling
    up three times and shedding on a flat, noisy lag series.
    """
    assert noise_significant_rate(6, 3.0) > 0.03
    # Same threshold, fewer degrees of freedom, far worse.
    assert noise_significant_rate(4, 3.0) > noise_significant_rate(8, 3.0)
    # More samples buy significance more cheaply.
    assert noise_significant_rate(8, 4.0) < noise_significant_rate(4, 4.0)


# --- scaling ------------------------------------------------------------------

def test_scales_up_when_lag_is_growing():
    c = LagController(cfg())
    decisions = feed(c, [300, 400, 500, 600, 700, 800])
    assert "scale_up" in actions(decisions)
    assert c.state.workers > 1


def test_does_not_scale_up_on_high_but_shrinking_lag():
    """The case a threshold controller gets wrong: recovery looks like overload."""
    c = LagController(cfg())
    decisions = feed(c, [5000, 4200, 3400, 2600, 1800, 1000])
    assert actions(decisions) == [] or "scale_up" not in actions(decisions)
    assert c.state.workers == 1, "lag is huge but draining -- adding workers is waste"


def test_scales_up_on_small_but_fast_growing_lag():
    """And the other case: action is needed before any threshold is crossed."""
    c = LagController(cfg(lag_floor=200))
    decisions = feed(c, [200, 260, 330, 400, 470])
    assert "scale_up" in actions(decisions)


def test_scales_back_down_once_lag_drains():
    c = LagController(cfg())
    feed(c, [300, 500, 700, 900])
    peak_workers = c.state.workers
    assert peak_workers > 1
    # Drain to near zero, well past the cooldown.
    feed(c, [400, 200, 90, 40, 20, 10, 5, 2], start=100.0)
    assert c.state.workers < peak_workers


def test_never_exceeds_max_or_drops_below_min():
    c = LagController(cfg(min_workers=2, max_workers=3))
    feed(c, [500 + 200 * i for i in range(40)])
    assert c.state.workers == 3
    feed(c, [10] * 40, start=1000.0)
    assert c.state.workers == 2


# --- the anti-flap requirement -------------------------------------------------

def test_does_not_flap_on_noise_around_a_threshold():
    """Lag jittering around a level must produce no actions at all.

    This is the scenario that makes a static-threshold controller oscillate:
    every sample straddling the line flips the decision.
    """
    rng = random.Random(7)
    c = LagController(cfg())
    noisy = [1000 + rng.uniform(-120, 120) for _ in range(60)]
    decisions = feed(c, noisy)
    assert actions(decisions) == [], f"flapped: {actions(decisions)}"
    assert c.state.workers == 1


def test_does_not_act_on_jitter_below_the_lag_floor():
    rng = random.Random(11)
    c = LagController(cfg(lag_floor=200))
    decisions = feed(c, [rng.uniform(0, 150) for _ in range(40)])
    assert "scale_up" not in actions(decisions)


def test_cooldown_prevents_back_to_back_scaling():
    c = LagController(cfg(cooldown_s=20.0))
    decisions = feed(c, [300 + 300 * i for i in range(8)], interval=2.0)
    ups = [d for d in decisions if d.action == "scale_up"]
    assert len(ups) == 1, "20s cooldown over 16s of samples allows exactly one action"


def test_a_single_transient_spike_does_not_trigger_scaling():
    c = LagController(cfg())
    decisions = feed(c, [50, 50, 50, 4000, 50, 50, 50, 50])
    assert "scale_up" not in actions(decisions)


# --- shedding -----------------------------------------------------------------

def test_sheds_only_at_max_workers_with_sustained_growth():
    c = LagController(cfg(max_workers=2, shed_after=3, cooldown_s=0.0))
    decisions = feed(c, [500 + 400 * i for i in range(20)])
    acts = actions(decisions)
    assert "shed" in acts
    assert acts.index("scale_up") < acts.index("shed"), "scaling must be tried first"
    assert c.state.shedding is True


def test_does_not_shed_while_scaling_headroom_remains():
    c = LagController(cfg(max_workers=8, shed_after=3))
    decisions = feed(c, [500 + 400 * i for i in range(10)])
    assert "shed" not in actions(decisions)


def test_does_not_shed_on_a_brief_burst_at_max():
    """Growth at max workers for fewer than `shed_after` samples must not shed."""
    c = LagController(cfg(max_workers=1, shed_after=4, cooldown_s=0.0))
    decisions = feed(c, [500, 900, 1300, 400, 200])
    assert "shed" not in actions(decisions)


def test_releases_shedding_once_lag_drains():
    c = LagController(cfg(max_workers=2, shed_after=3, unshed_after=3, cooldown_s=0.0))
    feed(c, [500 + 400 * i for i in range(20)])
    assert c.state.shedding is True
    feed(c, [2000, 1200, 600, 200, 80, 30, 10], start=500.0)
    assert c.state.shedding is False
    assert "unshed" in actions(c.state.history)


def test_every_action_records_the_lag_that_triggered_it():
    c = LagController(cfg(max_workers=2, shed_after=3, cooldown_s=0.0))
    feed(c, [500 + 400 * i for i in range(20)])
    for d in c.state.history:
        if d.changed:
            assert d.reason, "an action with no stated reason is unauditable"
            assert d.lag >= 0
            assert isinstance(d.slope, float)


def test_full_burst_and_recovery_cycle_is_ordered_sanely():
    """Ramp to overload, hold, then drain -- the shape of the 1.5 benchmark."""
    c = LagController(cfg(max_workers=3, shed_after=3, unshed_after=3, cooldown_s=4.0))
    burst = [200 + 500 * i for i in range(14)]
    plateau = [burst[-1]] * 4
    drain = [max(5, burst[-1] - 900 * i) for i in range(12)]
    feed(c, burst + plateau + drain)

    acts = actions(c.state.history)
    assert acts.count("scale_up") >= 2
    assert "shed" in acts and "unshed" in acts
    assert acts.index("shed") > acts.index("scale_up")
    assert acts.index("unshed") > acts.index("shed")
    assert c.state.shedding is False
    assert c.state.workers <= 3
