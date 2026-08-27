"""Lag-driven adaptive control — core claim #2.

The controller reacts to the *trend* in lag, not to a static threshold. That is
the whole design, and it is worth being precise about why.

A threshold controller asks "is lag above N?" That question is wrong twice over:

  * A large but *shrinking* lag needs no action -- the system is already
    recovering, and adding workers wastes resources on a problem that is solving
    itself. A threshold controller cannot tell recovery from collapse; both look
    like "lag is high".
  * A small but *rapidly growing* lag needs action now, well before it crosses
    any threshold. By the time lag is above N, the burst has been unabsorbed for
    however long it took to get there.

And it flaps. When lag hovers near N, every sample straddling the line produces
an opposite decision -- scale up, scale down, scale up -- and each of those is a
consumer-group rebalance that stops consumption entirely for a moment, which
raises lag, which triggers another decision. The failure feeds itself.

So this controller fits a least-squares line over a sliding window of samples and
asks "are we falling behind or catching up, and how fast?".

A slope alone is not enough, and the tests proved it: fed lag jittering +/-12%
around a flat 1000, an earlier version of this controller scaled up three times
and then shed, because random walk over a short window produces slopes far above
any fixed events-per-second threshold. Least squares damps a single outlier but
does not remove it -- on four points, one 10x spike still yields 91% of the naive
endpoint slope. So the trend must also be *statistically distinguishable from
noise*: the fitted slope is divided by its own standard error, and a decision
requires that ratio to clear `significance` as well as the absolute rate
threshold. Under pure noise the residuals are large, the standard error swamps
the slope, and nothing fires.

The default of 4.6 is not arbitrary and is not the "3 sigma" a normal
approximation would suggest. A 6-sample window leaves 4 degrees of freedom, and
the t-distribution has heavy tails there: measured against pure noise, t >= 3.0
fires on 4.62% of windows, while t >= 4.6 -- the 1% point for df=4 -- fires on
1.25%. **This threshold is tied to `window_samples`**; a 4-sample window (df=2)
needs t >= 8.6 for the same 1%, an 8-sample window only 4.0. Change one and the
other must move with it.

Three further guards keep it calm: a lag floor (below it, trend is meaningless
jitter), `confirm_samples` consecutive agreeing observations before any scaling
action, and a cooldown after every action.

Shedding is the admission that scaling has a ceiling. At max workers with lag
still climbing, something has to give, and the honest choice is to degrade
deliberately and visibly rather than let lag grow without bound.
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from math import sqrt
from typing import Literal

from src.control.lag_monitor import LagSample

log = logging.getLogger("contrail.control.controller")

Action = Literal["scale_up", "scale_down", "shed", "unshed", "hold"]


@dataclass
class ControllerConfig:
    window_samples: int = 6      # sliding window the trend is fitted over
    min_workers: int = 1
    max_workers: int = 4

    growth_threshold: float = 5.0   # lag events/s of growth that justifies scaling up
    drain_threshold: float = -5.0   # ... and of drain that justifies scaling back down

    # How many standard errors the fitted slope must clear before it counts as a
    # trend at all. This is what stops the controller chasing noise.
    # ~1% false-positive rate against pure noise at the default 6-sample window
    # (df=4). Tied to window_samples -- see the module docstring before changing
    # either: df=2 needs 8.6 for the same rate, df=6 needs only 4.0.
    significance: float = 4.6
    confirm_samples: int = 2     # consecutive agreeing observations before acting

    # Below this lag, trend is jitter: a handful of events either way at the tail
    # of a drained topic produces a slope with no meaning. Acting on it is how a
    # controller ends up oscillating while nothing is wrong.
    lag_floor: int = 200
    scale_down_lag: int = 100    # only give workers back once lag is genuinely small

    shed_after: int = 3          # consecutive at-max-and-still-growing decisions
    unshed_after: int = 3        # consecutive draining decisions before restoring
    cooldown_s: float = 10.0     # no two actions closer together than this


@dataclass(frozen=True)
class Decision:
    action: Action
    workers: int
    shedding: bool
    lag: int
    slope: float
    reason: str
    t_stat: float = 0.0

    @property
    def changed(self) -> bool:
        return self.action != "hold"


@dataclass
class ControllerState:
    workers: int
    shedding: bool = False
    slope: float = 0.0
    samples: int = 0
    growth_run: int = 0
    drain_run: int = 0
    at_max_growing: int = 0
    last_action_at: float | None = None
    history: list[Decision] = field(default_factory=list)


@dataclass(frozen=True)
class Trend:
    """A fitted trend and how much of it is signal rather than noise."""

    slope: float          # lag events per second
    t_stat: float = 0.0   # |slope| / standard error of the slope; inf on an exact fit


def slope(points: list[tuple[float, float]]) -> float:
    """Least-squares gradient of lag against time, in lag-events per second."""
    n = len(points)
    if n < 2:
        return 0.0
    mean_t = sum(t for t, _ in points) / n
    mean_v = sum(v for _, v in points) / n
    denom = sum((t - mean_t) ** 2 for t, _ in points)
    if denom == 0:
        return 0.0
    return sum((t - mean_t) * (v - mean_v) for t, v in points) / denom


def fit_trend(points: list[tuple[float, float]]) -> Trend:
    """Fit a line and report how confidently its gradient differs from zero.

    The t-statistic is the load-bearing part. A slope computed over noise is
    still a slope; what distinguishes a real ramp is that the points sit close
    to the fitted line, so the residual scatter -- and hence the standard error
    of the gradient -- is small relative to the gradient itself.
    """
    n = len(points)
    if n < 3:
        return Trend(slope(points), 0.0)  # too few points to separate signal from noise
    m = slope(points)
    mean_t = sum(t for t, _ in points) / n
    mean_v = sum(v for _, v in points) / n
    sxx = sum((t - mean_t) ** 2 for t, _ in points)
    if sxx == 0:
        return Trend(0.0, 0.0)
    intercept = mean_v - m * mean_t
    sse = sum((v - (m * t + intercept)) ** 2 for t, v in points)
    if sse <= 0:
        return Trend(m, float("inf"))  # a perfect fit is as significant as it gets
    stderr = sqrt(sse / (n - 2) / sxx)
    return Trend(m, abs(m) / stderr if stderr else float("inf"))


class LagController:
    """Pure decision logic. Feed it samples, it tells you what to do.

    Deliberately has no idea what a Kafka consumer is: everything it needs is in
    the sample, and everything it decides is returned. That is what makes the
    behaviour testable against a synthetic lag time series instead of only
    against live infrastructure.
    """

    def __init__(self, config: ControllerConfig | None = None) -> None:
        self.config = config or ControllerConfig()
        self._window: deque[LagSample] = deque(maxlen=self.config.window_samples)
        self.state = ControllerState(workers=self.config.min_workers)

    def observe(self, sample: LagSample) -> Decision:
        st = self.state
        self._window.append(sample)
        st.samples += 1
        trend = fit_trend([(s.at, float(s.total)) for s in self._window])
        st.slope = trend.slope

        decision = self._decide(sample, trend)
        st.history.append(decision)
        if decision.changed:
            st.last_action_at = sample.at
            st.workers = decision.workers
            st.shedding = decision.shedding
            log.info(
                "control action",
                extra={
                    "action": decision.action,
                    "lag": decision.lag,
                    "slope_per_s": round(decision.slope, 2),
                    "t_stat": round(decision.t_stat, 2),
                    "workers": decision.workers,
                    "shedding": decision.shedding,
                    "reason": decision.reason,
                },
            )
        return decision

    def _decide(self, sample: LagSample, trend: Trend) -> Decision:
        c, st = self.config, self.state
        lag = sample.total

        def hold(reason: str) -> Decision:
            return Decision("hold", st.workers, st.shedding, lag, trend.slope,
                            reason, trend.t_stat)

        def act(action: Action, workers: int, shedding: bool, reason: str) -> Decision:
            return Decision(action, workers, shedding, lag, trend.slope,
                            reason, trend.t_stat)

        if len(self._window) < 3:
            return hold("warming up: need three samples to separate trend from noise")

        real = trend.t_stat >= c.significance
        growing = real and trend.slope > c.growth_threshold and lag >= c.lag_floor
        quiet = lag < c.scale_down_lag
        # Give capacity back when lag is small and not climbing -- not only while
        # it is actively falling. A pool that waits for a negative slope never
        # shrinks once the system goes idle and the slope flattens to zero.
        draining = (real and trend.slope < c.drain_threshold) or (quiet and not growing)

        # Run counters advance every sample, cooldown or not: a cooldown should
        # delay the response, not erase the evidence that led to it.
        st.growth_run = st.growth_run + 1 if growing else 0
        st.drain_run = st.drain_run + 1 if draining else 0
        st.at_max_growing = (
            st.at_max_growing + 1 if growing and st.workers >= c.max_workers else 0
        )

        in_cooldown = (
            st.last_action_at is not None and sample.at - st.last_action_at < c.cooldown_s
        )

        # Shedding first: it answers the situation scaling cannot.
        if st.at_max_growing >= c.shed_after and not st.shedding:
            return act(
                "shed", st.workers, True,
                f"at max workers ({c.max_workers}) with lag growing "
                f"{trend.slope:.1f}/s (t={trend.t_stat:.1f}) for "
                f"{st.at_max_growing} consecutive samples",
            )
        if st.shedding and st.drain_run >= c.unshed_after:
            return act(
                "unshed", st.workers, False,
                f"lag draining ({trend.slope:.1f}/s, lag {lag}) for "
                f"{st.drain_run} consecutive samples",
            )

        if in_cooldown:
            return hold(
                f"cooldown, {c.cooldown_s - (sample.at - st.last_action_at):.1f}s left"
            )

        if st.growth_run >= c.confirm_samples and st.workers < c.max_workers:
            return act(
                "scale_up", st.workers + 1, st.shedding,
                f"lag {lag} growing {trend.slope:.1f}/s (t={trend.t_stat:.1f}) "
                f"for {st.growth_run} consecutive samples",
            )
        if (
            st.drain_run >= c.confirm_samples
            and st.workers > c.min_workers
            and not st.shedding
            and not growing
        ):
            return act(
                "scale_down", st.workers - 1, st.shedding,
                f"lag {lag} draining {trend.slope:.1f}/s for "
                f"{st.drain_run} consecutive samples",
            )

        if not real:
            return hold(
                f"lag {lag} slope {trend.slope:.1f}/s not significant "
                f"(t={trend.t_stat:.1f} < {c.significance})"
            )
        if lag < c.lag_floor:
            return hold(f"lag {lag} below floor {c.lag_floor}: trend is noise")
        return hold(f"lag {lag} stable at {trend.slope:.1f}/s")
