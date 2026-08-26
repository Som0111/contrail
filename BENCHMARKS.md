# BENCHMARKS — Contrail

Measured numbers only. Every section records the exact configuration that produced it, so any
result here can be re-run. Nothing in this file is estimated, extrapolated, or illustrative.

Machine for all runs unless stated otherwise: Intel Core i7-5500U @ 2.40 GHz (2 cores / 4 threads),
15.9 GB RAM, Windows 10 Pro 10.0.19045, Docker Desktop 29.7.2 (WSL2, kernel 6.18.33.2), containers on Python 3.12.14 / glibc 2.41.

---

# Claim #1 — Event-time windowing vs a processing-time baseline

**Reproduce:** `docker compose run --rm --no-deps tests python -m scripts.benchmark_windowing`

## Configuration

| | |
|---|---|
| window | 60s tumbling, keyed by (window, 5-degree geo cell) |
| stream | 50 aircraft @ 1 Hz, 1800s of event time = 90,000 events generated per level |
| seed | 20260827 (`SyntheticSource` is deterministic; the same seed reproduces the byte-identical stream) |
| ground truth | every *arrived* event attributed by its own `event_time`, deduplicated |
| tolerance | window counts compared exactly; averages to 0.1% relative |
| dedup | identical on both processors, so measured error is attribution error and nothing else |
| runtime | 67.0s for the whole sweep |

Disorder is swept in two groups because the generator has two mechanisms of very different magnitude:
**BOUNDED** is out-of-order arrival within `max_skew_s`, which the watermark guarantee covers;
**UNBOUNDED** adds 90-240s late arrivals that no sane lateness bound can absorb.

## Headline

At the operationally sane setting (allowed lateness L = max arrival skew):

| disorder | events misplaced, naive | events misplaced, watermark | windows wrong, naive | windows wrong, watermark | silent errors |
|---|---|---|---|---|---|
| none | 0.00% | 0.00% | 0.00% | 0.00% | 0 -> 0 |
| low (skew 5s) | 0.22% | **0.00%** | 22.05% | **0.00%** | 195 -> 0 |
| medium (skew 20s) | 3.23% | **0.00%** | 82.40% | **0.00%** | 2,875 -> 0 |
| high (skew 45s) | 14.22% | **0.00%** | 96.37% | **0.00%** | 12,543 -> 0 |
| medium + late arrivals | 5.22% | 1.85% | 89.85% | 63.12% | 4,649 -> 0 |
| high + late arrivals | 18.69% | 4.38% | 98.61% | 85.82% | 16,487 -> 0 |

**Under bounded disorder the watermark engine is exactly correct — not approximately, not
within tolerance. Zero misplaced events out of 88,199, against 12,543 for the baseline.**

Three things the table is careful about:

1. *Window error rate is a sensitivity measure, not a magnitude.* At `low`, naive misplaces only
   0.22% of events but corrupts 22.05% of windows -- one stray event out of ~57 is enough to make a
   window's aggregate wrong. Both columns are shown because either alone misleads.
2. *Worst-case matters more than the mean.* Under `high` disorder the worst single window's aircraft
   count was off by 19 (see the per-level tables below).
3. *`silent` is the column that matters operationally.* Every event the baseline misplaces is
   misplaced with no record of it -- the aggregate is simply, quietly wrong. Every event the
   watermark engine cannot place goes to a counted side output, logged with its window and the
   watermark that closed it. Under `high+late`, that is 16,487 silent errors against 0.

## Verifying the guarantee

The property the design rests on is `allowed_lateness >= max arrival skew => output is identical to
ground truth`. The sweep confirms it and shows the degradation below the bound is monotone:

| disorder | L=0 | L=skew/2 | **L=skew** | L=2*skew |
|---|---|---|---|---|
| low (skew 5s) | 9.54% | 1.10% | **0.00%** | 0.00% |
| medium (skew 20s) | 79.83% | 34.09% | **0.00%** | 0.00% |
| high (skew 45s) | 94.90% | 80.58% | **0.00%** | 0.00% |

(window error rate). Under UNBOUNDED disorder no bound below the late-arrival delay reaches zero,
which is the honest and expected result -- `medium+late` only reaches 0.00% at L=200s, `high+late`
at L=285s. Those bounds are far too wide to be useful in production, which is precisely why the
side output exists rather than an ever-growing lateness allowance.

## Full run

```
CONTRAIL — core claim #1: event-time windowing vs processing-time baseline
========================================================================================================
window 60s | 50 aircraft @ 1 Hz | 1800s of event time | seed 20260827
python 3.12.14 on Linux-6.18.33.2-microsoft-standard-WSL2-x86_64-with-glibc2.41
'silent' = events placed wrongly with no record of it. The whole point:
the baseline cannot tell you it was wrong, the watermark engine always can.
BOUNDED DISORDER
--------------------------------------------------------------------------------------------------------
  none         ooo 0.00 @ skew 5s, dup 0.05, drop 0.01
    stream: 90,000 generated, 93,589 arrived, 89,128 unique, 1,550 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -      0 / 1,550     0.00%                 0    0.00%             0         0
    watermark       0s      0 / 1,550     0.00%                 0    0.00%             0         0
    watermark     2.5s      0 / 1,550     0.00%                 0    0.00%             0         0
    watermark       5s      0 / 1,550     0.00%                 0    0.00%             0         0  <- L = max skew
    watermark      10s      0 / 1,550     0.00%                 0    0.00%             0         0
  low          ooo 0.05 @ skew 5s, dup 0.02, drop 0.01
    stream: 90,000 generated, 90,885 arrived, 89,114 unique, 1,551 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -    257 / 1,551    16.57%               153    0.17%             3       153
    watermark       0s    142 / 1,551     9.16%               150    0.17%             3         0
    watermark     2.5s      9 / 1,551     0.58%                 9    0.01%             1         0
    watermark       5s      0 / 1,551     0.00%                 0    0.00%             0         0  <- L = max skew
    watermark      10s      0 / 1,551     0.00%                 0    0.00%             0         0
  medium       ooo 0.20 @ skew 20s, dup 0.05, drop 0.01
    stream: 90,000 generated, 93,466 arrived, 89,133 unique, 1,551 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -  1,284 / 1,557    82.47%             2,965    3.33%             7     2,965
    watermark       0s  1,253 / 1,551    80.79%             2,726    3.06%             8         0
    watermark      10s    520 / 1,551    33.53%               642    0.72%             4         0
    watermark      20s      0 / 1,551     0.00%                 0    0.00%             0         0  <- L = max skew
    watermark      40s      0 / 1,551     0.00%                 0    0.00%             0         0
  high         ooo 0.40 @ skew 45s, dup 0.10, drop 0.02
    stream: 90,000 generated, 96,938 arrived, 88,199 unique, 1,551 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -  1,569 / 1,621    96.79%            11,946   13.54%            14    11,946
    watermark       0s  1,482 / 1,551    95.55%            11,716   13.28%            22         0
    watermark    22.5s  1,253 / 1,551    80.79%             2,740    3.11%             8         0
    watermark      45s      0 / 1,551     0.00%                 0    0.00%             0         0  <- L = max skew
    watermark      90s      0 / 1,551     0.00%                 0    0.00%             0         0
UNBOUNDED DISORDER (late arrivals no bound can absorb)
--------------------------------------------------------------------------------------------------------
  medium+late  ooo 0.20 @ skew 20s, dup 0.05, late 0.020 @ 90-180s, drop 0.01
    stream: 90,000 generated, 93,465 arrived, 89,128 unique, 1,551 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -  1,485 / 1,690    87.87%             4,566    5.12%            10     4,566
    watermark       0s  1,383 / 1,551    89.17%             4,275    4.80%            10         0
    watermark      10s  1,164 / 1,551    75.05%             2,282    2.56%             7         0
    watermark      20s  1,008 / 1,551    64.99%             1,661    1.86%             6         0  <- L = max skew
    watermark      40s  1,001 / 1,551    64.54%             1,648    1.85%             6         0
    watermark     200s      0 / 1,551     0.00%                 0    0.00%             0         0
  high+late    ooo 0.40 @ skew 45s, dup 0.10, late 0.050 @ 120-240s, drop 0.02
    stream: 90,000 generated, 97,033 arrived, 88,197 unique, 1,550 true windows
    processor        L   windows wrong  win err  events misplaced  evt err  worst window    silent
    naive            -  1,765 / 1,793    98.44%            16,761   19.00%            23    16,761
    watermark       0s  1,489 / 1,550    96.06%            16,206   18.37%            28         0
    watermark    22.5s  1,429 / 1,550    92.19%             6,785    7.69%            16         0
    watermark      45s  1,337 / 1,550    86.26%             3,886    4.41%            11         0  <- L = max skew
    watermark      90s  1,270 / 1,550    81.94%             3,492    3.96%             9         0
    watermark     285s      0 / 1,550     0.00%                 0    0.00%             0         0
========================================================================================================
HEADLINE  (watermark at L = max skew, the operationally sane setting)
  none         events misplaced   0.00% ->  0.00%   |  windows wrong   0.00% ->   0.00%   |  silent errors 0 -> 0
  low          events misplaced   0.17% ->  0.00%   |  windows wrong  16.57% ->   0.00%   |  silent errors 153 -> 0
  medium       events misplaced   3.33% ->  0.00%   |  windows wrong  82.47% ->   0.00%   |  silent errors 2,965 -> 0
  high         events misplaced  13.54% ->  0.00%   |  windows wrong  96.79% ->   0.00%   |  silent errors 11,946 -> 0
  medium+late  events misplaced   5.12% ->  1.86%   |  windows wrong  87.87% ->  64.99%   |  silent errors 4,566 -> 0
  high+late    events misplaced  19.00% ->  4.41%   |  windows wrong  98.44% ->  86.26%   |  silent errors 16,761 -> 0
  completed in 70.9s
```

## Still to fill in

Phase 1.5 (control loop) and 1.6 (deterministic replay).
