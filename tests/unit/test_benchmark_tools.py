"""The benchmark plumbing: statistics, stabilisation, result files, rendering."""

import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import benchlib
from scripts.render_benchmarks import render, spread


def test_summarize_reports_median_and_spread():
    s = benchlib.summarize([3, 1, 2, 10])
    assert s["n"] == 4 and s["median"] == 2.5 and s["min"] == 1 and s["max"] == 10
    assert s["stdev"] == pytest.approx(4.0, abs=0.5)


def test_one_run_is_marked_as_one_run_not_zero_spread():
    s = benchlib.summarize([5])
    assert s["n"] == 1 and s["stdev"] == 0.0
    assert "1 run" in spread(s)
    assert spread(benchlib.summarize([])) == "-"


def test_time_to_stabilize_needs_sustained_recovery():
    lags = [500, 400, 150, 900, 100, 90, 80, 70]
    # the dip at index 2 is not stable (900 follows); stable from index 4 for 3 samples
    assert benchlib.time_to_stabilize(lags, 2.0, 0, 200, hold=3) == 8.0
    assert benchlib.time_to_stabilize([500, 400], 2.0, 0, 200) is None
    assert benchlib.time_to_stabilize(lags, 2.0, 4, 200, hold=3) == 0.0


def test_false_positives_count_only_actions_before_the_burst():
    decisions = [SimpleNamespace(changed=c) for c in (False, True, False, True, True)]
    assert benchlib.actions_before(decisions, 3) == 1
    assert benchlib.actions_before(decisions, 5) == 3


def test_environment_records_where_it_ran():
    e = benchlib.environment()
    assert e["python"] and e["platform"] and e["cpu_count"]
    assert "git_commit" in e and "git_dirty" in e and "image_digests" in e


def test_result_files_carry_the_environment(tmp_path):
    path = benchlib.write_result("demo", {"x": 1}, tmp_path)
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["benchmark"] == "demo" and data["x"] == 1
    assert data["environment"]["recorded_at"]


def test_windowing_benchmark_end_to_end_into_rendered_tables(tmp_path):
    """Run the real (pure, fast) benchmark small, then render what it saved."""
    cmd = [sys.executable, "-m", "scripts.benchmark_windowing", "--aircraft", "4",
           "--ticks", "120", "--runs", "2", "--out-dir", str(tmp_path)]
    done = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    assert done.returncode == 0, done.stderr[-1500:]
    files = list(tmp_path.glob("windowing-*.json"))
    assert len(files) == 1
    saved = json.loads(files[0].read_text(encoding="utf-8"))
    assert len(saved["runs"]) == 2 and saved["runs"][1]["seed"] == saved["runs"][0]["seed"] + 1
    md = render([saved])
    assert "| level |" in md and "medium+late" in md and "commit `" in md


def test_control_render_from_a_synthetic_result():
    keys = ("peak_lag", "final_lag", "time_to_stabilize_s", "shed_dropped", "undrained",
            "absorbed_pct", "scale_actions", "false_positive_actions", "p50_s", "p95_s",
            "p99_s", "max_s", "cpu_s", "peak_rss_mb")
    arm = {k: benchlib.summarize([1, 2, 3]) for k in keys}
    result = {
        "benchmark": "control",
        "environment": {"recorded_at": "2026-10-17T00:00:00+00:00", "git_commit": "a" * 40,
                        "git_dirty": False, "python": "3.12", "platform": "x", "cpu_count": 2,
                        "memory_bytes": 2**33},
        "config": {"aircraft": 60, "base_rate": 2, "burst_rate": 12, "burst_s": 70,
                   "max_rate": 250, "min_workers": 1, "max_workers": 2},
        "runs": [[], [], []], "summary": {"STATIC": arm, "ADAPTIVE": arm},
    }
    md = render([result])
    assert "| metric | STATIC | ADAPTIVE |" in md and "2.00 [1.00 - 3.00]" in md


def _fake_arm(lags, changed_at=(), shed=0, published=1000, stored=900):
    from scripts.benchmark_control import Arm
    from src.control.controller import Decision
    from src.control.lag_monitor import LagSample
    from src.control.supervisor import SupervisionResult

    samples = [LagSample(at=i * 2.0, total=lag) for i, lag in enumerate(lags)]
    decisions = [Decision("scale_up" if i in changed_at else "hold", 2, False, lag, 0.0, "")
                 for i, lag in enumerate(lags)]
    sup = SupervisionResult(decisions=decisions, samples=samples, shed_dropped=shed)
    return Arm("ADAPTIVE", published, stored, sup, 1.0, 2.0, 3.0, 4.0, False,
               resources={"cpu_s": 5.0, "peak_rss_mb": 100.0}, timing={"startup_s": 2.0})


def test_arm_analysis_derives_the_documented_metrics():
    from scripts.benchmark_control import analyze, build_parser

    args = build_parser().parse_args(["--baseline-s", "4", "--burst-s", "4", "--interval", "2"])
    # baseline = samples 0-1, burst = samples 2-3, then recovery
    arm = _fake_arm([10, 20, 900, 1500, 400, 150, 120, 100], changed_at={1, 3}, shed=60)
    a = analyze(arm, args)
    assert a["peak_lag"] == 1500 and a["final_lag"] == 100
    assert a["scale_actions"] == 2 and a["false_positive_actions"] == 1  # index 1 is in the baseline
    assert a["time_to_stabilize_s"] == 2.0  # lag <= 200 sustained from the sample at t=10s, burst ended t=8s
    assert a["undrained"] == 1000 - 900 - 60 and a["shed_dropped"] == 60


def test_summarize_arms_counts_runs_that_never_stabilised():
    from scripts.benchmark_control import analyze, build_parser, summarize_arms

    args = build_parser().parse_args(["--baseline-s", "4", "--burst-s", "4", "--interval", "2"])
    ok = analyze(_fake_arm([10, 20, 900, 900, 100, 100, 100]), args)
    never = analyze(_fake_arm([10, 20, 900, 900, 900, 900, 900]), args)
    s = summarize_arms([ok, never])
    assert s["time_to_stabilize_s"]["n"] == 1 and s["time_to_stabilize_s"]["missing"] == 1
    assert s["peak_lag"]["n"] == 2


def test_sweep_grid_and_overload_ratio():
    from scripts.benchmark_control import build_parser
    from scripts.sweep_control import cells, overload_ratio

    grid = {"burst_rate": [12.0, 24.0], "max_workers": [2, 4]}
    assert len(cells(grid)) == 4 and {"burst_rate": 24.0, "max_workers": 2} in cells(grid)
    args = build_parser().parse_args(["--aircraft", "60", "--burst-rate", "12",
                                      "--max-rate", "250", "--max-workers", "2"])
    assert overload_ratio(args) == pytest.approx(60 * 12 / (250 * 2))  # 1.44: beyond the cap
