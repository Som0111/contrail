"""Shared plumbing for the benchmark scripts: repeat runs, record where they ran, save JSON.

A benchmark number without its environment and its spread is an anecdote. Every
result file written here carries the git commit, interpreter, machine and (when
docker is reachable) image digests it was measured with, and every metric repeated
across runs is stored as median plus min/max/stdev instead of one draw.
"""

import json
import os
import platform
import statistics
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

RESULTS_DIR = Path("benchmarks/results")


def _git(*args: str) -> str | None:
    try:
        out = subprocess.run(["git", *args], capture_output=True, text=True, timeout=10, check=True)
        return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def _image_digests() -> dict[str, str]:
    """service -> image id, best effort: empty when docker or the stack is not reachable."""
    try:
        out = subprocess.run(
            ["docker", "compose", "images", "--format", "json"],
            capture_output=True, text=True, timeout=20, check=True,
        ).stdout.strip()
        if not out:
            return {}
        rows = json.loads(out) if out.startswith("[") else [json.loads(x) for x in out.splitlines()]
        return {f"{r.get('ContainerName') or r.get('Service')}": f"{r.get('Repository')}:{r.get('Tag')}@{r.get('ID')}"
                for r in rows if r is not None}
    except (OSError, subprocess.SubprocessError, ValueError):
        return {}


def _memory_bytes() -> int | None:
    try:
        import psutil
        return psutil.virtual_memory().total
    except ImportError:
        pass
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError, OSError):
        return None


def environment() -> dict:
    status = _git("status", "--porcelain")
    return {
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "git_commit": _git("rev-parse", "HEAD"),
        # A dirty tree means the commit does not fully describe the code that ran.
        "git_dirty": bool(status) if status is not None else None,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": _memory_bytes(),
        "image_digests": _image_digests(),
    }


def summarize(values: list[float]) -> dict:
    """median, mean, stdev, min, max over repeated runs. stdev is 0 for a single run,
    which `n` makes visible: one run has no spread to report, not zero spread."""
    vals = [float(v) for v in values]
    if not vals:
        return {"n": 0}
    return {
        "n": len(vals),
        "median": statistics.median(vals),
        "mean": statistics.fmean(vals),
        "stdev": statistics.stdev(vals) if len(vals) > 1 else 0.0,
        "min": min(vals),
        "max": max(vals),
    }


def resource_snapshot() -> dict:
    """CPU seconds used by this process so far, and its peak resident memory in MB."""
    snap = {"cpu_s": time.process_time(), "peak_rss_mb": None}
    try:
        import resource  # POSIX only
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        snap["peak_rss_mb"] = rss / (1024 if platform.system() == "Darwin" else 1) / 1024
    except ImportError:
        try:
            import psutil
            snap["peak_rss_mb"] = psutil.Process().memory_info().rss / 1024 / 1024
        except ImportError:
            pass
    return snap


def time_to_stabilize(lags: list[int], interval_s: float, from_index: int,
                      threshold: int, hold: int = 3) -> float | None:
    """Seconds from `from_index` until lag stays at or below `threshold` for `hold`
    consecutive samples; None if it never does within the run."""
    run = 0
    for i in range(from_index, len(lags)):
        run = run + 1 if lags[i] <= threshold else 0
        if run >= hold:
            return (i - hold + 1 - from_index) * interval_s
    return None


def actions_before(decisions, index: int) -> int:
    """Applied (non-hold) decisions among the first `index`: for a run whose first
    phase is steady baseline load, every one of them is a false positive."""
    return sum(1 for d in decisions[:index] if d.changed)


def write_result(name: str, payload: dict, outdir: Path | None = None) -> Path:
    outdir = Path(outdir) if outdir else RESULTS_DIR
    outdir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = outdir / f"{name}-{stamp}.json"
    path.write_text(json.dumps({"benchmark": name, "environment": environment(), **payload},
                               indent=2, default=str) + "\n", encoding="utf-8")
    return path


def load_results(paths: list[Path]) -> list[dict]:
    return [json.loads(Path(p).read_text(encoding="utf-8")) for p in paths]
