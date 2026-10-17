"""Keep the documentation from drifting away from the code it describes."""

import re
from pathlib import Path

from src.common.config import Settings

ROOT = Path(__file__).resolve().parents[2]
OPERATIONS = (ROOT / "docs" / "OPERATIONS.md").read_text(encoding="utf-8")
README = (ROOT / "README.md").read_text(encoding="utf-8")


def table_rows() -> dict[str, str]:
    rows = {}
    for line in OPERATIONS.splitlines():
        m = re.match(r"\| `([A-Z0-9_]+)` \| `([^`]*)` \|", line)
        if m:
            rows[m.group(1)] = m.group(2)
    return rows


def test_every_setting_is_in_the_configuration_reference_with_its_real_default():
    rows = table_rows()
    for name, field in Settings.model_fields.items():
        env = name.upper()
        assert env in rows, f"{env} is missing from docs/OPERATIONS.md"
        default = "unset" if field.default is None else str(field.default)
        assert rows[env] == (default or "(empty)"), f"{env}: docs say {rows[env]!r}, code says {default!r}"


def test_the_reference_does_not_document_settings_that_do_not_exist():
    known = {n.upper() for n in Settings.model_fields}
    assert set(table_rows()) <= known


def test_dashboard_only_queries_metrics_that_exist():
    src = (ROOT / "src" / "common" / "metrics.py").read_text(encoding="utf-8")
    defined = set(re.findall(r'(?:Counter|Gauge|Histogram)\(\s*"([a-z0-9_]+)"', src))
    # a histogram exposes _bucket/_sum/_count series, a counter a _total one (already in its name)
    series = defined | {f"{n}{s}" for n in defined for s in ("_bucket", "_sum", "_count")}
    dashboard = (ROOT / "ops" / "grafana" / "dashboards" / "contrail.json").read_text(encoding="utf-8")
    used = set(re.findall(r"contrail_[a-z0-9_]+", dashboard))
    assert used, "found no metrics in the dashboard; the pattern is stale"
    assert used <= series, f"dashboard references undefined metrics: {sorted(used - series)}"


def test_readme_links_point_at_files_that_exist():
    for target in re.findall(r"\]\(([^)#]+)(?:#[^)]*)?\)", README):
        if target.startswith(("http://", "https://")):
            continue
        assert (ROOT / target).exists(), f"README links to missing {target}"


def test_readme_no_longer_claims_unfixed_what_is_now_implemented_without_caveat():
    # The old text said recovery was "known and unfixed"; the replacement must not
    # overclaim either: it has to keep the limited scope of the evidence in view.
    assert "Known and\n  unfixed" not in README
    # ...and it must keep the scope of what was actually tested visible
    assert "tested in one scenario, not in general" in README
    assert "test_windowing_recovery.py" in README
