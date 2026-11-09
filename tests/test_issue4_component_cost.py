import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "analyze_issue4_component_cost.py"


def _module():
    spec = importlib.util.spec_from_file_location("issue4_component_cost", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_selector_component_costs_remain_conservative() -> None:
    report = _module().analyze(ROOT)

    assert report["classification"] == "exploratory"
    assert report["records"] == {"total": 96, "consumers": 90}
    assert report["queries"]["q0"]["h2d_ms"]["count"] == 12
    assert round(report["queries"]["q0"]["h2d_ms"]["mean"], 3) == 88.698
    assert report["queries"]["q0"]["prefill_saved_ms"] == 705.9
    assert report["queries"]["q2"]["h2d_ms"]["median"] == 2.145
    assert report["registration"] == {
        "startup_events": 15,
        "duration_available": False,
    }
    assert set(report["unavailable_components"]) == {
        "host_packing",
        "pinned_host_copy",
        "d2h",
        "registration_duration",
        "queue_wait",
    }


def test_checked_in_component_table_matches_evidence() -> None:
    module = _module()
    expected = (ROOT / "docs" / "issue4-component-cost-table.md").read_text(
        encoding="utf-8"
    )
    assert module.render_markdown(module.analyze(ROOT)) == expected
