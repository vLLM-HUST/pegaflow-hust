"""Verify optional advisory off while real routing and D boundary work remain."""

import argparse
import json
from pathlib import Path

from audit_yielding_decode import audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for i in (1, 2, 3):
        case = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
        ref, ours = case["arms"]["reference"], case["arms"]["ours"]
        assert ref["internal_candidate"]["name"] == "prefix-reclaim-calendar-02"
        assert ours["internal_candidate"]["name"] == "no-decode-advice-01"
        assert not ours["decode_advice"] and ours["decode_advice_CPU_wall"] == 0
        assert not ours["decode_advice_ablation"]["enabled"]
        assert ref["decode_advice"], "Reference must actually execute the tested mechanism"
        normal = audit(ref)
        decisions = {r["id"]: r["initial_forecast"] for r in ours["decisions"]}
        assert len(decisions) == len(ours["per_request"]) == 95
        for r in ours["per_request"]:
            assert decisions[r["id"]]["d"] == r["d"]
        for arm in (ref, ours):
            controls = [r for r in arm["resource_events"] if r["operation"] == "D_control"]
            assert len(controls) == 95 and all(r["end"] > r["start"] for r in controls)
            assert arm["planning_wall"] > 0 and arm["boundary_CPU_wall"] > 0
            assert len(arm["stages"]) == 190 and arm["full_decode_drain_required"]
            assert all(q["reclaim_removal_method"] == "prefix_remove_victims"
                       and q["forecast_time_arithmetic"] == "binary64-exact-dyadic"
                       for q in arm["initial_cache_forecasts"].values())
        rows.append({"rate": case["rate"], "reference_advice": normal,
                     "ours_advice": 0, "ours_preserved_D_controls": 95})
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump({"all_checks_passed": True, "CPU_only": True, "cases": rows}, stream, indent=2)


if __name__ == "__main__":
    main()
