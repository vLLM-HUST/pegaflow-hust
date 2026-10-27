"""Verify actual forecast hook activation and optional full replay equivalence."""

import argparse
import json
from pathlib import Path


def main(candidate_method="linear_remove_victims", candidate_revision="linear-reclaim-calendar-01"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for index in (1, 2, 3):
        case = json.loads((args.result / f"case-{index:02d}.json").read_bytes())
        counts = {}
        for label, method, revision in (
            ("reference", "remove_victims", "exact-binary-calendar-02"),
            ("ours", candidate_method, candidate_revision),
        ):
            arm = case["arms"][label]
            quotes = arm["initial_cache_forecasts"]
            assert len(quotes) == len(arm["per_request"]) == 95
            assert arm["internal_candidate"]["name"] == revision
            assert all(q["forecast_time_arithmetic"] == "binary64-exact-dyadic"
                       and q["reclaim_removal_method"] == method for q in quotes.values())
            calls = sum(q["reclaim_removal_calls"] for q in quotes.values())
            assert calls > 0, "Hook identity alone does not prove victim removal ran"
            counts[label] = {"selected_forecasts": len(quotes), "actual_removal_calls": calls}
        if case.get("zero_control_exact_mechanism_equivalence"):
            a, b = case["arms"]["reference"], case["arms"]["ours"]
            for field in ("per_request", "stages", "resource_events", "batch_iterations", "decisions"):
                assert a[field] == b[field], field
            for key, quote in a["initial_cache_forecasts"].items():
                other = b["initial_cache_forecasts"][key]
                assert {k: v for k, v in quote.items() if k != "reclaim_removal_method"} == {
                    k: v for k, v in other.items() if k != "reclaim_removal_method"
                }, key
        rows.append({"rate": case["rate"], **counts})
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump({"all_checks_passed": True, "cases": rows}, stream, indent=2)
    print(json.dumps({"actual_forecasts_verified": 570, "all_checks_passed": True}))


if __name__ == "__main__":
    main()
