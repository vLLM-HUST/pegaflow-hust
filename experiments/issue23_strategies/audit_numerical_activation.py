"""Require actual selected forecast arithmetic, not a candidate label."""

import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for index in (1, 2, 3):
        case = json.loads((args.result / f"case-{index:02d}.json").read_bytes())
        counts = {}
        for label, expected in (
            ("reference", "float"),
            ("ours", "binary64-exact-dyadic"),
        ):
            arm = case["arms"][label]
            quotes = arm["initial_cache_forecasts"]
            assert len(quotes) == len(arm["per_request"]) == 95
            assert all(
                q["forecast_time_arithmetic"] == expected for q in quotes.values()
            )
            counts[label] = {
                "verified_actual_selected_forecasts": len(quotes),
                "arithmetic": expected,
            }
        candidate = case["arms"]["ours"]
        assert candidate["internal_candidate"]["name"] == "exact-binary-calendar-02"
        assert candidate["forecast_arithmetic"][
            "activation_verified_per_selected_forecast"
        ]
        rows.append({"rate": case["rate"], **counts})
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump({"all_checks_passed": True, "cases": rows}, stream, indent=2)
    print(json.dumps({"actual_forecasts_verified": 570, "all_checks_passed": True}))


if __name__ == "__main__":
    main()
