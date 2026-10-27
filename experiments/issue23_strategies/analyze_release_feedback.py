"""Align original commands, accepted cache and search work in completed CPU pairs."""

import argparse
import hashlib
import json
from pathlib import Path

from analyze_bounded_behavior import per_request


def arm_rows(arm):
    paths = per_request(arm)
    stages = {(r["id"], r["node"] < 2): r for r in arm["stages"]}
    searches = {(r["key"], r["kind"]): r for r in arm["search_records"]}
    rows = {}
    for decision in arm["decisions"]:
        key = decision["id"]
        forecast = arm["initial_cache_forecasts"][key]
        api = {r["node"] < 2: r for r in forecast["effective_API"]}
        rows[key] = {
            "path": paths[key],
            "decision": decision,
            "forecast_accepted_prefix": {
                "P": api[True]["prefix_tokens"],
                "D": api[False]["prefix_tokens"],
            },
            "actual_accepted_prefix": {
                "P": stages[key, True]["prefix"],
                "D": stages[key, False]["prefix"],
            },
            "search": {k: searches[key, k] for k in ("route", "boundary")},
        }
    return rows


def analyze(root):
    report = {
        "CPU_only": True,
        "causal_attribution_claimed": False,
        "limitation": "A forecast may apply release before its first actual execution; earlier route divergence alone does not identify CPU as its cause.",
        "cases": [],
    }
    for index in (1, 2, 3):
        path = root / f"case-{index:02d}.json"
        case = json.loads(path.read_bytes())
        arms = case["arms"]
        rows = {name: arm_rows(arms[name]) for name in ("reference", "ours")}
        if set(rows["reference"]) != set(rows["ours"]) or len(rows["ours"]) != 95:
            raise ValueError("Complete identical request set required")
        changes = []
        fields = ("p", "d", "source", "P_prefix_command", "D_prefix_command")
        for key, current in rows["ours"].items():
            previous = rows["reference"][key]
            delta = {
                k: [
                    previous["decision"]["initial_forecast"][k],
                    current["decision"]["initial_forecast"][k],
                ]
                for k in fields
                if previous["decision"]["initial_forecast"][k]
                != current["decision"]["initial_forecast"][k]
            }
            if delta:
                changes.append(
                    {
                        "request": key,
                        "origins": [
                            previous["decision"]["origin"],
                            current["decision"]["origin"],
                        ],
                        "changed": delta,
                    }
                )
        totals = {}
        for name in ("reference", "ours"):
            arm = arms[name]
            totals[name] = {
                "routing_seconds": arm["planning_wall"],
                "boundary_seconds": arm["boundary_CPU_wall"],
                "TTFT": arm["TTFT"],
                "first_actual_release_choice": min(
                    (r["origin"] for r in arm.get("slot_release_choices", [])),
                    default=None,
                ),
                "search": {
                    field: sum(r.get(field, 0) for r in arm["search_records"])
                    for field in (
                        "considered",
                        "full",
                        "calendar_full",
                        "pruned_calendars",
                        "bound_checks",
                    )
                },
            }
        report["cases"].append(
            {
                "rate": case["rate"],
                "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "summary": totals,
                "changed_commands": changes,
                "rows": rows,
            }
        )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(analyze(args.result), stream, indent=2)
