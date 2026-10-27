"""Align fixed-control replay paths and original forecasts, not net performance."""

import argparse
import hashlib
import json
from pathlib import Path


def analyze(root):
    scope = json.loads((root / "scope.json").read_bytes())
    assert scope["fixed_control_diagnostic"]
    rows = []
    for index in (1, 2, 3):
        case = json.loads((root / f"case-{index:02d}.json").read_bytes())
        a, b = (case["arms"][key] for key in ("reference", "ours"))
        per_a = {r["id"]: r for r in a["per_request"]}
        per_b = {r["id"]: r for r in b["per_request"]}
        assert len(per_a) == len(per_b) == 95 and per_a.keys() == per_b.keys()
        fields = ("p", "d", "source", "P_prefix_command", "D_prefix_command")
        changes = []
        endpoints = {key: [] for key in ("P_end", "first", "full")}
        first_trace_differences = {}
        for field in ("resource_events", "batch_iterations"):
            first_trace_differences[field] = next(
                (
                    {"record_index": j, "reference": x, "candidate": y}
                    for j, (x, y) in enumerate(zip(a[field], b[field]))
                    if x != y
                ),
                None,
            )
        decisions_b = {r["id"]: r for r in b["decisions"]}
        assert len(decisions_b) == len(a["decisions"]) == 95
        for x in a["decisions"]:
            y = decisions_b[x["id"]]
            f, g = x["initial_forecast"], y["initial_forecast"]
            if any(f[key] != g[key] for key in fields):
                changes.append({"id": x["id"], "reference": f, "candidate": g})
            for key, values in endpoints.items():
                values.append({"id": x["id"], "delta": g[key] - f[key]})
        rows.append(
            {
                "rate": case["rate"],
                "identical_execution": {
                    key: a[key] == b[key]
                    for key in (
                        "per_request",
                        "stages",
                        "resource_events",
                        "batch_iterations",
                    )
                },
                "per_request_equal_after_ID_alignment": per_a == per_b,
                "same_completion_order": [r["id"] for r in a["per_request"]]
                == [r["id"] for r in b["per_request"]],
                "same_dispatch_order": [r["id"] for r in a["decisions"]]
                == [r["id"] for r in b["decisions"]],
                "same_initial_commands": not changes,
                "first_trace_differences_not_single_cause_proof": first_trace_differences,
                "initial_command_changes": changes,
                "forecast_endpoint_changes": {
                    key: {
                        "count": sum(r["delta"] != 0 for r in values),
                        "max_absolute": max(abs(r["delta"]) for r in values),
                        "values": [r for r in values if r["delta"] != 0],
                    }
                    for key, values in endpoints.items()
                },
                "diagnostic_p99": [a["TTFT"]["p99"], b["TTFT"]["p99"]],
                "P_calls": [
                    len(a["prefill_revalidation"]),
                    len(b["prefill_revalidation"]),
                ],
            }
        )
    return {
        "fixed_control_mechanism_only": True,
        "not_net_performance": True,
        "scope_sha256": hashlib.sha256((root / "scope.json").read_bytes()).hexdigest(),
        "cases": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = analyze(args.result)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    print(
        json.dumps(
            [
                {
                    key: row[key]
                    for key in (
                        "rate",
                        "identical_execution",
                        "same_initial_commands",
                        "P_calls",
                    )
                }
                for row in report["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
