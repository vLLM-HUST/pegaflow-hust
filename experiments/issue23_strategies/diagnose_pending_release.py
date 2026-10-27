"""Post-hoc D-slot release decomposition; never an online oracle."""

import argparse
import json
from pathlib import Path

from analyze_bounded_behavior import per_request


def diagnose(root):
    report = {
        "scope": "Retrospective observed simulated paths; not causal attribution or hardware timers",
        "cases": [],
    }
    for i in (1, 2, 3):
        case = json.loads((root / f"case-{i:02d}.json").read_bytes())
        pair = {"rate": case["rate"], "arms": {}}
        for name in ("reference", "ours"):
            arm = case["arms"][name]
            paths = per_request(arm)
            p = {s["id"]: s for s in arm["stages"] if s["node"] < 2}
            d = {s["id"]: s for s in arm["stages"] if s["node"] >= 2}
            control = {
                e["request"]: e
                for e in arm["resource_events"]
                if e["operation"] == "D_control"
            }
            keys = {
                max(paths, key=lambda k: paths[k]["TTFT"]),
                max(paths, key=lambda k: d[k]["completed"] - paths[k]["ready"]),
            }
            rows = {}
            for key in keys:
                path, start = paths[key], p[key]["completed"]
                c = control[key]
                api_start = d[key]["completed"] - d[key]["full"]
                parts = {
                    "P_complete_to_D_control_start": c["start"] - start,
                    "D_control_interval": c["end"] - c["start"],
                    "control_end_to_D_API": api_start - c["end"],
                }
                assert abs(sum(parts.values()) - path["P_to_D"]) < 1e-7
                holders = [
                    {
                        "request": k,
                        "first_already_observed": paths[k]["first"] <= start,
                        "observed_release_after_target_P": other["completed"] - start,
                    }
                    for k, other in d.items()
                    if k != key
                    and other["node"] == d[key]["node"]
                    and other["completed"] - other["full"] <= start < other["completed"]
                ]
                rows[key] = {
                    "path": path,
                    "P_to_D_parts": parts,
                    "D_API_holders_at_target_P_end": holders,
                    "ready_to_full_completion": d[key]["completed"] - path["ready"],
                    "D_first_to_full_completion": d[key]["full"] - d[key]["first"],
                }
            pair["arms"][name] = rows
        report["cases"].append(pair)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x", encoding="utf8") as stream:
        json.dump(diagnose(args.result), stream, indent=2)
