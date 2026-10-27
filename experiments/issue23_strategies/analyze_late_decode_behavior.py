"""Align completed reselect costs with observed slot releases, not oracle gains."""

import argparse
import hashlib
import json
from pathlib import Path

from analyze_bounded_behavior import per_request


def analyze(root):
    report = {"CPU_only": True, "causal_attribution": False, "cases": []}
    for i in (1, 2, 3):
        path = root / f"case-{i:02d}.json"
        case = json.loads(path.read_bytes())
        arms = case["arms"]
        own = arms["ours"]
        paths = {n: per_request(arms[n]) for n in ("reference", "ours")}
        stages = {(r["id"], r["node"] >= 2): r for r in own["stages"]}
        controls = {
            r["request"]: r
            for r in own["resource_events"]
            if r["operation"] == "D_control"
        }
        rows = []
        for r in own["late_decode_decisions"]:
            holders = [
                key
                for key, control in controls.items()
                if control["node"] == r["before"]
                and control["start"] <= r["origin"] < stages[key, True]["completed"]
            ]
            # The current trace contains the decisions' effects. A release is
            # evidence of an opportunity, not a counterfactual TTFT improvement.
            first_release = min(
                (stages[key, True]["completed"] for key in holders), default=None
            )
            missed = (
                r["queued"][str(r["before"])] == 0
                and first_release is not None
                and first_release < r["decision_ready"]
            )
            best = min(
                r["evaluations"],
                key=lambda v: (
                    v["objective"],
                    v["payload"],
                    v["prefix_blocks"],
                    v["d"] != r["before"],
                ),
            )
            assert best["d"] == r["after"]
            key = r["id"]
            actual_D_start = controls[key]["start"]
            rows.append(
                {
                    **r,
                    "holders_at_origin": holders,
                    "first_observed_release": first_release,
                    "release_before_decision_without_prior_waiter": missed,
                    "same_destination": r["before"] == r["after"],
                    "old_D_CPU_hold_after_release": r["decision_ready"] - first_release
                    if missed
                    else 0,
                    "actual_D_control_start": actual_D_start,
                    "post_reselect_slot_wait": actual_D_start - r["committed"],
                    "selected_prediction": best,
                    "actual_first": paths["ours"][key]["first"],
                    "path": {n: p[key] for n, p in paths.items()},
                }
            )
        summary = {
            "calls": len(rows),
            "moves": sum(not r["same_destination"] for r in rows),
            "CPU_wall": own["late_decode_CPU_wall"],
            "CPU_no_move": sum(
                r["CPU_wall"] + r["commit_CPU_wall"]
                for r in rows
                if r["same_destination"]
            ),
            "release_before_decision_no_prior_waiter": sum(
                r["release_before_decision_without_prior_waiter"] for r in rows
            ),
            "same_D_release_before_decision": sum(
                r["release_before_decision_without_prior_waiter"]
                and r["same_destination"]
                for r in rows
            ),
            "CPU_hold_after_observed_release_sum_NOT_net_gain": sum(
                r["old_D_CPU_hold_after_release"] for r in rows
            ),
            "gain_vs_paired_reference_percent": 100
            * (1 - own["TTFT"]["p99"] / arms["reference"]["TTFT"]["p99"]),
        }
        worst = {
            name: max(values, key=lambda k: values[k]["TTFT"])
            for name, values in paths.items()
        }
        report["cases"].append(
            {
                "rate": case["rate"],
                "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "summary": summary,
                "arms": {
                    n: {
                        k: arms[n][k]
                        for k in (
                            "TTFT",
                            "nominal",
                            "makespan",
                            "planning_wall",
                            "boundary_CPU_wall",
                        )
                    }
                    for n in ("reference", "ours")
                },
                "worst_keys": worst,
                "worst_aligned": {
                    k: {n: p[k] for n, p in paths.items()} for k in set(worst.values())
                },
                "rows": rows,
            }
        )
    return report


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
                {"rate": r["rate"], **r["summary"], "worst": r["worst_keys"]}
                for r in report["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
