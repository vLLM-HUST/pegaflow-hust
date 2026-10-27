"""Align revalidation actions and full cost; no action is not a causal effect."""

import argparse
import hashlib
import json
from pathlib import Path

from analyze_bounded_behavior import per_request
from analyze_copy_waits import analyze_arm


def analyze(root):
    report = {
        "CPU_only": True,
        "trace_conditioned_NOT_causal_ablation": True,
        "cases": [],
    }
    for index in (1, 2, 3):
        path = root / f"case-{index:02d}.json"
        raw = json.loads(path.read_bytes())
        metrics, paths, waits = {}, {}, {}
        for name in ("reference", "ours"):
            a = raw["arms"][name]
            paths[name] = per_request(a)
            waits[name] = analyze_arm(a)
            metrics[name] = {
                k: a[k]
                for k in (
                    "TTFT",
                    "nominal",
                    "makespan",
                    "planning_wall",
                    "boundary_CPU_wall",
                    "fit_CPU_wall",
                    "iteration_scheduler_wall_charged",
                    "decode_advice_CPU_wall",
                )
            }
            metrics[name]["P_revalidation_CPU_wall"] = a.get(
                "prefill_revalidation_CPU_wall", 0
            )
            metrics[name]["D_moves"] = sum(
                r["status"] == "move" for r in a["decode_advice"]
            )
            metrics[name]["copy_wait_diagnostic"] = {
                k: v
                for k, v in waits[name].items()
                if k not in ("requests", "largest_waits")
            }
        actions = raw["arms"]["ours"]["prefill_revalidation"]
        worst = {n: max(paths[n], key=lambda k: paths[n][k]["TTFT"]) for n in paths}
        keys = {r["id"] for r in actions} | set(worst.values())
        aligned = {key: {n: paths[n][key] for n in paths} for key in sorted(keys)}
        ref, ours = (raw["arms"][n] for n in ("reference", "ours"))
        original_routes = [{r["id"]: r for r in a["decisions"]} for a in (ref, ours)]
        route_fields = ("p", "d", "source", "P_prefix_command", "D_prefix_command")
        diverged = [
            r["id"]
            for r in ref["decisions"]
            if any(
                original_routes[0][r["id"]]["initial_forecast"][f]
                != original_routes[1][r["id"]]["initial_forecast"][f]
                for f in route_fields
            )
        ]
        first_difference = (
            min(diverged, key=lambda k: paths["reference"][k]["ready"])
            if diverged
            else None
        )
        report["cases"].append(
            {
                "rate": raw["rate"],
                "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "metrics": metrics,
                "p99_reduction_percent": 100
                * (1 - ours["TTFT"]["p99"] / ref["TTFT"]["p99"]),
                "P_decisions": len(actions),
                "P_changes": sum(r["status"] == "local" for r in actions),
                "no_P_decision_does_not_identify_a_revalidation_effect": not actions,
                "worst": worst,
                "aligned": aligned,
                "actions": actions,
                "first_original_route_difference_by_reference_ready": {
                    "id": first_difference,
                    "records": {
                        n: original_routes[i][first_difference]
                        for i, n in enumerate(("reference", "ours"))
                    },
                }
                if diverged
                else None,
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
                {
                    k: v
                    for k, v in c.items()
                    if k
                    in (
                        "rate",
                        "p99_reduction_percent",
                        "P_decisions",
                        "P_changes",
                        "worst",
                    )
                }
                for c in report["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
