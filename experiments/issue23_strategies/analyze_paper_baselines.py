"""Summarize complete scoped-adapter matrices without claiming paper rankings."""

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from analyze_continuous_batch import analyze

LABELS = {
    "native": "Native RR model",
    "reference": "Tailguard FIFO + exact memo (internal reference)",
    "ours": "Tailguard completed-save-first candidate",
    "mooncake": "Mooncake prefill routing adapter; ratio=1",
    "memserve": "MemServe Eq1/2 adapter",
    "strata": "Strata balanced admission only; exclusive I/O",
}


def summarize(screen, inputs):
    audit = analyze(screen, inputs)
    cases = json.loads(screen.read_bytes())["cases"]
    rows = []
    for case in cases:
        arms = {}
        for name, arm in case["arms"].items():
            phase = defaultdict(float)
            for event in arm["batch_iterations"]:
                phase[event["kind"]] += event["end"] - event["start"]
            copies = [
                r
                for r in arm["actions"]
                if r["kind"] in ("selected_copy", "implicit_copy")
            ]
            stages = {
                (s["id"], "P" if s["node"] < 2 else "D"): s for s in arm["stages"]
            }
            paths = []
            for row in arm["per_request"]:
                p, d = (stages[row["id"], role] for role in ("P", "D"))
                first = d["completed"] - d["full"] + d["first"]
                ready = first - row["TTFT"]
                paths.append(
                    {
                        **row,
                        "before_P": p["completed"] - p["full"] - ready,
                        "P_API": p["full"],
                        "P_to_D": d["completed"] - d["full"] - p["completed"],
                        "D_API_to_first": d["first"],
                        "P_prefix": p["prefix"],
                        "D_prefix": d["prefix"],
                    }
                )
            arms[name] = {
                "label": "Tailguard service-portfolio candidate"
                if arm.get("internal_candidate", {}).get("name") == "service-portfolio"
                else LABELS[name],
                "TTFT": arm["TTFT"],
                "nominal": arm["nominal"],
                "makespan": arm["makespan"],
                "P_counts": [
                    sum(r["p"] == n for r in arm["per_request"]) for n in (0, 1)
                ],
                "D_counts": [
                    sum(r["d"] == n for r in arm["per_request"]) for n in (2, 3)
                ],
                "accepted_prefix_tokens": {
                    role: sum(
                        s["prefix"] for (key, r), s in stages.items() if r == role
                    )
                    for role in ("P", "D")
                },
                "D_phase_occupied_sum_not_arm_wall": dict(phase),
                "planning_wall": arm["planning_wall"],
                "boundary_CPU_wall": arm["boundary_CPU_wall"],
                "iteration_scheduler_wall": arm.get(
                    "iteration_scheduler_wall_charged", 0
                ),
                "admission_scheduler_wall": arm.get("admission_CPU_wall", 0),
                "copied_bytes": sum(r["payload"] for r in copies),
                "copy_service_sum": sum(r["seconds"] for r in copies),
                "action_counts": dict(Counter(r["kind"] for r in arm["actions"])),
                "D_peak": arm["D_summary"],
                "release_priority_opportunities": arm.get("release_priorities", {}),
                "strata_reordered_admissions": sum(
                    r["reordered"] for r in arm.get("strata_admission", [])
                ),
                "paper_scope": arm.get("paper_adapter"),
                "worst_requests": sorted(paths, key=lambda r: r["TTFT"], reverse=True)[
                    :5
                ],
                "all_request_paths": paths,
            }
        rows.append(
            {
                "rate": case["rate"],
                "arms": arms,
                "candidate_gain_vs_reference": 1
                - arms["ours"]["TTFT"]["p99"] / arms["reference"]["TTFT"]["p99"],
            }
        )
    return {
        "scope": "CPU causal model; scoped mechanisms, not full paper-system or hardware ranking",
        "validated_arms": audit["validated_arms"],
        "costs_already_in_TTFT_do_not_add_again": True,
        "cases": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--screen", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = summarize(args.screen, args.inputs)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    for row in report["cases"]:
        print(
            json.dumps(
                {
                    "rate": row["rate"],
                    "p99": {k: v["TTFT"]["p99"] for k, v in row["arms"].items()},
                    "candidate_gain_vs_reference": row["candidate_gain_vs_reference"],
                }
            )
        )


if __name__ == "__main__":
    main()
