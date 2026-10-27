"""Paired net tails and original endpoint residuals, not causal proof."""

import json
import math
import statistics
import sys
from pathlib import Path

from analyze_bounded_behavior import per_request


def analyze(root):
    result = {
        "scope": "same-run CPU simulation; no parallel NPU qualification",
        "cases": [],
    }
    for index in (1, 2, 3):
        case = json.loads((root / f"case-{index:02d}.json").read_bytes())
        summaries, all_rows = {}, {}
        for name in ("reference", "ours"):
            arm = case["arms"][name]
            rows = per_request(arm)
            all_rows[name] = rows
            signed = [
                rows[r["id"]]["first"] - r["initial_forecast"]["first"]
                for r in arm["decisions"]
            ]
            copies = [
                r for r in arm["actions"] if r["kind"] == "software_copy_gate_hold"
            ]
            summaries[name] = {
                "TTFT": arm["TTFT"],
                "makespan": arm["makespan"],
                "routing_wall": arm["planning_wall"],
                "boundary_wall": arm["boundary_CPU_wall"],
                "iteration_wall": arm["iteration_scheduler_wall_charged"],
                "P_distribution": [
                    sum(r["P"] == n for r in rows.values()) for n in (0, 1)
                ],
                "D_distribution": [
                    sum(r["D"] == n for r in rows.values()) for n in (2, 3)
                ],
                "first_endpoint_error": {
                    "MAE": statistics.mean(abs(v) for v in signed),
                    "RMSE": math.sqrt(statistics.mean(v * v for v in signed)),
                    "bias_actual_minus_prediction": statistics.mean(signed),
                    "not_pure_queue_error": True,
                },
                "software_link_wait_sum": sum(r["link_wait"] for r in copies),
                "software_copy_gate_hold_sum": sum(
                    r["released"] - r["acquired"] for r in copies
                ),
                "restore_path_sum": sum(
                    r["restore_path_not_pure_H2D"] for r in arm["stages"]
                ),
                "accepted_prefix_tokens": {
                    role: sum(
                        r["prefix"]
                        for r in arm["stages"]
                        if (r["node"] < 2) == (role == "P")
                    )
                    for role in ("P", "D")
                },
                "worst_request": max(rows, key=lambda k: rows[k]["TTFT"]),
            }
        ids = set(all_rows["ours"]) | set(all_rows["reference"])
        assert len(ids) == 95
        focused = dict.fromkeys(
            [s["worst_request"] for s in summaries.values()] + ["git-multibranch:11"]
        )
        paired = {}
        for key in focused:
            old, new = all_rows["reference"][key], all_rows["ours"][key]
            paired[key] = {
                "reference": old,
                "candidate": new,
                "delta": {
                    field: new[field] - old[field]
                    for field in (
                        "TTFT",
                        "before_P",
                        "P_API",
                        "P_to_D",
                        "D_API_to_first",
                    )
                },
            }
        result["cases"].append(
            {
                "rate": case["rate"],
                "arms": summaries,
                "net_p99_reduction_percent": 100
                * (
                    1
                    - summaries["ours"]["TTFT"]["p99"]
                    / summaries["reference"]["TTFT"]["p99"]
                ),
                "faster_requests": sum(
                    all_rows["ours"][k]["TTFT"] < all_rows["reference"][k]["TTFT"]
                    for k in ids
                ),
                "focused": paired,
            }
        )
    return result


if __name__ == "__main__":
    report = analyze(Path(sys.argv[1]))
    with Path(sys.argv[2]).open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    print(
        json.dumps(
            [
                {
                    "rate": c["rate"],
                    "gain": c["net_p99_reduction_percent"],
                    "ref": c["arms"]["reference"]["TTFT"],
                    "new": c["arms"]["ours"]["TTFT"],
                }
                for c in report["cases"]
            ]
        )
    )
