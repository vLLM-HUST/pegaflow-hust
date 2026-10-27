"""Validate and summarize a completed CPU screen; no hardware qualification."""

import argparse
import hashlib
import itertools
import json
import math
from collections import defaultdict
from pathlib import Path


def analyze(path, inputs):
    raw = path.read_bytes()
    report = json.loads(raw)
    bundle = json.loads(inputs.read_bytes())
    if report["input_sha256"] != hashlib.sha256(inputs.read_bytes()).hexdigest():
        raise ValueError("Input hash mismatch")
    order = [row["id"] for row in bundle["rows"]]
    sessions = {row["id"]: row["task"] for row in bundle["rows"]}
    summary = []
    checks = 0
    for case in report["cases"]:
        row = {
            "rate": case["rate"],
            "max_sequences": case["config"]["max_sequences"],
            "shared_fraction": case["config"]["shared_fraction"],
        }
        for mode, arm in case["arms"].items():
            completed = {r["id"]: r for r in arm["per_request"]}
            if len(completed) != len(order) or set(completed) != set(order):
                raise ValueError("Not the complete frozen request stream")
            # Nominal offsets are common across arms; infer only observed
            # ready/first differences, without reading future policy labels.
            for value in completed.values():
                if not math.isclose(
                    value["TTFT"] + value["dependency"],
                    value["nominal"],
                    rel_tol=1e-10,
                    abs_tol=1e-8,
                ):
                    raise ValueError("Latency accounting is inconsistent")
            for decision in arm["decisions"]:
                if decision["model_latest"] > decision["origin"]:
                    raise ValueError("Future completion label entered a route decision")
            for node, info in arm["D_summary"].items():
                if (
                    info["peak_active"] > case["config"]["max_sequences"]
                    or info["peak_kv_bytes"] > case["config"]["kv_bytes"]
                ):
                    raise ValueError(f"D{node} capacity violated")
            timelines = defaultdict(list)
            for step in arm["batch_iterations"]:
                if (
                    sum(piece[1] for piece in step["pieces"])
                    > case["config"]["token_budget"]
                ):
                    raise ValueError("Iteration token budget violated")
                timelines[step["node"]].append((step["start"], step["end"]))
            for intervals in timelines.values():
                intervals.sort()
                if any(a[1] > b[0] + 1e-8 for a, b in itertools.pairwise(intervals)):
                    raise ValueError(
                        "Two GPU iterations overlap on one simulated device"
                    )
            stages = defaultdict(dict)
            for stage in arm["stages"]:
                role = "P" if stage["node"] < 2 else "D"
                if role in stages[stage["id"]]:
                    raise ValueError("Duplicate stage completion")
                stages[stage["id"]][role] = stage
            previous = {}
            for key in order:
                if set(stages[key]) != {"P", "D"}:
                    raise ValueError("Missing P or D completion")
                p, d = stages[key]["P"], stages[key]["D"]
                d_start = d["completed"] - d["full"]
                p_start = p["completed"] - p["full"]
                if d_start < p["completed"] - 1e-8:
                    raise ValueError("D ran before its P published")
                if p_start < previous.get(sessions[key], 0.0) - 1e-8:
                    raise ValueError("Same-session causality violated")
                previous[sessions[key]] = d["completed"]
            row[mode] = {
                "TTFT": arm["TTFT"],
                "makespan": arm["makespan"],
                "planning_wall": arm["planning_wall"],
                "boundary_CPU_wall": arm["boundary_CPU_wall"],
                "D_summary": arm["D_summary"],
                "P_distribution": [
                    sum(v["p"] == n for v in completed.values()) for n in (0, 1)
                ],
                "D_distribution": [
                    sum(v["d"] == n for v in completed.values()) for n in (2, 3)
                ],
            }
            checks += 1
        row["gain"] = case["descriptive_p99_gain"]
        summary.append(row)
    return {
        "kind": "completed-CPU-screen-accounting-not-real-output-audit",
        "screen_sha256": hashlib.sha256(raw).hexdigest(),
        "validated_arms": checks,
        "requests_per_arm": len(order),
        "real_parallel_serving_validated": False,
        "summary": summary,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--screen", type=Path, required=True)
    parser.add_argument("--input-bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    value = analyze(args.screen, args.input_bundle)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
    print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
