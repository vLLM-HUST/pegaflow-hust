"""Partition observed D pre-first residence; overlaps are not counterfactual gains."""

import argparse
import hashlib
import json
from pathlib import Path

from analyze_advice_chronology import analyze as chronology


def partition(arm, key):
    stage = next(s for s in arm["stages"] if s["id"] == key and s["node"] >= 2)
    start = stage["completed"] - stage["full"]
    first = start + stage["first"]
    parts, barriers = {}, []
    for event in arm["batch_iterations"]:
        if event["node"] != stage["node"]:
            continue
        overlap = min(event["end"], first) - max(event["start"], start)
        if overlap <= 0:
            continue
        own = any(k == key for k, _, _ in event["pieces"])
        label = ("contains_target_" if own else "other_") + event["kind"]
        parts[label] = parts.get(label, 0) + overlap
        if event["kind"] != "compute":
            barriers.append(event)
    parts["uncovered_gate_or_controller_gap"] = max(
        0, first - start - sum(parts.values())
    )
    return {
        "API_start": start,
        "first": first,
        "residence": stage["first"],
        "interval_partition_not_additional_cost": parts,
        "barriers": barriers,
    }


def analyze(root):
    result = chronology(root, json.loads((root / "charged-feedback.json").read_bytes()))
    for i, case in enumerate(result["cases"], 1):
        path = root / f"case-{i:02d}.json"
        raw = json.loads(path.read_bytes())
        case["phase_blocking"] = {
            key: {
                name: partition(raw["arms"][name], key)
                for name in ("reference", "ours")
            }
            for key in (r["id"] for r in raw["arms"]["ours"]["per_request"])
        }
        arm = raw["arms"]["ours"]
        checks = arm.get("calendar_window_records", [])
        cancels = [
            r
            for r in arm["decode_advice"]
            if r["status"] == "predicted_acquisition_before_search"
        ]
        decisions = case["first_initial_route_difference"]
        first_check = min((q["origin"] for q in checks), default=None)
        first_cancel = min((q["origin"] for q in cancels), default=None)
        case.update(
            raw_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            window_checks=len(checks),
            window_cancellations=len(cancels),
            first_check=first_check,
            first_cancel=first_cancel,
            first_difference_before_check=(
                all(v["origin"] < first_check for v in decisions["records"].values())
                if decisions and first_check is not None
                else None
            ),
            first_difference_before_cancel=(
                all(v["origin"] < first_cancel for v in decisions["records"].values())
                if decisions and first_cancel is not None
                else None
            ),
        )
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--result", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    r = analyze(a.result)
    with a.output.open("x", encoding="utf-8") as f:
        json.dump(r, f, indent=2)
    print(
        json.dumps(
            [
                {
                    k: c[k]
                    for k in (
                        "rate",
                        "p99_reduction_percent",
                        "window_checks",
                        "window_cancellations",
                        "first_difference_before_check",
                        "first_difference_before_cancel",
                    )
                }
                for c in r["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
