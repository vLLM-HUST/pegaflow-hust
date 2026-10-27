"""Fully charged reclaim implementation comparison with cache/queue accounting."""

import argparse
import json
from pathlib import Path

from analyze_bounded_behavior import per_request
from analyze_numerical_charged import analyze
from analyze_prefill_feedback import analyze as feedback


def queue_context(arm, key):
    row = per_request(arm)[key]
    start = row["ready"] + row["before_P"]
    pend = start + row["P_API"]
    dstart = pend + row["P_to_D"]
    blockers = [e for e in arm["resource_events"] if e["request"] != key
                and e["node"] == row["P"] and e["end"] > row["ready"] and e["start"] < start]
    intervals = sorted((max(e["start"], row["ready"]), min(e["end"], start)) for e in blockers)
    merged = []
    for left, right in intervals:
        if merged and left <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], right)
        else:
            merged.append([left, right])
    iterations = [e for e in arm["batch_iterations"] if e["node"] == row["D"]
                  and e["end"] > pend and e["start"] < dstart]
    decision = next(d for d in arm["decisions"] if d["id"] == key)
    return {
        "path": row,
        "initial_decision_not_after_P_revision": decision,
        "actual_first_minus_original_prediction": row["first"] - decision["initial_forecast"]["first"],
        "other_P_resource_overlap_NOT_pure_queue_attribution": sum(b - a for a, b in merged),
        "P_overlap_events": blockers,
        "own_D_control_events": [e for e in arm["resource_events"]
                                 if e["request"] == key and e["operation"] == "D_control"],
        "D_gap_iteration_count_NOT_added_cost": len(iterations),
        "D_gap_first_last_iterations": iterations[:1] + iterations[-1:],
    }


def main(
    revision="linear-reclaim-calendar-01",
    audit_file="linear-reclaim-activation-audit.json",
    reference_revision="exact-binary-calendar-02",
):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = analyze(args.result, audit_file)
    extra = feedback(args.result)
    for index, (row, detail) in enumerate(zip(report["cases"], extra["cases"], strict=True), 1):
        case = json.loads((args.result / f"case-{index:02d}.json").read_bytes())
        assert case["arms"]["reference"]["internal_candidate"]["name"] == reference_revision
        assert case["arms"]["ours"]["internal_candidate"]["name"] == revision
        row["feedback"] = detail
        row["worst_queue_context"] = {
            key: {name: queue_context(case["arms"][name], key) for name in ("reference", "ours")}
            for key in set(detail["worst"].values())
        }
        row["accepted_cache_tokens"] = {
            name: {
                role: sum(s["prefix"] for s in arm["stages"] if (s["node"] < 2) == (role == "P"))
                for role in ("P", "D")
            }
            for name, arm in case["arms"].items() if name in ("reference", "ours")
        }
    with args.output.open("x", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(json.dumps([
        {k: row[k] for k in ("rate", "p99_reduction_percent", "requests_faster")}
        for row in report["cases"]
    ]))


if __name__ == "__main__":
    main()
