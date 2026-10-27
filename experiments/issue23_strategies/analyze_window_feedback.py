"""Trace-conditioned release-window diagnostics; never a policy gain oracle."""

import argparse
import hashlib
import json
from pathlib import Path

from analyze_advice_chronology import analyze as chronology


def analyze(root):
    report = chronology(root, json.loads((root / "charged-feedback.json").read_bytes()))
    for i, summary in enumerate(report["cases"], 1):
        path = root / f"case-{i:02d}.json"
        arm = json.loads(path.read_bytes())["arms"]["ours"]
        controls = {
            r["request"]: r
            for r in arm["resource_events"]
            if r["operation"] == "D_control"
        }
        advice = {r["id"]: r for r in arm["decode_advice"]}
        decisions = {r["id"]: r for r in arm["decisions"]}
        triggers = arm["advice_trigger_records"]
        rows = []
        for q in triggers:
            if not q["release_estimates"]:
                continue
            actual = controls[q["id"]]["start"]
            moved = advice.get(q["id"], {}).get("status") == "move"
            # Reconstruct only actually started operations. These observations
            # explain a trace, not the predictor's online information set.
            overlaps = []
            ledger = arm["resource_operation_ledger"]
            for e in ledger:
                if e["event"] != "start" or f"gate{q['node']}" not in e["resources"]:
                    continue
                if e["operation"] not in ("history", "handoff"):
                    continue
                ends = [
                    v["seconds"]
                    for v in ledger
                    if v["event"] == "finish"
                    and v["key"] == e["key"]
                    and v["operation"] == e["operation"]
                ]
                if not ends or max(ends) <= q["origin"] or e["seconds"] >= actual:
                    continue
                d = decisions[e["key"]]
                overlaps.append(
                    {
                        "operation": e,
                        "finish": max(ends),
                        "initial_route": d,
                        "route_CPU_done_before_trigger": d["origin"] + d["CPU_wall"]
                        <= q["origin"],
                    }
                )
            estimate = min(q["release_estimates"].values())
            rows.append(
                {
                    "trigger": q,
                    "same_destination": not moved,
                    "actual_D_acquisition": actual,
                    "actual_acquisition_minus_origin": actual - q["origin"],
                    "prediction_minus_actual": q["origin"] + estimate - actual,
                    "source_gate_copy_overlaps": overlaps,
                    "actual_acquisition_after_estimated_search": actual
                    > q["origin"] + q["predicted_CPU"] + q["RPC_seconds"],
                }
            )
        skips = [q for q in triggers if q["skip"]]
        first_trigger = triggers[0] if triggers else None
        first_skip = skips[0] if skips else None
        first_diff = summary["first_initial_route_difference"]
        errors = [r["prediction_minus_actual"] for r in rows if r["same_destination"]]
        summary.update(
            raw_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            first_trigger=first_trigger,
            first_skip=first_skip,
            initial_difference_precedes_first_trigger=(
                all(
                    v["origin"] < first_trigger["origin"]
                    for v in first_diff["records"].values()
                )
                if first_diff and first_trigger
                else None
            ),
            initial_difference_precedes_first_skip=(
                all(
                    v["origin"] < first_skip["origin"]
                    for v in first_diff["records"].values()
                )
                if first_diff and first_skip
                else None
            ),
            trigger_count=len(triggers),
            skip_count=len(skips),
            window_rows=rows,
            same_destination_window_error={
                "n": len(errors),
                "bias": sum(errors) / len(errors) if errors else None,
                "MAE": sum(map(abs, errors)) / len(errors) if errors else None,
            },
        )
    report["release_errors_include_control_and_future_interference"] = True
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.result)
    with args.output.open("x", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(
        json.dumps(
            [
                {
                    k: r[k]
                    for k in (
                        "rate",
                        "p99_reduction_percent",
                        "trigger_count",
                        "skip_count",
                        "initial_difference_precedes_first_trigger",
                        "initial_difference_precedes_first_skip",
                        "same_destination_window_error",
                    )
                }
                for r in result["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
