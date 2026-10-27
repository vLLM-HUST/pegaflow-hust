"""Pair fully charged numerical-calendar traces without claiming causality."""

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from analyze_bounded_behavior import per_request


def analyze(root, activation_file="numerical-activation-audit.json"):
    scope = json.loads((root / "scope.json").read_bytes())
    assert not scope["zero_control_diagnostic"]
    assert not scope["fixed_control_diagnostic"]
    activation = json.loads((root / activation_file).read_bytes())
    assert activation["all_checks_passed"]
    report = {
        "CPU_only": True,
        "all_measured_control_charged": True,
        "trace_accounting_NOT_causal_ablation": True,
        "single_exposed_replay_p99_is_max": True,
        "decode_sharing_fraction": scope["decode_sharing_fraction"],
        "scope_sha256": hashlib.sha256((root / "scope.json").read_bytes()).hexdigest(),
        "cases": [],
    }
    parts = ("before_P", "P_API", "P_to_D", "D_API_to_first")
    for index in (1, 2, 3):
        path = root / f"case-{index:02d}.json"
        case = json.loads(path.read_bytes())
        arms = case["arms"]
        paths = {name: per_request(arms[name]) for name in ("reference", "ours")}
        assert set(paths["reference"]) == set(paths["ours"])
        assert len(paths["ours"]) == 95
        costs = {}
        for name in paths:
            arm = arms[name]
            search = {}
            for kind in ("route", "boundary"):
                records = [r for r in arm["search_records"] if r["kind"] == kind]
                counts = {
                    key: sum(r.get(key, 0) for r in records)
                    for key in (
                        "considered",
                        "full",
                        "calendar_full",
                        "calendar_reused",
                        "refresh_full",
                        "refresh_reused",
                        "bound_checks",
                        "pruned_calendars",
                    )
                }
                wall = arm["planning_wall" if kind == "route" else "boundary_CPU_wall"]
                search[kind] = {
                    "calls": len(records),
                    "counts": counts,
                    "measured_wall": wall,
                    "wall_per_full_calendar_including_other_search_work": wall
                    / counts["calendar_full"]
                    if counts["calendar_full"]
                    else None,
                }
            costs[name] = {
                "TTFT": arm["TTFT"],
                "nominal": arm["nominal"],
                "makespan": arm["makespan"],
                "search": search,
                "D_advice_calls": len(arm["decode_advice"]),
                "D_advice_status": dict(
                    Counter(r["status"] for r in arm["decode_advice"])
                ),
                "D_advice_wall": arm["decode_advice_CPU_wall"],
                "P_revalidation_calls": len(arm["prefill_revalidation"]),
                "P_distribution": dict(Counter(r["p"] for r in arm["per_request"])),
                "D_distribution": dict(Counter(r["d"] for r in arm["per_request"])),
            }
        aligned = []
        for key in sorted(paths["reference"]):
            a, b = (paths[name][key] for name in ("reference", "ours"))
            # This checks accounting only; it is not an output-equality tolerance.
            for row in (a, b):
                assert abs(sum(row[k] for k in parts) - row["TTFT"]) < 1e-9
            aligned.append(
                {
                    "id": key,
                    "reference": a,
                    "ours": b,
                    "TTFT_delta": b["TTFT"] - a["TTFT"],
                    "stage_delta": {k: b[k] - a[k] for k in parts},
                }
            )
        aligned.sort(key=lambda row: row["TTFT_delta"], reverse=True)
        report["cases"].append(
            {
                "rate": case["rate"],
                "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "costs": costs,
                "p99_reduction_percent": 100
                * (
                    1 - costs["ours"]["TTFT"]["p99"] / costs["reference"]["TTFT"]["p99"]
                ),
                "requests_faster": sum(row["TTFT_delta"] < 0 for row in aligned),
                "largest_regressions": aligned[:5],
                "largest_improvements": aligned[-5:],
                "all_aligned_requests": aligned,
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
                    key: row[key]
                    for key in ("rate", "p99_reduction_percent", "requests_faster")
                }
                for row in report["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
