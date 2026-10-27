"""Observed advice lane overlaps are accounting, not counterfactual gains."""

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from analyze_bounded_behavior import per_request


def overlap(a, b, c, d):
    return max(0.0, min(b, d) - max(a, c))


def analyze(root):
    cases = []
    for i in (1, 2, 3):
        path = root / f"case-{i:02d}.json"
        raw = json.loads(path.read_bytes())
        arms = raw["arms"]
        own = arms["ours"]
        paths = {n: per_request(arms[n]) for n in ("reference", "ours")}
        controls = {
            r["request"]: r
            for r in own["resource_events"]
            if r["operation"] == "D_control"
        }
        advice = own["decode_advice"]
        decisions = {d["id"]: d for d in own["decisions"]}
        records = []
        for r in advice:
            start = controls[r["id"]]["start"]
            affected = []
            for key, p in paths["ours"].items():
                delayed = overlap(
                    p["ready"], decisions[key]["origin"], r["origin"], r["resolved"]
                )
                if delayed:
                    affected.append({"id": key, "observed_route_wait_overlap": delayed})
            records.append(
                {
                    **r,
                    "actual_D_acquisition": start,
                    "lane_after_D_acquisition": max(
                        0, r["resolved"] - max(r["origin"], start)
                    )
                    if r["status"] == "acquired_or_completed"
                    else 0,
                    "eligible_routes_held": affected,
                }
            )
        requests = []
        for key, p in paths["ours"].items():
            requests.append(
                {
                    "id": key,
                    "path": {n: ps[key] for n, ps in paths.items()},
                    "actual_initial_route_origin": decisions[key]["origin"],
                    "observed_route_wait": decisions[key]["origin"] - p["ready"],
                    "advice_overlap": sum(
                        overlap(
                            p["ready"],
                            decisions[key]["origin"],
                            r["origin"],
                            r["resolved"],
                        )
                        for r in advice
                    ),
                }
            )
        worst = {n: max(p, key=lambda k: p[k]["TTFT"]) for n, p in paths.items()}
        cases.append(
            {
                "rate": raw["rate"],
                "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "summary": {
                    "p99_gain_percent": 100
                    * (1 - own["TTFT"]["p99"] / arms["reference"]["TTFT"]["p99"]),
                    "calls": len(advice),
                    "statuses": dict(Counter(r["status"] for r in advice)),
                    "advice_CPU_wall": own["decode_advice_CPU_wall"],
                    "lane_after_D_acquisition_NOT_net_gain": sum(
                        r["lane_after_D_acquisition"] for r in records
                    ),
                    "summed_request_route_overlap_NOT_wall_gain": sum(
                        r["advice_overlap"] for r in requests
                    ),
                    "affected_initial_routes": sum(
                        r["advice_overlap"] > 0 for r in requests
                    ),
                },
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
                    for n in paths
                },
                "worst": worst,
                "worst_aligned": {
                    key: next(r for r in requests if r["id"] == key)
                    for key in set(worst.values())
                },
                "advice": records,
                "requests": requests,
            }
        )
    return {
        "CPU_only": True,
        "fixed_observed_timeline_accounting_not_counterfactual": True,
        "cases": cases,
    }


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
                {**c["summary"], "rate": c["rate"], "worst": c["worst_aligned"]}
                for c in report["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
