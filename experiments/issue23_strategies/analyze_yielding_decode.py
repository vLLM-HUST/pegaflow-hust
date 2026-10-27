"""Align four completed CPU arms; observed overlaps are not causal speedups."""

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from analyze_bounded_behavior import per_request
from analyze_progress_decode import overlap


def control_summary(arm, paths):
    advice = arm.get("decode_advice", [])
    decisions = {r["id"]: r for r in arm["decisions"]}
    controls = {
        r["request"]: r["start"]
        for r in arm["resource_events"]
        if r["operation"] == "D_control"
    }
    waits = {}
    for key, path in paths.items():
        stop = decisions[key]["origin"]
        waits[key] = {
            "ready_to_route": stop - path["ready"],
            "advice_overlap": sum(
                overlap(path["ready"], stop, r["origin"], r["resolved"]) for r in advice
            ),
        }
    after_acquisition = sum(
        max(0, r["resolved"] - max(r["origin"], controls[r["id"]]))
        for r in advice
        if r["status"] == "acquired_or_completed"
    )
    return {
        "calls": len(advice),
        "statuses": dict(Counter(r["status"] for r in advice)),
        "advice_CPU_wall": arm.get("decode_advice_CPU_wall", 0),
        "quanta": sum(len(r.get("quanta", [])) for r in advice),
        "partial_searches": sum(r.get("completed_search") is False for r in advice),
        "lane_after_acquisition_NOT_recoverable_gain": after_acquisition,
        "route_overlap_request_seconds_NOT_wall_gain": sum(
            v["advice_overlap"] for v in waits.values()
        ),
        "affected_initial_routes": sum(v["advice_overlap"] > 0 for v in waits.values()),
        "request_waits": waits,
    }


def analyze(root):
    cases = []
    for index in (1, 2, 3):
        source = root / f"case-{index:02d}.json"
        raw = json.loads(source.read_bytes())
        arms = raw["arms"]
        assert set(arms) == {"native", "reference", "parent", "ours"}
        paths = {name: per_request(arm) for name, arm in arms.items()}
        keys = set(paths["ours"])
        assert len(keys) == 95 and all(set(p) == keys for p in paths.values())
        controls = {
            name: control_summary(arms[name], paths[name])
            for name in ("reference", "parent", "ours")
        }
        worst = {
            name: max(p, key=lambda key: p[key]["TTFT"]) for name, p in paths.items()
        }
        pairs = {}
        for name in ("reference", "parent"):
            old, own = paths[name], paths["ours"]
            differences = {k: own[k]["TTFT"] - old[k]["TTFT"] for k in keys}
            rows = []
            for key in sorted(keys):
                delta = {
                    field: own[key][field] - old[key][field]
                    for field in ("before_P", "P_API", "P_to_D", "D_API_to_first")
                }
                assert abs(sum(delta.values()) - differences[key]) < 1e-7
                rows.append(
                    {
                        "id": key,
                        "TTFT_delta": differences[key],
                        "component_delta": delta,
                    }
                )
            pairs[name] = {
                "p99_reduction_percent": 100
                * (1 - arms["ours"]["TTFT"]["p99"] / arms[name]["TTFT"]["p99"]),
                "faster_requests": sum(v < 0 for v in differences.values()),
                "changed_P": sum(old[k]["P"] != own[k]["P"] for k in keys),
                "changed_D": sum(old[k]["D"] != own[k]["D"] for k in keys),
                "changed_P_accepted_prefix": sum(
                    old[k]["P_prefix"] != own[k]["P_prefix"] for k in keys
                ),
                "changed_D_accepted_prefix": sum(
                    old[k]["D_prefix"] != own[k]["D_prefix"] for k in keys
                ),
                "requests": rows,
            }
        cases.append(
            {
                "rate": raw["rate"],
                "raw_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "metrics": {
                    name: {
                        field: arm[field]
                        for field in (
                            "TTFT",
                            "nominal",
                            "makespan",
                            "planning_wall",
                            "boundary_CPU_wall",
                        )
                    }
                    for name, arm in arms.items()
                },
                "control": controls,
                "comparisons": pairs,
                "worst": worst,
                "worst_aligned": {
                    key: {name: p[key] for name, p in paths.items()}
                    for key in set(worst.values())
                },
                "all_request_paths": paths,
            }
        )
    return {
        "CPU_only": True,
        "real_parallel_serving_validated": False,
        "observed_timeline_accounting_not_counterfactual": True,
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
                {
                    "rate": case["rate"],
                    "p99": {
                        name: m["TTFT"]["p99"] for name, m in case["metrics"].items()
                    },
                    "reductions": {
                        name: p["p99_reduction_percent"]
                        for name, p in case["comparisons"].items()
                    },
                    "control": {
                        name: {k: v for k, v in r.items() if k != "request_waits"}
                        for name, r in case["control"].items()
                    },
                }
                for case in report["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
