"""Offline zero-control versus charged feedback; not a new policy gain estimate."""

import argparse
import hashlib
import json
from pathlib import Path

from analyze_bounded_behavior import per_request


def summarize(arm):
    return {
        "TTFT": arm["TTFT"],
        "nominal": arm["nominal"],
        "makespan": arm["makespan"],
        "control_components_not_added_to_TTFT": {
            k: arm[k]
            for k in (
                "planning_wall",
                "boundary_CPU_wall",
                "fit_CPU_wall",
                "iteration_scheduler_wall_charged",
            )
        },
        "P_distribution": [
            sum(r["p"] == n for r in arm["per_request"]) for n in (0, 1)
        ],
        "D_distribution": [
            sum(r["d"] == n for r in arm["per_request"]) for n in (2, 3)
        ],
        "release_choices": len(arm.get("slot_release_choices", [])),
    }


def compare(zero, charged):
    scopes = [json.loads((p / "scope.json").read_bytes()) for p in (zero, charged)]
    assert (
        scopes[0]["zero_control_diagnostic"]
        and not scopes[1]["zero_control_diagnostic"]
    )
    assert scopes[0]["input_sha256"] == scopes[1]["input_sha256"]
    # These pairs use the same frozen candidate package, including the parent.
    assert scopes[0]["source_sha256"] == scopes[1]["source_sha256"]
    for p in (zero, charged):
        assert (p / "complete.json").exists()
    report = {
        "CPU_only": True,
        "diagnostic_intervention": "all measured control/model/iteration overhead set to zero; calibrated RPC constants remain",
        "not_a_net_gain_or_fixed_routing_counterfactual": True,
        "limitations": "Causal arrival, route, accepted cache and conditional service samples may change. This does not isolate one overhead component or reproduce real hardware.",
        "input_sha256": scopes[0]["input_sha256"],
        "cases": [],
    }
    for i in (1, 2, 3):
        paths = [p / f"case-{i:02d}.json" for p in (zero, charged)]
        cases = [json.loads(p.read_bytes()) for p in paths]
        assert cases[0]["rate"] == cases[1]["rate"]
        assert cases[0]["config"] == cases[1]["config"]
        item = {
            "rate": cases[0]["rate"],
            "sha256": [hashlib.sha256(p.read_bytes()).hexdigest() for p in paths],
            "arms": {},
        }
        for name in ("reference", "ours"):
            arms = [case["arms"][name] for case in cases]
            assert (
                arms[0]["internal_candidate"]["name"]
                == arms[1]["internal_candidate"]["name"]
            )
            rows = [per_request(arm) for arm in arms]
            assert set(rows[0]) == set(rows[1]) and len(rows[0]) == 95
            differences = {
                key: rows[1][key]["TTFT"] - rows[0][key]["TTFT"] for key in rows[0]
            }
            decisions = [{r["id"]: r for r in arm["decisions"]} for arm in arms]
            selected = dict.fromkeys(
                [
                    max(rows[0], key=lambda k: rows[0][k]["TTFT"]),
                    max(rows[1], key=lambda k: rows[1][k]["TTFT"]),
                    *sorted(differences, key=lambda k: -abs(differences[k]))[:5],
                ]
            )
            item["arms"][name] = {
                "zero": summarize(arms[0]),
                "charged": summarize(arms[1]),
                "route_or_accepted_cache_changed": sum(
                    any(
                        rows[0][key][field] != rows[1][key][field]
                        for field in ("P", "D", "P_prefix", "D_prefix")
                    )
                    for key in rows[0]
                ),
                "paired_paths": {
                    key: {
                        "zero": rows[0][key],
                        "charged": rows[1][key],
                        "original_decisions": [d[key] for d in decisions],
                        "charged_minus_zero_components": {
                            field: rows[1][key][field] - rows[0][key][field]
                            for field in (
                                "TTFT",
                                "before_P",
                                "P_API",
                                "P_to_D",
                                "D_API_to_first",
                            )
                        },
                    }
                    for key in selected
                },
            }
        report["cases"].append(item)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zero", type=Path, required=True)
    parser.add_argument("--charged", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(compare(args.zero, args.charged), stream, indent=2)
