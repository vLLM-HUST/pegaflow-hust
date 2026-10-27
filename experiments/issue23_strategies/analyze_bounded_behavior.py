"""Observed causal components, not an oracle or a pure queue attribution."""

import json
import sys
from collections import defaultdict
from pathlib import Path


def per_request(arm):
    stages = {(s["id"], "P" if s["node"] < 2 else "D"): s for s in arm["stages"]}
    rows = {}
    for r in arm["per_request"]:
        p, d = (stages[r["id"], role] for role in ("P", "D"))
        first = d["completed"] - d["full"] + d["first"]
        ready = first - r["TTFT"]
        parts = {
            "before_P": p["completed"] - p["full"] - ready,
            "P_API": p["full"],
            "P_to_D": d["completed"] - d["full"] - p["completed"],
            "D_API_to_first": d["first"],
        }
        assert abs(sum(parts.values()) - r["TTFT"]) < 1e-7
        rows[r["id"]] = dict(
            TTFT=r["TTFT"],
            nominal=r["nominal"],
            dependency=r["dependency"],
            P=p["node"],
            D=d["node"],
            P_prefix=p["prefix"],
            D_prefix=d["prefix"],
            ready=ready,
            first=first,
            **parts,
        )
    return rows


def main(root):
    report = {
        "scope": "Observed path decomposition; overlapping batch sums are not added to TTFT; neither pure queue nor causal hardware proof",
        "cases": [],
    }
    for i in (1, 2, 3):
        raw = json.loads((root / f"case-{i:02d}.json").read_bytes())
        ref, new = (per_request(raw["arms"][name]) for name in ("reference", "ours"))
        differences = {key: new[key]["TTFT"] - ref[key]["TTFT"] for key in ref}
        worst_new = max(new, key=lambda k: new[k]["TTFT"])
        worst_ref = max(ref, key=lambda k: ref[k]["TTFT"])
        selected = dict.fromkeys(
            (
                worst_new,
                worst_ref,
                *sorted(differences, key=differences.get, reverse=True)[:3],
            )
        )
        paired = {
            k: {
                "reference": ref[k],
                "candidate": new[k],
                "component_delta": {
                    field: new[k][field] - ref[k][field]
                    for field in ("before_P", "P_API", "P_to_D", "D_API_to_first")
                },
                "TTFT_delta": differences[k],
            }
            for k in selected
        }
        nodes = {}
        for name in ("reference", "ours"):
            arm = raw["arms"][name]
            sums = defaultdict(float)
            for event in arm["batch_iterations"]:
                sums[f"D{event['node'] - 2}:{event['kind']}"] += (
                    event["end"] - event["start"]
                )
            nodes[name] = {
                "batch_seconds": dict(sums),
                "accepted_prefix_tokens": {
                    role: sum(
                        s["prefix"]
                        for s in arm["stages"]
                        if (s["node"] < 2) == (role == "P")
                    )
                    for role in ("P", "D")
                },
            }
        report["cases"].append(
            {
                "rate": raw["rate"],
                "worst_reference": worst_ref,
                "worst_candidate": worst_new,
                "candidate_faster_requests": sum(v < 0 for v in differences.values()),
                "changed_P_assignments": sum(ref[k]["P"] != new[k]["P"] for k in ref),
                "changed_D_assignments": sum(ref[k]["D"] != new[k]["D"] for k in ref),
                "changed_D_accepted_prefix": sum(
                    ref[k]["D_prefix"] != new[k]["D_prefix"] for k in ref
                ),
                "nodes": nodes,
                "paired": paired,
            }
        )
    return report


if __name__ == "__main__":
    report = main(Path(sys.argv[1]))
    with Path(sys.argv[2]).open("x") as stream:
        json.dump(report, stream, indent=2)
    print(json.dumps({"diagnostic": str(sys.argv[2]), "cases": len(report["cases"])}))
