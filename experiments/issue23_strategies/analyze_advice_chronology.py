"""Align advisory interventions with the first changed initial routing decision.

This is trace evidence, not a paired causal estimate of a specific move's gain.
"""

import argparse
import hashlib
import json
from pathlib import Path


def analyze(root, feedback):
    cases = []
    for i, summary in enumerate(feedback["cases"], 1):
        path = root / f"case-{i:02d}.json"
        raw = json.loads(path.read_bytes())
        ref, ours = raw["arms"]["reference"], raw["arms"]["ours"]
        decisions = {
            name: {d["id"]: d for d in arm["decisions"]}
            for name, arm in (("reference", ref), ("ours", ours))
        }
        route_fields = ("p", "d", "source", "P_prefix_command", "D_prefix_command")
        first_difference = None
        for d in ref["decisions"]:
            a, b = d["initial_forecast"], decisions["ours"][d["id"]]["initial_forecast"]
            if any(a[k] != b[k] for k in route_fields):
                first_difference = {
                    "id": d["id"],
                    "records": {n: v[d["id"]] for n, v in decisions.items()},
                }
                break
        advice = ref["decode_advice"]
        first_advice = (
            {k: advice[0][k] for k in ("id", "origin", "status")} if advice else None
        )
        moves = []
        for r in advice:
            if r["status"] != "move":
                continue
            keep, *alternatives = r["evaluations"]
            chosen = min(
                (v for v in alternatives if v["d"] == r["selected"]),
                key=lambda v: (v["objective"], v["payload"], v["prefix_blocks"]),
            )
            moves.append(
                {
                    "id": r["id"],
                    "origin": r["origin"],
                    "keep_first": keep["first"],
                    "revised_first": chosen["first"],
                    "local_max_before": keep["objective"][0],
                    "local_max_after": chosen["objective"][0],
                    "CPU_wall": r["CPU_wall"],
                    "RPC_seconds": r["RPC_seconds"],
                }
            )
        item = {
            k: summary[k]
            for k in (
                "rate",
                "p99_reduction_percent",
                "requests_faster",
                "costs",
                "accepted_cache_tokens",
                "all_aligned_requests",
            )
        }
        item.update(
            raw_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            first_advice=first_advice,
            first_initial_route_difference=first_difference,
            difference_precedes_first_advice=(
                all(
                    v["origin"] < first_advice["origin"]
                    for v in first_difference["records"].values()
                )
                if first_advice and first_difference
                else None
            ),
            committed_moves=moves,
            worst_paths=summary["feedback"]["aligned"],
        )
        cases.append(item)
    return {"CPU_only": True, "not_a_causal_move_gain_estimate": True, "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("result", "feedback", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    report = analyze(args.result, json.loads(args.feedback.read_bytes()))
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    print(
        json.dumps(
            [
                {
                    "rate": r["rate"],
                    "gain": r["p99_reduction_percent"],
                    "first_route_difference_precedes_advice": r[
                        "difference_precedes_first_advice"
                    ],
                }
                for r in report["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
