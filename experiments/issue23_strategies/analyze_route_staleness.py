"""Observe new D predecessors after route commitment, not an oracle routing gain."""

import argparse
import hashlib
import json
from pathlib import Path

from analyze_bounded_behavior import per_request


def analyze_arm(arm):
    paths = per_request(arm)
    decisions = {r["id"]: r for r in arm["decisions"]}
    stages = {(r["id"], r["node"] < 2): r for r in arm["stages"]}
    controls = [r for r in arm["resource_events"] if r["operation"] == "D_control"]
    assert len(controls) == len({r["request"] for r in controls}) == 95
    controls = {r["request"]: r for r in controls}
    rows = []
    for key, decision in decisions.items():
        origin = decision["origin"]
        published = stages[key, True]["completed"]
        target = controls[key]
        assert origin <= published <= target["start"]
        inventories = {}
        for node in (2, 3):
            holders, waiting = [], []
            for other, event in controls.items():
                if other == key or event["node"] != node:
                    continue
                other_p = stages[other, True]["completed"]
                finish = stages[other, False]["completed"]
                row = {
                    "id": other,
                    "route_origin": decisions[other]["origin"],
                    "P_done": other_p,
                    "D_admit": event["start"],
                    "full_end": finish,
                    "admitted_after_target_route": decisions[other]["origin"] > origin,
                }
                if event["start"] <= published < finish:
                    holders.append(row)
                elif other_p <= published < event["start"]:
                    waiting.append(row)
            assert len(holders) <= arm["batch_config"]["max_sequences"]
            inventories[node] = {"holders": holders, "waiting": waiting}
        current = inventories[target["node"]]
        later_predecessors = [
            r
            for r in (*current["holders"], *current["waiting"])
            if r["admitted_after_target_route"] and r["D_admit"] < target["start"]
        ]
        forecast = arm["initial_cache_forecasts"][key]
        predicted = [
            r
            for r in forecast["predicted_API_intervals"]
            if r["key"] == "__candidate__" and r["operation"] == "D_control"
        ]
        assert len(predicted) <= 1
        rows.append(
            {
                "id": key,
                "route_origin": origin,
                "P_done": published,
                "binding_age_at_P_done": published - origin,
                "D": target["node"],
                "actual_D_admit": target["start"],
                "P_done_to_D_admit": target["start"] - published,
                "predicted_D_admit_at_route": predicted[0]["start"]
                if predicted
                else None,
                "D_admit_endpoint_residual": target["start"] - predicted[0]["start"]
                if predicted
                else None,
                "later_predecessors": later_predecessors,
                "observed_slots_at_P_done": inventories,
                "path": paths[key],
            }
        )
    assert len(rows) == 95
    exposed = [r for r in rows if r["later_predecessors"]]
    idle_alternate = [
        r
        for r in exposed
        if not r["observed_slots_at_P_done"][5 - r["D"]]["holders"]
        and not r["observed_slots_at_P_done"][5 - r["D"]]["waiting"]
    ]
    return {
        "later_predecessor_exposed_requests": len(exposed),
        "exposed_with_empty_alternate_at_P_done": len(idle_alternate),
        "exposed_wait_sum_not_TTFT_gain": sum(r["P_done_to_D_admit"] for r in exposed),
        "top_exposed_by_TTFT": [
            r["id"] for r in sorted(exposed, key=lambda r: -r["path"]["TTFT"])[:5]
        ],
        "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert (args.result / "complete.json").exists()
    report = {
        "CPU_only": True,
        "net_gain_claim": False,
        "limitations": "Observed after-route arrivals are offline explanations, not known future policy inputs. Alternate emptiness does not price H2D, copy, compute or new planning. Changing assignment changes future states; wait totals are not recoverable gains.",
        "cases": [],
    }
    for i in (1, 2, 3):
        p = args.result / f"case-{i:02d}.json"
        case = json.loads(p.read_bytes())
        report["cases"].append(
            {
                "rate": case["rate"],
                "raw_sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                "arms": {
                    name: analyze_arm(case["arms"][name])
                    for name in ("reference", "ours")
                },
            }
        )
    with args.output.open("x", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
