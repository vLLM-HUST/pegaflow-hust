"""Audit causal release decisions and separate slot waiting from gate/control work."""

import argparse
import json
from pathlib import Path

import simulate_continuous_batch as sim
from analyze_bounded_behavior import per_request


def analyze(root):
    report = {"CPU_only": True, "hardware_causality_claimed": False, "cases": []}
    for index in (1, 2, 3):
        case = json.loads((root / f"case-{index:02d}.json").read_bytes())
        item = {"rate": case["rate"], "arms": {}}
        for name in ("reference", "ours"):
            arm = case["arms"][name]
            paths = per_request(arm)
            p = {s["id"]: s for s in arm["stages"] if s["node"] < 2}
            d = {s["id"]: s for s in arm["stages"] if s["node"] >= 2}
            control = {
                e["request"]: e
                for e in arm["resource_events"]
                if e["operation"] == "D_control"
            }
            choices = arm.get("slot_release_choices", [])
            for e in choices:
                origin, node = e["origin"], e["node"]
                assert e["model_latest"] <= origin
                assert len(e["remaining_estimates"]) == 4
                assert e["selected"] == min(
                    e["remaining_estimates"], key=e["remaining_estimates"].get
                )
                for key in e["remaining_estimates"]:
                    assert d[key]["node"] == node
                    assert paths[key]["first"] <= origin < d[key]["completed"]
                    assert 1 <= e["decoded"][key] < 128
                assert e["blocked"]
                for key in e["blocked"]:
                    assert p[key]["completed"] <= origin < control[key]["start"]
            rows = []
            for key, path in paths.items():
                c, ds = control[key], d[key]
                slot = c["start"] - p[key]["completed"]
                ctl = c["end"] - c["start"]
                gate = ds["completed"] - ds["full"] - c["end"]
                assert min(slot, ctl, gate) >= -1e-7
                assert abs(slot + ctl + gate - path["P_to_D"]) < 1e-7
                rows.append(
                    {
                        "request": key,
                        "D_node": ds["node"],
                        "TTFT": path["TTFT"],
                        "slot_wait": slot,
                        "control": ctl,
                        "gate_copy_wait": gate,
                        "D_API_first": path["D_API_to_first"],
                        "ready_full": ds["completed"] - path["ready"],
                        "post_first_full": ds["full"] - ds["first"],
                    }
                )
            item["arms"][name] = {
                "TTFT": arm["TTFT"],
                "nominal": arm["nominal"],
                "makespan": arm["makespan"],
                "release_choices_audited": len(choices),
                "release_jobs": len({e["selected"] for e in choices}),
                "waiters_served_by_rule": len(
                    {k for e in choices for k in e["blocked"]}
                ),
                "metrics": {
                    field: sim.parent.quantiles([r[field] for r in rows])
                    for field in (
                        "slot_wait",
                        "control",
                        "gate_copy_wait",
                        "D_API_first",
                        "ready_full",
                        "post_first_full",
                    )
                },
                "rows": rows,
            }
        report["cases"].append(item)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(analyze(args.result), stream, indent=2)
