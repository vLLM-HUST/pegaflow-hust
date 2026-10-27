"""Retrospective companion stretch; world labels never feed the online policy."""

import argparse
import json
from pathlib import Path

import simulate_continuous_batch as sim
from release_cobatch_search import shared_step_cost


def diagnose(root):
    report = {
        "scope": "Trace-conditioned cost diagnosis, not a new policy counterfactual or hardware proof",
        "cases": [],
    }
    for index in (1, 2, 3):
        case = json.loads((root / f"case-{index:02d}.json").read_bytes())
        arm = case["arms"]["ours"]
        physical = {
            (p["request"], p["node"]): p
            for p in arm["phase_residual_samples"]
            if p["node"] >= 2
        }
        steps = {(e["node"], e["start"]): e for e in arm["batch_iterations"]}
        cpu = {
            (e["node"], e["origin"]): e["seconds"]
            for e in arm["iteration_scheduler_CPU"]
        }
        rows = []
        for q in arm["slot_release_choices"]:
            selected, node = q["selected"], q["node"]
            cohort = q.get("cohort", [selected])
            actual = {k: physical[k, node]["decode_path"] / 127 for k in q["decoded"]}
            duration = shared_step_cost(
                [actual[k] for k in cohort], case["config"]["shared_fraction"]
            )
            step = steps[node, q["origin"]]
            assert set(cohort) == {p[0] for p in step["pieces"]}
            assert (
                abs(step["end"] - step["start"] - cpu[node, q["origin"]] - duration)
                < 1e-7
            )
            remaining = {
                k: actual[k] * (128 - n) + physical[k, node]["finish_path"]
                for k, n in q["decoded"].items()
            }
            rows.append(
                {
                    "origin": q["origin"],
                    "node": node,
                    "selected": selected,
                    "cohort": cohort,
                    "blocked": q["blocked"],
                    "extra_physical_step_vs_selected_alone": duration
                    - actual[selected],
                    "actual_remaining_rank_regret": remaining[selected]
                    - min(remaining.values()),
                    "not_online_oracle": True,
                }
            )
        extra = [r["extra_physical_step_vs_selected_alone"] for r in rows]
        report["cases"].append(
            {
                "rate": case["rate"],
                "sharing": case["config"]["shared_fraction"],
                "release_steps": len(rows),
                "with_companions": sum(len(r["cohort"]) > 1 for r in rows),
                "positive_actual_stretch_steps": sum(v > 1e-10 for v in extra),
                "extra_physical_step": sim.parent.quantiles(extra) if extra else None,
                "sum_extra_device_work_not_TTFT_penalty": sum(extra),
                "rank_error": sim.parent.quantiles(
                    [r["actual_remaining_rank_regret"] for r in rows]
                )
                if rows
                else None,
                "largest_stretch_examples": sorted(
                    rows,
                    key=lambda r: r["extra_physical_step_vs_selected_alone"],
                    reverse=True,
                )[:5],
            }
        )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x", encoding="utf8") as stream:
        json.dump(diagnose(args.result), stream, indent=2)
