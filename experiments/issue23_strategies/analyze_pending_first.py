"""First-token improvement must not hide removed decode or deferred final drain."""

import argparse
import json
from collections import Counter
from pathlib import Path

import simulate_continuous_batch as sim
from analyze_bounded_behavior import per_request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = {
        "scope": "CPU work conservation and full-release tradeoff; not hardware or ITL validation",
        "cases": [],
    }
    for i in (1, 2, 3):
        case = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
        row = {"rate": case["rate"], "arms": {}}
        for name, arm in case["arms"].items():
            paths = per_request(arm)
            stages = {r["id"]: r for r in arm["stages"] if r["node"] >= 2}
            counts, phases = Counter(), Counter()
            for e in arm["batch_iterations"]:
                for key, count, phase in e["pieces"]:
                    counts[key, phase] += count
                    phases[key, phase] += 1
            for key, stage in stages.items():
                assert counts[key, "prefill"] == stage["tokens"] - stage["prefix"]
                assert counts[key, "decode"] == 127
                assert phases[key, "restore"] == phases[key, "save"] == 1
            release = [r["end"] - paths[r["id"]]["ready"] for r in arm["per_request"]]
            after_first = [r["full"] - r["first"] for r in stages.values()]
            row["arms"][name] = {
                "TTFT": arm["TTFT"],
                "nominal": arm["nominal"],
                "ready_to_full_completion": sim.parent.quantiles(release),
                "D_first_to_full_completion": sim.parent.quantiles(after_first),
                "makespan_with_final_drain": arm["makespan"],
                "routing_wall": arm["planning_wall"],
                "boundary_wall": arm["boundary_CPU_wall"],
                "fit_wall": arm["fit_CPU_wall"],
                "iteration_wall": arm["iteration_scheduler_wall_charged"],
                "complete_decode_and_prefill_work_conserved": True,
            }
        report["cases"].append(row)
    with args.output.open("x", encoding="utf8") as stream:
        json.dump(report, stream, indent=2)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "checked_arms": sum(len(c["arms"]) for c in report["cases"]),
            }
        )
    )


if __name__ == "__main__":
    main()
