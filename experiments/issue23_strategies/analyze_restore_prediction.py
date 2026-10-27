"""Past-only estimator at D-control start, conditioned on actual accepted prefix.

Accepted prefix is a post-hoc diagnostic covariate, not the initial route's
prediction or an online oracle. Exclusive restore timers include the backend
and synchronization path; they are not pure DMA bandwidth measurements.
"""

import argparse
import hashlib
import json
import statistics
from pathlib import Path

import simulate_continuous_batch as sim
from completed_phase_search import CompletedPhaseModels


def analyze(root, inputs, seed):
    data = json.loads(inputs.read_bytes())
    values = dict(data["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**v) for v in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    initial = sim.Costs(**values)
    cases = []
    for i in (1, 2, 3):
        path = root / f"case-{i:02d}.json"
        raw = json.loads(path.read_bytes())
        arms = {}
        for name in ("reference", "ours"):
            arm = raw["arms"][name]
            model = CompletedPhaseModels(
                sim.parent.core.seed_operations(initial, seed, "phase")
            )
            history = iter(arm["exclusive_D_observations"])
            next_row = next(history, None)
            rows = []
            stages = {r["id"]: r for r in arm["stages"] if r["node"] >= 2}
            controls = sorted(
                (r for r in arm["resource_events"] if r["operation"] == "D_control"),
                key=lambda r: r["start"],
            )
            for event in controls:
                origin = event["start"]
                while next_row is not None and next_row["available"] <= origin:
                    model.observe_exclusive(next_row)
                    next_row = next(history, None)
                costs = model.freeze(origin)
                stage = stages[event["request"]]
                fit = costs.phase_fits[event["node"] - 2][0]
                # Otherwise the fallback depends on base-stage updates not
                # reconstructed by this narrowly scoped diagnostic.
                assert not stage["prefix"] or fit["coefficients"] is not None
                assert model.phase_latest <= origin
                predicted = costs.restore(event["node"], stage["prefix"])
                actual = stage["restore_path_not_pure_H2D"]
                rows.append(
                    {
                        "id": event["request"],
                        "node": event["node"],
                        "origin": origin,
                        "accepted_prefix_diagnostic_only": stage["prefix"],
                        "predicted_restore": predicted,
                        "actual_restore": actual,
                        "error_predicted_minus_actual": predicted - actual,
                        "phase_latest": model.phase_latest,
                        "fit": fit,
                    }
                )
            grouped = {}
            for node in (2, 3):
                selected = [
                    r
                    for r in rows
                    if r["node"] == node and r["accepted_prefix_diagnostic_only"]
                ]
                if not selected:
                    grouped[str(node)] = {"n": 0}
                    continue
                errors = [r["error_predicted_minus_actual"] for r in selected]
                grouped[str(node)] = {
                    "n": len(selected),
                    "MAE": statistics.mean(map(abs, errors)),
                    "bias": statistics.mean(errors),
                    "worst_underprediction": min(errors),
                    "actual_sum": sum(r["actual_restore"] for r in selected),
                    "predicted_sum": sum(r["predicted_restore"] for r in selected),
                    "largest_errors": sorted(
                        selected, key=lambda r: r["error_predicted_minus_actual"]
                    )[:3],
                }
            arms[name] = {"nodes": grouped, "requests": rows}
        cases.append(
            {
                "rate": raw["rate"],
                "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "arms": arms,
            }
        )
    return {
        "CPU_only": True,
        "not_original_route_forecasts": True,
        "posthoc_accepted_prefix_not_policy_input": True,
        "cases": cases,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.result, args.inputs, args.seed)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
    print(
        json.dumps(
            [
                {
                    "rate": c["rate"],
                    "nodes": {
                        n: {
                            node: {
                                k: v for k, v in row.items() if k != "largest_errors"
                            }
                            for node, row in a["nodes"].items()
                        }
                        for n, a in c["arms"].items()
                    },
                }
                for c in result["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
