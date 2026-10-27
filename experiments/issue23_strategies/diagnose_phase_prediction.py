"""Offline forecast-to-world phase check, excluding changed node/cache paths.

World phase labels are diagnostic evidence only, never online estimator inputs.
The first-service comparison excludes queue residence and batching interference.
"""

import argparse
import json
from pathlib import Path


def analyze(root):
    rows = []
    for i in (1, 2, 3):
        case = json.loads((root / f"case-{i:02d}.json").read_bytes())
        for name in ("reference", "ours"):
            arm = case["arms"][name]
            world = {
                (s["request"], s["node"]): s for s in arm["phase_residual_samples"]
            }
            for key, quote in arm["initial_cache_forecasts"].items():
                for predicted in quote["effective_API"]:
                    actual = world.get((key, predicted["node"]))
                    matching = (
                        actual is not None
                        and actual["prefix"] == predicted["prefix_tokens"]
                    )
                    rows.append(
                        {
                            "rate": case["rate"],
                            "arm": name,
                            "request": key,
                            "node": predicted["node"],
                            "origin": quote["origin"],
                            "model_latest": quote["latest_completed"],
                            "original_effective_API": predicted,
                            "same_node_and_accepted_prefix": matching,
                            "actual_world_sample_for_diagnosis_only": actual,
                            "first_service_residual": actual["first_total"]
                            - predicted["first_service_seconds"]
                            if matching
                            else None,
                        }
                    )
    matched = [r for r in rows if r["same_node_and_accepted_prefix"]]
    return {
        "CPU_only": True,
        "not_online_world_oracle": True,
        "not_queue_error_or_counterfactual_gain": True,
        "rows": rows,
        "matched": len(matched),
        "excluded": len(rows) - len(matched),
        "largest_absolute_residuals": sorted(
            matched, key=lambda r: abs(r["first_service_residual"]), reverse=True
        )[:20],
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--result", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    result = analyze(a.result)
    with a.output.open("x", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(
        json.dumps(
            {
                "matched": result["matched"],
                "excluded": result["excluded"],
                "largest": [
                    {
                        k: r[k]
                        for k in (
                            "rate",
                            "arm",
                            "request",
                            "node",
                            "first_service_residual",
                        )
                    }
                    for r in result["largest_absolute_residuals"][:6]
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
