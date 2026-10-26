"""Validate the simulator against an explicitly supplied numerical evidence directory."""

import argparse
import hashlib
import math
from pathlib import Path

from step_simulator import compare, fit_cost_model, predict, read_json, read_rows


def verify(root):
    root = Path(root)
    manifest = read_json(root / "manifest.json")
    for member in manifest["members"]:
        path = (root / member["path"]).resolve()
        if not path.is_relative_to(root.resolve()):
            raise ValueError("Evidence member escapes the selected root")
        raw = path.read_bytes()
        if len(raw) != member["bytes"] or hashlib.sha256(raw).hexdigest() != member["sha256"]:
            raise ValueError(f"Evidence member differs: {member['path']}")
    metadata = read_json(root / "data/calibration-metadata.json")
    frames = read_rows(root / "data/calibration-steps.jsonl.gz")
    if {row["case"] for row in frames} != set(metadata["training_cases"]):
        raise ValueError("Unexpected calibration cases")
    fitted = fit_cost_model(frames, metadata)
    published = read_json(root / "cost-model.json")
    for kind in ("decode", "prefill"):
        for actual, expected in zip(
            fitted["models"][kind]["coefficients"],
            published["models"][kind]["coefficients"],
            strict=True,
        ):
            if not math.isclose(actual, expected, rel_tol=1e-7, abs_tol=1e-10):
                raise ValueError("Published cost model differs from calibration fit")
    results = {}
    for name in ("private", "shared"):
        workload = read_json(root / f"data/validation-95-{name}-workload.json.gz")
        observed = read_json(root / f"data/validation-95-{name}-observed.json.gz")
        if set(metadata["training_tasks"]) & {row["task"] for row in workload["requests"]}:
            raise ValueError("Calibration and validation tasks overlap")
        prediction = predict(workload, fitted)
        result = compare(prediction, observed)
        for key in ("TTFT_mean_s", "TTFT_p95_s", "finish_span_s"):
            if abs(result["relative_error_percent"][key]) >= 10:
                raise ValueError(f"Development validation timing regression: {name}/{key}")
        for key in ("cache_hit_tokens", "peak_active_KV_blocks"):
            if result["measured"][key] != result["predicted"][key]:
                raise ValueError(f"Development validation state regression: {name}/{key}")
        stored = read_json(root / f"{name}-prediction.json.gz")
        for actual, expected in zip(prediction["requests"], stored["requests"], strict=True):
            if actual["id"] != expected["id"]:
                raise ValueError("Stored prediction identities differ")
            for key in ("first_s", "finish_s"):
                if not math.isclose(actual[key], expected[key], rel_tol=1e-7, abs_tol=1e-8):
                    raise ValueError("Stored prediction timing differs")
        results[name] = result["relative_error_percent"]
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    import json

    print(json.dumps(verify(args.root), indent=2))


if __name__ == "__main__":
    main()
