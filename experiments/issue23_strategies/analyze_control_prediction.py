"""Offline control-release prediction audit; no fitted policy or gain claim.

Route predictions are the original dispatch quotes. D-boundary predictions are
reconstructed from the observed decision state and only already completed CPU
samples, because the old traces did not save that quote explicitly. Search-event
counts are post-hoc explanatory measurements, never proposed online features.
"""

import argparse
import hashlib
import json
import math
import statistics
from pathlib import Path

import simulate_continuous_batch  # noqa: F401 - bind retained overlay
from analyze_bounded_behavior import per_request
from control_work_search import fit_control


def metrics(rows):
    errors = [r["predicted_seconds"] - r["actual_seconds"] for r in rows]
    return {
        "n": len(rows),
        "MAE": statistics.mean(abs(v) for v in errors),
        "RMSE": math.sqrt(statistics.mean(v * v for v in errors)),
        "bias_predicted_minus_actual": statistics.mean(errors),
        "sum_predicted": sum(r["predicted_seconds"] for r in rows),
        "sum_actual": sum(r["actual_seconds"] for r in rows),
        "max_underprediction": -min(errors),
    }


def correlation(rows, field):
    x = [r[field] for r in rows]
    y = [r["actual_seconds"] for r in rows]
    return statistics.correlation(x, y) if len(set(x)) > 1 and len(set(y)) > 1 else None


def analyze_arm(arm):
    observations = arm["control_work_observations"]
    paths = per_request(arm)
    decisions = {r["id"]: r for r in arm["decisions"]}
    search = {(r["key"], r["kind"]): r for r in arm["search_records"]}
    boundaries = [r for r in arm["resource_events"] if r["operation"] == "D_control"]
    assert len(boundaries) == len({r["request"] for r in boundaries}) == 95
    boundaries = {r["request"]: r for r in boundaries}
    rows = {"route": [], "boundary": []}
    for kind, collected in rows.items():
        for index, observation in enumerate(observations[kind]):
            key = (
                arm["decisions"][index]["id"] if kind == "route" else observation["key"]
            )
            origin = (
                decisions[key]["origin"]
                if kind == "route"
                else boundaries[key]["start"]
            )
            known = [r for r in observations[kind] if r["completed"] <= origin]
            assert observation not in known
            if kind == "route":
                profile = arm["initial_cache_forecasts"][key]["control_release_profile"]
                assert profile["origin"] == origin
                quote = profile["routing_predictor"]
                provenance = "original_dispatch_quote"
            else:
                quote = fit_control(observations[kind], observation["features"], origin)
                provenance = "completed_only_reconstruction_at_D_control_start"
                assert abs(boundaries[key]["end"] - observation["completed"]) < 1e-8
            assert quote["samples"] == len(known)
            assert quote["latest"] is None or quote["latest"] <= origin
            work = search[key, kind]
            mean = statistics.mean(r["seconds"] for r in known) if known else 0.0
            collected.append(
                {
                    "id": key,
                    "origin": origin,
                    "prediction_provenance": provenance,
                    "predicted_seconds": quote["predicted"],
                    "actual_seconds": observation["seconds"],
                    "completed_mean_diagnostic": mean,
                    "history_samples": len(known),
                    "latest_completed": quote["latest"],
                    "visible_features": observation["features"],
                    "bound_checks_post_hoc": work["bound_checks"],
                    "calendars_post_hoc": work["calendar_full"]
                    + work.get("pruned_calendars", 0),
                    "path": paths[key],
                }
            )
        assert len(collected) == 95
    return {
        "summary": {
            kind: {
                "retained_predictor": metrics(values),
                "completed_mean_diagnostic": metrics(
                    [
                        {**r, "predicted_seconds": r["completed_mean_diagnostic"]}
                        for r in values
                    ]
                ),
                "post_hoc_CPU_correlation_not_causal": {
                    field: correlation(values, field)
                    for field in ("bound_checks_post_hoc", "calendars_post_hoc")
                },
                "largest_underpredictions": [
                    r["id"]
                    for r in sorted(
                        values,
                        key=lambda r: r["predicted_seconds"] - r["actual_seconds"],
                    )[:5]
                ],
            }
            for kind, values in rows.items()
        },
        "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert (args.result / "complete.json").exists()
    result = {
        "CPU_only": True,
        "policy_or_cost_changed": False,
        "net_gain_claim": False,
        "warnings": [
            "CPU labels are measured controller wall durations, not device compute or pure FIFO wait",
            "Post-hoc search counts are not available as future online observations",
            "Boundary prediction is reconstructed, not an original saved quote",
        ],
        "cases": [],
    }
    for i in (1, 2, 3):
        path = args.result / f"case-{i:02d}.json"
        case = json.loads(path.read_bytes())
        result["cases"].append(
            {
                "rate": case["rate"],
                "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "arms": {
                    name: analyze_arm(case["arms"][name])
                    for name in ("reference", "ours")
                },
            }
        )
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)


if __name__ == "__main__":
    main()
