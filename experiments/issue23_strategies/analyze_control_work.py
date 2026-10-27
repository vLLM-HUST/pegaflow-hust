"""Audit completed-only fit labels and inspect component prediction, not just endpoints."""

import argparse
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--result", type=Path, required=True)
parser.add_argument("--inputs", type=Path, required=True)
parser.add_argument("--source-receipt", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
root, inputs = args.result, args.inputs
repo = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(repo / "python"), str(repo / "experiments/issue23_strategies")]
import simulate_continuous_batch  # noqa: F401
from analyze_bounded_behavior import main as paths
from analyze_bounded_behavior import per_request
from analyze_cache_evolution import analyze as behavior
from analyze_continuous_batch import analyze
from control_work_search import fit_control

manifest = json.loads(args.source_receipt.read_bytes())
scope = json.loads((root / "scope.json").read_bytes())
assert all(manifest["files"][k] == v for k, v in scope["source_sha256"].items())
assert scope["input_sha256"] == hashlib.sha256(inputs.read_bytes()).hexdigest()
audit = analyze(root / "screen.json", inputs)
report = behavior(root)
report["path_behavior"] = paths(root)


def errors(values):
    return {
        "MAE": statistics.mean(abs(v) for v in values),
        "RMSE": math.sqrt(statistics.mean(v * v for v in values)),
        "bias": statistics.mean(values),
        "max_absolute": max(map(abs, values)),
    }


for i, case in enumerate(report["cases"], 1):
    raw = json.loads((root / f"case-{i:02d}.json").read_bytes())
    arm = raw["arms"]["ours"]
    observations = arm["control_work_observations"]
    assert len(observations["route"]) == len(arm["decisions"]) == 95
    rpc = None
    for decision, observation in zip(arm["decisions"], observations["route"]):
        assert observation["seconds"] == decision["CPU_wall"]
        delay = observation["completed"] - decision["origin"] - decision["CPU_wall"]
        if rpc is None:
            rpc = delay
        assert abs(delay - rpc) < 1e-8
    assert len(observations["boundary"]) == len(arm["boundary_control_history"])
    for observation, original in zip(
        observations["boundary"], arm["boundary_control_history"]
    ):
        for field in ("key", "completed", "seconds"):
            assert observation[field] == original[field]
    methods = {}
    for key, quote in arm["initial_cache_forecasts"].items():
        profile = quote["control_release_profile"]
        assert profile["origin"] == quote["origin"]
        for label, predictor in [
            ("route", "routing_predictor"),
            ("boundary", "boundary_predictor"),
        ]:
            actual = profile[predictor]
            expected = fit_control(
                observations[label], profile["work_features"], quote["origin"]
            )
            assert actual["samples"] == expected["samples"]
            assert actual["latest"] == expected["latest"]
            assert actual["latest"] is None or actual["latest"] <= quote["origin"]
            assert abs(actual["predicted"] - expected["predicted"]) < 1e-7
            assert actual["coefficients"] is None or all(
                c >= 0 for c in actual["coefficients"]
            )
        method = profile["routing_predictor"]["method"]
        methods[method] = methods.get(method, 0) + 1
        assert {r["request"] for r in quote["effective_API"]} == {key}
    case["fit_methods"] = methods
    case["component_prediction"] = {}
    for name in ("reference", "ours"):
        a = raw["arms"][name]
        rows = per_request(a)
        residuals = {"route_CPU": [], "P_end": [], "first": []}
        for decision in a["decisions"]:
            key = decision["id"]
            quote = a["initial_cache_forecasts"][key]
            profile = quote.get("control_release_profile", {})
            predicted = (
                profile["routing_predictor"]["predicted"]
                if "routing_predictor" in profile
                else profile["routing_CPU"]["mean"]
                if "routing_CPU" in profile
                else None
            )
            if predicted is not None:
                residuals["route_CPU"].append(decision["CPU_wall"] - predicted)
            row = rows[key]
            residuals["P_end"].append(
                row["ready"]
                + row["before_P"]
                + row["P_API"]
                - decision["initial_forecast"]["P_end"]
            )
            residuals["first"].append(
                row["first"] - decision["initial_forecast"]["first"]
            )
        case["component_prediction"][name] = {
            k: errors(v)
            if v
            else {
                "available": False,
                "reason": "Reference did not forecast this component",
            }
            for k, v in residuals.items()
        }

audit.update(
    all_285_control_quotes_recomputed_from_completed_only=True,
    route_labels_match_charged_decision_CPU=True,
    boundary_labels_match_completed_control_history=True,
    state_feature_future_independence="Source inspection; not a separate trace of every inventory input",
)
with args.output.open("x", encoding="utf-8") as stream:
    json.dump({"audit": audit, "analysis": report}, stream, indent=2)
print(
    json.dumps({"validated_arms": audit["validated_arms"], "output": str(args.output)})
)
