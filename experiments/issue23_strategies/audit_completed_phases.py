"""Reconcile completed exclusive phase labels against owned event traces."""

import argparse
import json
from pathlib import Path

import simulate_continuous_batch  # noqa: F401
from completed_phase_search import exclusive_phase_observation

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--result", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
report = {"scope": "CPU trace accounting, not hardware timer validation", "cases": []}
for i in (1, 2, 3):
    case = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
    arm = case["arms"]["ours"]
    assert arm["phase_feedback_uses_world_labels"] is False
    rows = arm["exclusive_D_observations"]
    assert len(rows) == 95 and len({r["key"] for r in rows}) == 95
    stages = {(r["id"], r["node"]): r for r in arm["stages"]}
    for r in rows:
        stage = stages[r["key"], r["node"]]
        assert r["completed"] == stage["completed"]
        assert r["prefix"] == stage["prefix"]
        assert r["available"] == r["completed"] + r["maintenance_CPU"]
        reconstructed = exclusive_phase_observation(
            arm["batch_iterations"],
            arm["iteration_scheduler_CPU"],
            r["key"],
            r["node"],
            r["completed"],
        )
        for name, value in reconstructed.items():
            assert r[name] == value
        assert abs(r["restore"] - stage["restore_path_not_pure_H2D"]) < 1e-8
        assert abs(r["save"] - stage["synchronous_finish_seconds"]) < 1e-8
    assert sum(r["maintenance_CPU"] for r in rows) <= arm["fit_CPU_wall"]
    for decision in arm["decisions"]:
        assert decision["model_latest"] <= decision["origin"]
        known = [r for r in rows if r["completed"] <= decision["model_latest"]]
        assert all(r["available"] <= decision["origin"] for r in known)
    report["cases"].append(
        {
            "rate": case["rate"],
            "completed_owned_D_records": len(rows),
            "model_maintenance_CPU": sum(r["maintenance_CPU"] for r in rows),
            "all_phase_labels_reconciled": True,
            "no_future_observations": True,
            "not_trained_on_batched_residence": True,
            "fit_parameters_per_forecast": "Not separately logged; frozen source reviewed, completed labels reconciled",
        }
    )
with args.output.open("x", encoding="utf8") as stream:
    json.dump(report, stream, indent=2)
print(json.dumps(report, indent=2))
