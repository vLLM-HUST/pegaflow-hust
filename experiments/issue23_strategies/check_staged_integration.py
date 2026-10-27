"""One original request checks P/D/forecast integration, not performance."""

import argparse
import json
from pathlib import Path

import simulate_continuous_batch as sim
from pegaflow.strategies.memory_budget import Ascend910B2Budget
from prefix_reclaim_search import PrefixReclaimSimulation
from ready_prefill_search import ReadyPrefillSimulation
from staged_save_model import VERSION, StagedSaveSimulationMixin


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inputs", type=Path, required=True)
    p.add_argument("--seed", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    inputs = json.loads(a.inputs.read_bytes())
    values = dict(inputs["costs"])
    values["service"] = tuple(sim.parent.core.ObservedLatencyModel(**r) for r in values["service"])
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = sim.Costs(**values)
    rows = []
    for parent, mode in ((ReadyPrefillSimulation, "native"), (PrefixReclaimSimulation, "ours"), (ReadyPrefillSimulation, "ours")):
        cls = type("CheckedStaged" + parent.__name__, (StagedSaveSimulationMixin, parent), {})
        model = cls(inputs["rows"][:1], inputs["initial_inventory"], costs, a.seed,
                    inputs["control_cost_rows"], .49032341538142143, mode, True,
                    batch_config=sim.BatchConfig(max_sequences=4, bytes_per_token=costs.bytes_per_token,
                                                 kv_bytes=Ascend910B2Budget().kv_bytes),
                    phase_residual_world=True)
        result = model.run()
        publications = [e for e in result["actions"] if e["kind"] == "save_publication"]
        assert len(publications) == 2
        assert {e["node"] < 2 for e in publications} == {True, False}
        assert all(e["publication_model"] == VERSION for e in publications)
        assert result["save_publication_model"]["save_model_CPU_wall"] > 0
        rows.append({"class": parent.__name__, "mode": mode, "two_stage_publications": publications,
                     "save_model": result["save_publication_model"], "not_performance": True})
    with a.output.open("x", encoding="utf-8") as f:
        json.dump({"all_checks_passed": True, "original_requests": 1, "checks": rows}, f, indent=2)
    print("Three entry points, original one-request P/D/forecast source-contract check passed")


if __name__ == "__main__":
    main()
