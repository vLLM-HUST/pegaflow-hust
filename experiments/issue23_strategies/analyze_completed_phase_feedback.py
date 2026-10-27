"""Trace-conditioned phase prediction errors, not counterfactual policy gains."""

import argparse
import json
import math
import statistics
from pathlib import Path

import simulate_continuous_batch as sim
from completed_phase_search import CompletedPhaseModels

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--result", type=Path, required=True)
parser.add_argument("--inputs", type=Path, required=True)
parser.add_argument("--seed", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
data = json.loads(args.inputs.read_bytes())
values = dict(data["costs"])
values["service"] = tuple(
    sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
)
values["decode_seconds"] = tuple(values["decode_seconds"])
costs = sim.Costs(**values)


def errors(rows):
    return {
        "n": len(rows),
        "MAE": statistics.mean(abs(x) for x in rows),
        "RMSE": math.sqrt(statistics.mean(x * x for x in rows)),
        "bias_actual_minus_predicted": statistics.mean(rows),
    }


report = {
    "scope": "Fixed observed routes and accepted prefixes; predict at actual D API start using only already available phase observations. Not initial-route forecast or a policy-effect experiment.",
    "cases": [],
}
for i in (1, 2, 3):
    case = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
    arm = case["arms"]["ours"]
    records = arm["exclusive_D_observations"]
    stages = {(r["id"], r["node"]): r for r in arm["stages"]}
    samples = []
    for observed in records:
        n, prefix = observed["node"], observed["prefix"]
        stage = stages[observed["key"], n]
        origin = stage["completed"] - stage["full"]
        # Rebuild independently for each acquisition cutoff: completion order
        # differs from acquisition order, so do not use a later fit snapshot.
        initial = sim.parent.core.seed_operations(costs, args.seed, "phase")
        model = CompletedPhaseModels(initial)
        known = sorted(
            (r for r in records if r["available"] <= origin),
            key=lambda r: r["completed"],
        )
        for r in known:
            model.observe_exclusive(r)
        frozen, baseline = model.freeze(origin), initial.freeze(origin)
        samples.append(
            {
                "key": observed["key"],
                "node": n,
                "prefix": prefix,
                "origin": origin,
                "known": len(known),
                "latest_available": max((r["available"] for r in known), default=0),
                "restore_actual": observed["restore"],
                "restore_seed": baseline.restore(n, prefix),
                "restore_updated": frozen.restore(n, prefix),
                "save_actual": observed["save"],
                "save_seed": baseline.fits[n][4],
                "save_updated": frozen.fits[n][4],
            }
        )
    summaries = {}
    for n in (2, 3):
        summaries[str(n)] = {}
        for phase in ("restore", "save"):
            selected = [
                r
                for r in samples
                if r["node"] == n and (phase != "restore" or r["prefix"])
            ]
            summaries[str(n)][phase] = {
                kind: errors(
                    [r[phase + "_actual"] - r[phase + "_" + kind] for r in selected]
                )
                for kind in ("seed", "updated")
            }
    report["cases"].append(
        {"rate": case["rate"], "summary": summaries, "observations": samples}
    )
with args.output.open("x", encoding="utf8") as stream:
    json.dump(report, stream, indent=2)
print(
    json.dumps(
        {
            "cases": [
                {"rate": c["rate"], "summary": c["summary"]} for c in report["cases"]
            ]
        },
        indent=2,
    )
)
