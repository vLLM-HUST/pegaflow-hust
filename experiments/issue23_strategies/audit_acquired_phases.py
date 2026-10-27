"""Check acquired phase predictions against initial and completed-only evidence."""

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import simulate_continuous_batch as sim
from acquired_phase_search import acquired_quote, phase_samples
from completed_phase_search import CompletedPhaseModels


def main():
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
    initial = sim.Costs(**values)
    report = {
        "scope": "CPU acquired-state accounting, not hardware validation",
        "cases": [],
    }
    for i in (1, 2, 3):
        case = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
        arm = case["arms"]["ours"]
        models = CompletedPhaseModels(
            sim.parent.core.seed_operations(initial, args.seed, "phase")
        )
        seeded = phase_samples(models)
        owned = {(r["id"], r["node"]): r for r in arm["stages"]}
        count, origins = 0, []
        for snapshot in arm["acquired_phase_quotes"]:
            origin = snapshot["origin"]
            # Rebuild each snapshot; no label from a later call can leak backwards.
            model = CompletedPhaseModels(
                sim.parent.core.seed_operations(initial, args.seed, "phase")
            )
            known = [
                r for r in arm["exclusive_D_observations"] if r["available"] <= origin
            ]
            for row in known:
                model.observe_exclusive(row)
            costs = model.freeze(origin)
            for node_text, quote in snapshot["phases"].items():
                node = int(node_text)
                key = quote["pieces"][0][0]
                trace = [
                    e
                    for e in arm["batch_iterations"]
                    if e["node"] == node
                    and e["start"] == quote["started"]
                    and e["kind"] == quote["kind"]
                    and e["pieces"] == quote["pieces"]
                ]
                assert len(trace) == 1
                cpu = [
                    r["seconds"]
                    for r in arm["iteration_scheduler_CPU"]
                    if r["node"] == node and r["completed"] <= origin
                ]
                progress = SimpleNamespace(
                    key=key,
                    prefix_tokens=owned[key, node]["prefix"],
                    phase=quote["kind"],
                )
                expected = acquired_quote(
                    {
                        "node": node,
                        "kind": quote["kind"],
                        "start": quote["started"],
                        "pieces": quote["pieces"],
                    },
                    progress,
                    costs,
                    [*seeded, *known],
                    origin,
                    sum(cpu) / len(cpu) if cpu else 0.0,
                )
                for field in ("remaining", "point", "age", "latest"):
                    assert abs(quote[field] - expected[field]) < 1e-7, (i, key, field)
                assert quote["samples"] == expected["samples"]
                assert (
                    quote["latest"] <= origin and quote["future_finish_read"] is False
                )
                count += 1
            origins.append(origin)
        route_origins = {r["origin"] for r in arm["decisions"]}
        report["cases"].append(
            {
                "rate": case["rate"],
                "checked_phase_quotes": count,
                "route_decisions_with_acquired_phase": len(
                    route_origins.intersection(origins)
                ),
                "completed_only_samples_recomputed": True,
            }
        )
    with args.output.open("x", encoding="utf8") as stream:
        json.dump(report, stream, indent=2)
    print(json.dumps(report))


if __name__ == "__main__":
    main()
