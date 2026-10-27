"""Recompute release ranking and co-batching from isolated/past-only costs."""

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import simulate_continuous_batch as sim
from completed_phase_search import CompletedPhaseModels
from release_cobatch_search import decode_step_cost, shared_step_cost
from slot_release_search import release_estimate


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
    result = {"CPU_only": True, "no_future_world_cost": True, "cases": []}
    for index in (1, 2, 3):
        case = json.loads((args.result / f"case-{index:02d}.json").read_bytes())
        arm = case["arms"]["ours"]
        model = CompletedPhaseModels(
            sim.parent.core.seed_operations(initial, args.seed, "phase")
        )
        stages = {s["id"]: s for s in arm["stages"] if s["node"] >= 2}
        completed = iter(arm["exclusive_D_observations"])
        next_row = next(completed, None)
        last, count, companions = 0.0, 0, 0
        for quote in arm["slot_release_choices"]:
            origin, node = quote["origin"], quote["node"]
            assert origin >= last and quote["model_latest"] <= origin
            last = origin
            while next_row is not None and next_row["available"] <= origin:
                model.observe_exclusive(next_row)
                next_row = next(completed, None)
            costs = model.freeze(origin)
            rows = {
                k: SimpleNamespace(
                    prompt_tokens=stages[k]["tokens"], output_tokens=128, decoded=d
                )
                for k, d in quote["decoded"].items()
            }
            predicted = {k: release_estimate(s, costs, node) for k, s in rows.items()}
            assert all(
                abs(predicted[k] - v) < 1e-7
                for k, v in quote["remaining_estimates"].items()
            )
            selected = min(predicted, key=predicted.get)
            assert selected == quote["selected"]
            if arm["internal_candidate"]["name"] in (
                "release-preserving-cobatch-01",
                "delta-protection-cobatch-01",
                "delta-protection-cobatch-02",
                "dirty-scan-cobatch-01",
                "late-decode-reconsider-01",
                "progress-preserving-decode-01",
                "yielding-decode-advice-01",
                "yielding-decode-advice-02",
                "prefill-source-revalidation-01",
                "exact-binary-calendar-01",
                "exact-binary-calendar-02",
                "linear-reclaim-calendar-01",
                "prefix-reclaim-calendar-02",
                "no-decode-advice-01",
                "advice-release-window-01",
            ):
                step = {k: decode_step_cost(s, costs, node) for k, s in rows.items()}
                cohort, durations = [selected], [step[selected]]
                cap = shared_step_cost(durations, case["config"]["shared_fraction"])
                for key in rows:
                    if key == selected or len(cohort) >= case["config"]["token_budget"]:
                        continue
                    added = [*durations, step[key]]
                    if (
                        shared_step_cost(added, case["config"]["shared_fraction"])
                        <= cap
                    ):
                        cohort.append(key)
                        durations = added
                assert cohort == quote["cohort"]
                companions += len(cohort) - 1
            count += 1
        result["cases"].append(
            {
                "rate": case["rate"],
                "recomputed_choices": count,
                "non_extending_companions": companions,
            }
        )
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
