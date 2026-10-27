"""Reconstruct trigger costs from isolated seed and completed observations."""

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import simulate_continuous_batch as sim
from audit_late_decode import close
from audit_yielding_decode import audit
from completed_phase_search import CompletedPhaseModels
from release_cobatch_search import decode_step_cost, shared_step_cost
from slot_release_search import release_estimate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("result", "inputs", "seed", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    values = dict(json.loads(args.inputs.read_bytes())["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    initial, cases = sim.Costs(**values), []
    for i in (1, 2, 3):
        case = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
        ref, arm = case["arms"]["reference"], case["arms"]["ours"]
        assert ref["internal_candidate"]["name"] == "prefix-reclaim-calendar-02"
        assert arm["internal_candidate"]["name"] == "advice-release-window-01"
        ref_audit, own_audit = audit(ref), audit(arm)
        records = arm["advice_trigger_records"]
        assert len({r["id"] for r in records}) == len(records)
        advice = {r["id"]: r for r in arm["decode_advice"]}
        model = CompletedPhaseModels(
            sim.parent.core.seed_operations(initial, args.seed, "phase")
        )
        completed = iter(arm["exclusive_D_observations"])
        next_row = next(completed, None)
        last = 0.0
        for r in records:
            origin = r["origin"]
            assert origin >= last and r["CPU_wall"] >= 0
            last = r["ready"]
            close(last, origin + r["CPU_wall"])
            known = [
                v
                for v in advice.values()
                if v["completed_search"] and v["decision_ready"] <= origin
            ]
            assert r["history"] == [v["id"] for v in known]
            assert r["CPU_latest"] == max(
                (v["decision_ready"] for v in known), default=None
            )
            if known:
                close(
                    r["predicted_CPU"], sum(v["CPU_wall"] for v in known) / len(known)
                )
            else:
                assert r["predicted_CPU"] is None
            measurable = (
                r["queue"][0] == r["id"]
                and len(r["active"]) == case["config"]["max_sequences"]
                and all(s["phase"] == "decode" for s in r["active"].values())
                and not r["external_gate"]
                and known
            )
            while next_row is not None and next_row["available"] <= origin:
                model.observe_exclusive(next_row)
                next_row = next(completed, None)
            if measurable:
                costs = model.freeze(origin)
                assert r["model_latest"] <= origin
                busy_counts = dict(r["busy_pieces"])
                busy_seconds = (
                    shared_step_cost(
                        [
                            decode_step_cost(
                                SimpleNamespace(**r["active"][k]), costs, r["node"]
                            )
                            * count
                            for k, count in r["busy_pieces"]
                        ],
                        case["config"]["shared_fraction"],
                    )
                    if busy_counts
                    else 0.0
                )
                close(busy_seconds, r["estimated_busy_seconds"])
                estimates = {}
                for k, s in r["active"].items():
                    seen = 1 + sum(
                        count
                        for event_index, event in enumerate(
                            arm["batch_iterations"][: r["observed_trace_count"]]
                        )
                        if event_index != r["busy_trace_index"]
                        and event["node"] == r["node"]
                        for name, count, phase in event["pieces"]
                        if name == k and phase == "decode"
                    )
                    assert seen == s["decoded"], (k, seen, s["decoded"])
                    progress = {**s, "decoded": s["decoded"] + busy_counts.get(k, 0)}
                    estimates[k] = busy_seconds + release_estimate(
                        SimpleNamespace(**progress), costs, r["node"]
                    )
                for k, value in estimates.items():
                    close(value, r["release_estimates"][k])
                assert r["skip"] == (
                    min(estimates.values()) <= r["predicted_CPU"] + r["RPC_seconds"]
                )
            else:
                assert not r["skip"] and not r["release_estimates"]
            if r["skip"]:
                assert r["id"] not in advice
            else:
                close(advice[r["id"]]["origin"], r["ready"])
            assert arm["makespan"] >= r["ready"]
        close(arm["advice_trigger_CPU_wall"], sum(r["CPU_wall"] for r in records))
        assert len(records) == len(advice) + sum(r["skip"] for r in records)
        assert len(arm["stages"]) == 190 and arm["full_decode_drain_required"]
        assert all(
            q["reclaim_removal_method"] == "prefix_remove_victims"
            and q["forecast_time_arithmetic"] == "binary64-exact-dyadic"
            for q in arm["initial_cache_forecasts"].values()
        )
        cases.append(
            {
                "rate": case["rate"],
                "reference_advice": ref_audit,
                "ours_advice": own_audit,
                "checks": len(records),
                "skips": sum(r["skip"] for r in records),
                "trigger_CPU_wall": arm["advice_trigger_CPU_wall"],
                "all_checks_passed": True,
            }
        )
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(
            {"CPU_only": True, "all_checks_passed": True, "cases": cases},
            stream,
            indent=2,
        )


if __name__ == "__main__":
    main()
