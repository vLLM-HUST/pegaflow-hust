"""Fixed-state candidate scoring and arithmetic profiling, never net performance.

Replays the original causal workload with measured control time fixed to zero.
It does not reconstruct a previous charged run's state or use its future labels.
"""

import argparse
import copy
import cProfile
import hashlib
import json
import pstats
import time
from dataclasses import asdict
from pathlib import Path

import simulate_continuous_batch as sim
from numerical_calendar_search import NumericalCalendarSimulation
from pegaflow.resource_profile import route_order
from pegaflow.strategies.memory_budget import Ascend910B2Budget


def functions(profile):
    stats = pstats.Stats(profile)
    rows = [
        {
            "file": k[0],
            "line": k[1],
            "function": k[2],
            "calls": v[1],
            "self_seconds": v[2],
            "cumulative_seconds": v[3],
        }
        for k, v in stats.stats.items()
    ]
    return sorted(rows, key=lambda r: -r["self_seconds"])


def freeze_calendar_call(args, options):
    # Evaluate only the remaining-time queries that add() can make at this
    # origin. Do not deepcopy the bound snapshot method and its synchronization
    # lock. No future realized duration is supplied to either forecast variant.
    work, now, remaining = args[:3]
    residuals = {}
    for item in work:
        node = item.p if item.p_done is None else item.d
        operations = item.prefill if node < 2 else item.decode
        operations = [
            op for op in operations if op.name not in item.completed_operations
        ]
        if operations:
            op = operations[0]
            starts = dict(item.active_operations)
            if op.name in starts and op.name != "D_api":
                age = max(0.0, now - starts[op.name])
                residuals[item.key, op, age] = remaining(item, op, age)
    frozen_args = (*args[:2], lambda w, op, age: residuals[w.key, op, age], *args[3:])
    return clone_calendar_state(frozen_args, options)


def clone_calendar_state(args, options):
    # These are completed-only immutable cost snapshots, not trainable models.
    # Their delegation proxies intentionally do not support Python deepcopy.
    costs = args[5]
    assert costs.latest_completed_seconds <= args[1]
    memo = {id(costs): costs}
    projection = options.get("cache_state")
    if projection is not None:
        for frozen in (projection.costs, projection.state.costs):
            assert frozen.latest_completed_seconds <= args[1]
            memo[id(frozen)] = frozen
    return copy.deepcopy((args, options), memo)


def json_value(value):
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    raise TypeError(f"Unsupported diagnostic value: {type(value).__name__}")


def main():
    import cache_evolving_search as cache

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target", default="git-multibranch:12")
    parser.add_argument("--probe-only", action="store_true")
    parser.add_argument("--compare-reclaim", action="store_true")
    parser.add_argument("--compare-prefix-reclaim", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False)
    inputs = json.loads(args.inputs.read_bytes())
    if args.probe_only:
        inputs["rows"] = inputs["rows"][:1]
        args.target = inputs["rows"][0]["id"]
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**v) for v in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = sim.Costs(**values)
    clock = time.perf_counter
    time.perf_counter = lambda: 0.0
    captured = []

    class Diagnostic(NumericalCalendarSimulation):
        def choose(self, row, ready):
            if row["id"] != args.target:
                return super().choose(row, ready)
            report = {
                "id": row["id"],
                "origin": self.now,
                "ready": ready,
                "pending": [asdict(j["work"]) for j in self.jobs.values()],
                "alternatives": [],
            }
            original_forecast = cache.CacheEvolvingSnapshot.forecast
            original_calendar = cache.schedule_batched
            first_call = []
            profile = cProfile.Profile()

            def traced_calendar(*a, **kw):
                if not first_call:
                    profile.disable()
                    clean = dict(kw)
                    clean["first_profile_guard"] = None
                    first_call.append(freeze_calendar_call(a, clean))
                    profile.enable()
                return original_calendar(*a, **kw)

            def traced_forecast(shadow, route, now):
                record = {"command": asdict(route)}
                try:
                    result = original_forecast(shadow, route, now)
                except ValueError as error:
                    record["rejection"] = str(error)
                    raise
                else:
                    record.update(forecast=asdict(result), ordering=route_order(result))
                    return result
                finally:
                    report["alternatives"].append(record)
                    report["search_stats"] = dict(shadow.search_stats)

            cache.schedule_batched = traced_calendar
            cache.CacheEvolvingSnapshot.forecast = traced_forecast
            try:
                answer = profile.runcall(super().choose, row, ready)
            finally:
                cache.schedule_batched = original_calendar
                cache.CacheEvolvingSnapshot.forecast = original_forecast
            report["selected"] = asdict(answer[3])
            report["selected_ordering"] = route_order(answer[3])
            valid = [r for r in report["alternatives"] if "ordering" in r]
            assert report["selected_ordering"] == min(r["ordering"] for r in valid)
            report["selected_is_minimum_fully_scored_in_budget"] = True
            report["search_functions"] = functions(profile)
            report["identical_state_calendars"] = []
            variants = ([(True, False), (True, True), (True, True), (True, False)]
                        if args.compare_reclaim else [(e, False) for e in (False, True, True, False)])
            if args.compare_prefix_reclaim:
                variants = [(True, m) for m in (False, True, "prefix", "prefix", True, False)]
            full_predictions = []
            for exact, linear_reclaim in variants:
                a, kw = clone_calendar_state(*first_call[0])
                kw["exact_timeline"] = exact
                if linear_reclaim:
                    from linear_reclaim_search import linear_remove_victims

                    kw["cache_state"].state.remove_victims = linear_remove_victims
                if linear_reclaim == "prefix":
                    from prefix_reclaim_search import prefix_remove_victims

                    kw["cache_state"].state.remove_victims = prefix_remove_victims
                micro = cProfile.Profile()
                began = clock()
                prediction = micro.runcall(original_calendar, *a, **kw)
                elapsed = clock() - began
                full_predictions.append(prediction)
                report["identical_state_calendars"].append(
                    {
                        "exact": exact,
                        "linear_reclaim": linear_reclaim,
                        "profiled_wall_not_online_cost": elapsed,
                        "first": prediction["first"],
                        "full": prediction["full"],
                        "functions": functions(micro),
                        "first_bound_disabled_for_both": True,
                        "copy_setup_excluded": True,
                    }
                )
            if args.compare_reclaim or args.compare_prefix_reclaim:
                assert all(p == full_predictions[0] for p in full_predictions)
                report["identical_complete_calendar_predictions"] = True
            captured.append(report)
            return answer

    try:
        result = Diagnostic(
            inputs["rows"],
            inputs["initial_inventory"],
            costs,
            args.seed,
            inputs["control_cost_rows"],
            0.49032341538142143,
            "ours",
            True,
            batch_config=sim.BatchConfig(
                max_sequences=4,
                shared_fraction=1.0,
                bytes_per_token=costs.bytes_per_token,
                kv_bytes=Ascend910B2Budget(
                    bytes_per_token=costs.bytes_per_token
                ).kv_bytes,
            ),
            phase_residual_world=True,
        ).run()
    finally:
        time.perf_counter = clock
    assert len(captured) == 1 and result["requests"] == (1 if args.probe_only else 95)
    for name, value in {
        "fixed-control-result.json": result,
        "profile.json": {
            "CPU_only": True,
            "net_performance_claim": False,
            "not_old_charged_state_reconstruction": True,
            "probe_only": args.probe_only,
            "compare_reclaim": args.compare_reclaim,
            "compare_prefix_reclaim": args.compare_prefix_reclaim,
            "input_sha256": hashlib.sha256(args.inputs.read_bytes()).hexdigest(),
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "captured": captured,
        },
    }.items():
        data = json.dumps(value, indent=2, default=json_value)
        with (args.output / name).open("x", encoding="utf-8") as stream:
            stream.write(data)
    print(json.dumps({"complete": True, "captured": len(captured)}))


if __name__ == "__main__":
    main()
