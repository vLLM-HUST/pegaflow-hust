"""Paired fixed-state forecast CPU diagnostic, never an online policy result.

Both implementations receive fresh copies of the same detached state before
each calendar. Alternating order limits simple order bias. Copy/profiler time is
reported separately and is not injected into the zero-control causal replay.
"""

import argparse
import copy
import cProfile
import hashlib
import json
import pstats
import time
from pathlib import Path

import simulate_continuous_batch as sim
from cache_evolving_search import CacheEvolvingSnapshot
from delta_protection_search import DeltaProtectionProjection
from pegaflow.strategies.memory_budget import Ascend910B2Budget
from release_cobatch_search import ReleaseCobatchSimulation, ReleaseCobatchSnapshot


def cloned_shadow(shadow, arguments, projection):
    # These live coordination/capture capabilities are not forecast inputs.
    # Never copy their bound Simulation, lock or thread-local context.
    state = {
        k: v
        for k, v in shadow.__dict__.items()
        if k not in ("guard", "local_decision", "models") and not callable(v)
    }
    result = object.__new__(type(shadow))
    # Costs are frozen completed-history values, not a mutable model owner.
    # Their forwarding dataclasses deliberately share their read-only inputs;
    # deepcopy reconstruction would call __getattr__ before initialization.
    frozen = [arguments[-1], state.get("phase_costs")]
    if state.get("context") is not None:
        frozen.append(state["context"][2])
    memo = {id(value): value for value in frozen if value is not None}
    # Preserve immutable tuple identities used as cache keys. General deepcopy
    # would invalidate warm indexes and cannot reconstruct MappingProxyType.
    projected = state.get("projection_memo") or {}
    indexes = projected.get("node_indexes")
    if indexes is not None:
        cloned_indexes = copy.copy(indexes)
        cloned_indexes.values = {}
        memo[id(indexes)] = cloned_indexes
        for identity, index in indexes.values.items():
            cloned_index = copy.copy(index)
            object.__setattr__(cloned_index, "__dict__", index.__dict__.copy())
            cloned_indexes.values[identity] = cloned_index
            memo[id(index)] = cloned_index
            memo[id(index.units)] = index.units
    for units, _ in projected.get("saves", {}).values():
        memo[id(units)] = units
    layouts = [state.get("projected_layout")]
    if state.get("context") is not None:
        layouts.append(state["context"][1])
    for layout in layouts:
        if layout is None or id(layout) in memo:
            continue
        cloned_layout = copy.copy(layout)
        object.__setattr__(cloned_layout, "__dict__", layout.__dict__.copy())
        if hasattr(layout, "indexes"):
            object.__setattr__(cloned_layout, "indexes", memo[id(layout.indexes)])
        memo[id(layout)] = cloned_layout
        for units in layout.units:
            memo[id(units)] = units
    result.__dict__, arguments = copy.deepcopy((state, arguments), memo)
    result.projection_type = projection
    return result, arguments


def measured_call(original, shadow, arguments, projection, profile=False):
    began = time.process_time_ns()
    replica, arguments = cloned_shadow(shadow, arguments, projection)
    clone_cpu = (time.process_time_ns() - began) / 1e9
    recorder = cProfile.Profile() if profile else None
    began, wall = time.process_time_ns(), time.monotonic_ns()
    if recorder:
        recorder.enable()
    try:
        answer = original(replica, *arguments)
        outcome = ("returned", answer)
    except ValueError as error:
        outcome = (type(error).__qualname__, str(error))
    finally:
        if recorder:
            recorder.disable()
    cpu, elapsed = (
        (time.process_time_ns() - began) / 1e9,
        (time.monotonic_ns() - wall) / 1e9,
    )
    record = {"cpu": cpu, "wall": elapsed, "clone_cpu_excluded": clone_cpu}
    if recorder:
        record["functions"] = [
            {
                "file": k[0],
                "line": k[1],
                "function": k[2],
                "calls": v[1],
                "self_seconds": v[2],
                "cumulative_seconds": v[3],
            }
            for k, v in pstats.Stats(recorder).stats.items()
        ]
    return outcome, replica.search_stats, record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--targets", nargs="+", default=["headless-terminal:12", "headless-terminal:13"]
    )
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False)
    inputs = sim.parent.read(args.inputs)
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = sim.Costs(**values)
    captured, scope = [], []
    original = CacheEvolvingSnapshot.predict_batch
    projections = {
        "parent": ReleaseCobatchSnapshot.projection_type,
        "delta": DeltaProtectionProjection,
    }

    def paired(shadow, *arguments):
        if not scope:
            return original(shadow, *arguments)
        result = {
            "request": scope[-1][0],
            "kind": scope[-1][1],
            "origin": arguments[1],
            "measurements": [],
        }
        canonical = None
        for name in ("parent", "delta", "delta", "parent"):
            answer, stats, measured = measured_call(
                original, shadow, arguments, projections[name]
            )
            if canonical is None:
                canonical = answer, stats
            elif (answer, stats) != canonical:
                raise AssertionError("Fixed-state calendar or pruning differs")
            result["measurements"].append(dict(implementation=name, **measured))
        result["profiles"] = {}
        for name, projection in projections.items():
            answer, stats, measured = measured_call(
                original, shadow, arguments, projection, True
            )
            if (answer, stats) != canonical:
                raise AssertionError("Profiling changed forecast semantics")
            result["profiles"][name] = measured
        result.update(equal=True, outcome=canonical[0][0], search_stats=canonical[1])
        captured.append(result)
        try:
            actual = original(shadow, *arguments)
        except ValueError as error:
            assert (type(error).__qualname__, str(error)) == canonical[0]
            assert shadow.search_stats == canonical[1]
            raise
        assert ("returned", actual) == canonical[0]
        assert shadow.search_stats == canonical[1]
        return actual

    class Diagnostic(ReleaseCobatchSimulation):
        def choose(self, row, ready):
            if row["id"] not in args.targets:
                return super().choose(row, ready)
            scope.append((row["id"], "route"))
            try:
                return super().choose(row, ready)
            finally:
                scope.pop()

        def stage(self, key, node):
            if key not in args.targets or node < 2:
                return super().stage(key, node)
            scope.append((key, "boundary"))
            try:
                return super().stage(key, node)
            finally:
                scope.pop()

    clock = time.perf_counter
    time.perf_counter = lambda: 0.0
    CacheEvolvingSnapshot.predict_batch = paired
    try:
        simulation = Diagnostic(
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
        )
        # Fail immediately on unsupported snapshot capabilities, not after a
        # long causal replay has reached its diagnostic request.
        cloned_shadow(
            simulation.policy_queues.planning_snapshot(),
            ((), 0.0, {}, {}, simulation.frozen()),
            DeltaProtectionProjection,
        )
        result = simulation.run()
    finally:
        time.perf_counter = clock
        CacheEvolvingSnapshot.predict_batch = original
    assert result["requests"] == 95
    assert {(r["request"], r["kind"]) for r in captured} == {
        (key, kind) for key in args.targets for kind in ("route", "boundary")
    }
    for filename, value in (
        ("fixed-control-result.json", result),
        (
            "paired-profile.json",
            {
                "CPU_only": True,
                "net_performance_claim": False,
                "not_reconstruction_of_charged_timeline": True,
                "input_sha256": hashlib.sha256(args.inputs.read_bytes()).hexdigest(),
                "source_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "measurements": captured,
            },
        ),
    ):
        with (args.output / filename).open("x", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2)
    print(json.dumps({"complete": True, "paired_calendars": len(captured)}))


if __name__ == "__main__":
    main()
