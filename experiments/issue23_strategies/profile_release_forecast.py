"""Offline fixed-control profiling; never a net policy performance comparison.

Zero control cost gives a reproducible causal state for profiling a selected
decision. The profiler is outside the online cost model. Full original requests,
physical work, cache updates and completed-only model feedback still execute.
"""

import argparse
import cProfile
import hashlib
import json
import pstats
import time
from dataclasses import asdict
from pathlib import Path

import simulate_continuous_batch as sim
from cache_evolving_search import CacheEvolvingSnapshot
from pegaflow.strategies.memory_budget import Ascend910B2Budget
from release_cobatch_search import ReleaseCobatchSimulation


def write(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target", default="headless-terminal:13")
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False)
    inputs = sim.parent.read(args.inputs)
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = sim.Costs(**values)
    original_clock = time.perf_counter
    time.perf_counter = lambda: 0.0
    captured = []

    class ProfileSimulation(ReleaseCobatchSimulation):
        def choose(self, row, ready):
            if row["id"] != args.target:
                return super().choose(row, ready)
            item = self.item(row)
            context = {
                "id": row["id"],
                "origin": self.now,
                "ready": ready,
                "inventory_prefix_tokens": [
                    self.layout.prefix(n, item.hashes) * 128 for n in range(4)
                ],
                "layout": asdict(self.layout),
                "live_requests": list(self.jobs),
            }
            alternatives = []
            profiled_stats = {}
            original = CacheEvolvingSnapshot.forecast

            def traced(shadow, route, now):
                record = {"command": asdict(route)}
                try:
                    answer = original(shadow, route, now)
                except ValueError as error:
                    record["rejection"] = str(error)
                    alternatives.append(record)
                    raise
                finally:
                    profiled_stats.update(shadow.search_stats)
                record["forecast"] = asdict(answer)
                alternatives.append(record)
                return answer

            profile = cProfile.Profile()
            CacheEvolvingSnapshot.forecast = traced
            try:
                answer = profile.runcall(super().choose, row, ready)
            finally:
                CacheEvolvingSnapshot.forecast = original
            stats = pstats.Stats(profile)
            functions = [
                {
                    "file": key[0],
                    "line": key[1],
                    "function": key[2],
                    "primitive_calls": value[0],
                    "calls": value[1],
                    "self_seconds": value[2],
                    "cumulative_seconds": value[3],
                }
                for key, value in stats.stats.items()
            ]
            context.update(
                selected=asdict(answer[3]),
                alternatives=alternatives,
                functions=sorted(functions, key=lambda r: -r["self_seconds"]),
                total_profile_seconds=stats.total_tt,
                search_stats=profiled_stats,
            )
            # JSON cannot represent immutable key sets; only the diagnostic
            # output is converted. The executing inventory stays immutable.
            context["layout"] = json.loads(json.dumps(context["layout"], default=list))
            captured.append(context)
            return answer

    try:
        result = ProfileSimulation(
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
        time.perf_counter = original_clock
    if len(captured) != 1 or result["requests"] != 95:
        raise ValueError("Expected one target and the complete causal workload")
    write(args.output / "fixed-control-result.json", result)
    write(
        args.output / "profile.json",
        {
            "CPU_only": True,
            "diagnostic_control_cost": 0,
            "net_performance_claim": False,
            "not_reconstruction_of_previous_measured_timeline": True,
            "input_sha256": hashlib.sha256(args.inputs.read_bytes()).hexdigest(),
            "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "captured": captured,
        },
    )
    print(
        json.dumps(
            {"complete": True, "profile_seconds": captured[0]["total_profile_seconds"]}
        )
    )


if __name__ == "__main__":
    main()
