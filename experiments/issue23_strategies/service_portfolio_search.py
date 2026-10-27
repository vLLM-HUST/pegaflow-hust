"""Small structural route portfolio, with an explicit approximation boundary.

Each P/D pair contributes its cheapest feasible current service/copy plan.
The shared resource calendar then compares the four representatives, including
live full-decode occupation. This is not dominance pruning or an optimality
claim: a different prefix on the same pair may win under resource contention.
"""

import time
from dataclasses import replace

from paper_baseline_search import PaperQueues, PaperRoutingSimulation
from pegaflow.joint_policy import copy_package
from pegaflow.resource_cache_profile import CacheProfileQueues
from pegaflow.resource_profile import route_order
from pegaflow.strategies.model import Request, Snapshot
from pegaflow.strategies.resource_forecast import candidate_work
from timed_recovery_search import TimedMemoSimulation


class ServiceBoundaryQueues(PaperQueues):
    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
        demand = self.demands[key]
        local = layout.prefix(route.d, demand.hashes)
        limit = max(local, layout.prefix(route.p, demand.hashes))
        choices = []
        for length in range(limit + 1):
            payload = 0
            if length > local:
                try:
                    payload = (
                        copy_package(layout, route.p, route.d, demand.hashes[:length])
                        .operations[-1]
                        .payload_bytes
                    )
                except ValueError:
                    continue
            service = costs.compute(
                route.d, demand.tokens, length * demand.block_tokens
            )
            score = service + costs.copy_seconds(route.p, route.d, payload)
            choices.append((score, payload, length, service))
        _, _, length, service = min(choices)
        selected = replace(route, d_blocks=length, d_headroom=False)
        self.admission_choices[key, route.d] = False
        CacheProfileQueues.revise(self, key, selected, layout, costs)
        self.progress_version += 1
        self.search_records.append(
            {
                "kind": "service-boundary",
                "key": key,
                "blocks": length,
                "feasible_boundaries": len(choices),
                "global_interference_scored": False,
            }
        )
        return length, service


class ServicePortfolioSimulation(PaperRoutingSimulation):
    queue_type = ServiceBoundaryQueues
    paper_name = "internal-service-portfolio"

    def choose(self, row, ready):
        if self.mode == "native":
            return TimedMemoSimulation.choose(self, row, ready)
        began = time.perf_counter()
        costs = self.frozen()
        demand = replace(self.item(row), arrival=self.now)
        raw_plans = self.feasible_candidates(row, demand, costs)
        groups = {}
        for plan, raw in raw_plans.items():
            score = (
                plan.history_copy_seconds
                + plan.p_first_seconds
                + plan.handoff_seconds
                + plan.d_first_seconds,
                plan.d_full_seconds,
                raw.extra_bytes,
                plan.source,
                plan.p_prefix,
                plan.d_prefix,
            )
            pair = plan.p, plan.d
            if pair not in groups or score < groups[pair][0]:
                groups[pair] = score, plan, raw
        queues = self.policy_queues
        with queues.decision(demand, self.layout, costs, ready, self.now):
            forecasts = [
                (queues.forecast(raw, self.now), plan)
                for _, plan, raw in groups.values()
            ]
            route, plan = min(forecasts, key=lambda pair: route_order(pair[0]))
            queues.reserve(row["id"], route, self.now)
        snapshot = Snapshot(
            Request(row["id"], row["task"], row["tokens"], ready, self.sequence),
            self.now,
            self.workers(row, costs),
            (plan,),
            frozenset((row["id"],)),
        )
        seconds = time.perf_counter() - began
        self.planning.append(
            {
                "id": row["id"],
                "origin": self.now,
                "model_latest": costs.latest_completed_seconds,
                "CPU_wall": seconds,
                "candidates": len(raw_plans),
                "full_calendar_candidates": len(forecasts),
                "initial_forecast": {
                    "p": route.p,
                    "d": route.d,
                    "source": route.source,
                    "P_prefix_command": route.p_blocks * 128,
                    "D_prefix_command": route.d_blocks * 128,
                    "P_end": route.predicted_p_end,
                    "first": route.predicted_first,
                    "full": route.predicted_end,
                    "origin": route.forecast_origin_seconds,
                    "latest_completed": route.model_latest_completed_seconds,
                    "not_after_P_revised_forecast": True,
                },
            }
        )
        return plan, candidate_work(snapshot, plan), seconds, route

    def run(self):
        result = super().run()
        result.pop("paper_adapter", None)
        result["internal_candidate"] = {
            "name": "service-portfolio",
            "calendar_candidates": "one per physical P/D pair",
            "route_endpoints": "local/source-max/zero current prefixes",
            "D_boundary": "all feasible current prefixes, singleton service+copy cost",
            "global_boundary_optimum_claimed": False,
            "all_costs_charged": True,
        }
        return result
