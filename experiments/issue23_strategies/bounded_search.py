"""Exact early termination inside the retained, budgeted candidate search.

Unobserved predicted first tokens cannot precede the calendar's current event.
Their resulting ordered latency profile is an admissible lower bound. Only a
strictly worse bound is pruned; equality must preserve release/byte tie breaks.
This does not claim that the existing 64-candidate budget is globally optimal.
"""

import itertools
import math
from dataclasses import replace

from batch_calendar import schedule_batched
from memo_search import MemoSnapshot
from pegaflow.joint_policy import copy_package
from pegaflow.pending_planner import pending_profile
from pegaflow.resource_cache_profile import optional_work
from pegaflow.resource_state_profile import refresh_work
from timed_recovery_search import TimedMemoQueues, TimedMemoSimulation


class ForecastDominated(ValueError):
    """A feasible candidate cannot beat the current exact incumbent."""


def first_lower_bound(pending, first, clock):
    latencies = tuple(
        sorted(
            (first.get(key, clock) - arrival for key, arrival in pending), reverse=True
        )
    )
    if not latencies:
        raise ValueError("A first-token objective needs a pending request")
    return latencies[len(latencies) - math.ceil(0.99 * len(latencies))], latencies


class BoundedSnapshot(MemoSnapshot):
    def predict_batch(self, work, now, demands, routes, costs):
        self.phase_costs = costs
        memo = self.__dict__.setdefault("batch_memo", {})
        identity = (
            tuple(work),
            tuple(sorted((key, routes[key].d_blocks) for key in demands)),
        )
        if identity in memo:
            self.search_stats["calendar_reused"] += 1
            return memo[identity]
        pending = tuple((w.key, w.offered) for w in work if w.actual_first is None)
        bound = getattr(self, "incumbent_first_profile", None)

        def guard(clock, first):
            self.search_stats["bound_checks"] += 1
            if bound is not None and first_lower_bound(pending, first, clock) > bound:
                self.search_stats["pruned_calendars"] += 1
                raise ForecastDominated("First-token lower bound exceeds incumbent")

        prediction = schedule_batched(
            work,
            now,
            self._remaining_operation,
            demands,
            routes,
            costs,
            self.batch_config,
            self.batch_progress,
            iteration_overhead=getattr(self, "iteration_overhead", None),
            first_profile_guard=guard if bound is not None else None,
        )
        # Never cache incomplete calendars or carry incumbents across snapshots.
        memo[identity] = prediction
        self.search_stats["calendar_full"] += 1
        return prediction

    def forecast(self, route, now):
        result = super().forecast(route, now)
        value = result.predicted_system_p99, result.predicted_tail_profile
        old = getattr(self, "incumbent_first_profile", None)
        if old is None or value < old:
            self.incumbent_first_profile = value
        return result

    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
        # Same order, budget, feasibility and tie breaks as BatchedSnapshot.
        self.incumbent_first_profile = None
        self.refresh(layout, costs)
        item, current = self.demands[key], self.work[key]
        local = layout.prefix(route.d, item.hashes)
        limit = max(layout.prefix(route.p, item.hashes), local)
        lengths = dict.fromkeys((local, limit, 0, *range(limit - 1, 0, -1)))
        best = None
        for length in itertools.islice(lengths, max_candidates):
            payload = 0
            if length > local:
                try:
                    payload = (
                        copy_package(layout, route.p, route.d, item.hashes[:length])
                        .operations[-1]
                        .payload_bytes
                    )
                except ValueError:
                    continue
            option = replace(route, d_blocks=length, d_headroom=False)
            changed = refresh_work(
                current,
                optional_work(
                    key, current.order, current.offered, item, option, layout, costs
                ),
            )
            work = tuple(changed if w.key == key else w for w in self.work.values())
            try:
                prediction = self.predict_batch(
                    work, now, self.demands, {**self.routes, key: option}, costs
                )
            except ForecastDominated:
                continue
            value = pending_profile(work, prediction), payload, length
            if best is None or value < best[0]:
                best = value, option
                self.incumbent_first_profile = value[0][:2]
        if best is None:
            raise ValueError("No published D boundary including recomputation")
        self.admission_choices[key, route.d] = False
        self.revise(key, best[1], layout, costs)
        return best[1].d_blocks, costs.compute(
            route.d, item.tokens, best[1].d_blocks * item.block_tokens
        )


class BoundedQueues(TimedMemoQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = BoundedSnapshot
        shadow.incumbent_first_profile = None
        shadow.search_stats.update(bound_checks=0, pruned_calendars=0)
        return shadow


class BoundedSimulation(TimedMemoSimulation):
    queue_type = BoundedQueues

    def run(self):
        result = super().run()
        # Keep static candidate-set pruning distinct from aborting a provably
        # losing forecast. Old archives retain their inherited field verbatim.
        result["candidate_set_changed"] = False
        result["forecast_early_termination"] = True
        result["internal_candidate"] = {
            "name": "bounded-calendar-01",
            "same_candidate_order_and_budget": True,
            "strict_admissible_first_profile_bound": True,
            "tie_breaks_preserved": True,
            "all_measured_costs_charged": True,
            "real_serving_validated": False,
        }
        return result


class ProofMemoSnapshot(BoundedSnapshot):
    """Reuse a rejected calendar only under an equal or stronger incumbent."""

    def predict_batch(self, work, now, demands, routes, costs):
        identity = (
            tuple(work),
            tuple(sorted((key, routes[key].d_blocks) for key in demands)),
        )
        proofs = self.__dict__.setdefault("rejected_calendars", {})
        current = getattr(self, "incumbent_first_profile", None)
        if current is not None and identity in proofs and current <= proofs[identity]:
            self.search_stats["pruned_proof_reused"] += 1
            raise ForecastDominated("Same calendar already rejected by a weaker bound")
        try:
            return super().predict_batch(work, now, demands, routes, costs)
        except ForecastDominated:
            proofs[identity] = current
            raise

    def forecast(self, route, now):
        proofs = self.__dict__.setdefault("rejected_routes", {})
        current = getattr(self, "incumbent_first_profile", None)
        if current is not None and route in proofs and current <= proofs[route]:
            self.search_stats["pruned_route_reused"] += 1
            raise ForecastDominated("Same route already rejected by a weaker bound")
        try:
            return super().forecast(route, now)
        except ForecastDominated:
            proofs[route] = current
            raise


class ProofMemoQueues(BoundedQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = ProofMemoSnapshot
        shadow.search_stats.update(pruned_proof_reused=0, pruned_route_reused=0)
        return shadow


class ProofMemoSimulation(BoundedSimulation):
    queue_type = ProofMemoQueues

    def run(self):
        result = super().run()
        result["internal_candidate"].update(
            name="bounded-proof-memo-02", rejection_proofs_decision_local=True
        )
        return result
