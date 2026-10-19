"""Cheap owned-resource routing; no speculative future cache simulation.

Only published/current imported P prefixes can be quoted for a future D step.
After actual P publication, D selects its boundary again. H2D, both device
gates, directional copies, full decode and completed residuals remain priced.
Expensive cache-evolution simulation is optional repair work, not256 simulations
on every fast routing decision. No task/rate threshold or balancing quota.
"""

from dataclasses import replace
import itertools

from pegaflow.detached_control import DetachedQueues
from pegaflow.joint_policy import copy_package
from pegaflow.operation_schedule import schedule
from pegaflow.resource_cache_profile import CacheProfileQueues, CacheRoute, optional_work
from pegaflow.resource_state_profile import refresh_work
from pegaflow.pending_planner import pending_profile


def token(route):
    return tuple(getattr(route, key) for key in
                 ("p", "d", "source", "p_blocks", "d_blocks", "p_headroom", "d_headroom"))


class CalendarSnapshot(CacheProfileQueues):
    def forecast(self, route, now):
        demand, layout, costs, offered, origin = self.context
        # Cache that the current P step has not published is not a free hit.
        published = max(layout.prefix(route.d, demand.hashes),
                        layout.prefix(route.p, demand.hashes), route.p_blocks)
        option = CacheRoute(**{**route.__dict__, "d_blocks": min(route.d_blocks, published),
                              "p_headroom": False, "d_headroom": False})
        self.refresh(layout, costs)
        candidate = optional_work("__candidate__", self.sequence, offered, demand, option, layout, costs)
        work = (*self.work.values(), candidate)
        prediction = schedule(work, origin, self._remaining_operation)
        value = pending_profile(work, prediction)
        selected = replace(option, predicted_p_end=prediction["P_end"][candidate.key],
            predicted_first=prediction["first"][candidate.key], predicted_end=prediction["full"][candidate.key],
            predicted_system_p99=value[0], predicted_tail_profile=value[1],
            predicted_full_profile=value[2], predicted_P_release_profile=value[3],
            operation_model_version=costs.version, model_latest_completed_seconds=costs.latest_completed_seconds,
            forecast_origin_seconds=origin,
            objective_scope="pending-first resource calendar; only current published prefixes")
        self.quoted_routes[token(selected)] = dict(origin=origin, latest_completed=costs.latest_completed_seconds,
            model_version=costs.version,
            effective_API=[dict(request=candidate.key, node=node, prefix_tokens=length*demand.block_tokens,
                                effective_blocks=length,
                                first_service_seconds=costs.compute(node, demand.tokens, length*demand.block_tokens),
                                full_service_seconds=costs.compute(node, demand.tokens, length*demand.block_tokens)
                                  + (costs.decode_tail(node, demand.tokens) if node >= 2 else 0),
                                model_API_seconds=costs.compute(node, demand.tokens, length*demand.block_tokens))
                           for node, length in ((selected.p, selected.p_blocks), (selected.d, selected.d_blocks))],
            predicted_API_intervals=[e for e in prediction["operations"] if e["key"] == candidate.key],
            P_start=prediction["stage_start"].get((candidate.key, selected.p)),
            D_start=prediction["stage_start"].get((candidate.key, selected.d)),
            current_decision_CPU_not_prospectively_predicted=True,
            future_P_publication_assumed=False, cache_evolution_shadow_executed=False)
        return selected

    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
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
                    payload = copy_package(layout, route.p, route.d, item.hashes[:length]).operations[-1].payload_bytes
                except ValueError:
                    continue
            option = replace(route, d_blocks=length, d_headroom=False)
            changed = refresh_work(current, optional_work(key, current.order, current.offered,
                                                         item, option, layout, costs))
            work = tuple(changed if w.key == key else w for w in self.work.values())
            prediction = schedule(work, now, self._remaining_operation)
            value = pending_profile(work, prediction), payload, length
            if best is None or value < best[0]:
                best = value, option, costs.compute(route.d, item.tokens, length*item.block_tokens)
        if best is None:
            raise ValueError("No published boundary including safe recompute")
        self.admission_choices[key, route.d] = False
        self.revise(key, best[1], layout, costs)
        return best[1].d_blocks, best[2]


class DetachedCalendarQueues(DetachedQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = CalendarSnapshot
        return shadow

    def reserve(self, key, route, now):
        shadow = self.local_decision.shadow
        quote = shadow.quoted_routes[token(route)]
        super().reserve(key, route, now)
        with self.guard:
            self.selected_quotes[key] = {**quote,
                "effective_API": [{**q, "request": key} for q in quote["effective_API"]]}

    def revise(self, key, route, layout, costs):
        with self.guard:
            super().revise(key, route, layout, costs)
            self.progress_version += 1
