"""Couple the batch forecast to admitted-work publication and physical reclaim.

Candidate only: the execution world, route budget, objective and serving costs
stay unchanged. No future request or execution duration enters this projection.
"""

from dataclasses import replace
from types import MethodType

from batch_calendar import schedule_batched
from bounded_search import (
    ForecastDominated,
    ProofMemoQueues,
    ProofMemoSimulation,
    ProofMemoSnapshot,
    first_lower_bound,
)
from pegaflow.detached_calendar import token
from pegaflow.joint_policy import copy_package
from pegaflow.operation_schedule import Operation
from pegaflow.resource_cache_profile import CacheForecast
from pegaflow.resource_queue import direction
from pegaflow.strategies.continuous_batch import AllocationLeases


class StageCacheProjection:
    """Mirror stage acceptance, owned leases, copy commit and synchronous save."""

    def __init__(
        self, layout, demands, routes, costs, reclaim, handles, intrinsic, memo
    ):
        self.state = CacheForecast(layout, demands, routes, costs, reclaim, memo=memo)
        self.costs, self.routes, self.demands = costs, routes, demands
        self.accepted, self.quotes, self.copies = {}, {}, {}
        self.restored_host_owners = set()
        self.D_save_cursors = {}
        self.intrinsic = intrinsic
        self.leases = AllocationLeases()
        for (owner, node), identities in handles.items():
            self.leases.acquire(owner, node, identities)

    def effective_route(self, key, node):
        return replace(
            self.routes[key],
            **{"p_blocks" if node < 2 else "d_blocks": self.accepted[key, node]},
        )

    def api(self, key, node):
        demand = self.demands[key]
        count = self.accepted[key, node]
        first = self.costs.compute(node, demand.tokens, count * demand.block_tokens)
        full = first + (self.costs.decode_tail(node, demand.tokens) if node >= 2 else 0)
        self.quotes[key, node] = {
            "request": key,
            "node": node,
            "effective_blocks": count,
            "prefix_tokens": count * demand.block_tokens,
            "first_service_seconds": first,
            "full_service_seconds": full,
        }
        return Operation(
            "P_api" if node < 2 else "D_api",
            full,
            (f"gate{node}",),
            first if node >= 2 else None,
        )

    def admit(self, work, node, previous, progress):
        if node >= 2 and progress is not None and getattr(self.state, "incremental_D_save", False):
            self.D_save_cursors[work.key] = (progress.prefix_tokens + progress.prefilled + max(0, progress.decoded - 1)) // self.demands[work.key].block_tokens
        if (node >= 2 and getattr(self.state, "restore_scoped_host_lease", False)
                and ((progress is not None and progress.restored) or work.actual_first is not None)):
            self.restored_host_owners.add((work.key, "D_api"))
        demand, route = self.demands[work.key], self.routes[work.key]
        count = route.p_blocks if node < 2 else route.d_blocks
        # Already issued chains retain their observed acceptance, including
        # a local fallback. They are not repriced as a newly admitted request.
        started = work.p_started if node < 2 else work.d_started
        active_names = {name for name, _ in work.active_operations}
        stage_names = {"P_api", "history"} if node < 2 else {"D_api", "handoff"}
        if started is not None or active_names & stage_names:
            if node >= 2 and progress is not None:
                count = progress.prefix_tokens // demand.block_tokens
            self.accepted[work.key, node] = count
            self.api(work.key, node)
            return previous
        layout = self.state.layout
        source = route.source if node < 2 else route.p
        local = layout.prefix(node, demand.hashes)
        count = min(count, max(local, layout.prefix(source, demand.hashes)))
        copied = []
        if source != node and count > local:
            try:
                operation = copy_package(
                    layout, source, node, demand.hashes[:count]
                ).operations[-1]
            except ValueError:
                count = min(count, local)
            else:
                name = "history" if node < 2 else "handoff"
                self.copies[work.key, name] = source, node, demand.hashes[:count]
                copied.append(
                    Operation(
                        name,
                        self.costs.copy_seconds(source, node, operation.payload_bytes),
                        (
                            f"gate{source}",
                            f"gate{node}",
                            "link:" + direction(source, node),
                        ),
                    )
                )
        self.accepted[work.key, node] = count
        return (*copied, self.api(work.key, node))

    def sync_protection(self, node):
        nodes = list(self.state.layout.units)
        nodes[node] = tuple(
            replace(
                u,
                protected=(node, u.identity) in self.intrinsic
                or self.leases.protected(node, u.identity),
            )
            for u in nodes[node]
        )
        if getattr(self.state, "publication_model", None) is not None:
            nodes[node] = tuple(u for u in nodes[node] if u.keys or u.protected or u.virtual)
        self.state.layout = replace(self.state.layout, units=tuple(nodes))

    def start(self, work, node, operation):
        if operation.name not in ("P_api", "D_api", "history", "handoff"):
            return
        route = self.routes[work.key]
        copying = operation.name in ("history", "handoff")
        target = (route.source if node < 2 else route.p) if copying else node
        owner = work.key, operation.name
        if owner in self.restored_host_owners:
            return
        if (owner, target) in self.leases.handles:
            return
        count = self.accepted[work.key, node]
        keys = set(self.demands[work.key].hashes[:count])
        units = self.state.layout.units[target]
        self.leases.acquire(owner, target, (u.identity for u in units if u.keys & keys))
        # Match execution's owned allocation protection; do not invent LRU
        # touches that the retained execution model does not perform.
        self.sync_protection(target)

    def finish_restore(self, key, node, at):
        if not getattr(self.state, "restore_scoped_host_lease", False):
            return
        owner = key, "D_api"
        if (owner, node) in self.leases.handles:
            self.leases.release(owner, node)
            self.sync_protection(node)
        self.restored_host_owners.add(owner)
        self.state.events.append({"kind": "forecast_restore_lease_release", "request": key,
                                  "node": node, "at": at})

    def finish(self, work, node, operation):
        copying = operation.name in ("history", "handoff")
        if operation.name not in ("P_api", "D_api", "history", "handoff"):
            return
        route = self.routes[work.key]
        target = (route.source if node < 2 else route.p) if copying else node
        owner = work.key, operation.name
        if (owner, target) in self.leases.handles:
            self.leases.release(owner, target)
            self.sync_protection(target)
        if copying:
            count = self.accepted[work.key, node]
            hashes = self.demands[work.key].hashes[:count]
            self.state.pending_copies[work.key, operation.name] = target, node, hashes
            self.state.finished(work, node, operation, 0)
            self.accepted[work.key, node] = min(
                count, self.state.layout.prefix(node, hashes)
            )
        elif node >= 2 and getattr(self.state, "incremental_D_save", False):
            pass  # All completed block deltas were published by iteration callbacks.
        else:
            self.state.save(node, self.demands[work.key])

    def finish_compute(self, key, node, sequence, at):
        if not getattr(self.state, "incremental_D_save", False):
            return
        demand = self.demands[key]
        end = (sequence.prefix_tokens + sequence.prefilled + max(0, sequence.decoded - 1)) // demand.block_tokens
        begin = self.D_save_cursors.get(key, sequence.prefix_tokens // demand.block_tokens)
        if end > begin:
            self.state.event_time = at
            self.state.save_delta(node, demand, begin, end)
        self.D_save_cursors[key] = end

    def after_copy(self, work, node, operation, previous):
        if operation.name not in ("history", "handoff"):
            return previous
        updated = self.api(work.key, node)
        return tuple(updated if op.name == updated.name else op for op in previous)


class CacheEvolvingSnapshot(ProofMemoSnapshot):
    projection_type = StageCacheProjection

    def configure_projection(self, projection, now):
        return {}

    def refresh(self, layout, costs):
        self.projected_layout = layout
        return super().refresh(layout, costs)

    def predict_batch(self, work, now, demands, routes, costs):
        self.phase_costs = costs
        # Full route identities are required: equal current service chains can
        # have different future cache publication and physical owner closures.
        identity = tuple(work), tuple(sorted((k, token(v)) for k, v in routes.items()))
        memo = self.__dict__.setdefault("cache_batch_memo", {})
        if identity in memo:
            self.search_stats["calendar_reused"] += 1
            return memo[identity]
        bound = getattr(self, "incumbent_first_profile", None)
        pending = tuple((w.key, w.offered) for w in work if w.actual_first is None)

        def guard(clock, first):
            self.search_stats["bound_checks"] += 1
            if bound is not None and first_lower_bound(pending, first, clock) > bound:
                self.search_stats["pruned_calendars"] += 1
                raise ForecastDominated(
                    "Cache-evolving first-token bound exceeds incumbent"
                )

        projection = self.projection_type(
            self.projected_layout,
            demands,
            routes,
            costs,
            self.reclaim_batch,
            self.cache_handles,
            self.cache_intrinsic,
            self.projection_memo,
        )
        cache_type = getattr(self, "forecast_cache_type", None)
        if cache_type is not None:
            projection.state.__class__ = cache_type
            projection.sync_protection = MethodType(StageCacheProjection.sync_protection, projection)
        remover = getattr(self, "forecast_reclaim_remover", None)
        if remover is not None:
            projection.state.remove_victims = remover
        schedule_options = self.configure_projection(projection, now)
        override = getattr(self, "forecast_batch_factory", None)
        if override is not None:
            schedule_options["batch_factory"] = override
        # ControlReleaseSnapshot supplies its own configuration dictionary.
        # Arithmetic is a calendar contract, not an optional phase override.
        schedule_options["exact_timeline"] = getattr(self, "exact_timeline", False)
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
            cache_state=projection,
            **schedule_options,
        )
        prediction["reclaim_removal_method"] = getattr(
            projection.state, "reclaim_method", projection.state.remove_victims.__name__)
        prediction["save_model"] = getattr(projection.state, "publication_model", "legacy-immediate")
        prediction["reclaim_removal_calls"] = projection.state.reclaim_removal_calls
        memo[identity] = prediction
        expected = (
            "binary64-exact-dyadic" if schedule_options["exact_timeline"] else "float"
        )
        if prediction["timeline_arithmetic"] != expected:
            raise ValueError("Forecast calendar did not apply requested arithmetic")
        self.search_stats["calendar_full"] += 1
        self.last_cache_prediction = prediction
        return prediction

    def forecast(self, route, now):
        selected = super().forecast(route, now)
        # Retrieve the completed prediction for this exact selected route; no
        # actual later cache acceptance is substituted into the original quote.
        for (work, routes), prediction in self.cache_batch_memo.items():
            if dict(routes).get("__candidate__") == token(selected):
                self.quoted_routes[token(selected)].update(
                    effective_API=[
                        q
                        for q in prediction["effective_API"]
                        if q["request"] == "__candidate__"
                    ],
                    cache_evolution_shadow_executed=True,
                    future_P_publication_assumed="projected admitted work, not a guaranteed hit",
                    forecast_cache_events=prediction["cache_events"],
                    forecast_time_arithmetic=prediction["timeline_arithmetic"],
                    forecast_batch_factory=prediction["batch_factory"],
                    forecast_save_model=prediction["save_model"],
                    reclaim_removal_method=prediction["reclaim_removal_method"],
                    reclaim_removal_calls=prediction["reclaim_removal_calls"],
                )
                break
        return selected


class CacheEvolvingQueues(ProofMemoQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = CacheEvolvingSnapshot
        shadow.exact_timeline = getattr(self, "exact_timeline", False)
        shadow.forecast_reclaim_remover = getattr(self, "forecast_reclaim_remover", None)
        shadow.forecast_batch_factory = getattr(self, "forecast_batch_factory", None)
        shadow.forecast_cache_type = getattr(self, "forecast_cache_type", None)
        shadow.cache_handles, shadow.cache_intrinsic = self.cache_lease_reader()
        shadow.projection_memo = shadow.new_memo()
        return shadow


class CacheEvolvingSimulation(ProofMemoSimulation):
    queue_type = CacheEvolvingQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy_queues.cache_lease_reader = lambda: (
            dict(self.allocation_leases.handles),
            self.intrinsic_protection,
        )

    def run(self):
        result = super().run()
        result["initial_cache_forecasts"] = self.policy_queues.selected_quotes
        result["internal_candidate"].update(
            name="cache-evolving-batch-01",
            physical_cache_evolves_with_admitted_work=True,
            execution_world_unchanged=True,
            real_serving_validated=False,
        )
        return result
