"""Coupled admitted-work cache/resource forecast; optional priced headroom.

Native reclaim geometry comes from the archived implementation, never a tuned
load threshold. Unknown generated content occupies space but cannot be reused.
This is a CPU policy prototype, not an allocator/native execution certificate.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import contextlib
import hashlib
import itertools
import time

from pegaflow.joint_policy import Copy, Plan, Transition, Unit, copy_package, neighbors
from pegaflow.decision_cost import DecisionCosts
from pegaflow.operation_schedule import Operation, schedule
from pegaflow.resource_context_profile import context_work
from pegaflow.resource_headroom_profile import native_headroom
from pegaflow.resource_placement import relevant_history
from pegaflow.resource_profile import ProfileRoute, profile
from pegaflow.resource_queue import direction
from pegaflow.resource_state_profile import StateProfileQueues, refresh_work


@dataclass(frozen=True)
class CacheRoute(ProfileRoute):
    p_headroom: bool = False
    d_headroom: bool = False


def optional_work(key, order, offered, demand, route, layout, costs):
    work = context_work(key, order, offered, demand, route, layout, costs)
    def chain(node, operations):
        enabled = getattr(route, "p_headroom" if node < 2 else "d_headroom", False)
        if enabled:
            name = "P_admission" if node < 2 else "D_admission"
            return (Operation(name, costs.admission_seconds, (f"gate{node}",)), *operations)
        return operations
    return replace(work, prefill=chain(route.p, work.prefill), decode=chain(route.d, work.decode))


class CacheForecast:
    def __init__(self, layout, demands, routes, costs, reclaim_batch, output_tokens=128, memo=None):
        if type(reclaim_batch) is not int or reclaim_batch < 1:
            raise ValueError("Source-derived native reclaim batch required")
        self.layout, self.demands, self.routes, self.costs = layout, demands, routes, costs
        self.reclaim_batch, self.output_tokens = reclaim_batch, output_tokens
        self.pending_copies = {}
        self.events = []
        self.api_quotes = {}
        self.reclaim_removal_calls = 0
        self.memo = memo
        if memo is not None and "node_indexes" in memo:
            from pegaflow.projection_index import IndexedLayout
            self.layout = IndexedLayout.wrap(layout, memo["node_indexes"])

    def prepare(self, work, node, completed):
        demand, route = self.demands[work.key], self.routes[work.key]
        role = "P" if node < 2 else "D"
        api_name = role + "_api"
        if api_name in completed:
            return ()
        guard_name = role + "_admission"
        enabled = getattr(route, "p_headroom" if node < 2 else "d_headroom", False)
        projected = self.layout
        if enabled and guard_name not in completed:
            plan = native_headroom(projected, node, demand, self.costs, self.output_tokens)
            if plan and plan.victims:
                # Only the guard is executable now. Rebuild its successor from
                # the resulting layout, not a stale pre-removal source quote.
                return (Operation(guard_name, self.costs.admission_seconds, (f"gate{node}",)),)
        source = route.source if node < 2 else route.p
        count = route.p_blocks if node < 2 else route.d_blocks
        local = projected.prefix(node, demand.hashes)
        available = max(local, projected.prefix(source, demand.hashes))
        count = min(count, available)
        copy_name = "history" if node < 2 else "handoff"
        if copy_name not in completed and source != node and count > local:
            try:
                transition = copy_package(projected, source, node, demand.hashes[:count])
            except ValueError:
                count = 0
            else:
                op = transition.operations[-1]
                self.pending_copies[work.key, copy_name] = (source, node, demand.hashes[:count])
                return (Operation(copy_name, self.costs.copy_seconds(source, node, op.payload_bytes),
                    (f"gate{source}", f"gate{node}", "link:" + direction(source, node))),)
        elif copy_name in completed:
            count = min(count, projected.prefix(node, demand.hashes))
        prefix = count * demand.block_tokens
        first = self.costs.compute(node, demand.tokens, prefix)
        self.api_quotes[work.key, node] = dict(request=work.key, node=node, source=source,
            effective_blocks=count, local_prefix=self.layout.prefix(node, demand.hashes),
            source_prefix=self.layout.prefix(source, demand.hashes), first_service_seconds=first,
            full_service_seconds=first + (self.costs.decode_tail(node, demand.tokens) if node >= 2 else 0))
        if node < 2:
            return (Operation(api_name, first, (f"gate{node}",)),)
        return (Operation(api_name, first + self.costs.decode_tail(node, demand.tokens),
                          (f"gate{node}",), first),)

    @staticmethod
    def remove_victims(units, victims):
        for unit in victims:
            units.remove(unit)
        return units

    def save(self, node, demand):
        node_state = (self.memo["node_indexes"].get(self.layout.units[node])
                      if self.memo is not None and "node_indexes" in self.memo
                      else self.layout.units[node])
        memo_key = (node, node_state, self.layout.hidden[node],
                    self.layout.capacities[node], demand.key, demand.tokens, demand.hashes,
                    demand.block_tokens, self.costs.bytes_per_token, self.output_tokens, self.reclaim_batch)
        if self.memo is not None and memo_key in self.memo["saves"]:
            self.memo["hits"] += 1
            units, events = self.memo["saves"][memo_key]
            nodes = list(self.layout.units)
            nodes[node] = units
            self.layout = replace(self.layout, units=tuple(nodes))
            self.events.extend(dict(e) for e in events)
            return
        if self.memo is not None:
            self.memo["misses"] += 1
        prior_events = len(self.events)
        units = list(self.layout.units[node])
        present = set(self.layout.keys(node))
        used = self.layout.usage(node)
        block_bytes = demand.block_tokens * self.costs.bytes_per_token
        computed = demand.tokens + (self.output_tokens - 1 if node >= 2 else 0)
        extra = computed // demand.block_tokens - len(demand.hashes)
        unknown = tuple(hashlib.sha256(
            b"forecast-space-only-not-reusable\0" + demand.key.encode() + bytes((node,))
            + i.to_bytes(8, "little")).digest() for i in range(extra))
        for key in (*demand.hashes, *unknown):
            if key in present:
                continue
            while used + block_bytes > self.layout.capacities[node]:
                eligible = [u for u in units if not u.protected and not u.virtual]
                if not eligible:
                    break
                victims, count = [], 0
                for unit in eligible:
                    victims.append(unit)
                    count += len(unit.keys)
                    if count >= self.reclaim_batch:
                        break
                units = self.remove_victims(units, victims)
                self.reclaim_removal_calls += 1
                for unit in victims:
                    used -= unit.charged
                    present.difference_update(unit.keys)
                self.events.append(dict(kind="forecast_native_reclaim", node=node,
                    logical_blocks=count, charged=sum(u.charged for u in victims)))
            if used + block_bytes <= self.layout.capacities[node]:
                units.append(Unit(f"forecast:{node}:{key.hex()}", frozenset((key,)),
                                  block_bytes, ((key, block_bytes),)))
                used += block_bytes
                present.add(key)
        nodes = list(self.layout.units)
        nodes[node] = tuple(units)
        self.layout = replace(self.layout, units=tuple(nodes))
        if self.memo is not None:
            self.memo["saves"][memo_key] = tuple(units), tuple(dict(e) for e in self.events[prior_events:])

    def started(self, work, node, operation, at):
        demand, route = self.demands[work.key], self.routes[work.key]
        if operation.name in ("history", "handoff"):
            source = route.source if node < 2 else route.p
            count = route.p_blocks if node < 2 else route.d_blocks
            target = source
        elif operation.name in ("P_api", "D_api"):
            count = route.p_blocks if node < 2 else route.d_blocks
            target = node
        else:
            return
        keys = set(demand.hashes[:count])
        nodes = list(self.layout.units)
        units = nodes[target]
        nodes[target] = tuple(u for u in units if not u.keys.intersection(keys)) + tuple(
            u for u in units if u.keys.intersection(keys))
        self.layout = replace(self.layout, units=tuple(nodes))

    def finished(self, work, node, operation, at):
        demand, route = self.demands[work.key], self.routes[work.key]
        if operation.name in ("P_admission", "D_admission"):
            plan = native_headroom(self.layout, node, demand, self.costs, self.output_tokens)
            if plan and plan.victims:
                self.layout = self.layout.remove(node, plan.victims)
                self.events.append(dict(kind="forecast_selected_headroom", request=work.key,
                                        node=node, charged=plan.released_bytes))
        elif operation.name in ("history", "handoff"):
            selected = self.pending_copies.get((work.key, operation.name))
            if selected is None:
                source = route.source if node < 2 else route.p
                count = route.p_blocks if node < 2 else route.d_blocks
                selected = source, node, demand.hashes[:count]
            source, destination, hashes = selected
            try:
                transition = copy_package(self.layout, source, destination, hashes)
            except ValueError:
                # An in-flight copy may already be visible in the supplied
                # inventory; do not invent missing publication or capacity.
                return
            payloads = self.layout.payload_maps[source]
            nodes = list(self.layout.units)
            missing = transition.operations[-1].missing
            nodes[destination] = (*nodes[destination], *(Unit(
                f"forecast-copy:{destination}:{key.hex()}", frozenset((key,)),
                ((payloads[key] + 4095) // 4096) * 4096, ((key, payloads[key]),)) for key in missing))
            self.layout = replace(self.layout, units=tuple(nodes))
            if not self.layout.feasible():
                raise ValueError("Forecast copy exceeded quoted physical staging")
        elif operation.name in ("P_api", "D_api"):
            self.save(node, demand)


def cache_schedule(work, now, layout, demands, routes, costs, reclaim_batch,
                   remaining=None, resource_blocks=None, memo=None):
    state = CacheForecast(layout, demands, routes, costs, reclaim_batch, memo=memo)
    result = schedule(work, now, remaining, resource_blocks,
                      chain_builder=state.prepare, operation_finished=state.finished,
                      operation_started=state.started)
    result["forecast_cache_events"] = state.events
    result["shadow_api_quotes"] = tuple(state.api_quotes.values())
    return result


class CacheProfileQueues(StateProfileQueues):
    work_builder = staticmethod(optional_work)

    def __init__(self, models, reclaim_batch, *, memoize=False, predict_control=False,
                 index_projection=False):
        super().__init__(models)
        self.reclaim_batch = reclaim_batch
        self.admission_choices = {}
        self.cache_forecast_calls = 0
        self.memoize, self.predict_control = memoize, predict_control
        if index_projection and not memoize:
            raise ValueError("Projection indexes must have a bounded decision memo scope")
        self.index_projection = index_projection
        self.projection_memo = None
        self.memo_totals = dict(hits=0, misses=0, peak_entries=0)
        self.index_totals = dict(hits=0, peak_entries=0)
        self.control_costs = DecisionCosts()
        self.last_route_estimate = self.last_boundary_estimate = None
        self.quoted_routes, self.selected_quotes = {}, {}

    def new_memo(self):
        memo = dict(saves={}, hits=0, misses=0) if self.memoize else None
        if self.index_projection:
            from pegaflow.projection_index import NodeIndexes
            memo["node_indexes"] = NodeIndexes()
        return memo

    def record_memo(self, memo):
        if memo is not None:
            self.memo_totals["hits"] += memo["hits"]
            self.memo_totals["misses"] += memo["misses"]
            self.memo_totals["peak_entries"] = max(self.memo_totals["peak_entries"], len(memo["saves"]))
            if "node_indexes" in memo:
                indexes = memo["node_indexes"]
                self.index_totals["hits"] += indexes.hits
                self.index_totals["peak_entries"] = max(self.index_totals["peak_entries"], len(indexes.values))

    @contextlib.contextmanager
    def memo_scope(self):
        previous = self.projection_memo
        self.projection_memo = self.new_memo()
        try:
            yield
        finally:
            self.record_memo(self.projection_memo)
            self.projection_memo = previous

    @contextlib.contextmanager
    def decision(self, demand, layout, costs, offered, now):
        with super().decision(demand, layout, costs, offered, now), self.memo_scope():
            demands = {**self.demands, demand.key: demand}
            self.last_route_estimate = (self.control_costs.estimate(
                "route", demands.values(), layout, 256, now) if self.predict_control else None)
            self.quoted_routes = {}
            yield

    def predict(self, work, now, layout, demands, routes, costs, resource_blocks=None, memo=None):
        self.cache_forecast_calls += 1
        return cache_schedule(work, now, layout, demands, routes, costs, self.reclaim_batch,
                              self._remaining_operation, resource_blocks,
                              self.projection_memo if memo is None else memo)

    def forecast(self, route, now):
        with self.guard:
            demand, layout, costs, offered, origin = self.context
            self.refresh(layout, costs)
            best = None
            for p_guard, d_guard in itertools.product((False, True), repeat=2):
                option = CacheRoute(**{**route.__dict__, "p_headroom": p_guard, "d_headroom": d_guard})
                candidate = optional_work("__candidate__", self.sequence, offered, demand, option, layout, costs)
                if self.last_route_estimate is not None:
                    candidate = replace(candidate, p_not_before=origin + self.last_route_estimate.seconds)
                work = (*self.work.values(), candidate)
                prediction = self.predict(work, origin, layout,
                    {**self.demands, candidate.key: demand}, {**self.routes, candidate.key: option}, costs)
                value = profile(work, prediction)
                order = value, p_guard + d_guard, p_guard, d_guard
                if best is None or order < best[0]:
                    selected = replace(option, predicted_p_end=prediction["P_end"][candidate.key],
                        predicted_first=prediction["first"][candidate.key],
                        predicted_end=prediction["full"][candidate.key], predicted_system_p99=value[0],
                        predicted_tail_profile=value[1], predicted_full_profile=value[2],
                        predicted_P_release_profile=value[3], operation_model_version=costs.version,
                        model_latest_completed_seconds=costs.latest_completed_seconds,
                        forecast_origin_seconds=origin,
                        objective_scope="admitted-live-conventional-tail-with-causal-cache-evolution")
                    best = order, selected
                    best_quote = dict(origin=origin, latest_completed=costs.latest_completed_seconds,
                        model_version=costs.version,
                        effective_API=[q for q in prediction["shadow_api_quotes"] if q["request"] == candidate.key],
                        predicted_API_intervals=[e for e in prediction["operations"] if e["key"] == candidate.key],
                        P_start=prediction["stage_start"].get((candidate.key, option.p)),
                        D_start=prediction["stage_start"].get((candidate.key, option.d)),
                        current_decision_CPU_not_prospectively_predicted=True)
            selected = best[1]
            token = tuple(getattr(selected, k) for k in ("p", "d", "source", "p_blocks", "d_blocks", "p_headroom", "d_headroom"))
            self.quoted_routes[token] = best_quote
            return selected

    def reserve(self, key, route, now):
        super().reserve(key, route, now)
        token = tuple(getattr(route, k) for k in ("p", "d", "source", "p_blocks", "d_blocks", "p_headroom", "d_headroom"))
        quote = self.quoted_routes[token]
        self.selected_quotes[key] = {**quote, "effective_API": [{**q, "request": key} for q in quote["effective_API"]]}
        if self.last_route_estimate is not None:
            self.work[key] = replace(self.work[key], p_not_before=now + self.last_route_estimate.seconds)
        self.admission_choices[key, route.p] = route.p_headroom
        self.admission_choices[key, route.d] = route.d_headroom

    def boundary_released(self, key, now):
        with self.guard:
            self.work[key] = replace(self.work[key], d_not_before=now)
            self.progress_version += 1

    def revise(self, key, route, layout, costs):
        with self.guard:
            old = self.work[key]
            new = optional_work(key, old.order, old.offered, self.demands[key], route, layout, costs)
            self.work[key] = refresh_work(old, new)
            self.routes[key] = route

    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
        with self.guard, self.memo_scope():
            self.refresh(layout, costs)
            demand, current = self.demands[key], self.work[key]
            self.last_boundary_estimate = (self.control_costs.estimate(
                "boundary", self.demands.values(), layout, max_candidates, now)
                if self.predict_control else None)
            limit = max(layout.prefix(route.p, demand.hashes), layout.prefix(route.d, demand.hashes))
            lengths = tuple(dict.fromkeys((layout.prefix(route.d, demand.hashes), limit, 0,
                                          *range(limit - 1, 0, -1))))
            best = None
            for length, enabled in itertools.islice(itertools.product(lengths, (False, True)), max_candidates):
                option = replace(route, d_blocks=length, d_headroom=enabled)
                if length > layout.prefix(route.d, demand.hashes):
                    projected = layout
                    plan = native_headroom(projected, route.d, demand, costs)
                    if enabled and plan:
                        projected = projected.remove(route.d, plan.victims)
                    try:
                        copy_package(projected, route.p, route.d, demand.hashes[:length])
                    except ValueError:
                        continue
                proposed = refresh_work(current, optional_work(key, current.order, current.offered,
                                         demand, option, layout, costs))
                if self.last_boundary_estimate is not None:
                    proposed = replace(proposed, d_not_before=now + self.last_boundary_estimate.seconds)
                work = tuple(proposed if w.key == key else w for w in self.work.values())
                prediction = self.predict(work, now, layout, self.demands,
                                          {**self.routes, key: option}, costs)
                value = profile(work, prediction), enabled, length
                if best is None or value < best[0]:
                    best = value, option
            if best is None:
                raise ValueError("No safe cache-aware boundary including recompute")
            option = best[1]
            self.admission_choices[key, route.d] = option.d_headroom
            self.revise(key, option, layout, costs)
            if self.last_boundary_estimate is not None:
                self.work[key] = replace(self.work[key], d_not_before=now + self.last_boundary_estimate.seconds)
            return option.d_blocks, costs.compute(route.d, demand.tokens, option.d_blocks * demand.block_tokens)


def plan_cache_profile(layout, history, queues, costs, *, max_trials=256,
                       clock=time.monotonic, planning_overhead=0.0, **unused):
    started = clock()
    with queues.guard:
        work = tuple(queues.work.values())
        demands, routes = dict(queues.demands), dict(queues.routes)
        version, now = queues.progress_version, started - queues.started
    if not work:
        return Plan(Transition(layout), None, None, 0, 0, clock() - started,
                    tuple(d.key for d in history), "no_admitted_work"), version, ()

    def score(transition, planning):
        resources, seconds = set(), 0.0
        for op in transition.operations:
            if isinstance(op, Copy):
                resources.update((f"gate{op.source}", f"gate{op.destination}",
                                  "link:" + direction(op.source, op.destination)))
                seconds += costs.copy_seconds(op.source, op.destination, op.payload_bytes)
            else:
                resources.add(f"gate{op.node}")
                seconds += costs.eviction_fixed
        if any(set(o.resources) & resources for w in work for o in (*w.prefill, *w.decode)
               if o.name in dict(w.active_operations)):
            return None
        prediction = queues.predict(work, now + planning, transition.layout, demands, routes, costs,
                                    {r: now + planning + seconds for r in resources}, memo=memo)
        return profile(work, prediction)

    memo = queues.new_memo()
    initial = score(Transition(layout), 0)
    best, value, trials, evaluated = Transition(layout), initial, 1, 1
    for candidate in itertools.islice(neighbors(layout, relevant_history(history, demands.values())), max_trials - 1):
        trials += 1
        if candidate is None or not candidate.layout.feasible():
            continue
        result = score(candidate, clock() - started + planning_overhead)
        evaluated += 1
        if result is not None and result < value:
            best, value = candidate, result
    if best.operations:
        result = score(best, clock() - started + planning_overhead)
        if result is None or result >= initial:
            best, value = Transition(layout), initial
        else:
            value = result
    queues.record_memo(memo)
    return Plan(best, initial, value, trials, evaluated, clock() - started,
                tuple(d.key for d in history), "bounded_cache_resource_neighborhood"), version, tuple(w.key for w in work)
