"""Batch-aware forecast adapter over the retained physical route enumeration.

Only published cache is quoted. Forecasts use completed singleton fits plus an
explicit batch scaling scenario, never future world service labels. An acquired
iteration is conservatively restarted at the origin (at most one extra step).
"""

import heapq
import itertools
import math
from collections import deque
from dataclasses import replace

from binary_timeline import seconds, ticks
from pegaflow.detached_calendar import token
from pegaflow.joint_policy import copy_package
from pegaflow.pending_planner import pending_profile
from pegaflow.phase_release_calendar import PhaseReleaseQueues, PhaseSnapshot
from pegaflow.resource_cache_profile import CacheRoute, optional_work
from pegaflow.resource_state_profile import refresh_work
from pegaflow.strategies.continuous_batch import ContinuousBatcher, Iteration, Sequence
from pegaflow.strategies.resource_reservation import copy_reservations, permitted


def sequence_for(key, node, demand, route, costs, *, accepted=None):
    # Once submitted, execution owns the accepted prefix coordinate system.
    # A stale route may still request blocks whose copy fell back to recompute.
    prefix = (
        accepted.prefix_tokens
        if accepted is not None
        else route.d_blocks * demand.block_tokens
    )
    restore = costs.restore(node, prefix)
    first = max(0.0, costs.compute(node, demand.tokens, prefix) - restore)
    tail = costs.decode_tail(node, demand.tokens)
    save = min(tail, costs.fits[node][4])
    return Sequence(
        key, demand.tokens, prefix, first, max(0.0, tail - save), restore, save
    )


def schedule_batched(
    work,
    now,
    remaining,
    demands,
    routes,
    costs,
    config,
    progress,
    *,
    coalesce=True,
    batch_factory=ContinuousBatcher,
    reserve_copies=False,
    atomic_copies=False,
    iteration_overhead=None,
    first_profile_guard=None,
    cache_state=None,
    admission_releases=None,
    inflight_iterations=None,
    batch_context=None,
    exact_timeline=False,
):
    """P FIFO; multi-sequence D; exclusive copy/restore/save and bounded KV."""
    work = tuple(work)
    by_key = {w.key: w for w in work}
    queues = [deque() for _ in range(4)]
    active = {}
    stages = {}
    held, events = {}, []
    batches = {n: batch_factory(config) for n in (2, 3)}
    serial = itertools.count()
    first = {w.key: w.actual_first for w in work if w.actual_first is not None}
    p_end = {w.key: w.p_done for w in work if w.p_done is not None}
    full, starts, trace = {}, {}, []
    encode = ticks if exact_timeline else lambda value: value
    decode = seconds if exact_timeline else lambda value: value
    clock = encode(now)
    observed = dict(inflight_iterations or {})
    for node, event in observed.items():
        if (
            node not in batches
            or event["origin"] != now
            or event["started"] > now
            or not math.isfinite(event["remaining"])
            or event["remaining"] < 0
            or event["kind"] not in ("restore", "save")
            or len(event["pieces"]) != 1
        ):
            raise ValueError("Observed exclusive iteration contract required")
        key, count, phase = event["pieces"][0]
        if key not in progress or count != 0 or phase != event["kind"]:
            raise ValueError("Observed phase must belong to accepted D progress")
        # Preserve already acquired ownership before considering new copies.
        held[f"gate{node}"] = f"observed{node}"

    def add(key, node):
        w = by_key[key]
        ops = w.prefill if node < 2 else w.decode
        ops = deque(op for op in ops if op.name not in w.completed_operations)
        if ops and ops[0].name in dict(w.active_operations) and ops[0].name != "D_api":
            op = ops[0]
            elapsed = max(0.0, now - dict(w.active_operations)[op.name])
            residual = remaining(w, op, elapsed)
            seconds = getattr(residual, "full_seconds", residual)
            first_remaining = getattr(residual, "first_seconds", None)
            ops[0] = replace(op, seconds=seconds, first_seconds=first_remaining)
        stages[key] = {"node": node, "ops": ops, "busy": False}
        queues[node].append(key)

    for w in sorted(
        work,
        key=lambda w: (
            0 if w.active_operations else 1,
            w.d_enqueued
            if w.p_done is not None and w.d_enqueued is not None
            else w.order,
            w.order,
        ),
    ):
        release = (admission_releases or {}).get(w.key, now)
        if release > now and (w.p_done is None or w.d_started is None):
            node = w.p if w.p_done is None else w.d
            heapq.heappush(
                events, (encode(release), next(serial), "release", w.key, node)
            )
        else:
            add(w.key, w.p if w.p_done is None else w.d)

    def finish_stage(key):
        state = stages[key]
        node = state["node"]
        del active[key]
        if node < 2:
            p_end[key] = decode(clock)
            add(key, by_key[key].d)
        else:
            full[key] = decode(clock)

    while len(full) < len(work):
        if first_profile_guard is not None:
            first_profile_guard(decode(clock), first)
        changed = True
        while changed:
            changed = False
            for node in range(4):
                limit = 1 if node < 2 else config.max_sequences
                while queues[node] and sum(n == node for n in active.values()) < limit:
                    key = queues[node].popleft()
                    if cache_state is not None:
                        stages[key]["ops"] = deque(
                            cache_state.admit(
                                by_key[key], node, stages[key]["ops"], progress.get(key)
                            )
                        )
                    active[key] = node
                    starts[key, node] = decode(clock)
                    changed = True
            ordered = sorted(
                active.items(),
                key=lambda item: (
                    0 if by_key[item[0]].active_operations else 1,
                    by_key[item[0]].order,
                ),
            )
            reservations = (
                copy_reservations(
                    (by_key[key].order, state["ops"][0].resources)
                    for key in active
                    for state in (stages[key],)
                    if not state["busy"]
                    and state["ops"]
                    and state["ops"][0].name in ("history", "handoff")
                )
                if reserve_copies
                else {}
            )
            for key, node in ordered:
                state = stages[key]
                if state["busy"]:
                    continue
                if not state["ops"]:
                    finish_stage(key)
                    changed = True
                    continue
                op = state["ops"][0]
                acquired = op.name in dict(by_key[key].active_operations) or bool(
                    state.get("copy_gates")
                )
                if not permitted(
                    op.resources, by_key[key].order, reservations, acquired=acquired
                ):
                    continue
                if op.name == "D_api":
                    previous = progress.get(key)
                    sequence = sequence_for(
                        key,
                        node,
                        demands[key],
                        cache_state.effective_route(key, node)
                        if cache_state is not None
                        else routes[key],
                        costs,
                        accepted=previous,
                    )
                    if previous is not None:
                        if previous.prompt_tokens != sequence.prompt_tokens:
                            raise ValueError(
                                "Observed progress belongs to a different prompt"
                            )
                        for field in (
                            "prefilled",
                            "decoded",
                            "restored",
                            "phase",
                            "first_at",
                        ):
                            setattr(sequence, field, getattr(previous, field))
                    batches[node].submit(sequence)
                    if cache_state is not None:
                        cache_state.start(by_key[key], node, op)
                    state["busy"] = True
                    changed = True
                    continue
                resources = op.resources
                gates = tuple(r for r in resources if r.startswith("gate"))
                is_copy = op.name in ("history", "handoff")
                if is_copy and not state.get("copy_gates"):
                    acquisition = resources if atomic_copies else gates
                    if any(r in held for r in acquisition):
                        continue
                    for resource in gates:
                        held[resource] = key
                    state["copy_gates"] = gates
                acquire = tuple(r for r in resources if not is_copy or r not in gates)
                if any(resource in held for resource in acquire):
                    continue
                state["busy"] = True
                if cache_state is not None:
                    cache_state.start(by_key[key], node, op)
                for resource in acquire:
                    held[resource] = key
                heapq.heappush(
                    events, (clock + encode(op.seconds), next(serial), "op", key, op)
                )
                trace.append(
                    {
                        "key": key,
                        "operation": op.name,
                        "start": decode(clock),
                        "end": decode(clock + encode(op.seconds)),
                        "resources": resources,
                    }
                )
                changed = True
            if any(
                queues[n]
                and sum(v == n for v in active.values())
                < (1 if n < 2 else config.max_sequences)
                for n in range(4)
            ):
                changed = True
                continue
            plans = {}
            for node, batch in batches.items():
                resource = f"gate{node}"
                if node in observed:
                    event = observed[node]
                    batch.admit()
                    if not all(k in batch.active for k, _, _ in event["pieces"]):
                        continue
                    if held.get(resource) != f"observed{node}":
                        raise ValueError("Observed device ownership was bypassed")
                    if any(
                        batch.active[k].phase != phase
                        for k, _, phase in event["pieces"]
                    ):
                        raise ValueError("Observed phase changed before completion")
                    iteration = Iteration(
                        event["kind"], event["remaining"], tuple(event["pieces"])
                    )
                    batch.busy = iteration
                    plans[node] = iteration, 1
                    del observed[node]
                    continue
                if resource in held or batch.busy is not None:
                    continue
                if batch_context is not None:
                    batch_context(node, batch, tuple(queues[node]))
                iteration = (
                    batch.begin(
                        eligible=lambda key, r=resource, res=reservations: permitted(
                            (r,), by_key[key].order, res
                        )
                    )
                    if reserve_copies
                    else batch.begin()
                )
                if iteration is None:
                    continue
                if iteration_overhead:
                    iteration = replace(
                        iteration,
                        seconds=iteration.seconds + iteration_overhead.get(node, 0.0),
                    )
                gate_waiters = any(
                    not stages[k]["busy"]
                    and stages[k]["ops"]
                    and resource in stages[k]["ops"][0].resources
                    for k in active
                )
                repeats = 1
                if coalesce and iteration.kind == "compute" and not gate_waiters:
                    repeats = min(
                        (
                            batch.active[k].output_tokens - batch.active[k].decoded
                            if phase == "decode"
                            else batch.active[k].remaining_prefill
                        )
                        // count
                        for k, count, phase in iteration.pieces
                    )
                if hasattr(batch, "stable_repeats"):
                    repeats = batch.stable_repeats(iteration, repeats)
                plans[node] = iteration, repeats
            # Resolve both devices' next phase boundary before grouping either
            # stream. A just-enqueued stage or a cross-device gate waiter must
            # never disappear inside another device's coalesced decode span.
            horizons = [at for at, *_ in events]
            horizons.extend(
                clock + encode(op.seconds) * count for op, count in plans.values()
            )
            horizon = min(horizons, default=clock)
            for node, (iteration, repeats) in plans.items():
                resource = f"gate{node}"
                batch = batches[node]
                if iteration.seconds:
                    repeats = min(
                        repeats,
                        max(1, (horizon - clock) // encode(iteration.seconds))
                        if exact_timeline
                        else max(1, math.floor((horizon - clock) / iteration.seconds)),
                    )
                # Multiply the exact primitive, not an already rounded group.
                endpoint = clock + encode(iteration.seconds) * repeats
                iteration = replace(
                    iteration, seconds=iteration.seconds * repeats, repeats=repeats
                )
                batch.busy = iteration
                held[resource] = f"batch{node}"
                heapq.heappush(
                    events,
                    (endpoint, next(serial), "batch", node, iteration),
                )
                changed = True
        if not events:
            if len(full) == len(work):
                break
            raise ValueError("Batch forecast deadlocked or exceeded memory")
        clock, _, kind, key, value = heapq.heappop(events)
        if kind == "release":
            add(key, value)
        elif kind == "op":
            for resource in value.resources:
                del held[resource]
            stages[key].pop("copy_gates", None)
            stages[key]["ops"].popleft()
            if cache_state is not None:
                cache_state.state.event_time = decode(clock)
                cache_state.finish(by_key[key], stages[key]["node"], value)
                stages[key]["ops"] = deque(
                    cache_state.after_copy(
                        by_key[key], stages[key]["node"], value, stages[key]["ops"]
                    )
                )
            stages[key]["busy"] = False
        else:
            del held[f"gate{key}"]
            observed, finished = batches[key].finish(value, decode(clock))
            if cache_state is not None and value.kind == "restore":
                for request, _, _ in value.pieces:
                    cache_state.finish_restore(request, key, decode(clock))
            if cache_state is not None and value.kind == "compute":
                for request, _, _ in value.pieces:
                    cache_state.finish_compute(request, key, batches[key].active[request], decode(clock))
            for request in observed:
                first[request] = decode(clock)
            for request in finished:
                op = stages[request]["ops"].popleft()
                if cache_state is not None:
                    cache_state.state.event_time = decode(clock)
                    cache_state.finish(by_key[request], key, op)
                stages[request]["busy"] = False
    return {
        "batch_factory": batch_factory.__name__,
        "timeline_arithmetic": "binary64-exact-dyadic" if exact_timeline else "float",
        "P_end": p_end,
        "first": first,
        "full": full,
        "stage_start": starts,
        "operations": trace,
        **(
            {
                "effective_API": list(cache_state.quotes.values()),
                "cache_events": cache_state.state.events,
            }
            if cache_state is not None
            else {}
        ),
    }


class BatchedSnapshot(PhaseSnapshot):
    def predict_batch(self, work, now, demands, routes, costs):
        self.phase_costs = costs
        memo = self.__dict__.setdefault("batch_memo", {})
        # A snapshot fixes progress, costs, demand and layout. Equivalent work
        # chains and D prefixes need only one token replay, including aliases
        # for an unused remote source. This does not reduce the search budget.
        identity = (
            tuple(work),
            tuple(sorted((key, routes[key].d_blocks) for key in demands)),
        )
        if identity not in memo:
            memo[identity] = schedule_batched(
                work,
                now,
                self._remaining_operation,
                demands,
                routes,
                costs,
                self.batch_config,
                self.batch_progress,
                iteration_overhead=getattr(self, "iteration_overhead", None),
            )
        return memo[identity]

    def forecast(self, route, now):
        demand, layout, costs, offered, origin = self.context
        published = max(
            layout.prefix(route.d, demand.hashes),
            layout.prefix(route.p, demand.hashes),
            route.p_blocks,
        )
        option = CacheRoute(
            **{
                **route.__dict__,
                "d_blocks": min(route.d_blocks, published),
                "p_headroom": False,
                "d_headroom": False,
            }
        )
        self.refresh(layout, costs)
        candidate = optional_work(
            "__candidate__", self.sequence, offered, demand, option, layout, costs
        )
        work = (*self.work.values(), candidate)
        prediction = self.predict_batch(
            work,
            origin,
            {**self.demands, candidate.key: demand},
            {**self.routes, candidate.key: option},
            costs,
        )
        value = pending_profile(work, prediction)
        selected = replace(
            option,
            predicted_p_end=prediction["P_end"][candidate.key],
            predicted_first=prediction["first"][candidate.key],
            predicted_end=prediction["full"][candidate.key],
            predicted_system_p99=value[0],
            predicted_tail_profile=value[1],
            predicted_full_profile=value[2],
            predicted_P_release_profile=value[3],
            operation_model_version=costs.version,
            model_latest_completed_seconds=costs.latest_completed_seconds,
            forecast_origin_seconds=origin,
            objective_scope="pending-first continuous-D-batch resource calendar",
        )
        self.quoted_routes[token(selected)] = {
            "origin": origin,
            "latest_completed": costs.latest_completed_seconds,
            "model_version": costs.version,
            "effective_API": [],
            "predicted_API_intervals": prediction["operations"],
            "current_decision_CPU_not_prospectively_predicted": True,
            "future_P_publication_assumed": False,
            "batch_scaling_measured": False,
        }
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
            prediction = self.predict_batch(
                work, now, self.demands, {**self.routes, key: option}, costs
            )
            value = pending_profile(work, prediction), payload, length
            if best is None or value < best[0]:
                best = value, option
        if best is None:
            raise ValueError("No published D boundary including recomputation")
        self.admission_choices[key, route.d] = False
        self.revise(key, best[1], layout, costs)
        return best[1].d_blocks, costs.compute(
            route.d, item.tokens, best[1].d_blocks * 128
        )


class BatchedQueues(PhaseReleaseQueues):
    def __init__(self, *args, batch_config, progress, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_config, self.progress_reader = batch_config, progress

    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = BatchedSnapshot
        shadow.batch_config = self.batch_config
        # Only completed token/chunk counters, not a future iteration finish time.
        shadow.batch_progress = self.progress_reader()
        return shadow
