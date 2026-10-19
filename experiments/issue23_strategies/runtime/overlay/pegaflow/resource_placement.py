"""Budgeted live-work placement block, not repeated historical arrival replay."""

from __future__ import annotations

import itertools
import time
from dataclasses import replace

from pegaflow.joint_policy import Copy, Plan, Transition, neighbors
from pegaflow.operation_schedule import schedule
from pegaflow.resource_queue import build_work, direction, objective


def relevant_history(history, live):
    # A historical prefix absent from every admitted demand cannot improve
    # this finite live-work objective. Victim owner closures are not pruned.
    needed = set(key for demand in live for key in demand.hashes)
    result = []
    for demand in history:
        count = next((i for i, key in enumerate(demand.hashes) if key not in needed), len(demand.hashes))
        if count:
            result.append(replace(demand, hashes=demand.hashes[:count]))
    return tuple(result)


def score(transition, work, demands, routes, costs, now, remaining):
    nodes, seconds, resources = set(), 0.0, set()
    for op in transition.operations:
        if isinstance(op, Copy):
            nodes.update((op.source, op.destination))
            resources.add("link:" + direction(op.source, op.destination))
            seconds += costs.copy_seconds(op.source, op.destination, op.payload_bytes)
        else:
            nodes.add(op.node)
            seconds += costs.eviction_fixed
    resources.update(f"gate{node}" for node in nodes)
    if any(set(operation.resources) & resources for w in work for operation in (*w.prefill, *w.decode)
           if operation.name in dict(w.active_operations)):
        return None
    revised = []
    for w in work:
        new = build_work(w.key, w.order, w.offered, demands[w.key], routes[w.key], transition.layout, costs)
        # An acquired/finished operation is immutable. Candidate layouts affect
        # remaining copies only; no future native admission or reroute assumed.
        old = {op.name: op for op in (*w.prefill, *w.decode)}
        preserve = w.completed_operations | set(dict(w.active_operations))
        def chain(operations, previous):
            values = {op.name: op for op in operations}
            values.update({name: op for name, op in old.items() if name in preserve and op in previous})
            order = ("history", "P_api") if previous is w.prefill else ("handoff", "D_api")
            return tuple(values[name] for name in order if name in values)
        revised.append(replace(new, prefill=chain(new.prefill, w.prefill), decode=chain(new.decode, w.decode),
            p_done=w.p_done, d_enqueued=w.d_enqueued, p_started=w.p_started, d_started=w.d_started,
            actual_first=w.actual_first, completed_operations=w.completed_operations, active_operations=w.active_operations))
    prediction = schedule(revised, now, remaining, {resource: now+seconds for resource in resources})
    return objective(revised, prediction)


def plan_live(layout, history, queues, costs, *, max_trials=256, clock=time.monotonic):
    started = clock()
    with queues.guard:
        work = tuple(queues.work.values())
        demands, routes = dict(queues.demands), dict(queues.routes)
        version = queues.progress_version
        samples = {key: tuple(rows) for key, rows in queues.operation_samples.items()}
        now = started - queues.started
    if not work:
        return Plan(Transition(layout), None, None, 0, 0, clock()-started,
                    tuple(d.key for d in history), "no_admitted_work"), version, ()
    def remaining(w, op, elapsed):
        node = w.p if op.name in ("history", "P_api") else w.d
        values = [actual/predicted*op.seconds-elapsed for predicted, actual in samples.get((node, op.name), ())
                  if predicted > 0 and actual/predicted*op.seconds > elapsed]
        return sum(values)/len(values) if values else max(0, op.seconds-elapsed)
    initial = score(Transition(layout), work, demands, routes, costs, now, remaining)
    best, value, evaluated = Transition(layout), initial, 1
    candidates = relevant_history(history, demands.values())
    trials, reason = 1, "neighborhood_exhausted"
    # Every generated item consumes the existing explicit 256-candidate budget.
    # No wall-time cutoff can expire before the initial objective is evaluated.
    for candidate in itertools.islice(neighbors(layout, candidates), max_trials-1):
        trials += 1
        if candidate is None or not candidate.layout.feasible():
            continue
        predicted = score(candidate, work, demands, routes, costs, now, remaining)
        evaluated += 1
        if predicted is not None and predicted + (clock()-started) < value:
            best, value = candidate, predicted
    if trials == max_trials:
        reason = "candidate_budget_exhausted"
    # Charge the complete measured planning work, not just elapsed at discovery.
    elapsed = clock()-started
    if best.operations and value+elapsed >= initial:
        best, value, reason = Transition(layout), initial, "planning_cost_erases_predicted_benefit"
    return Plan(best, initial, value+elapsed if best.operations else value, trials, evaluated, elapsed,
                tuple(d.key for d in history), reason), version, tuple(w.key for w in work)
