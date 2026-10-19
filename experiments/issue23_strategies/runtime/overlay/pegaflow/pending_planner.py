"""Optional, event-coalesced repair of admitted requests awaiting first token.

This is not future-demand prepositioning. Completed-first decode tails remain
resource holders, but their immutable TTFT cannot veto repair of another job.
No capacity/occupancy bonus, task/rate switch or mandatory action is used.
"""

from __future__ import annotations

import itertools
import math
import time

from pegaflow.joint_policy import Copy, Plan, Transition, neighbors
from pegaflow.operation_cost import NonnegativeFit
from pegaflow.resource_placement import relevant_history
from pegaflow.resource_queue import direction


def pending_profile(work, prediction):
    pending = tuple(w for w in work if w.actual_first is None)
    first = tuple(sorted((prediction["first"][w.key] - w.offered for w in pending), reverse=True))
    if not first:
        return None
    rank = math.ceil(.99 * len(first))
    full = tuple(sorted((prediction["full"][w.key] - w.offered for w in work), reverse=True))
    p_end = tuple(sorted((prediction["P_end"][w.key] - w.offered for w in work), reverse=True))
    return first[len(first) - rank], first, full, p_end


class OpportunityTrigger:
    def __init__(self):
        self.last_signature = None
        self.records = []
        self.cpu_fit = NonnegativeFit(2)
        self.latest_completed = 0.0

    def features(self, layout, work):
        chunks = sum((w.tokens + w.block_tokens - 1) // w.block_tokens for w in work)
        return 1.0, float(256 * chunks * (1 + sum(map(len, layout.units))))

    def consider(self, layout, queues, now):
        pending = tuple(w for w in queues.work.values() if w.actual_first is None)
        if not pending:
            return self._record(now, False, "no_unserved_first", 0)
        active = {r for w in queues.work.values() for op in (*w.prefill, *w.decode)
                  if op.name in dict(w.active_operations) for r in op.resources}
        witness = []
        for w in pending:
            demand, route = queues.demands[w.key], queues.routes[w.key]
            for node, source, api in ((w.p, route.source, "P_api"), (w.d, w.p, "D_api")):
                if api in w.completed_operations or api in dict(w.active_operations):
                    continue
                local, remote = layout.prefix(node, demand.hashes), layout.prefix(source, demand.hashes)
                # Only an idle owner can be changed now. A source may remain
                # busy when the witness is capacity repair at the destination.
                if f"gate{node}" in active:
                    continue
                missing = (source != node and remote > local and f"gate{source}" not in active)
                required = max(0, demand.tokens // demand.block_tokens - local)
                free = layout.capacities[node] - layout.usage_values[node]
                pressure = free < required * demand.block_tokens * queues._trigger_bytes_per_token
                if missing or pressure:
                    witness.append((w.key, node, source, local, remote, free, pressure))
        if not witness:
            return self._record(now, False, "no_idle_repair_witness", len(pending))
        # Relevant owner closures, capacity and request suffixes define the
        # event, not a fixed number of completions or a fitted frequency.
        needed = frozenset(key for w in pending for key in queues.demands[w.key].hashes)
        owners = tuple(tuple((u.identity, u.charged, u.protected, u.keys & needed)
                             for u in units if u.keys & needed) for units in layout.units)
        signature = (tuple(witness), owners, tuple(sorted(active)))
        if signature == self.last_signature:
            return self._record(now, False, "same_repair_state", len(pending))
        self.last_signature = signature
        return self._record(now, True, "observable_idle_cache_or_capacity_change", len(pending))

    def _record(self, now, run, reason, pending):
        result = dict(origin=now, run=run, reason=reason, pending_first=pending)
        self.records.append(result)
        return result

    def observe(self, features, seconds, completed):
        if completed < self.latest_completed or seconds < 0:
            raise ValueError("Completed causal planning CPU sample required")
        self.cpu_fit.add(features, seconds)
        self.latest_completed = completed


def plan_opportunity(layout, history, queues, costs, *, trigger=None, max_trials=256,
                     clock=time.monotonic, planning_overhead=0.0, **unused):
    if type(max_trials) is not int or max_trials < 1:
        raise ValueError("Explicit positive candidate budget required")
    began = clock()
    work = tuple(queues.work.values())
    now = began - queues.started
    trigger = trigger or OpportunityTrigger()
    if trigger.latest_completed > now:
        raise ValueError("Planning forecast cannot use a future CPU completion")
    queues._trigger_bytes_per_token = costs.bytes_per_token
    event = trigger.consider(layout, queues, now)
    noop = Transition(layout)
    def finish(best, initial, value, trials, evaluated, reason):
        return (Plan(best, initial, value, trials, evaluated, clock() - began,
                     tuple(d.key for d in history), reason), queues.progress_version,
                tuple(w.key for w in work))
    if not event["run"]:
        return finish(noop, None, None, 0, 0, event["reason"])
    queues.refresh(layout, costs)
    work = tuple(queues.work.values())
    demands, routes = dict(queues.demands), dict(queues.routes)
    memo = queues.new_memo()
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
        predicted = queues.predict(work, now + planning, transition.layout, demands, routes,
            costs, {r: now + planning + seconds for r in resources}, memo=memo)
        return pending_profile(work, predicted)
    initial = score(noop, 0)
    features = trigger.features(layout, demands.values())
    predicted_cpu = trigger.cpu_fit.predict(features) if trigger.cpu_fit.observations else 0.0
    floor = sorted(now - w.offered for w in work if w.actual_first is None)
    lower = floor[math.ceil(.99 * len(floor)) - 1]
    # Even zero remaining service cannot produce a first before now. This
    # optimistic bound only declines futile work; it never certifies a gain.
    if initial[0] - lower <= predicted_cpu + planning_overhead:
        return finish(noop, initial, initial, 1, 1, "optimistic_gain_below_completed_CPU_cost")
    best, value, trials, evaluated = noop, initial, 1, 1
    known = {d.key: d for d in history}
    known.update((w.key, demands[w.key]) for w in work if w.actual_first is None)
    # Current prompt hashes are already known. Copy still requires an actual
    # complete published source; no future generated hash is introduced.
    relevant = relevant_history(tuple(known.values()),
                                (demands[w.key] for w in work if w.actual_first is None))
    for candidate in itertools.islice(neighbors(layout, relevant), max_trials - 1):
        trials += 1
        if candidate is None or not candidate.layout.feasible():
            continue
        result = score(candidate, clock() - began + planning_overhead)
        evaluated += 1
        if result is not None and result[0] < initial[0] and result < value:
            best, value = candidate, result
    if best.operations:
        value = score(best, clock() - began + planning_overhead)
        if value is None or value[0] >= initial[0]:
            best, value = noop, initial
    queues.record_memo(memo)
    return finish(best, initial, value, trials, evaluated,
                  "pending_first_strict_gain" if best.operations else "no_priced_pending_tail_gain")
