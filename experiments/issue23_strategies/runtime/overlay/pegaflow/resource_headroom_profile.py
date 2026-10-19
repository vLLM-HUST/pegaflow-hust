"""Known-operation write headroom, not arbitrary cache occupancy targets.

Minimal old unprotected closures can avoid the native coarse reclaim cliff.
This is a planning/CPU prototype, not a deployed allocator reservation lease.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from pegaflow.operation_context import ContextOperationCosts
from pegaflow.operation_schedule import Operation
from pegaflow.resource_context_profile import context_work
from pegaflow.resource_state_profile import StateProfileQueues, refresh_work


@dataclass(frozen=True)
class Headroom:
    node: int
    victims: tuple[str, ...]
    required_new_bytes: int
    released_bytes: int


def native_headroom(layout, node, demand, costs, output_tokens=128):
    computed = demand.tokens + (output_tokens-1 if node >= 2 else 0)
    extra_unknown = computed//demand.block_tokens-len(demand.hashes)
    current = set(demand.hashes)
    needed = (len(current-layout.keys(node))+extra_unknown)*demand.block_tokens*costs.bytes_per_token
    shortage = max(0, layout.usage(node)+needed-layout.capacities[node])
    victims, released = [], 0
    for unit in layout.units[node]:
        if released >= shortage:
            break
        if not unit.protected and not unit.virtual and not unit.keys.intersection(current):
            victims.append(unit.identity)
            released += unit.charged
    if released < shortage:
        return None
    return Headroom(node, tuple(victims), needed, released)


@dataclass(frozen=True)
class HeadroomCosts:
    current: object
    admission_seconds: float
    action_samples: int

    def __getattr__(self, name):
        return getattr(self.current, name)

    @property
    def version(self):
        return self.current.version+self.action_samples


class HeadroomOperationCosts(ContextOperationCosts):
    def __init__(self, original, block_tokens=128, output_tokens=128):
        super().__init__(original, block_tokens, output_tokens)
        self.admission_costs = []

    def observe_admission(self, seconds, completed):
        if seconds < 0 or completed < 0:
            raise ValueError("Completed nonnegative owned headroom control operation required")
        with self.guard:
            self.admission_costs.append((completed, seconds))
            self.latest = max(self.latest, completed)

    def freeze(self, now):
        with self.guard:
            current = super().freeze(now)
            mean = (sum(s for _, s in self.admission_costs)/len(self.admission_costs)
                    if self.admission_costs else self.original.eviction_fixed)
            return HeadroomCosts(current, mean, len(self.admission_costs))

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(), "completed_admission_costs": self.admission_costs}


def headroom_work(key, order, offered, demand, route, layout, costs):
    work = context_work(key, order, offered, demand, route, layout, costs)
    def chain(node, operations):
        plan = native_headroom(layout, node, demand, costs)
        if plan and plan.victims:
            role = "P" if node < 2 else "D"
            return (Operation(role+"_admission", costs.admission_seconds, (f"gate{node}",)), *operations)
        return operations
    return replace(work, prefill=chain(route.p, work.prefill), decode=chain(route.d, work.decode))


class HeadroomProfileQueues(StateProfileQueues):
    work_builder = staticmethod(headroom_work)

    def revise(self, key, route, layout, costs):
        with self.guard:
            old = self.work[key]
            new = headroom_work(key, old.order, old.offered, self.demands[key], route, layout, costs)
            self.work[key] = refresh_work(old, new)
            self.routes[key] = route

