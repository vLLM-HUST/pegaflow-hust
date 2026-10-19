"""Four-instance queue-aware routing and bounded complete-package placement.

All candidates start at the same real snapshot. Copy admission and exchanges
carry transition cost and transient allocation constraints. No future workload
or global-optimality assumption is made.
"""

from __future__ import annotations

import itertools
import heapq
import math
import time
from collections import deque
from dataclasses import dataclass, field, replace
from functools import cached_property
from typing import Callable

from pegaflow.placement_cost import ObservedLatencyModel
from pegaflow.placement_executor import NodeInventory, RecoveryPrefix


class PlanningExpired(RuntimeError):
    """A cooperative CPU planning budget expired; no partial score may commit."""


def check_budget(deadline, clock):
    if deadline is not None and clock() >= deadline:
        raise PlanningExpired("Explicit planning work budget exhausted")


@dataclass(frozen=True)
class Demand:
    key: str
    task: str
    tokens: int
    hashes: tuple[bytes, ...]
    arrival: float = 0.0
    block_tokens: int = 128

    def __post_init__(self):
        if not self.key or not self.task or type(self.tokens) is not int or self.tokens < 1:
            raise ValueError("Invalid demand identity/length")
        if self.arrival < 0 or not math.isfinite(self.arrival):
            raise ValueError("Invalid causal arrival")
        if len(self.hashes) * self.block_tokens >= self.tokens:
            raise ValueError("Recovery must leave the logits token")


@dataclass(frozen=True)
class Unit:
    identity: str
    keys: frozenset[bytes]
    charged: int
    payload: tuple[tuple[bytes, int], ...]
    protected: bool = False
    virtual: bool = False


@dataclass(frozen=True)
class View:
    node: int
    snapshot: NodeInventory
    units: tuple[Unit, ...]

    @classmethod
    def parse(cls, node, payload):
        if type(node) is not int or node not in range(4):
            raise ValueError("Exactly four allocated TP1 instances are supported")
        snapshot = NodeInventory.parse(1 if node < 2 else 2, payload)
        if len(snapshot.group_slots) != 1:
            raise ValueError("This live policy is attention-only, not an unqualified hybrid path")
        units = tuple(Unit(unit.key, frozenset(key[1] for key in unit.logical_keys),
                           unit.charged_bytes,
                           tuple((key[1], snapshot.blocks[key]["payload_bytes"])
                                 for key in sorted(unit.logical_keys)), unit.protected)
                      for unit in snapshot.units)
        return cls(node, snapshot, units)


@dataclass(frozen=True)
class Layout:
    units: tuple[tuple[Unit, ...], ...]
    capacities: tuple[int, ...]
    hidden: tuple[int, ...]

    @classmethod
    def current(cls, views):
        if tuple(view.node for view in views) != (0, 1, 2, 3):
            raise ValueError("One ordered P0/P1/D0/D1 snapshot required")
        if len({view.snapshot.namespace for view in views}) != 1:
            raise ValueError("Model namespace mismatch")
        if len({view.snapshot.group_slots for view in views}) != 1:
            raise ValueError("Registered recovery package slot counts differ")
        return cls(tuple(view.units for view in views),
                   tuple(view.snapshot.capacity_bytes for view in views),
                   tuple(view.snapshot.nonvisible_bytes for view in views))

    @cached_property
    def key_sets(self):
        return tuple(frozenset(key for unit in values for key in unit.keys) for values in self.units)

    @cached_property
    def payload_maps(self):
        result = []
        for values in self.units:
            payloads = {}
            for unit in values:
                payloads.update(unit.payload)
            result.append(payloads)
        return tuple(result)

    @cached_property
    def usage_values(self):
        return tuple(self.hidden[node] + sum(unit.charged for unit in self.units[node]) for node in range(4))

    def keys(self, node):
        return self.key_sets[node]

    def usage(self, node):
        return self.usage_values[node]

    def feasible(self):
        return all(self.usage(node) <= self.capacities[node] for node in range(4))

    def identity(self):
        return tuple(tuple(sorted(unit.identity for unit in units)) for units in self.units)

    def remove(self, node, identities):
        identities = frozenset(identities)
        if any(unit.protected and unit.identity in identities for unit in self.units[node]):
            raise ValueError("Active allocation cannot be removed")
        nodes = list(self.units)
        nodes[node] = tuple(unit for unit in nodes[node] if unit.identity not in identities)
        return replace(self, units=tuple(nodes))

    def prefix(self, node, hashes):
        keys = self.keys(node)
        return next((index for index, key in enumerate(hashes) if key not in keys), len(hashes))

    def bytes_for(self, node, hashes):
        values = self.payload_maps[node]
        return sum(values[key] for key in set(hashes))


@dataclass(frozen=True)
class Copy:
    source: int
    destination: int
    hashes: tuple[bytes, ...]
    missing: tuple[bytes, ...]
    allocation_bound: int
    payload_bytes: int


@dataclass(frozen=True)
class Remove:
    node: int
    identities: tuple[str, ...]


@dataclass(frozen=True)
class Transition:
    layout: Layout
    operations: tuple[Copy | Remove, ...] = ()


def copy_package(layout, source, destination, hashes, victims=()):
    if source == destination or not hashes or layout.prefix(source, hashes) != len(hashes):
        raise ValueError("A complete real source package is required")
    # Shared ancestors survive an exchange as complete physical owner closures.
    needed = set(hashes)
    victim_ids = tuple(unit.identity for unit in layout.units[destination]
                       if unit.identity in victims and not (unit.keys & needed))
    after = layout.remove(destination, victim_ids)
    missing = tuple(key for key in hashes if key not in after.keys(destination))
    if not missing:
        raise ValueError("No copy admission is needed")
    owners = [unit for unit in layout.units[source] if unit.keys.intersection(missing)]
    # A conservative source-allocation quote. The live allocator charges actual
    # rounded slabs and refuses a copy exceeding this bound without eviction.
    bound = sum(unit.charged for unit in owners)
    payloads = {}
    for unit in owners:
        payloads.update(unit.payload)
    payload = sum(payloads[key] for key in missing)
    if after.usage(destination) + bound > after.capacities[destination]:
        raise ValueError("Final and pre-publication staging do not fit")
    unit = Unit(f"copy:{source}:{destination}:{hashes[-1].hex()}:{','.join(sorted(victim_ids))}",
                frozenset(missing), bound, tuple((key, payloads[key]) for key in missing), virtual=True)
    nodes = list(after.units)
    nodes[destination] = (*nodes[destination], unit)
    copied = replace(after, units=tuple(nodes))
    operations = (() if not victim_ids else (Remove(destination, victim_ids),))
    return Transition(copied, (*operations, Copy(source, destination, hashes, missing, bound, payload)))


@dataclass(frozen=True)
class Costs:
    service: tuple[ObservedLatencyModel, ...]
    decode_seconds: tuple[float, float]
    transfer_fixed: float
    seconds_per_copy_byte: float
    seconds_per_pd_byte: float
    seconds_per_dp_byte: float
    eviction_fixed: float
    bytes_per_token: int

    def __post_init__(self):
        if len(self.service) != 4 or self.bytes_per_token < 1:
            raise ValueError("Measured four-instance costs required")
        if any(value < 0 or not math.isfinite(value) for value in (
            *self.decode_seconds, self.transfer_fixed, self.seconds_per_copy_byte,
            self.seconds_per_pd_byte, self.seconds_per_dp_byte, self.eviction_fixed)):
            raise ValueError("Invalid measured resource duration")

    def compute(self, node, tokens, prefix):
        model = self.service[node]
        # The second fit feature is resident-restore bytes, not RDMA already
        # executed by the coordinator. Do not count the wire twice.
        return model.predict(tokens - prefix, prefix * self.bytes_per_token)


@dataclass
class Calendar:
    available: dict[str, float] = field(default_factory=dict)

    def clone(self):
        return Calendar(dict(self.available))

    def stage(self, resources, ready, seconds):
        start = max(ready, *(self.available.get(resource, 0.0) for resource in resources))
        end = start + seconds
        for resource in resources:
            self.available[resource] = end
        return end


def transfer(calendar, costs, source, destination, payload, ready):
    if payload == 0:
        return ready
    if (source < 2) == (destination < 2):
        direction = "same_host_P" if source < 2 else "same_host_D"
        slope = costs.seconds_per_copy_byte
    else:
        direction = "pd" if source < 2 else "dp"
        slope = costs.seconds_per_pd_byte if direction == "pd" else costs.seconds_per_dp_byte
    # Fit the complete measured allocation/query/RDMA/publication RPC. Its
    # components are not independently identifiable from wall time; do not
    # invent a free wire stage or count the same measured copy three times.
    return calendar.stage((f"copy{source}", direction, f"copy{destination}"), ready,
                          costs.transfer_fixed + payload * slope)


@dataclass(frozen=True)
class Route:
    p: int
    d: int
    source: int
    p_blocks: int
    d_blocks: int
    predicted_first: float
    predicted_p_end: float
    predicted_end: float
    extra_bytes: int
    calendar: Calendar = field(compare=False)
    p_service_seconds: float = 0.0
    d_first_service_seconds: float = 0.0
    d_service_seconds: float = 0.0


def evaluate_route(demand, layout, costs, original, p, d, source, p_blocks, d_blocks, ready=None, prefix_counts=None, published_handoff=False):
    if p not in (0, 1) or d not in (2, 3):
        raise ValueError("One P and one D required")
    counts = prefix_counts or tuple(layout.prefix(node, demand.hashes) for node in range(4))
    if not 0 <= p_blocks <= counts[source]:
        raise ValueError("P recovery source is incomplete")
    if not 0 <= d_blocks <= len(demand.hashes):
        raise ValueError("Invalid D boundary")
    if published_handoff and d_blocks > max(counts[d], counts[p], p_blocks):
        raise ValueError("Future native admission is not a published recovery source")
    calendar = original.clone()
    now = demand.arrival if ready is None else max(ready, demand.arrival)
    extra = 0
    if source != p and p_blocks:
        missing = [key for key in demand.hashes[:p_blocks] if key not in layout.keys(p)]
        extra = layout.bytes_for(source, missing)
        if layout.usage(p) + extra > layout.capacities[p]:
            raise ValueError("Optional history fetch cannot stage without unplanned eviction")
        now = transfer(calendar, costs, source, p, extra, now)
    p_prefix = p_blocks * demand.block_tokens
    p_end = calendar.stage((f"npu{p}", f"copy{p}"), now,
                           costs.compute(p, demand.tokens, p_prefix))
    local_d = counts[d]
    newly_handed = max(0, d_blocks - local_d) * demand.block_tokens * costs.bytes_per_token
    if layout.usage(d) + newly_handed > layout.capacities[d]:
        raise ValueError("Required handoff requires capacity backpressure")
    d_ready = transfer(calendar, costs, p, d, newly_handed, p_end)
    first = calendar.stage((f"npu{d}", f"copy{d}"), d_ready,
                           costs.compute(d, demand.tokens, d_blocks * demand.block_tokens))
    end = calendar.stage((f"npu{d}",), first, costs.decode_seconds[d - 2])
    return Route(p, d, source, p_blocks, d_blocks, first, p_end, end, extra + newly_handed,
                 calendar, p_end - (demand.arrival if ready is None else max(ready, demand.arrival)),
                 first - p_end, end - p_end)


def best_route(demand, layout, costs, calendar, *, max_candidates=None, fixed_pair=None, queues=None,
               deadline=None, clock=time.monotonic, route_order=None):
    best, tried = None, 0
    pairs = (fixed_pair,) if fixed_pair else tuple(itertools.product((0, 1), (2, 3)))
    counts = tuple(layout.prefix(node, demand.hashes) for node in range(4))

    def candidates(p, d):
        # Safe no-external-restore fallback is considered before optional work.
        yield p, d, p, 0, 0
        d_lengths = tuple(dict.fromkeys((counts[d], len(demand.hashes), 0,
                                        *range(len(demand.hashes) - 1, 0, -1))))

        def from_source(source, lengths):
            for length in lengths:
                actual_lengths = d_lengths
                if queues is not None:
                    limit = max(counts[d], counts[p], length)
                    actual_lengths = tuple(dict.fromkeys((counts[d], limit, 0,
                                                         *range(limit - 1, 0, -1))))
                for d_length in actual_lengths:
                    yield p, d, source, length, d_length

        # Fairly interleave all actual sources before walking shorter boundary
        # combinations. An explicit work budget may stop enumeration, but no
        # legal boundary is permanently excluded by an ad-hoc threshold.
        sources = deque(iter(from_source(source, range(counts[source], 0, -1)))
                        for source in range(4))
        sources.append(iter(from_source(p, (0,))))
        while sources:
            stream = sources.popleft()
            try:
                yield next(stream)
                sources.append(stream)
            except StopIteration:
                pass

    streams = deque(iter(candidates(p, d)) for p, d in pairs)
    complete = True
    while streams:
        check_budget(deadline, clock)
        if max_candidates is not None and tried >= max_candidates:
            complete = False
            break
        stream = streams.popleft()
        try:
            p, d, source, length, d_length = next(stream)
            streams.append(stream)
        except StopIteration:
            continue
        tried += 1
        try:
            option = evaluate_route(demand, layout, costs, Calendar() if queues else calendar,
                                    p, d, source, length, d_length, prefix_counts=counts,
                                    published_handoff=queues is not None)
            if queues is not None:
                option = queues.forecast(option, demand.arrival)
        except ValueError:
            continue
        order = (route_order(option) if route_order else
                 (option.predicted_first, option.extra_bytes, p, d, source, length, d_length))
        if best is None or order < best[0]:
            best = (order, option)
    if best is None:
        raise ValueError("No feasible route, including the compute fallback")
    return best[1], tried, complete


def decode_boundary(demand, layout, costs, p, d):
    """Revalidate after P: never price native future admission as a cache hit.

    The D FIFO position is fixed now, so compare complete local/handoff prefixes
    by their D first-token service cost. No queue length or benefit threshold is
    introduced. Native generation/lease checks remain the final authority.
    """
    counts = tuple(layout.prefix(node, demand.hashes) for node in range(4))
    best = None
    for length in range(max(counts[p], counts[d]) + 1):
        payload = 0
        if length > counts[d]:
            try:
                transition = copy_package(layout, p, d, demand.hashes[:length])
            except ValueError:
                continue
            payload = transition.operations[-1].payload_bytes
        seconds = costs.compute(d, demand.tokens, length * demand.block_tokens)
        if payload:
            seconds += costs.transfer_fixed + payload * costs.seconds_per_pd_byte
        order = (seconds, payload, length)
        if best is None or order < best[0]:
            best = (order, length)
    return best[1], best[0][0]


def natural_save(layout, node, demand, costs):
    nodes = list(layout.units)
    units = list(nodes[node])
    present = layout.keys(node)
    block_bytes = demand.block_tokens * costs.bytes_per_token
    for key in demand.hashes:
        if key in present:
            continue
        while layout.hidden[node] + sum(unit.charged for unit in units) + block_bytes > layout.capacities[node]:
            victim = next((index for index, unit in enumerate(units) if not unit.protected), None)
            if victim is None:
                break
            units.pop(victim)
        if layout.hidden[node] + sum(unit.charged for unit in units) + block_bytes > layout.capacities[node]:
            continue
        units.append(Unit(f"save:{node}:{key.hex()}", frozenset((key,)), block_bytes, ((key, block_bytes),)))
        present = frozenset(value for unit in units for value in unit.keys)
    nodes[node] = tuple(units)
    return replace(layout, units=tuple(nodes))


def touch_layout(layout, node, hashes):
    """LRU hit promotion without allocating or making missing objects visible."""
    units = list(layout.units[node])
    for key in hashes:
        owner = next((unit for unit in units if key in unit.keys), None)
        if owner is not None:
            units.remove(owner)
            units.append(owner)
    nodes = list(layout.units)
    nodes[node] = tuple(units)
    return replace(layout, units=tuple(nodes))


def replay(demands, transition, costs, calendar=None, *, deadline=None, clock=time.monotonic,
           event_queues=False):
    if event_queues:
        if calendar is not None and calendar.available:
            raise ValueError("Working-set event replay starts at a declared empty queue, not a stale reservation calendar")
        return replay_event_queues(demands, transition, costs, deadline=deadline, clock=clock)
    check_budget(deadline, clock)
    state = (calendar or Calendar()).clone()
    delay = 0.0
    for operation in transition.operations:
        check_budget(deadline, clock)
        if isinstance(operation, Remove):
            delay = state.stage((f"control{operation.node}",), delay, costs.eviction_fixed)
        else:
            delay = transfer(state, costs, operation.source, operation.destination,
                             operation.payload_bytes, delay)
    events, sequence = [], itertools.count()
    for demand in demands:
        check_budget(deadline, clock)
        heapq.heappush(events, (demand.arrival, next(sequence), "arrival", demand))
    pending, active_tasks, values, layout = deque(), set(), [], transition.layout
    while events:
        check_budget(deadline, clock)
        now, _, kind, value = heapq.heappop(events)
        if kind == "arrival":
            pending.append(value)
        elif kind == "save":
            node, demand = value
            layout = natural_save(layout, node, demand, costs)
        elif kind == "done":
            active_tasks.remove(value.task)
        # Only dispatch when a real P resource is free. Future saves are
        # applied by events, never made visible at request reservation time.
        while pending and min(state.available.get(f"npu{p}", 0.0) for p in (0, 1)) <= now:
            eligible = next((row for row in pending if row.task not in active_tasks), None)
            if eligible is None:
                break
            pending.remove(eligible)
            active_tasks.add(eligible.task)
            request = replace(eligible, arrival=max(now, eligible.arrival, delay))
            route, _, _ = best_route(request, layout, costs, state, max_candidates=64,
                                     deadline=deadline, clock=clock)
            state = route.calendar
            values.append(route.predicted_first - eligible.arrival)
            for timestamp, event, data in (
                (route.predicted_p_end, "save", (route.p, eligible)),
                (route.predicted_end, "save", (route.d, eligible)),
                (route.predicted_end, "done", eligible),
            ):
                heapq.heappush(events, (timestamp, next(sequence), event, data))
        if pending and not any(row[2] == "wake" for row in events):
            wake = min(state.available.get(f"npu{p}", 0.0) for p in (0, 1))
            if wake > now:
                heapq.heappush(events, (wake, next(sequence), "wake", None))
    if not values:
        raise ValueError("Only completed historical demand may define an objective")
    values.sort()
    return values[math.ceil(0.99 * len(values)) - 1]


def replay_event_queues(demands, transition, costs, *, deadline=None, clock=time.monotonic):
    """Past-demand working-set proxy with actual simulated stage events.

    LRU hits promote complete physical owners. Source gates, publication timing
    and allocator fragmentation remain approximations in this cheap objective.
    """
    from pegaflow.joint_queue import QueueLedger

    check_budget(deadline, clock)
    state, delay = Calendar(), 0.0
    for operation in transition.operations:
        check_budget(deadline, clock)
        if isinstance(operation, Remove):
            delay = state.stage((f"control{operation.node}",), delay, costs.eviction_fixed)
        else:
            delay = transfer(state, costs, operation.source, operation.destination,
                             operation.payload_bytes, delay)
    queues, events, sequence = QueueLedger(), [], itertools.count()
    pending, active_tasks, values = deque(), set(), []
    d_pending, d_active, p_available = {2: deque(), 3: deque()}, set(), [0.0, 0.0]
    layout = transition.layout

    def schedule(at, kind, demand, route=None):
        heapq.heappush(events, (at, next(sequence), kind, demand, route))

    for demand in demands:
        schedule(demand.arrival, "arrival", demand)
    while events:
        check_budget(deadline, clock)
        now, _, kind, demand, route = heapq.heappop(events)
        if kind == "arrival":
            pending.append(demand)
        elif kind == "P_start":
            queues.observe(demand.key, route.p, "started", now)
            layout = touch_layout(layout, route.source, demand.hashes[:route.p_blocks])
            layout = touch_layout(layout, route.p, demand.hashes[:route.p_blocks])
        elif kind == "P_done":
            queues.observe(demand.key, route.p, "finished", now)
            layout = natural_save(layout, route.p, demand, costs)
            queues.observe(demand.key, route.d, "enqueued", now)
            d_pending[route.d].append((demand, route))
        elif kind == "first":
            queues.observe(demand.key, route.d, "first", now)
            values.append(now - demand.arrival)
        elif kind == "done":
            queues.observe(demand.key, route.d, "finished", now)
            layout = natural_save(layout, route.d, demand, costs)
            d_active.remove(route.d)
            active_tasks.remove(demand.task)
        for node in (2, 3):
            if node in d_active or not d_pending[node]:
                continue
            item, selected = d_pending[node].popleft()
            length, first_seconds = decode_boundary(item, layout, costs, selected.p, node)
            layout = touch_layout(layout, node, item.hashes[:length])
            complete = first_seconds + costs.decode_seconds[node - 2]
            queues.revise_decode(item.key, first_seconds, complete)
            queues.observe(item.key, node, "started", now)
            d_active.add(node)
            schedule(now + first_seconds, "first", item, selected)
            schedule(now + complete, "done", item, selected)
        while pending:
            eligible = next((row for row in pending if row.task not in active_tasks), None)
            if eligible is None:
                break
            pending.remove(eligible)
            active_tasks.add(eligible.task)
            ready = max(now, eligible.arrival, delay)
            request = replace(eligible, arrival=ready)
            selected, _, _ = best_route(request, layout, costs, Calendar(), max_candidates=64,
                                        queues=queues, deadline=deadline, clock=clock)
            queues.reserve(eligible.key, selected, ready)
            queues.observe(eligible.key, selected.p, "enqueued", ready)
            start = max(ready, p_available[selected.p])
            p_available[selected.p] = start + selected.p_service_seconds
            schedule(start, "P_start", eligible, selected)
            schedule(p_available[selected.p], "P_done", eligible, selected)
    if not values or pending or active_tasks or queues.jobs:
        raise ValueError("Incomplete causal working-set replay")
    values.sort()
    return values[math.ceil(0.99 * len(values)) - 1]


def packages(history, *, deadline=None, clock=time.monotonic):
    unique = {}
    for demand in history:
        for size in range(len(demand.hashes), 0, -1):
            check_budget(deadline, clock)
            if demand.hashes[size - 1] not in unique:
                unique[demand.hashes[size - 1]] = demand.hashes[:size]
    return tuple(unique.values())


def neighbors(initial, history, *, deadline=None, clock=time.monotonic):
    chains = packages(history, deadline=deadline, clock=clock)

    def removals():
        for node in range(4):
            for unit in initial.units[node]:
                if not unit.protected and not unit.virtual:
                    yield Transition(initial.remove(node, (unit.identity,)), (Remove(node, (unit.identity,)),))

    def admissions():
        for chain in chains:
            for source, destination in itertools.permutations(range(4), 2):
                try:
                    yield copy_package(initial, source, destination, chain)
                except ValueError:
                    yield None

    def exchanges():
        for chain in chains:
            for source, destination in itertools.permutations(range(4), 2):
                for old in chains:
                    victims = tuple(unit.identity for unit in initial.units[destination]
                                    if unit.keys.intersection(old) and not unit.protected and not unit.virtual)
                    if not victims:
                        yield None
                        continue
                    try:
                        yield copy_package(initial, source, destination, chain, victims)
                    except ValueError:
                        yield None

    def moves():
        for chain in chains:
            for source, destination in itertools.permutations(range(4), 2):
                victims = tuple(unit.identity for unit in initial.units[source]
                                if unit.keys.intersection(chain) and not unit.protected and not unit.virtual)
                if not victims:
                    yield None
                    continue
                try:
                    copied = copy_package(initial, source, destination, chain)
                    moved = copied.layout.remove(source, victims)
                    yield Transition(moved, (*copied.operations, Remove(source, victims)))
                except ValueError:
                    yield None

    streams = deque(iter(stream) for stream in (admissions(), exchanges(), moves(), removals()))
    while streams:
        check_budget(deadline, clock)
        stream = streams.popleft()
        try:
            yield next(stream)
            streams.append(stream)
        except StopIteration:
            pass


@dataclass(frozen=True)
class Plan:
    transition: Transition
    initial_p99: float | None
    predicted_p99: float | None
    trials: int
    evaluations: int
    elapsed_seconds: float
    completed_ids: tuple[str, ...]
    stop_reason: str
    actions_applied: bool = False


def plan_layout(layout, history, costs, *, max_trials, deadline_seconds, calendar=None,
                clock: Callable = time.monotonic, event_queues=False):
    if type(max_trials) is not int or max_trials < 1 or deadline_seconds <= 0 or not layout.feasible():
        raise ValueError("A feasible snapshot and explicit work budget are required")
    if len({d.key for d in history}) != len(history):
        raise ValueError("Distinct completed requests required")
    started = clock()
    deadline = started + deadline_seconds
    best = Transition(layout)
    try:
        initial = replay(history, best, costs, calendar, deadline=deadline, clock=clock,
                         event_queues=event_queues)
    except PlanningExpired:
        return Plan(best, None, None, 0, 0, clock() - started,
                    tuple(d.key for d in history), "budget_before_complete_initial_score")
    score = initial
    trials = evaluations = 1
    seen = {layout.identity(): score}
    # A proposed sequence starts at this real snapshot. Accepted virtual
    # intermediate states carry all preceding transition operations/costs;
    # they are not presented as already executed or published.
    reason = "neighborhood_exhausted"
    stream = iter(neighbors(layout, history, deadline=deadline, clock=clock))
    while True:
        if trials >= max_trials or clock() - started >= deadline_seconds:
            reason = "work_budget_exhausted"
            break
        try:
            candidate = next(stream)
        except PlanningExpired:
            reason = "work_budget_exhausted"
            break
        except StopIteration:
            break
        trials += 1
        if candidate is None or not candidate.layout.feasible():
            continue
        candidate = Transition(candidate.layout, (*best.operations, *candidate.operations))
        identity = (candidate.layout.identity(), candidate.operations)
        value = seen.get(identity)
        if value is None:
            try:
                value = replay(history, candidate, costs, calendar, deadline=deadline, clock=clock,
                               event_queues=event_queues)
            except PlanningExpired:
                reason = "work_budget_exhausted"
                break
            seen[identity] = value
            evaluations += 1
        if value < score:
            best, score = candidate, value
            stream = iter(neighbors(best.layout, history, deadline=deadline, clock=clock))
    return Plan(best, initial, score, trials, evaluations, clock() - started,
                tuple(d.key for d in history), reason)


def execute_plan(plan, views, backend):
    receipts = []
    expected_generations = {view.node: view.snapshot.generation for view in views}
    for operation in plan.transition.operations:
        # Obtain a fresh actual snapshot before each local commit. There is no
        # distributed mutex or rollback claim; partial results are explicit.
        node = operation.node if isinstance(operation, Remove) else operation.destination
        current = backend.inventory(node)
        expected = views[node].snapshot
        if current["cache"]["epoch"] != expected.epoch:
            return {"status": "conflict", "receipts": receipts, "distributed_atomicity_claimed": False}
        if current["cache"]["generation"] != expected_generations[node]:
            return {"status": "conflict", "receipts": receipts, "distributed_atomicity_claimed": False}
        if isinstance(operation, Copy):
            receipt = backend.copy(operation, current)
            if receipt.get("published_locally") is not True or receipt.get("allocated_before_rdma") is not True:
                raise ValueError("Copy was not reserved, completed and published")
            if receipt["allocated_bytes"] > operation.allocation_bound:
                raise ValueError("Actual copy exceeded its staging reservation")
        else:
            # Unit IDs originate from the frozen snapshot; current ownership,
            # lease and generation are checked again by the native endpoint.
            receipt = backend.remove(operation, current, plan.transition.layout)
            if receipt.get("actions_applied") is not True:
                raise ValueError("Selected removal did not execute")
        receipts.append({"operation": operation, "receipt": receipt})
        expected_generations[node] = receipt["generation_after"]
    return {"status": "applied" if receipts else "no_action", "receipts": receipts,
            "distributed_atomicity_claimed": False}
