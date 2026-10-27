"""Finite FIFO stage scheduling with shared source/destination resources.

An operation acquires all resources together and releases all at completion.
Waiting operations hold no device resource. This is a model of owned work,
not a guarantee about unknown platform contention or native kernel overlap.
"""

from __future__ import annotations

import heapq
import itertools
import math
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class Residual:
    full_seconds: float
    first_seconds: float | None = None

    def __post_init__(self):
        if not math.isfinite(self.full_seconds) or self.full_seconds < 0:
            raise ValueError("Finite nonnegative remaining lifetime required")
        if self.first_seconds is not None and not 0 <= self.first_seconds <= self.full_seconds:
            raise ValueError("Unobserved first token must lie within the remaining lifetime")


@dataclass(frozen=True)
class Operation:
    name: str
    seconds: float
    resources: tuple[str, ...]
    first_seconds: float | None = None

    def __post_init__(self):
        if not self.name or not math.isfinite(self.seconds) or self.seconds < 0:
            raise ValueError("Finite nonnegative owned operation required")
        if len(set(self.resources)) != len(self.resources):
            raise ValueError("Duplicate operation resource")
        if self.first_seconds is not None and not 0 <= self.first_seconds <= self.seconds:
            raise ValueError("First token must occur within its full operation")


def merge_operations(previous, updated, preserve):
    """Keep acquired work immutable without appending new predecessors late."""
    old = {operation.name: operation for operation in previous}
    new = {operation.name: operation for operation in updated}
    values = {name: old[name] if name in preserve else operation for name, operation in new.items()
              if name not in preserve or name in old}
    values.update({name: operation for name, operation in old.items() if name in preserve})
    order = [operation.name for operation in updated if operation.name in values]
    prior = [operation.name for operation in previous]
    for index, name in enumerate(prior):
        if name not in values or name in order:
            continue
        successor = next((n for n in prior[index+1:] if n in order), None)
        if successor is None:
            order.append(name)
        else:
            order.insert(order.index(successor), name)
    return tuple(values[name] for name in order)


@dataclass(frozen=True)
class Work:
    key: str
    p: int
    d: int
    order: int
    offered: float
    prefill: tuple[Operation, ...]
    decode: tuple[Operation, ...]
    p_done: float | None = None
    d_enqueued: float | None = None
    p_started: float | None = None
    d_started: float | None = None
    actual_first: float | None = None
    completed_operations: frozenset[str] = frozenset()
    active_operations: tuple[tuple[str, float], ...] = ()
    p_not_before: float | None = None
    d_not_before: float | None = None

    def __post_init__(self):
        if self.p not in (0, 1) or self.d not in (2, 3) or not self.key:
            raise ValueError("An identified P/D assignment is required")
        names = [o.name for o in (*self.prefill, *self.decode)]
        if len(set(names)) != len(names):
            raise ValueError("Unique per-request operation names required")
        if len(self.active_operations) > 1:
            raise ValueError("The declared request operation chain is sequential")
        if any(v is not None and (not math.isfinite(v) or v < 0)
               for v in (self.p_not_before, self.d_not_before)):
            raise ValueError("Finite nonnegative stage release required")


@dataclass
class _Stage:
    work: Work
    node: int
    operations: deque[Operation]
    ready: float


def schedule(work, now, remaining=None, resource_blocks=None, *, chain_builder=None,
             operation_finished=None, operation_started=None):
    """Forecast from actual progress, preserving FIFO and complete decode use.

    At most one executor stage per device runs. Copies can borrow a different
    device's gate without taking that device's executor slot. Future D FIFO
    order follows simulated P completions; already-enqueued D keeps its order.
    Complexity is O(V log V + V J), space O(V+J), with V operation events and
    J live requests. The ready scan has at most four stage heads.
    """
    if not math.isfinite(now) or now < 0:
        raise ValueError("Finite nonnegative forecast origin required")
    work = tuple(work)
    if len({w.key for w in work}) != len(work):
        raise ValueError("Distinct owned requests required")
    queues = [deque() for _ in range(4)]
    active: dict[int, _Stage] = {}
    held: dict[str, tuple[str, str]] = {}
    events, serial = [], itertools.count()
    first, p_done, full, starts = {}, {}, {}, {}
    trace = []
    completed = {w.key: set(w.completed_operations) for w in work}

    def event(at, kind, stage, operation=None):
        heapq.heappush(events, (at, next(serial), kind, stage, operation))

    def chain(w, p):
        operations = w.prefill if p else w.decode
        return deque(o for o in operations if o.name not in w.completed_operations)

    for node in (0, 1):
        for w in sorted((w for w in work if w.p == node and w.p_done is None), key=lambda w: w.order):
            ready = max(now, w.p_not_before or now)
            stage = _Stage(w, node, chain(w, True), ready)
            queues[node].append(stage)
            if ready > now:
                event(ready, "wake", stage)
    for node in (2, 3):
        ready = sorted((w for w in work if w.d == node and w.p_done is not None),
                       key=lambda w: (w.d_enqueued if w.d_enqueued is not None else w.p_done, w.order))
        for w in ready:
            p_done[w.key] = w.p_done
            ready_at = max(now, w.d_not_before or now)
            stage = _Stage(w, node, chain(w, False), ready_at)
            queues[node].append(stage)
            if ready_at > now:
                event(ready_at, "wake", stage)
    for w in work:
        if w.actual_first is not None:
            first[w.key] = w.actual_first

    # An accepted layout commit borrows the union of its device gates. Its
    # generation-checked execution is nonblocking if an owned operation holds
    # any of them; do not model a commit that the executor must defer.
    for resource, until in (resource_blocks or {}).items():
        if not math.isfinite(until) or until < now:
            raise ValueError("Invalid initial commit resource interval")
        if until > now:
            held[resource] = ("__commit__", resource)
            heapq.heappush(events, (until, next(serial), "unblock", None, resource))

    def finish_stage(stage, at):
        del active[stage.node]
        if stage.node < 2:
            p_done[stage.work.key] = at
            ready_at = max(at, stage.work.d_not_before or at)
            next_stage = _Stage(stage.work, stage.work.d, chain(stage.work, False), ready_at)
            queues[stage.work.d].append(next_stage)
            if ready_at > at:
                event(ready_at, "wake", next_stage)
        else:
            full[stage.work.key] = at

    # Install real holders before predicting any new acquisition. A started
    # executor waiting for a gate is not falsely installed as a gate holder.
    for node in range(4):
        if not queues[node]:
            continue
        stage = queues[node][0]
        began = stage.work.p_started if node < 2 else stage.work.d_started
        if began is None:
            continue
        active[node] = queues[node].popleft()
        starts[(stage.work.key, node)] = began
        actual = dict(stage.work.active_operations)
        if not actual:
            continue
        if not stage.operations or stage.operations[0].name not in actual:
            raise ValueError("Actual progress does not match the remaining operation chain")
        operation = stage.operations[0]
        elapsed = max(0.0, now - actual[operation.name])
        estimate = remaining(stage.work, operation, elapsed) if remaining else max(0, operation.seconds - elapsed)
        duration = estimate.full_seconds if isinstance(estimate, Residual) else estimate
        if not math.isfinite(duration) or duration < 0:
            raise ValueError("Invalid residual operation estimate")
        identity = stage.work.key, operation.name
        for resource in operation.resources:
            if resource in held:
                raise ValueError("Two actual operations claim the same exclusive resource")
            held[resource] = identity
        if operation_started is not None:
            operation_started(stage.work, stage.node, operation, now)
        if operation.first_seconds is not None and stage.work.key not in first:
            first_remaining = (estimate.first_seconds if isinstance(estimate, Residual)
                               and estimate.first_seconds is not None else max(0, operation.first_seconds-elapsed))
            event(now + min(duration, first_remaining), "first", stage, operation)
        event(now + duration, "finish", stage, operation)

    at = now
    running = {(e[3].work.key, e[4].name) for e in events if e[2] == "finish"}
    while len(full) < len(work):
        changed = True
        while changed:
            changed = False
            for node in range(4):
                if node not in active and queues[node] and queues[node][0].ready <= at:
                    stage = queues[node].popleft()
                    active[node] = stage
                    starts[(stage.work.key, node)] = at
                    changed = True
            for stage in sorted(active.values(), key=lambda s: (s.ready, s.work.order, s.node)):
                if stage.ready > at:
                    continue
                if not stage.operations and chain_builder is not None:
                    stage.operations = deque(chain_builder(
                        stage.work, stage.node, frozenset(completed[stage.work.key])))
                if not stage.operations:
                    finish_stage(stage, at)
                    changed = True
                    continue
                operation = stage.operations[0]
                identity = stage.work.key, operation.name
                if identity in running:
                    continue
                if chain_builder is not None:
                    stage.operations = deque(chain_builder(
                        stage.work, stage.node, frozenset(completed[stage.work.key])))
                    if not stage.operations:
                        finish_stage(stage, at)
                        changed = True
                        continue
                    operation = stage.operations[0]
                    identity = stage.work.key, operation.name
                if any(resource in held for resource in operation.resources):
                    continue
                for resource in operation.resources:
                    held[resource] = identity
                running.add(identity)
                if operation_started is not None:
                    operation_started(stage.work, stage.node, operation, at)
                trace.append({"key": stage.work.key, "node": stage.node, "operation": operation.name,
                              "start": at, "end": at + operation.seconds, "resources": operation.resources})
                if operation.first_seconds is not None and stage.work.key not in first:
                    event(at + operation.first_seconds, "first", stage, operation)
                event(at + operation.seconds, "finish", stage, operation)
                changed = True
        if not events:
            if len(full) == len(work):
                break
            raise ValueError("Owned operation graph cannot make progress")
        at, _, kind, stage, operation = heapq.heappop(events)
        if kind == "wake":
            continue
        if kind == "unblock":
            held.pop(operation)
            continue
        if kind == "first":
            first.setdefault(stage.work.key, at)
            continue
        identity = stage.work.key, operation.name
        running.remove(identity)
        for resource in operation.resources:
            if held.pop(resource) != identity:
                raise ValueError("Resource ownership changed before completion")
        if stage.operations.popleft() is not operation:
            raise ValueError("Operation completion order changed")
        completed[stage.work.key].add(operation.name)
        if operation_finished is not None:
            operation_finished(stage.work, stage.node, operation, at)
        stage.ready = at
    if any(queues) or active or held or len(first) != len(work):
        raise ValueError("Incomplete first-token/full-completion forecast")
    return {"P_end": p_done, "first": first, "full": full,
            "stage_start": starts, "operations": trace}
