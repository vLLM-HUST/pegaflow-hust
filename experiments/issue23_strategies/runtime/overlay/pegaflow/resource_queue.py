"""Receding-horizon resource DAG over admitted work, with real progress."""

from __future__ import annotations

import contextlib
import math
from dataclasses import dataclass, replace

from pegaflow.joint_policy import Route
from pegaflow.joint_policy import copy_package
from pegaflow.joint_queue import QueueLedger
from pegaflow.operation_schedule import Operation, Work, schedule


def direction(source, destination):
    return "pd" if source < 2 <= destination else "dp" if destination < 2 <= source else "P" if source < 2 else "D"


@dataclass(frozen=True)
class ResourceRoute(Route):
    predicted_system_p99: float = 0.0
    operation_model_version: int = 0
    model_latest_completed_seconds: float = 0.0
    forecast_origin_seconds: float = 0.0
    objective_scope: str = "admitted-live-requests-plus-candidate"


def build_work(key, order, offered, demand, route, layout, costs):
    def copied(name, source, destination, count):
        missing = set(demand.hashes[:count]) - layout.keys(destination)
        if source == destination or not missing:
            return ()
        payload = len(missing) * demand.block_tokens * costs.bytes_per_token
        return (Operation(name, costs.copy_seconds(source, destination, payload),
            (f"gate{source}", f"gate{destination}", "link:" + direction(source, destination))),)

    p_api = Operation("P_api", costs.compute(route.p, demand.tokens, route.p_blocks * demand.block_tokens), (f"gate{route.p}",))
    first = costs.compute(route.d, demand.tokens, route.d_blocks * demand.block_tokens)
    d_api = Operation("D_api", first + costs.decode_seconds[route.d - 2], (f"gate{route.d}",), first)
    return Work(key, route.p, route.d, order, offered,
        (*copied("history", route.source, route.p, route.p_blocks), p_api),
        (*copied("handoff", route.p, route.d, route.d_blocks), d_api))


def objective(work, prediction):
    delays = sorted(prediction["first"][w.key] - w.offered for w in work)
    return delays[math.ceil(0.99 * len(delays)) - 1] if delays else 0.0


class ResourceQueues(QueueLedger):
    def __init__(self, models):
        super().__init__()
        self.models = models
        self.work, self.demands, self.routes = {}, {}, {}
        self.operation_events, self.operation_samples = [], {}
        self.progress_version = 0
        self.context = None

    @contextlib.contextmanager
    def decision(self, demand, layout, costs, offered, now):
        # The lock freezes queue progress and the cost snapshot for all route
        # candidates; it never spans an inventory RPC or a device acquisition.
        with self.guard:
            if self.context is not None:
                raise ValueError("Nested routing decision")
            self.context = demand, layout, costs, offered, now
            try:
                yield
            finally:
                self.context = None

    def _remaining_operation(self, work, operation, elapsed):
        rows = self.operation_samples.get((work.p if operation.name in ("P_api", "history", "P_admission") else work.d, operation.name), ())
        # Conditional empirical residuals use completed observations only. A
        # live overrun is not silently relabelled a finished training sample.
        values = [actual / predicted * operation.seconds - elapsed for predicted, actual in rows
                  if predicted > 0 and actual / predicted * operation.seconds > elapsed]
        return sum(values) / len(values) if values else max(0.0, operation.seconds - elapsed)

    def forecast(self, route, now):
        with self.guard:
            demand, layout, costs, offered, origin = self.context
            candidate = build_work("__candidate__", self.sequence, offered, demand, route, layout, costs)
            work = (*self.work.values(), candidate)
            predicted = schedule(work, origin, self._remaining_operation)
            return ResourceRoute(**{**route.__dict__,
                "predicted_p_end": predicted["P_end"][candidate.key],
                "predicted_first": predicted["first"][candidate.key],
                "predicted_end": predicted["full"][candidate.key]},
                predicted_system_p99=objective(work, predicted), operation_model_version=costs.version,
                model_latest_completed_seconds=costs.latest_completed_seconds, forecast_origin_seconds=origin)

    def reserve(self, key, route, now):
        with self.guard:
            demand, layout, costs, offered, origin = self.context
            super().reserve(key, route, now)
            self.work[key] = build_work(key, self.jobs[key].order, offered, demand, route, layout, costs)
            self.demands[key], self.routes[key] = demand, route
            self.progress_version += 1

    def observe(self, key, node, kind, now):
        with self.guard:
            w = self.work[key]
            super().observe(key, node, kind, now)
            self.progress_version += 1
            if kind == "cancelled" or (kind == "finished" and node >= 2):
                self.work.pop(key)
                self.demands.pop(key)
                self.routes.pop(key)
                return
            fields = {}
            if kind == "started":
                fields["p_started" if node < 2 else "d_started"] = now
            elif kind == "enqueued" and node >= 2:
                fields["d_enqueued"] = now
                fields["d_not_before"] = now
            elif kind == "enqueued" and node < 2:
                fields["p_not_before"] = now
            elif kind == "finished" and node < 2:
                fields["p_done"] = now
            elif kind == "first":
                fields["actual_first"] = now
            self.work[key] = replace(w, **fields)

    def operation(self, key, name, kind, now, resources=None):
        with self.guard:
            w = self.work[key]
            all_ops = (*w.prefill, *w.decode)
            op = next((o for o in all_ops if o.name == name), None)
            if op is None:
                if resources is None or name not in ("history", "handoff", "P_admission", "D_admission"):
                    raise ValueError("Undeclared owned operation")
                op = Operation(name, 0.0, tuple(resources))
                is_prefill = name in ("history", "P_admission")
                w = replace(w, **{("prefill" if is_prefill else "decode"):
                    (op, *(w.prefill if is_prefill else w.decode))})
            if kind == "start":
                if w.active_operations:
                    raise ValueError("Owned sequential chain already active")
                w = replace(w, active_operations=((name, now),))
            elif kind == "finish":
                actual = dict(w.active_operations)
                if name not in actual or now < actual[name]:
                    raise ValueError("Operation completion without acquisition")
                node = w.p if name in ("history", "P_api", "P_admission") else w.d
                self.operation_samples.setdefault((node, name), []).append((op.seconds, now - actual[name]))
                w = replace(w, active_operations=(), completed_operations=w.completed_operations | {name})
            else:
                raise ValueError("Invalid resource operation event")
            self.work[key] = w
            self.progress_version += 1
            self.operation_events.append(dict(key=key, operation=name, event=kind, seconds=now,
                resources=op.resources, predicted_seconds=op.seconds))

    def skip_copy(self, key, name):
        with self.guard:
            w = self.work[key]
            self.work[key] = replace(w, completed_operations=w.completed_operations | {name})
            self.progress_version += 1

    def revise(self, key, route, layout, costs):
        with self.guard:
            w = self.work[key]
            newer = build_work(key, w.order, w.offered, self.demands[key], route, layout, costs)
            # Revisions affect only unstarted operations; the original route
            # prediction remains in requests.jsonl for honest error analysis.
            newer = replace(newer, p_started=w.p_started, p_done=w.p_done, d_enqueued=w.d_enqueued,
                d_started=w.d_started, actual_first=w.actual_first,
                completed_operations=w.completed_operations, active_operations=w.active_operations)
            self.work[key], self.routes[key] = newer, route
            self.progress_version += 1

    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
        with self.guard:
            item, current = self.demands[key], self.work[key]
            limit = max(layout.prefix(route.p, item.hashes), layout.prefix(route.d, item.hashes))
            lengths = tuple(dict.fromkeys((layout.prefix(route.d, item.hashes), limit, 0, *range(limit-1, 0, -1))))
            best = None
            for length in lengths[:max_candidates]:
                payload = 0
                if length > layout.prefix(route.d, item.hashes):
                    try:
                        payload = copy_package(layout, route.p, route.d, item.hashes[:length]).operations[-1].payload_bytes
                    except ValueError:
                        continue
                candidate = replace(route, d_blocks=length)
                w = build_work(key, current.order, current.offered, item, candidate, layout, costs)
                w = replace(w, p_done=current.p_done, d_enqueued=current.d_enqueued,
                    p_started=current.p_started, d_started=current.d_started,
                    actual_first=current.actual_first, completed_operations=current.completed_operations,
                    active_operations=current.active_operations)
                work = tuple(w if value.key == key else value for value in self.work.values())
                predicted = schedule(work, now, self._remaining_operation)
                initial = costs.compute(route.d, item.tokens, length*item.block_tokens)
                seconds = initial + costs.copy_seconds(route.p, route.d, payload)
                order = objective(work, predicted), predicted["first"][key], payload, length
                if best is None or order < best[0]:
                    best = order, length, seconds
            if best is None:
                raise ValueError("No safe decode/recompute boundary")
            return best[1:]

    def snapshot(self):
        with self.guard:
            data = super().snapshot()
            data.update(resource_operation_events=list(self.operation_events),
                source_and_destination_gates_modelled=True, progress_version=self.progress_version,
                resource_live_requests=len(self.work), objective_scope="admitted-live-requests",
                resource_calendar_is_unknown_external_contention_guarantee=False)
            return data
