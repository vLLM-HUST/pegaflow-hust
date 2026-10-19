"""Event-anchored FIFO forecasts; completed observations never reserve devices.

This is a deterministic finite-workload forecast, not an M/M/1 model or a
throughput guarantee. D FIFO order is actual enqueue order, then estimated P
completion order for work which has not reached D yet.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, replace


@dataclass
class Pending:
    key: str
    p: int
    d: int
    order: int
    submitted: float
    p_seconds: float
    d_first_seconds: float
    d_seconds: float
    p_started: float | None = None
    p_done: float | None = None
    d_enqueued: float | None = None
    d_started: float | None = None
    d_first: float | None = None


class QueueLedger:
    def __init__(self):
        self.guard = threading.RLock()
        self.jobs: dict[str, Pending] = {}
        self.sequence = 0
        self.completed: list[list[tuple[float, float]]] = [[] for _ in range(4)]
        self.completed_first: list[list[tuple[float, float]]] = [[] for _ in range(4)]
        self.events: list[dict] = []

    def reserve(self, key, route, now):
        with self.guard:
            if key in self.jobs:
                raise ValueError("Duplicate live request reservation")
            values = (route.p_service_seconds, route.d_first_service_seconds,
                      route.d_service_seconds, now)
            if any(not math.isfinite(v) or v < 0 for v in values):
                raise ValueError("Finite nonnegative stage forecasts required")
            if route.d_first_service_seconds > route.d_service_seconds:
                raise ValueError("First token cannot follow full completion")
            self.jobs[key] = Pending(key, route.p, route.d, self.sequence, now,
                                     *values[:3])
            self.sequence += 1

    def observe(self, key, node, kind, now):
        with self.guard:
            job = self.jobs.get(key)
            if job is None:
                raise ValueError("Queue event has no owned reservation")
            if node not in (job.p, job.d) or not math.isfinite(now) or now < job.submitted:
                raise ValueError("Invalid owned queue event")
            self.events.append({"key": key, "node": node, "event": kind, "seconds": now})
            if kind == "cancelled":
                del self.jobs[key]
            elif kind == "enqueued" and node == job.d:
                job.d_enqueued = now
            elif kind == "started":
                if node == job.p:
                    job.p_started = now
                else:
                    if job.p_done is None:
                        raise ValueError("D cannot start before actual P completion")
                    job.d_started = now
            elif kind == "first":
                if node != job.d or job.d_started is None or now < job.d_started:
                    raise ValueError("First token without actual D start")
                if job.d_first is not None:
                    raise ValueError("Duplicate first token observation")
                job.d_first = now
            elif kind == "finished":
                started = job.p_started if node == job.p else job.d_started
                if started is None or now < started:
                    raise ValueError("Completion without actual stage start")
                predicted = job.p_seconds if node == job.p else job.d_seconds
                self.completed[node].append((predicted, now - started))
                if node == job.p:
                    job.p_done = now
                else:
                    if job.d_first is not None:
                        self.completed_first[node].append((job.d_first_seconds,
                                                            job.d_first - started))
                    del self.jobs[key]

    def revise_decode(self, key, first_seconds, complete_seconds):
        if not 0 <= first_seconds <= complete_seconds or not math.isfinite(complete_seconds):
            raise ValueError("Invalid revalidated D service estimate")
        with self.guard:
            job = self.jobs[key]
            job.d_first_seconds, job.d_seconds = first_seconds, complete_seconds

    def _duration(self, node, base):
        # Complete stage wall includes inventory, gate, transfer and API work.
        # Learn only the nonnegative omitted-overhead mean, not queue waiting.
        rows = self.completed[node]
        overhead = max(0.0, sum(actual - predicted for predicted, actual in rows) / len(rows)) if rows else 0.0
        return base + overhead

    def _remaining(self, node, base, elapsed):
        # Empirical mean excess on past completed duration/base ratios. A
        # right-censored live overrun is NOT inserted as a completed sample.
        ratios = [actual / predicted for predicted, actual in self.completed[node]
                  if predicted > 0 and actual / predicted * base > elapsed]
        if ratios:
            return sum(ratio * base - elapsed for ratio in ratios) / len(ratios)
        return max(0.0, self._duration(node, base) - elapsed)

    def forecast(self, route, now):
        with self.guard:
            candidate = Pending("__candidate__", route.p, route.d, self.sequence, now,
                                route.p_service_seconds, route.d_first_service_seconds,
                                route.d_service_seconds)
            jobs = [replace(job) for job in self.jobs.values()] + [candidate]
            p_done, first, done = {}, {}, {}
            for node in (0, 1):
                cursor = now
                for job in sorted((j for j in jobs if j.p == node), key=lambda j: j.order):
                    if job.p_done is not None:
                        p_done[job.key] = job.p_done
                        continue
                    seconds = self._duration(node, job.p_seconds)
                    if job.p_started is not None:
                        seconds = self._remaining(node, job.p_seconds, now - job.p_started)
                    cursor += seconds
                    p_done[job.key] = cursor
            for node in (2, 3):
                cursor = now
                ready = sorted((j for j in jobs if j.d == node and j.d_enqueued is not None),
                               key=lambda j: (j.d_enqueued, j.order))
                future = sorted((j for j in jobs if j.d == node and j.d_enqueued is None),
                                key=lambda j: (p_done[j.key], j.order))
                for job in (*ready, *future):
                    cursor = max(cursor, p_done[job.key])
                    total = self._duration(node, job.d_seconds)
                    first_rows = self.completed_first[node]
                    overhead = max(0.0, sum(actual - predicted for predicted, actual in first_rows) / len(first_rows)) if first_rows else 0.0
                    initial = min(total, job.d_first_seconds + overhead)
                    if job.d_started is not None:
                        total = self._remaining(node, job.d_seconds, now - job.d_started)
                        initial = min(total, max(0.0, initial - (now - job.d_started)))
                    first[job.key], cursor = cursor + initial, cursor + total
                    done[job.key] = cursor
            return replace(route, predicted_p_end=p_done[candidate.key],
                           predicted_first=first[candidate.key], predicted_end=done[candidate.key])

    def snapshot(self):
        with self.guard:
            return {"live_requests": len(self.jobs), "completed_stage_samples":
                    [len(rows) for rows in self.completed], "events": list(self.events),
                    "future_D_order": "predicted_P_completion_not_router_submission",
                    "right_censored_overrun_is_completed_sample": False,
                    "unknown_external_gate_and_link_contention_guaranteed": False}
