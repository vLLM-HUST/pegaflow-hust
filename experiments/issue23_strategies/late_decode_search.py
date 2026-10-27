"""Reconsider an unacquired D destination after actual P publication.

Only an observed full/queued destination with a currently available alternative
triggers search. This is not a balancing quota or a promise of a future slot.
Search and its RPC delay hold no device/sequence lease. D boundary is checked
again at actual admission, so projected cache acceptance is never guaranteed.
"""

import itertools
import time
from dataclasses import replace

from bounded_search import ForecastDominated
from pegaflow.joint_policy import copy_package
from pegaflow.pending_planner import pending_profile
from pegaflow.resource_cache_profile import optional_work
from pegaflow.resource_state_profile import refresh_work
from release_cobatch_search import (
    ReleaseCobatchQueues,
    ReleaseCobatchSimulation,
    ReleaseCobatchSnapshot,
)


class LateDecodeSnapshot(ReleaseCobatchSnapshot):
    def configure_projection(self, projection, now):
        options = super().configure_projection(projection, now)
        releases = options.setdefault("admission_releases", {})
        releases.update(
            {
                w.key: w.d_not_before
                for w in self.work.values()
                if w.p_done is not None
                and w.d_started is None
                and w.d_not_before is not None
                and w.d_not_before > now
            }
        )
        releases.update(getattr(self, "late_release_overrides", {}))
        return options


class LateDecodeQueues(ReleaseCobatchQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = LateDecodeSnapshot
        return shadow


def should_reconsider(destination, active, queued, capacity):
    alternative = 5 - destination
    return (active[destination] >= capacity or queued[destination]) and (
        active[alternative] < capacity and not queued[alternative]
    )


def require_unacquired(work, route):
    if (
        work.p_done is None
        or work.d_started is not None
        or work.actual_first is not None
        or work.active_operations
        or any(
            name.startswith("D_") or name == "handoff"
            for name in work.completed_operations
        )
        or route.p != work.p
    ):
        raise ValueError("D reassignment requires a published, unacquired suffix")


def select_destination(shadow, key, route, layout, costs, now, delay, budget=64):
    current = shadow.work[key]
    require_unacquired(current, route)
    shadow.refresh(layout, costs)
    current, item = shadow.work[key], shadow.demands[key]
    shadow.boundary_request = key
    shadow.incumbent_first_profile = None
    shadow.late_release_overrides = {key: now + delay}
    sources = []
    for node in (route.d, 5 - route.d):
        local = layout.prefix(node, item.hashes)
        limit = max(local, layout.prefix(route.p, item.hashes))
        lengths = dict.fromkeys((local, limit, 0, *range(limit - 1, 0, -1)))
        sources.append(iter(tuple((node, n, local) for n in lengths)))
    # Interleave both destinations under the existing 64-candidate budget.
    # Neither destination monopolizes the budget. Ties retain the initial D.
    candidates = (
        v for pair in itertools.zip_longest(*sources) for v in pair if v is not None
    )
    best, evaluations = None, []
    for node, length, local in itertools.islice(candidates, budget):
        payload = 0
        if length > local:
            try:
                payload = (
                    copy_package(layout, route.p, node, item.hashes[:length])
                    .operations[-1]
                    .payload_bytes
                )
            except ValueError:
                continue
        option = replace(route, d=node, d_blocks=length, d_headroom=False)
        changed = refresh_work(
            current,
            optional_work(
                key, current.order, current.offered, item, option, layout, costs
            ),
        )
        changed = replace(changed, d_enqueued=now + delay, d_not_before=now + delay)
        work = tuple(changed if w.key == key else w for w in shadow.work.values())
        try:
            prediction = shadow.predict_batch(
                work, now, shadow.demands, {**shadow.routes, key: option}, costs
            )
        except ForecastDominated:
            continue
        objective = pending_profile(work, prediction)
        value = objective, payload, length, node != route.d
        evaluations.append(
            {
                "d": node,
                "prefix_blocks": length,
                "payload": payload,
                "objective": objective,
                "first": prediction["first"][key],
                "full": prediction["full"][key],
            }
        )
        if best is None or value < best[0]:
            best = value, option
            shadow.incumbent_first_profile = objective[:2]
    if best is None:
        raise ValueError("No feasible late D candidate including recomputation")
    return best[1], evaluations


class LateDecodeSimulation(ReleaseCobatchSimulation):
    queue_type = LateDecodeQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.late_pending, self.late_records = {}, []

    def enqueue_decode(self, key):
        if self.mode == "native":
            return super().enqueue_decode(key)
        job = self.jobs[key]
        destination = job["plan"].d
        active = {
            node: sum(n == node for n in self.d_active.values()) for node in (2, 3)
        }
        queued = {node: len(self.queues[node]) for node in (2, 3)}
        if not should_reconsider(
            destination, active, queued, self.batch_config.max_sequences
        ):
            return super().enqueue_decode(key)
        began = time.perf_counter()
        origin = self.now
        costs = self.frozen()
        shadow = self.policy_queues.planning_snapshot()
        known = [r for r in self.late_records if r["decision_ready"] <= origin]
        estimate = sum(r["CPU_wall"] for r in known) / len(known) if known else 0.0
        selected, evaluations = select_destination(
            shadow,
            key,
            job["live_route"],
            self.layout,
            costs,
            origin,
            estimate + self.rpc_boundary,
        )
        record = {
            "id": key,
            "origin": origin,
            "before": destination,
            "after": selected.d,
            "slots": active,
            "queued": queued,
            "predicted_CPU": estimate,
            "completed_CPU_samples": len(known),
            "model_latest": costs.latest_completed_seconds,
            "CPU_latest": max((r["decision_ready"] for r in known), default=None),
            "evaluations": evaluations,
            "search": dict(shadow.search_stats),
        }
        # Snapshot/fit/search/record construction is inside the measured cost.
        cpu = time.perf_counter() - began
        record.update(
            CPU_wall=cpu,
            RPC_seconds=self.rpc_boundary,
            decision_ready=origin + cpu + self.rpc_boundary,
        )
        self.late_records.append(record)
        self.late_pending[key] = selected, record
        job["work"] = replace(
            job["work"], p_done=self.now, d_not_before=record["decision_ready"]
        )
        self.policy_queues.work[key] = replace(
            self.policy_queues.work[key], d_not_before=record["decision_ready"]
        )
        self.event(record["decision_ready"], "admission", key)

    def pump(self):
        for key, (selected, record) in tuple(self.late_pending.items()):
            if self.now < record["decision_ready"]:
                continue
            began = time.perf_counter()
            job = self.jobs[key]
            queues = self.policy_queues
            require_unacquired(queues.work[key], selected)
            if key in self.d_active or any(key in queue for queue in self.queues):
                raise ValueError("Late route duplicated an owned queue/slot")
            # No whole-snapshot replacement: only this request's unstarted D.
            job["plan"] = replace(
                job["plan"], d=selected.d, d_prefix=selected.d_blocks * 128
            )
            job["live_route"] = selected
            job["work"] = replace(job["work"], d=selected.d)
            queues.jobs[key].d = selected.d
            queues.routes[key] = selected
            queues.work[key] = replace(queues.work[key], d=selected.d)
            queues.admission_choices[key, selected.d] = False
            queues.progress_version += 1
            record["commit_CPU_wall"] = time.perf_counter() - began
            self.now += record["commit_CPU_wall"]
            record["committed"] = self.now
            super().enqueue_decode(key)
            del self.late_pending[key]
        return super().pump()

    def run(self):
        result = super().run()
        if self.late_pending:
            raise ValueError("Late decisions did not drain")
        result["internal_candidate"]["name"] = "late-decode-reconsider-01"
        result["late_decode_decisions"] = self.late_records
        result["late_decode_CPU_wall"] = sum(
            r["CPU_wall"] + r["commit_CPU_wall"] for r in self.late_records
        )
        result["late_decode_cost_in_TTFT"] = True
        return result
