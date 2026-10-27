"""Reconsider unacquired P-head recovery when its remote P source is busy.

One bounded decision on the existing route CPU lane; no device lease is held.
Keep is immediate in the comparison. Alternatives pay completed-only controller
cost and retain P FIFO ownership while considering local cache or recomputation.
No future gate end, physics label, quota, or congestion penalty is consulted.
"""

import time
from dataclasses import replace

from late_decode_search import LateDecodeSnapshot
from pegaflow.operation_schedule import Operation
from pegaflow.pending_planner import pending_profile
from pegaflow.resource_cache_profile import optional_work
from pegaflow.resource_state_profile import refresh_work
from yielding_decode_search import YieldingDecodeSimulation


def require_waiting_P(work, route):
    if (
        work.p_started is not None
        or work.p_done is not None
        or work.d_started is not None
        or work.active_operations
        or work.completed_operations - {"control"}
        or work.p != route.p
    ):
        raise ValueError("P revalidation requires an unacquired FIFO head")


class PrefillControlSnapshot(LateDecodeSnapshot):
    def configure_projection(self, projection, now):
        options = super().configure_projection(projection, now)
        target, delay = getattr(self, "prefill_control", (None, 0.0))
        original_admit = projection.admit

        def admit(work, node, previous, progress):
            operations = original_admit(work, node, previous, progress)
            if work.key == target and node < 2:
                require_waiting_P(work, self.routes[target])
                # Occupy the P FIFO position, not its device. Releasing the key
                # out of the FIFO would incorrectly let younger P requests pass.
                return (Operation("P_boundary_control", delay, ()), *operations)
            return operations

        projection.admit = admit
        return options


def select_prefill(shadow, key, route, layout, costs, now, delay):
    require_waiting_P(shadow.work[key], route)
    shadow.__class__ = PrefillControlSnapshot
    shadow.refresh(layout, costs)
    shadow.boundary_request = key
    shadow.incumbent_first_profile = None
    shadow.late_release_overrides = {}
    shadow.prefill_control = (None, 0.0)
    current, item = shadow.work[key], shadow.demands[key]
    original = shadow.predict_batch(
        tuple(shadow.work.values()), now, shadow.demands, shadow.routes, costs
    )
    best = pending_profile(tuple(shadow.work.values()), original)
    selected = route
    rows = [
        {
            "kind": "keep",
            "prefix": route.p_blocks,
            "objective": best,
            "first": original["first"][key],
            "P_end": original["P_end"][key],
            "delay": 0.0,
        }
    ]
    # At most three calendars: unchanged, currently local prefix, zero-prefix.
    # No candidate budget is increased or extra restore-safety parameter added.
    for count in dict.fromkeys((layout.prefix(route.p, item.hashes), 0)):
        option = replace(route, source=route.p, p_blocks=count, p_headroom=False)
        changed = refresh_work(
            current,
            optional_work(
                key, current.order, current.offered, item, option, layout, costs
            ),
        )
        works = tuple(changed if w.key == key else w for w in shadow.work.values())
        shadow.prefill_control = (key, delay)
        # Control is an evaluation-local prefix; do not reuse a no-control memo.
        shadow.cache_batch_memo = {}
        prediction = shadow.predict_batch(
            works, now, shadow.demands, {**shadow.routes, key: option}, costs
        )
        objective = pending_profile(works, prediction)
        rows.append(
            {
                "kind": "local",
                "prefix": count,
                "objective": objective,
                "first": prediction["first"][key],
                "P_end": prediction["P_end"][key],
                "delay": delay,
            }
        )
        if objective < best:
            selected, best = option, objective
    return selected, rows


class PrefillRevalidationSimulation(YieldingDecodeSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefill_pending = None
        self.prefill_attempted = set()
        self.prefill_records = []

    def admission_ready(self, node, free_slots):
        if self.mode == "native" or node >= 2:
            return super().admission_ready(node, free_slots)
        if self.prefill_pending is not None and self.prefill_pending[2]["node"] == node:
            return False
        if free_slots <= 0 or not self.queues[node] or self.router_busy:
            return super().admission_ready(node, free_slots)
        key = self.queues[node][0]
        job = self.jobs[key]
        route = job["live_route"]
        source = route.source
        held = self.held.get(f"gate{source}")
        if (
            key in self.prefill_attempted
            or source >= 2
            or source == node
            or held is None
            or held[1] != "P_api"
            or self.layout.prefix(source, self.item(job["row"]).hashes)
            <= self.layout.prefix(node, self.item(job["row"]).hashes)
            or route.p_blocks <= self.layout.prefix(node, self.item(job["row"]).hashes)
        ):
            return super().admission_ready(node, free_slots)
        self.prefill_attempted.add(key)
        began, origin = time.perf_counter(), self.now
        costs = self.frozen()
        shadow = self.policy_queues.planning_snapshot()
        known = [r for r in self.prefill_records if r["resolved"] <= origin]
        estimate = (
            sum(r["CPU_wall"] + r["commit_CPU_wall"] for r in known) / len(known)
            if known
            else 0.0
        )
        selected, evaluations = select_prefill(
            shadow, key, route, self.layout, costs, origin, estimate + self.rpc_boundary
        )
        record = {
            "id": key,
            "node": node,
            "source": source,
            "source_owner": held,
            "origin": origin,
            "model_latest": costs.latest_completed_seconds,
            "initial_prefix": route.p_blocks,
            "selected_prefix": selected.p_blocks,
            "selected_source": selected.source,
            "predicted_CPU": estimate,
            "CPU_latest": max((r["resolved"] for r in known), default=None),
            "completed_CPU_samples": len(known),
            "evaluations": evaluations,
            "queue_before": list(self.queues[node]),
            "search": dict(shadow.search_stats),
        }
        cpu = time.perf_counter() - began
        record.update(
            CPU_wall=cpu,
            RPC_seconds=self.rpc_boundary,
            decision_ready=origin + cpu + self.rpc_boundary,
        )
        self.prefill_pending = key, selected, record
        self.router_busy = True
        self.event(record["decision_ready"], "admission", key)
        return False

    def route(self):
        if self.prefill_pending is not None:
            key, selected, record = self.prefill_pending
            if self.now < record["decision_ready"]:
                return
            began = time.perf_counter()
            node = record["node"]
            if (
                node in self.active
                or not self.queues[node]
                or self.queues[node][0] != key
            ):
                raise ValueError("P boundary controller lost FIFO ownership")
            job = self.jobs[key]
            require_waiting_P(self.policy_queues.work[key], job["live_route"])
            job["live_route"] = selected
            job["plan"] = replace(
                job["plan"], source=selected.source, p_prefix=selected.p_blocks * 128
            )
            self.policy_queues.revise(key, selected, self.layout, self.frozen())
            job["work"] = self.policy_queues.work[key]
            record["status"] = "local" if selected.source == node else "keep"
            record["queue_after"] = list(self.queues[node])
            record["commit_CPU_wall"] = time.perf_counter() - began
            self.now += record["commit_CPU_wall"]
            record["resolved"] = self.now
            self.prefill_records.append(record)
            self.prefill_pending = None
            self.router_busy = False
        super().route()

    def run(self):
        result = super().run()
        if self.prefill_pending is not None:
            raise ValueError("P revalidation controller failed to drain")
        result["internal_candidate"]["name"] = "prefill-source-revalidation-01"
        result["prefill_revalidation"] = self.prefill_records
        result["prefill_revalidation_CPU_wall"] = sum(
            r["CPU_wall"] + r["commit_CPU_wall"] for r in self.prefill_records
        )
        result["prefill_revalidation_scope"] = (
            "unacquired P FIFO head, observed busy remote P source; local/zero boundary only"
        )
        return result
