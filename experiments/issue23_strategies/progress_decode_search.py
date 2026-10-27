"""Cancellable D advice on the existing serialized route-control lane.

Original FIFO admission continues while advice is computed. The no-op forecast
has no added planning delay; alternatives pay estimated completed-only cost.
Actual CPU/RPC blocks the route-control lane, never a sequence/device lease.
"""

import itertools
import time
from dataclasses import replace

from bounded_search import ForecastDominated
from late_decode_search import LateDecodeQueues, require_unacquired
from pegaflow.joint_policy import copy_package
from pegaflow.pending_planner import pending_profile
from pegaflow.resource_cache_profile import optional_work
from pegaflow.resource_state_profile import refresh_work
from release_cobatch_search import ReleaseCobatchSimulation


def select_advice(shadow, key, route, layout, costs, now, delay, budget=64):
    """Compare unchanged immediate admission to delayed alternate-D choices."""
    require_unacquired(shadow.work[key], route)
    shadow.refresh(layout, costs)
    current, item = shadow.work[key], shadow.demands[key]
    shadow.boundary_request = key
    shadow.incumbent_first_profile = None
    shadow.late_release_overrides = {}
    unchanged = shadow.predict_batch(
        tuple(shadow.work.values()), now, shadow.demands, shadow.routes, costs
    )
    incumbent = pending_profile(tuple(shadow.work.values()), unchanged)
    rows = [
        {
            "kind": "keep",
            "d": route.d,
            "prefix_blocks": route.d_blocks,
            "objective": incumbent,
            "first": unchanged["first"][key],
            "full": unchanged["full"][key],
            "delay": 0.0,
            "payload": 0,
        }
    ]
    selected, best = route, incumbent
    selected_tie = (0, route.d_blocks)
    shadow.incumbent_first_profile = incumbent[:2]
    node = 5 - route.d
    local = layout.prefix(node, item.hashes)
    limit = max(local, layout.prefix(route.p, item.hashes))
    lengths = dict.fromkeys((local, limit, 0, *range(limit - 1, 0, -1)))
    shadow.late_release_overrides = {key: now + delay}
    for length in itertools.islice(lengths, max(0, budget - 1)):
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
        works = tuple(changed if w.key == key else w for w in shadow.work.values())
        try:
            prediction = shadow.predict_batch(
                works, now, shadow.demands, {**shadow.routes, key: option}, costs
            )
        except ForecastDominated:
            continue
        objective = pending_profile(works, prediction)
        rows.append(
            {
                "kind": "move",
                "d": node,
                "prefix_blocks": length,
                "objective": objective,
                "first": prediction["first"][key],
                "full": prediction["full"][key],
                "delay": delay,
                "payload": payload,
            }
        )
        # Equality cannot justify disturbing a progressing incumbent. Ties
        # between improving alternate boundaries prefer less physical copying.
        if objective < best or (
            objective == best
            and selected.d != route.d
            and (payload, length) < selected_tie
        ):
            selected, best, selected_tie = option, objective, (payload, length)
            shadow.incumbent_first_profile = objective[:2]
    return selected, rows


def commit_status(key, before, after, jobs, queues, active, capacity):
    if key not in jobs or key in active:
        return "acquired_or_completed"
    if key not in queues[before]:
        raise ValueError("Unacquired advice target lost original FIFO ownership")
    if before == after:
        return "no_predicted_improvement"
    if sum(n == after for n in active.values()) >= capacity or queues[after]:
        return "alternative_no_longer_available"
    return "move"


class ProgressDecodeSimulation(ReleaseCobatchSimulation):
    queue_type = LateDecodeQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.advice_pending, self.advice_records = None, []
        self.advice_attempted = set()

    def finish_advice(self):
        if self.advice_pending is None:
            return
        key, selected, record = self.advice_pending
        if self.now < record["decision_ready"]:
            return
        began = time.perf_counter()
        status = commit_status(
            key,
            record["before"],
            selected.d,
            self.jobs,
            self.queues,
            self.d_active,
            self.batch_config.max_sequences,
        )
        record["commit_queue_before"] = [list(q) for q in self.queues]
        record["commit_active"] = dict(self.d_active)
        if status == "move":
            job, ledger = self.jobs[key], self.policy_queues
            require_unacquired(ledger.work[key], selected)
            # Change only the owned unacquired D suffix. Other FIFO order and P
            # work remain immutable; actual admission rechecks cache/boundary.
            self.queues[record["before"]].remove(key)
            self.queues[selected.d].append(key)
            job["plan"] = replace(
                job["plan"], d=selected.d, d_prefix=selected.d_blocks * 128
            )
            job["live_route"] = selected
            job["work"] = replace(job["work"], d=selected.d, d_enqueued=self.now)
            ledger.jobs[key].d = selected.d
            ledger.routes[key] = selected
            ledger.work[key] = replace(
                ledger.work[key], d=selected.d, d_enqueued=self.now
            )
            ledger.admission_choices[key, selected.d] = False
            ledger.observe(key, selected.d, "enqueued", self.now)
            ledger.progress_version += 1
        record["status"] = status
        record["commit_queue_after"] = [list(q) for q in self.queues]
        record["commit_CPU_wall"] = time.perf_counter() - began
        self.now += record["commit_CPU_wall"]
        record["resolved"] = self.now
        self.advice_pending = None
        self.router_busy = False

    def route(self):
        self.finish_advice()
        # Newly eligible requests retain priority on the same route CPU lane.
        super().route()
        if self.mode == "native" or self.router_busy:
            return
        active = {
            node: sum(n == node for n in self.d_active.values()) for node in (2, 3)
        }
        queued = {node: len(self.queues[node]) for node in (2, 3)}
        key = next(
            (
                k
                for node in (2, 3)
                for index, k in enumerate(self.queues[node])
                if k not in self.advice_attempted
                and active[node] + index >= self.batch_config.max_sequences
                and active[5 - node] < self.batch_config.max_sequences
                and queued[5 - node] == 0
            ),
            None,
        )
        if key is None:
            return
        self.advice_attempted.add(key)
        began = time.perf_counter()
        origin, job = self.now, self.jobs[key]
        costs = self.frozen()
        shadow = self.policy_queues.planning_snapshot()
        known = [r for r in self.advice_records if r["decision_ready"] <= origin]
        estimate = sum(r["CPU_wall"] for r in known) / len(known) if known else 0.0
        selected, evaluations = select_advice(
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
            "P_completed": job["work"].p_done,
            "before": job["plan"].d,
            "selected": selected.d,
            "slots": active,
            "queued": queued,
            "predicted_CPU": estimate,
            "completed_CPU_samples": len(known),
            "model_latest": costs.latest_completed_seconds,
            "CPU_latest": max((r["decision_ready"] for r in known), default=None),
            "evaluations": evaluations,
            "search": dict(shadow.search_stats),
        }
        cpu = time.perf_counter() - began
        record.update(
            CPU_wall=cpu,
            RPC_seconds=self.rpc_boundary,
            decision_ready=origin + cpu + self.rpc_boundary,
        )
        self.advice_records.append(record)
        self.advice_pending = key, selected, record
        self.router_busy = True
        self.event(record["decision_ready"], "admission", key)

    def run(self):
        result = super().run()
        if self.advice_pending is not None:
            raise ValueError("Advice controller did not drain")
        result["internal_candidate"]["name"] = "progress-preserving-decode-01"
        result["decode_advice"] = self.advice_records
        result["decode_advice_CPU_wall"] = sum(
            r["CPU_wall"] + r["commit_CPU_wall"] for r in self.advice_records
        )
        result["advice_CPU_lane"] = (
            "serialized with initial routing; FIFO/device progress continues"
        )
        result["advice_real_parallel_control_validated"] = False
        return result
