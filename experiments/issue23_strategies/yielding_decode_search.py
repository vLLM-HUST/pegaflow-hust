"""Yield optional advice at each candidate; acquired work and real routes win.

The frozen decision state and candidate ordering are identical to progress01.
This is cooperative cancellation, not free parallel control or future arrivals.
"""

import itertools
import time
from dataclasses import replace

from bounded_search import ForecastDominated
from late_decode_search import require_unacquired
from pegaflow.joint_policy import copy_package
from pegaflow.pending_planner import pending_profile
from pegaflow.resource_cache_profile import optional_work
from pegaflow.resource_state_profile import refresh_work
from progress_decode_search import ProgressDecodeSimulation
from release_cobatch_search import ReleaseCobatchSimulation


def advice_steps(shadow, key, route, layout, costs, now, delay, budget=64):
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
    yield {
        "kind": "keep",
        "evaluations": rows,
        "selected": selected,
        "incumbent_prediction": unchanged,
    }
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
                yield {"kind": "infeasible", "evaluations": rows, "selected": selected}
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
            yield {"kind": "pruned", "evaluations": rows, "selected": selected}
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
        yield {"kind": "candidate", "evaluations": rows, "selected": selected}
    return selected, rows


def cancellation(
    key, before, jobs, active, queues, pending_rows, rowmap, active_sessions
):
    if key not in jobs or key in active:
        return "acquired_or_completed"
    if key not in queues[before]:
        raise ValueError("Advice target lost unacquired FIFO ownership")
    if any(rowmap[k]["task"] not in active_sessions for k in pending_rows):
        return "route_priority"
    return None


class YieldingDecodeSimulation(ProgressDecodeSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.advice_iterator = None

    def make_steps(self, key, record):
        # Generator construction does no work; this body runs inside the first
        # measured quantum. Cancelled partial searches are never full CPU labels.
        job = self.jobs[key]
        # Setup has already occupied the controller. Resource observations and
        # the calendar must use this same post-setup epoch, not admission time.
        forecast_origin = self.now
        record["forecast_origin"] = forecast_origin
        costs = self.frozen()
        shadow = self.policy_queues.planning_snapshot()
        known = [
            r
            for r in self.advice_records
            if r is not record
            and r.get("completed_search")
            and r["decision_ready"] <= forecast_origin
        ]
        estimate = sum(r["CPU_wall"] for r in known) / len(known) if known else 0.0
        record.update(
            predicted_CPU=estimate,
            completed_CPU_samples=len(known),
            CPU_latest=max((r["decision_ready"] for r in known), default=None),
            model_latest=costs.latest_completed_seconds,
        )
        selected, rows = yield from advice_steps(
            shadow,
            key,
            job["live_route"],
            self.layout,
            costs,
            forecast_origin,
            estimate + self.rpc_boundary,
        )
        record["search"] = dict(shadow.search_stats)
        return selected, rows

    def next_quantum(self):
        key, selected, record = self.advice_pending
        began = time.perf_counter()
        origin = self.now
        try:
            step = next(self.advice_iterator)
            selected, rows, kind = step["selected"], step["evaluations"], step["kind"]
            observer = getattr(self, "observe_advice_keep", None)
            if kind == "keep" and observer is not None:
                observer(record, step["incumbent_prediction"])
        except StopIteration as done:
            selected, rows = done.value
            kind = "done"
            record["completed_search"] = True
        record["selected"], record["evaluations"] = selected.d, rows
        rpc = self.rpc_boundary if not record["quanta"] else 0.0
        cpu = time.perf_counter() - began
        quantum = {
            "origin": origin,
            "CPU_wall": cpu,
            "RPC_seconds": rpc,
            "ready": origin + cpu + rpc,
            "kind": kind,
        }
        record["quanta"].append(quantum)
        record["CPU_wall"] += cpu
        record["RPC_seconds"] += rpc
        record["decision_ready"] = quantum["ready"]
        self.advice_pending = key, selected, record
        self.router_busy = True
        self.event(quantum["ready"], "admission", key)

    def resolve_advice(self, forced=None):
        key, selected, record = self.advice_pending
        began = time.perf_counter()
        if forced:
            self.advice_iterator.close()
            # Keep the progressing incumbent. Reuse the audited owned-commit
            # helper with a no-op route, but retain what the search proposed.
            self.advice_pending = key, replace(selected, d=record["before"]), record
        super().finish_advice()
        if forced:
            record["status"] = forced
        elapsed = time.perf_counter() - began
        # Parent already charged its inner commit; charge only the remainder.
        self.now += elapsed - record["commit_CPU_wall"]
        record["commit_CPU_wall"] = elapsed
        record["resolved"] = self.now
        self.advice_iterator = None

    def route(self):
        if self.advice_pending is not None:
            key, _, record = self.advice_pending
            if self.now < record["decision_ready"]:
                return
            reason = cancellation(
                key,
                record["before"],
                self.jobs,
                self.d_active,
                self.queues,
                self.pending,
                self.rowmap,
                self.active_sessions,
            )
            if reason == "acquired_or_completed":
                self.resolve_advice(reason)
            elif record["completed_search"]:
                self.resolve_advice()
            elif reason:
                self.resolve_advice(reason)
            else:
                self.next_quantum()
                return
        # Do not call ProgressDecodeSimulation.route: it launches a monolithic
        # search. Preserve the same inherited initial route implementation.
        ReleaseCobatchSimulation.route(self)
        if (
            self.mode == "native"
            or self.router_busy
            or not getattr(self, "enable_decode_advice", True)
        ):
            return
        active = {n: sum(d == n for d in self.d_active.values()) for n in (2, 3)}
        queued = {n: len(self.queues[n]) for n in (2, 3)}
        key = next(
            (
                k
                for n in (2, 3)
                for index, k in enumerate(self.queues[n])
                if k not in self.advice_attempted
                and active[n] + index >= self.batch_config.max_sequences
                and active[5 - n] < self.batch_config.max_sequences
                and not queued[5 - n]
            ),
            None,
        )
        if key is None:
            return
        self.advice_attempted.add(key)
        trigger = getattr(self, "advice_trigger", None)
        if trigger is not None and not trigger(key):
            return
        began = time.perf_counter()
        job = self.jobs[key]
        record = {
            "id": key,
            "origin": self.now,
            "P_completed": job["work"].p_done,
            "before": job["plan"].d,
            "selected": job["plan"].d,
            "slots": active,
            "queued": queued,
            "quanta": [],
            "completed_search": False,
            "CPU_wall": 0.0,
            "RPC_seconds": 0.0,
            "evaluations": [],
            "search": {},
        }
        self.advice_records.append(record)
        self.advice_iterator = self.make_steps(key, record)
        self.advice_pending = key, job["live_route"], record
        setup = time.perf_counter() - began
        self.now += setup
        record["setup_CPU_wall"] = setup
        record["CPU_wall"] += setup
        self.next_quantum()

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "yielding-decode-advice-02"
        result["advice_cancel_granularity"] = (
            "one forecast candidate, not instruction preemption"
        )
        result["partial_advice_is_full_CPU_label"] = False
        return result
