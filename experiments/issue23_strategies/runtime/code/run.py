"""Four-instance full-policy A/B with explicit copies and causal open arrivals."""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, is_dataclass, replace

from common import (AGENT_PORT, D_IP, META_GRPC, META_HTTP, MODEL, P_IP, ROOT,
                    assert_port_free, cache_url, dump, file_sha, rpc, stop, wait, worker_url)
from legacy_node import Workers
from pegaflow.joint_policy import Calendar, Costs, Demand, Layout, Remove, best_route, decode_boundary, plan_layout
from pegaflow.joint_queue import QueueLedger
from pegaflow.joint_runtime import Backend, CommitConflict, json_rpc
from pegaflow.placement_cost import ObservedLatencyModel
from pegaflow.placement_executor import UnstableInventory
from pipeline import DevicePipeline
from arrivals import causal_ready, load_condition
from pegaflow.operation_cost import OperationCosts
from pegaflow.resource_queue import ResourceQueues
from pegaflow.resource_runtime import ResourceBackend
from pegaflow.resource_placement import plan_live
from pegaflow.resource_cache_profile import CacheProfileQueues, plan_cache_profile
from pegaflow.resource_headroom_profile import HeadroomOperationCosts
from pegaflow.detached_calendar import DetachedCalendarQueues as DetachedQueues
from pegaflow.pending_planner import OpportunityTrigger, plan_opportunity
from pegaflow.resource_profile import route_order
from cache_live_bridge import selected_guard, shifted_profile


def encode(value):
    if is_dataclass(value):
        return encode(asdict(value))
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (set, frozenset, tuple)):
        return [encode(v) for v in value]
    if isinstance(value, list):
        return [encode(v) for v in value]
    if isinstance(value, dict):
        return {str(k): encode(v) for k, v in value.items()}
    return value


def save_candidate_protocol(output, protocol):
    reference = output / "reference-protocol-before-candidate.json"
    if reference.exists():
        raise ValueError("Candidate protocol was already finalized")
    (output / "protocol.json").rename(reference)
    dump(output / "protocol.json", protocol)


def generate(node, row, *, prefix=0, cold=False, first_observer=None, native_cache=False):
    from pegaflow.strategies.live_adapter import request_payload

    role, slot = ("P", node) if node < 2 else ("D", node - 2)
    payload = json.dumps(request_payload(row, prefix, cold, native_cache)).encode()
    request = urllib.request.Request(worker_url(role, slot) + "/generate", data=payload,
                                     headers={"Content-Type": "application/json"})
    started, first, result, token = time.monotonic(), None, None, None
    with urllib.request.urlopen(request, timeout=600) as response:
        for raw in response:
            event = json.loads(raw)
            if event["event"] == "first_token":
                if first is not None:
                    raise ValueError("Duplicate first-token event")
                first, token = time.monotonic() - started, event["token_id"]
                if first_observer is not None:
                    first_observer()
            elif event["event"] == "complete":
                result = event["result"]
            else:
                raise RuntimeError(str(event))
    if result is None or first is None or token != result["output_token_ids"][0] or result["id"] != row["id"]:
        raise ValueError("Streaming/output identity gate failed")
    if result["rdma_bytes"] != 0 and not cold and not native_cache:
        raise ValueError("Worker performed an unpriced implicit remote fetch")
    if result.get("policy_native_cache") is not native_cache:
        raise ValueError("Worker executed a different cache strategy mode")
    return result, {"first_seconds": first, "complete_seconds": time.monotonic() - started}


def backend_for(ready):
    return ResourceBackend([cache_url("P" if n < 2 else "D", n % 2) for n in range(4)],
                   [f"{P_IP if n < 2 else D_IP}:{50300 + n % 2}" for n in range(4)],
                   [r["instance_id"] for r in ready])


def wait_for_d_after_preload(processes, directory, timeout=3600):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(child.poll() is not None for child in processes):
            raise RuntimeError("Preloaded P worker exited while waiting for D")
        try:
            state = rpc(f"http://{D_IP}:{AGENT_PORT}/status", timeout=3)
        except OSError:
            time.sleep(2)
            continue
        if (state.get("ready") is not True or state.get("cursor") != 0
                or state.get("phase") is not None or state.get("children") != []):
            raise ValueError("D controller is not a fresh unused owned phase controller")
        dump(directory / "D-controller-available.json", state)
        return
    raise TimeoutError("D controller unavailable within bounded P preload wait")


def services(phase, output, preload_p=False):
    from pegaflow._bin_utils import binary_env, find_binary

    directory = output / phase
    directory.mkdir(exist_ok=False)
    assert_port_free(P_IP, (META_GRPC, META_HTTP))
    log = (directory / "meta.log").open("x", encoding="utf-8")
    meta = subprocess.Popen([str(find_binary("pegaflow-metaserver-py")), "--addr", f"{P_IP}:{META_GRPC}",
                             "--http-addr", f"{P_IP}:{META_HTTP}"], env=binary_env(), stdout=log, stderr=subprocess.STDOUT)
    workers = Workers("P", P_IP, output / "workers")
    d_start_requested = False
    try:
        wait(f"http://{P_IP}:{META_HTTP}/health", (meta,), timeout=180)
        if preload_p:
            if phase != "calibration":
                raise ValueError("Early P loading is only permitted before calibration")
            workers.start(phase)
            p_ready = [wait(worker_url("P", slot) + "/ready", workers.children, timeout=900)
                       for slot in (0, 1)]
            if any(value.get("phase") != phase or value.get("role") != "P"
                   or value.get("slot") != slot or not value.get("instance_id")
                   for slot, value in enumerate(p_ready)):
                raise ValueError("Preloaded P identity/phase differs")
            dump(directory / "P-preload-ready.json", {"ready": p_ready,
                 "ready_unix": time.time(), "formal_sampling_started": False,
                 "reuse_for_same_calibration_phase": True, "D_wait_timeout_seconds": 3600})
            print(json.dumps({"phase": "P_preloaded_waiting_for_D", "P_instances": 2,
                              "tp": 1, "formal_sampling_started": False}), flush=True)
            wait_for_d_after_preload(workers.children, directory)
        d_start_requested = True
        rpc(f"http://{D_IP}:{AGENT_PORT}/start", {"phase": phase})
        if not preload_p:
            workers.start(phase)
        ready = [wait(worker_url("P" if n < 2 else "D", n % 2) + "/ready", workers.children, timeout=1800) for n in range(4)]
        if any(r.get("phase") != phase or not r.get("instance_id") for r in ready):
            raise ValueError("Actual four-instance readiness failed")
        dump(directory / "readiness.json", ready)
        return directory, backend_for(ready), workers, meta, log
    except BaseException:
        workers.close()
        if d_start_requested:
            with contextlib.suppress(OSError):
                rpc(f"http://{D_IP}:{AGENT_PORT}/abort", {}, timeout=90)
        stop([meta])
        log.close()
        raise


def close_services(phase, workers, meta, log):
    workers.close()
    rpc(f"http://{D_IP}:{AGENT_PORT}/stop", {"phase": phase}, timeout=120)
    stop([meta])
    log.close()


def demand(row):
    preview = json_rpc(worker_url("P", 0) + "/preview", {"row": row})
    return Demand(row["id"], row["task"], row["prompt_tokens"], tuple(bytes.fromhex(v) for v in preview["hashes"]))


def handoff(backend, source, destination, hashes, limit):
    try:
        event = backend.fetch_prefix(source, destination, hashes[:limit])
        return limit, event
    except (CommitConflict, UnstableInventory) as error:
        # No free capacity is a legal recompute fallback, not a correctness
        # bypass. Unknown HTTP outcomes/timeouts propagate and stop this arm.
        view = backend.views()
        local = Layout.current(view).prefix(destination, hashes)
        return min(local, limit), {"status": "capacity_or_snapshot_fallback", "reason": str(error)}


def warmups(backend, rows, directory):
    results = []
    for lane, row in enumerate(rows):
        item = demand(row)
        cold, cold_wire = generate(lane + 2, row, cold=True)
        p, _ = generate(lane, row, prefix=0)
        limit, copy = handoff(backend, lane, lane + 2, item.hashes, len(item.hashes))
        d, _ = generate(lane + 2, row, prefix=limit)
        if d["output_token_ids"] != cold["output_token_ids"] or p["output_token_ids"][0] != d["output_token_ids"][0]:
            raise ValueError("Integrated actual-copy warmup output gate failed")
        if copy.get("status") != "copied" or d["num_cached_tokens"] < 128:
            raise ValueError("Warmup did not exercise explicit complete-prefix RDMA restore")
        results.append({"lane": lane, "cold": cold, "cold_wire": cold_wire, "P": p, "D": d, "copy": copy, "passed": True})
    dump(directory / "integrated-warmup.json", encode(results))
    return results


def causal_order(rows, tasks):
    grouped = {task: deque(r for r in rows if r["task"] == task) for task in sorted(tasks)}
    for values in grouped.values():
        steps = [r["step_id"] for r in values]
        if not steps or steps != sorted(set(steps)):
            raise ValueError("Whole-session causal ledger failed")
    ordered = []
    while any(grouped.values()):
        for values in grouped.values():
            if values:
                ordered.append(values.popleft())
    return ordered


def calibrate(backend, rows, directory):
    records = []
    # Isolated calibration tasks only; process each original input once.
    for index, row in enumerate(rows):
        lane = index % 2
        item = demand(row)
        p, pw = generate(lane, row, prefix=len(item.hashes))
        boundary, copied = handoff(backend, lane, lane + 2, item.hashes, len(item.hashes))
        d, dw = generate(lane + 2, row, prefix=boundary)
        if p["output_token_ids"][0] != d["output_token_ids"][0]:
            raise ValueError("Calibration P/D first-token gate failed")
        records.append({"row_id": row["id"], "lane": lane, "P": p, "D": d, "P_wire": pw, "D_wire": dw, "handoff": copied})
        dump(directory / f"calibration-{index:03d}.json", encode(records[-1]))
    # Real reverse and same-host copies qualify the paths used by planning.
    transfer_events = []
    past = [demand(row) for row in rows]
    for source, destination in ((2, 1), (1, 3), (0, 1), (2, 3)):
        views = backend.views()
        state = Layout.current(views)
        candidates = [(state.prefix(source, item.hashes), item) for item in past]
        length, item = max(candidates, key=lambda value: value[0])
        if not length:
            raise ValueError("Calibration copy source unavailable")
        length = max(1, min(length, state.prefix(destination, item.hashes) + 1))
        chain = item.hashes[:length]
        # Select one closed optional allocation, not cleanup-all. This prices a
        # real eviction RPC and makes room for a small qualified copy direction.
        duplicate = state.prefix(destination, chain) == length
        victim = next((unit for unit in views[destination].units if not unit.protected
                       and ((chain[-1] in unit.keys) if duplicate else not (unit.keys & set(chain)))), None)
        if victim is None:
            raise ValueError("Cannot qualify a copy while calibration allocations are leased")
        backend.remove(Remove(destination, (victim.identity,)), backend.inventory(destination), None)
        dump(directory / f"copy-direction-{source}-{destination}-intent.json", encode({
            "source": source, "destination": destination, "prefix_blocks": length,
            "removed": backend.events[-1], "source_inventory": backend.inventory(source),
            "destination_inventory_after_removal": backend.inventory(destination)}))
        event = backend.fetch_prefix(source, destination, chain)
        if event["status"] != "copied":
            raise ValueError("Declared copy direction was not actually exercised")
        original = next(record for record in records if record["row_id"] == item.key)
        row = next(row for row in rows if row["id"] == item.key)
        restored, wire = generate(destination, row, prefix=length)
        expected = original["D"]["output_token_ids"] if destination >= 2 else original["D"]["output_token_ids"][:1]
        if restored["output_token_ids"] != expected:
            raise ValueError("Explicit reverse/same-host copy did not preserve the consumer output")
        transfer_events.append({"source": source, "destination": destination, "event": event,
                                "restored": restored, "wire": wire, "output_equal": True})
    return records, transfer_events


def fit(records, events, bytes_per_token, warmup_records):
    models, samples, decodes = [], {}, []
    for node in range(4):
        role, lane = ("P", node) if node < 2 else ("D", node - 2)
        samples[str(node)] = [(r[role]["uncached_prompt_tokens"],
                              r[role]["num_cached_tokens"] * bytes_per_token,
                              r["P_wire"]["complete_seconds"] if role == "P" else r["D_wire"]["first_seconds"])
                             for r in records if r["lane"] == lane]
        if role == "D":
            cold = warmup_records[lane]
            samples[str(node)].append((cold["cold"]["prompt_tokens"], 0, cold["cold_wire"]["first_seconds"]))
        model = ObservedLatencyModel.fit(tuple(samples[str(node)]))
        if not model.rdma_observed:
            raise ValueError("Resident restore cost is unobserved; cannot price a free cache path")
        models.append(model)
        if role == "D":
            values = [r["D_wire"]["complete_seconds"] - r["D_wire"]["first_seconds"] for r in records if r["lane"] == lane]
            decodes.append(sum(values) / len(values))
    direction = {name: [] for name in ("pd", "dp", "same")}
    for event in events:
        if event.get("kind") == "copy":
            name = event["direction"] if event["direction"] in ("pd", "dp") else "same"
            direction[name].append((event["receipt"]["payload_bytes"], event["rpc_seconds"]))
    if not all(direction.values()):
        raise ValueError("Every enabled copy direction requires a real completed measurement")
    # A zero intercept through-origin slope is identifiable from one copy;
    # it is a measured surrogate, not independent wire/host stage estimates.
    slopes = {k: sum(b * s for b, s in v) / sum(b * b for b, _ in v) for k, v in direction.items()}
    removals = [event["rpc_seconds"] for event in events if event.get("kind") == "remove"]
    if not removals:
        raise ValueError("Selected-removal control cost has not been measured")
    costs = Costs(tuple(models), tuple(decodes), 0, slopes["same"], slopes["pd"], slopes["dp"],
                  sum(removals) / len(removals), bytes_per_token)
    return costs, {"costs": asdict(costs), "samples": samples, "transfer_samples": direction,
                   "resident_feature_is_not_wire_bytes": True, "heldout_data_used": False,
                   "eviction_rpc_cost_unmeasured": False, "eviction_samples": removals}


class Policy:
    def __init__(self, backend, costs, event_queue_policy=False):
        self.backend, self.costs = backend, costs
        self.event_queue_policy = event_queue_policy
        self.history, self.records, self.pending, self.guard = [], [], None, threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=1)

    def completed(self, item, calendar):
        with self.guard:
            self.history.append(item)
            if self.pending is not None and not self.pending.done():
                return
            if self.pending is not None:
                self.pending.result()
            history = tuple(self.history)
            self.pending = self.pool.submit(self.optimize, history, calendar)

    def optimize(self, history, calendar):
        started = time.monotonic()
        cpu_started = time.thread_time()
        record = {"completed_ids": [d.key for d in history], "action_count": 0}
        record["completed_arrivals_seconds"] = {d.key: d.arrival for d in history}
        record["started_seconds"] = started - self.started
        try:
            views = self.backend.views()
            history_for_objective = history
            if self.event_queue_policy:
                origin = min(item.arrival for item in history)
                history_for_objective = tuple(replace(item, arrival=item.arrival - origin) for item in history)
                calendar = None
                record["objective_scope"] = "past-demand working-set replay; not predicted future arrivals"
            remaining = 0.1 - (time.monotonic() - started)
            if remaining <= 0:
                record.update(status="no_action", reason="planning_budget_spent_before_objective")
                return
            plan = plan_layout(Layout.current(views), history_for_objective, self.costs, max_trials=256,
                               deadline_seconds=remaining, calendar=calendar,
                               event_queues=self.event_queue_policy)
            result = self.backend.apply(plan, views, history)
            record.update(plan=asdict(plan), execution=result,
                          action_count=len(result.get("receipts", [])))
        except (CommitConflict, UnstableInventory) as error:
            record.update(status="normal_defer", reason=str(error))
        finally:
            record["total_seconds"] = time.monotonic() - started
            record["cpu_seconds"] = time.thread_time() - cpu_started
            self.records.append(record)

    def close(self):
        self.pool.shutdown(wait=True)
        if self.pending is not None:
            self.pending.result()


class ResourcePolicy(Policy):
    def __init__(self, backend, costs, queues, mode="opportunity"):
        super().__init__(backend, costs, event_queue_policy=True)
        self.queues = queues
        if mode not in ("off", "opportunity"):
            raise ValueError("Declare slow policy off or opportunity")
        self.mode, self.trigger = mode, OpportunityTrigger()

    def completed(self, item, calendar):
        if self.mode == "off":
            self.history.append(item)
            return
        super().completed(item, calendar)

    def optimize(self, history, calendar):
        started, cpu_started = time.monotonic(), time.thread_time()
        record = dict(completed_ids=[d.key for d in history], action_count=0,
            completed_arrivals_seconds={d.key: d.arrival for d in history},
            started_seconds=started-self.started, objective_scope="admitted-live-resource-DAG",
            planning_inventory_is_cached_not_commit_authority=True)
        try:
            views = self.backend.cached_views()
            if views is None:
                record.update(status="normal_defer", reason="no_real_snapshot")
                return
            costs = freeze_completed(self.queues.models, self.started)
            record["model_frozen_seconds"] = time.monotonic()-self.started
            shadow = self.queues.planning_snapshot()
            plan, version, live = plan_opportunity(Layout.current(views), history, shadow, costs,
                                                  trigger=self.trigger)
            record.update(plan=asdict(plan), live_ids=live, model_version=costs.version,
                          latest_model_completion=costs.latest_completed_seconds,
                          progress_version=version)
            if not plan.transition.operations:
                record.update(status="no_action", reason=plan.stop_reason)
                return
            # Queue progress, like cache generation, is a snapshot contract.
            # Never force a stale plan through in order to manufacture actions.
            nodes = {n for op in plan.transition.operations
                     for n in ((op.source, op.destination) if hasattr(op, "source") else (op.node,))}
            # Device acquisition is nonblocking and all-or-none. The state
            # mutex protects only a short validation, NEVER a native RPC.
            with self.backend.locked(nodes, blocking=False):
                with self.queues.guard:
                    if version != self.queues.progress_version:
                        record.update(status="normal_defer", reason="owned_queue_progress_changed")
                        return
                    extra = max(0.0, time.monotonic() - started - plan.elapsed_seconds)
                    priced = shifted_profile(plan.predicted_p99, tuple(self.queues.work.values()), extra)
                    record["complete_control_priced_profile"] = priced
                    if priced[0] >= plan.initial_p99[0]:
                        record.update(status="no_action", reason="complete_planning_cost_erases_benefit")
                        return
                result = self.backend.apply(plan, views, history)
                record.update(execution=result, action_count=len(result.get("receipts", [])))
        except (CommitConflict, UnstableInventory) as error:
            record.update(status="normal_defer", reason=str(error))
        finally:
            record["total_seconds"], record["cpu_seconds"] = time.monotonic()-started, time.thread_time()-cpu_started
            if record.get("plan", {}).get("trials", 0) > 1:
                self.trigger.observe(self.trigger.features(Layout.current(views), shadow.demands.values()),
                                     record["cpu_seconds"], time.monotonic()-self.started)
            self.records.append(record)


def seed_operations(costs, seed_directory, operation_variant="parent"):
    from pegaflow.operation_phase_release import PhaseReleaseCosts
    if operation_variant not in ("parent", "phase"):
        raise ValueError("Unknown frozen operation model variant")
    models = (PhaseReleaseCosts if operation_variant == "phase" else HeadroomOperationCosts)(costs)
    files = sorted(seed_directory.glob("calibration-*.json"))
    if len(files) != 32:
        raise ValueError("Exactly the original 32 completed calibration requests must seed the model")
    for path in files:
        record = json.loads(path.read_text(encoding="utf-8"))
        for role in ("P", "D"):
            node = record["lane"] + (0 if role == "P" else 2)
            models.observe(node, record[role], record[role+"_wire"], 0.0, calibration=True)
    return models


def freeze_completed(models, started):
    # Sample the origin under the model lock, rather than rejecting a valid
    # concurrent completion which occurs between reading the clock and lock.
    with models.guard:
        return models.freeze(time.monotonic()-started)


def formal(phase, backend, rows, costs, interval, directory, baseline=None, pipeline=False,
           arrival_offsets=None, causal_arrivals=False, event_queue_policy=False,
           baseline_routing="task-affinity", slow_policy="off", operation_variant="parent",
           naive_native=False):
    ready_rows = [(row, None) for row in rows]
    policy = None
    started, pending, active_tasks = time.monotonic(), deque(), set()
    if policy:
        policy.started = started
    results, index, lock = [], 0, threading.Lock()
    schedule = tuple(arrival_offsets) if arrival_offsets is not None else tuple(i * interval for i in range(len(rows)))
    if len(schedule) != len(rows) or (causal_arrivals and not pipeline):
        raise ValueError("A matching arrival ledger and independent pipeline are required")
    session_completed = {}
    # The legacy two-slot arm remains available only for archived comparison.
    # Pipeline mode admits every causally eligible arrived session head.
    calendar = Calendar()
    if event_queue_policy and not pipeline:
        raise ValueError("Event-anchored routing requires the independent device pipeline")
    models = seed_operations(costs, directory.parent / "operation-seed", operation_variant) if phase == "treatment" else None
    from pegaflow.phase_release_calendar import PhaseReleaseQueues
    queue_type = PhaseReleaseQueues if operation_variant == "phase" else DetachedQueues
    queues = queue_type(models, 512, memoize=True, index_projection=True, predict_control=False) if models else QueueLedger()
    backend.started = started
    if models:
        queues.started = started
        policy = ResourcePolicy(backend, costs, queues, mode=slow_policy)
        policy.started, policy.interval = started, interval
    if naive_native and (phase != "baseline" or models is not None or slow_policy != "off"):
        raise ValueError("The naive native baseline cannot execute our planner")
    routing_sequence = 0
    dump(directory / "shared-strategy-contract.json", {
        "policy": "vllm-pegaflow-native" if naive_native else "tailguard-pending-release-v1",
        "naive_native": naive_native, "same_host_authority_fixture_both_arms": True,
        "native_implicit_RDMA_allowed": naive_native, "source_audit_required_from_native_logs": True,
        "slow_policy": slow_policy, "all_control_copy_H2D_save_release_costs_in_wall": True})

    def route_request(row, offered=None, causal_origin=None):
        nonlocal routing_sequence
        decision_started = time.monotonic()
        decision_cpu = time.thread_time()
        if naive_native:
            from pegaflow.joint_policy import Route
            from pegaflow.strategies.live_adapter import native_pair

            now = time.monotonic() - started
            p, d = native_pair(row["id"], row["task"], row["prompt_tokens"],
                               now if causal_origin is None else causal_origin, now, routing_sequence)
            routing_sequence += 1
            item = Demand(row["id"], row["task"], row["prompt_tokens"], ())
            # Zero estimates are explicitly unused; native routing calls no predictor.
            route = Route(p, d, p, 0, 0, now, now, now, 0, Calendar())
            queues.reserve(item.key, route, now)
            return item, route, 0, True, decision_started, time.monotonic()-decision_started, time.thread_time()-decision_cpu
        item = demand(row)
        views = backend.views()
        layout = Layout.current(views)
        with lock:
            now = time.monotonic() - started
            request = replace(item, arrival=now)
            fixed = (sorted({r["task"] for r in rows}).index(row["task"]) % 2,
                     2 + sorted({r["task"] for r in rows}).index(row["task"]) % 2)
            local_costs = freeze_completed(models, started) if models else costs
            now = time.monotonic() - started
            request = replace(item, arrival=now)
            options = {"max_candidates": 64, "fixed_pair": fixed if phase == "baseline" else None}
            if queues is not None:
                options["queues"] = queues
            if phase == "baseline" and baseline_routing != "task-affinity":
                if queues is None:
                    raise ValueError("Additional same-stack references require event queues")
                from pegaflow.joint_baselines import reference_route

                route, tried, exhaustive = reference_route(
                    baseline_routing, request, layout, costs, queues,
                    tuple(sorted({r["task"] for r in rows})))
            else:
                if models:
                    from pegaflow.strategies.live_adapter import controlled_route

                    route, tried, exhaustive = controlled_route(request, layout, local_costs, queues,
                        calendar, now if causal_origin is None else causal_origin, now)
                else:
                    route, tried, exhaustive = best_route(request, layout, local_costs, calendar, **options)
            if queues is not None:
                if not models:
                    queues.reserve(item.key, route, now)
            else:
                calendar.available = dict(route.calendar.available)
        route_seconds = time.monotonic() - decision_started
        route_cpu_seconds = time.thread_time() - decision_cpu
        return item, route, tried, exhaustive, decision_started, route_seconds, route_cpu_seconds

    def prefill(row, decision):
        item, route = decision[:2]
        before_restore = time.monotonic()
        if naive_native:
            with backend.locked((route.p,)):
                p_submitted = time.monotonic()
                p, p_wire = generate(route.p, row, native_cache=True)
                complete = time.monotonic()
            return {"P": p, "P_wire": p_wire, "history_fetch": {"status": "native_lookup"},
                    "P_headroom": None, "P_model_complete_seconds": complete-started,
                    "P_post_model_ledger_seconds": 0., "pre_P_wait_and_copy_seconds": 0.,
                    "P_gate_wait_seconds": p_submitted-before_restore,
                    "P_submission_seconds": p_submitted-started, "P_complete_seconds": complete-started}
        guard_receipt = selected_guard(backend, queues, item, route.p, started) if models else None
        context = backend.copy_context(queues, item.key, "history") if models else contextlib.nullcontext()
        with context:
            restore, copied = handoff(backend, route.source, route.p, item.hashes, route.p_blocks)
        if models:
            queues.skip_copy(item.key, "history")
            queues.revise(item.key, replace(route, p_blocks=restore), Layout.current(backend.cached_views()),
                          freeze_completed(models, started))
        before_p = time.monotonic()
        with backend.locked((route.p,)):
            p_submitted = time.monotonic()
            if models:
                queues.operation(item.key, "P_api", "start", p_submitted-started)
            try:
                p, p_wire = generate(route.p, row, prefix=restore)
                p_model_complete = time.monotonic()
            finally:
                if models:
                    queues.operation(item.key, "P_api", "finish", time.monotonic()-started)
        after_p = time.monotonic()
        if models:
            models.observe(route.p, p, p_wire, after_p-started)
        return {"P": p, "P_wire": p_wire, "history_fetch": copied,
                "P_headroom": guard_receipt, "P_model_complete_seconds": p_model_complete - started,
                "P_post_model_ledger_seconds": after_p - p_model_complete,
                "pre_P_wait_and_copy_seconds": before_p - before_restore,
                "P_gate_wait_seconds": p_submitted - before_p,
                "P_submission_seconds": p_submitted - started,
                "P_complete_seconds": after_p - started}

    def decode(row, decision, arrival, p_result, offered=None):
        item, route, tried, exhaustive, decision_started, route_seconds, route_cpu_seconds = decision
        initial_boundary = route.d_blocks
        boundary_started, boundary_CPU_started = time.monotonic(), time.thread_time()
        guard_receipt = None
        revalidation_reason = "published_inventory"
        if naive_native:
            revalidation_reason = "native_lookup_no_policy_boundary"
        if queues is not None and not naive_native:
            # P has completed, but native admission may reject or evict blocks.
            # Do not assume the P-produced prefix is still fully published.
            local_costs = freeze_completed(models, started) if models else costs
            current = Layout.current(backend.cached_views())
            try:
                current = Layout.current(backend.views())
                if models:
                    length, seconds = queues.decode_boundary(item.key, route, current, local_costs, time.monotonic()-started)
                else:
                    length, seconds = decode_boundary(item, current, local_costs, route.p, route.d)
            except (CommitConflict, UnstableInventory):
                length, seconds = 0, local_costs.compute(route.d, item.tokens, 0)
                revalidation_reason = "unstable_inventory_safe_recompute"
            if models:
                guard_receipt = selected_guard(backend, queues, item, route.d, started)
                if guard_receipt and guard_receipt.get("status") == "applied":
                    current = Layout.current(backend.views())
                    local_costs = freeze_completed(models, started)
                    length, seconds = queues.decode_boundary(item.key, route, current, local_costs, time.monotonic()-started)
            route = replace(route, d_blocks=length, **({"d_headroom": queues.admission_choices.get((item.key, route.d), False)} if models else {}))
            tail = local_costs.decode_tail(route.d, item.tokens) if models else local_costs.decode_seconds[route.d - 2]
            queues.revise_decode(item.key, seconds, seconds + tail)
            if models:
                queues.revise(item.key, route, current, local_costs)
        boundary_ready, boundary_CPU = time.monotonic(), time.thread_time() - boundary_CPU_started
        before_handoff = time.monotonic()
        context = backend.copy_context(queues, item.key, "handoff") if models else contextlib.nullcontext()
        if naive_native:
            boundary, handed = 0, {"status": "native_lookup"}
        else:
            with context:
                boundary, handed = handoff(backend, route.p, route.d, item.hashes, route.d_blocks)
        if models:
            queues.skip_copy(item.key, "handoff")
            queues.revise(item.key, replace(route, d_blocks=boundary), Layout.current(backend.cached_views()),
                          freeze_completed(models, started))
        before_d = time.monotonic()
        with backend.locked((route.d,)):
            d_submitted = time.monotonic()
            options = {"prefix": boundary, "native_cache": naive_native}
            if queues is not None:
                options["first_observer"] = lambda: queues.observe(item.key, route.d, "first", time.monotonic() - started)
            if models:
                queues.operation(item.key, "D_api", "start", d_submitted-started)
            try:
                d, d_wire = generate(route.d, row, **options)
                d_model_complete = time.monotonic()
            finally:
                if models:
                    queues.operation(item.key, "D_api", "finish", time.monotonic()-started)
        end = time.monotonic()
        if models:
            models.observe(route.d, d, d_wire, end-started)
        if p_result["P"]["output_token_ids"][0] != d["output_token_ids"][0]:
            raise ValueError("Formal P/D first-token gate failed")
        if baseline is not None and baseline[row["id"]]["D"]["output_token_ids"] != d["output_token_ids"]:
            raise ValueError("Treatment full output differs from same-input baseline")
        result = {"id": row["id"], "task": row["task"], "step_id": row["step_id"],
                  "arrival_seconds": arrival, "dispatch_seconds": decision_started - started,
                  "route": asdict(route), "initial_route": asdict(decision[1]), "route_trials": tried, "route_exhaustive": exhaustive,
                  "initial_shadow_forecast": queues.selected_quotes.get(item.key) if models else None,
                  "D_headroom": guard_receipt, "D_boundary_start_seconds": boundary_started - started,
                  "D_boundary_ready_seconds": boundary_ready - started, "D_boundary_CPU_seconds": boundary_CPU,
                  "D_model_complete_seconds": d_model_complete - started,
                  "D_post_model_ledger_seconds": end - d_model_complete,
                  "route_seconds": route_seconds, "handoff": handed,
                  "route_cpu_seconds": route_cpu_seconds,
                  **p_result, "D": d, "D_wire": d_wire,
                  "handoff_and_wait_seconds": before_d - before_handoff,
                  "D_gate_wait_seconds": d_submitted - before_d,
                  "D_submission_seconds": d_submitted - started,
                  "ttft_seconds": d_submitted - started - arrival + d_wire["first_seconds"],
                  "completion_seconds": end - started - arrival,
                  "offered_arrival_seconds": arrival if offered is None else offered,
                  "causality_wait_seconds": 0 if offered is None else arrival - offered,
                  "offered_to_first_seconds": d_submitted - started - (arrival if offered is None else offered) + d_wire["first_seconds"],
                  "_completed_demand": item,
                  "reusable_tokens_no_eviction": row["reusable_tokens_no_eviction"]}
        result["policy_native_cache"] = naive_native
        result["route_forecast_used"] = not naive_native
        result["actual_implicit_rdma_bytes"] = p_result["P"]["rdma_bytes"] + d["rdma_bytes"]
        if queues is not None:
            result["boundary_revalidation"] = {"before_P": initial_boundary,
                                                "after_actual_P": route.d_blocks,
                                                "future_admission_assumed": False,
                                                "reason": revalidation_reason}
        return result

    def one(row, item, arrival):
        decision = route_request(row)
        return decode(row, decision, arrival, prefill(row, decision))

    futures = {}
    devices = (DevicePipeline(started, observer=queues.observe) if queues is not None
               else DevicePipeline(started)) if pipeline else None
    try:
        context = contextlib.nullcontext(None) if devices else ThreadPoolExecutor(max_workers=2)
        with context as pool, (directory / "requests.jsonl").open("x", encoding="utf-8") as stream:
            while index < len(ready_rows) or pending or futures:
                elapsed = time.monotonic() - started
                while index < len(ready_rows) and schedule[index] <= elapsed:
                    pending.append((*ready_rows[index], schedule[index]))
                    index += 1
                for future in list(futures):
                    if future.done():
                        task = futures.pop(future)
                        active_tasks.remove(task)
                        if devices:
                            result, timing = future.result()
                            result["pipeline_timing"] = timing
                        else:
                            result = future.result()
                        item = result.pop("_completed_demand")
                        session_completed[result["task"]] = result["arrival_seconds"] + result["completion_seconds"]
                        if policy:
                            elapsed_now = time.monotonic() - started
                            with lock:
                                historical_arrival = result["offered_arrival_seconds"] if causal_arrivals else result["arrival_seconds"]
                                policy.completed(replace(item, arrival=historical_arrival),
                                    Calendar({k: max(0, v - elapsed_now) for k, v in calendar.available.items()}))
                        results.append(result)
                        stream.write(json.dumps(encode(result)) + "\n")
                        stream.flush()
                        print(json.dumps({"phase": phase, "finished": len(results), "id": result["id"],
                              "P": result["route"]["p"], "D": result["route"]["d"], "ttft": result["ttft_seconds"]}), flush=True)
                while devices is not None or len(futures) < 2:
                    eligible = next((entry for entry in pending if entry[0]["task"] not in active_tasks), None)
                    if eligible is None:
                        break
                    pending.remove(eligible)
                    active_tasks.add(eligible[0]["task"])
                    if devices:
                        row, _, arrival = eligible
                        offered = arrival
                        if causal_arrivals:
                            arrival = causal_ready(offered, session_completed.get(row["task"], 0))
                        decision = route_request(row, offered, arrival)
                        route = decision[1]
                        future = devices.submit(route.p, route.d,
                            lambda row=row, decision=decision: prefill(row, decision),
                            lambda value, row=row, decision=decision, arrival=arrival, offered=offered:
                                decode(row, decision, arrival, value, offered),
                            key=decision[0].key)
                    else:
                        future = pool.submit(one, *eligible)
                    futures[future] = eligible[0]["task"]
                if futures or pending or index < len(ready_rows):
                    time.sleep(0.005)
    finally:
        if devices:
            dump(directory / "pipeline-summary.json", devices.close())
        if queues is not None:
            dump(directory / "event-queue-ledger.json", queues.snapshot())
        if policy:
            policy.close()
            dump(directory / "policy.json", encode(policy.records))
            dump(directory / "slow-policy-contract.json", {"mode": slow_policy,
                 "trigger_records": policy.trigger.records,
                 "no_forced_action": True, "objective_scope": "admitted-pending-first"})
        if models:
            dump(directory / "operation-costs.json", encode(models.snapshot()))
            dump(directory / "cache-projection-summary.json", encode({"memo": queues.memo_totals, "node_indexes": queues.index_totals,
                 "forecast_calls": queues.cache_forecast_calls, "prospective_decision_CPU_prediction": False}))
        dump(directory / "cache-actions.json", encode(backend.events))
        backend.close()
    dump(directory / "phase-complete.json", {"requests": len(results), "seconds_including_planning_and_final_commit": time.monotonic() - started})
    return results


def load_pipeline_reference(reference, manifest, test, output, arrival_manifest=None, arrival_condition=None):
    protocol = json.loads((reference / "protocol.json").read_text(encoding="utf-8"))
    cost_path = reference / "cost-model.json"
    if (protocol["source_manifest"] != manifest
            or protocol["test_order"] != [row["id"] for row in test]
            or protocol["cost_sha256"] != file_sha(cost_path)
            or protocol["frozen_before_test"] is not True):
        raise ValueError("Pipeline must reuse the exact frozen stream and calibration costs")
    expected = {"tp": 1, "P": 2, "D": 2, "block": 128, "prefill": 128,
                "context": 32768, "dtype": "bfloat16", "P_output": 1, "D_output": 128,
                "pool_bytes_per_instance": 2 * 1024**3, "npu_kv_bytes": 2 * 1024**3,
                "model": str(MODEL)}
    if any(protocol.get(key) != value for key, value in expected.items()):
        raise ValueError("Declared serving configuration or cache budget changed")
    fitted = json.loads(cost_path.read_text(encoding="utf-8"))
    values = dict(fitted["costs"])
    values["service"] = tuple(ObservedLatencyModel(**row) for row in values["service"])
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = Costs(**values)
    interval = protocol["arrival_interval_seconds"]
    if isinstance(interval, bool) or not math.isfinite(interval) or interval <= 0:
        raise ValueError("Invalid frozen arrival interval")
    with (output / "cost-model.json").open("xb") as stream:
        stream.write(cost_path.read_bytes())
    protocol.update(execution_mode="independent_P_D_FIFO_queues",
                    global_end_to_end_inflight_limit=None, per_instance_active_limit=1,
                    D_queue_includes_priced_handoff=True,
                    reference_protocol_sha256=file_sha(reference / "protocol.json"),
                    reference_directory=str(reference),
                    reused_test_inputs_are_exposed=True,
                    statistical_scope="paired same-stream pipeline replay; descriptive, not fresh-heldout or stable-p99")
    if arrival_manifest is not None:
        suite, condition = load_condition(arrival_manifest, arrival_condition, protocol)
        with (output / "arrival-manifest.json").open("xb") as stream:
            stream.write(arrival_manifest.read_bytes())
        interval = 1 / condition["rate_rps"]
        protocol.update(arrival_model=condition["distribution"],
                        arrival_condition=condition, arrival_manifest_sha256=file_sha(arrival_manifest),
                        arrival_interval_seconds=interval, TTFT_origin="causal_ready_before_router_queue",
                        nominal_times_identical_actual_times_endogenous=True,
                        objective_metric="offered_to_first_seconds",
                        objective="nearest-rank nominal-offered-to-first-token p99 including upstream causality wait; unique resident bytes tie-break",
                        external_think_time_seconds=0,
                        statistical_scope=suite["sample_scope"])
    dump(output / "protocol.json", protocol)
    dump(output / "pipeline-contract.json", {
        "queues": ["P0", "P1", "D0", "D1"], "global_end_to_end_inflight_limit": None,
        "per_session_causality": True, "reference_protocol_sha256": protocol["reference_protocol_sha256"],
        "recalibration_or_test_based_retuning": False,
        "cost_sha256": protocol["cost_sha256"], "arrival_interval_seconds": interval})
    return costs, interval


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=__import__("pathlib").Path, required=True)
    parser.add_argument("--preload-p-first", action="store_true")
    parser.add_argument("--pipeline-reference", type=__import__("pathlib").Path)
    parser.add_argument("--arrival-manifest", type=__import__("pathlib").Path)
    parser.add_argument("--arrival-condition")
    parser.add_argument("--event-queue-policy", action="store_true")
    parser.add_argument("--operation-seed", type=__import__("pathlib").Path, required=True)
    parser.add_argument("--baseline-routing", default="task-affinity",
                        choices=("task-affinity", "shortest-queue", "local-prefix-affinity"))
    args = parser.parse_args()
    if not args.event_queue_policy or not args.pipeline_reference or args.baseline_routing != "task-affinity":
        raise ValueError("This revision requires the declared original reference baseline")
    seed_output = args.output / "operation-seed"
    seed_output.mkdir(exist_ok=False)
    for path in sorted(args.operation_seed.glob("calibration-*.json")):
        with (seed_output / path.name).open("xb") as stream:
            stream.write(path.read_bytes())
    dump(args.output / "operation-seed-manifest.json", {
        path.name: file_sha(path) for path in sorted(seed_output.glob("*.json"))})
    if args.baseline_routing != "task-affinity" and not args.event_queue_policy:
        raise ValueError("Named same-stack references require the event-queue pipeline")
    if args.event_queue_policy and not args.pipeline_reference:
        raise ValueError("Event queue candidate requires an explicitly frozen pipeline reference")
    if bool(args.arrival_manifest) != bool(args.arrival_condition) or (args.arrival_manifest and not args.pipeline_reference):
        raise ValueError("Arrival replay requires a frozen pipeline reference and an explicit condition")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    manifest = json.loads((ROOT / "workload/manifest.json").read_text(encoding="utf-8"))
    if manifest["requests_sha256"] != file_sha(ROOT / "workload/requests.jsonl"):
        raise ValueError("Frozen workload hash changed")
    rows = [json.loads(line) for line in (ROOT / "workload/requests.jsonl").read_text(encoding="utf-8").splitlines()]
    calibration = causal_order(rows, manifest["calibration_tasks"])
    test = causal_order(rows, manifest["test_tasks"])
    warmup_rows = [next(r for r in calibration if r["task"] == task) for task in manifest["calibration_tasks"]]
    baseline, costs, interval = None, None, None
    if args.pipeline_reference:
        if args.preload_p_first:
            raise ValueError("A frozen pipeline replay cannot enter calibration/preload mode")
        costs, interval = load_pipeline_reference(args.pipeline_reference, manifest, test, args.output,
                                                 args.arrival_manifest, args.arrival_condition)
        protocol = json.loads((args.output / "protocol.json").read_text(encoding="utf-8"))
        if args.event_queue_policy:
            import pegaflow.joint_policy as candidate_policy
            import pegaflow.joint_queue as candidate_queue
            import pegaflow.operation_cost as operation_cost
            import pegaflow.operation_schedule as operation_schedule
            import pegaflow.resource_queue as resource_queue
            import pegaflow.resource_runtime as resource_runtime
            import pegaflow.resource_placement as resource_placement

            candidate_sources = {}
            for name, module in (("joint_policy", candidate_policy), ("joint_queue", candidate_queue),
                ("operation_cost", operation_cost), ("operation_schedule", operation_schedule),
                ("resource_queue", resource_queue), ("resource_runtime", resource_runtime),
                ("resource_placement", resource_placement)):
                source_path = __import__("pathlib").Path(module.__file__)
                candidate_sources[name] = {"path": str(source_path), "sha256": file_sha(source_path)}
                snapshot = args.output / (name + "-executed-source.py")
                with snapshot.open("xb") as stream:
                    stream.write(source_path.read_bytes())
            protocol.update(candidate_revision="resource-DAG-01",
                            policy_source_sha256={name: value["sha256"] for name, value in candidate_sources.items()},
                            queue_forecast="owned operation DAG; source/destination gates; future D enqueue follows P completion",
                            D_prefix="revalidated after actual P; no assumed native admission",
                            placement_acceptance="strict predicted latency improvement; no resident-byte reward",
                            cost_adaptation="past-completed physical-feature operation NNLS; no window or fitted switch threshold",
                            objective="nominal-offered-to-first-token p99; memory is a feasibility constraint only",
                            frozen_before_test=False,
                            original_cost_frozen_before_exposed_replay=True,
                            exposed_replay_not_fresh_heldout=True,
                            independent_candidate_audit_required=True,
                            slow_work_seconds=None, slow_work_budget_kind="256 generated candidates; live-work score",
                            fast_objective_scope="admitted-live-requests-plus-candidate p99",
                            slow_objective_scope="admitted-live-requests fixed-route placement block",
                            operation_seed_manifest_sha256=file_sha(args.output / "operation-seed-manifest.json"),
                            operation_seed_initial_test_data_used=False,
                            original_cost_coefficients_replaced=False,
                            source_gate_link_model="exclusive direction conservatively reserved across full owned copy hold",
                            baseline="same PegaFlow stack task-parity plus original calibrated boundary; not native vLLM or Mooncake",
                            treatment="resource-DAG routing and budgeted live-work copy/admit/exchange/move/remove")
            if args.baseline_routing != "task-affinity":
                import pegaflow.joint_baselines as reference_module

                reference_path = __import__("pathlib").Path(reference_module.__file__)
                with (args.output / "joint_baselines-executed-source.py").open("xb") as stream:
                    stream.write(reference_path.read_bytes())
                protocol.update(baseline_routing=args.baseline_routing,
                                baseline_reference_source_sha256=file_sha(reference_path),
                                baseline="Named same-stack PegaFlow routing reference; not native vLLM or full Mooncake")
            save_candidate_protocol(args.output, protocol)
            # Keep the original coefficient hash; declare the post-observation
            # algorithm revision instead of relabelling it preregistered.
            contract_path = args.output / "event-queue-contract.json"
            dump(contract_path, {"candidate_revision": "resource-DAG-01",
                                 "cost_sha256": protocol["cost_sha256"],
                                 "executed_policy_sources": candidate_sources,
                                 "cost_coefficients_refitted": False,
                                 "additional_operation_model_seed": "original32calibration-only",
                                 "additional_operation_model_online_update": "past-completed-only",
                                 "test_outcome_tuned_thresholds": False,
                                 "algorithm_revised_after_exposed_results": True})
        dump(args.output / "controller-provenance.json", {
            "python": sys.executable, "root": str(ROOT),
            "sources": {name: file_sha(__import__("pathlib").Path(__file__).with_name(name))
                        for name in ("run.py", "pipeline.py", "arrivals.py", "common.py", "worker.py", "legacy_node.py")}})
    d_controller_acquired = False
    try:
        phases = ("baseline", "treatment") if args.pipeline_reference else ("calibration", "baseline", "treatment")
        for phase in phases:
            directory, backend, workers, meta, log = services(
                phase, args.output, preload_p=args.preload_p_first and phase == "calibration")
            d_controller_acquired = True
            try:
                dump(directory / "counters-before.json", [backend.rdma_bytes(n) for n in range(4)])
                warmup_records = warmups(backend, warmup_rows, directory)
                if phase == "calibration":
                    records, paths = calibrate(backend, calibration, directory)
                    sample = next(block for view in backend.views() for block in view.snapshot.blocks.values())
                    bytes_per_token = sample["payload_bytes"] // 128
                    costs, fitted = fit(records, backend.events, bytes_per_token, warmup_records)
                    dump(args.output / "cost-model.json", encode(fitted))
                    # Offered load at measured two-D capacity. This rate uses
                    # calibration only, never held-out service times or reward.
                    interval = sum(r["D_wire"]["complete_seconds"] for r in records) / (2 * len(records))
                    protocol = {"model": str(MODEL), "tp": 1, "P": 2, "D": 2, "block": 128,
                        "prefill": 128, "context": 32768, "dtype": "bfloat16", "P_output": 1, "D_output": 128,
                        "pool_bytes_per_instance": 2 * 1024**3, "npu_kv_bytes": 2 * 1024**3,
                        "arrival_interval_seconds": interval, "open_arrivals_with_per_session_causality": True,
                        "test_order": [r["id"] for r in test], "source_manifest": manifest,
                        "cost_sha256": file_sha(args.output / "cost-model.json"), "slow_candidate_budget": 256,
                        "slow_work_seconds": 0.1, "fast_candidate_budget": 64,
                        "baseline": "fixed task-parity pair; measured fetch/recompute selection; native admission/replacement",
                        "treatment": "four-pair routing + past-only queue-coupled admission/copy/exchange/removal search + native checked real commits",
                        "planning_and_allocation_and_copy_and_wait_included": True,
                        "objective": "nearest-rank arrival-to-first-token p99; unique resident bytes as lexicographic tie-break",
                        "RDMA_byte_scope": "successful application payload; failed partial-wire bytes unavailable, failed RPC wall time included",
                        "host_inventory_authority_GPU_opaque_hits_disabled_in_both_arms": True,
                        "global_optimality_claimed": False, "strict_zero_prefill_claimed": False,
                        "replay_pressure_model": "causal unprotected FIFO approximation, not an exact reconstruction of native TinyLFU admission",
                        "restore_service_feature": "actually accepted resident cache bytes; explicit wire copy priced separately",
                        "frozen_before_test": True, "statistical_scope": "single paired stream; descriptive, not stable-p99 or accuracy"}
                    dump(args.output / "protocol.json", encode(protocol))
                    dump(directory / "copy-paths.json", encode(paths))
                    dump(directory / "cache-actions.json", encode(backend.events))
                else:
                    if args.pipeline_reference:
                        dump(directory / "initial-inventory.json", [backend.inventory(n) for n in range(4)])
                    dump(directory / "formal-arm-started.json", {"phase": phase, "started_unix": time.time(),
                         "protocol_sha256": file_sha(args.output / "protocol.json"), "four_ready": True})
                    result = formal(phase, backend, test, costs, interval, directory, baseline,
                                    pipeline=args.pipeline_reference is not None,
                                    arrival_offsets=None if not args.arrival_manifest else protocol["arrival_condition"]["offered_arrivals_seconds"],
                                    causal_arrivals=args.arrival_manifest is not None,
                                    event_queue_policy=args.event_queue_policy,
                                    baseline_routing=args.baseline_routing)
                    if phase == "baseline":
                        baseline = {r["id"]: r for r in result}
                dump(directory / "counters-after.json", [backend.rdma_bytes(n) for n in range(4)])
                dump(directory / "final-inventory.json", [backend.inventory(n) for n in range(4)])
            finally:
                if phase == "calibration" and not (directory / "cache-actions.json").exists():
                    dump(directory / "cache-actions.json", encode(backend.events))
                close_services(phase, workers, meta, log)
        dump(args.output / "pair-complete.json", {"completed": True, "independent_audit_pending": True})
        rpc(f"http://{D_IP}:{AGENT_PORT}/finish", {}, timeout=120)
    except BaseException as error:
        dump(args.output / "failure.json", {"type": type(error).__name__, "error": str(error)})
        if d_controller_acquired:
            with contextlib.suppress(OSError):
                rpc(f"http://{D_IP}:{AGENT_PORT}/abort", {}, timeout=120)
        raise


if __name__ == "__main__":
    main()
