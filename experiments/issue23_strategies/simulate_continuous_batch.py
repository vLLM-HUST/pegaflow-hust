"""Causal 2P2D CPU screen with bounded, iteration-level D batching.

The immutable real worker remains serial. This new simulator and route calendar
are a candidate execution model, not a validated parallel serving deployment.
"""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
from dataclasses import asdict, replace
from pathlib import Path

import simulate_policy_feedback as parent
from pegaflow import strategies

# The historical parent pins its own overlay first. Extend only the new policy
# package; never replace bytes in that frozen real-serving snapshot.
strategies.__path__.insert(
    0, str(Path(__file__).resolve().parents[2] / "python/pegaflow/strategies")
)

from batch_calendar import BatchedQueues
from pegaflow.joint_policy import Costs
from pegaflow.resource_queue import direction
from pegaflow.strategies.continuous_batch import (
    AllocationLeases,
    BatchConfig,
    ContinuousBatcher,
    Sequence,
)


class BatchSimulation(parent.Simulation):
    atomic_copy_acquire = False

    def __init__(self, *args, batch_config, phase_residual_world=False, **kwargs):
        super().__init__(*args, **kwargs)
        if phase_residual_world and not self.residual_world:
            raise ValueError(
                "Phase allocation requires the retained aggregate residual world"
            )
        self.phase_residual_world = phase_residual_world
        self.phase_residual_samples = []
        self.batch_config = batch_config
        self.d_batches = {n: ContinuousBatcher(batch_config) for n in (2, 3)}
        self.d_active = {}
        self.batch_trace = []
        self.allocation_leases = AllocationLeases()
        self.intrinsic_protection = frozenset(
            (n, u.identity)
            for n, units in enumerate(self.layout.units)
            for u in units
            if u.protected
        )
        self.lease_owner = None
        self.policy_queues = BatchedQueues(
            self.models,
            512,
            memoize=True,
            index_projection=True,
            predict_control=False,
            batch_config=batch_config,
            progress=self.progress,
        )
        self.policy_queues.started = 0.0

    def pin(self, node, hashes, sign, *, owner=None):
        owner = owner if owner is not None else self.lease_owner
        if owner is None:
            raise ValueError("Concurrent leases require a request/operation owner")
        if sign == 1:
            self.allocation_leases.acquire(
                owner,
                node,
                (
                    u.identity
                    for u in self.layout.units[node]
                    if u.keys.intersection(hashes)
                ),
            )
        elif sign == -1:
            identities = self.allocation_leases.release(owner, node)
            if not identities.issubset({u.identity for u in self.layout.units[node]}):
                raise ValueError("A leased allocation disappeared before release")
        else:
            raise ValueError("Lease acquisition/release sign required")
        units = list(self.layout.units)
        units[node] = tuple(
            replace(
                u,
                protected=(node, u.identity) in self.intrinsic_protection
                or self.allocation_leases.protected(node, u.identity),
            )
            for u in units[node]
        )
        self.layout = replace(self.layout, units=tuple(units))

    def complete_operation(self, key, operation):
        previous = self.lease_owner
        self.lease_owner = key, operation[0]
        try:
            return super().complete_operation(key, operation)
        finally:
            self.lease_owner = previous

    def workers(self, row, frozen):
        return tuple(
            replace(
                w, active_sequences=sum(n == w.node for n in self.d_active.values())
            )
            if w.node >= 2
            else w
            for w in super().workers(row, frozen)
        )

    def progress(self):
        return {
            sequence.key: replace(sequence)
            for batch in self.d_batches.values()
            for sequence in (*batch.active.values(), *batch.waiting)
        }

    def pump(self):
        changed = True
        while changed:
            changed = False
            for node in range(4):
                capacity = 1 if node < 2 else self.batch_config.max_sequences
                active_count = (
                    int(node in self.active)
                    if node < 2
                    else sum(n == node for n in self.d_active.values())
                )
                if not self.admission_ready(node, capacity - active_count):
                    continue
                while self.queues[node] and active_count < capacity:
                    key = self.queues[node].popleft()
                    (self.active if node < 2 else self.d_active)[
                        node if node < 2 else key
                    ] = key if node < 2 else node
                    if self.mode != "native":
                        self.policy_queues.observe(key, node, "started", self.now)
                    self.stage(key, node)
                    active_count += 1
                    changed = True
            owned = [(node, key) for node, key in self.active.items()]
            owned += [(node, key) for key, node in self.d_active.items()]
            self.prepare_dispatch(owned)
            for node, key in sorted(owned, key=self.operation_priority):
                job = self.jobs[key]
                if job.get("running") or not job["steps"]:
                    continue
                operation = job["steps"][0]
                name, duration, resources, copy_data, first = operation
                if not self.operation_permitted(key, resources):
                    continue
                if name == "D_api":
                    tail = duration - first
                    restore, save = self.api_phase_split(
                        key, node, job["count"] * 128, first, tail
                    )
                    sequence = Sequence(
                        key,
                        job["row"]["tokens"],
                        job["count"] * 128,
                        max(0.0, first - restore),
                        max(0.0, tail - save),
                        restore,
                        save,
                    )
                    self.batch_submitted(key, node, sequence)
                    self.pin(
                        node,
                        set(self.item(job["row"]).hashes[: job["count"]]),
                        1,
                        owner=(key, name),
                    )
                    self.d_batches[node].submit(sequence)
                    job["running"], job["api_start"] = operation, self.now
                    job["batch_restore"], job["batch_save"] = restore, save
                    if self.mode != "native":
                        self.policy_queues.skip_copy(key, "handoff")
                        self.policy_queues.operation(
                            key, name, "start", self.now, resources
                        )
                    changed = True
                    continue
                if copy_data and self.mode != "native":
                    gates = tuple(sorted({f"gate{copy_data[0]}", f"gate{node}"}))
                    if not job.get("copy_gate_hold"):
                        acquisition = (
                            (*gates, "link:" + direction(copy_data[0], node))
                            if self.atomic_copy_acquire
                            else gates
                        )
                        if any(r in self.held for r in acquisition):
                            continue
                        for resource in gates:
                            self.held[resource] = key, name
                        job["copy_gate_hold"] = gates, self.now
                        self.policy_queues.operation(
                            key,
                            name,
                            "start",
                            self.now,
                            (*gates, "link:" + direction(copy_data[0], node)),
                        )
                    resources = ("link:" + direction(copy_data[0], node),)
                    operation = (name, duration, resources, copy_data, first)
                    job["steps"][0] = operation
                if any(r in self.held for r in resources):
                    continue
                for resource in resources:
                    self.held[resource] = key, name
                if copy_data:
                    self.pin(copy_data[0], set(copy_data[2]), 1, owner=(key, name))
                elif name.endswith("_api"):
                    self.pin(
                        node,
                        set(self.item(job["row"]).hashes[: job["count"]]),
                        1,
                        owner=(key, name),
                    )
                job["running"] = operation
                self.resource_events.append(
                    {
                        "request": key,
                        "node": node,
                        "operation": name,
                        "start": self.now,
                        "end": self.now + duration,
                        "resources": resources,
                    }
                )
                if self.mode != "native" and name != "D_control" and not copy_data:
                    if name.endswith("_api"):
                        self.policy_queues.skip_copy(key, "history")
                    self.policy_queues.operation(
                        key, name, "start", self.now, resources
                    )
                job["work"] = replace(
                    job["work"], active_operations=((name, self.now),)
                )
                self.event(self.now + duration, "operation", key, operation)
                changed = True
            for node, batch in self.d_batches.items():
                resource = f"gate{node}"
                if resource in self.held or batch.busy is not None:
                    continue
                iteration = self.begin_iteration(node, batch)
                if iteration is None:
                    continue
                self.held[resource] = f"batch{node}", iteration.kind
                self.batch_trace.append(
                    {
                        "node": node,
                        "start": self.now,
                        "end": self.now + iteration.seconds,
                        "kind": iteration.kind,
                        "pieces": iteration.pieces,
                        "kv_bytes": batch.used_bytes,
                    }
                )
                self.event(self.now + iteration.seconds, "batch", node, iteration)
                changed = True

    def operation_priority(self, entry):
        return entry

    def api_phase_split(self, key, node, prefix, first, tail):
        if not self.phase_residual_world:
            return super().api_phase_split(key, node, prefix, first, tail)
        from phase_residual_world import joint_phase_split

        restore, save, record = joint_phase_split(
            self.world_seed, self.world, key, node, prefix, first, tail
        )
        self.phase_residual_samples.append(record)
        return restore, save

    def admission_ready(self, node, free_slots):
        return True

    def prepare_dispatch(self, owned):
        pass

    def operation_permitted(self, key, resources):
        return True

    def begin_iteration(self, node, batch):
        return batch.begin()

    def batch_submitted(self, key, node, sequence):
        pass

    def batch_iteration_finished(self, node, iteration):
        pass

    def finish_d(self, key):
        job = self.jobs[key]
        node = job["node"]
        if job["first"] is None:
            raise ValueError("D completion before its first token")
        item = self.item(job["row"])
        self.pin(node, set(item.hashes[: job["count"]]), -1, owner=(key, "D_api"))
        self.save_cache(node, item, self.costs)
        if self.mode != "native":
            self.policy_queues.operation(key, "D_api", "finish", self.now)
            self.policy_queues.observe(key, node, "finished", self.now)
        # Batched residence time contains other jobs' service. Do not train it
        # as an isolated singleton compute/H2D sample and double count queues.
        self.api_records.append(
            {
                "id": key,
                "node": node,
                "tokens": item.tokens,
                "prefix": job["count"] * 128,
                "first": job["first"] - job["api_start"],
                "full": self.now - job["api_start"],
                "completed": self.now,
                "restore_path_not_pure_H2D": job["batch_restore"],
                "synchronous_finish_seconds": job["batch_save"],
                "trained_as_singleton": False,
            }
        )
        self.finished[key] = {
            "id": key,
            "TTFT": job["first"] - job["ready"],
            "nominal": job["first"] - job["offered"],
            "dependency": job["ready"] - job["offered"],
            "p": job["plan"].p,
            "d": job["plan"].d,
            "end": self.now,
        }
        self.sessions[job["row"]["task"]] = self.now
        self.active_sessions.remove(job["row"]["task"])
        del self.d_active[key]
        del self.jobs[key]

    def run(self):
        while self.events:
            at, _, kind, key, value = heapq.heappop(self.events)
            self.now = max(self.now, at)
            if kind == "arrival":
                self.pending.append(key)
            elif kind == "routed":
                self.router_busy = False
                job = self.jobs[key]
                self.cpu_samples.append(job["pred_CPU"])
                self.queues[job["plan"].p].append(key)
                if self.mode != "native":
                    self.policy_queues.observe(key, job["plan"].p, "enqueued", self.now)
            elif kind == "batch":
                if self.held.pop(f"gate{key}") != (f"batch{key}", value.kind):
                    raise ValueError("Batch device ownership changed")
                first, finished = self.d_batches[key].finish(value, self.now)
                self.batch_iteration_finished(key, value)
                for request in first:
                    job = self.jobs[request]
                    job["first"] = self.now
                    job["work"] = replace(job["work"], actual_first=self.now)
                    if self.mode != "native":
                        self.policy_queues.observe(request, key, "first", self.now)
                for request in finished:
                    self.finish_d(request)
            elif kind == "operation":
                self.complete_operation(key, value)
            elif kind == "admission":
                # A scheduler's measured CPU decision has completed. Its
                # selected queue prefix is consumed by the following pump.
                pass
            else:
                raise ValueError("Unknown parallel simulation event")
            self.route()
            self.pump()
            self.peak_usage = [
                max(a, b)
                for a, b in zip(self.peak_usage, self.layout.usage_values, strict=True)
            ]
            if not self.layout.feasible():
                raise ValueError("Physical host allocation exceeded")
        if (
            len(self.finished) != len(self.rows)
            or self.jobs
            or self.pending
            or self.held
            or self.d_active
            or any(not batch.idle for batch in self.d_batches.values())
            or self.allocation_leases.handles
        ):
            raise ValueError("Incomplete parallel causal replay")
        return {
            "policy": self.mode,
            "requests": len(self.finished),
            "TTFT": parent.quantiles([r["TTFT"] for r in self.finished.values()]),
            "nominal": parent.quantiles([r["nominal"] for r in self.finished.values()]),
            "makespan": self.now,
            "planning_wall": sum(r["CPU_wall"] for r in self.planning),
            "boundary_CPU_wall": sum(self.boundary_CPU),
            "fit_CPU_wall": sum(self.fit_CPU),
            "batch_config": asdict(self.batch_config),
            "phase_residual_world": self.phase_residual_world,
            "phase_residual_samples": self.phase_residual_samples,
            "D_summary": {
                str(n): {
                    "peak_active": b.peak_sequences,
                    "peak_iteration_sequences": b.peak_iteration_sequences,
                    "peak_kv_bytes": b.peak_bytes,
                }
                for n, b in self.d_batches.items()
            },
            "per_request": list(self.finished.values()),
            "stages": self.api_records,
            "batch_iterations": self.batch_trace,
            "resource_events": self.resource_events,
            "actions": self.actions,
            "decisions": self.planning,
            "peak_host_bytes": self.peak_usage,
            "host_capacity": self.layout.capacities,
            "D_residence_not_trained_as_singleton": True,
            "same_batch_calendar_for_policy_and_world": True,
            "forecast_restarts_one_inflight_iteration": True,
            "P_execution": "one whole prefill per P instance",
            "slow_layout_policy": "off",
            "native_source_and_publication_approximate": True,
            "batch_scaling_measured": False,
            "real_parallel_serving_validated": False,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-bundle", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-sequences", type=int, nargs="+", default=[1, 4])
    parser.add_argument("--shared-fraction", type=float, nargs="+", default=[0.0, 0.5])
    parser.add_argument("--residual-world", action="store_true")
    parser.add_argument(
        "--kv-gib",
        type=int,
        help="Explicit capacity ablation; default uses the 910B2 scenario",
    )
    parser.add_argument(
        "--limit", type=int, help="Contract smoke only; not a benchmark subset"
    )
    args = parser.parse_args()
    if args.kv_gib is not None and args.kv_gib <= 0:
        parser.error("KV GiB must be positive")
    if args.output.exists():
        raise FileExistsError("Never overwrite a screen")
    case_directory = args.output.with_suffix(".cases")
    case_directory.mkdir(parents=True, exist_ok=False)
    bundle = parent.read(args.input_bundle)
    values = dict(bundle["costs"])
    values["service"] = tuple(
        parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = Costs(**values)
    rows = bundle["rows"][: args.limit] if args.limit else bundle["rows"]
    result = {
        "kind": "continuous-D-batch-causal-CPU-sensitivity-not-real-performance",
        "input_sha256": hashlib.sha256(args.input_bundle.read_bytes()).hexdigest(),
        "requests": len(rows),
        "session_count": len({r["task"] for r in rows}),
        "batch_speedup_uncalibrated": True,
        "actual_GPU_runtime_modified": False,
        "cases": [],
    }
    for sequences in args.max_sequences:
        for shared in args.shared_fraction:
            if sequences == 1 and shared != args.shared_fraction[0]:
                continue
            for rate in parent.RATES:
                config = BatchConfig(
                    max_sequences=sequences,
                    shared_fraction=shared,
                    bytes_per_token=costs.bytes_per_token,
                    **({"kv_bytes": args.kv_gib * 1024**3} if args.kv_gib else {}),
                )
                case = {"rate": rate, "config": asdict(config), "arms": {}}
                for mode in ("native", "ours"):
                    print(
                        json.dumps(
                            {
                                "start": mode,
                                "rate": rate,
                                "max_sequences": sequences,
                                "shared": shared,
                            }
                        ),
                        flush=True,
                    )
                    case["arms"][mode] = BatchSimulation(
                        rows,
                        bundle["initial_inventory"],
                        costs,
                        args.seed,
                        bundle["control_cost_rows"],
                        rate,
                        mode,
                        args.residual_world,
                        batch_config=config,
                    ).run()
                b, t = (case["arms"][m]["TTFT"]["p99"] for m in ("native", "ours"))
                case["descriptive_p99_gain"] = 1 - t / b
                result["cases"].append(case)
                with (case_directory / f"case-{len(result['cases']):02d}.json").open(
                    "x", encoding="utf-8"
                ) as stream:
                    json.dump(case, stream, indent=2)
                print(
                    json.dumps(
                        {
                            "rate": rate,
                            "max_sequences": sequences,
                            "shared": shared,
                            "B_p99": b,
                            "T_p99": t,
                            "gain": 1 - t / b,
                        }
                    ),
                    flush=True,
                )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)


if __name__ == "__main__":
    main()
