"""P/D engine iterations with asynchronous queries and batched KV restoration."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict, deque
from pathlib import Path

from pd_multinode_simulator import CalibratedPD, HandoffPool
from pd_serving_simulator import BLOCK_BYTES, seed_gpu
from step_simulator import Job, vector


def curve_duration(points, blocks):
    """Interpolate deployment component probes without request identities."""
    if not points or blocks <= 0:
        return 0.0
    curve = sorted([(0, 0.0), *((p["blocks"], p["seconds"]) for p in points)])
    below = max((p for p in curve if p[0] <= blocks), default=curve[0])
    above = min((p for p in curve if p[0] >= blocks), default=curve[-1])
    if below == above:
        if blocks == below[0]:
            return below[1]
        below, above = curve[-2:]
    return max(
        0,
        below[1] + (above[1] - below[1]) * (blocks - below[0]) / (above[0] - below[0]),
    )


def graph_vector(row):
    if row["prefill_tokens"] == 0:
        return [
            1,
            row["padded_sequences"] / 8,
            row["decode_context"] / (8 * 8192),
            row["max_context"] / 8192,
        ]
    return [
        1,
        (row["prefill_tokens"] + row["decode_tokens"]) / 2048,
        row["prefill_attention"] / (2048 * 8192),
        row["decode_context"] / (8 * 8192),
        row["sequences"] / 8,
    ]


class GraphCost:
    def __init__(self, data, restore_active=None):
        self.data = data
        self.restore_active = restore_active or (lambda: False)

    def duration(self, row):
        kind = "decode" if row["prefill_tokens"] == 0 else "prefill"
        model = self.data["models"][kind]
        inputs = (
            graph_vector(row) if model.get("kind") == "graph_shape" else vector(row)
        )
        duration = max(
            1e-6, sum(a * b for a, b in zip(model["coefficients"], inputs, strict=True))
        )
        if self.restore_active():
            duration *= model.get("restore_busy_slowdown", 1)
        return duration


class ReservedHandoffPool(HandoffPool):
    """Keep actual destination and existing prefix pages alive during async loads."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reservations = {}

    def reserve_restore(self, job, target, gpu=None):
        prefix = (
            super().prefix(job)
            if gpu is None
            else [next(iter(self.cached[self.key(job, i)])) for i in range(gpu)]
        )
        needed = target - len(prefix)
        if needed + sum(self.blocks[b].references == 0 for b in prefix) > len(
            self.free
        ):
            raise ValueError(
                "Pending GPU restoration requires an uncalibrated preemption"
            )
        pages = list(prefix)
        for index in prefix:
            block = self.blocks[index]
            if block.references == 0:
                del self.free[index]
            block.references += 1
        for _ in range(needed):
            index, _ = self.free.popitem(last=False)
            block = self.blocks[index]
            if block.key is not None:
                options = self.cached[block.key]
                del options[index]
                if not options:
                    del self.cached[block.key]
                self.evictions += 1
            block.key = None
            block.references = 1
            pages.append(index)
        self.reservations[job.request.id] = (pages, len(prefix))
        self.peak_active = max(
            self.peak_active, sum(b.references > 0 for b in self.blocks)
        )

    def publish_restore(self, job):
        pages, existing = self.reservations[job.request.id]
        for position, index in enumerate(pages[existing:], existing):
            key = self.key(job, position)
            self.blocks[index].key = key
            self.cached.setdefault(key, {}).update({index: None})

    def prefix(self, job):
        if job.request.id in self.reservations:
            return self.reservations[job.request.id][0]
        return super().prefix(job)

    def allocate(self, job, tokens, prefix=()):
        result = super().allocate(job, tokens, prefix)
        if result and job.request.id in self.reservations:
            pages, _ = self.reservations.pop(job.request.id)
            for index in pages:
                self.blocks[index].references -= 1
        return result


class PipelineSimulation(CalibratedPD):
    def __init__(self, workload, profile, config=None):
        super().__init__(workload, profile, config)
        self.reserve_pages = profile.get("reserve_restore_pages", True)
        if self.reserve_pages:
            self.pools = [
                ReservedHandoffPool(
                    len(pool.blocks), pool.block_size, pool.enable_caching
                )
                for pool in self.pools
            ]
        if profile.get("graph_models"):
            self.role_models = {
                role: GraphCost(
                    model,
                    lambda: (
                        self.current_compute_node in self.flow
                        or any(
                            n == self.current_compute_node for n, _ in self.publications
                        )
                    ),
                )
                for role, model in profile["graph_models"].items()
            }
        self.incoming = defaultdict(deque)
        self.fetch_ready = defaultdict(deque)
        self.copy_queue = defaultdict(deque)
        self.loaded = defaultdict(deque)
        self.flow = {}
        self.compute_active = defaultdict(bool)
        self.cycle_event = {}
        self.last_cycle = {}
        self.pipeline_ready = set()
        self.copy_sequence = 0
        self.copy_batches = []
        self.idle_poll = profile.get("idle_poll_s", 0.001)
        self.restore_model = profile["restore_model"]
        self.frontend_free = defaultdict(float)
        self.network_flows = {}
        self.network_sequence = 0
        self.current_compute_node = None
        self.publications = {}

    def api_delay(self, role, tokens):
        a, b = self.profile.get("api_inbound", {}).get(role, [0, 0])
        return max(0, a + b * tokens / 8192)

    def prepare(self, key, node):
        role = "P" if node < self.p_count else "D"
        marker = key, node
        if marker not in self.pipeline_ready:
            self.pipeline_ready.add(marker)
            delay = self.api_delay(role, self.rows[key]["tokens"])
            if role == "D":
                delay += self.profile.get("client_handoff_gap_s", 0)
            start = self.now
            if self.config.get("frontend_serialization", False):
                start = max(start, self.frontend_free[node])
                self.frontend_free[node] = start + delay
            self.event(start + delay, "api_delivered", key, node)
            return
        if role == "P":
            return super().prepare(key, node)
        live = self.jobs[key]
        live["engine_jobs"][node] = Job(self.request(key, node))
        live["stages"]["D"] = {"prepare": self.now, "node": node}
        self.preparing.add((node, key))
        self.incoming[node].append(key)
        self.ensure_cycle(
            node,
            immediate=not (
                self.flow.get(node) or self.copy_queue[node] or self.fetch_ready[node]
            ),
        )

    def ensure_cycle(self, node, *, immediate=False):
        if node in self.engine_busy:
            return
        when = self.now if immediate else self.now + self.idle_poll
        if self.cycle_event.get(node, float("inf")) <= when:
            return
        self.cycle_event[node] = when
        self.event(when, "D_cycle", str(node), (node, when))

    def restore(self, key, node, gpu, target):
        # RPC prefetch completion becomes visible at a scheduler iteration.
        self.fetch_ready[node].append((key, gpu, target))
        if target > gpu:
            self.host[node].pins.update(self.rows[key]["hashes"][gpu:target])
        self.ensure_cycle(node)

    def cycle(self, node):
        if self.last_cycle.get(node) == self.now:
            return
        self.last_cycle[node] = self.now
        while self.loaded[node]:
            batch = self.loaded[node].popleft()
            for key, gpu, target in batch["items"]:
                if self.reserve_pages:
                    self.pools[node].publish_restore(
                        self.jobs[key]["engine_jobs"][node]
                    )
                else:
                    seed_gpu(self.pools[node], self.rows[key]["hashes"][gpu:target])
                self.jobs[key]["stages"]["D"].update(
                    restore_end=batch["end"],
                    restore_observed=self.now,
                    completion_feedback_s=self.now - batch["end"],
                )
                self.admit(key, node)
        limit = self.config.get(
            "D_max_sequences", self.config.get("max_sequences", 128)
        )
        # vLLM skips asynchronous waits while a free running slot exists.
        if len(self.running[node]) + len(self.waiting[node]) < limit:
            remaining = deque()
            while self.incoming[node]:
                key = self.incoming[node].popleft()
                if len(self.running[node]) + len(self.waiting[node]) >= limit:
                    remaining.append(key)
                    continue
                live = self.jobs[key]
                row = self.rows[key]
                job = live["engine_jobs"][node]
                gpu = len(self.pools[node].prefix(job))
                target = len(row["hashes"])
                source = live["P"]
                if self.config.get("handoff_policy", "full") == "best_effort":
                    target = gpu
                    while target < len(row["hashes"]) and (
                        row["hashes"][target] in self.host[node].keys
                        or row["hashes"][target] in self.host[source].keys
                    ):
                        target += 1
                live["stages"]["D"].update(
                    NPU_initial_prefix_blocks=gpu,
                    target_prefix_blocks=target,
                    restore_bytes=(target - gpu) * BLOCK_BYTES,
                    query_start=self.now,
                )
                if target == gpu:
                    job.external_prefix = target
                    self.admit(key, node)
                    continue
                missing = [
                    h
                    for h in row["hashes"][gpu:target]
                    if h not in self.host[node].keys
                ]
                if any(h not in self.host[source].keys for h in missing):
                    if (source, key) in self.publications:
                        remaining.append(key)
                        continue
                    raise ValueError("P cache cannot supply the required prefix")
                job.external_prefix = target
                if missing:
                    self.host[source].pins.update(missing)
                    a, b = self.profile.get("discovery_model", {}).get(
                        "coefficients", [0, 0]
                    )
                    query = (
                        self.profile.get("query_fixed_s", 0) + a + b * len(missing) / 64
                    )
                    self.event(
                        self.now + query,
                        "pipeline_fetch",
                        key,
                        (node, gpu, target, source, missing),
                    )
                else:
                    self.restore(key, node, gpu, target)
            self.incoming[node] = remaining
            if self.fetch_ready[node]:
                items = list(self.fetch_ready[node])
                self.fetch_ready[node].clear()
                if self.reserve_pages:
                    for key, gpu, target in items:
                        self.pools[node].reserve_restore(
                            self.jobs[key]["engine_jobs"][node], target, gpu
                        )
                self.copy_sequence += 1
                batch = {
                    "batch": self.copy_sequence,
                    "node": node,
                    "items": items,
                    "queued": self.now,
                    "blocks": sum(target - gpu for _, gpu, target in items),
                    "request_count": len(items),
                }
                for key, _, _ in items:
                    self.jobs[key]["stages"]["D"].update(
                        restore_batch=batch["batch"], restore_queued=self.now
                    )
                self.copy_queue[node].append(batch)
        if node not in self.flow and self.copy_queue[node]:
            self.start_copy(node)

    def start_copy(self, node):
        if node in self.flow or not self.copy_queue[node]:
            return
        batch = self.copy_queue[node].popleft()
        coefficients = self.restore_model["coefficients"]
        if self.restore_model.get("kind") == "curve_capacity":
            curve = self.config.get("device_restore_curves", {}).get(
                str(node), self.restore_model["default_curve"]
            )
            size = curve_duration(curve, batch["blocks"])
        elif self.restore_model.get("kind") == "capacity":
            rate = self.config.get("device_copy_GBps", {}).get(
                str(node),
                self.config.get("host_copy_GBps", {}).get(
                    self.hosts[node], self.restore_model["default_copy_GBps"]
                ),
            )
            size = batch["blocks"] * BLOCK_BYTES / (rate * 1e9)
        else:
            size = batch["blocks"] / 64
        vector = [1, size, max(0, batch["request_count"] - 1) / 4]
        work = max(1e-6, sum(a * b for a, b in zip(coefficients, vector, strict=True)))
        batch.update(
            start=self.now, work_s=work, remaining=work, last=self.now, generation=0
        )
        self.flow[node] = batch
        self.copy_busy[node] += 1
        for key, _, _ in batch["items"]:
            self.jobs[key]["stages"]["D"]["restore_start"] = self.now
        self.advance_copy(node)

    def advance_copy(self, node):
        flow = self.flow.get(node)
        if flow is None:
            return
        old_rate = flow.get("rate", 1)
        flow["remaining"] = max(
            0, flow["remaining"] - (self.now - flow["last"]) * old_rate
        )
        flow["last"] = self.now
        slowdown = self.restore_model.get("busy_slowdown", 1)
        flow["rate"] = 1 / slowdown if self.compute_active[node] else 1
        flow["generation"] += 1
        self.event(
            self.now + flow["remaining"] / flow["rate"],
            "copy_complete",
            str(node),
            (node, flow["batch"], flow["generation"]),
        )

    def set_compute(self, node, active):
        if self.compute_active[node] == active:
            return
        self.compute_active[node] = active
        self.advance_copy(node)

    def schedule(self, node):
        self.current_compute_node = node
        if node < self.p_count:
            return super().schedule(node)
        if node in self.engine_busy:
            return
        if self.running[node] or self.waiting[node]:
            self.cycle(node)
        before = len(self.steps)
        super().schedule(node)
        if len(self.steps) > before:
            self.set_compute(node, True)
        elif self.incoming[node] or self.fetch_ready[node] or self.loaded[node]:
            self.ensure_cycle(node)

    def step_done(self, node, scheduled):
        super().step_done(node, scheduled)
        if node >= self.p_count and not self.running[node]:
            self.set_compute(node, False)

    def engine_done(self, key, node):
        model = self.profile.get("P_publish_model")
        if node >= self.p_count or model is None:
            return super().engine_done(key, node)
        live = self.jobs[key]
        stage = live["stages"]["P"]
        stage["engine_end"] = self.now
        missing = [h for h in self.rows[key]["hashes"] if h not in self.host[node].keys]
        curve = self.config.get("device_publish_curves", {}).get(
            str(node), model["default_curve"]
        )
        delay = (
            curve_duration(curve, len(missing))
            if missing
            else model.get("baseline_s", 0)
        )
        live["D"] = self.p_count + self.route(key, "D")
        self.publications[node, key] = live["engine_jobs"][node]
        stage.update(publish_queued=self.now, publish_blocks=len(missing))
        if missing:
            self.resource(
                f"P-publication:{node}",
                delay,
                "P_publish",
                key,
                (node, missing),
                payload=len(missing) * BLOCK_BYTES,
            )
        else:
            self.event(self.now + delay, "P_publish", key, (node, missing))
        if not self.config.get("source_publish_gate", False):
            self.prepare(key, live["D"])

    def extra_event(self, kind, key, data):
        if kind == "P_publish":
            node, missing = data
            self.host[node].add(missing)
            job = self.publications.pop((node, key))
            self.pools[node].release(job)
            live = self.jobs.get(key) or self.finished[key]
            live["stages"]["P"]["publish_end"] = self.now
            if key in self.jobs:
                if self.config.get("source_publish_gate", False):
                    self.prepare(key, live["D"])
                else:
                    self.ensure_cycle(live["D"], immediate=True)
            return True
        if kind == "pipeline_fetch":
            payload = len(data[4]) * BLOCK_BYTES
            if self.config.get("network_sharing", "fifo") == "processor_sharing":
                self.advance_network()
                self.network_sequence += 1
                target, _, _, source, _ = data
                self.network_flows[self.network_sequence] = {
                    "id": key,
                    "data": data,
                    "bytes": payload,
                    "remaining": float(payload),
                    "queued": self.now,
                    "start": self.now,
                    "last": self.now,
                    "endpoints": (
                        f"{self.hosts[source]}:out",
                        f"{self.hosts[target]}:in",
                    ),
                    "generation": 0,
                }
                self.schedule_network()
                return True
            self.resource(
                "RDMA:P-to-D",
                payload * self.network_s_per_byte,
                "remote_done",
                key,
                data,
                payload=payload,
            )
            return True
        if kind == "network_complete":
            identifier, generation = data
            flow = self.network_flows.get(identifier)
            if flow is None or flow["generation"] != generation:
                return True
            self.advance_network()
            flow = self.network_flows.pop(identifier)
            target, gpu, count, source, missing = flow["data"]
            self.host[target].add(missing)
            self.host[source].pins.subtract(missing)
            self.transfers.append(
                {
                    "id": flow["id"],
                    "resource": f"RDMA:{self.hosts[source]}->{self.hosts[target]}",
                    "queued": flow["queued"],
                    "start": flow["start"],
                    "end": self.now,
                    "bytes": flow["bytes"],
                }
            )
            self.restore(flow["id"], target, gpu, count)
            self.schedule_network()
            return True
        if kind == "api_delivered":
            self.prepare(key, data)
            return True
        if kind == "D_cycle":
            node, when = data
            if self.cycle_event.get(node) != when:
                return True
            del self.cycle_event[node]
            if node in self.engine_busy:
                return True
            self.cycle(node)
            return True
        if kind == "copy_complete":
            node, batch_id, generation = data
            flow = self.flow.get(node)
            if (
                flow is None
                or flow["batch"] != batch_id
                or flow["generation"] != generation
            ):
                return True
            self.flow.pop(node)
            flow["end"] = self.now
            self.copy_busy[node] -= 1
            for request, gpu, target in flow["items"]:
                self.host[node].pins.subtract(self.rows[request]["hashes"][gpu:target])
            self.copy_batches.append(
                {
                    k: v
                    for k, v in flow.items()
                    if k not in ["remaining", "last", "generation", "rate"]
                }
            )
            self.transfers.append(
                {
                    "id": f"copy-batch-{batch_id}",
                    "request_ids": [r[0] for r in flow["items"]],
                    "resource": f"H2D:{node}",
                    "queued": flow["queued"],
                    "start": flow["start"],
                    "end": self.now,
                    "bytes": flow["blocks"] * BLOCK_BYTES,
                }
            )
            self.loaded[node].append(flow)
            self.ensure_cycle(node)
            if self.copy_queue[node]:
                self.start_copy(node)
            return True
        return False

    def advance_network(self):
        for flow in self.network_flows.values():
            flow["remaining"] = max(
                0, flow["remaining"] - (self.now - flow["last"]) * flow.get("rate", 0)
            )
            flow["last"] = self.now

    def schedule_network(self):
        capacity = (
            self.config["network_Gbps"] * 1e9 / 8
            if "network_Gbps" in self.config
            else 1 / max(self.network_s_per_byte, 1e-15)
        )
        active = set(self.network_flows)
        budgets = {
            endpoint: capacity
            for flow in self.network_flows.values()
            for endpoint in flow["endpoints"]
        }
        rates = dict.fromkeys(active, 0.0)
        while active:
            counts = defaultdict(int)
            for identifier in active:
                for endpoint in self.network_flows[identifier]["endpoints"]:
                    counts[endpoint] += 1
            increments = {
                endpoint: budgets[endpoint] / count
                for endpoint, count in counts.items()
            }
            delta = min(increments.values())
            for identifier in active:
                rates[identifier] += delta
            for endpoint, count in counts.items():
                budgets[endpoint] = max(0, budgets[endpoint] - delta * count)
            saturated = {
                endpoint
                for endpoint, value in increments.items()
                if value <= delta + 1e-6
            }
            active = {
                identifier
                for identifier in active
                if not saturated.intersection(
                    self.network_flows[identifier]["endpoints"]
                )
            }
        for identifier, flow in self.network_flows.items():
            flow["rate"] = rates[identifier]
            flow["generation"] += 1
            self.event(
                self.now + flow["remaining"] / flow["rate"],
                "network_complete",
                flow["id"],
                (identifier, flow["generation"]),
            )

    def run(self):
        result = super().run()
        result["schema"] = "pegaflow-pd-pipeline-simulation/v3"
        result["copy_batches"] = self.copy_batches
        result["settings"]["transfer_model"] = (
            "engine-iteration discovery, asynchronous prefetch, native copy cohorts and completion feedback"
        )
        result["settings"]["idle_poll_s"] = self.idle_poll
        result["settings"]["reserve_restore_pages"] = self.reserve_pages
        result["settings"]["network_sharing"] = self.config.get(
            "network_sharing", "fifo"
        )
        result["settings"]["restore_busy_slowdown"] = self.restore_model.get(
            "busy_slowdown", 1
        )
        outside = sum(
            any(
                batch[key] < low or batch[key] > high
                for key, (low, high) in self.profile.get("restore_coverage", {}).items()
            )
            for batch in self.copy_batches
        )
        result["summary"]["extrapolated_restore_batches"] = outside
        if outside:
            result["coverage"]["extrapolations"].append(
                "Copy cohort exceeds calibrated size or request-count range"
            )
            result["coverage"]["classification"] = "extrapolated"
        if self.restore_model.get("kind") == "capacity" and any(
            self.hosts[n] not in self.config.get("host_copy_GBps", {})
            and str(n) not in self.config.get("device_copy_GBps", {})
            for n in range(self.p_count, self.node_count)
        ):
            result["coverage"]["extrapolations"].append(
                "Destination copy capability uses the profile median; supply host_copy_GBps for calibrated hardware"
            )
            result["coverage"]["classification"] = "extrapolated"
        result["scope"] = (
            "Iteration-aware, condition-calibrated P/D simulation with asynchronous cache handoff"
        )
        result["limitations"][2] = (
            "Copy/compute coupling is selected using calibration role swaps; host-memory and PCIe interference remain approximate"
        )
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = PipelineSimulation(
        json.loads(args.workload.read_text()),
        json.loads(args.profile.read_text()),
        json.loads(args.config.read_text()) if args.config else None,
    ).run()
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result["summary"], indent=2))
