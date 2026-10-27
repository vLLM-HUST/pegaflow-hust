"""Executable, scoped paper adapters in the common causal CPU serving world.

Only current cache/owned work and past-completed fits are policy inputs. Native
H2D, sync save, full decode, copies, capacity and measured CPU costs stay paid.
"""

import math
import time
from collections import deque
from dataclasses import replace

from pegaflow.joint_policy import Calendar, copy_package, evaluate_route
from pegaflow.resource_cache_profile import CacheProfileQueues
from pegaflow.strategies.baselines import MemServeStrategy
from pegaflow.strategies.model import Request, Snapshot
from pegaflow.strategies.mooncake import MooncakePrefillStrategy
from pegaflow.strategies.resource_forecast import candidate_work
from pegaflow.strategies.strata import BatchDemand, balanced_batch
from timed_recovery_search import TimedMemoQueues, TimedMemoSimulation


class PaperQueues(TimedMemoQueues):
    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
        # These paper adapters reuse the largest safely available published
        # context, not Tailguard's global tail-minimizing boundary search.
        demand = self.demands[key]
        local = layout.prefix(route.d, demand.hashes)
        limit = max(local, layout.prefix(route.p, demand.hashes))
        for length in range(limit, -1, -1):
            if length > local:
                try:
                    copy_package(layout, route.p, route.d, demand.hashes[:length])
                except ValueError:
                    continue
            selected = replace(route, d_blocks=length, d_headroom=False)
            self.admission_choices[key, route.d] = False
            CacheProfileQueues.revise(self, key, selected, layout, costs)
            self.progress_version += 1
            self.search_records.append(
                {"kind": "paper-max-safe-boundary", "key": key, "blocks": length}
            )
            return length, costs.compute(
                route.d, demand.tokens, length * demand.block_tokens
            )
        raise ValueError("Missing safe recompute boundary")


class PaperRoutingSimulation(TimedMemoSimulation):
    queue_type = PaperQueues
    paper_name = "memserve"

    def feasible_candidates(self, row, demand, costs):
        result = {}
        for plan in self.candidates(row, costs):
            try:
                raw = evaluate_route(
                    demand,
                    self.layout,
                    costs,
                    Calendar(),
                    plan.p,
                    plan.d,
                    plan.source,
                    plan.p_prefix // 128,
                    plan.d_prefix // 128,
                    published_handoff=True,
                )
            except ValueError:
                # Match the controlled dispatcher's staging contract before
                # applying the paper objective, including safe D recompute.
                continue
            result[plan] = raw
        return result

    def choose(self, row, ready):
        if self.mode == "native":
            return super().choose(row, ready)
        began = time.perf_counter()
        costs = self.frozen()
        demand = replace(self.item(row), arrival=self.now)
        queues = self.policy_queues
        with queues.decision(demand, self.layout, costs, ready, self.now):
            shadow = queues.local_decision.shadow
            shadow.refresh(self.layout, costs)
            prediction = shadow.predict_batch(
                tuple(shadow.work.values()),
                self.now,
                shadow.demands,
                shadow.routes,
                costs,
            )
            workers = []
            for worker in self.workers(row, costs):
                assigned = [
                    w
                    for w in shadow.work.values()
                    if (w.p == worker.node and w.p_done is None)
                    or (w.d == worker.node and worker.node >= 2)
                ]
                endpoint = "P_end" if worker.node < 2 else "full"
                release = max(
                    (prediction[endpoint][w.key] for w in assigned), default=self.now
                )
                # D load includes already assigned future arrivals as well as
                # queued/active requests; no future workload is inspected.
                workers.append(
                    replace(
                        worker,
                        queue_service_seconds=max(0.0, release - self.now),
                        queued_requests=len(assigned),
                        active_sequences=0,
                    )
                )
            raw_plans = self.feasible_candidates(row, demand, costs)
            plans = tuple(raw_plans)
            snapshot = Snapshot(
                Request(row["id"], row["task"], row["tokens"], ready, self.sequence),
                self.now,
                tuple(workers),
                plans,
                frozenset((row["id"],)),
            )
            strategy = (
                MooncakePrefillStrategy(balancing_ratio=self.balancing_ratio)
                if self.paper_name == "mooncake"
                else MemServeStrategy()
            )
            decision = strategy.choose(snapshot)
            plan = decision.candidate
            raw = raw_plans[plan]
            route = queues.forecast(raw, self.now)
            queues.reserve(row["id"], route, self.now)
        seconds = time.perf_counter() - began
        self.planning.append(
            {
                "id": row["id"],
                "CPU_wall": seconds,
                "origin": self.now,
                "model_latest": costs.latest_completed_seconds,
                "candidates": decision.candidates_evaluated,
                "paper_adapter": decision.provenance,
                "worker_load": [w.__dict__ for w in workers],
            }
        )
        return plan, candidate_work(snapshot, plan), seconds, route

    def run(self):
        result = super().run()
        result["paper_adapter"] = {
            "name": self.paper_name,
            "full_system_reproduction": False,
            "D_boundary": "largest currently published feasible prefix",
            "cache_allocator": "common PegaFlow host authority",
            "world_costs_unchanged": True,
            "balancing_ratio": getattr(self, "balancing_ratio", None),
        }
        return result


class MooncakeSimulation(PaperRoutingSimulation):
    paper_name = "mooncake"
    balancing_ratio = 1.0  # Declared no-extra-margin scenario, not a paper default.


class MemServeSimulation(PaperRoutingSimulation):
    paper_name = "memserve"


class StrataSimulation(TimedMemoSimulation):
    """Algorithm 1 admission adapter + native RR/cache, on exclusive I/O.

    Delay-hit waiting, GPU shared contexts, layer overlap, GPU-assisted copy
    and storage policies are not emulated. Do not call this the Strata system.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.mode != "native":
            raise ValueError("Strata admission adapter uses declared native RR routing")
        self.admission_records, self.admission_pending = [], {}

    def admission_ready(self, node, free_slots):
        if node < 2 or free_slots <= 0 or not self.queues[node]:
            return True
        if node in self.admission_pending:
            if self.now < self.admission_pending[node]:
                return False
            del self.admission_pending[node]
            return True
        began = time.perf_counter()
        queued = list(self.queues[node])
        demands = []
        config = self.batch_config
        for key in queued:
            item = self.item(self.jobs[key]["row"])
            prefix = (
                max(self.layout.prefix(n, item.hashes) for n in range(4))
                * config.block_tokens
            )
            reservation = (
                math.ceil((item.tokens + 128) / config.block_tokens)
                * config.block_tokens
                * config.bytes_per_token
            )
            demands.append(
                BatchDemand(
                    key,
                    prefix,
                    item.tokens - prefix,
                    reservation,
                    item.hashes[: prefix // config.block_tokens],
                )
            )
        occupied = sum(
            math.ceil((self.jobs[k]["row"]["tokens"] + 128) / config.block_tokens)
            * config.block_tokens
            * config.bytes_per_token
            for k, n in self.d_active.items()
            if n == node
        )
        selected = balanced_batch(
            demands, free_slots, config.kv_bytes - occupied, loading_ratio=100.0
        )
        if not selected:
            return False
        self.queues[node] = deque(
            (*selected, *(k for k in queued if k not in selected))
        )
        seconds = time.perf_counter() - began
        self.admission_records.append(
            {
                "node": node,
                "origin": self.now,
                "completed": self.now + seconds,
                "CPU_wall": seconds,
                "queued": queued,
                "selected": selected,
                "reordered": tuple(queued[: len(selected)]) != selected,
                "loading_ratio": 100.0,
            }
        )
        if seconds == 0:
            # A zero-cost mechanism replay must not create an extra event
            # that lets an old batch jump ahead of unchanged FIFO admission.
            return True
        self.admission_pending[node] = self.now + seconds
        self.event(self.now + seconds, "admission", node)
        return False

    def begin_iteration(self, node, batch):
        # Admission and iteration construction share the D scheduler. It
        # cannot submit a fresh old-only iteration while its next admission
        # decision is still being computed. Already running work continues.
        if node in self.admission_pending:
            return None
        return super().begin_iteration(node, batch)

    def run(self):
        result = super().run()
        result["strata_admission"] = self.admission_records
        result["admission_CPU_wall"] = sum(
            r["CPU_wall"] for r in self.admission_records
        )
        result["paper_adapter"] = {
            "name": "strata-balanced-admission-only",
            "full_system_reproduction": False,
            "loading_ratio": 100,
            "ratio_profiled_on_910B2": False,
            "GPU_IO_or_overlap_speedup": False,
            "shared_GPU_prefix_or_delay_hit": False,
            "publication_and_save": "unchanged PegaFlow",
            "routing": "native RR",
        }
        return result
