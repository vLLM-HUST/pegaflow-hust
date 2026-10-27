"""CPU candidate: reserve a pending copy's turn without partial gate ownership.

An older eligible host-copy operation can stop younger work from repeatedly
reacquiring its endpoints. Older existing decode work and unrelated resources
still progress. No gate is recorded held until the complete acquisition succeeds.
"""

import simulate_continuous_batch  # noqa: F401 - retained CPU overlay
from batch_calendar import schedule_batched
from fair_endpoint_search import FairMemoSimulation
from memo_search import MemoQueues, MemoSnapshot
from pegaflow.resource_queue import direction
from pegaflow.strategies.resource_reservation import copy_reservations, permitted


class ReservationSnapshot(MemoSnapshot):
    def predict_batch(self, work, now, demands, routes, costs):
        self.phase_costs = costs
        memo = self.__dict__.setdefault("batch_memo", {})
        identity = (
            tuple(work),
            tuple(sorted((key, routes[key].d_blocks) for key in demands)),
        )
        if identity not in memo:
            memo[identity] = schedule_batched(
                work,
                now,
                self._remaining_operation,
                demands,
                routes,
                costs,
                self.batch_config,
                self.batch_progress,
                reserve_copies=True,
            )
            self.search_stats["calendar_full"] += 1
        else:
            self.search_stats["calendar_reused"] += 1
        return memo[identity]


class ReservationQueues(MemoQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = ReservationSnapshot
        return shadow


class ReservationSimulation(FairMemoSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reservations = {}
        self.reservation_blocks = []
        if self.mode != "native":
            self.policy_queues = ReservationQueues(
                self.models,
                512,
                memoize=True,
                index_projection=True,
                predict_control=False,
                batch_config=self.batch_config,
                progress=self.progress,
            )
            self.policy_queues.started = 0.0

    def prepare_dispatch(self, owned):
        pending = []
        if self.mode != "native":
            for node, key in owned:
                job = self.jobs[key]
                if job.get("running") or not job["steps"]:
                    continue
                copy = job["steps"][0][3]
                if copy:
                    resources = (
                        f"gate{copy[0]}",
                        f"gate{node}",
                        "link:" + direction(copy[0], node),
                    )
                    pending.append((job["work"].order, resources))
        self.reservations = copy_reservations(pending)

    def operation_permitted(self, key, resources):
        allowed = permitted(
            resources,
            self.jobs[key]["work"].order,
            self.reservations,
            acquired=bool(self.jobs[key].get("copy_gate_hold")),
        )
        if not allowed:
            self.reservation_blocks.append(
                {
                    "at": self.now,
                    "request": key,
                    "resources": resources,
                    "reservations": dict(self.reservations),
                    "partial_device_ownership": False,
                }
            )
        return allowed

    def begin_iteration(self, node, batch):
        return batch.begin(
            eligible=lambda key: permitted(
                (f"gate{node}",), self.jobs[key]["work"].order, self.reservations
            )
        )

    def run(self):
        result = super().run()
        result["resource_reservation_blocks"] = self.reservation_blocks
        result["copy_no_overtaking_reservations"] = True
        return result
