"""CPU candidate: acquire the copy's gates and direction slot together.

The source/destination protection and service times remain conservative. No
device is held while merely waiting for the software direction slot. This is
an execution contract proposal, not a deployed native-server change.
"""

import simulate_continuous_batch  # noqa: F401 - frozen overlay setup
from batch_calendar import schedule_batched
from fair_endpoint_search import FairMemoSimulation
from memo_search import MemoQueues, MemoSnapshot


class JointAcquireSnapshot(MemoSnapshot):
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
                atomic_copies=True,
            )
            self.search_stats["calendar_full"] += 1
        else:
            self.search_stats["calendar_reused"] += 1
        return memo[identity]


class JointAcquireQueues(MemoQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = JointAcquireSnapshot
        return shadow


class JointAcquireSimulation(FairMemoSimulation):
    atomic_copy_acquire = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.mode != "native":
            self.policy_queues = JointAcquireQueues(
                self.models,
                512,
                memoize=True,
                index_projection=True,
                predict_control=False,
                batch_config=self.batch_config,
                progress=self.progress,
            )
            self.policy_queues.started = 0.0

    def run(self):
        result = super().run()
        result["atomic_copy_resource_acquisition"] = self.mode != "native"
        result["source_device_gate_removed"] = False
        return result
