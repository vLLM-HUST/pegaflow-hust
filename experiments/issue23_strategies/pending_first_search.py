"""TTFT-first compute visitation; full decode and synchronous phases remain.

No preemption of running iterations, no free batch speedup, no balancing quota.
Only already admitted prefill can precede later decode tokens; all outstanding
decode still reserves memory/slots and must drain before session release.
"""

from acquired_phase_search import (
    AcquiredPhaseQueues,
    AcquiredPhaseSimulation,
    AcquiredPhaseSnapshot,
)
from pegaflow.strategies.continuous_batch import ContinuousBatcher


class PendingFirstBatcher(ContinuousBatcher):
    compute_phase_order = ("prefill", "decode")


class PendingFirstSnapshot(AcquiredPhaseSnapshot):
    def configure_projection(self, projection, now):
        options = super().configure_projection(projection, now)
        options["batch_factory"] = PendingFirstBatcher
        return options


class PendingFirstQueues(AcquiredPhaseQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = PendingFirstSnapshot
        return shadow


class PendingFirstSimulation(AcquiredPhaseSimulation):
    queue_type = PendingFirstQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.mode != "native":
            self.d_batches = {n: PendingFirstBatcher(self.batch_config) for n in (2, 3)}

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "pending-first-compute-01"
        result["compute_phase_order"] = (
            ["prefill", "decode"] if self.mode != "native" else ["decode", "prefill"]
        )
        result["full_decode_drain_required"] = True
        result["decode_ITL_improvement_claimed"] = False
        return result
