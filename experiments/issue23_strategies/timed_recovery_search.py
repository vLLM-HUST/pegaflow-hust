"""Charge productive D scheduling CPU wall in both arms' device timelines.

Same convention for FIFO reference and recovery-aware candidate. Empty simulator
polls are not serving decisions. A decision's cost enters later forecasts only
after its modeled CPU completion. No historical test label is supplied online.
"""

import time
from dataclasses import replace

from fair_endpoint_search import FairMemoSimulation
from memo_search import MemoQueues
from pegaflow.strategies.recovery_priority import RecoveryDeadlineBatcher
from pegaflow.strategies.release_first import ReleaseFirstBatcher
from recovery_priority_search import (
    RecoveryPriorityQueues,
    RecoveryPrioritySimulation,
    RecoveryPrioritySnapshot,
)


class TimedQueuesMixin:
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.iteration_overhead = self.iteration_overhead_reader()
        return shadow


class TimedMemoQueues(TimedQueuesMixin, MemoQueues):
    pass


class TimedRecoveryQueues(TimedQueuesMixin, RecoveryPriorityQueues):
    pass


class IterationAccountingMixin:
    queue_type = TimedMemoQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.iteration_cpu = []
        if self.mode != "native":
            self.policy_queues = self.queue_type(
                self.models,
                512,
                memoize=True,
                index_projection=True,
                predict_control=False,
                batch_config=self.batch_config,
                progress=self.progress,
            )
            self.policy_queues.started = 0.0
            self.policy_queues.iteration_overhead_reader = (
                self.completed_iteration_overhead
            )
            self.policy_queues.issued_phase_reader = lambda: self.issued_phases

    def completed_iteration_overhead(self):
        result = {}
        for node in (2, 3):
            rows = [
                r["seconds"]
                for r in self.iteration_cpu
                if r["node"] == node and r["completed"] <= self.now
            ]
            result[node] = sum(rows) / len(rows) if rows else 0.0
        return result

    def begin_iteration(self, node, batch):
        began = time.perf_counter()
        step = super().begin_iteration(node, batch)
        seconds = time.perf_counter() - began
        if step is None:
            return None
        self.iteration_cpu.append(
            {
                "node": node,
                "origin": self.now,
                "completed": self.now + seconds,
                "seconds": seconds,
                "kind": step.kind,
                "requests": [p[0] for p in step.pieces],
            }
        )
        step = replace(step, seconds=step.seconds + seconds)
        batch.busy = step
        return step

    def choose(self, row, ready):
        began = time.perf_counter()
        plan, work, cpu, route = super().choose(row, ready)
        if route is not None:
            self.planning[-1]["initial_forecast"] = {
                "p": route.p,
                "d": route.d,
                "source": route.source,
                "P_prefix_command": route.p_blocks * 128,
                "D_prefix_command": route.d_blocks * 128,
                "P_end": route.predicted_p_end,
                "first": route.predicted_first,
                "full": route.predicted_end,
                "origin": route.forecast_origin_seconds,
                "latest_completed": route.model_latest_completed_seconds,
                "not_after_P_revised_forecast": True,
            }
        cpu = time.perf_counter() - began
        self.planning[-1]["CPU_wall"] = cpu
        return plan, work, cpu, route

    def run(self):
        result = super().run()
        result["iteration_scheduler_CPU"] = self.iteration_cpu
        result["resource_operation_ledger"] = list(self.policy_queues.operation_events)
        result["initial_cache_forecasts"] = dict(self.policy_queues.selected_quotes)
        result["iteration_scheduler_wall_charged"] = sum(
            r["seconds"] for r in self.iteration_cpu
        )
        result["iteration_scheduler_accounting"] = (
            "productive decision wall delays device submission; included in TTFT, not added again"
        )
        return result


class TimedMemoSimulation(IterationAccountingMixin, FairMemoSimulation):
    pass


class TimedRecoverySimulation(IterationAccountingMixin, RecoveryPrioritySimulation):
    queue_type = TimedRecoveryQueues


class RecoveryDeadlineSnapshot(RecoveryPrioritySnapshot):
    batcher_type = RecoveryDeadlineBatcher


class TimedDeadlineQueues(TimedRecoveryQueues):
    snapshot_type = RecoveryDeadlineSnapshot


class TimedDeadlineSimulation(TimedRecoverySimulation):
    batcher_type = RecoveryDeadlineBatcher
    queue_type = TimedDeadlineQueues

    def run(self):
        result = super().run()
        result["recovery_priority"] = (
            "causal-ready deadline across restore and prefill; release saves first"
        )
        return result


class ReleaseFirstSnapshot(RecoveryPrioritySnapshot):
    batcher_type = ReleaseFirstBatcher


class TimedReleaseQueues(TimedRecoveryQueues):
    snapshot_type = ReleaseFirstSnapshot


class TimedReleaseSimulation(TimedRecoverySimulation):
    batcher_type = ReleaseFirstBatcher
    queue_type = TimedReleaseQueues

    def run(self):
        result = super().run()
        result["recovery_priority"] = (
            "completed synchronous saves first; unchanged FIFO restore/compute"
        )
        result["release_priorities"] = {
            n: getattr(b, "release_priorities", 0) for n, b in self.d_batches.items()
        }
        return result
