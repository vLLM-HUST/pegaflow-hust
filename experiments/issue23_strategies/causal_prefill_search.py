"""Closed-loop candidate: exact memo plus causal-ready eligible prefill."""

import simulate_continuous_batch  # noqa: F401 - bind retained CPU overlay
from batch_calendar import schedule_batched
from memo_search import MemoQueues, MemoSimulation, MemoSnapshot
from pegaflow.strategies.causal_prefill import (
    CausalPrefillBatcher,
    LaxityPrefillBatcher,
)


class CausalSnapshot(MemoSnapshot):
    batcher_type = CausalPrefillBatcher

    def predict_batch(self, work, now, demands, routes, costs):
        self.phase_costs = costs
        memo = self.__dict__.setdefault("batch_memo", {})
        identity = (
            tuple(work),
            tuple(sorted((key, routes[key].d_blocks) for key in demands)),
        )
        if identity not in memo:
            ready = {w.key: w.offered for w in work}
            memo[identity] = schedule_batched(
                work,
                now,
                self._remaining_operation,
                demands,
                routes,
                costs,
                self.batch_config,
                self.batch_progress,
                batch_factory=lambda config: self.batcher_type(
                    config, ready.__getitem__
                ),
            )
            self.search_stats["calendar_full"] += 1
        else:
            self.search_stats["calendar_reused"] += 1
        return memo[identity]


class CausalQueues(MemoQueues):
    snapshot_type = CausalSnapshot

    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = self.snapshot_type
        return shadow


class CausalSimulation(MemoSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.mode != "native":
            self.d_batches = {
                n: CausalPrefillBatcher(
                    self.batch_config, lambda key: self.jobs[key]["ready"]
                )
                for n in (2, 3)
            }
            self.policy_queues = CausalQueues(
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
        result["eligible_prefill_order"] = (
            "causal-ready" if self.mode != "native" else "API-FIFO"
        )
        return result


class LaxitySnapshot(CausalSnapshot):
    batcher_type = LaxityPrefillBatcher


class LaxityQueues(CausalQueues):
    snapshot_type = LaxitySnapshot


class LaxitySimulation(CausalSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.mode != "native":
            self.d_batches = {
                n: LaxityPrefillBatcher(
                    self.batch_config, lambda key: self.jobs[key]["ready"]
                )
                for n in (2, 3)
            }
            self.policy_queues = LaxityQueues(
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
        if self.mode != "native":
            result["eligible_prefill_order"] = (
                "causal-ready minus predicted remaining first work"
            )
        return result
