"""Causal scheduling of device-exclusive restore versus pending prefill.

Only the candidate changes I/O/compute order. Both arms retain the same physical
service world and all capacity, lease, publication and complete-decode costs.
"""

import simulate_continuous_batch  # noqa: F401
from batch_calendar import schedule_batched
from fair_endpoint_search import FairMemoSimulation
from memo_search import MemoQueues, MemoSnapshot
from pegaflow.strategies.recovery_priority import RecoveryPriorityBatcher


def predicted_phases(costs, node, tokens, prefix):
    restore = costs.restore(node, prefix)
    return max(0.0, costs.compute(node, tokens, prefix) - restore), restore


class RecoveryPrioritySnapshot(MemoSnapshot):
    batcher_type = RecoveryPriorityBatcher

    def predict_batch(self, work, now, demands, routes, costs):
        self.phase_costs = costs
        memo = self.__dict__.setdefault("batch_memo", {})
        identity = (
            tuple(work),
            tuple(sorted((key, r.d_blocks) for key, r in routes.items())),
        )
        if identity not in memo:
            ready = {w.key: w.offered for w in work}
            quoted = {
                key: predicted_phases(
                    costs,
                    routes[key].d,
                    d.tokens,
                    routes[key].d_blocks * d.block_tokens,
                )
                for key, d in demands.items()
            }
            for w in work:
                if "D_api" in dict(w.active_operations) and w.key in self.issued_phases:
                    quoted[w.key] = self.issued_phases[w.key]
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
                    config, ready.__getitem__, quoted.__getitem__
                ),
                iteration_overhead=getattr(self, "iteration_overhead", None),
            )
            self.search_stats["calendar_full"] += 1
        else:
            self.search_stats["calendar_reused"] += 1
        return memo[identity]


class RecoveryPriorityQueues(MemoQueues):
    snapshot_type = RecoveryPrioritySnapshot

    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = self.snapshot_type
        shadow.issued_phases = dict(self.issued_phase_reader())
        return shadow


class RecoveryPrioritySimulation(FairMemoSimulation):
    batcher_type = RecoveryPriorityBatcher

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.issued_phases, self.phase_quotes = {}, []
        if self.mode != "native":
            self.d_batches = {
                n: self.batcher_type(
                    self.batch_config,
                    lambda k: self.jobs[k]["ready"],
                    self.issued_phases.__getitem__,
                )
                for n in (2, 3)
            }
            self.policy_queues = RecoveryPriorityQueues(
                self.models,
                512,
                memoize=True,
                index_projection=True,
                predict_control=False,
                batch_config=self.batch_config,
                progress=self.progress,
            )
            self.policy_queues.started = 0.0
            self.policy_queues.issued_phase_reader = lambda: self.issued_phases

    def batch_submitted(self, key, node, sequence):
        if self.mode == "native":
            return
        costs = self.frozen()
        if costs.latest_completed_seconds > self.now:
            raise ValueError("Future phase fit in iteration ordering")
        quote = predicted_phases(
            costs, node, sequence.prompt_tokens, sequence.prefix_tokens
        )
        self.issued_phases[key] = quote
        self.phase_quotes.append(
            {
                "key": key,
                "node": node,
                "origin": self.now,
                "latest_completed": costs.latest_completed_seconds,
                "prefix": sequence.prefix_tokens,
                "predicted_body": quote[0],
                "predicted_restore": quote[1],
                "world_service_read": False,
            }
        )

    def run(self):
        result = super().run()
        result["iteration_phase_quotes"] = self.phase_quotes
        result["restore_deferrals"] = {
            n: getattr(b, "restore_deferrals", 0) for n, b in self.d_batches.items()
        }
        result["recovery_priority"] = (
            "ready minus issued remaining restore and first work"
        )
        return result
