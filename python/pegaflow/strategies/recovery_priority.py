"""Rank pending first-token work across restore and prefill boundaries.

Ranks use causal ready time and issued predictions, never the execution world's
service samples. Decode remains batched first; completed saves drain immediately
to release reserved memory. H2D/save remain exclusive and non-preemptible.
"""

import math

from .continuous_batch import ContinuousBatcher, Iteration


class RecoveryPriorityBatcher(ContinuousBatcher):
    def __init__(self, config, ready, predicted):
        super().__init__(config)
        self.ready, self.predicted = ready, predicted
        self.restore_deferrals = 0

    def priority(self, sequence):
        body, restore = self.predicted(sequence.key)
        remaining = (
            body * sequence.remaining_prefill / (sequence.prompt_tokens - sequence.prefix_tokens)
        )
        if sequence.phase == "restore":
            remaining += restore
        return self.ready(sequence.key) - remaining

    def begin(self):
        if self.busy is not None:
            return None
        self.admit()
        saves = [s for s in self.active.values() if s.phase == "save"]
        if saves:
            selected = saves[0]
            self.busy = Iteration("save", selected.save_seconds, ((selected.key, 0, "save"),))
            return self.busy
        pending = [s for s in self.active.values() if s.phase in ("restore", "prefill")]
        if pending:
            selected = min(pending, key=self.priority)
            if selected.phase == "restore":
                self.busy = Iteration(
                    "restore", selected.restore_seconds, ((selected.key, 0, "restore"),)
                )
                return self.busy
            self.restore_deferrals += any(s.phase == "restore" for s in pending)
        original = self.active
        self.active = dict(sorted(original.items(), key=lambda item: self.priority(item[1])))
        try:
            return super().begin(eligible=lambda key: original[key].phase != "restore")
        finally:
            self.active = original

    def stable_repeats(self, iteration, upper):
        selected = [(k, n) for k, n, phase in iteration.pieces if phase == "prefill"]
        if not selected:
            return upper
        if len(selected) != 1:
            return 1
        key, count = selected[0]
        sequence = self.active[key]
        competitors = [
            self.priority(s)
            for k, s in self.active.items()
            if k != key and s.phase in ("restore", "prefill")
        ]
        if not competitors:
            return upper
        body, _ = self.predicted(key)
        step = body * count / (sequence.prompt_tokens - sequence.prefix_tokens)
        if step <= 0:
            return upper
        return min(upper, max(1, math.ceil((min(competitors) - self.priority(sequence)) / step)))


class RecoveryDeadlineBatcher(RecoveryPriorityBatcher):
    """EDD across restore/prefill; the common TTFT target cancels from deadlines.

    Single-machine all-ready EDD motivates this rule. Memory, decode and distributed
    dependencies prevent extending that optimality result to this serving system.
    """

    def priority(self, sequence):
        return self.ready(sequence.key)

    def stable_repeats(self, iteration, upper):
        return upper
