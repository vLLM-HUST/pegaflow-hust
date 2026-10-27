"""Eligible prefill ordered by its causal-ready deadline, not API arrival.

For a common prospective TTFT target J, deadline_r = ready_r + J; earliest
deadline order thus needs no fitted J. Decode, KV admission and I/O barriers
retain their existing semantics. This local EDD rule is not a global p99 proof.
"""

from .continuous_batch import ContinuousBatcher


class CausalPrefillBatcher(ContinuousBatcher):
    def __init__(self, config, ready):
        super().__init__(config)
        self.ready = ready

    def priority(self, sequence):
        return self.ready(sequence.key)

    def stable_repeats(self, iteration, upper):
        return upper

    def begin(self):
        if self.busy is not None:
            return None
        self.admit()
        if any(s.phase in ("restore", "save") for s in self.active.values()):
            return super().begin()
        original = self.active
        # Temporary iteration visitation only. Preserve insertion order for
        # admission, restore/save and future ties; do not bypass capacity FIFO.
        self.active = dict(sorted(original.items(), key=lambda item: self.priority(item[1])))
        try:
            return super().begin()
        finally:
            self.active = original


class LaxityPrefillBatcher(CausalPrefillBatcher):
    """Common TTFT deadline minus predicted remaining prefill; J cancels."""

    def priority(self, sequence):
        remaining = (
            sequence.first_seconds
            * sequence.remaining_prefill
            / (sequence.prompt_tokens - sequence.prefix_tokens)
        )
        return self.ready(sequence.key) - remaining

    def stable_repeats(self, iteration, upper):
        import math

        selected = [(key, count) for key, count, phase in iteration.pieces if phase == "prefill"]
        if not selected:
            return upper
        if len(selected) > 1:
            return 1
        key, count = selected[0]
        current = self.active[key]
        competitors = [
            self.priority(s)
            for other, s in self.active.items()
            if other != key and s.phase == "prefill"
        ]
        if not competitors:
            return upper
        step = current.first_seconds * count / (current.prompt_tokens - current.prefix_tokens)
        if not step:
            return upper
        # Replay before a priority crossing; ties are replayed conservatively.
        return min(upper, max(1, math.ceil((min(competitors) - self.priority(current)) / step)))
