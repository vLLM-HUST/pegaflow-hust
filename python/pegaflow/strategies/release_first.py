"""Drain completed synchronous saves before new restores; preserve FIFO compute.

No admission quota or penalty coefficient: a completed save releases capacity,
leases and a causal session dependency. This is a candidate, not an optimum.
"""

from .continuous_batch import ContinuousBatcher, Iteration


class ReleaseFirstBatcher(ContinuousBatcher):
    def __init__(self, config, ready=None, predicted=None):
        super().__init__(config)
        self.release_priorities = 0

    def begin(self, eligible=None):
        if self.busy is not None:
            return None
        self.admit()
        saves = [
            s
            for s in self.active.values()
            if s.phase == "save" and (eligible is None or eligible(s.key))
        ]
        if saves:
            selected = saves[0]
            self.release_priorities += any(s.phase == "restore" for s in self.active.values())
            self.busy = Iteration("save", selected.save_seconds, ((selected.key, 0, "save"),))
            return self.busy
        return super().begin(eligible)
