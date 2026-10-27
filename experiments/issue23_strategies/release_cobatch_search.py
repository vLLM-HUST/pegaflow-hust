"""Preserve predicted release speed without throwing away shared decode work."""

from slot_release_search import (
    SlotReleaseBatcher,
    SlotReleaseQueues,
    SlotReleaseSimulation,
    SlotReleaseSnapshot,
)


def decode_step_cost(sequence, costs, node):
    tail = costs.decode_tail(node, sequence.prompt_tokens)
    save = min(tail, costs.fits[node][4])
    return max(0.0, tail - save) / max(1, sequence.output_tokens - 1)


def shared_step_cost(values, fraction):
    return (1 - fraction) * sum(values) + fraction * max(values, default=0.0)


class ReleaseCobatchBatcher(SlotReleaseBatcher):
    def release_cohort(self, selected, choices):
        costs = {s.key: decode_step_cost(s, self.costs, self.node) for s in choices}
        cohort = [selected]
        values = [costs[selected]]
        cap = shared_step_cost(values, self.config.shared_fraction)
        # Admit only predicted non-extending work, in stable FIFO order. There
        # is no epsilon/gain threshold or request/rate-dependent parameter.
        for s in choices:
            if s.key == selected or len(cohort) >= self.config.token_budget:
                continue
            extended = [*values, costs[s.key]]
            if shared_step_cost(extended, self.config.shared_fraction) <= cap:
                cohort.append(s.key)
                values = extended
        return tuple(cohort)


class ReleaseCobatchSnapshot(SlotReleaseSnapshot):
    def configure_projection(self, projection, now):
        options = super().configure_projection(projection, now)
        options["batch_factory"] = ReleaseCobatchBatcher
        return options


class ReleaseCobatchQueues(SlotReleaseQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = ReleaseCobatchSnapshot
        return shadow


class ReleaseCobatchSimulation(SlotReleaseSimulation):
    queue_type = ReleaseCobatchQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.mode != "native":
            self.d_batches = {
                node: ReleaseCobatchBatcher(
                    self.batch_config, lambda node=node: self.release_context(node)
                )
                for node in (2, 3)
            }

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "release-preserving-cobatch-01"
        return result
