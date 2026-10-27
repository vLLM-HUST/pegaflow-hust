"""Drain one predicted release predecessor when known work cannot get a D slot.

Pending-first visitation remains unless the D FIFO is blocked and every admitted
sequence has its first token. This is a local earliest-release rule, not a global
p99 optimum. No sampled world service duration participates in the selection.
"""

from pending_first_search import (
    PendingFirstBatcher,
    PendingFirstQueues,
    PendingFirstSimulation,
    PendingFirstSnapshot,
)


def release_estimate(sequence, costs, node):
    tail = costs.decode_tail(node, sequence.prompt_tokens)
    save = min(tail, costs.fits[node][4])
    decode = max(0.0, tail - save)
    remaining = sequence.output_tokens - sequence.decoded
    return decode * remaining / max(1, sequence.output_tokens - 1) + save


class SlotReleaseBatcher(PendingFirstBatcher):
    def __init__(self, config, context_reader=None):
        super().__init__(config)
        self.context_reader = context_reader
        self.blocked = ()
        self.costs = None
        self.node = None
        self.release_choices = []

    def set_context(self, blocked, costs, node):
        self.blocked, self.costs, self.node = blocked, costs, node

    def release_cohort(self, selected, choices):
        return (selected,)

    def begin(self, eligible=None):
        if self.busy is not None:
            return None
        self.admit()
        if self.context_reader is not None:
            self.set_context(*self.context_reader())
        # An observed full slot set must consist entirely of post-first decode.
        # Exclusive restore/save or a pending first token retain the parent rule.
        if (
            not self.blocked
            or len(self.active) != self.config.max_sequences
            or any(s.phase != "decode" for s in self.active.values())
        ):
            return super().begin(eligible)
        choices = [
            s for s in self.active.values() if eligible is None or eligible(s.key)
        ]
        if not choices:
            return None
        estimates = {s.key: release_estimate(s, self.costs, self.node) for s in choices}
        # Stable FIFO tie breaking; request identifiers are not policy features.
        selected = min(choices, key=lambda s: estimates[s.key]).key
        cohort = self.release_cohort(selected, choices)
        self.release_choices.append(
            {
                "selected": selected,
                "blocked": tuple(self.blocked),
                "remaining_estimates": estimates,
                "decoded": {s.key: s.decoded for s in choices},
                "model_latest": self.costs.latest_completed_seconds,
                "cohort": cohort,
            }
        )
        return super().begin(lambda key: key in cohort)


class SlotReleaseSnapshot(PendingFirstSnapshot):
    def configure_projection(self, projection, now):
        options = super().configure_projection(projection, now)
        options["batch_factory"] = SlotReleaseBatcher
        options["batch_context"] = lambda node, batch, queued: batch.set_context(
            queued, projection.costs, node
        )
        return options


class SlotReleaseQueues(PendingFirstQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = SlotReleaseSnapshot
        return shadow


class SlotReleaseSimulation(PendingFirstSimulation):
    queue_type = SlotReleaseQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.release_observations = []
        if self.mode != "native":
            self.d_batches = {
                node: SlotReleaseBatcher(
                    self.batch_config, lambda node=node: self.release_context(node)
                )
                for node in (2, 3)
            }

    def release_context(self, node):
        blocked = tuple(self.queues[node])
        batch = self.d_batches[node]
        needed = (
            blocked
            and len(batch.active) == self.batch_config.max_sequences
            and all(s.phase == "decode" for s in batch.active.values())
        )
        # Freeze only for a productive choice; this CPU is inside the inherited
        # timed begin_iteration, and is charged to the device timeline once.
        costs = self.frozen() if needed else None
        if costs is not None and costs.latest_completed_seconds > self.now:
            raise ValueError("Future release estimate")
        return blocked, costs, node

    def begin_iteration(self, node, batch):
        before = len(getattr(batch, "release_choices", ()))
        step = super().begin_iteration(node, batch)
        if len(getattr(batch, "release_choices", ())) > before:
            self.release_observations.append(
                {"origin": self.now, "node": node, **batch.release_choices[-1]}
            )
        return step

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "slot-release-first-01"
        result["slot_release_choices"] = self.release_observations
        result["release_rule_world_durations_read"] = False
        return result
