"""Do not let a later restore overtake already ready FIFO prefill.

The device is still exclusive, existing work is never preempted, and save keeps
its original priority. This removes a phase-order inversion, not H2D cost.
"""

from prefix_reclaim_search import PrefixReclaimSimulation
from release_cobatch_search import ReleaseCobatchBatcher


class ReadyPrefillBatcher(ReleaseCobatchBatcher):
    def __init__(self, config, context_reader=None, observation_reader=None):
        super().__init__(config, context_reader)
        self.observation_reader = observation_reader
        self.restore_inversions = []

    def begin(self, eligible=None):
        if self.busy is not None:
            return None
        self.admit()
        sequences = [
            s for s in self.active.values() if eligible is None or eligible(s.key)
        ]
        older_prefill = []
        for sequence in sequences:
            if sequence.phase == "prefill":
                older_prefill.append(sequence.key)
            if sequence.phase in ("restore", "save"):
                if sequence.phase == "restore" and older_prefill:
                    # Only advance work already ahead in the admitted FIFO.
                    # No service duration, task ID feature or fitted margin is read.
                    selected = tuple(older_prefill)
                    step = super().begin(lambda key, selected=selected: key in selected)
                    if self.observation_reader is not None:
                        self.restore_inversions.append(
                            {
                                **self.observation_reader(),
                                "eligible_phases": [
                                    (s.key, s.phase) for s in sequences
                                ],
                                "deferred_restore": sequence.key,
                                "older_prefill": selected,
                                "selected_pieces": step.pieces,
                            }
                        )
                    return step
                break
        return super().begin(eligible)


class ReadyPrefillSimulation(PrefixReclaimSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.mode != "native":
            self.policy_queues.forecast_batch_factory = ReadyPrefillBatcher
            self.d_batches = {
                n: ReadyPrefillBatcher(
                    self.batch_config,
                    lambda n=n: self.release_context(n),
                    lambda n=n: {"origin": self.now, "node": n},
                )
                for n in (2, 3)
            }

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "ready-prefill-before-restore-01"
        result["restore_inversions"] = [
            r
            for b in self.d_batches.values()
            for r in getattr(b, "restore_inversions", [])
        ]
        result["restore_inversion_rule"] = {
            "only_older_ready_prefill": True,
            "save_priority_unchanged": True,
            "running_work_nonpreemptible": True,
            "future_durations_read": False,
            "cost_accounting": "existing measured iteration scheduler and full resource work",
        }
        if self.mode != "native":
            assert all(
                q["forecast_batch_factory"] == "ReadyPrefillBatcher"
                for q in result["initial_cache_forecasts"].values()
            )
        return result
