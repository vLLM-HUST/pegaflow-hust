"""Use the existing eligible-prefix invariant, not hashed victim counts.

CacheForecast.save selects the first eligible owners in order until its native
reclaim-batch count is met. Removing that prefix needs no hashing or repeated
search. A caller that violates this structural precondition takes the retained
general first-equal-removal implementation; no measured threshold is involved.
"""

from numerical_calendar_search import NumericalCalendarSimulation
from pegaflow.resource_cache_profile import CacheForecast


def prefix_remove_victims(units, victims):
    index = 0
    kept = []
    for unit in units:
        if index < len(victims) and not unit.protected and not unit.virtual:
            if unit is not victims[index]:
                return CacheForecast.remove_victims(list(units), victims)
            index += 1
        else:
            kept.append(unit)
    if index != len(victims):
        return CacheForecast.remove_victims(list(units), victims)
    return kept


class PrefixReclaimSimulation(NumericalCalendarSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy_queues.forecast_reclaim_remover = prefix_remove_victims

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "prefix-reclaim-calendar-02"
        cache_type = getattr(self.policy_queues, "forecast_cache_type", None)
        expected = getattr(cache_type, "reclaim_method", "prefix_remove_victims")
        if self.mode != "native" and any(
            q["reclaim_removal_method"] != expected
            for q in result["initial_cache_forecasts"].values()
        ):
            raise ValueError("Selected forecast did not use prefix reclaim")
        result["forecast_reclaim"] = {
            "policy": "unchanged source-derived eligible prefix and batch",
            "implementation": "one identity scan, legacy fallback outside precondition",
            "physical_execution": "unchanged",
            "cost_accounting": "existing measured control lane; matrix records diagnostic overrides",
        }
        return result
