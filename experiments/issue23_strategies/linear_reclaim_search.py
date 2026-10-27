"""Equivalent forecast victim removal; no change to physical reclamation policy.

Count equal victims and consume their earliest occurrences in one ordered scan,
preserving legacy first-equal removal even for equal-valued or aliased owners.
This avoids repeated list scans/shifts, with unchanged owner accounting.
"""

from collections import Counter

from numerical_calendar_search import NumericalCalendarSimulation


def linear_remove_victims(units, victims):
    remaining = Counter(victims)
    kept = []
    for unit in units:
        if remaining.get(unit, 0):
            remaining[unit] -= 1
        else:
            kept.append(unit)
    if any(remaining.values()):
        raise ValueError("Reclaim victim missing from inventory")
    return kept


class LinearReclaimSimulation(NumericalCalendarSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy_queues.forecast_reclaim_remover = linear_remove_victims

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "linear-reclaim-calendar-01"
        if self.mode != "native" and any(
            q["reclaim_removal_method"] != "linear_remove_victims"
            for q in result["initial_cache_forecasts"].values()
        ):
            raise ValueError("Selected forecast did not use linear reclaim")
        result["forecast_reclaim"] = {
            "policy": "unchanged source-derived victim order and batch",
            "implementation": "equal-victim counts, one ordered scan",
            "physical_execution": "unchanged",
            "cost_accounting": "existing measured control lane; matrix records diagnostic overrides",
        }
        return result
