"""Exact primitive-time forecast, retaining the P/D advisory policy unchanged.

This fixes summation/grouping, not cost estimation or physical simulation.
All measured extra calendar CPU remains on the existing control lane.
"""

from prefill_revalidation_search import PrefillRevalidationSimulation


class NumericalCalendarSimulation(PrefillRevalidationSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy_queues.exact_timeline = True

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "exact-binary-calendar-02"
        if self.mode != "native":
            quotes = result["initial_cache_forecasts"]
            if len(quotes) != len(result["per_request"]) or any(
                q["forecast_time_arithmetic"] != "binary64-exact-dyadic"
                for q in quotes.values()
            ):
                raise ValueError(
                    "Every actual selected forecast must use exact timeline"
                )
        result["forecast_arithmetic"] = {
            "primitive_costs": "unchanged binary64 predictions",
            "sums_and_repeat_products": "exact integer multiples of 2**-1074",
            "public_endpoints": "one binary64 rounding per endpoint",
            "fitted_tolerance": None,
            "execution_world": "unchanged retained float event simulator",
            "not_a_claim_of_exact_physical_costs": True,
            "activation_verified_per_selected_forecast": self.mode != "native",
        }
        return result
