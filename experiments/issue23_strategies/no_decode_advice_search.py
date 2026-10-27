"""Isolate optional post-P rerouting from the retained full initial scheduler.

This is a causal policy ablation, not a claim that fixed FIFO is universally
better. Keep original P/D routing, cache projection, P boundary checks, D
boundary restoration and full decode scheduling. Only avoid launching optional
post-P advice, so its CPU/RPC cost is genuinely not performed, not subtracted.
"""

from prefix_reclaim_search import PrefixReclaimSimulation


class NoDecodeAdviceSimulation(PrefixReclaimSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.enable_decode_advice = False

    def run(self):
        result = super().run()
        if self.advice_records or self.advice_pending is not None:
            raise ValueError("Disabled advisory path executed or failed to drain")
        result["internal_candidate"]["name"] = "no-decode-advice-01"
        result["decode_advice_ablation"] = {
            "enabled": False,
            "reference": "prefix-reclaim-calendar-02",
            "initial_joint_route_and_P_revalidation": "unchanged",
            "D_boundary_restore_compute_and_release": "unchanged",
            "cost_removed_by_not_executing": "only optional post-P advice",
            "all_remaining_control_costs_charged": True,
            "not_a_new_strategy_win_claim": True,
        }
        return result
