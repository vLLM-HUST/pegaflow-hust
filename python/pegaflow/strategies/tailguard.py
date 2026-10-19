"""Pending-first tail optimization over actual resource-release forecasts."""

from .model import Decision


class TailguardStrategy:
    name = "tailguard-pending-release-v1"

    def choose(self, snapshot, forecast):
        if forecast is None:
            raise ValueError("Tailguard requires an explicit resource-calendar forecaster")
        choices = []
        for plan in snapshot.feasible():
            predicted = forecast(snapshot, plan)
            predicted.validate(snapshot)
            tail_profile = tuple(sorted((value for _, value in predicted.first_latencies),
                                        reverse=True))
            release_profile = tuple(sorted((value for _, value in predicted.full_release),
                                           reverse=True))
            # Already-first requests remain in resource calendars, not the
            # mutable first-token objective. No residency/tie reward is used.
            order = (tail_profile, release_profile, len(plan.layout_actions),
                     plan.history_copy_seconds + plan.handoff_seconds,
                     plan.p, plan.d, plan.source, plan.p_prefix, plan.d_prefix)
            choices.append((order, plan))
        if not choices:
            raise ValueError("No safely executable route/boundary candidate")
        _, chosen = min(choices, key=lambda item: item[0])
        return Decision(self.name, chosen.p, chosen.d, chosen, False,
                        "Pending-first descending latency profile; full decode/save release "
                        "and source/destination waits retained; no assumed RDMA congestion", len(choices))
