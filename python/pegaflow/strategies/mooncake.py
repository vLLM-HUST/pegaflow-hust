"""FAST25 Algorithm 1 prefill-selection adapter, not the Mooncake system.

The manually configured paper ratio is REQUIRED, never fitted to our results.
The all-request benchmark does not emulate SLO rejection. D's least-request
selector and measured costs are explicit adaptations. Store, TE, continuous
batching and CPP are not reproduced by this Python strategy.
"""

import math

from .model import Decision


class MooncakePrefillStrategy:
    name = "mooncake-fast25-prefill-no-reject-adapter"
    source = "https://www.usenix.org/system/files/fast25-qin.pdf"

    def __init__(self, *, balancing_ratio):
        if type(balancing_ratio) not in (int, float) or not math.isfinite(balancing_ratio) or balancing_ratio < 1:
            raise ValueError("Declare the paper's finite cache-balancing ratio >=1")
        self.balancing_ratio = balancing_ratio

    def choose(self, snapshot, forecast=None):
        ps = [w for w in snapshot.workers if w.role == "P"]
        ds = [w for w in snapshot.workers if w.role == "D"]
        if not ps or not ds:
            raise ValueError("Both pools are required")
        best = min(ps, key=lambda w: (-w.local_prefix, w.node))
        d = min(ds, key=lambda w: (w.queued_requests, w.queue_service_seconds, w.node))
        choices = []
        for p in ps:
            transfer = best.local_prefix > self.balancing_ratio * p.local_prefix
            prefix = best.local_prefix if transfer else p.local_prefix
            source = best.node if transfer else p.node
            plans = [plan for plan in snapshot.feasible() if plan.p == p.node and plan.d == d.node
                     and plan.source == source and plan.p_prefix == prefix and not plan.layout_actions]
            if not plans:
                continue
            plan = min(plans, key=lambda v: (-v.d_prefix, v.history_copy_seconds))
            choices.append((p.queue_service_seconds + plan.p_first_seconds + plan.history_copy_seconds,
                            p.node, plan))
        if not choices:
            raise ValueError("No complete safely transferable paper-selected prefix")
        _, _, plan = min(choices, key=lambda item: item[:2])
        return Decision(self.name, plan.p, plan.d, plan, False,
                        f"FAST25 Algorithm1 prefill adapter; ratio={self.balancing_ratio}; "
                        "least-request D; rejection OFF; not full Mooncake", len(choices))
