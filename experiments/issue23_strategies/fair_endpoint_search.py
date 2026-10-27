"""Use the forecast's request-order arbitration in the execution world too.

This repairs a CPU-model ordering mismatch, not a claimed native-server fix.
Copies still acquire both endpoint gates and the directional link. No overlap,
transfer bandwidth or lease protection is relaxed to make the result improve.
"""

from causal_prefill_search import LaxitySimulation
from memo_search import MemoSimulation


class RequestOrderMixin:
    def operation_priority(self, entry):
        node, key = entry
        return self.jobs[key]["work"].order, node, key

    def run(self):
        result = super().run()
        result["shared_endpoint_arbitration"] = (
            "request-order; match forecast; no node-index priority"
        )
        result["native_server_modified"] = False
        return result


class FairMemoSimulation(RequestOrderMixin, MemoSimulation):
    pass


class FairLaxitySimulation(RequestOrderMixin, LaxitySimulation):
    pass
