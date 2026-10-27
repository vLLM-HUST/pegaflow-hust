"""Shared live strategy dispatch; the owned physical executor stays unchanged."""

from .baselines import NativeStrategy
from .model import Request, Snapshot, Worker


def native_pair(key, session, tokens, ready, now, sequence):
    workers = tuple(Worker(n, "P" if n < 2 else "D", 0, 0, 0, 0, 0., now)
                    for n in range(4))
    snapshot = Snapshot(Request(key, session, tokens, ready, sequence), now, workers, (),
                        frozenset((key,)))
    decision = NativeStrategy().choose(snapshot)
    return decision.p, decision.d


def controlled_route(request, layout, costs, queues, calendar, causal_origin, now):
    # The full parent's physical package, stage-cost and detached calendar
    # implementation is used here, not a simplified local eviction score.
    from pegaflow.joint_policy import best_route
    from pegaflow.resource_profile import route_order

    with queues.decision(request, layout, costs, causal_origin, now):
        result = best_route(request, layout, costs, calendar, max_candidates=64,
                            queues=queues, route_order=route_order)
        queues.reserve(request.key, result[0], now)
        return result


def request_payload(row, prefix=0, cold=False, native_cache=False):
    if type(native_cache) is not bool or (cold and native_cache):
        raise ValueError("Cold reference and explicit native serving mode are distinct")
    body = {"row": row, "cold": cold, "policy_native_cache": native_cache}
    if not native_cache:
        if type(prefix) is not int or prefix < 0:
            raise ValueError("Controlled restore requires integer block units")
        body["max_prefix_blocks"] = prefix
    return body
