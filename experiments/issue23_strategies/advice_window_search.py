"""Spend optional search time only inside an estimated slot-release window.

This is a completed-history trigger, not a proof that a real device will release
on time. Mixed phases, non-head FIFO work and cold control history fall back to
the parent. A skipped search is never a completed search-cost observation.
"""

import time
from dataclasses import replace

from prefix_reclaim_search import PrefixReclaimSimulation
from release_cobatch_search import decode_step_cost, shared_step_cost
from slot_release_search import release_estimate


def trigger_quote(
    key,
    node,
    queue,
    active,
    capacity,
    records,
    costs,
    origin,
    rpc,
    *,
    busy_pieces=(),
    sharing=0.0,
    external_gate=False,
):
    known = [
        r
        for r in records
        if r.get("completed_search") and r["decision_ready"] <= origin
    ]
    quote = {
        "id": key,
        "origin": origin,
        "node": node,
        "queue": list(queue),
        "active": {
            s.key: {
                "phase": s.phase,
                "prompt_tokens": s.prompt_tokens,
                "output_tokens": s.output_tokens,
                "decoded": s.decoded,
            }
            for s in active.values()
        },
        "history": [r["id"] for r in known],
        "CPU_latest": max((r["decision_ready"] for r in known), default=None),
        "predicted_CPU": sum(r["CPU_wall"] for r in known) / len(known)
        if known
        else None,
        "RPC_seconds": rpc,
        "model_latest": None,
        "release_estimates": {},
        "skip": False,
        "reason": "unknown_window",
        "busy_pieces": list(busy_pieces),
        "external_gate": external_gate,
        "estimated_busy_seconds": 0.0,
    }
    if not queue or queue[0] != key or len(active) != capacity:
        return quote
    if external_gate or any(s.phase != "decode" for s in active.values()):
        return quote
    if not known:
        quote["reason"] = "cold_control_history"
        return quote
    assert costs is not None and costs.latest_completed_seconds <= origin
    # The issued iteration is non-preemptible. Use its observed work counts,
    # never Iteration.seconds (a sampled execution-world duration). Conservatively
    # include its whole predicted duration because elapsed service is censored.
    counts = dict(busy_pieces)
    busy_seconds = (
        shared_step_cost(
            [
                decode_step_cost(active[k], costs, node) * count
                for k, count in busy_pieces
            ],
            sharing,
        )
        if busy_pieces
        else 0.0
    )
    estimates = {
        s.key: busy_seconds
        + release_estimate(
            replace(s, decoded=s.decoded + counts.get(s.key, 0)), costs, node
        )
        for s in active.values()
    }
    quote["estimated_busy_seconds"] = busy_seconds
    quote.update(
        model_latest=costs.latest_completed_seconds, release_estimates=estimates
    )
    # No fitted margin: compare expected decision readiness to the predecessor's
    # remaining full decode plus sync-save release under the existing drain rule.
    quote["skip"] = min(estimates.values()) <= quote["predicted_CPU"] + rpc
    quote["reason"] = "release_before_search" if quote["skip"] else "search_window_open"
    return quote


class AdviceWindowSimulation(PrefixReclaimSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.advice_trigger_records = []

    def advice_trigger(self, key):
        began, origin = time.perf_counter(), self.now
        node = self.jobs[key]["live_route"].d
        queue, batch = self.queues[node], self.d_batches[node]
        owner = self.held.get(f"gate{node}")
        external_gate = owner is not None and owner[0] != f"batch{node}"
        known = any(
            r.get("completed_search") and r["decision_ready"] <= origin
            for r in self.advice_records
        )
        measurable = (
            queue[0] == key
            and len(batch.active) == self.batch_config.max_sequences
            and all(s.phase == "decode" for s in batch.active.values())
            and not external_gate
            and known
        )
        costs = self.frozen() if measurable else None
        pieces = (
            [(k, count * batch.busy.repeats) for k, count, phase in batch.busy.pieces]
            if measurable and batch.busy is not None
            else []
        )
        quote = trigger_quote(
            key,
            node,
            queue,
            batch.active,
            self.batch_config.max_sequences,
            self.advice_records,
            costs,
            origin,
            self.rpc_boundary,
            busy_pieces=pieces,
            sharing=self.batch_config.shared_fraction,
            external_gate=external_gate,
        )
        quote["batch_busy"] = batch.busy is not None
        quote["observed_trace_count"] = len(self.batch_trace)
        quote["busy_trace_index"] = (
            next(
                (
                    i
                    for i in range(len(self.batch_trace) - 1, -1, -1)
                    if self.batch_trace[i]["node"] == node
                ),
                None,
            )
            if batch.busy is not None
            else None
        )
        self.advice_trigger_records.append(quote)
        elapsed = time.perf_counter() - began
        self.now += elapsed
        quote.update(CPU_wall=elapsed, ready=self.now)
        return not quote["skip"]

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "advice-release-window-01"
        result["advice_trigger_records"] = self.advice_trigger_records
        result["advice_trigger_CPU_wall"] = sum(
            r["CPU_wall"] for r in self.advice_trigger_records
        )
        result["advice_trigger_model"] = {
            "rule": "expected remaining decode+sync release <= completed-only search CPU+RPC",
            "guaranteed_deadline": False,
            "future_world_times_used": False,
            "unknown_window": "retain parent search",
            "all_trigger_cost_charged": True,
        }
        return result
