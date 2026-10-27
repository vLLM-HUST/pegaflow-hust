"""Cancel optional remaining search using the already computed resource DAG.

This prices an expected useful decision window, not a guaranteed deadline.
Known copies, all phases and queued work are included by the unchanged calendar;
future arrivals and unobserved physical delays are not accessible.
"""

import time

from prefix_reclaim_search import PrefixReclaimSimulation
from yielding_decode_search import cancellation


def remaining_quote(record, records, origin):
    known = [
        r
        for r in records
        if r is not record and r["completed_search"] and r["resolved"] <= origin
    ]
    samples = [
        sum(q["CPU_wall"] for q in r["quanta"][1:]) + r["commit_CPU_wall"]
        for r in known
    ]
    expected = sum(samples) / len(samples) if samples else None
    acquired = record["incumbent_calendar"]["D_acquire"]
    return {
        "id": record["id"],
        "origin": origin,
        "history": [r["id"] for r in known],
        "history_latest": max((r["resolved"] for r in known), default=None),
        "remaining_CPU_samples": samples,
        "predicted_remaining_CPU": expected,
        "predicted_D_acquire": acquired,
        "cancel": expected is not None
        and acquired is not None
        and acquired <= origin + expected,
        "reason": "cold_history"
        if expected is None
        else "unknown_acquisition"
        if acquired is None
        else "calendar_window",
    }


class AdviceCalendarSimulation(PrefixReclaimSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calendar_window_records = []

    def observe_advice_keep(self, record, prediction):
        # Called inside the existing measured first search quantum. No second
        # snapshot/forecast and no read from the execution-world event queue.
        key, node = record["id"], record["before"]
        record["incumbent_calendar"] = {
            "origin": record["forecast_origin"],
            "model_latest": record["model_latest"],
            "D_acquire": prediction.get("stage_start", {}).get((key, node)),
            "operations": prediction.get("operations", []),
        }

    def route(self):
        if self.advice_pending is not None:
            key, _, record = self.advice_pending
            if (
                self.now >= record["decision_ready"]
                and len(record["quanta"]) == 1
                and not record.get("calendar_window_checked")
            ):
                began, origin = time.perf_counter(), self.now
                reason = cancellation(
                    key,
                    record["before"],
                    self.jobs,
                    self.d_active,
                    self.queues,
                    self.pending,
                    self.rowmap,
                    self.active_sessions,
                )
                quote = remaining_quote(record, self.advice_records, origin)
                quote["existing_cancellation"] = reason
                record["calendar_window_checked"] = True
                self.calendar_window_records.append(quote)
                self.now += time.perf_counter() - began
                quote.update(CPU_wall=self.now - origin, ready=self.now)
                if reason is None and quote["cancel"]:
                    # The remaining search never completed: no full-search
                    # training label, no layout action, and no ownership change.
                    self.resolve_advice("predicted_acquisition_before_search")
        super().route()

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "advice-calendar-window-01"
        result["calendar_window_records"] = self.calendar_window_records
        result["calendar_window_CPU_wall"] = sum(
            r["CPU_wall"] for r in self.calendar_window_records
        )
        result["calendar_window_model"] = {
            "release_source": "unchanged incumbent resource DAG stage_start",
            "forecast_reused": True,
            "future_world_durations_used": False,
            "guaranteed_deadline": False,
            "all_trigger_cost_charged": True,
            "cancelled_search_is_completed_training_label": False,
        }
        return result
