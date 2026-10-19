"""Admission-time resource DAG adapter, retaining unfinished decode holders.

The scheduler is copied unchanged from the preserved phase-release parent.
First/full service durations already contain restore, compute and synchronous
save. Nested H2D-task timers must NOT be added to those durations again.
This module is a source-only predictor, not a cross-machine executor.
"""

from .model import Forecast, nonnegative
from .resource_schedule import Operation, Work, schedule
from .residuals import phase_remaining


def copy_resources(source, destination, serialize_direction=False):
    resources = tuple(sorted({f"gate:{source}", f"gate:{destination}"}))
    if serialize_direction and (source < 2) != (destination < 2):
        direction = "P>D" if source < 2 else "D>P"
        return (*resources, f"link:{direction}")
    return resources


def candidate_work(snapshot, plan, serialize_direction=False):
    """Costs and resource requirements are explicit; no bandwidth penalty."""
    p, d = plan.p, plan.d
    prefill = (
        Operation("control", plan.control_seconds, ()),
        Operation("history_copy", plan.history_copy_seconds,
                  copy_resources(plan.source, p, serialize_direction)),
        Operation("P_api", plan.p_full_seconds, (f"gate:{p}", f"npu:{p}")),
    )
    decode = (
        Operation("handoff", plan.handoff_seconds,
                  copy_resources(p, d, serialize_direction)),
        Operation("D_api", plan.d_full_seconds, (f"gate:{d}", f"npu:{d}"),
                  first_seconds=plan.d_first_seconds),
    )
    return Work(snapshot.request.key, p, d, snapshot.request.sequence,
                snapshot.request.ready, prefill, decode)


class ResourceForecaster:
    """Freeze actual live progress and completed-only costs at one origin.

    live_work must include jobs that emitted first but still decode/save.
    resource_blocks describe accepted asynchronous commits, not speculative
    planner returns. Gate release here is conservative full-API release; the
    executor must explicitly report a different phase-release contract before
    using one. Serial links are optional hypotheses, OFF by default.
    """

    def __init__(self, live_work=(), *, latest_completed=0.0, remaining=None,
                 resource_blocks=None, serialize_direction=False):
        self.live_work = tuple(live_work)
        nonnegative(latest_completed)
        self.latest_completed = latest_completed
        self.remaining = remaining or phase_remaining
        self.resource_blocks = dict(resource_blocks or {})
        self.serialize_direction = serialize_direction

    def __call__(self, snapshot, plan):
        if plan.layout_actions:
            raise ValueError("Layout-changing forecasts require the full native inventory projector; "
                             "do not price unchanged live cache as a layout optimization")
        keys = {w.key for w in self.live_work}
        if snapshot.request.key in keys:
            raise ValueError("A candidate must not duplicate an already owned request")
        pending = {w.key for w in self.live_work if w.actual_first is None}
        if pending | {snapshot.request.key} != set(snapshot.pending_first):
            raise ValueError("Live progress does not cover the pending-first objective")
        if any(w.actual_first is not None and w.key not in snapshot.observed_first
               for w in self.live_work):
            raise ValueError("An observed first token was not disclosed in the snapshot")
        work = (*self.live_work, candidate_work(snapshot, plan, self.serialize_direction))
        result = schedule(work, snapshot.now, remaining=self.remaining,
                          resource_blocks=self.resource_blocks)
        ready = {w.key: w.offered for w in work}
        releases = {w.node: snapshot.now for w in snapshot.workers}
        for job in work:
            releases[job.p] = max(releases[job.p], result["P_end"][job.key])
            releases[job.d] = max(releases[job.d], result["full"][job.key])
        forecast = Forecast(
            tuple(sorted((key, result["first"][key] - ready[key])
                         for key in snapshot.pending_first)),
            tuple(sorted(releases.items())), self.latest_completed, snapshot.now, True)
        forecast.validate(snapshot)
        return forecast
