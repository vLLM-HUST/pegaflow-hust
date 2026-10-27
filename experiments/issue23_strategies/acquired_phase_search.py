"""Condition acquired exclusive D phases on observed age and completed samples.

Only start/kind/owner metadata is read from an active iteration. Its scheduled
future finish/duration and the execution world's residual labels are forbidden.
"""

from completed_phase_search import CompletedPhaseSimulation
from control_work_search import ControlWorkQueues, ControlWorkSnapshot
from pegaflow.detached_calendar import token
from pegaflow.phase_release_calendar import remaining


def phase_samples(models):
    rows = []
    for node, result, wire in models.phase_calibration:
        if node < 2:
            continue
        raw = result["raw_metrics"]
        rows.append(
            {
                "node": node,
                "prefix": result["num_cached_tokens"],
                "available": 0.0,
                "restore": raw["scheduled_ts"] - raw["queued_ts"],
                "save": max(
                    0.0,
                    wire["complete_seconds"]
                    - wire["first_seconds"]
                    - (raw["last_token_ts"] - raw["first_token_ts"]),
                ),
            }
        )
    return rows


def acquired_quote(event, progress, costs, samples, now, overhead):
    node, kind, started = event["node"], event["kind"], event["start"]
    if kind not in ("restore", "save") or len(event["pieces"]) != 1:
        raise ValueError("Only a device-exclusive owned phase can be conditioned")
    key, count, phase = event["pieces"][0]
    if (
        started > now
        or count
        or phase != kind
        or progress.phase != kind
        or progress.key != key
    ):
        raise ValueError("Observed phase/progress mismatch")
    prefix = progress.prefix_tokens
    point = costs.restore(node, prefix) if kind == "restore" else costs.fits[node][4]
    known = [
        r
        for r in samples
        if r["node"] == node
        and r["available"] <= now
        and (kind != "restore" or bool(r["prefix"]) == bool(prefix))
    ]
    errors = [
        r[kind]
        - (
            costs.restore(node, r["prefix"])
            if kind == "restore"
            else costs.fits[node][4]
        )
        for r in known
    ]
    if kind == "restore" and not prefix:
        # The retained execution contract has no H2D body for a cold prefix.
        # Calibration queue-to-scheduled noise is not a cold transfer phase.
        errors = []
    age = now - started
    return {
        "origin": now,
        "started": started,
        "kind": kind,
        "pieces": tuple(event["pieces"]),
        "remaining": remaining(errors, point + overhead, age),
        "point": point + overhead,
        "age": age,
        "samples": len(known),
        "latest": max((r["available"] for r in known), default=0.0),
        "future_finish_read": False,
    }


class AcquiredPhaseSnapshot(ControlWorkSnapshot):
    def configure_projection(self, projection, now):
        options = super().configure_projection(projection, now)
        options["inflight_iterations"] = self.inflight_iterations
        return options

    def forecast(self, route, now):
        result = super().forecast(route, now)
        self.quoted_routes[token(result)]["acquired_exclusive_phases"] = (
            self.inflight_iterations
        )
        return result


class AcquiredPhaseQueues(ControlWorkQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = AcquiredPhaseSnapshot
        shadow.inflight_iterations = self.acquired_phase_reader()
        return shadow


class AcquiredPhaseSimulation(CompletedPhaseSimulation):
    queue_type = AcquiredPhaseQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.acquired_quotes = []
        if self.mode != "native":
            self.initial_phase_samples = phase_samples(self.models)
            self.policy_queues.acquired_phase_reader = self.acquired_phases

    def acquired_phases(self):
        costs, progress = self.frozen(), self.progress()
        samples = [*self.initial_phase_samples, *self.models.exclusive_records]
        overhead = self.completed_iteration_overhead()
        result = {}
        for node, batch in self.d_batches.items():
            if batch.busy is None or batch.busy.kind not in ("restore", "save"):
                continue
            # The trace's future end is deliberately not copied or inspected.
            current = next(e for e in reversed(self.batch_trace) if e["node"] == node)
            event = {k: current[k] for k in ("node", "kind", "start", "pieces")}
            key = event["pieces"][0][0]
            result[node] = acquired_quote(
                event, progress[key], costs, samples, self.now, overhead[node]
            )
        if result:
            self.acquired_quotes.append({"origin": self.now, "phases": result})
        return result

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "acquired-exclusive-phase-01"
        result["acquired_phase_quotes"] = self.acquired_quotes
        result["forecast_restarts_one_inflight_iteration"] = (
            "compute only; exclusive phases age-conditioned"
        )
        return result
