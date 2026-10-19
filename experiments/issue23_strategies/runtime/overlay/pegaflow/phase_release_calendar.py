"""Renew D's release forecast at the observed first-token event."""

from pegaflow.detached_calendar import CalendarSnapshot, DetachedCalendarQueues, token
from pegaflow.operation_schedule import Residual


def remaining(errors, point, age):
    if point < 0 or age < 0:
        raise ValueError("Nonnegative phase mean and observed age required")
    durations = [max(0.0, point + error) for error in errors]
    if not durations:
        return max(0.0, point - age) if age < point else point
    survivors = [duration - age for duration in durations if duration > age]
    if survivors:
        return sum(survivors) / len(survivors)
    # A live operation beyond every completed duration cannot be declared done.
    # Positive maximum-entropy lifetime of the completed mean is an explicit
    # extrapolation assumption, not a fitted safety constant or partial label.
    return sum(durations) / len(durations) or point


class PhaseSnapshot(CalendarSnapshot):
    def _remaining_operation(self, work, operation, elapsed):
        if operation.name != "D_api":
            return super()._remaining_operation(work, operation, elapsed)
        if work.actual_first is None:
            return super()._remaining_operation(work, operation, elapsed)
        started = dict(work.active_operations)["D_api"]
        origin = started + elapsed
        restored = bool(self.routes[work.key].d_blocks)
        first_rows = self.phase_samples.get((work.d, "first", restored), ())
        tail_rows = self.phase_samples.get((work.d, "tail", None), ())
        first_errors = [error for completed, error in first_rows if completed <= origin]
        tail_errors = [error for completed, error in tail_rows if completed <= origin]
        demand = self.demands[work.key]
        point_tail = self.phase_costs.decode_tail(work.d, demand.tokens)
        if work.actual_first is not None:
            age = max(0.0, origin - work.actual_first)
            return Residual(remaining(tail_errors, point_tail, age), 0.0)
        point_first = operation.first_seconds
        if point_first is None:
            raise ValueError("D API acquisition must retain its first-token prediction")
        first = remaining(first_errors, point_first, elapsed)
        # Before first, the entire post-first decode/save phase is still ahead.
        return Residual(first + point_tail, first)

    def forecast(self, route, now):
        self.phase_costs = self.context[2]
        selected = super().forecast(route, now)
        self.quoted_routes[token(selected)].update(
            release_conditioning="actual first-token renews decode/save age; original prefirst retained",
            observed_decode_holders=[dict(request=w.key, node=w.d, first=w.actual_first)
                for w in self.work.values() if w.actual_first is not None],
            RDMA_congestion_assumed=False)
        return selected

    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
        self.phase_costs = costs
        return super().decode_boundary(key, route, layout, costs, now, max_candidates)


class PhaseReleaseQueues(DetachedCalendarQueues):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.phase_samples = {}
        self.phase_completion_records = []
        point = self.models.freeze(0)
        for node, result, wire in self.models.phase_calibration:
            if node < 2:
                continue
            first = point.compute(node, result["prompt_tokens"], result["num_cached_tokens"])
            tail = point.decode_tail(node, result["prompt_tokens"])
            self.phase_samples.setdefault((node, "first", bool(result["num_cached_tokens"])), []).append(
                (0.0, wire["first_seconds"] - first))
            self.phase_samples.setdefault((node, "tail", None), []).append(
                (0.0, wire["complete_seconds"] - wire["first_seconds"] - tail))

    def planning_snapshot(self):
        with self.guard:
            shadow = super().planning_snapshot()
            shadow.__class__ = PhaseSnapshot
            shadow.phase_samples = {key: tuple(values) for key, values in self.phase_samples.items()}
            # A model object may finish concurrently. Forecasts must use the
            # immutable costs in the decision context, not future live fits.
            return shadow

    def operation(self, key, name, kind, now, resources=None):
        with self.guard:
            if name == "D_api" and kind == "finish":
                work = self.work[key]
                started = dict(work.active_operations)[name]
                operation = next(op for op in work.decode if op.name == name)
                if work.actual_first is None or not started <= work.actual_first <= now:
                    raise ValueError("Completed D API needs its real first-token event")
                restored = bool(self.routes[key].d_blocks)
                actual_first, actual_tail = work.actual_first - started, now - work.actual_first
                point_first = operation.first_seconds
                point_tail = operation.seconds - point_first
                self.phase_samples.setdefault((work.d, "first", restored), []).append(
                    (now, actual_first - point_first))
                self.phase_samples.setdefault((work.d, "tail", None), []).append(
                    (now, actual_tail - point_tail))
                self.phase_completion_records.append(dict(request=key, node=work.d,
                    completed=now, first_observed=work.actual_first, issued_first=point_first,
                    issued_tail=point_tail, actual_first=actual_first, actual_tail=actual_tail,
                    restored_command=restored, live_age_used_as_completed=False))
            return super().operation(key, name, kind, now, resources)

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(), "phase_completion_records": self.phase_completion_records,
                    "phase_residuals": [dict(node=k[0], phase=k[1], restored=k[2], values=v)
                                        for k, v in self.phase_samples.items()],
                    "release_forecast": "first observed renews the post-first decode/save phase",
                    "before_first_estimator_changed": False,
                    "RDMA_congestion_assumed": False}
