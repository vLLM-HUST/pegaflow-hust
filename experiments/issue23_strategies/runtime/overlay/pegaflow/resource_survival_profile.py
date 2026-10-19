"""Actual-first-conditioned resource forecast without zero-overrun optimism."""
from __future__ import annotations

from pegaflow.operation_schedule import Residual
from pegaflow.operation_survival import conditional_remaining
from pegaflow.resource_state_profile import StateProfileQueues


class SurvivalProfileQueues(StateProfileQueues):
    def __init__(self, models):
        super().__init__(models)
        self.first_samples = {}
        self.residual_calls, self.beyond_completed_support = 0, 0

    def seed_calibration(self, values, frozen):
        if self.jobs or self.operation_samples or self.first_samples:
            raise ValueError("Residual calibration must precede any owned live work")
        for value in values:
            for role in ("P", "D"):
                node = value["lane"] + (2 if role == "D" else 0)
                result, wire = value[role], value[role+"_wire"]
                first = frozen.compute(node, result["prompt_tokens"], result["num_cached_tokens"])
                full = first + (frozen.decode_tail(node, result["prompt_tokens"]) if node >= 2 else 0)
                self.operation_samples.setdefault((node, role+"_api"), []).append((full, wire["complete_seconds"]))
                if node >= 2:
                    self.first_samples.setdefault(node, []).append((first, wire["first_seconds"]))

    def observe(self, key, node, kind, now):
        with self.guard:
            if kind == "first":
                work = self.work[key]
                start = dict(work.active_operations).get("D_api")
                operation = next((o for o in work.decode if o.name == "D_api"), None)
                if start is None or operation is None or now < start:
                    raise ValueError("First-token observation needs the actual owned D API interval")
                self.first_samples.setdefault(node, []).append((operation.first_seconds, now-start))
            return super().observe(key, node, kind, now)

    def _remaining_operation(self, work, operation, elapsed):
        node = work.p if operation.name in ("history", "P_api") else work.d
        samples = self.operation_samples.get((node, operation.name), ())
        self.residual_calls += 1
        if samples and all(actual/issued*operation.seconds <= elapsed
                           for issued, actual in samples if issued > 0):
            self.beyond_completed_support += 1
        full = conditional_remaining(samples, operation.seconds, elapsed)
        first = None
        if operation.first_seconds is not None and work.actual_first is None:
            first = conditional_remaining(self.first_samples.get(node, ()), operation.first_seconds, elapsed)
            full = max(full, first)
        return Residual(full, first)

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(), "residual_model": "completed-lognormal-ratio-conditioned-on-live-age",
                "live_age_used_as_completed_sample": False,
                "first_samples": self.first_samples, "residual_calls": self.residual_calls,
                "beyond_completed_empirical_support": self.beyond_completed_support,
                "degenerate_support_fallback": "positive maximum-entropy lifetime of completed mean"}
