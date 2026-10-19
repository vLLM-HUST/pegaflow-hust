"""Completed-only CPU estimates from known bounded planning work, not TTFT labels."""

from dataclasses import dataclass
import math

from pegaflow.operation_cost import NonnegativeFit


@dataclass(frozen=True)
class DecisionEstimate:
    kind: str
    origin: float
    features: tuple[float, float]
    seconds: float
    observations: int
    latest_completed: float


class DecisionCosts:
    def __init__(self):
        self.fits = {kind: NonnegativeFit(2) for kind in ("route", "boundary")}
        self.latest = 0.0
        self.records = []

    def estimate(self, kind, demands, layout, trial_bound, now):
        if self.latest > now:
            raise ValueError("A control forecast cannot use a future completed decision")
        if kind not in self.fits or type(trial_bound) is not int or trial_bound <= 0:
            raise ValueError("Declared finite decision work required")
        # This is a known upper work proxy, not a claim of exact CPU complexity.
        # The coefficients are fitted only to this algorithm's completed CPU.
        chunks = sum((d.tokens + d.block_tokens - 1) // d.block_tokens for d in demands)
        owners = sum(len(units) for units in layout.units)
        features = 1.0, float(trial_bound * chunks * (1 + owners))
        fit = self.fits[kind]
        seconds = fit.predict(features) if fit.observations else 0.0
        return DecisionEstimate(kind, now, features, seconds, fit.observations, self.latest)

    def observe(self, estimate, cpu_seconds, completed):
        if not math.isfinite(completed) or completed < estimate.origin or cpu_seconds < 0:
            raise ValueError("Only completed finite decision CPU observations may train")
        self.fits[estimate.kind].add(estimate.features, cpu_seconds)
        self.latest = max(self.latest, completed)
        self.records.append(dict(kind=estimate.kind, origin=estimate.origin,
            features=estimate.features, predicted_CPU=estimate.seconds,
            observed_CPU=cpu_seconds, completed=completed,
            forecast_latest_completed=estimate.latest_completed,
            forecast_observations=estimate.observations))

    def snapshot(self):
        return dict(kind="nonnegative-completed-decision-CPU-work-fit",
            initial_without_own_CPU_observation="unknown represented by0 forecast; actual cost never free",
            features=["fixed", "trial_bound*known_prefill_chunks*(1+visible_owner_components)"],
            fits={kind: fit.snapshot() for kind, fit in self.fits.items()},
            records=self.records, latest_completed=self.latest)
