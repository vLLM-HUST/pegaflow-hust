"""Positive residual-life predictions from completed operation ratios only.

Live age is a right-censoring event, never a completed training duration.
The lognormal family is a stated modeling assumption, not a tail guarantee.
"""
from __future__ import annotations

import math


def log_survival(z):
    value = .5 * math.erfc(z / math.sqrt(2))
    if value:
        return math.log(value)
    # erfc can underflow for very remote normal tails. The first asymptotic
    # Mills terms are numerical evaluation, not a policy load/latency threshold.
    inverse = 1 / (z*z)
    return -.5*z*z - math.log(z) - .5*math.log(2*math.pi) + math.log1p(-inverse+3*inverse*inverse)


def conditional_remaining(samples, predicted, elapsed):
    """E[T-age | T>age], using completed T / issued_prediction ratios."""
    if not math.isfinite(predicted) or predicted < 0 or not math.isfinite(elapsed) or elapsed < 0:
        raise ValueError("Finite nonnegative operation duration and actual elapsed age required")
    ratios = [actual / issued for issued, actual in samples if issued > 0 and actual > 0]
    if any(not math.isfinite(value) for value in ratios):
        raise ValueError("Finite completed operation ratios required")
    if predicted == 0:
        if elapsed:
            raise ValueError("A zero-duration operation cannot have a live elapsed interval")
        return 0.0
    if not ratios:
        # Maximum-entropy positive lifetime with the existing point mean.
        return predicted
    logs = [math.log(value) for value in ratios]
    mean = sum(logs) / len(logs)
    variance = sum((value-mean)**2 for value in logs) / len(logs)
    scale = math.exp(mean) * predicted
    if variance == 0:
        if elapsed < scale:
            return scale-elapsed
        # The live operation contradicts the degenerate completed support.
        # Do not claim that it will end immediately or train on its partial age.
        # A memoryless positive lifetime of completed mean is an explicit
        # fallback assumption, not an arbitrary epsilon or fitted safety margin.
        return sum(ratios) / len(ratios) * predicted
    deviation = math.sqrt(variance)
    if elapsed == 0:
        return math.exp(mean+.5*variance) * predicted
    z = (math.log(elapsed/predicted)-mean) / deviation
    log_mean_over_age = (mean+.5*variance+math.log(predicted/elapsed)
                         + log_survival(z-deviation)-log_survival(z))
    return max(0.0, elapsed*math.expm1(log_mean_over_age))
