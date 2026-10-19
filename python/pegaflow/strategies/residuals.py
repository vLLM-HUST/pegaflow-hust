"""Explicit conditional lifetime, never fitting unfinished right-censored work."""

from .model import nonnegative
from .resource_schedule import Residual


def conditional_remaining(point, age, completed_errors=()):
    nonnegative(point)
    nonnegative(age)
    lifetimes = []
    for error in completed_errors:
        # Inputs are residuals of COMPLETED operations from the same phase.
        duration = max(0., point + error)
        nonnegative(duration)
        lifetimes.append(duration)
    if lifetimes:
        survivors = [duration - age for duration in lifetimes if duration > age]
        if survivors:
            return sum(survivors) / len(survivors)
        # Beyond empirical support, a memoryless continuation of the completed
        # mean is explicit extrapolation. No fitted safety multiplier/window.
        mean = sum(lifetimes) / len(lifetimes)
        if mean > 0:
            return mean
    if age < point:
        return point - age
    if point > 0:
        return point
    raise ValueError("A live zero-lifetime operation needs a measured cost, not false release")


def phase_remaining(work, operation, elapsed):
    """Default without residual labels; supplied trained callbacks can replace it."""
    if operation.first_seconds is None:
        return Residual(conditional_remaining(operation.seconds, elapsed))
    tail = operation.seconds - operation.first_seconds
    if work.actual_first is not None:
        started = dict(work.active_operations)[operation.name]
        age = max(0., started + elapsed - work.actual_first)
        return Residual(conditional_remaining(tail, age), 0.)
    first = conditional_remaining(operation.first_seconds, elapsed)
    return Residual(first + tail, first)
