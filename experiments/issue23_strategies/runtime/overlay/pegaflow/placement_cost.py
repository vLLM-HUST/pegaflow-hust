"""Fit a small nonnegative latency model from completed real observations."""

from __future__ import annotations

import itertools
import math
import sys
from dataclasses import dataclass


def _solve(matrix, values):
    size = len(values)
    rows = [list(row) + [value] for row, value in zip(matrix, values, strict=True)]
    tolerance = sys.float_info.epsilon * max(1.0, sum(abs(v) for row in matrix for v in row))
    for column in range(size):
        pivot = max(range(column, size), key=lambda index: abs(rows[index][column]))
        if abs(rows[pivot][column]) <= tolerance:
            return None
        rows[column], rows[pivot] = rows[pivot], rows[column]
        divisor = rows[column][column]
        rows[column] = [value / divisor for value in rows[column]]
        for index in range(size):
            if index != column:
                multiplier = rows[index][column]
                rows[index] = [
                    value - multiplier * other
                    for value, other in zip(rows[index], rows[column], strict=True)
                ]
    return tuple(row[-1] for row in rows)


@dataclass(frozen=True)
class ObservedLatencyModel:
    intercept_seconds: float
    seconds_per_uncached_token: float
    seconds_per_rdma_byte: float
    observations: int
    rmse_seconds: float
    rdma_observed: bool

    def predict(self, uncached_tokens: int, rdma_bytes: int) -> float:
        if any(type(value) is not int or value < 0 for value in (uncached_tokens, rdma_bytes)):
            raise ValueError("Nonnegative observed token and transfer counts required")
        if rdma_bytes and not self.rdma_observed:
            raise ValueError("No measured RDMA observations for this node/direction")
        return (
            self.intercept_seconds
            + self.seconds_per_uncached_token * uncached_tokens
            + self.seconds_per_rdma_byte * rdma_bytes
        )

    @classmethod
    def fit(cls, samples: tuple[tuple[int, int, float], ...]) -> ObservedLatencyModel:
        """NNLS over three fixed features, with no regularization or score weights.

        Enumerate the eight active sets. Singular sets are skipped; their feasible
        lower-dimensional boundaries are considered separately. Features are
        scaled by observed maxima solely for numerical conditioning.
        """
        if len(samples) < 3:
            raise ValueError("At least three completed calibration observations required")
        for tokens, transferred, seconds in samples:
            if (
                type(tokens) is not int
                or tokens < 0
                or type(transferred) is not int
                or transferred < 0
                or isinstance(seconds, bool)
                or not math.isfinite(seconds)
                or seconds < 0
            ):
                raise ValueError("Invalid real calibration observation")
        scales = (
            1.0,
            max(1, max(row[0] for row in samples)),
            max(1, max(row[1] for row in samples)),
        )
        points = [
            ((1.0, tokens / scales[1], transferred / scales[2]), seconds)
            for tokens, transferred, seconds in samples
        ]
        candidates = []
        for active_bits in itertools.product((False, True), repeat=3):
            active = [index for index, enabled in enumerate(active_bits) if enabled]
            coefficients = [0.0] * 3
            if active:
                matrix = [[sum(x[i] * x[j] for x, _ in points) for j in active] for i in active]
                values = [sum(x[i] * y for x, y in points) for i in active]
                solved = _solve(matrix, values)
                if solved is None or any(not math.isfinite(value) or value < 0 for value in solved):
                    continue
                for index, value in zip(active, solved, strict=True):
                    coefficients[index] = value
            error = sum(
                (sum(a * b for a, b in zip(x, coefficients, strict=True)) - y) ** 2
                for x, y in points
            )
            candidates.append((error, tuple(coefficients)))
        error, coefficients = min(candidates)
        actual = tuple(value / scale for value, scale in zip(coefficients, scales, strict=True))
        return cls(
            *actual,
            len(samples),
            math.sqrt(error / len(samples)),
            any(row[1] > 0 for row in samples),
        )
