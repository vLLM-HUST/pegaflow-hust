"""Budget-bounded placement over shared physical cache objects.

This pure planner executes no mutations. Bundle membership, measured objective,
staging reservations and atomic lease-aware application are caller contracts.
No unknown-future, global-optimum or real-latency guarantee is implied.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from itertools import islice


@dataclass(frozen=True)
class CacheObject:
    key: str
    p_bytes: int
    d_bytes: int
    allowed: tuple[int, ...] = (0, 1, 2, 3)
    mandatory: int = 0


@dataclass(frozen=True)
class PlacementBundle:
    """A caller-validated recovery dependency set, possibly sharing objects."""

    key: str
    objects: frozenset[str]

    def available(self, states: Mapping[str, int], host: int) -> bool:
        if type(host) is not int or host not in (1, 2) or not self.objects:
            raise ValueError("A nonempty bundle and one host bit are required")
        return all(states.get(key, 0) & host for key in self.objects)


@dataclass(frozen=True)
class PlacementResult:
    states: dict[str, int]
    p_bytes: int
    d_bytes: int
    initial_p99: float
    p99: float
    trials: int
    evaluations: int
    duplicate_trials: int
    infeasible_trials: int
    accepted_objectives: tuple[tuple[float, int], ...]
    stop_reason: str
    neighborhood_local_optimum_certified: bool
    global_optimum_claimed: bool = False
    actions_applied: bool = False


def resident_bytes(objects: tuple[CacheObject, ...], states: Mapping[str, int]) -> tuple[int, int]:
    """Count each physical object once, never once per logical bundle."""
    keys = {item.key for item in objects}
    if len(keys) != len(objects) or set(states) - keys:
        raise ValueError("Duplicate physical objects or unknown residence keys")
    if any(type(state) is not int or state not in range(4) for state in states.values()):
        raise ValueError("Invalid residence state")
    if any(
        type(size) is not int or size < 0
        for item in objects
        for size in (item.p_bytes, item.d_bytes)
    ):
        raise ValueError("Object byte sizes must be nonnegative integers")
    return (
        sum(item.p_bytes for item in objects if states.get(item.key, 0) & 1),
        sum(item.d_bytes for item in objects if states.get(item.key, 0) & 2),
    )


def _neighbors(
    values: tuple[int, ...], bundles: tuple[tuple[int, ...], ...]
) -> Iterator[tuple[int, ...]]:
    for index, current in enumerate(values):
        for state in range(4):
            if state != current:
                candidate = list(values)
                candidate[index] = state
                yield tuple(candidate)
    for host in (1, 2):
        for members in bundles:
            for enabled in (True, False):
                candidate = list(values)
                for index in members:
                    candidate[index] = (
                        candidate[index] | host if enabled else candidate[index] & ~host
                    )
                yield tuple(candidate)
        for removed in range(len(bundles)):
            for added in range(len(bundles)):
                if removed == added:
                    continue
                candidate = list(values)
                for index in bundles[removed]:
                    candidate[index] &= ~host
                # Add last: shared ancestors needed by the new bundle survive.
                for index in bundles[added]:
                    candidate[index] |= host
                yield tuple(candidate)


def improve_shared_placement(
    objects: tuple[CacheObject, ...],
    bundles: tuple[PlacementBundle, ...],
    initial: Mapping[str, int],
    p_capacity: int,
    d_capacity: int,
    objective: Callable[[dict[str, int]], float],
    *,
    max_trials: int,
) -> PlacementResult:
    """Return a feasible model improvement within an explicit work budget.

    Every attempted candidate consumes budget, including duplicates, no-ops and
    infeasible layouts. Accept lexicographically smaller (model p99, bytes),
    not a tunable weighted sum. Only a complete unsuccessful neighborhood sweep
    certifies neighborhood-local optimality, never global optimality.
    """
    if type(max_trials) is not int or max_trials < 1:
        raise ValueError("max_trials must be a positive integer")
    if any(type(value) is not int or value < 0 for value in (p_capacity, d_capacity)):
        raise ValueError("Capacities must be nonnegative integers after staging reservations")
    objects = tuple(sorted(objects, key=lambda item: item.key))
    bundles = tuple(sorted(bundles, key=lambda bundle: bundle.key))
    keys = tuple(item.key for item in objects)
    if len(set(keys)) != len(keys) or set(initial) - set(keys):
        raise ValueError("Duplicate physical objects or unknown initial keys")
    if len({bundle.key for bundle in bundles}) != len(bundles):
        raise ValueError("Duplicate bundle identities")
    indices = {key: index for index, key in enumerate(keys)}
    indexed_bundles = []
    for bundle in bundles:
        if not bundle.objects or bundle.objects - indices.keys():
            raise ValueError("Bundles must contain known physical objects")
        indexed_bundles.append(tuple(sorted(indices[key] for key in bundle.objects)))
    indexed_bundles = tuple(indexed_bundles)
    for item in objects:
        if any(type(size) is not int or size < 0 for size in (item.p_bytes, item.d_bytes)):
            raise ValueError("Object byte sizes must be nonnegative integers")
        if (
            type(item.mandatory) is not int
            or item.mandatory not in range(4)
            or not item.allowed
            or any(type(state) is not int or state not in range(4) for state in item.allowed)
            or not any(state & item.mandatory == item.mandatory for state in item.allowed)
        ):
            raise ValueError("Invalid allowed or mandatory residence states")

    def allocation(values):
        p_bytes = d_bytes = 0
        for item, state in zip(objects, values, strict=True):
            if (
                type(state) is not int
                or state not in item.allowed
                or state & item.mandatory != item.mandatory
            ):
                return None
            p_bytes += item.p_bytes if state & 1 else 0
            d_bytes += item.d_bytes if state & 2 else 0
        if p_bytes > p_capacity or d_bytes > d_capacity:
            return None
        return p_bytes, d_bytes

    def score(values, used):
        value = float(objective(dict(zip(keys, values, strict=True))))
        if not math.isfinite(value) or value < 0:
            raise ValueError("Objective must return a finite nonnegative model p99")
        return value, sum(used)

    current = tuple(initial.get(key, 0) for key in keys)
    used = allocation(current)
    if used is None:
        raise ValueError("Initial placement violates capacity, allowed states or mandatory leases")
    best = score(current, used)
    initial_p99 = best[0]
    accepted = [best]
    seen = {current: best}
    trials = evaluations = 1
    duplicates = infeasible = 0
    stop_reason = "budget_exhausted"
    while trials < max_trials:
        improved = False
        count = 3 * len(objects) + 4 * len(bundles) + 2 * len(bundles) * (len(bundles) - 1)
        sweep_limit = min(count, max_trials - trials)
        for candidate in islice(_neighbors(current, indexed_bundles), sweep_limit):
            trials += 1
            candidate_used = allocation(candidate)
            if candidate_used is None:
                infeasible += 1
                continue
            candidate_score = seen.get(candidate)
            if candidate_score is None:
                candidate_score = score(candidate, candidate_used)
                seen[candidate] = candidate_score
                evaluations += 1
            else:
                duplicates += 1
            if candidate_score < best:
                current, used, best = candidate, candidate_used, candidate_score
                accepted.append(best)
                improved = True
                break
        if not improved:
            if sweep_limit == count:
                stop_reason = "no_improving_neighbor"
            break
    return PlacementResult(
        dict(zip(keys, current, strict=True)),
        used[0],
        used[1],
        initial_p99,
        best[0],
        trials,
        evaluations,
        duplicates,
        infeasible,
        tuple(accepted),
        stop_reason,
        stop_reason == "no_improving_neighbor",
    )
