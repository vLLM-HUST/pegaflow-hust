"""Admissible bounds and identical decisions under identical control costs."""

import json
import math
import os
import random
import time
from pathlib import Path

import pytest
import simulate_continuous_batch as sim
from bounded_search import BoundedSimulation, ProofMemoSimulation, first_lower_bound
from timed_recovery_search import TimedMemoSimulation


def test_partial_first_profile_is_a_lower_bound_at_all_percentile_ranks():
    rng = random.Random(20261026)
    for count in (1, 8, 95, 101):
        pending = tuple((str(i), rng.uniform(0, 10)) for i in range(count))
        completed = {key: 10 + rng.uniform(0, 100) for key, _ in pending}
        exact = first_lower_bound(pending, completed, 110)
        for clock in (10, 20, 50, 110):
            seen = {key: at for key, at in completed.items() if at <= clock}
            bound = first_lower_bound(pending, seen, clock)
            assert bound <= exact
            assert all(math.isfinite(v) for v in bound[1])
    # Equal first-token bounds cannot discard candidates with better releases.
    assert not first_lower_bound((("a", 0),), {"a": 1}, 2) > (1, (1,))


def test_same_causal_replay_and_boundaries_at_zero_control_cost(monkeypatch):
    source, seed = os.getenv("ISSUE23_SIM_INPUTS"), os.getenv("ISSUE23_SIM_SEED")
    if not source or not seed:
        pytest.skip("Explicit frozen input and calibration required")
    inputs = json.loads(Path(source).read_text(encoding="utf-8"))
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    monkeypatch.setattr(time, "perf_counter", lambda: 0.0)
    results = []
    for cls in (TimedMemoSimulation, BoundedSimulation, ProofMemoSimulation):
        model = cls(
            inputs["rows"][:16],
            inputs["initial_inventory"],
            sim.Costs(**values),
            Path(seed),
            inputs["control_cost_rows"],
            0.49032341538142143,
            "ours",
            False,
            batch_config=sim.BatchConfig(),
        )
        results.append(model.run())
        assert not model.jobs and not model.held and not model.allocation_leases.handles
    for result in results[1:]:
        assert results[0]["per_request"] == result["per_request"]
        assert results[0]["stages"] == result["stages"]
        assert results[0]["resource_events"] == result["resource_events"]
    assert sum(r.get("pruned_calendars", 0) for r in results[1]["search_records"]) > 0
    assert (
        sum(
            r.get("pruned_proof_reused", 0) + r.get("pruned_route_reused", 0)
            for r in results[2]["search_records"]
        )
        > 0
    )
