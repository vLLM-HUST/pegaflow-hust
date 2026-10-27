"""Scheduling contracts for the P/D capacity counterfactual."""

from types import SimpleNamespace

import pytest
from pd_capacity_batch_simulator import (
    BatchedSimulation,
    batch_ratio,
    decode_step_seconds,
)


def test_mixed_steps_share_one_token_budget():
    sim = object.__new__(BatchedSimulation)
    sim.gates, sim.steps, sim.now = {}, [], 0.0
    sim.engine = {
        2: {
            "d0": {"remaining": 0, "decode_left": 127},
            "d1": {"remaining": 0, "decode_left": 127},
            "p0": {"remaining": 80, "head_per_token": 0.01},
            "p1": {"remaining": 100, "head_per_token": 0.01},
        }
    }
    sim.rows = {key: {"tokens": 1000} for key in sim.engine[2]}
    sim.protocol = {"prefill": 128}
    sim.costs = SimpleNamespace(decode_seconds=(7.62, 7.62))
    sim.profile = [{"context_tokens": 1000, "normalized": {"1": 1.0, "2": 1.05}}]
    sim.unbatchable = 0.25
    events = []
    sim.event = lambda *args: events.append(args)
    sim.schedule_step(2)
    step = sim.steps[0]
    assert len(step["decoding"]) + sum(n for _, n in step["heads"]) == 128
    assert step["heads"] == [("p0", 80), ("p1", 46)]
    assert step["end"] == pytest.approx(1.26 + (0.75 * 0.06 * 1.05 + 0.25 * 0.12))
    assert events[0][1] == "engine_step"
    assert sim.gates[2] == "engine"


def test_single_sequence_preserves_original_decode_cost():
    profile = [
        {"context_tokens": 1000, "normalized": {"1": 1.0, "4": 1.1}},
        {"context_tokens": 2000, "normalized": {"1": 1.0, "4": 1.2}},
    ]
    for context in (0, 1500, 3000):
        ratio = batch_ratio(profile, 1, context)
        assert ratio == 1.0
        for fraction in (0.0, 0.25, 1.0):
            assert decode_step_seconds([0.06], ratio, fraction) == pytest.approx(0.06)
