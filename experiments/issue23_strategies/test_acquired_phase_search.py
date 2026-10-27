"""Acquired H2D/save cannot be replayed or preempted by a new copy."""

from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from acquired_phase_search import acquired_quote
from batch_calendar import schedule_batched
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig, Sequence
from test_continuous_batch_calendar import Costs


def test_age_conditions_only_completed_matching_phase_samples():
    cost = SimpleNamespace(restore=lambda n, p: p / 10, fits=[(None, 0, 0, 0, 1)] * 4)
    event = {
        "node": 2,
        "kind": "restore",
        "start": 10,
        "pieces": (("a", 0, "restore"),),
    }
    sequence = Sequence("a", 30, 20, 1, 1)
    samples = [
        {"node": 2, "prefix": 20, "restore": x, "available": t}
        for x, t in ((2, 0), (4, 1), (1000, 30))
    ]
    quote = acquired_quote(event, sequence, cost, samples, 13, 0)
    assert quote["remaining"] == 1  # Only the four-second completed survivor.
    assert quote["latest"] == 1
    assert quote["samples"] == 2
    assert acquired_quote(event, sequence, cost, samples, 15, 0)["remaining"] == 3
    with pytest.raises(ValueError):
        acquired_quote(event, sequence, cost, samples, 9, 0)


@pytest.mark.parametrize("phase", ["restore", "save"])
def test_acquired_exclusive_phase_holds_gate_and_finishes_once(phase):
    work = [
        Work(
            "a",
            0,
            2,
            0,
            0,
            (),
            (Operation("D_api", 137, ("gate2",), 9),),
            p_done=0,
            active_operations=(("D_api", 0),),
            actual_first=0 if phase == "save" else None,
        ),
        Work(
            "b",
            1,
            3,
            1,
            0,
            (),
            (
                Operation("handoff", 3, ("gate2", "gate3", "link:dd")),
                Operation("D_api", 137, ("gate3",), 9),
            ),
            p_done=0,
        ),
    ]
    progress = {
        "a": Sequence(
            "a",
            9,
            0,
            9,
            127,
            phase=phase,
            prefilled=9 if phase == "save" else 0,
            decoded=128 if phase == "save" else 0,
        )
    }
    result = schedule_batched(
        work,
        10,
        lambda w, o, a: o.seconds,
        {w.key: SimpleNamespace(tokens=9, block_tokens=1) for w in work},
        {w.key: SimpleNamespace(d_blocks=0) for w in work},
        Costs(),
        BatchConfig(token_budget=4, bytes_per_token=1),
        progress,
        inflight_iterations={
            2: {
                "origin": 10,
                "started": 7,
                "remaining": 2,
                "kind": phase,
                "pieces": (("a", 0, phase),),
            }
        },
    )
    copy = next(e for e in result["operations"] if e["operation"] == "handoff")
    assert copy["start"] == 12
    if phase == "save":
        assert result["full"]["a"] == 12
    else:
        assert result["first"]["a"] >= 12 + 9 * (1 + 2 / 7)
