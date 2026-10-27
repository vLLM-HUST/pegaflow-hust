"""Yield points preserve uninterrupted selection and stop obsolete work."""

from collections import deque
from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from pegaflow.operation_schedule import Work
from progress_decode_search import select_advice
from test_cache_evolving_search import Costs, fixture
from yielding_decode_search import YieldingDecodeSimulation, advice_steps, cancellation


def state():
    _, _, layout, item, route = fixture()
    work = Work("a", 0, 2, 0, 0, (), (), p_done=1, d_enqueued=1)
    calls = []
    shadow = SimpleNamespace(
        work={"a": work},
        demands={"a": item},
        routes={"a": route},
        refresh=lambda *_: None,
    )

    def forecast(works, now, demands, routes, costs):
        w = works[0]
        calls.append((w.d, routes["a"].d_blocks))
        first = 10 if w.d == 2 else 5 + w.d_enqueued + routes["a"].d_blocks
        return {"first": {"a": first}, "full": {"a": first + 128}, "P_end": {"a": 1}}

    shadow.predict_batch = forecast
    costs = Costs()
    costs.decode_seconds = (128, 128)
    return shadow, route, layout, costs, calls


@pytest.mark.parametrize("delay", [0, 10])
def test_uninterrupted_steps_equal_original_candidate_sequence_and_choice(delay):
    shadow, route, layout, costs, expected_calls = state()
    expected = select_advice(shadow, "a", route, layout, costs, 1, delay, budget=4)
    shadow, route, layout, costs, calls = state()
    steps = advice_steps(shadow, "a", route, layout, costs, 1, delay, budget=4)
    quanta = []
    while True:
        try:
            quanta.append(next(steps))
        except StopIteration as done:
            actual = done.value
            break
    assert actual == expected and calls == expected_calls
    assert len(quanta) == 4 and quanta[0]["kind"] == "keep"


def test_closing_after_one_calendar_never_computes_remaining_candidates():
    shadow, route, layout, costs, calls = state()
    steps = advice_steps(shadow, "a", route, layout, costs, 1, 0, budget=4)
    assert not calls
    assert next(steps)["kind"] == "keep" and len(calls) == 1
    steps.close()
    assert len(calls) == 1
    with pytest.raises(StopIteration):
        next(steps)


def test_only_observed_acquisition_and_eligible_causal_request_cancel():
    queues = [deque(), deque(), deque(["a"]), deque()]
    rowmap = {"future-session-turn": {"task": "busy"}, "ready": {"task": "free"}}
    args = ("a", 2, {"a": {}}, {}, queues)
    assert cancellation(*args, ["future-session-turn"], rowmap, {"busy"}) is None
    assert cancellation(*args, ["ready"], rowmap, {"busy"}) == "route_priority"
    assert (
        cancellation("a", 2, {"a": {}}, {"a": 2}, queues, [], {}, set())
        == "acquired_or_completed"
    )
    assert (
        cancellation("a", 2, {}, {}, queues, [], {}, set()) == "acquired_or_completed"
    )
    assert list(queues[2]) == ["a"]


def test_setup_charge_does_not_backdate_acquired_snapshot_epoch(monkeypatch):
    shadow, route, layout, costs, _ = state()
    costs.latest_completed_seconds = 1.0
    sim = object.__new__(YieldingDecodeSimulation)
    sim.now = 1.25
    sim.jobs = {"a": {"live_route": route}}
    sim.advice_records = []
    sim.layout, sim.rpc_boundary = layout, 0.1
    sim.frozen = lambda: costs
    sim.policy_queues = SimpleNamespace(planning_snapshot=lambda: shadow)
    observed = []

    def check_epoch(snapshot, key, route, layout, costs, now, delay):
        assert now == sim.now  # Acquired phase reader has this exact epoch.
        observed.append(now)
        yield {"kind": "keep"}
        return route, []

    monkeypatch.setattr("yielding_decode_search.advice_steps", check_epoch)
    record = {"origin": 1.0, "setup_CPU_wall": 0.25}
    step = sim.make_steps("a", record)
    next(step)
    assert record["forecast_origin"] == 1.25 and record["origin"] == 1.0
    assert observed == [1.25]
    step.close()
