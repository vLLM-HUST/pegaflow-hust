"""Optional advice must not delay, duplicate or override acquired D work."""

from collections import deque
from dataclasses import make_dataclass, replace
from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from pegaflow.operation_schedule import Work
from progress_decode_search import (
    ProgressDecodeSimulation,
    commit_status,
    select_advice,
)
from test_cache_evolving_search import Costs, fixture


@pytest.mark.parametrize("delay,expected", [(0, 3), (10, 2)])
def test_incumbent_keeps_original_release_alternative_pays_advice(delay, expected):
    _, _, layout, item, route = fixture()
    current = Work("a", 0, 2, 0, 0, (), (), p_done=1, d_enqueued=1)
    calls = []
    shadow = SimpleNamespace(
        work={"a": current},
        demands={"a": item},
        routes={"a": route},
        refresh=lambda *_: None,
    )

    def predict(works, now, demands, routes, costs):
        w = works[0]
        calls.append(w)
        first = 10 if w.d == 2 else w.d_enqueued + 5
        return {"first": {"a": first}, "full": {"a": first + 128}, "P_end": {"a": 1}}

    shadow.predict_batch = predict
    costs = Costs()
    costs.decode_seconds = (128, 128)
    selected, rows = select_advice(
        shadow, "a", route, layout, costs, 1, delay, budget=4
    )
    assert selected.d == expected
    assert rows[0]["kind"] == "keep" and rows[0]["delay"] == 0
    assert calls[0] is current and len(calls) == 4
    assert all(w.p == 0 and w.p_done == 1 for w in calls)
    assert all(w.d == 3 and w.d_enqueued == 1 + delay for w in calls[1:])
    assert shadow.work["a"] is current


def test_acquisition_or_completion_cancels_before_queue_or_route_mutation():
    queues = [deque(), deque(["p"]), deque(["a", "other"]), deque()]
    before = [list(q) for q in queues]
    assert (
        commit_status("a", 2, 3, {"a": {}}, queues, {"a": 2}, 4)
        == "acquired_or_completed"
    )
    assert commit_status("a", 2, 3, {}, queues, {}, 4) == "acquired_or_completed"
    assert [list(q) for q in queues] == before
    assert commit_status("a", 2, 3, {"a": {}}, queues, {}, 4) == "move"
    queues[3].append("prior")
    assert (
        commit_status("a", 2, 3, {"a": {}}, queues, {}, 4)
        == "alternative_no_longer_available"
    )


def test_noop_and_cancellation_release_CPU_lane_without_holding_FIFO(monkeypatch):
    sim = object.__new__(ProgressDecodeSimulation)
    _, _, _, _, route = fixture()
    sim.jobs, sim.d_active = {"a": {}}, {"a": 2}
    sim.queues = [deque(), deque(), deque(["other"]), deque()]
    sim.batch_config = SimpleNamespace(max_sequences=4)
    record = {"before": 2, "decision_ready": 5}
    sim.advice_pending = "a", replace(route, d=3), record
    sim.now, sim.router_busy = 4, True
    sim.finish_advice()
    assert sim.advice_pending is not None and sim.router_busy
    monkeypatch.setattr(
        "progress_decode_search.time.perf_counter", iter([10, 10.25]).__next__
    )
    sim.now = 5
    sim.finish_advice()
    assert record["status"] == "acquired_or_completed"
    assert record["commit_queue_before"] == record["commit_queue_after"]
    assert sim.now == 5.25 and record["commit_CPU_wall"] == 0.25
    assert sim.advice_pending is None and not sim.router_busy


def test_missing_unacquired_queue_ownership_is_an_error():
    with pytest.raises(ValueError, match="FIFO"):
        commit_status("a", 2, 3, {"a": {}}, [deque() for _ in range(4)], {}, 4)


def test_commit_changes_only_owned_suffix_and_appends_to_alternate_FIFO(monkeypatch):
    sim = object.__new__(ProgressDecodeSimulation)
    _, _, _, _, route = fixture()
    work = Work("a", 0, 2, 0, 0, (), (), p_done=1, d_enqueued=1)
    sim.jobs = {"a": {"plan": SimpleNamespace(), "work": work, "live_route": route}}
    # The real plan is a dataclass; a small equivalent retains P fields too.
    sim.jobs["a"]["plan"] = plan = make_dataclass(
        "Plan", [("p", int), ("d", int), ("d_prefix", int)]
    )(0, 2, 256)
    observations = []
    ledger = SimpleNamespace(
        work={"a": work},
        jobs={"a": SimpleNamespace(d=2)},
        routes={"a": route},
        admission_choices={},
        progress_version=0,
        observe=lambda *args: observations.append(args),
    )
    sim.policy_queues, sim.d_active = ledger, {}
    sim.queues = [
        deque(["p-other"]),
        deque(),
        deque(["older", "a", "younger"]),
        deque(),
    ]
    sim.batch_config = SimpleNamespace(max_sequences=4)
    record = {"before": 2, "decision_ready": 5}
    sim.advice_pending = "a", replace(route, d=3, d_blocks=0), record
    sim.now, sim.router_busy = 5, True
    monkeypatch.setattr(
        "progress_decode_search.time.perf_counter", iter([10, 10.25]).__next__
    )
    sim.finish_advice()
    assert record["status"] == "move"
    assert list(sim.queues[0]) == ["p-other"]
    assert list(sim.queues[2]) == ["older", "younger"] and list(sim.queues[3]) == ["a"]
    assert sim.jobs["a"]["plan"].p == plan.p == 0
    assert ledger.work["a"].p_done == 1 and ledger.jobs["a"].d == 3
    assert observations == [("a", 3, "enqueued", 5)]
