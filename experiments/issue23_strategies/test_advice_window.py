"""Deadline estimate must retain sync-save, observed progress and causal history."""

from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from advice_window_search import AdviceWindowSimulation, trigger_quote
from pegaflow.strategies.continuous_batch import Sequence


def fixture():
    active = {
        k: Sequence(
            key=k,
            phase="decode",
            prompt_tokens=512,
            prefix_tokens=0,
            first_seconds=1,
            tail_seconds=128,
            output_tokens=128,
            decoded=d,
        )
        for k, d in (("a", 127), ("b", 1))
    }
    costs = SimpleNamespace(
        decode_tail=lambda n, p: 128.0,
        fits={2: [0, 0, 0, 0, 1.0]},
        latest_completed_seconds=4,
    )
    records = [
        {"id": "past", "completed_search": True, "decision_ready": 4, "CPU_wall": 2},
        {
            "id": "partial",
            "completed_search": False,
            "decision_ready": 3,
            "CPU_wall": 100,
        },
        {
            "id": "future",
            "completed_search": True,
            "decision_ready": 6,
            "CPU_wall": 100,
        },
    ]
    return active, costs, records


def test_only_completed_history_and_remaining_decode_plus_save_define_window():
    active, costs, records = fixture()
    q = trigger_quote("q", 2, ["q"], active, 2, records, costs, 5, 0)
    assert q["history"] == ["past"] and q["CPU_latest"] == 4
    assert q["release_estimates"] == {"a": 2.0, "b": 128.0}
    assert q["skip"]  # Equality cannot finish the full search before release.
    records[0]["CPU_wall"] = 1.5
    assert not trigger_quote("q", 2, ["q"], active, 2, records, costs, 5, 0)["skip"]


@pytest.mark.parametrize("unknown", ["cold", "later_fifo", "mixed_phase", "free_slot"])
def test_unknown_windows_do_not_invent_a_deadline(unknown):
    active, _costs, records = fixture()
    queue, capacity = ["q"], 2
    if unknown == "cold":
        records = records[1:]
    elif unknown == "later_fifo":
        queue = ["other", "q"]
    elif unknown == "mixed_phase":
        active["a"].phase = "save"
    else:
        capacity = 3
    q = trigger_quote("q", 2, queue, active, capacity, records, None, 5, 0)
    assert not q["skip"] and not q["release_estimates"]


def test_trigger_cost_advances_controller_without_claiming_search_completion(
    monkeypatch,
):
    active, costs, records = fixture()
    sim = object.__new__(AdviceWindowSimulation)
    sim.now = 5.0
    sim.jobs = {"q": {"live_route": SimpleNamespace(d=2)}}
    sim.queues = {2: ["q"]}
    sim.d_batches = {2: SimpleNamespace(active=active, busy=None)}
    sim.batch_config = SimpleNamespace(max_sequences=2, shared_fraction=0)
    sim.held = {}
    sim.batch_trace = []
    sim.advice_records, sim.advice_trigger_records = records, []
    sim.rpc_boundary, sim.frozen = 0, lambda: costs
    counter = iter([10.0, 10.25])
    monkeypatch.setattr("advice_window_search.time.perf_counter", lambda: next(counter))
    assert not sim.advice_trigger("q")
    assert sim.now == 5.25 and len(sim.advice_records) == 3
    assert sim.advice_trigger_records[0]["CPU_wall"] == 0.25


def test_issued_work_is_predicted_without_reading_sampled_world_duration():
    active, costs, records = fixture()
    q = trigger_quote(
        "q",
        2,
        ["q"],
        active,
        2,
        records,
        costs,
        5,
        0,
        busy_pieces=[("b", 2)],
        sharing=0,
    )
    assert q["estimated_busy_seconds"] == 2 and q["release_estimates"]["a"] == 4
    assert not q["skip"]
    q = trigger_quote("q", 2, ["q"], active, 2, records, None, 5, 0, external_gate=True)
    assert not q["skip"] and not q["release_estimates"]
