"""A resource-window decision reuses predictions and charges every check."""

import simulate_continuous_batch  # noqa: F401
from advice_calendar_search import AdviceCalendarSimulation, remaining_quote
from prefix_reclaim_search import PrefixReclaimSimulation
from test_yielding_decode_search import state
from yielding_decode_search import YieldingDecodeSimulation, advice_steps


def fixture():
    full = {
        "id": "past",
        "completed_search": True,
        "resolved": 4,
        "quanta": [{"CPU_wall": 5, "RPC_seconds": 1}, {"CPU_wall": 2}],
        "commit_CPU_wall": 0.25,
    }
    current = {"id": "q", "incumbent_calendar": {"D_acquire": 7.25}}
    return current, [
        full,
        {**full, "id": "future", "resolved": 6},
        {**full, "id": "partial", "completed_search": False},
    ]


def test_completed_remaining_cost_excludes_keep_RPC_partial_and_future():
    r, history = fixture()
    q = remaining_quote(r, history, 5)
    assert q["history"] == ["past"] and q["history_latest"] == 4
    assert q["predicted_remaining_CPU"] == 2.25 and q["cancel"]
    r["incumbent_calendar"]["D_acquire"] = 8.5  # known source-copy contention
    assert not remaining_quote(r, history, 5)["cancel"]


def test_unknown_window_and_cold_history_retain_optional_search():
    r, history = fixture()
    assert not remaining_quote(r, history[1:], 5)["cancel"]
    r["incumbent_calendar"]["D_acquire"] = None
    assert not remaining_quote(r, history, 5)["cancel"]


def test_reused_keep_prediction_contains_known_resource_work_without_resampling():
    shadow, route, layout, costs, calls = state()
    original = shadow.predict_batch
    copy = {
        "key": "other",
        "operation": "history",
        "start": 2,
        "end": 4,
        "resources": ["gate2", "gate0", "link:dp"],
    }

    def forecast(*args):
        p = original(*args)
        p.update(stage_start={("a", 2): 7}, operations=[copy])
        return p

    shadow.predict_batch = forecast
    step = next(advice_steps(shadow, "a", route, layout, costs, 1, 0))
    sim = object.__new__(AdviceCalendarSimulation)
    r = {"id": "a", "before": 2, "forecast_origin": 1, "model_latest": 0}
    sim.observe_advice_keep(r, step["incumbent_prediction"])
    assert len(calls) == 1
    assert r["incumbent_calendar"]["D_acquire"] == 7
    assert r["incumbent_calendar"]["operations"] == [copy]


def test_cancel_is_charged_and_remains_partial_without_claiming_move(monkeypatch):
    r, history = fixture()
    r.update(
        quanta=[{"kind": "keep"}], decision_ready=5, before=2, completed_search=False
    )
    sim = object.__new__(AdviceCalendarSimulation)
    sim.now = 5
    sim.advice_pending = "q", None, r
    sim.advice_records, sim.calendar_window_records = history + [r], []
    sim.jobs, sim.d_active = {"q": {}}, {}
    sim.queues = [[], [], ["q"], []]
    sim.pending, sim.rowmap, sim.active_sessions = [], {}, set()
    observed = []
    sim.resolve_advice = lambda status: observed.append((status, sim.now))
    monkeypatch.setattr(PrefixReclaimSimulation, "route", lambda _: None)
    timer = iter([10, 10.125])
    monkeypatch.setattr("advice_calendar_search.time.perf_counter", lambda: next(timer))
    sim.route()
    assert observed == [("predicted_acquisition_before_search", 5.125)]
    assert sim.calendar_window_records[0]["CPU_wall"] == 0.125
    assert not r["completed_search"]


def test_keep_observation_is_inside_original_charged_quantum(monkeypatch):
    sim = object.__new__(YieldingDecodeSimulation)
    r = {"quanta": [], "CPU_wall": 0, "RPC_seconds": 0}
    sim.advice_pending = "q", None, r
    sim.advice_iterator = iter(
        [
            {
                "kind": "keep",
                "selected": type("R", (), {"d": 2})(),
                "evaluations": [],
                "incumbent_prediction": {"known": 1},
            }
        ]
    )
    sim.now, sim.rpc_boundary = 5, 1
    observed = []
    sim.observe_advice_keep = lambda record, prediction: observed.append(prediction)
    sim.event = lambda *args: None
    timer = iter([10, 10.25])
    monkeypatch.setattr("yielding_decode_search.time.perf_counter", lambda: next(timer))
    sim.next_quantum()
    assert observed == [{"known": 1}] and r["CPU_wall"] == 0.25
    assert r["decision_ready"] == 6.25
