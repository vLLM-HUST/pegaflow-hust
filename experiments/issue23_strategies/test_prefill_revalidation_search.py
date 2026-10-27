from dataclasses import replace
from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from late_decode_search import LateDecodeSnapshot
from pegaflow.operation_schedule import Work
from prefill_revalidation_search import require_waiting_P, select_prefill
from test_cache_evolving_search import Costs, fixture


@pytest.mark.parametrize(
    "change",
    [
        {"p_started": 0},
        {"p_done": 1},
        {"active_operations": (("history", 0),)},
        {"completed_operations": frozenset({"history"})},
    ],
)
def test_acquired_or_completed_P_cannot_be_rewritten(change):
    *_, route = fixture()
    work = Work("a", 0, 2, 0, 0, (), ())
    with pytest.raises(ValueError, match="unacquired"):
        require_waiting_P(replace(work, **change), route)


@pytest.mark.parametrize("delay, expected_prefix", [(0, 0), (20, 2)])
def test_global_profile_includes_new_control_and_keep_is_immediate(
    delay, expected_prefix
):
    _, _, layout, item, route = fixture()
    work = Work("a", 0, 2, 0, 0, (), ())
    shadow = object.__new__(LateDecodeSnapshot)
    shadow.work = {"a": work}
    shadow.demands = {"a": item}
    shadow.routes = {"a": route}
    shadow.refresh = lambda *_: None
    calls = []

    def predict(works, now, demands, routes, costs):
        key, charge = shadow.prefill_control
        value = 5 + charge if routes["a"].p_blocks == 0 else 10 + charge
        calls.append((key, charge, routes["a"].p_blocks))
        return {
            "first": {"a": value},
            "P_end": {"a": value - 1},
            "full": {"a": value + 128},
        }

    shadow.predict_batch = predict
    costs = Costs()
    costs.decode_seconds = (128, 128)
    selected, rows = select_prefill(shadow, "a", route, layout, costs, 0, delay)
    assert selected.p_blocks == expected_prefix
    assert rows[0]["delay"] == 0 and calls[0] == (None, 0, 2)
    assert all(r["delay"] == delay for r in rows[1:])
    assert len(calls) <= 3


def test_projection_control_keeps_FIFO_position_without_a_device_lease(monkeypatch):
    from prefill_revalidation_search import PrefillControlSnapshot

    monkeypatch.setattr(LateDecodeSnapshot, "configure_projection", lambda *_: {})
    projection = SimpleNamespace(admit=lambda *_: ("api",))
    shadow = object.__new__(PrefillControlSnapshot)
    *_, route = fixture()
    shadow.routes = {"a": route}
    shadow.prefill_control = ("a", 2.5)
    shadow.configure_projection(projection, 0)
    ops = projection.admit(Work("a", 0, 2, 0, 0, (), ()), 0, (), None)
    assert ops[0].name == "P_boundary_control"
    assert ops[0].seconds == 2.5 and ops[0].resources == ()
    assert ops[1:] == ("api",)
    assert projection.admit(Work("b", 0, 2, 1, 0, (), ()), 0, (), None) == ("api",)


def test_full_calendar_does_not_let_younger_P_overtake_control(monkeypatch):
    from batch_calendar import schedule_batched
    from cache_evolving_search import StageCacheProjection
    from pegaflow.strategies.continuous_batch import BatchConfig
    from prefill_revalidation_search import PrefillControlSnapshot

    monkeypatch.setattr(LateDecodeSnapshot, "configure_projection", lambda *_: {})
    _, _, layout, item, route = fixture()
    demands = {"a": item, "b": replace(item, key="b")}
    routes = {"a": route, "b": route}
    works = tuple(Work(k, 0, 2, i, 0, (), ()) for i, k in enumerate(("a", "b")))
    projection = StageCacheProjection(
        layout, demands, routes, Costs(), 512, {}, frozenset(), None
    )
    shadow = object.__new__(PrefillControlSnapshot)
    shadow.routes, shadow.prefill_control = routes, ("a", 2.5)
    shadow.configure_projection(projection, 0)
    prediction = schedule_batched(
        works,
        0,
        lambda w, op, age: op.seconds,
        demands,
        routes,
        Costs(),
        BatchConfig(token_budget=1, block_tokens=1, bytes_per_token=4096),
        {},
        cache_state=projection,
    )
    assert prediction["P_end"]["a"] == 5.5
    assert prediction["P_end"]["b"] > prediction["P_end"]["a"]
