"""Late D selection cannot reserve a slot early or change acquired ownership."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from batch_calendar import schedule_batched
from late_decode_search import require_unacquired, select_destination, should_reconsider
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig
from test_cache_evolving_search import Costs, fixture


def test_trigger_uses_observed_admission_availability_not_balance_quota():
    assert should_reconsider(2, {2: 4, 3: 1}, {2: 0, 3: 0}, 4)
    assert not should_reconsider(2, {2: 3, 3: 0}, {2: 0, 3: 0}, 4)
    assert not should_reconsider(2, {2: 4, 3: 4}, {2: 0, 3: 0}, 4)
    assert not should_reconsider(2, {2: 4, 3: 0}, {2: 0, 3: 1}, 4)


def test_future_control_completion_owns_no_D_slot_and_preserves_other_FIFO():
    _, _, _, item, route = fixture()
    decode = (Operation("D_api", 133, ("gate2",), 5),)
    late = Work("late", 0, 2, 0, 0, (), decode, p_done=0)
    ready = replace(late, key="ready", order=1)
    arguments = (
        (late, ready),
        0,
        lambda w, o, t: max(0, o.seconds - t),
        {k: replace(item, key=k) for k in ("late", "ready")},
        {k: route for k in ("late", "ready")},
        Costs(),
        BatchConfig(
            max_sequences=1, token_budget=1, block_tokens=1, bytes_per_token=4096
        ),
        {},
    )
    original = schedule_batched(*arguments)
    delayed = schedule_batched(*arguments, admission_releases={"late": 10})
    assert original["stage_start"]["late", 2] == 0
    assert delayed["stage_start"]["ready", 2] == 0
    assert delayed["stage_start"]["late", 2] >= 10
    assert delayed["stage_start"]["late", 2] >= delayed["full"]["ready"]
    assert delayed["first"]["late"] > original["first"]["late"]


@pytest.mark.parametrize(
    "fields",
    [
        {"p_done": None},
        {"d_started": 2},
        {"actual_first": 3},
        {"active_operations": (("handoff", 1),)},
        {"completed_operations": frozenset({"D_api"})},
    ],
)
def test_acquired_or_unpublished_suffix_rejected(fields):
    _, _, _, _, route = fixture()
    work = Work("a", 0, 2, 0, 0, (), (), p_done=1)
    require_unacquired(work, route)
    with pytest.raises(ValueError):
        require_unacquired(replace(work, **fields), route)


def test_both_destinations_are_priced_under_one_budget_with_owned_P_fixed():
    _, _, layout, item, route = fixture()
    work = Work("a", 0, 2, 0, 0, (), (), p_done=1)
    calls = []
    shadow = SimpleNamespace(
        work={"a": work},
        demands={"a": item},
        routes={"a": route},
        refresh=lambda layout, costs: None,
    )

    def predict(works, now, demands, routes, costs):
        w = works[0]
        calls.append((w, routes["a"]))
        first = 20 if w.d == 2 else 10
        return {"first": {"a": first}, "full": {"a": first + 1}, "P_end": {"a": 1}}

    shadow.predict_batch = predict
    costs = Costs()
    costs.decode_seconds = (128, 128)
    selected, rows = select_destination(
        shadow, "a", route, layout, costs, 1, 2, budget=4
    )
    assert selected.d == 3
    assert len(calls) == len(rows) == 4
    assert [w.d for w, _ in calls] == [2, 3, 2, 3]
    assert all(w.p == 0 and w.p_done == 1 and w.d_not_before == 3 for w, _ in calls)
    assert shadow.work["a"] is work and shadow.routes["a"] == route
