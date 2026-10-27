"""Controller delay is a release constraint, not extra device ownership."""

from dataclasses import replace

import simulate_continuous_batch  # noqa: F401
from batch_calendar import schedule_batched
from control_release_search import ControlReleaseProjection, completed_mean
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig
from test_cache_evolving_search import Costs, fixture


def test_delayed_routing_does_not_block_an_already_admitted_p_request():
    _, _, _, demand, route = fixture()
    routes = {key: replace(route, p_blocks=0, d_blocks=0) for key in ("a", "b")}
    demands = {key: replace(demand, key=key) for key in routes}
    work = tuple(
        Work(
            key,
            0,
            2,
            i,
            0,
            (Operation("P_api", i + 1, ("gate0",)),),
            (Operation("D_api", 133, ("gate2",), 5),),
        )
        for i, key in enumerate(routes)
    )
    prediction = schedule_batched(
        work,
        0,
        lambda w, op, age: op.seconds,
        demands,
        routes,
        Costs(),
        BatchConfig(token_budget=1, block_tokens=1, bytes_per_token=4096),
        {},
        admission_releases={"a": 3},
    )
    assert prediction["P_end"] == {"b": 2, "a": 4}
    assert prediction["stage_start"]["a", 0] == 3


def test_d_control_holds_a_sequence_slot_but_not_the_device_gate():
    _, _, layout, demand, route = fixture()
    routes = {key: replace(route, p_blocks=0, d_blocks=0) for key in ("a", "b")}
    demands = {key: replace(demand, key=key) for key in routes}
    work = tuple(
        Work(
            key,
            0,
            2,
            i,
            0,
            (),
            (Operation("D_api", 133, ("gate2",), 5),),
            p_done=0,
            d_started=0 if key == "b" else None,
        )
        for i, key in enumerate(routes)
    )
    state = ControlReleaseProjection(
        layout, demands, routes, Costs(), 512, {}, frozenset(), None
    )
    state.control_profile = {"D_delay": 3, "active_D_remaining": {}}
    state.boundary_request = None
    result = schedule_batched(
        work,
        0,
        lambda w, op, age: op.seconds,
        demands,
        routes,
        Costs(),
        BatchConfig(
            max_sequences=2, token_budget=1, block_tokens=1, bytes_per_token=4096
        ),
        {},
        cache_state=state,
    )
    control = [r for r in result["operations"] if r["operation"] == "D_control"]
    assert len(control) == 1 and control[0]["resources"] == ()
    assert control[0]["start"] == 0 and control[0]["end"] == 3
    assert result["first"]["b"] < result["first"]["a"]
    assert not state.leases.handles


def test_control_estimator_excludes_unfinished_samples():
    rows = [{"seconds": 2, "completed": 4}, {"seconds": 100, "completed": 110}]
    assert completed_mean(rows, 3) == {"mean": 0, "samples": 0, "latest": None}
    assert completed_mean(rows, 5) == {"mean": 2, "samples": 1, "latest": 4}
