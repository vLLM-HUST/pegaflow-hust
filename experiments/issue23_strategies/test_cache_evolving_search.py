"""Publication, capacity fallback and owned lifetime contracts for the forecast."""

import json
import os
from dataclasses import replace
from pathlib import Path

import pytest
import simulate_continuous_batch
from batch_calendar import schedule_batched
from cache_evolving_search import CacheEvolvingSimulation, StageCacheProjection
from pegaflow.joint_policy import Calendar, Demand, Layout, Unit
from pegaflow.operation_schedule import Operation, Work
from pegaflow.resource_cache_profile import CacheRoute
from pegaflow.strategies.continuous_batch import BatchConfig


class Costs:
    bytes_per_token = 4096
    fits = [(0, 0, 0, 0, 0)] * 4

    def compute(self, node, tokens, prefix):
        return tokens - prefix

    def restore(self, node, prefix):
        return 0

    def decode_tail(self, node, tokens):
        return 128

    def copy_seconds(self, source, node, payload):
        return payload / 8192


def fixture():
    keys = tuple(bytes((i,)) for i in range(4))
    units = tuple(
        Unit(str(i), frozenset((k,)), 4096, ((k, 4096),))
        for i, k in enumerate(keys[:2])
    )
    layout = Layout((units, (), (), ()), (8192,) * 4, (0,) * 4)
    demand = Demand("a", "session", 5, keys, block_tokens=1)
    route = CacheRoute(
        p=0,
        d=2,
        source=0,
        p_blocks=2,
        d_blocks=2,
        predicted_p_end=0,
        predicted_first=0,
        predicted_end=0,
        extra_bytes=0,
        calendar=Calendar(),
    )
    return keys, units, layout, demand, route


def test_own_synchronous_save_can_destroy_the_quoted_d_prefix():
    _, _, layout, demand, route = fixture()
    work = Work(
        "a",
        0,
        2,
        0,
        0,
        (Operation("P_api", 3, ("gate0",)),),
        (
            Operation("handoff", 1, ("gate0", "gate2", "link:pd")),
            Operation("D_api", 131, ("gate2",), 3),
        ),
    )
    common = (
        (work,),
        0,
        lambda w, op, age: op.seconds,
        {"a": demand},
        {"a": route},
        Costs(),
        BatchConfig(token_budget=1, block_tokens=1, bytes_per_token=4096),
        {},
    )
    old = schedule_batched(*common)
    state = StageCacheProjection(
        layout, {"a": demand}, {"a": route}, Costs(), 512, {}, frozenset(), None
    )
    new = schedule_batched(*common, cache_state=state)
    assert state.quotes["a", 2]["effective_blocks"] == 0
    assert state.quotes["a", 2]["first_service_seconds"] == 5
    assert all(op["operation"] != "handoff" for op in new["operations"])
    assert new["first"]["a"] != old["first"]["a"]
    assert not state.leases.handles
    assert state.state.layout.feasible()


def test_staging_failure_preserves_the_available_local_prefix():
    keys, units, layout, demand, route = fixture()
    nodes = list(layout.units)
    nodes[2] = (units[0], Unit("other", frozenset((b"x",)), 4096, ((b"x", 4096),)))
    layout = replace(layout, units=tuple(nodes))
    state = StageCacheProjection(
        layout, {"a": demand}, {"a": route}, Costs(), 512, {}, frozenset(), None
    )
    ops = state.admit(Work("a", 0, 2, 0, 0, (), (), p_done=0), 2, (), None)
    assert state.accepted["a", 2] == 1
    assert [op.name for op in ops] == ["D_api"]
    assert layout.prefix(2, keys) == 1


def test_a_shared_owner_stays_protected_until_both_api_leases_finish():
    _, units, layout, demand, route = fixture()
    protected = tuple(replace(u, protected=True) for u in units)
    layout = replace(layout, units=(protected, (), (), ()))
    handles = {
        (("a", "P_api"), 0): frozenset(("0",)),
        (("b", "P_api"), 0): frozenset(("0",)),
    }
    state = StageCacheProjection(
        layout, {"a": demand}, {"a": route}, Costs(), 512, handles, frozenset(), None
    )
    state.leases.release(("a", "P_api"), 0)
    state.sync_protection(0)
    assert state.state.layout.units[0][0].protected
    state.leases.release(("b", "P_api"), 0)
    state.sync_protection(0)
    assert not state.state.layout.units[0][0].protected


def test_original_quote_only_contains_its_own_two_stages():
    path, seed = os.getenv("ISSUE23_SIM_INPUTS"), os.getenv("ISSUE23_SIM_SEED")
    if not path or not seed:
        pytest.skip("Explicit frozen input and isolated seed required")
    sim = simulate_continuous_batch
    inputs = json.loads(Path(path).read_bytes())
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    model = CacheEvolvingSimulation(
        inputs["rows"][:16],
        inputs["initial_inventory"],
        sim.Costs(**values),
        Path(seed),
        inputs["control_cost_rows"],
        0.49032341538142143,
        "ours",
        False,
        batch_config=BatchConfig(),
    )
    result = model.run()
    completed = {row["id"]: row for row in result["per_request"]}
    for key, quote in result["initial_cache_forecasts"].items():
        rows = quote["effective_API"]
        assert len(rows) == 2
        assert {r["request"] for r in rows} == {key}
        assert {r["node"] for r in rows} == {completed[key]["p"], completed[key]["d"]}
        assert quote["latest_completed"] <= quote["origin"]
    assert not (model.jobs or model.held or model.allocation_leases.handles)
