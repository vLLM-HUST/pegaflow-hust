"""Small native-lifetime contracts; no model loading or hardware assertions."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from cache_evolving_search import StageCacheProjection
from pegaflow.joint_policy import Demand, Layout, Unit, copy_package
from pegaflow.resource_cache_profile import CacheForecast
from staged_save_model import SaveAllocationError, StagedCacheForecast, published_copy

SIZE = 4096


def unit(key, protected=False):
    return Unit(key.decode(), frozenset((key,)), SIZE, ((key, SIZE),), protected)


def fixture(capacity=12, old=10, new=6):
    layout = Layout(
        (tuple(unit(f"old-{i}".encode()) for i in range(old)), (), (), ()),
        (capacity * SIZE,) * 4,
        (0,) * 4,
    )
    keys = tuple(f"new-{i}".encode() for i in range(new))
    demand = Demand("save", "session", new * 128 + 1, keys)
    return layout, demand, SimpleNamespace(bytes_per_token=SIZE // 128)


def test_fitting_save_does_not_reclaim_its_unpublished_prefix():
    layout, demand, costs = fixture()
    old = CacheForecast(layout, {}, {}, costs, 512)
    new = StagedCacheForecast(layout, {}, {}, costs, 512)
    old.save(0, demand)
    new.save(0, demand)
    assert old.layout.prefix(0, demand.hashes) == 0
    assert new.layout.prefix(0, demand.hashes) == 6
    assert new.events[-1]["peak_unique_bytes"] == 12 * SIZE
    assert new.events[-1]["published_new_blocks"] == 6
    assert all(
        not v["owner"].startswith("staged:")
        for e in new.events
        if e["kind"] == "forecast_native_reclaim"
        for v in e["victims"]
    )


def test_failed_save_rolls_back_staging_not_evicted_old_entries():
    layout, demand, costs = fixture(capacity=4, old=3, new=6)
    model = StagedCacheForecast(layout, {}, {}, costs, 512)
    with pytest.raises(SaveAllocationError):
        model.save(0, demand)
    assert model.layout.usage(0) == 0
    assert not model.layout.keys(0)
    assert model.events[-1]["rolled_back_staging_bytes"] == 4 * SIZE
    assert model.events[-1]["published_new_blocks"] == 0


def test_unindexed_shared_owner_remains_charged_until_last_lease():
    layout, demand, costs = fixture(capacity=4, old=0, new=3)
    shared = Unit(
        "shared", frozenset((b"x", b"y")), 2 * SIZE, ((b"x", SIZE), (b"y", SIZE)), True
    )
    layout = replace(layout, units=((shared,), (), (), ()))
    handles = {
        (("a", "P_api"), 0): frozenset(("shared",)),
        (("b", "P_api"), 0): frozenset(("shared",)),
    }
    state = StageCacheProjection(layout, {}, {}, costs, 512, handles, frozenset(), None)
    state.state.__class__ = StagedCacheForecast
    with pytest.raises(SaveAllocationError):
        state.state.save(0, demand)
    assert not state.state.layout.keys(0)
    assert state.state.layout.usage(0) == 2 * SIZE
    state.leases.release(("a", "P_api"), 0)
    state.sync_protection(0)
    assert state.state.layout.usage(0) == 2 * SIZE
    state.leases.release(("b", "P_api"), 0)
    state.sync_protection(0)
    assert state.state.layout.usage(0) == 0


def test_filtered_hit_evicted_during_save_is_not_resaved():
    layout, demand, costs = fixture(capacity=4, old=3, new=3)
    demand = replace(demand, hashes=(b"old-0", *demand.hashes[:2]))
    model = StagedCacheForecast(layout, {}, {}, costs, 512)
    model.save(0, demand)
    assert b"old-0" not in model.layout.keys(0)
    assert model.events[-1]["filtered_hits"] == 1
    assert model.events[-1]["published_new_blocks"] == 2


@pytest.mark.parametrize("capacity", [4, 12])
def test_memo_preserves_success_and_failure_semantics(capacity):
    layout, demand, costs = fixture(capacity=capacity, old=3)
    memo = {"saves": {}, "hits": 0, "misses": 0}
    snapshots = []
    for at in (1, 9):
        state = StagedCacheForecast(layout, {}, {}, costs, 512, memo=memo)
        state.event_time = at
        try:
            state.save(0, demand)
            failed = False
        except SaveAllocationError:
            failed = True
        assert all(e["at"] == at for e in state.events)
        snapshots.append(
            (
                state.layout,
                failed,
                [{k: v for k, v in e.items() if k != "at"} for e in state.events],
            )
        )
    assert snapshots[0] == snapshots[1]
    assert memo["hits"] == memo["misses"] == 1


def test_allocation_rounding_and_hidden_bytes_are_charged_once():
    layout, demand, costs = fixture(capacity=2, old=0, new=1)
    layout = replace(layout, hidden=(SIZE, 0, 0, 0))
    costs.bytes_per_token = 1
    state = StagedCacheForecast(layout, {}, {}, costs, 512)
    state.save(0, demand)
    assert state.layout.usage(0) == 2 * SIZE
    assert state.events[-1]["staged_bytes"] == SIZE


def test_committed_copy_is_published_and_can_be_reclaimed():
    layout, demand, costs = fixture(capacity=4, old=3, new=3)
    copied = tuple(layout.keys(0))
    transition = copy_package(layout, 0, 2, copied)
    assert transition.layout.units[2][0].virtual
    actual = published_copy(layout, transition, "previous-copy")
    assert actual.prefix(2, copied) == 3
    assert not any(u.virtual for u in actual.units[2])
    state = StagedCacheForecast(actual, {}, {}, costs, 512, output_tokens=1)
    state.save(2, demand)
    assert state.layout.prefix(2, demand.hashes) == 3
    assert state.layout.usage(2) == 3 * SIZE
    assert all(v["owner"].startswith("published-copy:")
               for e in state.events if e["kind"] == "forecast_native_reclaim" for v in e["victims"])


def test_copy_prediction_uses_the_same_publication_transition():
    from pegaflow.operation_schedule import Operation

    layout, _demand, costs = fixture(capacity=4, old=3, new=3)
    keys = tuple(layout.keys(0))
    transition = copy_package(layout, 0, 2, keys)
    state = StagedCacheForecast(layout, {}, {}, costs, 512)
    state.pending_copies["copy", "handoff"] = 0, 2, keys
    state.finished(SimpleNamespace(key="copy"), 2, Operation("handoff", 1, ()), 1)
    assert state.layout == published_copy(layout, transition, "copy")
