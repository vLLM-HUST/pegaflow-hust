"""A diagnostic clone must not copy live locks/models or alias mutable memos."""

import threading
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401 - bind retained overlay
from pegaflow.operation_phase_release import PhaseCosts
from pegaflow.projection_index import NodeIndexes
from profile_paired_protection import cloned_shadow
from release_cobatch_search import ReleaseCobatchSnapshot
from test_cache_evolving_search import fixture


def test_clone_excludes_live_model_and_preserves_frozen_cost_identity():
    costs = PhaseCosts(SimpleNamespace(), (), 128)
    shadow = object.__new__(ReleaseCobatchSnapshot)
    shadow.__dict__ = {
        "models": SimpleNamespace(guard=threading.RLock()),
        "guard": threading.RLock(),
        "local_decision": threading.local(),
        "context": (None, None, costs, 0, 0),
        "phase_costs": costs,
        "search_stats": {"bound_checks": 1},
        "cache_batch_memo": {"a": {"first": {"r": 2.0}}},
    }
    replica, arguments = cloned_shadow(shadow, ((), 0, {}, {}, costs), object)
    assert "models" not in replica.__dict__ and "guard" not in replica.__dict__
    assert arguments[-1] is replica.phase_costs is replica.context[2] is costs
    replica.search_stats["bound_checks"] += 1
    replica.cache_batch_memo["a"]["first"]["r"] = 99
    assert shadow.search_stats == {"bound_checks": 1}
    assert shadow.cache_batch_memo["a"]["first"]["r"] == 2


def test_warm_physical_indexes_keep_identity_without_shared_counters():
    _, _, layout, _, _ = fixture()
    indexes = NodeIndexes()
    index = indexes.get(layout.units[0])
    assert index.payload
    costs = PhaseCosts(SimpleNamespace(), (), 128)
    shadow = object.__new__(ReleaseCobatchSnapshot)
    shadow.__dict__ = {
        "projected_layout": layout,
        "projection_memo": {"node_indexes": indexes, "saves": {}, "hits": 0},
    }
    replica, _ = cloned_shadow(shadow, ((), 0, {}, {}, costs), object)
    new_indexes = replica.projection_memo["node_indexes"]
    assert replica.projected_layout is not layout
    assert replica.projected_layout.units[0] is layout.units[0]
    actual = new_indexes.get(replica.projected_layout.units[0])
    assert actual.units is index.units and actual.payload == index.payload
    assert actual is not index and new_indexes.hits == 1 and indexes.hits == 0
