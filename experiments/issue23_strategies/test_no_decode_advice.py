"""Disable only optional D advisory launch, preserving normal routing."""

from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401
from no_decode_advice_search import NoDecodeAdviceSimulation
from prefix_reclaim_search import PrefixReclaimSimulation
from release_cobatch_search import ReleaseCobatchSimulation
from yielding_decode_search import YieldingDecodeSimulation


def test_disabled_advice_still_executes_initial_route_and_never_reads_optional_state(monkeypatch):
    seen = []
    monkeypatch.setattr(ReleaseCobatchSimulation, "route", lambda s: seen.append(s))
    state = SimpleNamespace(
        advice_pending=None, mode="ours", router_busy=False, enable_decode_advice=False
    )
    # No queue/d_active fields: accessing them means optional work was attempted.
    YieldingDecodeSimulation.route(state)
    assert seen == [state]


def test_off_switch_is_per_instance_not_a_parent_or_physics_mutation(monkeypatch):
    monkeypatch.setattr(PrefixReclaimSimulation, "__init__", lambda self: None)
    off = NoDecodeAdviceSimulation()
    parent = PrefixReclaimSimulation()
    assert off.enable_decode_advice is False
    assert not hasattr(parent, "enable_decode_advice")
