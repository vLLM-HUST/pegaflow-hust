"""Preserve allocator forecast semantics, including protected/shared owners."""

import random
from dataclasses import replace
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401 -- bind the frozen runtime overlay first
from linear_reclaim_search import linear_remove_victims
from pegaflow.joint_policy import Demand, Layout, Unit
from pegaflow.resource_cache_profile import CacheForecast
from prefix_reclaim_search import prefix_remove_victims


def test_first_equal_removal_with_aliases_and_equal_values():
    a = Unit("a", frozenset((b"a",)), 8, ((b"a", 8),))
    b = Unit("b", frozenset((b"b",)), 8, ((b"b", 8),))
    units = [a, b, replace(a), a, b]
    for victims in ([a], [a, a], [b, a], [a, b, a, a], []):
        old = CacheForecast.remove_victims(list(units), victims)
        for method in (linear_remove_victims, prefix_remove_victims):
            new = method(units, victims)
            assert new == old
            assert all(x is y for x, y in zip(old, new, strict=True))


def test_save_reclaim_and_memo_are_identical_for_random_owner_layouts():
    rng = random.Random(0)
    total_removals = 0
    for scenario in range(100):
        units = []
        for i in range(rng.randrange(3, 50)):
            keys = tuple(bytes((i, j)) for j in range(rng.randrange(1, 4)))
            units.append(Unit(
                str(i), frozenset(keys), 8 * len(keys), tuple((k, 8) for k in keys),
                protected=bool(rng.randrange(4) == 0), virtual=bool(rng.randrange(5) == 0),
            ))
        hidden = rng.randrange(3) * 8
        capacity = sum(u.charged for u in units) + hidden + rng.randrange(3) * 8
        layout = Layout((tuple(units), (), (), ()), (capacity,) * 4, (hidden, 0, 0, 0))
        # Existing keys, multiple new saves, D output-only occupancy, no eligible
        # victim and reclaim batches larger than inventory are all exercised.
        demand = Demand("x", "s", 14, (b"\x00\x00", *(bytes((255, j)) for j in range(12))),
                        block_tokens=1)
        batch = (1, 3, 512)[scenario % 3]
        pair = []
        for remover in (CacheForecast.remove_victims, linear_remove_victims, prefix_remove_victims):
            memo = {"saves": {}, "hits": 0, "misses": 0}
            state = CacheForecast(layout, {}, {}, SimpleNamespace(bytes_per_token=8),
                                  batch, output_tokens=4, memo=memo)
            state.remove_victims = remover
            state.save(0, demand)
            state.save(2, demand)
            state.save(0, replace(demand, key="y"))
            pair.append(state)
        a = pair[0]
        for b in pair[1:]:
            assert a.layout == b.layout
            assert a.events == b.events
            assert a.memo == b.memo
            assert a.reclaim_removal_calls == b.reclaim_removal_calls
        total_removals += a.reclaim_removal_calls
    assert total_removals > 100


def test_actual_eligible_prefix_needs_neither_hashing_nor_equality():
    class Owner:
        virtual = False

        def __init__(self, protected=False):
            self.protected = protected

        def __hash__(self):
            raise AssertionError("No victim hashing needed for this contract")

        def __eq__(self, other):
            raise AssertionError("Actual prefix owners have exact identities")

    units = [Owner(True), Owner(), Owner(), Owner(True), Owner()]
    kept = prefix_remove_victims(units, [units[1], units[2]])
    assert len(kept) == 3
    assert all(a is b for a, b in zip(kept, (units[0], units[3], units[4]), strict=True))
