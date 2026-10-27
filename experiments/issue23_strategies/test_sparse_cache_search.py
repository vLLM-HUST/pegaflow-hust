"""Differential protection checks across overlapping owners and publication."""

import random
from dataclasses import replace

import simulate_continuous_batch  # noqa: F401
from cache_evolving_search import StageCacheProjection
from sparse_cache_search import SparseCacheProjection
from test_cache_evolving_search import Costs, fixture


def test_sparse_protection_preserves_owner_semantics_and_unchanged_identity():
    _, units, layout, demand, route = fixture()
    intrinsic = frozenset(((0, "1"),))
    old, new = [
        cls(layout, {"a": demand}, {"a": route}, Costs(), 512, {}, intrinsic, None)
        for cls in (StageCacheProjection, SparseCacheProjection)
    ]
    rng = random.Random(20261026)
    for step in range(100):
        owner = str(rng.randrange(4))
        for state in (old, new):
            if (owner, 0) in state.leases.handles:
                state.leases.release(owner, 0)
            else:
                state.leases.acquire(owner, 0, ("0", "1"))
        if step % 7 == 0:
            # Publication changes identities/layout independently of lease calls.
            for state in (old, new):
                state.state.layout = replace(
                    state.state.layout,
                    units=(
                        (replace(units[0], identity=f"new-{step}"), units[1]),
                        (),
                        (),
                        (),
                    ),
                )
        before = new.state.layout
        old.sync_protection(0)
        new.sync_protection(0)
        assert new.state.layout == old.state.layout
        for a, b in zip(before.units[0], new.state.layout.units[0], strict=True):
            if a.protected == b.protected:
                assert a is b
        after = new.state.layout
        new.sync_protection(0)
        assert new.state.layout is after
    assert new.state.layout.units[0][1].protected
