"""Differential checks for owned leases, intrinsic guards and new publications."""

import random
from dataclasses import replace
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401 - bind retained overlay
from delta_protection_search import DeltaLeases, DeltaProtectionProjection
from dirty_scan_search import DirtyScanProjection
from release_cobatch_search import ReleaseCobatchSnapshot
from sparse_cache_search import SparseCacheProjection
from test_cache_evolving_search import Costs, fixture


def test_dirty_tracks_zero_crossings_and_releases_the_owned_set():
    leases = DeltaLeases()
    leases.acquire("a", 0, ("x", "y"))
    assert leases.dirty[0] == {"x", "y"}
    leases.dirty[0].clear()
    leases.acquire("b", 0, ("x",))
    assert not leases.dirty[0]
    assert leases.release("a", 0) == {"x", "y"}
    assert leases.dirty[0] == {"y"} and leases.protected(0, "x")
    leases.dirty[0].clear()
    leases.release("b", 0)
    assert leases.dirty[0] == {"x"} and not leases.protected(0, "x")


def test_delta_matches_full_scan_with_shared_owners_and_publication():
    _, units, layout, demand, route = fixture()
    states = [
        cls(
            layout,
            {"a": demand},
            {"a": route},
            Costs(),
            512,
            {},
            frozenset(((0, "1"),)),
            None,
        )
        for cls in (
            SparseCacheProjection,
            DeltaProtectionProjection,
            DirtyScanProjection,
        )
    ]
    rng = random.Random(20261027)
    for step in range(100):
        owner = str(rng.randrange(4))
        for state in states:
            if (owner, 0) in state.leases.handles:
                state.leases.release(owner, 0)
            else:
                state.leases.acquire(owner, 0, ("0", "1"))
            if step % 7 == 0:
                state.state.layout = replace(
                    state.state.layout,
                    units=(
                        (replace(units[0], identity=f"new-{step}"), units[1]),
                        (),
                        (),
                        (),
                    ),
                )
            state.sync_protection(0)
        assert (
            states[0].state.layout == states[1].state.layout == states[2].state.layout
        )
        for state in states[1:]:
            before = state.state.layout
            state.sync_protection(0)
            assert state.state.layout is before
    assert states[1].state.layout.units[0][1].protected


def test_parent_D_control_and_owned_slot_delay_are_preserved():
    _, _, layout, demand, route = fixture()
    for started, boundary, known in [
        (None, None, {}),
        (2, None, {"a": 3.0}),
        (2, "a", {}),
    ]:
        states = [
            cls(
                layout, {"a": demand}, {"a": route}, Costs(), 512, {}, frozenset(), None
            )
            for cls in (
                ReleaseCobatchSnapshot.projection_type,
                DeltaProtectionProjection,
                DirtyScanProjection,
            )
        ]
        work = SimpleNamespace(
            key="a", d_started=started, p_started=None, active_operations=()
        )
        outcomes = []
        for state in states:
            state.control_profile = {"D_delay": 7.0, "active_D_remaining": known}
            state.boundary_request = boundary
            outcomes.append(state.admit(work, 2, (), None))
        assert outcomes[0] == outcomes[1] == outcomes[2]
        assert outcomes[1][0].name == "D_control"
        assert outcomes[1][0].seconds == known.get("a", 7.0)
        assert not outcomes[1][0].resources
