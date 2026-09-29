"""Hybrid-cache prefix reconciliation for attention and recurrent groups."""

from .unit_stubs import install_connector_unit_stubs

install_connector_unit_stubs()

from pegaflow.connector.common import reconcile_hybrid_hit  # noqa: E402


def _hits(*per_shard_positions: tuple[int, ...]) -> tuple[tuple[int, ...], ...]:
    return tuple(tuple(positions) for positions in per_shard_positions)


def test_rightmost_checkpoint_within_attention_prefix():
    assert reconcile_hybrid_hit(3, (_hits((2,)),)) == (3, 2, frozenset({2}))


def test_checkpoint_beyond_attention_prefix_is_unusable():
    assert reconcile_hybrid_hit(2, (_hits((2,)),)) == (0, None, frozenset())


def test_attention_without_recurrent_checkpoint_recomputes():
    assert reconcile_hybrid_hit(3, (_hits(()),)) == (0, None, frozenset())


def test_every_tp_shard_must_hold_the_checkpoint():
    assert reconcile_hybrid_hit(4, (_hits((1, 2), (2, 3)),)) == (
        3,
        2,
        frozenset({2}),
    )


def test_every_recurrent_group_must_hold_the_checkpoint():
    assert reconcile_hybrid_hit(3, (_hits((0, 2)), _hits((1, 2)))) == (
        3,
        2,
        frozenset({2}),
    )
    assert reconcile_hybrid_hit(3, (_hits((0, 2)), _hits((1,)))) == (
        0,
        None,
        frozenset(),
    )


def test_sparse_membership_uses_rightmost_common_boundary():
    assert reconcile_hybrid_hit(3, (_hits((0, 2)),)) == (
        3,
        2,
        frozenset({0, 2}),
    )


def test_usable_boundaries_survive_for_budget_rederivation():
    assert reconcile_hybrid_hit(4, (_hits((1, 3)),)) == (
        4,
        3,
        frozenset({1, 3}),
    )
