"""Same aggregate service with different blocking locations and valid phases."""

from types import SimpleNamespace

from phase_residual_world import joint_phase_split


def test_joint_row_preserves_aggregate_but_moves_exclusive_occupancy():
    world = SimpleNamespace(
        restore=lambda n, p: 2.0 if p else 0.0, fits=((None, None, 0, 0, 1.0),) * 4
    )
    row = {
        "id": "calibration-only",
        "num_cached_tokens": 128,
        "raw_metrics": {
            "queued_ts": 10,
            "scheduled_ts": 15,
            "first_token_ts": 18,
            "last_token_ts": 19,
        },
    }
    seed = [(2, row, {"first_seconds": 8, "complete_seconds": 11})]
    restore, save, result = joint_phase_split(seed, world, "new", 2, 256, 8, 3)
    assert (restore, save) == (5, 2)
    assert restore + result["remaining_first_path"] == 8
    assert save + result["decode_path"] == 3
    assert result["calibration_id"] == "calibration-only"
    assert joint_phase_split(seed, world, "new", 2, 256, 1, 0.5)[:2] == (1, 0.5)
    # A cold request must not borrow a warm restore residual, or another node.
    assert joint_phase_split(seed, world, "new", 2, 0, 8, 3)[:2] == (0, 1)
    assert joint_phase_split(seed, world, "new", 3, 256, 8, 3)[:2] == (2, 1)
