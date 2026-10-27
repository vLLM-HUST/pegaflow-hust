"""Release-preserving companions depend on the physical sharing model, not rate."""

from dataclasses import replace

import simulate_continuous_batch  # noqa: F401 - bind retained overlay
from release_cobatch_search import ReleaseCobatchBatcher
from slot_release_search import SlotReleaseBatcher
from test_slot_release_search import setup


def test_zero_sharing_retains_single_release_predecessor():
    batch = setup(ReleaseCobatchBatcher)
    assert batch.begin().pieces == setup(SlotReleaseBatcher).begin().pieces


def test_non_extending_decode_can_share_without_delaying_selected_release():
    batch = setup(ReleaseCobatchBatcher)
    batch.config = replace(batch.config, shared_fraction=1.0)
    batch.admit()
    # The long-context holder is nearest release, but the short-context decode
    # has a smaller estimated step and fits inside its modeled shared iteration.
    batch.active["long"].decoded = 4
    step = batch.begin()
    assert batch.release_choices[-1]["selected"] == "long"
    assert {k for k, _, _ in step.pieces} == {"short", "long"}
    assert all(count == 1 for _, count, _ in step.pieces)


def test_predicted_extending_work_is_not_called_free():
    batch = setup(ReleaseCobatchBatcher)
    batch.config = replace(batch.config, shared_fraction=1.0)
    assert batch.begin().pieces == (("short", 1, "decode"),)
