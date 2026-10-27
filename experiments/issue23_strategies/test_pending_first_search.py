"""Compute priority may improve first token only by moving, not deleting, work."""

import pytest
import simulate_continuous_batch  # noqa: F401 - bind retained overlay
from pegaflow.strategies.continuous_batch import (
    BatchConfig,
    ContinuousBatcher,
    Sequence,
)
from pending_first_search import PendingFirstBatcher


def drain(kind):
    batch = kind(BatchConfig(token_budget=4, bytes_per_token=1))
    batch.submit(
        Sequence(
            "old",
            4,
            0,
            2,
            12,
            output_tokens=5,
            prefilled=4,
            decoded=1,
            restored=True,
            phase="decode",
            first_at=0,
        )
    )
    batch.submit(
        Sequence("new", 8, 0, 2, 6, output_tokens=5, restored=True, phase="prefill")
    )
    now, first, completed, decode_tokens = 0.0, {}, {}, 0
    while not batch.idle:
        step = batch.begin()
        now += step.seconds
        decode_tokens += sum(
            count for _, count, phase in step.pieces if phase == "decode"
        )
        observed, finished = batch.finish(step, now)
        first.update((key, now) for key in observed)
        completed.update((key, now) for key in finished)
    assert batch.used_bytes == 0 and decode_tokens == 8
    return now, first, completed


def test_first_priority_moves_decode_but_conserves_all_work():
    before = drain(ContinuousBatcher)
    after = drain(PendingFirstBatcher)
    assert after[1]["new"] < before[1]["new"]
    assert after[0] == pytest.approx(before[0])
    assert after[2]["old"] > before[2]["old"]  # The release/ITL cost is real.
