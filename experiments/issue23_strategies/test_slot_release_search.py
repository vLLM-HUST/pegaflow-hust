"""Known slot pressure may prioritize release, never erase full decode work."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401 - bind retained overlay
from pegaflow.strategies.continuous_batch import BatchConfig, Sequence
from pending_first_search import PendingFirstBatcher
from slot_release_search import SlotReleaseBatcher


def costs():
    return SimpleNamespace(
        decode_tail=lambda node, tokens: {4: 12.0, 8: 24.0}[tokens],
        fits={2: [0, 0, 0, 0, 2.0]},
        latest_completed_seconds=0.0,
    )


def decoder(key, prompt, decoded, tail):
    return Sequence(
        key,
        prompt,
        0,
        1,
        tail,
        save_seconds=2,
        output_tokens=5,
        prefilled=prompt,
        decoded=decoded,
        restored=True,
        phase="decode",
        first_at=0.0,
    )


def setup(kind, blocked=True):
    batch = kind(BatchConfig(max_sequences=2, token_budget=4, bytes_per_token=1))
    # More decoded tokens do not imply an earlier release: context costs differ.
    batch.submit(decoder("short", 4, 2, 10))
    batch.submit(decoder("long", 8, 3, 22))
    if isinstance(batch, SlotReleaseBatcher):
        batch.set_context(("waiting",) if blocked else (), costs(), 2)
    return batch


def test_release_uses_estimated_remaining_work_not_world_duration_or_id():
    batch = setup(SlotReleaseBatcher)
    batch.admit()
    # Sampled execution tail deliberately contradicts the online phase estimate.
    batch.active["short"].tail_seconds = 999
    batch.active["long"].tail_seconds = 0.01
    step = batch.begin()
    assert step.pieces == (("short", 1, "decode"),)
    assert batch.release_choices[-1]["remaining_estimates"] == {
        "short": 9.5,
        "long": 13.0,
    }


def test_no_slot_pressure_retains_batch_and_nonpreemptive_ownership():
    batch = setup(SlotReleaseBatcher, False)
    step = batch.begin()
    assert len(step.pieces) == 2
    assert batch.begin() is None and batch.busy == step
    assert not batch.release_choices


def test_pending_first_and_exclusive_barriers_are_not_bypassed():
    batch = setup(SlotReleaseBatcher)
    batch.admit()
    batch.active["long"] = replace(
        batch.active["long"], phase="prefill", prefilled=4, decoded=0
    )
    assert batch.begin().pieces == (("long", 4, "prefill"),)
    batch = setup(SlotReleaseBatcher)
    batch.admit()
    batch.active["long"].phase = "save"
    assert batch.begin().kind == "save"


def drain(kind):
    batch = setup(kind)
    clock, done, tokens = 0.0, {}, 0
    while not batch.idle:
        step = batch.begin()
        clock += step.seconds
        tokens += sum(count for _, count, phase in step.pieces if phase == "decode")
        _, finished = batch.finish(step, clock)
        done.update((key, clock) for key in finished)
    assert tokens == 5 and batch.used_bytes == 0
    return clock, done


def test_earlier_first_release_conserves_all_service_and_output():
    before, after = drain(PendingFirstBatcher), drain(SlotReleaseBatcher)
    assert after[1]["short"] < before[1]["short"]
    assert after[0] == pytest.approx(before[0])


@pytest.mark.parametrize("cohort", [False, True])
@pytest.mark.parametrize("sharing", [0.0, 1.0])
def test_calendar_sees_external_slot_waiters_and_coalesces_exactly(cohort, sharing):
    from batch_calendar import schedule_batched
    from pegaflow.operation_schedule import Operation, Work
    from release_cobatch_search import ReleaseCobatchBatcher

    class Cost:
        fits = [(None, None, 0, 0, 1)] * 4
        latest_completed_seconds = 0.0

        def restore(self, node, prefix):
            return 0.0

        def compute(self, node, tokens, prefix):
            return tokens - prefix

        def decode_tail(self, node, tokens):
            return tokens * 127.0 + 1

    work = [
        Work(
            k,
            0,
            2,
            i,
            0,
            (),
            (Operation("D_api", 20, ("gate2",), 1),),
            p_done=0,
            actual_first=0 if i < 2 else None,
        )
        for i, k in enumerate(("short", "long", "waiting"))
    ]
    progress = {
        k: Sequence(
            k,
            n,
            0,
            1,
            n * 127.0,
            save_seconds=1,
            prefilled=n,
            decoded=120,
            restored=True,
            phase="decode",
            first_at=0,
        )
        for k, n in (("short", 4), ("long", 8))
    }
    seen = []

    def context(node, batch, queued):
        if queued:
            seen.append(tuple(queued))
        batch.set_context(queued, Cost(), node)

    results = [
        schedule_batched(
            work,
            0,
            lambda w, op, age: op.seconds,
            {
                k: SimpleNamespace(tokens=n, block_tokens=1)
                for k, n in (("short", 4), ("long", 8), ("waiting", 4))
            },
            {w.key: SimpleNamespace(d_blocks=0) for w in work},
            Cost(),
            BatchConfig(
                max_sequences=2,
                token_budget=4,
                bytes_per_token=1,
                shared_fraction=sharing,
            ),
            progress,
            coalesce=coalesce,
            batch_factory=ReleaseCobatchBatcher if cohort else SlotReleaseBatcher,
            batch_context=context,
        )
        for coalesce in (False, True)
    ]
    assert ("waiting",) in seen
    for field in ("P_end", "first", "full"):
        assert results[0][field] == pytest.approx(results[1][field])
    assert results[0]["stage_start"]["waiting", 2] == results[0]["full"]["short"]
