"""Phase ordering protects ready work without deleting restore or decode."""

from collections import Counter
from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from batch_calendar import schedule_batched
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig, Sequence
from ready_prefill_search import ReadyPrefillBatcher
from release_cobatch_search import ReleaseCobatchBatcher


def sequence(key, phase, restore=4):
    return Sequence(
        key,
        8,
        0,
        2,
        6,
        output_tokens=5,
        restore_seconds=restore,
        save_seconds=1,
        restored=phase != "restore",
        phase=phase,
    )


def drain(kind):
    b = kind(BatchConfig(token_budget=4, bytes_per_token=1))
    b.submit(sequence("a", "prefill"))
    b.submit(sequence("b", "restore"))
    now, first, counts, phases = 0, {}, Counter(), Counter()
    while not b.idle:
        step = b.begin()
        now += step.seconds
        for key, n, phase in step.pieces:
            counts[key, phase] += n
            phases[key, phase] += 1
        f, _ = b.finish(step, now)
        first.update((key, now) for key in f)
    return now, first, counts, phases, b.used_bytes


def test_isolated_interchange_advances_older_first_preserves_all_work_and_drain():
    a, b = drain(ReleaseCobatchBatcher), drain(ReadyPrefillBatcher)
    assert b[1]["a"] < a[1]["a"]
    assert b[0] == a[0] and b[2:] == a[2:]
    assert b[2]["b", "decode"] == 4 and b[3]["b", "restore"] == 1
    assert b[3]["a", "save"] == b[3]["b", "save"] == 1 and b[-1] == 0


@pytest.mark.parametrize(
    "first_phase,second_phase,expected",
    [
        ("restore", "prefill", "restore"),
        ("prefill", "save", "save"),
        ("prefill", "restore", "compute"),
    ],
)
def test_only_later_restore_is_deferred_and_save_remains_a_barrier(
    first_phase, second_phase, expected
):
    b = ReadyPrefillBatcher(BatchConfig(bytes_per_token=1))
    b.submit(sequence("a", first_phase))
    b.submit(sequence("b", second_phase))
    step = b.begin()
    assert step.kind == expected
    assert b.begin() is None and b.busy is step


def test_eligibility_and_no_world_cost_dependence():
    b = ReadyPrefillBatcher(BatchConfig(bytes_per_token=1))
    b.submit(sequence("a", "prefill"))
    b.submit(sequence("b", "restore", restore=0.00001))
    assert b.begin(lambda key: key == "b").kind == "restore"
    c = ReadyPrefillBatcher(BatchConfig(bytes_per_token=1))
    c.submit(sequence("a", "prefill"))
    c.submit(sequence("b", "restore", restore=0.00001))
    assert c.begin().kind == "compute"  # no fitted minimum restore threshold


def test_resource_calendar_uses_the_same_phase_order_and_preserves_completions():
    class Costs:
        decode_seconds = (6, 6)
        def __init__(self):
            self.fits = {2: [0, 0, 0, 0, 1], 3: [0, 0, 0, 0, 1]}

        def restore(self, node, prefix):
            return 4 if prefix else 0

        def compute(self, node, prompt, prefix):
            return 2

        def decode_tail(self, node, prompt):
            return 6

    work = [
        Work(
            k,
            0,
            2,
            0,
            i,
            (),
            (Operation("D_api", 8, ("gate2",), 2),),
            p_done=0,
            d_enqueued=0,
        )
        for i, k in enumerate(("a", "b"))
    ]
    progress = {"a": sequence("a", "prefill")}
    args = (
        work,
        0,
        lambda w, o, a: o.seconds,
        {k: SimpleNamespace(tokens=8, block_tokens=1) for k in ("a", "b")},
        {k: SimpleNamespace(d_blocks=0) for k in ("a", "b")},
        Costs(),
        BatchConfig(token_budget=4, bytes_per_token=1),
        progress,
    )
    ref = schedule_batched(*args, batch_factory=ReleaseCobatchBatcher)
    new = schedule_batched(*args, batch_factory=ReadyPrefillBatcher)
    assert new["batch_factory"] == "ReadyPrefillBatcher"
    assert new["first"]["a"] <= ref["first"]["a"]
    assert set(new["full"]) == {"a", "b"}
