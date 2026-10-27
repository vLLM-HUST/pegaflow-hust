"""Ordering changes no device capability, token budget or I/O barrier."""

import unittest
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401 - retained CPU overlay
from batch_calendar import schedule_batched
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.causal_prefill import (
    CausalPrefillBatcher,
    LaxityPrefillBatcher,
)
from pegaflow.strategies.continuous_batch import BatchConfig, Sequence
from test_continuous_batch_calendar import Costs


class CausalTests(unittest.TestCase):
    def test_laxity_includes_remaining_work_without_a_weight(self):
        batch = LaxityPrefillBatcher(
            BatchConfig(token_budget=4, bytes_per_token=1),
            {"old_short": 0.0, "new_long": 1.0}.__getitem__,
        )
        for key, seconds in [("old_short", 1.0), ("new_long", 10.0)]:
            sequence = Sequence(key, 8, 0, seconds, 127.0)
            sequence.phase = "prefill"
            batch.submit(sequence)
        iteration = batch.begin()
        self.assertEqual(iteration.pieces[0][0], "new_long")
        self.assertEqual(batch.stable_repeats(iteration, 10), 2)

    def test_forecast_coalescing_preserves_ready_order_after_p_completions(self):
        work = [
            Work(
                str(i),
                i % 2,
                2 + i // 2,
                i,
                float(3 - i),
                (Operation("P_api", float(i + 1), (f"gate{i % 2}",)),),
                (Operation("D_api", 200.0, (f"gate{2 + i // 2}",), 9.0),),
            )
            for i in range(4)
        ]
        demands = {w.key: SimpleNamespace(tokens=9, block_tokens=1) for w in work}
        routes = {w.key: SimpleNamespace(d_blocks=0) for w in work}
        ready = {w.key: w.offered for w in work}
        config = BatchConfig(token_budget=4, bytes_per_token=1)
        for batcher_type in (CausalPrefillBatcher, LaxityPrefillBatcher):
            answers = [
                schedule_batched(
                    work,
                    4.0,
                    lambda w, o, a: o.seconds,
                    demands,
                    routes,
                    Costs(),
                    config,
                    {},
                    coalesce=coalesce,
                    batch_factory=lambda c, cls=batcher_type: cls(c, ready.__getitem__),
                )
                for coalesce in (False, True)
            ]
            for field in ("first", "full", "P_end"):
                for key, value in answers[0][field].items():
                    self.assertAlmostEqual(value, answers[1][field][key])

    def test_earlier_causal_ready_not_earlier_api_gets_prefill(self):
        batch = CausalPrefillBatcher(
            BatchConfig(token_budget=4, bytes_per_token=1),
            {"early_api": 9.0, "late_api": 1.0}.__getitem__,
        )
        for key in ("early_api", "late_api"):
            sequence = Sequence(key, 8, 0, 8.0, 127.0)
            sequence.phase = "prefill"
            batch.submit(sequence)
        iteration = batch.begin()
        self.assertEqual(iteration.pieces, (("late_api", 4, "prefill"),))
        self.assertEqual(tuple(batch.active), ("early_api", "late_api"))

    def test_decode_and_barriers_keep_their_priority(self):
        batch = CausalPrefillBatcher(
            BatchConfig(token_budget=4, bytes_per_token=1),
            {"decoding": 8.0, "prefilling": 1.0, "restoring": 9.0}.__getitem__,
        )
        for key, phase in [
            ("decoding", "decode"),
            ("prefilling", "prefill"),
            ("restoring", "restore"),
        ]:
            sequence = Sequence(key, 8, 0, 8.0, 127.0, restore_seconds=2.0)
            sequence.phase = phase
            batch.submit(sequence)
        barrier = batch.begin()
        self.assertEqual(barrier.kind, "restore")
        batch.finish(barrier, 2.0)
        iteration = batch.begin()
        self.assertEqual(iteration.pieces[0], ("decoding", 1, "decode"))
        self.assertEqual(iteration.pieces[1], ("prefilling", 3, "prefill"))


if __name__ == "__main__":
    unittest.main()
