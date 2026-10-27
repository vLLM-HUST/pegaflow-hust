"""No world oracle; preserve memory release, token budget and calendar order."""

import unittest
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401
from batch_calendar import schedule_batched
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig, Sequence
from pegaflow.strategies.recovery_priority import (
    RecoveryDeadlineBatcher,
    RecoveryPriorityBatcher,
)
from test_continuous_batch_calendar import Costs


class RecoveryTests(unittest.TestCase):
    def test_deadline_avoids_displacing_old_prefill_with_large_young_restore_estimate(
        self,
    ):
        quotes = {"old": (1, 0), "young": (100, 100)}
        choices = []
        for batcher in (RecoveryPriorityBatcher, RecoveryDeadlineBatcher):
            batch = batcher(
                BatchConfig(token_budget=4, bytes_per_token=1),
                {"old": 0, "young": 1}.__getitem__,
                quotes.__getitem__,
            )
            batch.submit(Sequence("old", 9, 0, 1, 10, phase="prefill", restored=True))
            batch.submit(Sequence("young", 9, 0, 1, 10, 1))
            choices.append(batch.begin().pieces[0][0])
        self.assertEqual(choices, ["young", "old"])

    def test_known_pending_prefill_can_precede_new_restore_without_world_oracle(self):
        for world_body, world_restore in ((1, 100), (1000, 0.001)):
            quotes = {"ready": (10, 0), "reload": (1, 1)}
            batch = RecoveryPriorityBatcher(
                BatchConfig(token_budget=4, bytes_per_token=1),
                {"ready": 0, "reload": 10}.__getitem__,
                quotes.__getitem__,
            )
            batch.submit(
                Sequence("ready", 9, 0, world_body, 10, phase="prefill", restored=True)
            )
            batch.submit(Sequence("reload", 9, 0, world_body, 10, world_restore))
            step = batch.begin()
            self.assertEqual(step.kind, "compute")
            self.assertEqual(step.pieces, (("ready", 4, "prefill"),))
            self.assertEqual(batch.restore_deferrals, 1)
            self.assertIsNone(batch.begin())

    def test_completed_save_releases_capacity_before_more_work(self):
        batch = RecoveryPriorityBatcher(
            BatchConfig(bytes_per_token=1), lambda k: 0, lambda k: (100, 100)
        )
        batch.submit(Sequence("reload", 9, 0, 1, 10, 2))
        batch.submit(
            Sequence(
                "done",
                9,
                0,
                1,
                10,
                save_seconds=3,
                phase="save",
                prefilled=9,
                decoded=128,
                first_at=0,
            )
        )
        step = batch.begin()
        self.assertEqual(step.pieces, (("done", 0, "save"),))
        before = batch.used_bytes
        _, done = batch.finish(step, 3)
        self.assertEqual(done, ["done"])
        self.assertLess(batch.used_bytes, before)

    def test_coalescing_replays_priority_crossing_against_unrestored_job(self):
        work = [
            Work(
                k,
                0,
                2,
                i,
                i * 2,
                (),
                (Operation("D_api", 140, ("gate2",), 9),),
                p_done=0,
            )
            for i, k in enumerate(("ready", "reload"))
        ]
        demands = {w.key: SimpleNamespace(tokens=9, block_tokens=1) for w in work}
        routes = {w.key: SimpleNamespace(d_blocks=0) for w in work}
        progress = {
            "ready": Sequence("ready", 9, 0, 9, 127, phase="prefill", restored=True)
        }
        result = [
            schedule_batched(
                work,
                0,
                lambda w, o, a: o.seconds,
                demands,
                routes,
                Costs(),
                BatchConfig(token_budget=4, bytes_per_token=1),
                progress,
                coalesce=c,
                batch_factory=lambda config: RecoveryPriorityBatcher(
                    config,
                    {"ready": 0, "reload": 2}.__getitem__,
                    {"ready": (9, 0), "reload": (9, 0)}.__getitem__,
                ),
            )
            for c in (False, True)
        ]
        for field in ("first", "full", "P_end"):
            for key, value in result[0][field].items():
                self.assertAlmostEqual(value, result[1][field][key])


if __name__ == "__main__":
    unittest.main()
