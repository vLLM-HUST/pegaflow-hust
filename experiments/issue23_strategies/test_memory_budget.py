import unittest
from collections import deque
from dataclasses import replace
from types import SimpleNamespace

import simulate_continuous_batch as sim
from pegaflow.strategies.continuous_batch import (
    BatchConfig,
    ContinuousBatcher,
    Sequence,
)
from pegaflow.strategies.memory_budget import GIB, Ascend910B2Budget, serving_kwargs


class MemoryBudgetTests(unittest.TestCase):
    def test_normal_auto_serving_does_not_force_kv_bytes(self):
        self.assertEqual(serving_kwargs(), {"gpu_memory_utilization": 0.9})

    def test_weights_runtime_and_rounding_are_all_charged(self):
        budget = Ascend910B2Budget()
        raw = (
            budget.runtime_visible_bytes * 9 // 10
            - budget.weights_bytes
            - budget.runtime_reserve_bytes
        )
        block = budget.block_tokens * budget.bytes_per_token
        self.assertLessEqual(budget.kv_bytes, raw)
        self.assertLess(raw - budget.kv_bytes, block)
        self.assertEqual(budget.kv_bytes % block, 0)

    def test_capacity_sensitivity_does_not_throttle_four_full_contexts(self):
        events = []
        for reserve in (2, 4, 8):
            budget = Ascend910B2Budget(runtime_reserve_bytes=reserve * GIB)
            self.assertTrue(budget.report()["nonbinding_for_active_sequences"])
            batch = ContinuousBatcher(BatchConfig(kv_bytes=budget.kv_bytes))
            for n in range(4):
                batch.submit(Sequence(str(n), 32768, 0, 20, 10, save_seconds=0.2))
            elapsed, firsts = 0, {}
            while batch.waiting or batch.active:
                step = batch.begin()
                self.assertIsNotNone(step)
                elapsed += step.seconds
                first, _ = batch.finish(step, elapsed)
                firsts.update({key: elapsed for key in first})
            self.assertEqual(batch.peak_sequences, 4)
            self.assertLessEqual(batch.peak_bytes, budget.kv_bytes)
            events.append((firsts, elapsed, batch.peak_bytes))
        self.assertEqual(events[0], events[1])
        self.assertEqual(events[1], events[2])

    def test_progress_contains_admission_waiters_without_future_costs_used(self):
        s = Sequence("waiting", 9, 0, 9, 10)
        state = object.__new__(sim.BatchSimulation)
        state.d_batches = {2: SimpleNamespace(active={}, waiting=deque([s]))}
        observed = state.progress()
        self.assertEqual(observed["waiting"].prefix_tokens, 0)
        self.assertIsNot(observed["waiting"], s)

    def test_nonpositive_budget_rejected(self):
        with self.assertRaises(ValueError):
            replace(Ascend910B2Budget(), runtime_reserve_bytes=64 * GIB)


if __name__ == "__main__":
    unittest.main()
