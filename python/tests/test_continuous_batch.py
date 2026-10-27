"""Resource conservation and iteration boundaries without GPU dependencies."""

import math
import unittest
from dataclasses import replace

from pegaflow.strategies.continuous_batch import (
    AllocationLeases,
    BatchConfig,
    ContinuousBatcher,
    Sequence,
)


def drain(batch):
    now, first, done, trace = 0.0, {}, {}, []
    while not batch.idle:
        step = batch.begin()
        if step is None:
            raise AssertionError("Unexpected deadlock")
        now += step.seconds
        observed, finished = batch.finish(step, now)
        first.update(dict.fromkeys(observed, now))
        done.update(dict.fromkeys(finished, now))
        trace.append(step)
    return first, done, trace


class BatchTests(unittest.TestCase):
    def test_lease_release_uses_acquired_owner_closure(self):
        leases = AllocationLeases()
        leases.acquire(("a", "api"), 2, ("old-allocation",))
        # Another save publishes a new owner of the same logical prefix.
        leases.acquire(("b", "api"), 2, ("old-allocation", "new-allocation"))
        self.assertEqual(leases.release(("a", "api"), 2), {"old-allocation"})
        self.assertTrue(leases.protected(2, "old-allocation"))
        self.assertTrue(leases.protected(2, "new-allocation"))
        leases.release(("b", "api"), 2)
        self.assertFalse(leases.handles)
        self.assertFalse(any(leases.counts.values()))

    def config(self, **kwargs):
        return BatchConfig(
            token_budget=4, block_tokens=4, kv_bytes=1024, bytes_per_token=1, **kwargs
        )

    def test_singleton_conserves_restore_prefill_decode_save(self):
        batch = ContinuousBatcher(self.config(max_sequences=1))
        batch.submit(Sequence("a", 9, 1, 8, 6, 2, 3, output_tokens=4))
        first, done, _ = drain(batch)
        self.assertEqual(first["a"], 10)
        self.assertEqual(done["a"], 19)
        self.assertEqual(batch.used_bytes, 0)

    def test_decode_mixes_requests_before_full_completion(self):
        batch = ContinuousBatcher(self.config(max_sequences=2))
        for key in ("a", "b"):
            batch.submit(Sequence(key, 2, 0, 2, 3, output_tokens=4))
        first, done, trace = drain(batch)
        self.assertLess(first["b"], done["a"])
        self.assertTrue(any(len(step.pieces) == 2 for step in trace))
        self.assertTrue(all(sum(n for _, n, _ in step.pieces) <= 4 for step in trace))

    def test_memory_not_just_sequence_count_limits_admission(self):
        batch = ContinuousBatcher(
            BatchConfig(
                max_sequences=4, token_budget=4, block_tokens=4, kv_bytes=12, bytes_per_token=1
            )
        )
        for key in ("a", "b"):
            batch.submit(Sequence(key, 5, 0, 5, 3, output_tokens=4))
        first, done, _ = drain(batch)
        self.assertLess(done["a"], first["b"])
        self.assertEqual(batch.peak_sequences, 1)
        self.assertEqual(batch.peak_bytes, 12)

    def test_sharing_sensitivity_is_not_free_parallel_service(self):
        elapsed = []
        for shared in (0.0, 0.5, 1.0):
            batch = ContinuousBatcher(self.config(max_sequences=2, shared_fraction=shared))
            for key in ("a", "b"):
                batch.submit(Sequence(key, 1, 0, 1, 30, 2, 3, output_tokens=4))
            _, done, _ = drain(batch)
            elapsed.append(max(done.values()))
        self.assertGreater(elapsed[0], elapsed[1])
        self.assertGreater(elapsed[1], elapsed[2])
        self.assertGreaterEqual(elapsed[2], 2 * (2 + 1 + 3) + 30)

    def test_invalid_or_oversized_input_fails_instead_of_deadlocking(self):
        with self.assertRaises(ValueError):
            BatchConfig(max_sequences=True)
        with self.assertRaises(ValueError):
            BatchConfig(shared_fraction=math.nan)
        batch = ContinuousBatcher(self.config())
        with self.assertRaises(ValueError):
            batch.submit(Sequence("huge", 2000, 0, 1, 1))

    def test_coalescing_decode_preserves_complete_time(self):
        batch = ContinuousBatcher(self.config(max_sequences=1))
        batch.submit(Sequence("a", 2, 0, 2, 12, output_tokens=4))
        now = 0.0
        for _ in range(2):
            step = batch.begin()
            now += step.seconds
            batch.finish(step, now)
        step = batch.begin()
        combined = replace(step, seconds=step.seconds * 3, repeats=3)
        batch.busy = combined
        now += combined.seconds
        batch.finish(combined, now)
        step = batch.begin()
        batch.finish(step, now)
        self.assertTrue(batch.idle)
        self.assertEqual(now, 14)


if __name__ == "__main__":
    unittest.main()
