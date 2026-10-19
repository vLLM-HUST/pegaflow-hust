import unittest
from dataclasses import replace

from pegaflow.strategies import Candidate, Request, Snapshot, Worker, create_strategy


class MooncakeTests(unittest.TestCase):
    def test_paper_ratio_requires_declaration(self):
        with self.assertRaises(TypeError):
            create_strategy("mooncake-fast25-prefill-no-reject-adapter")
        for ratio in (True, .5, float("inf")):
            with self.assertRaises(ValueError):
                create_strategy("mooncake-fast25-prefill-no-reject-adapter", balancing_ratio=ratio)

    def test_global_prefix_copy_and_queue_choose_alternative_prefill(self):
        workers = (Worker(0, "P", 512, 0, 1, 2, 10., 10.),
                   Worker(1, "P", 128, 0, 0, 0, 0., 0.),
                   Worker(2, "D", 0, 0, 0, 0, 0., 0.),
                   Worker(3, "D", 0, 0, 0, 1, 9., 9.))
        plans = (Candidate(0, 2, 0, 512, 0, 1., 1., 1., 9.),
                 Candidate(1, 2, 1, 128, 0, 4., 4., 1., 9.),
                 Candidate(1, 2, 0, 512, 0, 1., 1., 1., 9., history_copy_seconds=.5))
        snap = Snapshot(Request("r", "s", 1024, 0., 0), 0., workers, plans, frozenset({"r"}))
        strategy = create_strategy("mooncake-fast25-prefill-no-reject-adapter", balancing_ratio=2.)
        chosen = strategy.choose(snap)
        self.assertEqual((chosen.p, chosen.d, chosen.candidate.source), (1, 2, 0))
        # Strict > in FAST25: equal ratio uses the local branch.
        changed = (workers[0], replace(workers[1], local_prefix=256), *workers[2:])
        plans = (plans[0], replace(plans[1], p_prefix=256), plans[2])
        chosen = strategy.choose(replace(snap, workers=changed, candidates=plans))
        self.assertEqual(chosen.candidate.source, 1)


if __name__ == "__main__":
    unittest.main()
