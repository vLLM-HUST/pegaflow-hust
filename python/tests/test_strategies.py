import unittest
from dataclasses import replace

from pegaflow.strategies import Candidate, Forecast, Request, Snapshot, Worker, create_strategy
from pegaflow.strategies.baselines import lmetric_score


def sample():
    workers = tuple(Worker(node, "P" if node < 2 else "D", 128, 0, 0, 0,
                           8.0 if node == 0 else 0.0, 0.0) for node in range(4))
    plans = tuple(Candidate(p, d, p, 128, 128, 2, 2, 1, 9, generation=(1, 1, 1, 1))
                  for p in (0, 1) for d in (2, 3))
    return Snapshot(Request("r", "session", 1024, 0.0, 0), 0.0, workers, plans,
                    frozenset(("r", "waiting")), frozenset(("already-first",)))


class StrategyTests(unittest.TestCase):
    def test_native_does_not_run_any_predictor_or_limit_cache(self):
        def forbidden(*_):
            raise AssertionError("Native baseline must not call Tailguard")
        for seq in (0, 1, 2):
            state = replace(sample(), request=replace(sample().request, sequence=seq))
            chosen = create_strategy("vllm-pegaflow-native").choose(state, forbidden)
            self.assertEqual((chosen.p, chosen.d), (seq % 2, 2 + seq % 2))
            self.assertEqual(chosen.worker_options("D"), {"policy_native_cache": True})
            self.assertIsNone(chosen.candidate)

    def test_memserve_queue_then_beneficial_remote_copy(self):
        state = sample()
        remote = replace(state.candidates[2], source=2, p_prefix=256,
                         p_first_seconds=1, history_copy_seconds=0.5)
        state = replace(state, candidates=state.candidates + (remote,))
        chosen = create_strategy("memserve-eq12-adapter").choose(state)
        self.assertEqual((chosen.p, chosen.d, chosen.candidate.source), (1, 2, 2))
        expensive = replace(remote, history_copy_seconds=2)
        chosen = create_strategy("memserve-eq12-adapter").choose(
            replace(state, candidates=sample().candidates + (expensive,)))
        self.assertEqual(chosen.candidate.source, 1)

    def test_lmetric_uses_supplied_batch_indicator(self):
        self.assertEqual(lmetric_score(300, 4), 1200)
        for values in ((True, 1), (-1, 1), (1, -1)):
            with self.assertRaises(ValueError):
                lmetric_score(*values)

    def test_pending_tail_prices_other_requests_and_full_release(self):
        state = sample()
        def quote(s, plan):
            delays = (("r", 1 if plan.p == 0 else 2),
                      ("waiting", 40 if plan.p == 0 else 3))
            releases = tuple((w.node, plan.d_full_seconds if w.node == plan.d else 0.)
                             for w in s.workers)
            return Forecast(delays, releases, 0, s.now, True)
        chosen = create_strategy("tailguard-pending-release-v1").choose(state, quote)
        self.assertEqual(chosen.p, 1)
        self.assertEqual(chosen.candidates_evaluated, 4)

    def test_future_label_missing_request_and_unpriced_cost_are_rejected(self):
        state = sample()
        invalid = (
            Forecast((("r", 1), ("waiting", 2)), ((2, 9),), 1, 0, True),
            Forecast((("r", 1),), ((2, 9),), 0, 0, True),
            Forecast((("r", 1), ("waiting", 2)), ((2, 9),), 0, 0, False),
        )
        for value in invalid:
            with self.assertRaises(ValueError):
                create_strategy("tailguard-pending-release-v1").choose(state, lambda *_, value=value: value)

    def test_unsafe_candidate_and_immutable_first_are_not_optimized(self):
        state = sample()
        with self.assertRaises(ValueError):
            replace(state, pending_first=frozenset(("r", "already-first")))
        state = replace(state, candidates=tuple(replace(p, feasible=False)
                                                for p in state.candidates))
        with self.assertRaises(ValueError):
            create_strategy("tailguard-pending-release-v1").choose(state, lambda *_: None)
        with self.assertRaises(ValueError):
            create_strategy("lmetric-full-paper")


if __name__ == "__main__":
    unittest.main()
