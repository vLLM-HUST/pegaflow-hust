import unittest
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401
from batch_calendar import schedule_batched
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig
from test_continuous_batch_calendar import Costs
from timed_recovery_search import IterationAccountingMixin


class AccountingTests(unittest.TestCase):
    def test_submission_cost_counted_once_per_literal_iteration(self):
        work = [
            Work(
                "a", 0, 2, 0, 0, (), (Operation("D_api", 137, ("gate2",), 9),), p_done=0
            )
        ]
        results = [
            schedule_batched(
                work,
                0,
                lambda w, o, a: o.seconds,
                {"a": SimpleNamespace(tokens=9, block_tokens=1)},
                {"a": SimpleNamespace(d_blocks=0)},
                Costs(),
                BatchConfig(token_budget=4, bytes_per_token=1),
                {},
                coalesce=c,
                iteration_overhead={2: overhead},
            )
            for c, overhead in ((False, 0), (False, 0.1), (True, 0.1))
        ]
        self.assertAlmostEqual(results[1]["first"]["a"] - results[0]["first"]["a"], 0.4)
        self.assertAlmostEqual(results[1]["full"]["a"] - results[0]["full"]["a"], 13.2)
        for field in ("first", "full"):
            self.assertAlmostEqual(results[1][field]["a"], results[2][field]["a"])

    def test_only_completed_scheduler_observations_enter_forecast(self):
        state = object.__new__(IterationAccountingMixin)
        state.now = 10
        state.iteration_cpu = [
            {"node": 2, "completed": 9, "seconds": 0.01},
            {"node": 2, "completed": 11, "seconds": 10},
        ]
        self.assertEqual(state.completed_iteration_overhead(), {2: 0.01, 3: 0})


if __name__ == "__main__":
    unittest.main()
