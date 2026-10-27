"""A pending direction slot must not hold unrelated endpoint computation."""

import unittest
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401
from batch_calendar import schedule_batched
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig
from test_continuous_batch_calendar import Costs


class JointAcquireTests(unittest.TestCase):
    def test_no_gate_held_while_software_link_busy(self):
        work = [
            Work(
                "copyA",
                0,
                2,
                0,
                0,
                (),
                (
                    Operation("handoff", 3, ("gate0", "gate2", "link:pd")),
                    Operation("D_api", 137, ("gate2",), 9),
                ),
                p_done=0,
            ),
            Work(
                "copyB",
                1,
                3,
                1,
                0,
                (),
                (
                    Operation("handoff", 5, ("gate1", "gate3", "link:pd")),
                    Operation("D_api", 137, ("gate3",), 9),
                ),
                p_done=0,
            ),
            Work(
                "P",
                1,
                3,
                2,
                0,
                (Operation("P_api", 1, ("gate1",)),),
                (Operation("D_api", 137, ("gate3",), 9),),
            ),
        ]
        demands = {w.key: SimpleNamespace(tokens=9, block_tokens=1) for w in work}
        routes = {w.key: SimpleNamespace(d_blocks=0) for w in work}
        results = [
            schedule_batched(
                work,
                0,
                lambda w, o, a: o.seconds,
                demands,
                routes,
                Costs(),
                BatchConfig(bytes_per_token=1),
                {},
                atomic_copies=atomic,
                coalesce=c,
            )
            for atomic, c in ((False, False), (True, False), (True, True))
        ]
        self.assertGreaterEqual(results[0]["P_end"]["P"], 9)
        self.assertEqual(results[1]["P_end"]["P"], 1)
        copy = next(o for o in results[1]["operations"] if o["key"] == "copyB")
        self.assertGreaterEqual(copy["start"], 3)
        self.assertEqual(set(copy["resources"]), {"gate1", "gate3", "link:pd"})
        for field in ("first", "full", "P_end"):
            for key, value in results[1][field].items():
                self.assertAlmostEqual(value, results[2][field][key])


if __name__ == "__main__":
    unittest.main()
