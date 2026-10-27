"""An old copy keeps its turn while finite older holders can drain."""

import unittest
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401 - retained CPU overlay
from batch_calendar import schedule_batched
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig, Sequence
from pegaflow.strategies.resource_reservation import copy_reservations, permitted
from test_continuous_batch_calendar import Costs


class ReservationTests(unittest.TestCase):
    def test_turn_is_not_partial_ownership_and_preserves_older_work(self):
        reserved = copy_reservations([(3, ("gate0", "gate2", "link:pd"))])
        self.assertFalse(permitted(("gate0",), 4, reserved))
        self.assertTrue(permitted(("gate0",), 4, reserved, acquired=True))
        self.assertTrue(permitted(("gate2",), 2, reserved))
        self.assertTrue(permitted(("gate1",), 9, reserved))

    def test_pending_copy_precedes_younger_source_reacquisition(self):
        work = [
            Work(
                "oldD",
                1,
                2,
                0,
                0.0,
                (),
                (Operation("D_api", 200.0, ("gate2",), 9.0),),
                p_done=0.0,
                actual_first=0.0,
            ),
            Work(
                "copy",
                0,
                2,
                1,
                0.0,
                (),
                (
                    Operation("handoff", 0.1, ("gate0", "gate2", "link:pd")),
                    Operation("D_api", 200.0, ("gate2",), 9.0),
                ),
                p_done=0.0,
            ),
            Work(
                "runningP",
                0,
                3,
                2,
                0.0,
                (Operation("P_api", 3.0, ("gate0",)),),
                (Operation("D_api", 200.0, ("gate3",), 9.0),),
                active_operations=(("P_api", 0.0),),
            ),
            Work(
                "newP",
                0,
                3,
                3,
                0.0,
                (Operation("P_api", 10.0, ("gate0",)),),
                (Operation("D_api", 200.0, ("gate3",), 9.0),),
            ),
        ]
        demands = {w.key: SimpleNamespace(tokens=9, block_tokens=1) for w in work}
        routes = {w.key: SimpleNamespace(d_blocks=0) for w in work}
        progress = {
            "oldD": Sequence(
                "oldD",
                9,
                0,
                9.0,
                127.0,
                prefilled=9,
                decoded=1,
                restored=True,
                phase="decode",
                first_at=0.0,
            )
        }
        results = [
            schedule_batched(
                work,
                0.0,
                lambda w, o, a: o.seconds,
                demands,
                routes,
                Costs(),
                BatchConfig(token_budget=4, bytes_per_token=1),
                progress,
                reserve_copies=True,
                coalesce=c,
            )
            for c in (False, True)
        ]
        operations = results[0]["operations"]
        self.assertAlmostEqual(results[0]["P_end"]["runningP"], 3.0)
        copy = next(
            e for e in operations if e["key"] == "copy" and e["operation"] == "handoff"
        )
        new = next(
            e for e in operations if e["key"] == "newP" and e["operation"] == "P_api"
        )
        self.assertLess(copy["start"], new["start"])
        for field in ("first", "full", "P_end"):
            for key, value in results[0][field].items():
                self.assertAlmostEqual(value, results[1][field][key])


if __name__ == "__main__":
    unittest.main()
