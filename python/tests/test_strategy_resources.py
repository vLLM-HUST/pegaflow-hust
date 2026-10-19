import unittest
from dataclasses import replace

from pegaflow.strategies import Candidate, Request, Snapshot, Worker, create_strategy
from pegaflow.strategies.resource_forecast import ResourceForecaster, candidate_work, copy_resources
from pegaflow.strategies.resource_schedule import Operation, Residual, Work, schedule


class ResourceTests(unittest.TestCase):
    def snapshot(self, plans, pending=frozenset({"new"}), observed=frozenset()):
        workers = tuple(Worker(n, "P" if n < 2 else "D", 0, 0, 0, 0, 0., 0.)
                        for n in range(4))
        return Snapshot(Request("new", "s", 256, 0., 1), 0., workers, tuple(plans),
                        pending, observed)

    def test_full_decode_holder_is_not_removed_at_first(self):
        holder = Work("old", 0, 2, 0, 0., (),
                      (Operation("D_api", 10., ("gate:2", "npu:2"), 1.),),
                      p_done=0., d_enqueued=0., d_started=0., actual_first=0.,
                      active_operations=(("D_api", 0.),))
        plans = (Candidate(1, 2, 1, 0, 0, 1., 1., 1., 9.),
                 Candidate(1, 3, 1, 0, 0, 1., 1., 1., 9.))
        snap = self.snapshot(plans, observed=frozenset({"old"}))
        predict = ResourceForecaster((holder,), remaining=lambda *_: Residual(10., 0.))
        blocked, free = (predict(snap, p) for p in plans)
        self.assertEqual(dict(blocked.first_latencies)["new"], 11.)
        self.assertEqual(dict(free.first_latencies)["new"], 2.)
        self.assertEqual(create_strategy("tailguard-pending-release-v1").choose(snap, predict).d, 3)

    def test_source_gate_delays_copy_without_holding_destination(self):
        hold = Work("old", 0, 2, 0, 0.,
                    (Operation("P_api", 5., ("gate:0", "npu:0")),),
                    (Operation("D_api", 1., ("gate:2", "npu:2"), 0.5),),
                    p_started=0., active_operations=(("P_api", 0.),))
        p = Candidate(1, 3, 0, 0, 0, 1., 1., 1., 9., history_copy_seconds=2.)
        snap = self.snapshot((p,), pending=frozenset({"old", "new"}))
        forecast = ResourceForecaster((hold,))(snap, p)
        self.assertEqual(dict(forecast.first_latencies)["new"], 9.)
        result = schedule((hold, candidate_work(snap, p)), 0.)
        copy = next(op for op in result["operations"] if op["operation"] == "history_copy")
        self.assertEqual(copy["start"], 5.)
        self.assertFalse(any("link" in r for r in copy["resources"]))

    def test_control_handoff_and_sync_tail_are_included_once(self):
        plan = Candidate(0, 2, 0, 0, 0, 1., 3., 2., 12.,
                         control_seconds=.5, handoff_seconds=1.)
        snap = self.snapshot((plan,))
        result = ResourceForecaster()(snap, plan)
        self.assertEqual(dict(result.first_latencies)["new"], 6.5)
        self.assertEqual(dict(result.full_release)[2], 16.5)
        self.assertEqual(dict(result.full_release)[0], 3.5)

    def test_forecast_does_not_insert_invisible_live_work(self):
        plan = Candidate(0, 2, 0, 0, 0, 1., 1., 1., 9.)
        snap = self.snapshot((plan,), pending=frozenset({"missing", "new"}))
        with self.assertRaises(ValueError):
            ResourceForecaster()(snap, plan)
        with self.assertRaises(ValueError):
            ResourceForecaster(latest_completed=float("nan"))

    def test_future_decode_fifo_follows_actual_prefill_completion(self):
        a = Work("slow", 0, 2, 0, 0., (Operation("P", 5., ("gate:0",)),),
                 (Operation("D", 3., ("gate:2",), 1.),))
        b = Work("fast", 1, 2, 1, 0., (Operation("P", 1., ("gate:1",)),),
                 (Operation("D", 3., ("gate:2",), 1.),))
        result = schedule((a, b), 0.)
        self.assertEqual(result["first"]["fast"], 2.)
        self.assertEqual(result["first"]["slow"], 6.)
        self.assertEqual(copy_resources(0, 2), ("gate:0", "gate:2"))
        self.assertIn("link:P>D", copy_resources(0, 2, True))


if __name__ == "__main__":
    unittest.main()
