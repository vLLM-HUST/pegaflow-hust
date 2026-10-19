import unittest
from pegaflow.strategies.residuals import conditional_remaining, phase_remaining
from pegaflow.strategies.resource_schedule import Operation, Work


class ResidualTests(unittest.TestCase):
    def test_overrun_is_not_falsely_declared_free(self):
        self.assertEqual(conditional_remaining(5., 6.), 5.)
        self.assertEqual(conditional_remaining(5., 2.), 3.)
        self.assertEqual(conditional_remaining(5., 6., (0., 3.)), 2.)
        with self.assertRaises(ValueError):
            conditional_remaining(0., 1.)

    def test_actual_first_renews_decode_release_not_prefirst(self):
        operation = Operation("D_api", 12., ("gate:2",), first_seconds=4.)
        work = Work("r", 0, 2, 0, 0., (), (operation,), p_done=0.,
                    d_started=0., actual_first=6., active_operations=(("D_api", 0.),))
        result = phase_remaining(work, operation, 7.)
        self.assertEqual((result.full_seconds, result.first_seconds), (7., 0.))


if __name__ == "__main__":
    unittest.main()
