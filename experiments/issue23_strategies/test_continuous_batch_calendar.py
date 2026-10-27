"""Forecast conservation and observed progress, without loading a model."""

import random
import unittest
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401 - binds the retained CPU parent
from batch_calendar import schedule_batched
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig, Sequence


class Costs:
    fits = [(None, None, 0, 0, 1)] * 4

    def restore(self, node, prefix):
        return 0.0

    def compute(self, node, tokens, prefix):
        return (tokens - prefix) * (1 + node / 7)

    def decode_tail(self, node, tokens):
        return 128.0 * (1 + node / 5)


class CalendarTests(unittest.TestCase):
    def test_acquired_prefix_fallback_keeps_observed_token_coordinates(self):
        work = [
            Work(
                "a",
                0,
                2,
                0,
                0,
                (),
                (Operation("D_api", 137, ("gate2",), 9),),
                p_done=0,
                active_operations=(("D_api", 0),),
            )
        ]
        progress = {
            "a": Sequence(
                "a", 9, 0, 9, 127, prefilled=8, restored=True, phase="prefill"
            )
        }
        result = schedule_batched(
            work,
            0,
            lambda w, o, age: o.seconds,
            {"a": SimpleNamespace(tokens=9, block_tokens=1)},
            {"a": SimpleNamespace(d_blocks=1)},
            Costs(),
            BatchConfig(token_budget=4, bytes_per_token=1),
            progress,
        )
        self.assertAlmostEqual(result["first"]["a"], 1 + 2 / 7)

    def predict(self, work, config, *, coalesce, progress=None):
        demands = {w.key: SimpleNamespace(tokens=9, block_tokens=1) for w in work}
        routes = {w.key: SimpleNamespace(d_blocks=0) for w in work}
        return schedule_batched(
            work,
            0.0,
            lambda w, op, age: max(0, op.seconds - age),
            demands,
            routes,
            Costs(),
            config,
            progress or {},
            coalesce=coalesce,
        )

    def test_parallel_first_precedes_other_full_completion(self):
        work = [
            Work(
                k,
                0,
                2,
                i,
                0.0,
                (),
                (Operation("D_api", 137.0, ("gate2",), 9.0),),
                p_done=0.0,
            )
            for i, k in enumerate(("a", "b"))
        ]
        config = BatchConfig(max_sequences=2, token_budget=4, bytes_per_token=1)
        result = self.predict(work, config, coalesce=True)
        self.assertLess(max(result["first"].values()), min(result["full"].values()))

    def test_coalescing_matches_literal_steps_with_cross_device_copy(self):
        work = [
            Work(
                "a",
                0,
                2,
                0,
                0.0,
                (),
                (Operation("D_api", 137.0, ("gate2",), 9.0),),
                p_done=0.0,
            ),
            Work(
                "b",
                1,
                3,
                1,
                0.0,
                (Operation("P_api", 12, ("gate1",)),),
                (
                    Operation("handoff", 2, ("gate1", "gate3", "link:pd")),
                    Operation("D_api", 137.0, ("gate3",), 9.0),
                ),
            ),
            Work(
                "c",
                0,
                2,
                2,
                0.0,
                (),
                (
                    Operation("handoff", 3, ("gate3", "gate2", "link:D")),
                    Operation("D_api", 137.0, ("gate2",), 9.0),
                ),
                p_done=0.0,
            ),
        ]
        config = BatchConfig(max_sequences=2, token_budget=4, bytes_per_token=1)
        a = self.predict(work, config, coalesce=False)
        b = self.predict(work, config, coalesce=True)
        for field in ("P_end", "first", "full"):
            for key, value in a[field].items():
                self.assertAlmostEqual(value, b[field][key])

    def test_copy_gate_is_held_while_waiting_for_direction(self):
        work = [
            Work(
                "a",
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
                active_operations=(("handoff", 0),),
            ),
            Work(
                "b",
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
                active_operations=(("handoff", 0),),
            ),
            Work(
                "c",
                1,
                3,
                2,
                0,
                (Operation("P_api", 1, ("gate1",)),),
                (Operation("D_api", 137, ("gate3",), 9),),
            ),
        ]
        result = self.predict(work, BatchConfig(bytes_per_token=1), coalesce=True)
        self.assertGreaterEqual(result["P_end"]["c"], 9)

    def test_coalescing_preserves_two_device_phase_and_copy_order(self):
        rng = random.Random(20261026)
        for index in range(24):
            work = []
            for number in range(4):
                p, d = number % 2, 2 + (number // 2)
                key = str(number)
                chain = (
                    Operation(
                        "handoff",
                        rng.uniform(0, 3),
                        (f"gate{p}", f"gate{d}", "link:pd"),
                    ),
                    Operation("D_api", 137, (f"gate{d}",), 9),
                )
                work.append(
                    Work(
                        key,
                        p,
                        d,
                        number,
                        0,
                        (Operation("P_api", rng.uniform(0, 10), (f"gate{p}",)),),
                        chain,
                    )
                )
            config = BatchConfig(
                max_sequences=1 + index % 4,
                token_budget=4,
                bytes_per_token=1,
                shared_fraction=(index % 2) / 2,
            )
            literal = self.predict(work, config, coalesce=False)
            grouped = self.predict(work, config, coalesce=True)
            for field in ("P_end", "first", "full"):
                for key, value in literal[field].items():
                    self.assertAlmostEqual(value, grouped[field][key])


if __name__ == "__main__":
    unittest.main()
