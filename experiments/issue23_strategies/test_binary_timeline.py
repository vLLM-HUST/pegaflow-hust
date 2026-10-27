"""Numerical mechanisms: no gain tolerance, no performance assertions."""

import json
import math
import os
import random
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401 - binds retained overlay
from batch_calendar import schedule_batched
from binary_timeline import SCALE, seconds, ticks
from diagnose_calendar_roundoff import diagnostic
from pegaflow.operation_schedule import Operation, Work
from pegaflow.strategies.continuous_batch import BatchConfig
from test_continuous_batch_calendar import Costs


def test_binary64_roundtrip_and_exact_grouping():
    values = [0.0, 5e-324, -5e-324, 166.9, 0.41992161351259943, 1e100]
    for value in values:
        assert seconds(ticks(value)) == value
        assert Fraction(ticks(value), SCALE) == Fraction(value)
    origin, duration = ticks(166.9), ticks(6.344471176237789 / 1000 * 128)
    assert origin + duration * 7 == sum([duration] * 7, start=origin)
    with pytest.raises(ValueError):
        ticks(math.inf)


def test_independent_horizon_cannot_change_first_or_grouping():
    old = diagnostic()
    new = diagnostic(exact_timeline=True)
    assert old["different_endpoints"]["True"] > 1
    assert len({r["a_first_hex"] for r in new["rows"]}) == 1


def test_numerical_tie_cannot_override_a_worse_secondary():
    rows = diagnostic(exact_timeline=True)["rows"]
    first = rows[0]["a_first"] - 166.9
    for row in rows:
        candidate = row["a_first"] - 166.9
        assert not (candidate, (candidate, 6.08)) < (first, (first, 5.86))


def test_competing_gates_links_batches_have_exact_grouping_equivalence():
    rng = random.Random(20261027)
    for index in range(24):
        work = []
        for number in range(4):
            p, d = number % 2, 2 + number // 2
            work.append(
                Work(
                    str(number),
                    p,
                    d,
                    number,
                    0,
                    (Operation("P_api", rng.uniform(0, 10), (f"gate{p}",)),),
                    (
                        Operation(
                            "handoff",
                            rng.uniform(0, 3),
                            (f"gate{p}", f"gate{d}", "link:pd"),
                        ),
                        Operation("D_api", 137, (f"gate{d}",), 9),
                    ),
                )
            )
        args = (
            work,
            0.0,
            lambda w, op, age: max(0, op.seconds - age),
            {w.key: SimpleNamespace(tokens=9, block_tokens=1) for w in work},
            {w.key: SimpleNamespace(d_blocks=0) for w in work},
            Costs(),
            BatchConfig(
                max_sequences=1 + index % 4,
                token_budget=4,
                bytes_per_token=1,
                shared_fraction=(index % 2) / 2,
            ),
            {},
        )
        literal = schedule_batched(*args, coalesce=False, exact_timeline=True)
        grouped = schedule_batched(*args, coalesce=True, exact_timeline=True)
        for field in ("P_end", "first", "full", "stage_start", "operations"):
            assert literal[field] == grouped[field], (index, field)


def test_option_reaches_real_request_forecasts(monkeypatch):
    import cache_evolving_search
    import simulate_continuous_batch as sim
    from numerical_calendar_search import NumericalCalendarSimulation

    path, seed = os.getenv("ISSUE23_SIM_INPUTS"), os.getenv("ISSUE23_SIM_SEED")
    if not path or not seed:
        pytest.skip("Explicit frozen input and isolated seed required")
    inputs = json.loads(Path(path).read_bytes())
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**v) for v in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    calls = []
    original = cache_evolving_search.schedule_batched

    def observe(*args, **kwargs):
        calls.append(kwargs.get("exact_timeline", False))
        return original(*args, **kwargs)

    monkeypatch.setattr(cache_evolving_search, "schedule_batched", observe)
    model = NumericalCalendarSimulation(
        inputs["rows"][:1],
        inputs["initial_inventory"],
        sim.Costs(**values),
        Path(seed),
        inputs["control_cost_rows"],
        0.49032341538142143,
        "ours",
        False,
        batch_config=BatchConfig(),
    )
    model.run()
    assert calls and all(calls), {"calls": len(calls), "enabled": sum(calls)}
