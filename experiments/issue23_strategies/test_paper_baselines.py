"""CPU contracts for scoped paper mechanisms and fair accounting."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
import simulate_continuous_batch  # noqa: F401
from pegaflow.strategies.continuous_batch import (
    BatchConfig,
    ContinuousBatcher,
    Sequence,
)
from pegaflow.strategies.release_first import ReleaseFirstBatcher
from pegaflow.strategies.strata import BatchDemand, balanced_batch


def test_strata_defers_loading_bound_then_backfills_fifo():
    rows = [
        BatchDemand("old", 100, 1, 10),
        BatchDemand("load", 1000, 1, 10),
        BatchDemand("compute", 0, 100, 10),
    ]
    assert balanced_batch(rows, 2, 30, loading_ratio=100) == ("old", "compute")
    assert balanced_batch(rows, 3, 30, loading_ratio=100) == ("old", "compute", "load")


def test_strata_bundle_priority_does_not_make_allocation_free():
    rows = [
        BatchDemand("a", 100, 10, 10, (b"a",)),
        BatchDemand("b", 0, 100, 10, (b"b",)),
        BatchDemand("c", 100, 10, 10, (b"a",)),
    ]
    assert balanced_batch(rows, 2, 20, loading_ratio=100) == ("a", "c")
    assert balanced_batch(rows, 2, 10, loading_ratio=100) == ("a",)
    assert balanced_batch(rows, 2, 9, loading_ratio=100) == ()


def test_release_save_precedes_restore_without_dropping_cost():
    config = BatchConfig()
    for cls, expected in (
        (ContinuousBatcher, "restore"),
        (ReleaseFirstBatcher, "save"),
    ):
        b = cls(config)
        b.submit(Sequence("load", 256, 128, 1, 2, restore_seconds=3))
        b.submit(
            Sequence("done", 256, 0, 1, 2, save_seconds=4, phase="save", decoded=128)
        )
        step = b.begin()
        assert step.kind == expected
        assert step.seconds == (3 if expected == "restore" else 4)
        before = b.used_bytes
        _, finished = b.finish(step, step.seconds)
        assert (b.used_bytes < before) == bool(finished)


def test_full_cpu_adapter_smoke(tmp_path):
    """Small causal replay checks actual adapter dispatch, not just formulas."""
    import json
    import os

    import pytest
    import simulate_continuous_batch as sim
    from paper_baseline_search import (
        MemServeSimulation,
        MooncakeSimulation,
        StrataSimulation,
    )
    from service_portfolio_search import ServicePortfolioSimulation

    source = os.environ.get("ISSUE23_SIM_INPUTS")
    seed = os.environ.get("ISSUE23_SIM_SEED")
    if not source or not seed:
        pytest.skip("Explicit frozen simulation inputs required for integration smoke")
    inputs = json.loads(Path(source).read_text(encoding="utf-8"))
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = sim.Costs(**values)
    for cls, mode in (
        (MooncakeSimulation, "ours"),
        (MemServeSimulation, "ours"),
        (StrataSimulation, "native"),
        (ServicePortfolioSimulation, "ours"),
    ):
        model = cls(
            inputs["rows"][:4],
            inputs["initial_inventory"],
            costs,
            Path(seed),
            inputs["control_cost_rows"],
            0.49,
            mode,
            False,
            batch_config=sim.BatchConfig(),
        )
        result = model.run()
        assert result["requests"] == 4
        if cls is ServicePortfolioSimulation:
            assert all(r["full_calendar_candidates"] <= 4 for r in result["decisions"])
            assert (
                result["internal_candidate"]["global_boundary_optimum_claimed"] is False
            )
        else:
            assert result["paper_adapter"]["full_system_reproduction"] is False
        assert not model.jobs and not model.held and not model.allocation_leases.handles


def test_capacity_rejected_handoff_is_filtered_before_paper_selection(monkeypatch):
    import paper_baseline_search as module
    from pegaflow.strategies.model import Candidate

    instance = object.__new__(module.PaperRoutingSimulation)
    instance.layout = object()
    local = Candidate(0, 2, 0, 0, 0, 1, 1, 1, 2)
    oversized = Candidate(0, 2, 0, 0, 128, 1, 1, 1, 2)
    instance.candidates = lambda *_: (oversized, local)

    def physical_gate(*args, **kwargs):
        if args[8]:
            raise ValueError("Required handoff requires capacity backpressure")
        return "safe-recompute"

    monkeypatch.setattr(module, "evaluate_route", physical_gate)
    assert instance.feasible_candidates({}, object(), object()) == {
        local: "safe-recompute"
    }


def test_admission_and_iteration_share_one_scheduler():
    from paper_baseline_search import StrataSimulation

    instance = object.__new__(StrataSimulation)
    instance.admission_pending = {2: 1.0}
    assert instance.begin_iteration(2, None) is None
