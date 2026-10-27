"""Check graph costs apply once per real batch and preserve restore totals."""

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from pd_graph_comparison import GraphWorld, configure_peer


def world():
    data = {
        "schema": "pegaflow-step-cost/v1",
        "models": {
            "prefill": {"coefficients": [1, 0, 0, 0, 0]},
            "decode": {"coefficients": [1, 0, 0]},
        },
        "dispatch_gap_s": 0.1,
        "response_delay_s": 0.2,
    }
    components = {
        "native": [
            {"tokens": 1024, "load_complete_seconds": 1.0, "save_RPC_seconds": 2.0}
        ]
    }
    return GraphWorld(SimpleNamespace(), data, components)


def test_singleton_first_contains_restore_and_save_once():
    model = world()
    assert model.compute(0, 2048, 1024) == pytest.approx(1.1 + 1.0 + 0.2 + 2.0)
    assert model.compute(2, 2048, 1024) == pytest.approx(1.1 + 1.0 + 0.2)


def test_peer_batch_uses_one_measured_graph_step_instead_of_sum_singletons():
    @dataclass(frozen=True)
    class Step:
        kind: str
        seconds: float
        pieces: tuple

    @dataclass(frozen=True)
    class Config:
        token_budget: int = 128

    step = Step("compute", 999.0, (("a", 1, "decode"), ("b", 1, "decode")))
    batch = SimpleNamespace(
        config=Config(),
        active={
            key: SimpleNamespace(prompt_tokens=2048, decoded=1) for key in ("a", "b")
        },
    )
    sim = SimpleNamespace(
        batch_config=Config(),
        d_batches={2: batch},
        begin_iteration=lambda node, batch: step,
    )
    configure_peer(sim, world())
    observed = sim.begin_iteration(2, batch)
    assert observed.seconds == pytest.approx(1.1)
    assert observed.pieces == step.pieces
    assert batch.config.token_budget == 2048
