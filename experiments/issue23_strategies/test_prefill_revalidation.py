from types import SimpleNamespace

from analyze_prefill_revalidation import (
    active_operations,
    completed_samples,
    quote_recovery,
)


def test_busy_source_changes_comparison_without_restore_penalty():
    costs = SimpleNamespace(
        bytes_per_token=1,
        copy_seconds=lambda *args: 0.1,
        compute=lambda node, tokens, prefix: 4 if prefix else 6,
    )
    assert (
        quote_recovery(costs, 0, 1000, 128, 1, 0)[
            "cold_margin_before_new_control_NOT_net_TTFT"
        ]
        < 0
    )
    assert (
        quote_recovery(costs, 0, 1000, 128, 1, 5)[
            "cold_margin_before_new_control_NOT_net_TTFT"
        ]
        > 0
    )


def test_future_completion_cannot_train_live_source_or_close_its_gate():
    start = {
        "key": "a",
        "operation": "P_api",
        "event": "start",
        "seconds": 2,
        "resources": ["gate1"],
        "predicted_seconds": 3,
    }
    end = {**start, "event": "finish", "seconds": 20}
    assert active_operations([start, end], 10) == [start]
    assert completed_samples([start, end], 1, 10) == []
    assert completed_samples([start, end], 1, 20) == [(3, 18)]
