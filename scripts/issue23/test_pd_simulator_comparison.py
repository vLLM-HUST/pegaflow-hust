"""Guard trace comparisons against invalid identity and clock contracts."""

import pytest

from report_pd_simulator_comparison import normalized


def test_peer_nominal_and_dependency_clocks_recover_ready_latency():
    result = {
        "per_request": [
            {
                "id": "x",
                "dependency": 30.0,
                "nominal": 35.0,
                "end": 52.0,
                "TTFT": 5.0,
                "p": 0,
                "d": 2,
            }
        ]
    }
    row = normalized(result, "peer", ["x"], {"x": 10.0})["x"]
    assert (row["ready"], row["first"], row["E2E"]) == (40.0, 45.0, 12.0)


def test_duplicate_completions_are_rejected():
    row = {"id": "x", "ready": 0, "first": 1, "end": 2, "TTFT": 1, "P": 0, "D": 2}
    with pytest.raises(ValueError, match="exactly once"):
        normalized({"per_request": [row, row]}, "ours", ["x"], {"x": 0})


@pytest.mark.parametrize("first,end,ttft", [(3, 2, 3), (1, 2, 5), (float("nan"), 2, 1)])
def test_invalid_trace_clocks_are_rejected(first, end, ttft):
    row = {
        "id": "x",
        "ready": 0,
        "first": first,
        "end": end,
        "TTFT": ttft,
        "P": 0,
        "D": 2,
    }
    with pytest.raises(ValueError):
        normalized({"per_request": [row]}, "ours", ["x"], {"x": 0})
