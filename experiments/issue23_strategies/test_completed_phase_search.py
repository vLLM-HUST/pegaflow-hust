"""Phase ownership, no residence-as-service, and exactly-once cost replacement."""

from types import SimpleNamespace

import pytest
import simulate_continuous_batch  # noqa: F401
from completed_phase_search import CompletedPhaseCosts, exclusive_phase_observation


def test_completed_exclusive_trace_ignores_other_jobs_and_future():
    trace = [
        {"node": 2, "kind": k, "start": s, "end": e, "pieces": [("r", 1)]}
        for k, s, e in (("restore", 10, 12), ("save", 30, 31))
    ]
    cpu = [
        {
            "node": 2,
            "kind": e["kind"],
            "origin": e["start"],
            "requests": ["r"],
            "completed": e["start"] + 0.1,
            "seconds": 0.1,
        }
        for e in trace
    ]
    result = exclusive_phase_observation(trace, cpu, "r", 2, 31)
    assert result["restore"] == pytest.approx(1.9)
    assert result["save"] == pytest.approx(0.9)
    # The twenty-one-second residence interval is never a training duration.
    with pytest.raises(ValueError):
        exclusive_phase_observation(trace, cpu, "r", 2, 30)
    with pytest.raises(ValueError):
        exclusive_phase_observation(trace, cpu, "another", 2, 31)


def test_replace_only_exclusive_components_once_and_preserve_p():
    base = SimpleNamespace(
        fits=((None, None, 0, 0, 2.0),) * 4,
        restore=lambda n, p: 1.0 if p else 0.0,
        compute=lambda n, t, p: 11.0 if p else 10.0,
        decode_tail=lambda n, t: 22.0,
        decode_seconds=(22.0, 22.0),
        bytes_per_token=1,
        version=64,
        latest_completed_seconds=0,
    )
    cost = CompletedPhaseCosts(base, (({"coefficients": (2.0, 0.01)}, 3.0),) * 2, 12, 1)
    assert cost.compute(2, 200, 100) == 13
    assert cost.compute(0, 200, 100) == 11
    assert cost.compute(2, 200, 0) == 10
    assert cost.decode_tail(2, 200) == 23
    assert cost.decode_seconds == (23, 23)
    assert cost.latest_completed_seconds == 12
