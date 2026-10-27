"""Contracts required to compare a fresh serial simulation with historical runs."""

from types import SimpleNamespace

import pytest
from pd_serial_simulator import OriginalWorld, Simulation, load_runtime, quantiles


def test_small_sample_tail_ranks():
    result = quantiles(list(range(1, 96)))
    assert result["p95"] == 91
    assert result["p99"] == result["max"] == 95


def test_zero_payload_does_not_charge_a_copy():
    world = OriginalWorld(SimpleNamespace(transfer_fixed=2.0))
    assert world.copy_seconds(0, 2, 0) == 0.0
    assert world.copy_seconds(0, 0, 100) == 0.0


def test_native_reclamation_can_drop_a_waiting_requests_published_prefix():
    rt = load_runtime()
    sim = object.__new__(Simulation)
    block_bytes = 128 * 57344
    sim.rt, sim.costs = rt, SimpleNamespace(bytes_per_token=57344)
    sim.protocol = {"source_derived_reclaim_batch": 512}
    sim.layout = rt["Layout"](((), (), (), ()), (2 * block_bytes,) * 4, (0,) * 4)
    sim.now, sim.actions, sim.jobs = 0.0, [], {"waiting": {}}
    sim.rows = {
        "waiting": {
            "id": "waiting",
            "task": "session",
            "tokens": 257,
            "hashes": [b"a".hex(), b"b".hex()],
        }
    }
    sim.publish(0, "waiting", 0, b"a")
    sim.publish(0, "waiting", 1, b"b")
    assert sim.layout.prefix(0, sim.demand("waiting").hashes) == 2
    sim.publish(0, "another", 0, b"c")
    assert sim.layout.prefix(0, sim.demand("waiting").hashes) == 0
    assert sim.actions[0]["pending_prefix_drops"]["waiting"] == [2, 0]
    assert sim.layout.feasible()


def test_target_latencies_cannot_enter_workload_rows():
    bundle = {"rows": [{"id": "x", "task": "s", "tokens": 1, "hashes": [], "TTFT": 5}]}
    with pytest.raises(ValueError, match="unexpected fields"):
        Simulation({}, bundle, {}, None)
