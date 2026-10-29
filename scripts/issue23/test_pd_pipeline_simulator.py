"""Causal restoration, admission and calibration isolation contracts."""

import copy

import pytest
from fit_pipeline_profile import robust_fit
from pd_pipeline_simulator import PipelineSimulation, ReservedHandoffPool
from step_simulator import Job, Request, features
from test_pd_multinode_simulator import fixture


def setup():
    workload, profile = fixture()
    profile["restore_model"] = {"coefficients": [0.002, 0.064, 0], "busy_slowdown": 1}
    profile["idle_poll_s"] = 0.004
    profile["network_seconds_per_byte"] = 0
    return workload, profile


@pytest.mark.parametrize("slots", [1, 4, 16])
def test_iteration_pipeline_drains_and_obeys_slots(slots):
    workload, profile = setup()
    result = PipelineSimulation(workload, profile, {"D_max_sequences": slots}).run()
    assert result["summary"]["requests"] == 12
    assert result["summary"]["peak_running_each"][-1] <= slots
    assert result["summary"]["D_uncached_tokens"] == 12
    for r in result["per_request"]:
        d = r["stages"]["D"]
        if d["restore_bytes"]:
            assert d["engine_start"] >= d["restore_end"] >= d["restore_start"]


def test_simultaneously_ready_prefixes_share_one_copy_batch():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:4]
    for r in workload["requests"]:
        r.update(P=0, D=0)
    result = PipelineSimulation(workload, profile, {"P": 1, "D": 1}).run()
    assert any(b["request_count"] > 1 for b in result["copy_batches"])
    assert sum(b["blocks"] for b in result["copy_batches"]) == 8


def test_slowdown_changes_overlap_service_without_changing_payload():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:2]
    workload["requests"][1]["arrival_s"] = 0.04
    base = PipelineSimulation(workload, profile).run()
    alternate = copy.deepcopy(profile)
    alternate["restore_model"]["busy_slowdown"] = 4
    slow = PipelineSimulation(workload, alternate).run()
    assert sum(b["blocks"] for b in base["copy_batches"]) == sum(
        b["blocks"] for b in slow["copy_batches"]
    )
    assert slow["summary"]["finish_span"] >= base["summary"]["finish_span"]


def test_condition_weights_survive_duplicate_decode_tokens():
    xs, ys, groups = [[1], [1]], [0.01, 0.03], ["a", "b"]
    base = robust_fit(xs, ys, groups)
    repeated = robust_fit(
        xs[:1] * 100 + xs[1:], ys[:1] * 100 + ys[1:], groups[:1] * 100 + groups[1:]
    )
    assert repeated == pytest.approx(base, abs=1e-8)


def test_empty_schedule_has_no_graph_batch():
    assert features([])["padded_sequences"] == 0


def test_copy_capability_is_an_explicit_resource_input():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:1]
    profile["restore_model"] = {
        "kind": "capacity",
        "coefficients": [0, 1, 0],
        "default_copy_GBps": 1,
        "busy_slowdown": 1,
    }
    fast = PipelineSimulation(
        workload, profile, {"host_copy_GBps": {"D-host": 2}}
    ).run()
    slow = PipelineSimulation(
        workload, profile, {"host_copy_GBps": {"D-host": 1}}
    ).run()
    a, b = fast["copy_batches"][0], slow["copy_batches"][0]
    assert b["end"] - b["start"] == pytest.approx(2 * (a["end"] - a["start"]))
    assert slow["summary"]["TTFT"]["mean"] > fast["summary"]["TTFT"]["mean"]


def test_pending_restores_reserve_pages_before_they_publish_cache_hits():
    pool = ReservedHandoffPool(6, 128)
    job = Job(Request("pending", 0, 257, 1, ("a", "b"), "model"))
    pool.reserve_restore(job, 2)
    assert len(pool.free) == 3
    assert not pool.cached
    second = Job(Request("other", 0, 257, 1, ("a", "b"), "model"))
    assert pool.prefix(second) == []
    pool.publish_restore(job)
    assert len(pool.prefix(second)) == 2
    assert pool.allocate(job, 1, prefix=pool.prefix(job))
    assert all(pool.blocks[index].references == 1 for index in job.blocks)
    pool.release(job)
    assert len(pool.free) == 5


@pytest.mark.parametrize("sharing", ["fifo", "processor_sharing"])
def test_reserved_handoff_with_concurrent_network_flows_drains(sharing):
    workload, profile = setup()
    profile["reserve_restore_pages"] = True
    profile["network_seconds_per_byte"] = 1e-9
    result = PipelineSimulation(
        workload, profile, {"D": 2, "network_sharing": sharing}
    ).run()
    assert result["summary"]["requests"] == 12
    assert result["summary"]["D_uncached_tokens"] == 12
    assert all(
        row["stages"]["D"]["completion_feedback_s"] >= 0
        for row in result["per_request"]
        if row["stages"]["D"]["restore_bytes"]
    )


def test_device_copy_capability_overrides_shared_host_default():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:2]
    for i, row in enumerate(workload["requests"]):
        row.update(P=0, D=i)
    profile["restore_model"] = {
        "kind": "capacity",
        "coefficients": [0, 1, 0],
        "default_copy_GBps": 1,
        "busy_slowdown": 1,
    }
    result = PipelineSimulation(
        workload,
        profile,
        {
            "P": 1,
            "D": 2,
            "engine_hosts": ["P-host", "D-host", "D-host"],
            "device_copy_GBps": {"1": 1, "2": 2},
        },
    ).run()
    by_node = {batch["node"]: batch for batch in result["copy_batches"]}
    assert by_node[1]["work_s"] == pytest.approx(2 * by_node[2]["work_s"])


@pytest.mark.parametrize("gate", [False, True])
def test_source_publication_is_distinct_from_prefill_response(gate):
    workload, profile = setup()
    workload["requests"] = workload["requests"][:1]
    profile["P_publish_model"] = {
        "default_curve": [{"blocks": 2, "seconds": 0.2}],
        "baseline_s": 0.001,
    }
    result = PipelineSimulation(workload, profile, {"source_publish_gate": gate}).run()
    row = result["per_request"][0]
    assert row["stages"]["P"]["publish_end"] == pytest.approx(
        row["stages"]["P"]["engine_end"] + 0.2
    )
    assert row["stages"]["D"]["restore_start"] >= row["stages"]["P"]["publish_end"]
    if gate:
        assert row["stages"]["D"]["prepare"] >= row["stages"]["P"]["publish_end"]
