"""Lifecycle and memory contracts for the four-engine serving scenario."""

import pytest
from pd_serving_simulator import (
    BLOCK_BYTES,
    DOMAIN,
    HostCache,
    ServingSimulation,
    memory_budget,
    seed_gpu,
)
from step_simulator import Job, KVPool, Request


def test_budget_uses_hardware_memory_and_runtime_headroom():
    budget = memory_budget()
    assert budget["NPU_KV_GiB"] == 38
    assert budget["NPU_KV_GiB"] + budget["measured_non_KV_GiB"] < 64 * 0.9
    assert (
        2 * budget["CPU_pool_GiB_per_instance"] + budget["host_headroom_GiB_per_node"]
        == 64
    )


def test_host_eviction_preserves_a_pinned_rdma_source():
    cache = HostCache(2 * BLOCK_BYTES)
    cache.add(["a", "b"])
    cache.pins["a"] += 1
    cache.add(["c"])
    assert set(cache.keys) == {"a", "c"}
    assert cache.evictions == 1
    cache.pins["c"] += 1
    with pytest.raises(ValueError, match="Pinned"):
        cache.add(["d"])


def test_completed_gpu_restore_becomes_an_apc_hit_without_overwriting_active_pages():
    pool = KVPool(10)
    active = Job(Request("active", 0, 256, 1, ("old",), DOMAIN))
    assert pool.allocate(active, 128)
    page = active.blocks[0]
    seed_gpu(pool, ["new0", "new1"])
    following = Job(Request("following", 0, 257, 1, ("new0", "new1"), DOMAIN))
    assert len(pool.prefix(following)) == 2
    assert pool.blocks[page].references == 1
    assert pool.blocks[page].key == (DOMAIN, "old")


def test_p_and_d_batch_and_full_handoff_leaves_only_logits_suffix():
    rows = []
    for step in range(2):
        for task in "abcdefgh":
            tokens = 5001 + step * 128
            rows.append(
                {
                    "id": f"{task}:{step}",
                    "task": task,
                    "tokens": tokens,
                    "hashes": [f"{task}-{i}" for i in range((tokens - 1) // 128)],
                }
            )
    bundle = {
        "rows": rows,
        "initial_inventory": [{"cache": {"blocks": []}} for _ in range(4)],
        "costs": {"seconds_per_pd_byte": 1e-10},
    }
    protocol = {
        "test_order": [r["id"] for r in rows],
        "arrival_condition": {"offered_arrivals_seconds": [0] * len(rows)},
    }
    model = {
        "schema": "pegaflow-step-cost/v1",
        "models": {
            "decode": {"coefficients": [0.01, 0, 0]},
            "prefill": {"coefficients": [0.01, 0.01, 0, 0, 0]},
        },
        "dispatch_gap_s": 0.0001,
        "response_delay_s": 0.001,
    }
    components = {
        "native": [
            {
                "tokens": 128,
                "save_RPC_seconds": 0.001,
                "load_complete_seconds": 0.001,
                "query_seconds": 0.0001,
            }
        ]
    }
    result = ServingSimulation(bundle, protocol, model, components, p_tokens=2048).run()
    assert result["summary"]["requests"] == 16
    assert max(result["summary"]["peak_running_each"][:2]) > 1
    assert max(result["summary"]["peak_running_each"][2:]) > 1
    completed = {r["id"]: r for r in result["per_request"]}
    for row in rows:
        actual = completed[row["id"]]
        assert actual["stages"]["D"]["cached_tokens"] == len(row["hashes"]) * 128
        assert actual["stages"]["P"]["engine_end"] <= actual["stages"]["D"]["prepare"]
        if row["id"].endswith(":1"):
            assert actual["ready"] >= completed[row["task"] + ":0"]["end"]
