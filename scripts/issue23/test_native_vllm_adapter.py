"""Native integration contracts; opt in with NATIVE_VLLM_MODEL in vLLM 0.23."""

import os
from types import SimpleNamespace

import pytest
from native_vllm_adapter import NativeVllmEngine
from pd_pipeline_simulator import create_pipeline_simulation
from test_pd_pipeline_simulator import setup

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("NATIVE_VLLM_MODEL"),
        reason="Set NATIVE_VLLM_MODEL for native runtime tests",
    ),
]


def engine(**kwargs):
    return NativeVllmEngine(
        model_path=os.environ["NATIVE_VLLM_MODEL"],
        num_blocks=64,
        token_budget=512,
        max_model_len=4096,
        **kwargs,
    )


def drain(core, limit=500):
    rows = []
    for _ in range(limit):
        if not core.scheduler.get_num_unfinished_requests():
            return rows
        rows.append(core.schedule())
        core.complete()
    raise AssertionError("Native scheduler failed to drain")


def test_native_chunking_and_shared_prefix_reuse():
    core = engine(max_sequences=2)
    try:
        core.add("first", 1025, 3, 0, prefix_hashes=["a", "b", "c", "d", "e", "f", "g", "h"])
        rows = drain(core)
        assert [row[0][1] for row in rows] == [512, 512, 1, 1, 1]
        core.add("warm", 1025, 2, 1, prefix_hashes=["a", "b", "c", "d", "e", "f", "g", "h"])
        assert core.schedule() == [("warm", 1, 1024)]
        core.complete()
        drain(core)
    finally:
        core.shutdown()


def test_native_remote_pages_wait_for_worker_completion():
    allocated = []
    callbacks = SimpleNamespace(
        query=lambda request, computed: (256 - computed, True),
        allocated=lambda request, blocks, count: allocated.append((request.request_id, count)),
        finished=lambda request, blocks: False,
    )
    core = engine(transfer=callbacks)
    try:
        core.add("load", 257, 2, 0, prefix_hashes=["a", "b"])
        assert core.schedule() == []
        core.complete()
        assert allocated == [("load", 256)]
        assert core.snapshot()["load"]["status"] == "WAITING_FOR_REMOTE_KVS"
        core.signal_transfer("load")
        assert core.schedule() == [("load", 1, 256)]
        core.complete()
        drain(core)
    finally:
        core.shutdown()


def test_native_preemption_eventually_drains():
    core = NativeVllmEngine(
        model_path=os.environ["NATIVE_VLLM_MODEL"],
        num_blocks=6,
        token_budget=256,
        max_model_len=1024,
        max_sequences=4,
        scheduler_options={"scheduler_reserve_full_isl": False},
    )
    try:
        with pytest.raises(ValueError, match="cannot fit"):
            core.add("oversized", 700, 1, 0)
        for i in range(3):
            core.add(str(i), 257, 130, 0, prefix_hashes=[f"{i}a", f"{i}b"])
        drain(core, 1000)
        assert core.preemptions > 0
    finally:
        core.shutdown()


@pytest.mark.parametrize("producers,consumers", [(1, 1), (2, 2)])
def test_native_pipeline_async_publication_and_handoff(producers, consumers):
    workload, profile = setup()
    profile["P_publish_model"] = {
        "default_curve": [{"blocks": 2, "seconds": 0.02}],
        "baseline_s": 0.001,
    }
    result = create_pipeline_simulation(
        workload,
        profile,
        {
            "scheduler_backend": "vllm",
            "native_model_path": os.environ["NATIVE_VLLM_MODEL"],
            "P": producers,
            "D": consumers,
            "P_token_budget": 128,
            "D_token_budget": 128,
            "max_sequences": 4,
            "P_routing": "round_robin",
            "D_routing": "round_robin",
        },
    ).run()
    assert result["summary"]["requests"] == 12
    assert result["summary"]["D_total_prefill_tokens"] == 12
    assert len(result["native_scheduler"]) == producers + consumers
    assert max(result["summary"]["peak_running_each"]) <= 4
    for row in result["per_request"]:
        p, d = row["stages"]["P"], row["stages"]["D"]
        if d["restore_bytes"]:
            assert d["restore_start"] >= p["publish_end"]
            assert d["engine_start"] >= d["restore_end"]


def test_native_exact_block_prompt_keeps_a_logits_suffix():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:1]
    workload["requests"][0]["prompt_tokens"] = 256
    result = create_pipeline_simulation(
        workload,
        profile,
        {
            "scheduler_backend": "vllm",
            "native_model_path": os.environ["NATIVE_VLLM_MODEL"],
            "P": 1,
            "D": 1,
            "max_sequences": 4,
        },
    ).run()
    assert result["summary"]["D_total_prefill_tokens"] == 128
