"""State-machine checks and measured single-engine regression coverage."""

from dataclasses import replace

import pytest
from step_simulator import (
    CostModel,
    Job,
    KVPool,
    Request,
    Simulator,
    UnsupportedCapacity,
    compare,
)


def constant_cost(delay=0.0):
    return CostModel(
        {
            "schema": "pegaflow-step-cost/v1",
            "models": {
                "decode": {"coefficients": [0.1, 0.0, 0.0]},
                "prefill": {"coefficients": [0.1, 0.0, 0.0, 0.0, 0.0]},
            },
            "response_delay_s": delay,
        }
    )


def config(**changes):
    return {"token_budget": 128, "max_sequences": 8, "total_blocks": 100, **changes}


def test_kv_grows_by_scheduled_tokens():
    request = Request("long", 0.0, 1024, 4)
    result = Simulator([request], config(), constant_cost()).run()
    assert result["steps"][0]["active_KV_blocks"] == 1
    assert result["summary"]["peak_active_KV_blocks"] == 9
    assert len(result["steps"]) == 11
    assert result["requests"][0]["first_s"] == pytest.approx(0.8)
    assert result["requests"][0]["finish_s"] == pytest.approx(1.1)


def test_prefix_can_be_shared_within_the_same_scheduled_step():
    a = Request("a", 0.0, 128, 2, ("p0",))
    b = Request("b", 0.0, 256, 2, ("p0", "p1"))
    result = Simulator([a, b], config(token_budget=256), constant_cost()).run()
    assert result["requests"][1]["cached_tokens"] == 128
    assert result["steps"][0]["prefill_tokens"] == 256
    assert result["steps"][0]["active_KV_blocks"] == 2


def test_private_domains_keep_identical_prompts_separate():
    a = Request("a", 0.0, 256, 2, ("p0", "p1"), "a")
    b = replace(a, id="b", cache_domain="b")
    result = Simulator([a, b], config(token_budget=512), constant_cost()).run()
    assert result["summary"]["cache_hit_tokens"] == 0
    assert result["steps"][0]["active_KV_blocks"] == 4


def test_shared_page_is_released_after_its_last_reference():
    pool = KVPool(10)
    a = Job(Request("a", 0.0, 256, 1, ("p0", "p1")))
    b = Job(Request("b", 0.0, 256, 1, ("p0", "p1")))
    assert pool.allocate(a, 128)
    shared = a.blocks[0]
    assert pool.allocate(b, 128, pool.prefix(b))
    assert pool.blocks[shared].references == 2
    pool.release(a)
    assert pool.blocks[shared].references == 1
    assert shared not in pool.free
    pool.release(b)
    assert pool.blocks[shared].references == 0
    assert shared in pool.free


def test_free_tail_pages_are_evicted_before_prefix_pages():
    pool = KVPool(4)
    a = Job(Request("a", 0.0, 256, 1, ("p0", "p1")))
    assert pool.allocate(a, 256)
    prefix, tail = a.blocks
    pool.release(a)
    b = Job(Request("b", 0.0, 256, 1, ("q0", "q1")))
    assert pool.allocate(b, 256)
    assert pool.blocks[prefix].key == ("shared", "p0")
    assert pool.blocks[tail].key != ("shared", "p1")
    assert pool.evictions == 1


def test_capacity_failure_is_explicit_instead_of_free_preemption():
    with pytest.raises(UnsupportedCapacity, match="preemption"):
        Simulator([Request("a", 0.0, 1024, 1)], config(total_blocks=3), constant_cost()).run()


def test_session_waits_for_client_completion():
    a = Request("a", 0.0, 128, 1)
    b = Request("b", 0.0, 128, 1, depends_on="a")
    result = Simulator([a, b], config(), constant_cost(delay=0.03)).run()
    assert result["requests"][0]["finish_s"] == pytest.approx(0.13)
    assert result["requests"][1]["admitted_s"] >= result["requests"][0]["finish_s"]


def test_cyclic_dependencies_are_rejected():
    a = Request("a", 0.0, 128, 1, depends_on="b")
    b = Request("b", 0.0, 128, 1, depends_on="a")
    with pytest.raises(ValueError, match="dependencies"):
        Simulator([a, b], config(), constant_cost()).run()


def test_batching_uses_one_shared_engine_step():
    requests = [Request("a", 0.0, 128, 1), Request("b", 0.0, 128, 1)]
    simultaneous = Simulator(requests, config(token_budget=256), constant_cost()).run()
    serial = Simulator(requests, config(token_budget=256, max_sequences=1), constant_cost()).run()
    assert simultaneous["summary"]["finish_span_s"] == pytest.approx(0.1)
    assert serial["summary"]["finish_span_s"] == pytest.approx(0.2)


def test_waiting_policy_does_not_preempt_running_work():
    requests = [
        Request("active", 0.0, 128, 4),
        Request("long", 0.01, 512, 1),
        Request("short", 0.01, 128, 1),
    ]
    fcfs = Simulator(requests, config(max_sequences=1), constant_cost()).run()
    short = Simulator(
        requests, config(max_sequences=1, policy="shortest_remaining"), constant_cost()
    ).run()
    assert fcfs["requests"][0]["finish_s"] == short["requests"][0]["finish_s"]
    assert short["requests"][2]["first_s"] < short["requests"][1]["first_s"]
    assert fcfs["requests"][1]["first_s"] < fcfs["requests"][2]["first_s"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("arrival_s", -1),
        ("arrival_s", float("nan")),
        ("prompt_tokens", 0),
        ("output_tokens", 1.5),
    ],
)
def test_invalid_requests_are_rejected(field, value):
    row = {
        "id": "a",
        "arrival_s": 0.0,
        "prompt_tokens": 128,
        "output_tokens": 1,
        field: value,
    }
    with pytest.raises(ValueError):
        Request.from_dict(row)


def test_observed_latencies_cannot_enter_request_inputs():
    with pytest.raises(ValueError, match="Unexpected request fields"):
        Request.from_dict(
            {
                "id": "a",
                "arrival_s": 0,
                "prompt_tokens": 128,
                "output_tokens": 1,
                "observed_first_s": 4.0,
            }
        )


@pytest.mark.parametrize("coefficients", [[-1, 0, 0], [float("nan"), 0, 0], []])
def test_invalid_cost_model_is_rejected(coefficients):
    data = constant_cost().data
    data["models"]["decode"]["coefficients"] = coefficients
    with pytest.raises(ValueError, match="coefficients"):
        CostModel(data)


def test_uncalibrated_token_budget_requires_explicit_extrapolation():
    cost = constant_cost()
    cost.data["metadata"] = {"token_budget": 128}
    with pytest.raises(ValueError, match="calibration support"):
        Simulator([Request("a", 0.0, 128, 1)], config(token_budget=256), cost)
    result = Simulator(
        [Request("a", 0.0, 128, 1)],
        config(token_budget=256, allow_extrapolation=True),
        cost,
    ).run()
    assert result["extrapolation"] == ["token_budget"]


def test_dependency_validation_excludes_predecessor_wait():
    requests = [Request("a", 0, 128, 1), Request("b", 0, 128, 1, depends_on="a")]
    result = Simulator(requests, config(), constant_cost(delay=0.03)).run()
    observed = {
        "requests": [
            {k: r[k] for k in ("id", "arrival_s", "first_s", "finish_s", "cached_tokens")}
            for r in result["requests"]
        ]
    }
    validation = compare(result, observed)
    assert validation["relative_error_percent"]["TTFT_mean_s"] == pytest.approx(0)
