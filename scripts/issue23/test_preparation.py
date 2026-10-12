import asyncio
import copy
import json
import time

import httpx
import pytest
from aiohttp import web

from analyze import trace_analysis
from client import ArmFailure, make_client, open_loop, stream_request
from common import read_json, read_jsonl, sha256, write_json
from freeze_plan import freeze, validate_plan
from runner import run_one, validate_deployment


def fixture_plan():
    requests = []
    for slot in range(4):
        rid = f"issue23-fixture-{slot}"
        requests.append({"request_id": rid, "cohort_id": "cohort-0", "smooth_slot": slot,
                         "operations": [{"operation_id": f"{rid}|0{slot}|0|k", "block_hash_hex": f"0{slot}",
                                         "slot_index": 0, "segment": "k", "source": "p", "destination": "d",
                                         "transfer_type": "load", "logical_payload_bytes": 16, "qp_index": 0}]})
    return {"schema_version": 1, "protocol_id": "fixture", "block_id": "fixture", "qps_per_peer": 2,
            "requests": requests}


def fixture_trace(plan, schedule="burst", backend="rdma"):
    events = [{"event": "experiment_opened", "schedule": schedule, "backend": backend, "monotonic_ns": 0},
              {"event": "experiment_activated", "monotonic_ns": 1}]
    for request in plan["requests"]:
        common = {k: request[k] for k in ["request_id", "cohort_id", "smooth_slot"]}
        posted = 15_000_100 if schedule == "burst" else request["smooth_slot"] * 10_000_000 + 100
        events += [{"event": "bundle_observed", "phase": "measured", "operations": request["operations"],
                    "monotonic_ns": 10, **common},
                   {"event": "bundle_eligible", "monotonic_ns": 100, **common},
                   {"event": "bundle_posted", "scheduled_offset_ms": 15 if schedule == "burst" else request["smooth_slot"] * 10,
                    "monotonic_ns": posted, **common},
                   {"event": "bundle_completed", "status": "ok", "completed_payload_bytes": 16,
                    "monotonic_ns": posted + 1000, **common}]
    if schedule == "burst":
        events.append({"event": "cohort_posting_spread", "cohort_id": "cohort-0", "posting_spread_ns": 0,
                       "pass": True, "monotonic_ns": 15_000_200})
    return events


@pytest.mark.parametrize("fault", ["duplicate_slot", "missing_member", "wrong_qp", "empty_bundle"])
def test_frozen_plan_rejects_incomplete_or_drifted_treatment(fault):
    plan = fixture_plan()
    if fault == "duplicate_slot":
        plan["requests"][1]["smooth_slot"] = 0
    elif fault == "missing_member":
        plan["requests"].pop()
    elif fault == "wrong_qp":
        plan["requests"][0]["operations"][0]["qp_index"] = 1
    else:
        plan["requests"][0]["operations"] = []
    with pytest.raises(ValueError):
        validate_plan(plan)


def test_freeze_rejects_multiple_bundles_for_same_engine_request(tmp_path):
    plan = fixture_plan()
    cases = [{k: r[k] for k in ["request_id", "cohort_id", "smooth_slot"]} for r in plan["requests"]]
    write_json(tmp_path / "cases.json", cases)
    write_json(tmp_path / "contract.json", {"protocol_id": "fixture"})
    events = [{"event": "bundle_observed", "phase": "prepare", **r} for r in plan["requests"]]
    events.append(copy.deepcopy(events[0]))
    trace = tmp_path / "trace.jsonl"
    trace.write_text("\n".join(json.dumps(e) for e in events))
    with pytest.raises(ValueError, match="repeated=1"):
        freeze(tmp_path / "cases.json", tmp_path / "contract.json", trace, tmp_path / "plan.json", "e0")
    assert not (tmp_path / "plan.json").exists()


@pytest.mark.parametrize("fault", ["backend", "spread", "missing_completion", "manifest", "slot"])
def test_analysis_rejects_invalid_transport_evidence(fault):
    plan = fixture_plan()
    events = fixture_trace(plan)
    assert trace_analysis(events, plan, "burst", "rdma")["valid"]
    if fault == "backend":
        events[0]["backend"] = "local_copy"
    elif fault == "spread":
        events[-1]["pass"] = False
        events[-1]["posting_spread_ns"] = 1_000_001
    elif fault == "missing_completion":
        events = [e for e in events if not (e["event"] == "bundle_completed" and e["request_id"].endswith("-0"))]
    elif fault == "manifest":
        events[2]["operations"] = []
    else:
        events[3]["smooth_slot"] = 3
    assert not trace_analysis(events, plan, "burst", "rdma")["valid"]


def test_engine_clocks_are_not_subtracted_across_engines():
    plan = fixture_plan()
    a = fixture_trace(plan, "smooth")
    b = copy.deepcopy(a)
    for event in b:
        event["monotonic_ns"] += 987_654_321
    left = trace_analysis(a, plan, "smooth", "rdma")
    right = trace_analysis(b, plan, "smooth", "rdma")
    assert left["valid"] and right["valid"] and left["requests"] == right["requests"]


def test_pending_hardware_cannot_be_started():
    with pytest.raises(ValueError, match="qualification pending"):
        validate_deployment({"qualification": {}})


def test_default_scheduler_requires_its_actual_qualification():
    checks = {key: True for key in ["single_replica", "pd_hybrid_correctness", "rdma_link",
                                  "runtime_identity", "cache_reset", "marker_visibility",
                                  "rank_bundle_topology", "memlock", "realtime_scheduler"]}
    deployment = {"scheduler_policy": "sched_other", "qualification": checks}
    with pytest.raises(ValueError, match="normal_scheduler"):
        validate_deployment(deployment)
    checks["normal_scheduler"] = True
    with pytest.raises(ValueError, match="Missing service URL"):
        validate_deployment(deployment)
    deployment["scheduler_policy"] = "unsupported"
    with pytest.raises(ValueError, match="Unknown scheduler policy"):
        validate_deployment(deployment)


def test_real_http_stream_ttft_open_loop_and_arm_cancellation(tmp_path):
    async def exercise():
        arrivals = []

        async def handler(request):
            body = await request.json()
            arrivals.append((body["request_id"], time.monotonic()))
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            if body.get("hang"):
                await asyncio.sleep(0.3)
                return response
            await response.write(b'data: {"choices":[{"text":"","finish_reason":null}]}\n\n')
            await asyncio.sleep(0.08)
            event = json.dumps({"choices": [{"text": "The answer is (A)", "finish_reason": "stop"}],
                                "usage": {"prompt_tokens": 7, "completion_tokens": 5}}).encode()
            await response.write(b"data: " + event[:13])
            await response.write(event[13:] + b"\n\ndata: [DONE]\n\n")
            await response.write_eof()
            return response

        app = web.Application()
        app.router.add_post("/v1/completions", handler)
        server = web.AppRunner(app)
        await server.setup()
        site = web.TCPSite(server, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        cases = [{"request_id": f"req-{i}", "cohort_id": "c", "input_tokens": 7,
                  "planned_offset_seconds": i * 0.02, "payload": {"request_id": f"req-{i}"}} for i in range(2)]
        try:
            async with make_client(2) as client:
                results = await open_loop(client, url, cases, tmp_path, 1)
                assert all(r["ttft_seconds"] > r["first_sse_seconds"] + 0.05 for r in results)
                assert arrivals[1][1] - arrivals[0][1] < 0.06
                assert all(r["text"] == "The answer is (A)" for r in results)
                failed = copy.deepcopy(cases)
                failed[0]["payload"]["hang"] = True
                failed[1]["planned_offset_seconds"] = 0.2
                target = tmp_path / "failure"
                target.mkdir()
                before = len(arrivals)
                with pytest.raises(TimeoutError):
                    await open_loop(client, url, failed, target, 0.03)
                assert len(arrivals) == before + 1
                records = read_jsonl(target / "client.jsonl")
                assert records[0]["status"] == "error" and "TimeoutError" in records[0]["error"]
        finally:
            await server.cleanup()
    asyncio.run(exercise())


@pytest.mark.parametrize("fault", ["truncated", "sse_error", "token_mismatch"])
def test_stream_failures_are_preserved(fault, tmp_path):
    event = {"choices": [{"text": "A", "finish_reason": "stop"}], "usage": {"prompt_tokens": 7, "completion_tokens": 1}}
    if fault == "sse_error":
        event = {"error": {"message": "fixture transfer failure"}}
    if fault == "token_mismatch":
        event["usage"]["prompt_tokens"] = 8
    content = "data: " + json.dumps(event) + "\n\n"
    if fault != "truncated":
        content += "data: [DONE]\n\n"

    async def exercise():
        transport = httpx.MockTransport(lambda request: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=content))
        async with httpx.AsyncClient(transport=transport) as client:
            case = {"request_id": "failure", "cohort_id": "c", "input_tokens": 7, "payload": {}}
            with pytest.raises(ArmFailure):
                await stream_request(client, "http://fixture", case, tmp_path, time.monotonic(), 0, 1)
        assert read_jsonl(tmp_path / "client.jsonl")[0]["status"] == "error"
    asyncio.run(exercise())


@pytest.mark.parametrize("failure_stage", ["start", "activate"])
def test_lifecycle_stops_whole_arm_and_keeps_failure_record(failure_stage, tmp_path, monkeypatch):
    import runner

    cases = [{"request_id": r["request_id"], "cohort_id": r["cohort_id"], "smooth_slot": r["smooth_slot"],
              "input_tokens": 7, "prompt": "fixture prompt", "planned_offset_seconds": 0,
              "payload": {"request_id": r["request_id"], "prompt": "fixture prompt"}}
             for r in fixture_plan()["requests"]]
    cases_path = tmp_path / "prepared/main.json"
    write_json(cases_path, cases)
    contract_path = tmp_path / "prepared/contract.json"
    write_json(contract_path, {"timeout_seconds": 1, "split_summary": {
        "main": {"requests": 4, "sha256": sha256(cases_path)}}})
    plan = fixture_plan()
    plan["freeze_provenance"] = {"cases_sha256": sha256(cases_path), "contract_sha256": sha256(contract_path)}
    write_json(tmp_path / "prepared/plans-main/e0.json", plan)
    deployment = {"qualification": {k: True for k in ["single_replica", "pd_hybrid_correctness", "rdma_link", "runtime_identity",
                                                     "cache_reset", "marker_visibility", "rank_bundle_topology", "memlock", "realtime_scheduler"]},
                  "router_url": "http://fixture/router", "prefill_url": "http://fixture/p", "decode_url": "http://fixture/d",
                  "prefill_ack_ranks": 2, "start_argv": ["fixture"], "stop_argv": ["fixture"],
                  "engines": [{"engine_id": "e0", "covered_tp_ranks": [0, 1], "url": "http://fixture/engine",
                               "trace_path": "{run_dir}/trace.jsonl"}]}
    calls = []

    async def fake_hook(deployment, key, environment, output, timeout):
        calls.append(key)
        if key == "start_argv" and failure_stage == "start":
            raise RuntimeError("fixture partial startup failure")

    def response(request):
        if str(request.url).endswith("/issue23/activate"):
            return httpx.Response(412, text="fixture hidden snapshot incomplete")
        if str(request.url).endswith("/v1/completions"):
            content = 'data: {"choices":[{"text":"A","finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":1}}\n\ndata: [DONE]\n\n'
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=content)
        return httpx.Response(200, json=True)

    monkeypatch.setattr(runner, "hook", fake_hook)
    monkeypatch.setattr(runner, "make_client", lambda count: httpx.AsyncClient(transport=httpx.MockTransport(response)))
    with pytest.raises((RuntimeError, httpx.HTTPStatusError)):
        asyncio.run(run_one(tmp_path, deployment, "main", "arm", "rdma-smooth", "failure-arm"))
    assert "stop_argv" in calls
    output = tmp_path / "runs/failure-arm"
    assert read_json(output / "status.json")["status"] == "failed"
    assert not (output / "client.jsonl").exists()
    if failure_stage == "activate":
        assert len(read_jsonl(output / "prepare/client.jsonl")) == 4
