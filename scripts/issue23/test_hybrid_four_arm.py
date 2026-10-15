"""Four-arm validity must cover every group and the same frozen data."""

import copy
import json

import pytest
from analyze import trace_analysis
from common import read_json, write_json
from deployment_runtime import engine_command, environment, model_command
from freeze_plan import freeze, operation_identity, validate_plan
from prepare_four_arm import prepare


def plan_fixture():
    requests = []
    for slot in range(4):
        rid = f"request-{slot}"
        operations = []
        for group in range(4):
            op = {
                "namespace": "ns",
                "storage_group_id": group,
                "block_hash_hex": f"0{slot}" + (f"{group:08x}" if group else ""),
                "slot_index": 0,
                "segment": "k",
                "source": "p",
                "destination": "d",
                "transfer_type": "load",
                "logical_payload_bytes": 16 * (group + 1),
                "qp_index": group % 2,
            }
            op["operation_id"] = operation_identity(rid, op)
            operations.append(op)
        requests.append(
            {
                "request_id": rid,
                "cohort_id": "c0",
                "smooth_slot": slot,
                "operations": operations,
            }
        )
    return {
        "schema_version": 2,
        "protocol_id": "fixture",
        "block_id": "fixture",
        "qps_per_peer": 2,
        "requests": requests,
    }


def trace_fixture(plan, schedule="burst", backend="rdma"):
    events = [
        {
            "event": "experiment_opened",
            "schedule": schedule,
            "backend": backend,
            "monotonic_ns": 0,
            "smooth_scheduler_policy": "sched_other",
            "smooth_scheduler_priority": 0,
        },
        {"event": "experiment_activated", "monotonic_ns": 1},
    ]
    for request in plan["requests"]:
        rid = request["request_id"]
        eligible = 11 + request["smooth_slot"]
        posted = (
            14
            + (15 if schedule == "burst" else request["smooth_slot"] * 10) * 1_000_000
        )
        completed = posted + 1_000_000
        for op in request["operations"]:
            group = op["storage_group_id"]
            common = {
                "request_id": rid,
                "namespace": op["namespace"],
                "storage_group_id": group,
            }
            events.extend(
                [
                    {
                        "event": "bundle_part_observed",
                        "phase": "measured",
                        "operations": [dict(op, qp_index=0)],
                        "monotonic_ns": 2 + group,
                        **common,
                    },
                    {
                        "event": "bundle_part_eligible",
                        "monotonic_ns": 3 + group,
                        **common,
                    },
                    {
                        "event": "fetch_completed",
                        "completed_payload_bytes": op["logical_payload_bytes"],
                        "monotonic_ns": completed + group + 1,
                        **common,
                    },
                ]
            )
        common = {k: request[k] for k in ("request_id", "cohort_id", "smooth_slot")}
        events.extend(
            [
                {
                    "event": "bundle_observed",
                    "phase": "measured",
                    "operations": request["operations"],
                    "monotonic_ns": 10,
                    **common,
                },
                {"event": "bundle_eligible", "monotonic_ns": eligible, **common},
                {
                    "event": "bundle_assembled",
                    "group_assembly_ns": 8,
                    "manifest_and_prepare_ns": 1,
                    "monotonic_ns": eligible,
                    **common,
                },
                {
                    "event": "bundle_posted",
                    "scheduled_offset_ms": 15
                    if schedule == "burst"
                    else request["smooth_slot"] * 10,
                    "monotonic_ns": posted,
                    **common,
                },
                {
                    "event": "bundle_completed",
                    "status": "ok",
                    "completed_payload_bytes": 160,
                    "monotonic_ns": completed,
                    **common,
                },
            ]
        )
        if backend == "local_copy":
            events.append(
                {
                    "event": "local_copy_work",
                    "request_id": rid,
                    "copy_started_ns": posted + 1,
                    "copy_finished_ns": completed - 1,
                    "copy_ns": completed - posted - 2,
                    "completed_payload_bytes": 160,
                    "monotonic_ns": completed - 1,
                }
            )
    if schedule == "burst":
        events.append(
            {
                "event": "cohort_posting_spread",
                "cohort_id": "c0",
                "posting_spread_ns": 0,
                "pass": True,
                "monotonic_ns": 15_000_015,
            }
        )
    return events


@pytest.mark.parametrize(
    "schedule,backend",
    [(s, b) for s in ("burst", "smooth") for b in ("rdma", "local_copy")],
)
def test_complete_group_evidence_qualifies_each_original_arm(schedule, backend):
    plan = plan_fixture()
    validate_plan(plan)
    result = trace_analysis(
        trace_fixture(plan, schedule, backend), plan, schedule, backend, "sched_other"
    )
    assert result["valid"]
    assert all(len(groups) == 4 for groups in result["storage_groups"].values())


@pytest.mark.parametrize(
    "fault",
    ["missing_group", "repeated_group", "namespace", "bytes", "source", "extra_group"],
)
def test_partial_or_changed_recurrent_evidence_rejects_the_arm(fault):
    plan = plan_fixture()
    events = trace_fixture(plan)
    index = next(
        i
        for i, e in enumerate(events)
        if e.get("event") == "bundle_part_observed" and e.get("storage_group_id") == 2
    )
    if fault == "missing_group":
        events.pop(index)
    elif fault == "repeated_group":
        events.append(copy.deepcopy(events[index]))
    elif fault == "namespace":
        events[index]["namespace"] = "other"
    elif fault == "source":
        events[index]["operations"][0]["source"] = "other"
    elif fault == "extra_group":
        extra = copy.deepcopy(events[index])
        extra["storage_group_id"] = 9
        events.append(extra)
    else:
        next(
            e
            for e in events
            if e.get("event") == "fetch_completed" and e.get("storage_group_id") == 2
        )["completed_payload_bytes"] += 1
    assert not trace_analysis(events, plan, "burst", "rdma")["valid"]


@pytest.mark.parametrize("fault", [None, "missing", "duplicate"])
def test_freeze_assembles_all_groups_and_requires_complete_coverage(tmp_path, fault):
    plan = plan_fixture()
    cases = [
        {k: request[k] for k in ("request_id", "cohort_id", "smooth_slot")}
        for request in plan["requests"]
    ]
    events = []
    for request in plan["requests"]:
        for op in request["operations"]:
            events.append(
                {
                    "event": "bundle_part_observed",
                    "phase": "prepare",
                    "request_id": request["request_id"],
                    "namespace": op["namespace"],
                    "storage_group_id": op["storage_group_id"],
                    "operations": [dict(op, qp_index=0)],
                }
            )
    if fault == "missing":
        events.pop()
    if fault == "duplicate":
        events.append(copy.deepcopy(events[0]))
    write_json(tmp_path / "cases.json", cases)
    write_json(tmp_path / "contract.json", {"protocol_id": "fixture"})
    (tmp_path / "trace.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    args = (
        tmp_path / "cases.json",
        tmp_path / "contract.json",
        tmp_path / "trace.jsonl",
        tmp_path / "plan.json",
        "engine",
    )
    if fault:
        with pytest.raises(ValueError):
            freeze(*args, required_groups=[0, 1, 2, 3], covered_tp_ranks=[0, 1])
        assert not (tmp_path / "plan.json").exists()
    else:
        freeze(*args, required_groups=[0, 1, 2, 3], covered_tp_ranks=[0, 1])
        frozen = read_json(tmp_path / "plan.json")
        assert frozen["requests"] == plan["requests"]
        assert frozen["freeze_provenance"]["covered_tp_ranks"] == [0, 1]


def test_preparation_preserves_inputs_and_freezes_runtime_budget(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    original = []
    for cohort, split in enumerate(("smoke", "frontier-smoke")):
        cases = [
            {
                "request_id": f"r{cohort}-{slot}",
                "cohort_id": f"c{cohort}",
                "input_tokens": 4100 + slot,
                "prompt": [1, 2, 3],
                "dataset": split,
                "payload": {"prompt": [1, 2, 3], "request_id": f"r{cohort}-{slot}"},
            }
            for slot in range(4)
        ]
        original.extend(copy.deepcopy(cases))
        write_json(source / f"{split}.json", cases)
    write_json(source / "contract.json", {"prefix_caching_hash_algo": "sha256"})
    root = tmp_path / "run"
    contract = prepare(source, root, rounds=2)
    cases = read_json(root / "prepared/smoke.json")
    assert len({c["payload"]["cache_salt"] for c in cases}) == 8
    for before, after in zip(original, cases, strict=True):
        assert before["prompt"] == after["prompt"] == after["payload"]["prompt"]
        assert before["input_tokens"] == after["input_tokens"]
        assert after["payload"]["max_tokens"] == 64
    config = {
        "model_path": "/model",
        "model_revision": "rev",
        "runtime": {"python": "/python", "bin": "/bin"},
        "nodes": {
            "prefill": {
                "host": "p",
                "engine_port": 1,
                "engine_http_port": 2,
                "model_port": 3,
            },
            "decode": {
                "host": "d",
                "meta_port": 4,
                "engine_port": 5,
                "engine_http_port": 6,
                "model_port": 7,
            },
        },
    }
    for role in config["nodes"]:
        argv = model_command(config, role)
        for flag, key in (
            ("--max-model-len", "max_model_len"),
            ("--max-num-seqs", "max_num_seqs"),
            ("--max-num-batched-tokens", "max_num_batched_tokens"),
            ("--kv-cache-memory-bytes", "device_cache_bytes_per_card"),
        ):
            assert argv[argv.index(flag) + 1] == str(contract[key])
        engine = engine_command(config, role, root, "capture")
        assert engine[engine.index("--devices") + 1] == "0,1"
        assert (
            engine[engine.index("--pool-size") + 1]
            == str(contract["host_pool_bytes_per_node"]) + "b"
        )
        assert environment(config, role, root)["PEGAFLOW_ISSUE23_HYBRID_PREFETCH"] == (
            "1" if role == "decode" else "0"
        )


def test_node_trace_reader_ignores_an_unfinished_append(tmp_path):
    from node_agent import process_job

    output = tmp_path / "runs" / "trial"
    output.mkdir(parents=True)
    (output / "engine.jsonl").write_text(
        '{"event":"complete"}\n{"event":', encoding="utf-8"
    )
    result = process_job(
        {"root": str(tmp_path)},
        "decode",
        {
            "action": "read-trace",
            "run_dir": str(output),
            "request_ids": ["engine.jsonl"],
        },
    )
    assert result["events"] == [{"event": "complete"}]


@pytest.mark.parametrize("profile", ["performance", "eager_sync"])
def test_inference_profile_controls_graph_and_async_execution(profile):
    config = {
        "model_path": "/model",
        "model_revision": "revision",
        "max_num_seqs": 16,
        "inference_profile": profile,
        "runtime": {"python": "/python"},
        "nodes": {"decode": {"host": "host", "model_port": 1, "engine_port": 2}},
    }
    argv = model_command(config, "decode")
    if profile == "performance":
        assert "--enforce-eager" not in argv
        assert "--async-scheduling" in argv
        compilation = json.loads(argv[argv.index("--compilation-config") + 1])
        assert compilation["cudagraph_mode"] == "FULL_DECODE_ONLY"
        assert compilation["cudagraph_capture_sizes"] == [1, 2, 4, 8, 12, 16]
    else:
        assert "--enforce-eager" in argv
        assert "--no-async-scheduling" in argv
        assert "--compilation-config" not in argv


def test_scaled_cohorts_follow_frozen_size_and_reject_partial_batches():
    from scaled_campaign import cohort_groups

    cases = [{"request_id": str(i)} for i in range(128)]
    groups = cohort_groups(cases, {"cohort_size": 16})
    assert len(groups) == 8 and all(len(group) == 16 for group in groups)
    assert [item for group in groups for item in group] == cases
    with pytest.raises(ValueError, match="Complete cohorts"):
        cohort_groups(cases[:-1], {"cohort_size": 16})


def test_network_competitor_uses_host_read_and_multiple_qps():
    from rdma_background import command

    server = command(None, 29942, qps=16)
    client = command("192.168.0.4", 29942, qps=16)
    assert client[:-1] == server and client[-1] == "192.168.0.4"
    assert "/usr/bin/ib_read_bw" in client
    assert "--rate_limit" not in client
    assert client[client.index("-q") + 1] == "16"


def test_cleanup_attempts_both_nodes_when_first_stop_fails(tmp_path, monkeypatch):
    import asyncio

    import scaled_campaign

    attempted = []

    def stop(config, role, action, output):
        attempted.append(role)
        if role == "decode":
            raise OSError("node unavailable")

    monkeypatch.setattr(scaled_campaign, "request", stop)
    with pytest.raises(RuntimeError, match="Service cleanup failed"):
        asyncio.run(scaled_campaign.stop_services({}, tmp_path, tmp_path))
    assert attempted == ["decode", "prefill"]
    record = read_json(tmp_path / "cleanup.json")
    assert record["status"] == "failed" and record["errors"][0]["role"] == "decode"
