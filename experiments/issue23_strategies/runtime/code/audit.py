"""Read-only independent audit of full-path formal A/B and physical receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
from pathlib import Path


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def worker_directory(host, role, phase, slot):
    if role == "P":
        return host / "workers" / phase / f"P{slot}"
    if role == "D":
        return host / phase / f"D{slot}"
    raise ValueError("Unknown owned worker role")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--p", type=Path, required=True)
    parser.add_argument("--d", type=Path, required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    protocol = read(args.p / "protocol.json")
    manifest = read(args.workload / "manifest.json")
    assert sha(args.workload / "requests.jsonl") == manifest["requests_sha256"]
    assert sha(args.p / "cost-model.json") == protocol["cost_sha256"]
    assert protocol["source_manifest"] == manifest
    candidate = protocol.get("candidate_revision")
    reference_policy = protocol.get("baseline_routing", "task-affinity")
    assert reference_policy in ("task-affinity", "shortest-queue", "local-prefix-affinity")
    if reference_policy != "task-affinity":
        assert sha(args.p / "joint_baselines-executed-source.py") == protocol["baseline_reference_source_sha256"]
        assert "not native vLLM or full Mooncake" in protocol["baseline"]
    if candidate is None:
        assert protocol["frozen_before_test"] is True
    else:
        assert candidate in ("event-queue-01", "resource-DAG-01")
        assert protocol["frozen_before_test"] is False
        assert protocol["exposed_replay_not_fresh_heldout"] is True
        event_contract = read(args.p / "event-queue-contract.json")
        assert event_contract["candidate_revision"] == candidate
        assert event_contract["cost_sha256"] == protocol["cost_sha256"]
        assert event_contract["cost_coefficients_refitted"] is False
        assert event_contract["algorithm_revised_after_exposed_results"] is True
        expected_sources = {"joint_policy", "joint_queue"}
        if candidate == "resource-DAG-01":
            expected_sources |= {"operation_cost", "operation_schedule", "resource_queue", "resource_runtime", "resource_placement"}
            assert protocol["operation_seed_initial_test_data_used"] is False
            assert protocol["slow_work_seconds"] is None
            assert event_contract["additional_operation_model_seed"] == "original32calibration-only"
            assert event_contract["additional_operation_model_online_update"] == "past-completed-only"
            assert event_contract["test_outcome_tuned_thresholds"] is False
            seed = read(args.p / "operation-seed-manifest.json")
            assert sha(args.p / "operation-seed-manifest.json") == protocol["operation_seed_manifest_sha256"]
            assert len(seed) == 32
            for name, expected in seed.items():
                assert sha(args.p / "operation-seed" / name) == expected
        assert set(protocol["policy_source_sha256"]) == expected_sources
        for name, expected_sha in protocol["policy_source_sha256"].items():
            assert event_contract["executed_policy_sources"][name]["sha256"] == expected_sha
            assert sha(args.p / (name + "-executed-source.py")) == expected_sha
    assert protocol["planning_and_allocation_and_copy_and_wait_included"] is True
    pipeline = protocol.get("execution_mode") == "independent_P_D_FIFO_queues"
    if pipeline:
        assert protocol["global_end_to_end_inflight_limit"] is None
        assert protocol["per_instance_active_limit"] == 1
        contract = read(args.p / "pipeline-contract.json")
        assert contract["global_end_to_end_inflight_limit"] is None
        assert contract["recalibration_or_test_based_retuning"] is False
        assert contract["cost_sha256"] == protocol["cost_sha256"]
    assert (protocol["P"], protocol["D"], protocol["tp"], protocol["dtype"]) == (2, 2, 1, "bfloat16")
    inputs = {r["id"]: r for r in (json.loads(line) for line in (args.workload / "requests.jsonl").read_text(encoding="utf-8").splitlines())}
    interval, ids = protocol["arrival_interval_seconds"], protocol["test_order"]
    causal_arrivals = protocol.get("TTFT_origin") == "causal_ready_before_router_queue"
    arrival_schedule = None
    if causal_arrivals:
        from arrivals import load_condition

        assert sha(args.p / "arrival-manifest.json") == protocol["arrival_manifest_sha256"]
        _, condition = load_condition(args.p / "arrival-manifest.json", protocol["arrival_condition"]["name"], protocol)
        assert condition == protocol["arrival_condition"]
        assert protocol["objective_metric"] == "offered_to_first_seconds"
        arrival_schedule = condition["offered_arrivals_seconds"]
    assert len(ids) == len(set(ids)) and interval > 0
    positions, arms, summary, source = {key: i for i, key in enumerate(ids)}, {}, {}, None
    for role, directory in (("P", args.p), ("D", args.d)):
        result = read(directory / "result.json")
        assert result["exit_code"] == 0 and result["stop_reason"] is None, role
    assert read(args.p / "pair-complete.json")["completed"] is True
    for phase in ("baseline", "treatment"):
        directory = args.p / phase
        started = read(directory / "formal-arm-started.json")
        assert started["protocol_sha256"] == sha(args.p / "protocol.json")
        records = [json.loads(line) for line in (directory / "requests.jsonl").read_text(encoding="utf-8").splitlines()]
        assert len(records) == len(ids) and {r["id"] for r in records} == set(ids)
        ledger, phasesources, session_ends = {}, [], {}
        stage_intervals = [[] for _ in range(4)]
        for node in range(4):
            host, role, slot = (args.p, "P", node) if node < 2 else (args.d, "D", node - 2)
            worker = worker_directory(host, role, phase, slot)
            configuration = read(worker / "configuration.json")
            assert configuration["tensor_parallel_size"] == 1 and configuration["dtype"] == "bfloat16"
            assert configuration["max_num_batched_tokens"] == configuration["block_size"] == 128
            assert configuration["kv_cache_memory_bytes"] == 2 * 1024**3
            assert read(worker / "prefix-authority.json")["host_inventory_authority"] is True
            actual = read(worker / "provenance.json")["sources"]
            assert actual["vllm"]["path"].startswith("/vllm-workspace/")
            phasesources.append(actual)
        if source is None:
            source = phasesources[0]
        assert all(value == source for value in phasesources), "Actual serving source differs"
        for item in records:
            row = inputs[item["id"]]
            assert item["task"] == row["task"] and item["step_id"] == row["step_id"]
            if causal_arrivals:
                nominal = arrival_schedule[positions[item["id"]]]
                eligible = max(nominal, session_ends.get(row["task"], 0))
                assert item["offered_arrival_seconds"] == nominal
                assert item["arrival_seconds"] == eligible
                assert item["causality_wait_seconds"] == eligible - nominal
                assert math.isclose(item["offered_to_first_seconds"], item["ttft_seconds"] + eligible - nominal,
                                    rel_tol=0, abs_tol=1e-9)
                session_ends[row["task"]] = eligible + item["completion_seconds"]
            else:
                assert item["arrival_seconds"] == positions[item["id"]] * interval
            previous = ledger.get(row["task"], -1)
            assert row["step_id"] > previous
            ledger[row["task"]] = row["step_id"]
            assert item["route"]["p"] in (0, 1) and item["route"]["d"] in (2, 3)
            assert item["route_trials"] <= protocol["fast_candidate_budget"]
            assert item["route_seconds"] >= item["route_cpu_seconds"] >= 0
            for role, count in (("P", 1), ("D", 128)):
                response = item[role]
                assert response["id"] == row["id"] and response["prompt_tokens"] == len(row["prompt_token_ids"])
                output = response["output_token_ids"]
                assert len(output) == count and all(type(token) is int for token in output)
                assert hashlib.sha256(json.dumps(output).encode()).hexdigest() == response["output_sha256"]
                assert response["rdma_bytes"] == 0, "Unpriced implicit worker fetch"
                assert response["num_cached_tokens"] <= response["explicit_restore_limit"] * 128
                assert response["local_completed_demand_join_passed"] is True
            assert item["P"]["output_token_ids"][0] == item["D"]["output_token_ids"][0]
            measured = item["D_submission_seconds"] - item["arrival_seconds"] + item["D_wire"]["first_seconds"]
            assert math.isclose(measured, item["ttft_seconds"], rel_tol=0, abs_tol=1e-9)
            assert item["completion_seconds"] >= item["ttft_seconds"] > 0
            if candidate is not None:
                boundary = item["boundary_revalidation"]
                assert boundary["future_admission_assumed"] is False
                assert boundary["after_actual_P"] == item["route"]["d_blocks"]
                assert item["D"]["explicit_restore_limit"] <= boundary["after_actual_P"]
                if candidate == "resource-DAG-01" and phase == "treatment":
                    route = item["route"]
                    assert 0 <= route["model_latest_completed_seconds"] <= route["forecast_origin_seconds"]
                    assert route["forecast_origin_seconds"] <= item["pipeline_timing"]["P_enqueued_seconds"]
                    assert route["objective_scope"] == "admitted-live-requests-plus-candidate"
            if pipeline:
                timing = item["pipeline_timing"]
                for label, node in (("P", item["route"]["p"]), ("D", item["route"]["d"])):
                    enqueued, begun, ended = (timing[label + suffix] for suffix in
                        ("_enqueued_seconds", "_started_seconds", "_finished_seconds"))
                    assert 0 <= enqueued <= begun <= ended
                    stage_intervals[node].append((begun, ended))
                    assert begun <= item[label + "_submission_seconds"] <= ended
                assert timing["P_finished_seconds"] <= timing["D_enqueued_seconds"]
                assert item["dispatch_seconds"] <= timing["P_enqueued_seconds"]
        if pipeline:
            for intervals in stage_intervals:
                ordered = sorted(intervals)
                assert all(previous[1] <= following[0] for previous, following in zip(ordered, ordered[1:]))
            pipeline_summary = read(directory / "pipeline-summary.json")
            assert pipeline_summary["global_end_to_end_limit"] is None
            assert pipeline_summary["per_instance_active_limit"] == 1
            if candidate is not None:
                queue_events = read(directory / "event-queue-ledger.json")
                assert queue_events["live_requests"] == 0
                assert queue_events["future_D_order"] == "predicted_P_completion_not_router_submission"
                assert queue_events["right_censored_overrun_is_completed_sample"] is False
                assert sum(queue_events["completed_stage_samples"]) == 2 * len(ids)
                for key in ids:
                    observed = [event for event in queue_events["events"] if event["key"] == key]
                    assert sum(event["event"] == "started" for event in observed) == 2
                    assert sum(event["event"] == "finished" for event in observed) == 2
                    assert sum(event["event"] == "first" for event in observed) == 1
        arms[phase] = {r["id"]: r for r in records}
        actions = read(directory / "cache-actions.json")
        expected, copies, removals, failed = [0] * 4, 0, 0, 0
        for action in actions:
            if candidate == "resource-DAG-01" and action["kind"] == "inventory_read":
                assert action["seconds"] >= 0 and action["nodes"] == list(range(4))
                continue
            if candidate == "resource-DAG-01" and action["kind"] == "owned_copy_resource":
                assert 0 <= action["link_wait_seconds"] <= action["held_seconds"]
                assert action["gate_wait_seconds"] >= 0 and action["payload_bytes"] > 0
                continue
            request = action["request"]
            if action["kind"].startswith("copy"):
                node = action["operation"]["destination"]
                expected[node] += action["actual_rdma_bytes"]
                assert action["source_preserved"] is True and action["rpc_seconds"] >= 0
                if action["kind"] == "copy_failed":
                    failed += 1
                    continue
                receipt = action["receipt"]
                assert receipt["allocated_before_rdma"] is True and receipt["published_locally"] is True
                assert receipt["distributed_atomicity_claimed"] is False
                assert 0 < receipt["allocated_bytes"] <= request["max_allocation_bytes"]
                assert receipt["allocated_bytes"] == receipt["staging_peak_bytes"]
                assert receipt["payload_bytes"] == action["actual_rdma_bytes"]
                assert receipt["source"] == request["remote_addr"]
                assert receipt["generation_before"] == request["generation"]
                assert receipt["generation_after"] == request["generation"] + 1
                copies += 1
            elif action["kind"] == "remove":
                receipt = action["receipt"]
                assert receipt["actions_applied"] is True and receipt["local_only"] is True
                assert receipt["generation_before"] == request["generation"]
                assert receipt["generation_after"] == request["generation"] + 1
                assert receipt["released_allocation_bytes"] > 0
                removals += 1
            else:
                raise AssertionError("Unknown cache action")
        before, after = read(directory / "counters-before.json"), read(directory / "counters-after.json")
        assert [end - begin for begin, end in zip(before, after, strict=True)] == expected
        inventories = read(directory / "final-inventory.json")
        assert len(inventories) == 4
        for inventory in inventories:
            assert inventory["allocator_capacity_bytes"] == protocol["pool_bytes_per_instance"]
            assert inventory["allocator_used_bytes"] <= inventory["allocator_capacity_bytes"]
            assert inventory["migration_supported"] is True
        values = sorted(r["ttft_seconds"] for r in records)
        summary[phase] = {"requests": len(records), "ttft_p50_seconds": values[math.ceil(0.5 * len(values)) - 1],
            "ttft_p99_seconds_descriptive": values[math.ceil(0.99 * len(values)) - 1],
            "phase_seconds_including_final_async_commit": read(directory / "phase-complete.json")["seconds_including_planning_and_final_commit"],
            "actual_copy_bytes_by_destination": expected, "copy_receipts_including_warmup": copies,
            "selected_removal_receipts": removals, "failed_copy_attempts_including_cost": failed,
            "fast_routing_wall_seconds": sum(r["route_seconds"] for r in records),
            "fast_routing_cpu_seconds": sum(r["route_cpu_seconds"] for r in records),
            "copy_RPC_seconds": sum(r["rpc_seconds"] for r in actions if r["kind"].startswith("copy")),
            "link_wait_seconds": sum(r.get("link_wait_seconds", 0) for r in actions if r["kind"].startswith("copy"))}
        if phase == "treatment":
            policy = read(directory / "policy.json")
            for event in policy:
                assert set(event["completed_ids"]) <= set(ids)
                if pipeline:
                    assert all(arms[phase][key]["arrival_seconds"] + arms[phase][key]["completion_seconds"]
                               <= event["started_seconds"] for key in event["completed_ids"])
                if causal_arrivals:
                    assert event["completed_arrivals_seconds"] == {
                        key: arms[phase][key]["offered_arrival_seconds"] for key in event["completed_ids"]}
                if "plan" in event:
                    assert event["plan"]["trials"] <= protocol["slow_candidate_budget"]
                    assert event["plan"]["completed_ids"] == event["completed_ids"]
                    if candidate is not None and event["plan"]["transition"]["operations"]:
                        assert event["plan"]["predicted_p99"] < event["plan"]["initial_p99"]
            summary[phase]["slow_planning_and_execution_wall_seconds"] = sum(e["total_seconds"] for e in policy)
            summary[phase]["slow_planning_and_execution_cpu_seconds"] = sum(e["cpu_seconds"] for e in policy)
            if candidate == "resource-DAG-01":
                model = read(directory / "operation-costs.json")
                assert sum(r["calibration"] for r in model["records"]) == 64
                assert len(model["records"]) == 64 + 2*len(ids)
                for r in model["records"]:
                    if not r["calibration"]:
                        assert r["id"] in arms[phase]
                        label = "P" if r["node"] < 2 else "D"
                        row = arms[phase][r["id"]]
                        assert row["route"][label.lower()] == r["node"]
                        assert r["completed_seconds"] >= row[label+"_submission_seconds"]
                qe = read(directory / "event-queue-ledger.json")
                assert qe["resource_live_requests"] == 0 and qe["source_and_destination_gates_modelled"] is True
                events = qe["resource_operation_events"]
                for key in ids:
                    for name in ("P_api", "D_api"):
                        pairs = [e for e in events if e["key"] == key and e["operation"] == name]
                        assert [e["event"] for e in pairs] == ["start", "finish"]
                        assert pairs[0]["seconds"] <= pairs[1]["seconds"]
        if pipeline:
            summary[phase]["pipeline"] = pipeline_summary
        if causal_arrivals:
            actual = sorted(row["arrival_seconds"] for row in records)
            gaps = [b - a for a, b in zip(actual, actual[1:])]
            offered_latencies = sorted(row["offered_to_first_seconds"] for row in records)
            summary[phase]["arrival_condition"] = protocol["arrival_condition"]["name"]
            summary[phase]["TTFT_origin"] = protocol["TTFT_origin"]
            summary[phase]["offered_to_first_p99_seconds_descriptive"] = offered_latencies[math.ceil(0.99 * len(records)) - 1]
            summary[phase]["primary_objective_metric"] = "offered_to_first_seconds"
            summary[phase]["upstream_causality_wait_seconds"] = sum(row["causality_wait_seconds"] for row in records)
            summary[phase]["realized_causal_ingress_rps"] = 1 / statistics.mean(gaps)
            summary[phase]["realized_causal_ingress_gap_cv"] = statistics.pstdev(gaps) / statistics.mean(gaps)
            summary[phase]["actual_ingress_identical_across_arms_claimed"] = False
    assert all(arms["baseline"][key]["D"]["output_token_ids"] == arms["treatment"][key]["D"]["output_token_ids"] for key in ids)
    report = {"passed": True, "formal_request_pairs": len(ids), "summary": summary,
              "candidate_revision": candidate, "exposed_replay_not_fresh_heldout": candidate is not None,
              "independent_hardware_interference_audit_pending": True,
              "stable_p99_or_task_accuracy_claimed": False, "global_optimality_claimed": False}
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
