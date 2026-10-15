"""Freeze actual engine capture events; fail on missing or repeated bundles."""

import argparse
import re
from collections import Counter
from pathlib import Path

from common import read_json, read_jsonl, sha256, write_json

OPERATION_FIELDS = {
    "operation_id",
    "block_hash_hex",
    "slot_index",
    "segment",
    "source",
    "destination",
    "transfer_type",
    "logical_payload_bytes",
    "qp_index",
}
HYBRID_OPERATION_FIELDS = OPERATION_FIELDS | {"namespace", "storage_group_id"}


def operation_identity(rid, op, schema=2):
    suffix = f"{op['block_hash_hex']}|{op['slot_index']}|{op['segment']}"
    if schema == 1:
        return f"{rid}|{suffix}"
    namespace = op["namespace"]
    return f"{rid}|{len(namespace.encode('utf-8'))}:{namespace}|g{op['storage_group_id']}|{suffix}"


def validate_plan(plan):
    if plan["schema_version"] not in (1, 2) or plan["qps_per_peer"] != 2:
        raise ValueError("Unsupported plan schema or QP count")
    if not plan["requests"] or not plan["protocol_id"] or not plan["block_id"]:
        raise ValueError("Empty plan identity or request set")
    request_ids, operation_ids, cohorts = set(), set(), {}
    for request in plan["requests"]:
        rid = request["request_id"]
        if not rid or rid in request_ids or not request["cohort_id"]:
            raise ValueError(f"Duplicate or empty request identity: {rid}")
        request_ids.add(rid)
        cohorts.setdefault(request["cohort_id"], []).append(request["smooth_slot"])
        if not request["operations"]:
            raise ValueError(f"Empty transfer bundle: {rid}")
        for index, op in enumerate(request["operations"]):
            if set(op) != (
                HYBRID_OPERATION_FIELDS
                if plan["schema_version"] == 2
                else OPERATION_FIELDS
            ):
                raise ValueError(f"Unexpected operation fields: {rid}")
            value = op["block_hash_hex"]
            if not value or len(value) % 2 or not re.fullmatch(r"[0-9a-fA-F]+", value):
                raise ValueError(f"Invalid block hash: {rid}")
            if op["logical_payload_bytes"] <= 0 or op["slot_index"] < 0:
                raise ValueError(f"Invalid payload or slot: {rid}")
            if op["segment"] not in {"k", "v"} or op["transfer_type"] != "load":
                raise ValueError(f"Invalid operation kind: {rid}")
            if not op["source"] or not op["destination"] or op["qp_index"] != index % 2:
                raise ValueError(f"Invalid endpoint or QP mapping: {rid}")
            if plan["schema_version"] == 2 and (
                not op["namespace"]
                or not isinstance(op["storage_group_id"], int)
                or op["storage_group_id"] < 0
            ):
                raise ValueError(f"Invalid namespace or storage group: {rid}")
            expected_id = operation_identity(rid, op, plan["schema_version"])
            if op["operation_id"] != expected_id or expected_id in operation_ids:
                raise ValueError(f"Invalid or duplicate operation identity: {rid}")
            operation_ids.add(expected_id)
        if plan["schema_version"] == 2 and not any(
            op["storage_group_id"] == 0 for op in request["operations"]
        ):
            raise ValueError(f"Frozen request lacks attention group: {rid}")
    if any(
        sorted(slots) != list(range(plan.get("cohort_size", 4)))
        for slots in cohorts.values()
    ):
        raise ValueError("Every cohort must contain slots 0,1,2,3 exactly once")


def freeze(
    cases_path,
    contract_path,
    trace_path,
    output,
    engine_id,
    required_groups=None,
    covered_tp_ranks=None,
):
    cases, contract, events = (
        read_json(cases_path),
        read_json(contract_path),
        read_jsonl(trace_path),
    )
    expected = {case["request_id"] for case in cases}
    parts = [event for event in events if event.get("event") == "bundle_part_observed"]
    observed = [event for event in events if event.get("event") == "bundle_observed"]
    if parts:
        if observed:
            raise ValueError("Capture mixed complete and partial bundle protocols")
        grouped = {}
        for event in parts:
            rid = event["request_id"]
            if rid not in expected or event.get("phase") != "prepare":
                raise ValueError("Unexpected request or measured group in capture")
            key = (event["namespace"], event["storage_group_id"])
            groups = grouped.setdefault(rid, {})
            if key in groups:
                raise ValueError(f"Repeated group capture: {rid}/{key}")
            if not event["operations"] or any(
                (op["namespace"], op["storage_group_id"]) != key
                for op in event["operations"]
            ):
                raise ValueError(f"Group identity drift: {rid}")
            groups[key] = event["operations"]
        if set(grouped) != expected:
            raise ValueError("Capture lacks one or more requests")
        observed = []
        cohort_groups = {}
        for case in cases:
            groups = grouped[case["request_id"]]
            ids = sorted(group for _, group in groups)
            if required_groups is not None and ids != sorted(required_groups):
                raise ValueError(
                    f"Incomplete hybrid group set: {case['request_id']} observed={ids} expected={required_groups}"
                )
            keys = frozenset(groups)
            previous = cohort_groups.setdefault(case["cohort_id"], keys)
            if keys != previous:
                raise ValueError(
                    "Cohort requests have different storage groups or namespaces"
                )
            operations = [dict(op) for key in sorted(groups) for op in groups[key]]
            for index, op in enumerate(operations):
                op["qp_index"] = index % 2
            observed.append(
                {
                    "request_id": case["request_id"],
                    "phase": "prepare",
                    "operations": operations,
                }
            )
    counts = Counter(event["request_id"] for event in observed)
    if set(counts) != expected or any(n != 1 for n in counts.values()):
        raise ValueError(
            f"Capture requires one bundle per request per engine; missing={len(expected - set(counts))}, "
            f"extra={len(set(counts) - expected)}, repeated={sum(n != 1 for n in counts.values())}. "
            "Qualify TP/cache-group bundle topology before freezing."
        )
    if any(event.get("phase") != "prepare" for event in observed):
        raise ValueError("Capture trace contains measured bundles")
    by_id = {event["request_id"]: event for event in observed}
    plan = {
        "schema_version": 2 if parts else 1,
        "protocol_id": contract["protocol_id"],
        "block_id": f"{Path(cases_path).stem}-{engine_id}",
        "qps_per_peer": 2,
        "cohort_size": contract.get("cohort_size", 4),
        "smooth_interval_ms": contract.get("smooth_interval_ms", 10),
        "rdma_rate_bps": contract.get("rdma_rate_bps", 0),
        "requests": [
            {
                "request_id": case["request_id"],
                "cohort_id": case["cohort_id"],
                "smooth_slot": case["smooth_slot"],
                "operations": by_id[case["request_id"]]["operations"],
            }
            for case in cases
        ],
        "freeze_provenance": {
            "engine_id": engine_id,
            "cases_sha256": sha256(cases_path),
            "contract_sha256": sha256(contract_path),
            "trace_sha256": sha256(trace_path),
        },
    }
    validate_plan(plan)
    if covered_tp_ranks is not None:
        plan["freeze_provenance"]["covered_tp_ranks"] = covered_tp_ranks
    if required_groups is not None:
        plan["freeze_provenance"]["storage_group_ids"] = required_groups
    unique = {}
    for request in plan["requests"]:
        for op in request["operations"]:
            key = (
                op.get("namespace", ""),
                op["block_hash_hex"],
                op["slot_index"],
                op["segment"],
            )
            if key in unique and unique[key] != op["logical_payload_bytes"]:
                raise ValueError("Conflicting logical size for the same hidden segment")
            unique[key] = op["logical_payload_bytes"]
    write_json(output, plan)
    return {
        "engine_id": engine_id,
        "requests": len(cases),
        "plan_sha256": sha256(output),
        "unique_segments": len(unique),
        "unique_logical_bytes": sum(unique.values()),
        "pool_budget_note": "logical bytes exclude allocator/runtime overhead and transient copies",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--trace", required=True)
    parser.add_argument("--engine-id", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--required-groups", type=int, nargs="+")
    args = parser.parse_args()
    summary = freeze(
        args.cases,
        args.contract,
        args.trace,
        args.output,
        args.engine_id,
        args.required_groups,
    )
    write_json(args.output.with_suffix(".summary.json"), summary)
    print(summary)


if __name__ == "__main__":
    main()
