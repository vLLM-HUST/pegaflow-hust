"""Recompute client latency, task quality, and engine timing validity."""

import argparse
import re
from collections import Counter
from pathlib import Path

from common import distribution, read_json, read_jsonl, sha256, write_json
from freeze_plan import validate_plan


def extracted_answer(text):
    matches = re.findall(r"(?:[Tt]he answer is|[Aa]nswer\s*:)\s*\(?([A-J])\)?", text)
    if matches:
        return matches[-1]
    return text.strip() if re.fullmatch(r"[A-J]", text.strip()) else None


def trace_analysis(events, plan, schedule, backend, scheduler_policy=None):
    validate_plan(plan)
    expected = {r["request_id"]: r for r in plan["requests"]}
    active = [e for e in events if e.get("event") == "experiment_activated"]
    issues = []
    if len(active) != 1:
        return {"valid": False, "issues": ["expected one activation"]}
    opened = [e for e in events if e.get("event") == "experiment_opened"]
    if (
        len(opened) != 1
        or opened[0].get("schedule") != schedule
        or opened[0].get("backend") != backend
    ):
        issues.append("experiment backend or schedule identity mismatch")
    if schedule == "smooth" and opened and opened[0].get("smooth_scheduler_error"):
        issues.append("smooth scheduler configuration failed")
    if schedule == "smooth" and opened:
        policy = opened[0].get("smooth_scheduler_policy")
        priority = opened[0].get("smooth_scheduler_priority")
        if policy not in {"sched_other", "sched_fifo"} or priority != {
            "sched_other": 0,
            "sched_fifo": 10,
        }.get(policy):
            issues.append("smooth scheduler policy or priority is invalid")
        if scheduler_policy is not None and policy != scheduler_policy:
            issues.append("smooth scheduler differs from the qualified deployment")
    measured = [e for e in events if e.get("monotonic_ns", -1) > active[0]["monotonic_ns"]]
    by_kind = {}
    for kind in ["bundle_observed", "bundle_eligible", "bundle_posted", "bundle_completed"]:
        selected = [e for e in measured if e.get("event") == kind]
        counts = Counter(e["request_id"] for e in selected)
        if set(counts) != set(expected) or any(v != 1 for v in counts.values()):
            issues.append(f"{kind}: missing, extra or repeated requests")
        by_kind[kind] = {e["request_id"]: e for e in selected}
    rows = []
    tg = {}
    for rid, event in by_kind["bundle_eligible"].items():
        if rid in expected:
            cohort = expected[rid]["cohort_id"]
            tg[cohort] = max(tg.get(cohort, 0), event["monotonic_ns"])
    for rid, request in expected.items():
        if any(rid not in by_kind[k] for k in by_kind):
            continue
        observed = by_kind["bundle_observed"][rid]
        eligible, posted, completed = (
            by_kind[k][rid] for k in ["bundle_eligible", "bundle_posted", "bundle_completed"]
        )
        if observed.get("phase") != "measured" or observed["operations"] != request["operations"]:
            issues.append(f"manifest drift: {rid}")
        for event in [eligible, posted]:
            if (
                event.get("cohort_id") != request["cohort_id"]
                or event.get("smooth_slot") != request["smooth_slot"]
            ):
                issues.append(f"cohort or slot identity mismatch: {rid}")
        logical = sum(op["logical_payload_bytes"] for op in request["operations"])
        if completed.get("status") != "ok" or completed.get("completed_payload_bytes") != logical:
            issues.append(f"completion error or byte mismatch: {rid}")
        offset = [0, 10, 20, 30][request["smooth_slot"]] if schedule == "smooth" else 15
        if posted.get("scheduled_offset_ms") != offset:
            issues.append(f"target offset mismatch: {rid}")
        gate = tg[request["cohort_id"]]
        if (
            not observed["monotonic_ns"]
            <= eligible["monotonic_ns"]
            <= gate
            <= posted["monotonic_ns"]
            <= completed["monotonic_ns"]
        ):
            issues.append(f"event timing order mismatch: {rid}")
        rows.append(
            {
                "request_id": rid,
                "cohort_id": request["cohort_id"],
                "gate_wait_ms": (gate - eligible["monotonic_ns"]) / 1e6,
                "posting_lateness_ms": (posted["monotonic_ns"] - gate) / 1e6 - offset,
                "completion_observation_ms": (completed["monotonic_ns"] - posted["monotonic_ns"])
                / 1e6,
            }
        )
    if schedule == "burst":
        spread = [e for e in measured if e.get("event") == "cohort_posting_spread"]
        cohorts = {r["cohort_id"] for r in plan["requests"]}
        if Counter(e.get("cohort_id") for e in spread) != Counter(dict.fromkeys(cohorts, 1)):
            issues.append("missing or repeated burst spread checks")
        if any(not e.get("pass") or e["posting_spread_ns"] > 1_000_000 for e in spread):
            issues.append("burst posting spread exceeded 1 ms")
    if any(e.get("event") == "cohort_post_task_failed" for e in measured):
        issues.append("cohort posting task failed")
    group_report = {}
    if plan["schema_version"] == 2:
        group_kinds = {"bundle_part_observed", "bundle_part_eligible", "fetch_completed"}
        expected_groups = {
            (rid, op["namespace"], op["storage_group_id"])
            for rid, request in expected.items()
            for op in request["operations"]
        }
        if any(
            (e.get("request_id"), e.get("namespace"), e.get("storage_group_id"))
            not in expected_groups
            for e in measured
            if e.get("event") in group_kinds
        ):
            issues.append("unexpected storage group or request evidence")
        for rid, request in expected.items():
            planned = {}
            for op in request["operations"]:
                key = (op["namespace"], op["storage_group_id"])
                planned.setdefault(key, []).append(op)
            groups = []
            for key, ops in planned.items():
                selected = {
                    kind: [
                        e
                        for e in measured
                        if e.get("event") == kind
                        and e.get("request_id") == rid
                        and (e.get("namespace"), e.get("storage_group_id")) == key
                    ]
                    for kind in ("bundle_part_observed", "bundle_part_eligible", "fetch_completed")
                }
                if any(len(events) != 1 for events in selected.values()):
                    issues.append(f"missing or repeated storage group evidence: {rid}/{key[1]}")
                    continue
                observed, eligible, completed = (selected[k][0] for k in selected)
                part_ops = [dict(op, qp_index=i % 2) for i, op in enumerate(ops)]
                if observed.get("phase") != "measured" or observed.get("operations") != part_ops:
                    issues.append(f"group manifest drift: {rid}/{key[1]}")
                if completed.get("completed_payload_bytes") != sum(
                    op["logical_payload_bytes"] for op in ops
                ):
                    issues.append(f"group completion byte mismatch: {rid}/{key[1]}")
                assembled = by_kind["bundle_eligible"].get(rid)
                full_completion = by_kind["bundle_completed"].get(rid)
                if (
                    assembled
                    and full_completion
                    and not (
                        observed["monotonic_ns"]
                        <= eligible["monotonic_ns"]
                        <= assembled["monotonic_ns"]
                        <= full_completion["monotonic_ns"]
                        <= completed["monotonic_ns"]
                    )
                ):
                    issues.append(f"group timing order mismatch: {rid}/{key[1]}")
                groups.append(
                    {
                        "namespace": key[0],
                        "storage_group_id": key[1],
                        "logical_payload_bytes": sum(op["logical_payload_bytes"] for op in ops),
                        "eligible_to_full_bundle_ms": (
                            assembled["monotonic_ns"] - eligible["monotonic_ns"]
                        )
                        / 1e6
                        if assembled
                        else None,
                        **{
                            field: completed.get(field)
                            for field in (
                                "connect_ns",
                                "remote_query_ns",
                                "allocation_and_descriptor_ns",
                                "submit_ns",
                                "gate_and_transport_wait_ns",
                                "total_ns",
                            )
                        },
                    }
                )
            group_report[rid] = groups
        if any(e.get("event") in {"fetch_unavailable", "fetch_failed"} for e in measured):
            issues.append("one or more frozen group fetches failed or were unavailable")
        for row in rows:
            rid = row["request_id"]
            assembly = [
                e
                for e in measured
                if e.get("event") == "bundle_assembled" and e.get("request_id") == rid
            ]
            if len(assembly) != 1:
                issues.append(f"missing or repeated bundle assembly evidence: {rid}")
            else:
                row.update(
                    {
                        "group_assembly_ms": assembly[0]["group_assembly_ns"] / 1e6,
                        "manifest_and_prepare_ms": assembly[0]["manifest_and_prepare_ns"] / 1e6,
                    }
                )
            if backend == "local_copy":
                copies = [
                    e
                    for e in measured
                    if e.get("event") == "local_copy_work" and e.get("request_id") == rid
                ]
                if len(copies) != 1:
                    issues.append(f"missing or repeated local copy work evidence: {rid}")
                else:
                    copy = copies[0]
                    posted, completed = (
                        by_kind[k][rid] for k in ("bundle_posted", "bundle_completed")
                    )
                    submit_started = posted["monotonic_ns"] - posted.get("submit_ns", 0)
                    if (
                        copy.get("completed_payload_bytes")
                        != completed.get("completed_payload_bytes")
                        or copy["copy_ns"] != copy["copy_finished_ns"] - copy["copy_started_ns"]
                        or not submit_started
                        <= copy["copy_started_ns"]
                        <= copy["copy_finished_ns"]
                        <= completed["monotonic_ns"]
                    ):
                        issues.append(f"local copy work byte or timing mismatch: {rid}")
                    row["copy_ms"] = copy["copy_ns"] / 1e6
                    row["copy_dispatch_wait_ms"] = (copy["copy_started_ns"] - submit_started) / 1e6
    return {
        "valid": not issues,
        "issues": issues,
        "requests": rows,
        "scheduler_policy": opened[0].get("smooth_scheduler_policy") if opened else None,
        "scheduler_priority": opened[0].get("smooth_scheduler_priority") if opened else None,
        "posting_lateness_ms": distribution([r["posting_lateness_ms"] for r in rows]),
        **({"storage_groups": group_report} if plan["schema_version"] == 2 else {}),
        "clock_note": "Each engine's monotonic clock is analyzed independently; completion is an observation timestamp.",
    }


def analyze_run(cases, results, traces, plans, schedule, backend, scheduler_policy=None):
    by_id = {r["request_id"]: r for r in results}
    expected = {c["request_id"] for c in cases}
    failures = []
    if set(by_id) != expected or len(by_id) != len(results):
        failures.append("client result identity mismatch")
    if any(r.get("status") != "ok" for r in results):
        failures.append("one or more client requests failed")
    if any(r.get("dispatch_lateness_ms", 0) > 100 for r in results):
        failures.append("open-loop dispatch lateness exceeded 100 ms")
    if set(traces) != set(plans):
        failures.append("engine trace set differs from the frozen plan set")
    case_identity = {c["request_id"]: (c["cohort_id"], c["smooth_slot"]) for c in cases}
    for engine, plan in plans.items():
        validate_plan(plan)
        plan_identity = {
            r["request_id"]: (r["cohort_id"], r["smooth_slot"]) for r in plan["requests"]
        }
        if plan_identity != case_identity:
            failures.append(f"frozen plan request identity mismatch: {engine}")
    trace_reports = {
        engine: trace_analysis(events, plans[engine], schedule, backend, scheduler_policy)
        for engine, events in traces.items()
        if engine in plans
    }
    if not trace_reports or any(not r["valid"] for r in trace_reports.values()):
        failures.append("engine trace qualification failed")
    quality = []
    for case in cases:
        result = by_id.get(case["request_id"], {})
        answer = extracted_answer(result.get("text", "")) if case["dataset"] == "mmlu-pro" else None
        quality.append(
            {
                "request_id": case["request_id"],
                "category": case["category"],
                "extracted_answer": answer,
                "correct": answer == case["reference_answer"]
                if case["dataset"] == "mmlu-pro"
                else None,
                "parse_failed": answer is None if case["dataset"] == "mmlu-pro" else None,
                "finish_reason": result.get("finish_reason"),
                "output_tokens": (result.get("usage") or {}).get("completion_tokens"),
                "output_truncated": result.get("finish_reason") == "length",
            }
        )
    successful = [r for r in results if r.get("status") == "ok"]
    return {
        "valid_timing_arm": not failures,
        "validity_issues": failures,
        "client_requests": len(results),
        "client_failures": len(results) - len(successful),
        "ttft_seconds": distribution([r["ttft_seconds"] for r in successful]),
        "e2e_seconds": distribution([r["e2e_seconds"] for r in successful]),
        "dispatch_lateness_ms": distribution([r["dispatch_lateness_ms"] for r in results]),
        "quality": quality,
        "engine_reports": trace_reports,
        "accuracy": sum(q["correct"] is True for q in quality) / len(quality)
        if quality and cases[0]["dataset"] == "mmlu-pro"
        else None,
        "grading_note": "FrontierScience outputs require a separately frozen grader or documented manual review.",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", required=True)
    parser.add_argument("--results", required=True)
    parser.add_argument("--schedule", choices=["smooth", "burst"], required=True)
    parser.add_argument("--backend", choices=["rdma", "local_copy"], required=True)
    parser.add_argument("--scheduler-policy", choices=["sched_other", "sched_fifo"])
    parser.add_argument("--trace", action="append", required=True, help="engine_id=trace.jsonl")
    parser.add_argument("--plans", required=True, type=Path)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    traces = {}
    for value in args.trace:
        key, path = value.split("=", 1)
        if key in traces:
            raise ValueError(f"Duplicate engine trace: {key}")
        traces[key] = read_jsonl(path)
    plans = {
        path.stem: read_json(path)
        for path in args.plans.glob("*.json")
        if path.name != "summary.json" and not path.name.endswith(".summary.json")
    }
    summary = analyze_run(
        read_json(args.cases),
        read_jsonl(args.results),
        traces,
        plans,
        args.schedule,
        args.backend,
        args.scheduler_policy,
    )
    summary["provenance"] = {
        "cases_sha256": sha256(args.cases),
        "results_sha256": sha256(args.results),
    }
    write_json(args.output, summary)
    print({"valid_timing_arm": summary["valid_timing_arm"], "issues": summary["validity_issues"]})
    raise SystemExit(0 if summary["valid_timing_arm"] else 1)


if __name__ == "__main__":
    main()
