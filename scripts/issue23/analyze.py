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


def trace_analysis(events, plan, schedule, backend):
    validate_plan(plan)
    expected = {r["request_id"]: r for r in plan["requests"]}
    active = [e for e in events if e.get("event") == "experiment_activated"]
    issues = []
    if len(active) != 1:
        return {"valid": False, "issues": ["expected one activation"]}
    opened = [e for e in events if e.get("event") == "experiment_opened"]
    if len(opened) != 1 or opened[0].get("schedule") != schedule or opened[0].get("backend") != backend:
        issues.append("experiment backend or schedule identity mismatch")
    if schedule == "smooth" and opened and opened[0].get("smooth_scheduler_error"):
        issues.append("smooth scheduler configuration failed")
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
        eligible, posted, completed = (by_kind[k][rid] for k in ["bundle_eligible", "bundle_posted", "bundle_completed"])
        if observed.get("phase") != "measured" or observed["operations"] != request["operations"]:
            issues.append(f"manifest drift: {rid}")
        for event in [eligible, posted]:
            if event.get("cohort_id") != request["cohort_id"] or event.get("smooth_slot") != request["smooth_slot"]:
                issues.append(f"cohort or slot identity mismatch: {rid}")
        logical = sum(op["logical_payload_bytes"] for op in request["operations"])
        if completed.get("status") != "ok" or completed.get("completed_payload_bytes") != logical:
            issues.append(f"completion error or byte mismatch: {rid}")
        offset = [0, 10, 20, 30][request["smooth_slot"]] if schedule == "smooth" else 15
        if posted.get("scheduled_offset_ms") != offset:
            issues.append(f"target offset mismatch: {rid}")
        gate = tg[request["cohort_id"]]
        if not eligible["monotonic_ns"] <= posted["monotonic_ns"] <= completed["monotonic_ns"]:
            issues.append(f"event timing order mismatch: {rid}")
        rows.append({"request_id": rid, "cohort_id": request["cohort_id"],
                     "gate_wait_ms": (gate - eligible["monotonic_ns"]) / 1e6,
                     "posting_lateness_ms": (posted["monotonic_ns"] - gate) / 1e6 - offset,
                     "completion_observation_ms": (completed["monotonic_ns"] - posted["monotonic_ns"]) / 1e6})
    if schedule == "burst":
        spread = [e for e in measured if e.get("event") == "cohort_posting_spread"]
        cohorts = {r["cohort_id"] for r in plan["requests"]}
        if Counter(e.get("cohort_id") for e in spread) != Counter({c: 1 for c in cohorts}):
            issues.append("missing or repeated burst spread checks")
        if any(not e.get("pass") or e["posting_spread_ns"] > 1_000_000 for e in spread):
            issues.append("burst posting spread exceeded 1 ms")
    if any(e.get("event") == "cohort_post_task_failed" for e in measured):
        issues.append("cohort posting task failed")
    return {"valid": not issues, "issues": issues, "requests": rows,
            "scheduler_policy": opened[0].get("smooth_scheduler_policy") if opened else None,
            "scheduler_priority": opened[0].get("smooth_scheduler_priority") if opened else None,
            "posting_lateness_ms": distribution([r["posting_lateness_ms"] for r in rows]),
            "clock_note": "Each engine's monotonic clock is analyzed independently; completion is an observation timestamp."}


def analyze_run(cases, results, traces, plans, schedule, backend):
    by_id = {r["request_id"]: r for r in results}
    expected = {c["request_id"] for c in cases}
    failures = []
    if set(by_id) != expected or len(by_id) != len(results):
        failures.append("client result identity mismatch")
    if any(r.get("status") != "ok" for r in results):
        failures.append("one or more client requests failed")
    if any(r.get("dispatch_lateness_ms", 0) > 100 for r in results):
        failures.append("open-loop dispatch lateness exceeded 100 ms")
    trace_reports = {engine: trace_analysis(events, plans[engine], schedule, backend) for engine, events in traces.items()}
    if not trace_reports or any(not r["valid"] for r in trace_reports.values()):
        failures.append("engine trace qualification failed")
    quality = []
    for case in cases:
        result = by_id.get(case["request_id"], {})
        answer = extracted_answer(result.get("text", "")) if case["dataset"] == "mmlu-pro" else None
        quality.append({"request_id": case["request_id"], "category": case["category"],
                        "extracted_answer": answer, "correct": answer == case["reference_answer"] if case["dataset"] == "mmlu-pro" else None,
                        "parse_failed": answer is None if case["dataset"] == "mmlu-pro" else None,
                        "finish_reason": result.get("finish_reason"),
                        "output_tokens": (result.get("usage") or {}).get("completion_tokens"),
                        "output_truncated": result.get("finish_reason") == "length"})
    successful = [r for r in results if r.get("status") == "ok"]
    return {"valid_timing_arm": not failures, "validity_issues": failures,
            "client_requests": len(results), "client_failures": len(results) - len(successful),
            "ttft_seconds": distribution([r["ttft_seconds"] for r in successful]),
            "e2e_seconds": distribution([r["e2e_seconds"] for r in successful]),
            "dispatch_lateness_ms": distribution([r["dispatch_lateness_ms"] for r in results]),
            "quality": quality, "engine_reports": trace_reports,
            "accuracy": sum(q["correct"] is True for q in quality) / len(quality) if quality and cases[0]["dataset"] == "mmlu-pro" else None,
            "grading_note": "FrontierScience outputs require a separately frozen grader or documented manual review."}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", required=True)
    parser.add_argument("--results", required=True)
    parser.add_argument("--schedule", choices=["smooth", "burst"], required=True)
    parser.add_argument("--backend", choices=["rdma", "local_copy"], required=True)
    parser.add_argument("--trace", action="append", required=True, help="engine_id=trace.jsonl")
    parser.add_argument("--plans", required=True, type=Path)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    traces = {key: read_jsonl(path) for key, path in (value.split("=", 1) for value in args.trace)}
    plans = {key: read_json(args.plans / f"{key}.json") for key in traces}
    summary = analyze_run(read_json(args.cases), read_jsonl(args.results), traces, plans, args.schedule, args.backend)
    summary["provenance"] = {"cases_sha256": sha256(args.cases), "results_sha256": sha256(args.results)}
    write_json(args.output, summary)
    print({"valid_timing_arm": summary["valid_timing_arm"], "issues": summary["validity_issues"]})
    raise SystemExit(0 if summary["valid_timing_arm"] else 1)


if __name__ == "__main__":
    main()
