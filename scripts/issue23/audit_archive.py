"""Verify frozen pilot bytes and recompute its results without a serving machine."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath

from analyze import analyze_run
from common import read_json, read_jsonl, sha256, write_json


def require(condition, message):
    if not condition:
        raise ValueError(message)


def metric(path, name, status):
    return sum(float(line.rsplit(" ", 1)[1]) for line in path.read_text(encoding="utf-8").splitlines()
               if line.startswith(name + "{") and f'status="{status}"' in line)


def audit_archive(root):
    root = root.resolve()
    entries = read_json(root / "manifest.json")
    paths = set()
    for entry in entries:
        name = entry["path"]
        path = root / name
        require(name not in paths and path.resolve().is_relative_to(root), "Invalid manifest path")
        paths.add(name)
        require(path.is_file() and not path.is_symlink(), f"Missing regular evidence file: {name}")
        require(path.stat().st_size == entry["bytes"] and sha256(path) == entry["sha256"],
                f"Frozen evidence changed: {name}")
    contract = read_json(root / "prepared/contract.json")
    payload = gzip.decompress((root / "prepared/smoke.json.gz").read_bytes())
    require(hashlib.sha256(payload).hexdigest() == contract["split_summary"]["smoke"]["sha256"],
            "Decompressed request identity differs from the contract")
    cases = json.loads(payload)
    require(len(cases) == contract["split_summary"]["smoke"]["requests"], "Unexpected request count")
    deployment = read_json(root / "config/deployment.actual.json")
    require(deployment["experiment_scope"] == "attention_transfer_diagnostic"
            and deployment["hybrid_prefix_reuse"] is False
            and deployment["qualification"]["pd_hybrid_correctness"] is False,
            "Archive scope differs from the reported diagnostic scope")
    plans = {e["engine_id"]: read_json(root / f"prepared/plans-smoke/{e['engine_id']}.json")
             for e in deployment["engines"]}
    for engine, plan in plans.items():
        provenance = plan["freeze_provenance"]
        require(provenance["engine_id"] == engine
                and provenance["cases_sha256"] == hashlib.sha256(payload).hexdigest()
                and provenance["contract_sha256"] == sha256(root / "prepared/contract.json")
                and provenance["trace_sha256"] == sha256(root / f"runs/qualification-capture-default-05/trace-{engine}.jsonl"),
                f"Frozen plan provenance differs: {engine}")
    summary = read_json(root / "runs/smoke-pilot-default-summary.json")
    expected_arms = {"rdma-smooth", "local_copy-smooth", "rdma-burst", "local_copy-burst"}
    require(set(summary["arms"]) == expected_arms and set(summary["arm_order"]) == expected_arms
            and len(summary["arm_order"]) == 4, "Four-arm archive is incomplete")
    evidence = read_json(root / "runs/smoke-pilot-result-evidence.json")
    report = read_json(root / "verification.json")
    arms, outputs = {}, {}
    for arm in summary["arm_order"]:
        item = summary["arms"][arm]
        run = root / "runs" / PurePosixPath(item["run_dir"]).name
        backend, schedule = arm.rsplit("-", 1)
        traces = {e: read_jsonl(run / f"trace-{e}.jsonl") for e in plans}
        results = read_jsonl(run / "client.jsonl")
        analysis = analyze_run(cases, results, traces, plans, schedule, backend,
                               deployment["scheduler_policy"])
        require(item["status"] == "completed" and analysis["valid_timing_arm"],
                f"Archive arm failed qualification: {arm}: {analysis['validity_issues']}")
        require(analysis == item["analysis"] == read_json(run / "analysis.json"),
                f"Recomputed analysis differs from archived values: {arm}")
        require(report["arms"][arm]["recomputed_summary_matches"], f"Archived verification failed: {arm}")
        by_id = {r["request_id"]: r for r in results}
        for case in cases:
            result = by_id[case["request_id"]]
            events = read_jsonl(run / f"sse/{case['request_id']}.jsonl")
            require(events and events[-1]["data"] == "[DONE]", "Incomplete archived SSE")
            text, usage, finish, first_text = [], None, None, None
            for event in events[:-1]:
                data = json.loads(event["data"])
                require(not data.get("error"), "Archived SSE contains an error")
                usage = data.get("usage") or usage
                for choice in data.get("choices", []):
                    piece = choice.get("text", "")
                    if piece:
                        if first_text is None:
                            first_text = event["offset_seconds"]
                        text.append(piece)
                    finish = choice.get("finish_reason") or finish
            require("".join(text) == result["text"] and usage == result["usage"]
                    and finish == result["finish_reason"] and first_text == result["ttft_seconds"],
                    f"Client record differs from raw SSE: {case['request_id']}")
            require(usage["prompt_tokens"] == case["input_tokens"]
                    and 0 < usage["completion_tokens"] <= case["payload"]["max_tokens"],
                    "Archived token usage differs from frozen input or generation bounds")
        transport = {}
        for engine, plan in plans.items():
            before = run / f"after-prepare-{engine}.prom"
            after = run / f"after-measurement-{engine}.prom"
            total = sum(op["logical_payload_bytes"] for r in plan["requests"] for op in r["operations"])
            actual = int(metric(after, "pegaflow_rdma_fetch_bytes_total", "ok")
                         - metric(before, "pegaflow_rdma_fetch_bytes_total", "ok"))
            errors = metric(after, "pegaflow_rdma_fetch_total_total", "error") - metric(before, "pegaflow_rdma_fetch_total_total", "error")
            require(actual == total == evidence["arms"][arm]["transport"][engine]["measurement_fetch_bytes"]
                    and errors == 0, f"Fetch metrics differ from frozen bytes: {engine}")
            transport[engine] = {"logical_fetch_bytes": actual, "fetch_errors": errors}
        outputs[arm] = {rid: (r["text"], r["usage"]) for rid, r in by_id.items()}
        arms[arm] = {"requests": len(results), "recomputed_analysis_matches": True,
                     "sse_matches_client": True, "ttft_seconds": analysis["ttft_seconds"],
                     "e2e_seconds": analysis["e2e_seconds"], "accuracy": analysis["accuracy"],
                     "output_truncated": sum(q["output_truncated"] for q in analysis["quality"]),
                     "transport": transport}
    first = summary["arm_order"][0]
    for arm, output in outputs.items():
        matches = sum(value == outputs[first][rid] for rid, value in output.items())
        require(matches == evidence["output_comparisons"][arm]["exact_text_and_usage_matches"],
                f"Cross-arm output comparison differs: {arm}")
    return {"status": "passed", "manifest_files_verified": len(entries),
            "measured_requests_verified": sum(a["requests"] for a in arms.values()),
            "scope": deployment["experiment_scope"], "hybrid_prefix_reuse": False,
            "pd_hybrid_correctness": False, "arms": arms}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.resolve().is_relative_to(args.archive.resolve()),
            "Write the review output outside the frozen archive")
    result = audit_archive(args.archive)
    write_json(args.output, result)
    print({k: v for k, v in result.items() if k != "arms"})


if __name__ == "__main__":
    main()
