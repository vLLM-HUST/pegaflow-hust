"""Join measured client, transfer and actual model work evidence by request."""

import argparse
from pathlib import Path
from statistics import mean

from common import distribution, read_json, read_jsonl, write_json


def stats(values):
    return {**distribution(values), "mean": mean(values) if values else None}


def stable_request(raw, ids):
    return next((rid for rid in ids if raw == rid or raw.startswith("cmpl-" + rid + "-")), None)


def summarize(root, label):
    cases = read_json(root / "prepared/smoke.json")
    ids = {case["request_id"] for case in cases}
    arms, paired = {}, []
    for path in sorted((root / "runs").glob(label + "-round-*")):
        status = read_json(path / "status.json") if (path / "status.json").exists() else {}
        contract = read_json(path / "run-contract.json")
        arm = contract["arm"]
        if not (path / "analysis.json").exists():
            arms[path.name] = {"arm": arm, "status": status, "analysis": "pending"}
            continue
        analysis = read_json(path / "analysis.json")
        trace = read_jsonl(path / "trace-decode-engine.jsonl")
        activation = next(e for e in trace if e.get("event") == "experiment_activated")
        start = activation["wall_time_unix_ns"]
        first, errors, dropped = {}, [], []
        for telemetry in (path / "telemetry/decode").glob("*.jsonl"):
            for event in read_jsonl(telemetry):
                if event.get("unix_ns", 0) < start:
                    continue
                if event.get("event") == "model_work":
                    for work in event["requests"]:
                        rid = stable_request(work["request_id"], ids)
                        if rid:
                            key = (rid, event["tp_rank"])
                            candidate = {**work, "unix_ns": event["unix_ns"]}
                            if key not in first or candidate["unix_ns"] < first[key]["unix_ns"]:
                                first[key] = candidate
                elif event.get("event") == "model_step_error":
                    errors.append(event)
                elif event.get("event") == "telemetry_closed":
                    dropped.append(event.get("dropped_events", 0))
        report = analysis["engine_reports"]["decode-engine"]
        transfer = {row["request_id"]: row for row in report["requests"]}
        completed = [
            e
            for e in trace
            if e.get("event") == "bundle_completed" and e.get("wall_time_unix_ns", 0) >= start
        ]
        for result in read_jsonl(path / "client.jsonl"):
            rid = result["request_id"]
            paired.append(
                {
                    "run": path.name,
                    "arm": arm,
                    "request_id": rid,
                    "ttft_seconds": result.get("ttft_seconds"),
                    "e2e_seconds": result.get("e2e_seconds"),
                    "usage": result.get("usage"),
                    "output_sha256": result.get("output_sha256"),
                    "first_model_work": {str(rank): first.get((rid, rank)) for rank in (0, 1)},
                    "transfer": transfer.get(rid),
                }
            )
        arms[path.name] = {
            "arm": arm,
            "status": status,
            "valid_timing_arm": analysis["valid_timing_arm"],
            "validity_issues": analysis["validity_issues"],
            "client_requests": analysis["client_requests"],
            "client_failures": analysis["client_failures"],
            "ttft_seconds": stats([r["ttft_seconds"] for r in paired if r["run"] == path.name]),
            "e2e_seconds": stats([r["e2e_seconds"] for r in paired if r["run"] == path.name]),
            "transfer_completion_ms": stats(
                [r["completion_observation_ms"] for r in transfer.values()]
            ),
            "posting_lateness_ms": report["posting_lateness_ms"],
            "payload_bytes": sum(e["completed_payload_bytes"] for e in completed),
            "first_model_work_records": len(first),
            "expected_model_work_records": len(ids) * 2,
            "computed_token_counts": sorted({w["computed_tokens"] for w in first.values()}),
            "scheduled_token_counts": sorted({w["scheduled_tokens"] for w in first.values()}),
            "model_step_errors": errors,
            "reported_telemetry_drops": dropped,
            "copy_ms": stats([r["copy_ms"] for r in transfer.values() if "copy_ms" in r]),
        }
    return {
        "schema_version": 1,
        "cases": len(cases),
        "arms": arms,
        "paired_requests": paired,
        "interpretation": "One initial round; quantify repeated balanced rounds before making p99 causal claims.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.root, args.label)
    write_json(args.output, result)
    for name, arm in result["arms"].items():
        print(name, arm.get("valid_timing_arm"), arm.get("ttft_seconds"), flush=True)
