"""Provisional P-side four-arm diagnosis; never substitutes for dual-node audit."""

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"n": 0}
    return {"n": len(values), "mean": sum(values) / len(values),
            **{f"p{p}": values[math.ceil(p * len(values) / 100) - 1]
               for p in (50, 90, 95, 99)}, "max": values[-1]}


def analyze(job):
    arms, hashes, rows_by_arm = {}, {}, {}
    for arm in ("B-before", "T-parent", "T-phase", "B-after"):
        directory = job / arm / "baseline"
        path = directory / "requests.jsonl"
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        rows_by_arm[arm] = {r["id"]: r for r in rows}
        if len(rows_by_arm[arm]) != len(rows):
            raise ValueError("Duplicate request identity")
        hashes[arm] = hashlib.sha256(path.read_bytes()).hexdigest()
        measures = {}
        for field in ("ttft_seconds", "offered_to_first_seconds", "causality_wait_seconds",
                      "route_seconds", "route_cpu_seconds", "P_gate_wait_seconds",
                      "D_gate_wait_seconds", "pre_P_wait_and_copy_seconds",
                      "handoff_and_wait_seconds"):
            values = [r[field] for r in rows if field in r]
            measures[field] = {**quantiles(values), "sum": sum(values)}
        for role in ("P", "D"):
            values = [r[role + "_wire"]["complete_seconds"] - r[role + "_wire"]["first_seconds"]
                      for r in rows]
            measures[role + "_wire_post_first_tail"] = {**quantiles(values), "sum": sum(values)}
        for role in ("P", "D"):
            errors = [r[role + "_submission_seconds"] + r[role + "_wire"][
                      "complete_seconds" if role == "P" else "first_seconds"] -
                      r["initial_route"]["predicted_p_end" if role == "P" else "predicted_first"]
                      for r in rows]
            measures[role + "_endpoint_prediction_error_not_pure_queue"] = {
                "bias": sum(errors) / len(errors),
                "MAE": sum(abs(v) for v in errors) / len(errors),
                "RMSE": math.sqrt(sum(v * v for v in errors) / len(errors))}
        for role in ("P", "D"):
            measures[role + "_FIFO_wait"] = quantiles([
                r["pipeline_timing"][role + "_started_seconds"] -
                r["pipeline_timing"][role + "_enqueued_seconds"] for r in rows])
        operation_path = directory / "operation-costs.json"
        operation = {}
        if operation_path.exists():
            records = [r for r in read(operation_path)["records"] if not r.get("calibration")]
            for node in range(4):
                for field in ("restore_seconds", "prefill_sync_path_seconds", "decode_tail_seconds"):
                    values = [r[field] for r in records if r["node"] == node and field in r]
                    operation[f"node{node}/{field}"] = {**quantiles(values), "sum": sum(values)}
        arms[arm] = {"requests": len(rows), "timing": measures, "operations": operation,
                     "phase": read(directory / "phase-complete.json"),
                     "P_routes": dict(Counter(r["route"]["p"] for r in rows)),
                     "D_routes": dict(Counter(r["route"]["d"] for r in rows)),
                     "P_cache_tokens": sum(r["P"]["num_cached_tokens"] for r in rows),
                     "D_cache_tokens": sum(r["D"]["num_cached_tokens"] for r in rows),
                     "explicit_copied_payload_bytes": sum(r.get("handoff", {}).get("receipt", {}).get(
                         "payload_bytes", 0) + r.get("history_fetch", {}).get("receipt", {}).get(
                         "payload_bytes", 0) for r in rows)}
    reference = rows_by_arm["B-before"]
    for arm, rows in rows_by_arm.items():
        if set(rows) != set(reference) or len(rows) != 95:
            raise ValueError("Incomplete or changed request identity set")
        for key, row in rows.items():
            original = reference[key]
            if row["D"]["output_token_ids"] != original["D"]["output_token_ids"]:
                raise ValueError("Cross-arm output IDs differ")
            if len(row["D"]["output_token_ids"]) != 128 or row["P"]["output_token_ids"][0] != row["D"]["output_token_ids"][0]:
                raise ValueError("Output length or P/D first-token contract differs")
            if row["D"]["prompt_tokens"] != original["D"]["prompt_tokens"]:
                raise ValueError("Prompt length differs")
    paired = {}
    for arm in ("T-parent", "T-phase", "B-after"):
        values = []
        for key, row in rows_by_arm[arm].items():
            b = reference[key]
            values.append({"id": key, "TTFT_delta": row["ttft_seconds"] - b["ttft_seconds"],
                           "nominal_delta": row["offered_to_first_seconds"] - b["offered_to_first_seconds"],
                           "dependency_delta": row["causality_wait_seconds"] - b["causality_wait_seconds"],
                           "P_cache_delta": row["P"]["num_cached_tokens"] - b["P"]["num_cached_tokens"],
                           "D_cache_delta": row["D"]["num_cached_tokens"] - b["D"]["num_cached_tokens"]})
        paired[arm] = sorted(values, key=lambda v: abs(v["TTFT_delta"]), reverse=True)
    return {"kind": "P-side-provisional-diagnosis-not-independent-audit",
            "dual_node_archive_audit_pending": True, "clean_hardware_certified": False,
            "p99_definition": "nearest-rank; n95 p99 equals maximum",
            "nested_restore_timers_are_not_added_to_total": True,
            "source_request_SHA256": hashes, "arms": arms, "paired_requests": paired}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Preserve prior evidence; choose an independent output")
    result = analyze(args.job)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, ensure_ascii=False)
    print(json.dumps({"status": result["kind"], "arms": {
        arm: {"TTFT": data["timing"]["ttft_seconds"], "P_routes": data["P_routes"],
              "D_routes": data["D_routes"], "phase": data["phase"]}
        for arm, data in result["arms"].items()}}, indent=2))


if __name__ == "__main__":
    main()
