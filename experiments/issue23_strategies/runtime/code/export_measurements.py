"""Export portable real measurements without turning replay results into truth."""

import argparse
import hashlib
import json
import math
from pathlib import Path


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def export(p, d, workload, output, audit=None):
    output.mkdir(parents=True, exist_ok=False)
    protocol, costs = read(p / "protocol.json"), read(p / "cost-model.json")
    inputs = {row["id"]: row for row in
              (json.loads(line) for line in (workload / "requests.jsonl").read_text(encoding="utf-8").splitlines())}
    assert digest(workload / "requests.jsonl") == protocol["source_manifest"]["requests_sha256"]
    records, phases, metric_summaries = [], {}, {}
    for phase in ("baseline", "treatment"):
        path = p / phase / "requests.jsonl"
        if not path.exists():
            continue
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        phases[phase] = {"requests": len(rows), "raw_ledger_sha256": digest(path)}
        predecessor_ends, corrected_times = {}, []
        for row in rows:
            item = {key: row[key] for key in (
                "id", "task", "step_id", "arrival_seconds", "dispatch_seconds", "route",
                "route_seconds", "route_cpu_seconds", "history_fetch", "handoff",
                "pre_P_wait_and_copy_seconds", "handoff_and_wait_seconds", "P_gate_wait_seconds",
                "D_gate_wait_seconds", "P_submission_seconds", "D_submission_seconds",
                "ttft_seconds", "completion_seconds", "reusable_tokens_no_eviction")}
            item.update(phase=phase, prompt_tokens=inputs[row["id"]]["prompt_tokens"])
            item["prefix_hashes"] = row["P"]["observation"]["full_prompt_hashes"][:
                (item["prompt_tokens"] - 1) // protocol["block"]]
            for role in ("P", "D"):
                item[role] = {key: row[role][key] for key in (
                    "slot", "num_cached_tokens", "uncached_prompt_tokens", "wall_seconds",
                    "ttft_seconds", "output_sha256", "explicit_restore_limit")}
                item[role + "_wire"] = row[role + "_wire"]
            if "pipeline_timing" in row:
                item["pipeline_timing"] = row["pipeline_timing"]
            for name in ("offered_arrival_seconds", "causality_wait_seconds", "offered_to_first_seconds"):
                if name in row:
                    item[name] = row[name]
            nominal = row.get("offered_arrival_seconds", row["arrival_seconds"])
            ready = max(nominal, predecessor_ends.get(row["task"], 0))
            first = row["D_submission_seconds"] + row["D_wire"]["first_seconds"]
            predecessor_ends[row["task"]] = row["arrival_seconds"] + row["completion_seconds"]
            item["causal_ready_seconds_reconstructed"] = ready
            item["upstream_dependency_wait_seconds_reconstructed"] = ready - nominal
            item["server_TTFT_seconds_reconstructed"] = first - ready
            item["offered_to_first_seconds"] = first - nominal
            corrected_times.append(first - ready)
            records.append(item)
        corrected_times.sort()
        metric_summaries[phase] = {
            "causal_ready_TTFT_p50_seconds": corrected_times[math.ceil(0.5 * len(rows)) - 1],
            "causal_ready_TTFT_p99_seconds_descriptive": corrected_times[math.ceil(0.99 * len(rows)) - 1],
            "old_raw_metric_origin": "causal_ready" if protocol.get("TTFT_origin") else "nominal offered time including predecessor wait"}
        for name in ("cache-actions.json", "policy.json", "pipeline-summary.json", "initial-inventory.json"):
            source = p / phase / name
            if source.exists():
                (output / f"{phase}-{name}").write_bytes(source.read_bytes())
    validation = read(audit) if audit is not None else None
    metadata = {"source_P": str(p), "source_D": str(d), "phases": phases,
                "metric_summaries": metric_summaries,
                "source_protocol_sha256": digest(p / "protocol.json"),
                "source_cost_sha256": digest(p / "cost-model.json"),
                "P_supervisor": read(p / "result.json"), "D_supervisor": read(d / "result.json"),
                "independent_audit": validation,
                "use": "empirical calibration and simulator validation; not simulated proof of RDMA or accuracy",
                "test_inputs_become_exposed_if_used_for_refitting": True,
                "stage_feature_scope": "API/engine wall times include synchronization; not isolated kernel/wire time",
                "simulated_and_measured_results_must_be_reported_separately": True}
    for name, value in (("metadata.json", metadata), ("protocol.json", protocol),
                        ("cost-model.json", costs), ("workload-manifest.json", protocol["source_manifest"])):
        (output / name).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    with (output / "measurements.jsonl").open("x", encoding="utf-8") as stream:
        for row in records:
            stream.write(json.dumps(row) + "\n")
    # Session IDs, lengths and relative arrivals suffice for service replay;
    # no original trajectory commands are executed or required by this bundle.
    with (output / "request-stream.jsonl").open("x", encoding="utf-8") as stream:
        observed_hashes = {row["id"]: row["prefix_hashes"] for row in records}
        arrivals = protocol.get("arrival_condition", {}).get("offered_arrivals_seconds")
        for position, key in enumerate(protocol["test_order"]):
            row = inputs[key]
            stream.write(json.dumps({"id": key, "task": row["task"], "step_id": row["step_id"],
                "prompt_tokens": row["prompt_tokens"],
                "offered_arrival_seconds": arrivals[position] if arrivals is not None else position * protocol["arrival_interval_seconds"],
                "prefix_hashes": observed_hashes.get(key),
                "reusable_tokens_no_eviction": row["reusable_tokens_no_eviction"]}) + "\n")
    hashes = {path.name: digest(path) for path in sorted(output.iterdir()) if path.is_file()}
    (output / "SHA256SUMS.json").write_text(json.dumps(hashes, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "measurements": len(records), "phases": phases,
                      "audit_passed": bool(validation and validation.get("passed"))}))


def main():
    parser = argparse.ArgumentParser()
    for name in ("p", "d", "workload", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--audit", type=Path)
    args = parser.parse_args()
    export(args.p, args.d, args.workload, args.output, args.audit)


if __name__ == "__main__":
    main()
